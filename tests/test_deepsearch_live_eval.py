from __future__ import annotations

import json
from pathlib import Path

import pytest

from scholarpath.evals import deepsearch_live as live_eval

pytestmark = [
    pytest.mark.filterwarnings("error::RuntimeWarning"),
    pytest.mark.filterwarnings("error::ResourceWarning"),
]


def test_compute_duplicate_ratio_from_rows() -> None:
    rows = [
        {
            "school_id": "s1",
            "variable_name": "acceptance_rate",
            "source_name": "scorecard",
            "value_text": "5%",
            "value_numeric": None,
        },
        {
            "school_id": "s1",
            "variable_name": "acceptance_rate",
            "source_name": "scorecard",
            "value_text": "5%",
            "value_numeric": None,
        },
        {
            "school_id": "s1",
            "variable_name": "acceptance_rate",
            "source_name": "niche",
            "value_text": "6%",
            "value_numeric": None,
        },
    ]
    ratio = live_eval.compute_duplicate_ratio_from_rows(rows, freshness_days=90)
    assert ratio["total_rows"] == 3
    assert ratio["duplicate_rows"] == 1
    assert ratio["duplicate_ratio"] == pytest.approx(0.3333, abs=1e-4)


def test_grade_live_eval_status_thresholds() -> None:
    good = live_eval.grade_live_eval_status(
        critical_slot_recall=0.82,
        db_hit_uplift=0.24,
        db_duplicate_row_ratio_90d=0.01,
    )
    assert good["overall"] == "good"

    watch = live_eval.grade_live_eval_status(
        critical_slot_recall=0.70,
        db_hit_uplift=0.12,
        db_duplicate_row_ratio_90d=0.03,
    )
    assert watch["overall"] == "watch"

    bad = live_eval.grade_live_eval_status(
        critical_slot_recall=0.45,
        db_hit_uplift=0.05,
        db_duplicate_row_ratio_90d=0.08,
    )
    assert bad["overall"] == "bad"


def test_report_schema_serialization() -> None:
    pass_report = live_eval.DeepSearchPassReport(
        pass_name="pass1",
        eval_run_id="run-p1",
        execution_mode="local_fallback",
        status="ok",
        started_at="2026-04-01T00:00:00+00:00",
        ended_at="2026-04-01T00:00:02+00:00",
        elapsed_seconds=2.0,
    )
    report = live_eval.DeepSearchLiveEvalReport(
        run_id="run-id",
        generated_at="2026-04-01T00:00:05+00:00",
        config={"dataset_id": "test"},
        pass1=pass_report,
        pass2=None,
        metrics={"required_slot_recall": 0.9},
        status="good",
        recommendations=["ok"],
    )
    payload = report.to_dict()
    assert payload["run_id"] == "run-id"
    assert payload["pass1"]["eval_run_id"] == "run-p1"
    assert payload["metrics"]["required_slot_recall"] == 0.9


def test_load_default_coldmix_dataset_has_fixed_10_10_cohorts() -> None:
    dataset = live_eval.load_dataset(live_eval.DEFAULT_DATASET_PATH)
    assert dataset.dataset_id == "deepsearch_live_coldmix_v1"
    assert len(dataset.schools) == 20
    in_db = [case for case in dataset.schools if case.cohort == live_eval.COHORT_IN_DB]
    out_db = [case for case in dataset.schools if case.cohort == live_eval.COHORT_OUT_DB]
    assert len(in_db) == 10
    assert len(out_db) == 10

    in_db_names = {case.school_name for case in in_db}
    out_db_names = {case.school_name for case in out_db}
    assert "Massachusetts Institute of Technology" in in_db_names
    assert "Stanford University" in in_db_names
    assert "Brown University" in out_db_names
    assert "New York University" in out_db_names


@pytest.mark.asyncio
async def test_cold_reset_out_group_deletes_conflicts_before_data_points(
    monkeypatch,
) -> None:
    call_sql: list[str] = []

    async def fake_resolve_school_ids(*, names: list[str]) -> dict[str, str]:
        assert names == ["Brown University", "Rice University"]
        return {
            "brown university": "school-1",
            "rice university": "school-2",
        }

    class _ScalarResult:
        def __init__(self, values: list[str]) -> None:
            self._values = values

        def all(self) -> list[str]:
            return list(self._values)

    class _Result:
        def __init__(self, *, values: list[str] | None = None, rowcount: int = 0) -> None:
            self._values = values or []
            self.rowcount = rowcount

        def scalars(self) -> _ScalarResult:
            return _ScalarResult(self._values)

    class _FakeSession:
        async def execute(self, stmt: object) -> _Result:
            sql = str(stmt)
            call_sql.append(sql)
            if sql.startswith("SELECT data_points.id"):
                return _Result(values=["dp-1", "dp-2"])
            if sql.startswith("DELETE FROM conflicts"):
                return _Result(rowcount=3)
            if sql.startswith("DELETE FROM data_points"):
                return _Result(rowcount=2)
            raise AssertionError(f"Unexpected SQL: {sql}")

        async def commit(self) -> None:
            return None

    class _SessionFactory:
        def __call__(self) -> "_SessionFactory":
            return self

        async def __aenter__(self) -> _FakeSession:
            return _FakeSession()

        async def __aexit__(
            self,
            exc_type: object,
            exc: object,
            tb: object,
        ) -> bool:
            return False

    monkeypatch.setattr(live_eval, "_resolve_school_ids", fake_resolve_school_ids)
    monkeypatch.setattr("scholarpath.db.session.async_session_factory", _SessionFactory())

    stats = await live_eval._cold_reset_out_group_data(
        out_cases=[
            live_eval.SchoolEvalCase(
                school_name="Brown University",
                cohort=live_eval.COHORT_OUT_DB,
            ),
            live_eval.SchoolEvalCase(
                school_name="Rice University",
                cohort=live_eval.COHORT_OUT_DB,
            ),
        ],
        freshness_days=90,
    )

    assert stats["schools_targeted"] == 2
    assert stats["schools_matched"] == 2
    assert stats["deleted_conflicts"] == 3
    assert stats["deleted_data_points"] == 2

    conflict_delete_idx = next(
        idx for idx, sql in enumerate(call_sql) if sql.startswith("DELETE FROM conflicts")
    )
    datapoint_delete_idx = next(
        idx for idx, sql in enumerate(call_sql) if sql.startswith("DELETE FROM data_points")
    )
    assert conflict_delete_idx < datapoint_delete_idx
    select_sql = next(sql for sql in call_sql if sql.startswith("SELECT data_points.id"))
    assert "data_points.school_id IN" in select_sql
    assert "data_points.crawled_at >=" in select_sql


@pytest.mark.asyncio
async def test_execute_deepsearch_task_prefers_celery(monkeypatch) -> None:
    async def fake_celery(**_: object) -> dict:
        return {"search_metadata": {}, "schools": [], "errors": []}

    async def fake_local(**_: object) -> dict:
        raise AssertionError("Local fallback should not be used")

    monkeypatch.setattr(live_eval, "_run_via_celery", fake_celery)
    monkeypatch.setattr(live_eval, "_run_via_local", fake_local)

    mode, result, errors = await live_eval._execute_deepsearch_task(
        student_id="00000000-0000-0000-0000-000000000001",
        school_names=["MIT"],
        required_fields=None,
        freshness_days=90,
        max_internal_websearch_calls_per_school=1,
        budget_mode="balanced",
        eval_run_id="run-p1",
        timeout_seconds=5,
        poll_interval_seconds=0.1,
    )
    assert mode == "celery"
    assert result["errors"] == []
    assert errors == []


@pytest.mark.asyncio
async def test_execute_deepsearch_task_falls_back_to_local(monkeypatch) -> None:
    async def fake_celery(**_: object) -> dict:
        raise RuntimeError("broker unavailable")

    async def fake_local(**_: object) -> dict:
        return {"search_metadata": {}, "schools": [], "errors": []}

    monkeypatch.setattr(live_eval, "_run_via_celery", fake_celery)
    monkeypatch.setattr(live_eval, "_run_via_local", fake_local)

    mode, _, errors = await live_eval._execute_deepsearch_task(
        student_id="00000000-0000-0000-0000-000000000001",
        school_names=["MIT"],
        required_fields=None,
        freshness_days=90,
        max_internal_websearch_calls_per_school=1,
        budget_mode="balanced",
        eval_run_id="run-p1",
        timeout_seconds=5,
        poll_interval_seconds=0.1,
    )
    assert mode == "local_fallback"
    assert errors
    assert errors[0]["stage"] == "celery"


@pytest.mark.asyncio
async def test_run_deepsearch_live_eval_two_pass_uplift(monkeypatch, tmp_path: Path) -> None:
    dataset = {
        "dataset_id": "tiny",
        "version": "1.0",
        "required_fields_default": ["acceptance_rate", "city"],
        "rules_default": {
            "acceptance_rate": {"kind": "numeric_range", "min": 0, "max": 100},
            "city": {"kind": "non_empty_text"},
        },
        "schools": [
            {"school_name": "MIT", "aliases": ["Massachusetts Institute of Technology"]},
            {"school_name": "Stanford University", "aliases": ["Stanford"]},
        ],
    }
    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(json.dumps(dataset), encoding="utf-8")

    async def fake_execute(**kwargs: object):
        eval_run_id = str(kwargs["eval_run_id"])
        if eval_run_id.endswith("-p1"):
            return (
                "local_fallback",
                {
                    "search_metadata": {
                        "db_hit_ratio": 0.15,
                        "fallback_trigger_rate": 0.5,
                        "self_source_calls": 6,
                        "internal_websearch_calls": 2,
                        "persisted_data_points": 12,
                        "tokens_by_stage": {"total_estimated": 22000},
                        "raw_fact_count_before_merge": 20,
                        "unique_fact_count_after_merge": 14,
                        "dedupe_drop_count": 6,
                        "multi_source_agreement_count": 2,
                        "multi_source_conflict_count": 1,
                    },
                    "schools": [
                        {
                            "name": "Massachusetts Institute of Technology",
                            "aliases": ["MIT"],
                            "data": {
                                "acceptance_rate": {"value": 4.0},
                                "city": {"value": "Cambridge"},
                            },
                        },
                    ],
                    "errors": [],
                },
                [],
            )
        return (
            "local_fallback",
            {
                "search_metadata": {
                    "db_hit_ratio": 0.52,
                    "fallback_trigger_rate": 0.1,
                    "self_source_calls": 2,
                    "internal_websearch_calls": 0,
                    "persisted_data_points": 2,
                    "tokens_by_stage": {"total_estimated": 9000},
                    "raw_fact_count_before_merge": 12,
                    "unique_fact_count_after_merge": 10,
                    "dedupe_drop_count": 2,
                    "multi_source_agreement_count": 3,
                    "multi_source_conflict_count": 0,
                },
                "schools": [
                    {
                        "name": "Massachusetts Institute of Technology",
                        "aliases": ["MIT"],
                        "data": {
                            "acceptance_rate": {"value": 4.0},
                            "city": {"value": "Cambridge"},
                        },
                    },
                    {
                        "name": "Stanford University",
                        "aliases": ["Stanford"],
                        "data": {
                            "acceptance_rate": {"value": 4.0},
                            "city": {"value": "Stanford"},
                        },
                    },
                ],
                "errors": [],
            },
            [],
        )

    async def fake_judge_pass(**kwargs: object) -> dict:
        if str(kwargs["pass_name"]) == "pass1":
            return {
                "pass_name": "pass1",
                "eval_run_id": str(kwargs["eval_run_id"]),
                "status": "ok",
                "school_results": [],
                "school_count": 2,
                "avg_school_score": 72.0,
                "field_pass_rate": 0.75,
                "errors": [],
            }
        return {
            "pass_name": "pass2",
            "eval_run_id": str(kwargs["eval_run_id"]),
            "status": "ok",
            "school_results": [],
            "school_count": 2,
            "avg_school_score": 85.0,
            "field_pass_rate": 0.90,
            "errors": [],
        }

    async def fake_judge_summary(**kwargs: object) -> dict:
        return {
            "run_id": str(kwargs["run_id"]),
            "eval_run_id": str(kwargs["eval_run_id"]),
            "status": "good",
            "overall_score": 86.0,
            "score_uplift": 13.0,
            "highlights": ["db hit improved"],
            "risks": [],
            "recommendations": ["keep"],
            "error": None,
        }

    async def fake_usage(
        eval_run_id: str,
        caller_prefixes: tuple[str, ...] | None = None,
    ) -> dict:
        prefixes = caller_prefixes or ()
        if eval_run_id.endswith("-p1"):
            if prefixes == ("search.",):
                return {"calls": 4, "errors": 0, "tokens": 18000, "p95_latency_ms": 2100}
            if prefixes == ("eval.deepsearch.judge.",):
                return {"calls": 2, "errors": 0, "tokens": 2200, "p95_latency_ms": 900}
            return {"calls": 6, "errors": 0, "tokens": 20200, "p95_latency_ms": 2100}
        if eval_run_id.endswith("-p2"):
            if prefixes == ("search.",):
                return {"calls": 2, "errors": 0, "tokens": 7000, "p95_latency_ms": 1200}
            if prefixes == ("eval.deepsearch.judge.",):
                return {"calls": 2, "errors": 0, "tokens": 1800, "p95_latency_ms": 800}
            return {"calls": 4, "errors": 0, "tokens": 8800, "p95_latency_ms": 1200}
        if eval_run_id.endswith("-judge-summary"):
            return {"calls": 1, "errors": 0, "tokens": 600, "p95_latency_ms": 700}
        return {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0}

    async def fake_audit(**_: object) -> dict:
        return {
            "status": "ok",
            "duplicate_ratio": 0.01,
            "total_rows": 100,
            "unique_rows": 99,
            "duplicate_rows": 1,
        }

    cohort_ratios = [
        {live_eval.COHORT_IN_DB: 1.0, live_eval.COHORT_OUT_DB: 0.0},
        {live_eval.COHORT_IN_DB: 1.0, live_eval.COHORT_OUT_DB: 0.45},
    ]

    async def fake_compute_cohort_ratio(**_: object) -> dict[str, float]:
        if cohort_ratios:
            return cohort_ratios.pop(0)
        return {live_eval.COHORT_IN_DB: 1.0, live_eval.COHORT_OUT_DB: 0.45}

    async def fake_cold_reset(**_: object) -> dict[str, int]:
        return {
            "schools_targeted": 0,
            "schools_matched": 0,
            "deleted_conflicts": 0,
            "deleted_data_points": 0,
        }

    monkeypatch.setattr(live_eval, "_execute_deepsearch_task", fake_execute)
    monkeypatch.setattr(live_eval, "_run_pass_judge", fake_judge_pass)
    monkeypatch.setattr(live_eval, "_run_eval_judge_summary", fake_judge_summary)
    monkeypatch.setattr(live_eval, "_collect_token_usage", fake_usage)
    monkeypatch.setattr(live_eval, "_audit_db_duplicate_ratio", fake_audit)
    monkeypatch.setattr(live_eval, "_compute_cohort_db_hit_ratio", fake_compute_cohort_ratio)
    monkeypatch.setattr(live_eval, "_cold_reset_out_group_data", fake_cold_reset)

    report = await live_eval.run_deepsearch_live_eval(
        student_id="00000000-0000-0000-0000-000000000001",
        dataset_path=dataset_path,
        second_pass=True,
        output_dir=tmp_path / "bench",
    )

    assert report.metrics["db_hit_uplift"] == pytest.approx(0.37, abs=1e-4)
    assert report.metrics["external_call_reduction"] > 0.0
    assert report.metrics["persist_insert_reduction"] > 0.0
    assert report.metrics["tokens_actual"] == 29600
    assert report.metrics["tokens_actual_search"] == 25000
    assert report.metrics["tokens_actual_judge"] == 4600
    assert report.metrics["judge_overall_score"] == 86.0
    assert report.metrics["judge_score_uplift_pass2_vs_pass1"] == 13.0
    assert report.metrics["cohort_db_hit_ratio_pass1"][live_eval.COHORT_OUT_DB] == 0.0
    assert report.metrics["cohort_db_hit_ratio_pass2"][live_eval.COHORT_OUT_DB] == 0.45
    assert report.metrics["cohort_db_hit_uplift"][live_eval.COHORT_OUT_DB] == 0.45
    assert report.metrics["cold_reset_deleted_rows"] == 0

    run_dir = Path(report.config["output_dir"])
    assert (run_dir / "report.json").exists()
    assert (run_dir / "summary.md").exists()
    assert (run_dir / "judge_pass1.json").exists()
    assert (run_dir / "judge_pass2.json").exists()
    assert (run_dir / "judge_summary.json").exists()
    assert (tmp_path / "bench" / "history.csv").exists()


@pytest.mark.asyncio
async def test_run_deepsearch_live_eval_judge_failure_is_partial(monkeypatch, tmp_path: Path) -> None:
    dataset = {
        "dataset_id": "tiny",
        "version": "1.0",
        "required_fields_default": ["acceptance_rate"],
        "rules_default": {
            "acceptance_rate": {"kind": "numeric_range", "min": 0, "max": 100},
        },
        "schools": [
            {"school_name": "MIT", "aliases": ["Massachusetts Institute of Technology"]},
        ],
    }
    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(json.dumps(dataset), encoding="utf-8")

    async def fake_execute(**_: object):
        return (
            "local_fallback",
            {
                "search_metadata": {
                    "db_hit_ratio": 0.1,
                    "fallback_trigger_rate": 0.5,
                    "self_source_calls": 3,
                    "internal_websearch_calls": 1,
                    "persisted_data_points": 4,
                    "tokens_by_stage": {"total_estimated": 10000},
                    "raw_fact_count_before_merge": 8,
                    "unique_fact_count_after_merge": 5,
                    "dedupe_drop_count": 3,
                    "multi_source_agreement_count": 1,
                    "multi_source_conflict_count": 1,
                },
                "schools": [],
                "errors": [],
            },
            [],
        )

    async def fake_usage(
        eval_run_id: str,
        caller_prefixes: tuple[str, ...] | None = None,
    ) -> dict:
        prefixes = caller_prefixes or ()
        if eval_run_id.endswith("-p1") and prefixes == ("search.",):
            return {"calls": 2, "errors": 0, "tokens": 5000, "p95_latency_ms": 900}
        if eval_run_id.endswith("-p1") and prefixes == ("eval.deepsearch.judge.",):
            return {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0}
        if eval_run_id.endswith("-p1"):
            return {"calls": 2, "errors": 0, "tokens": 5000, "p95_latency_ms": 900}
        return {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0}

    async def fake_audit(**_: object) -> dict:
        return {
            "status": "ok",
            "duplicate_ratio": 0.0,
            "total_rows": 10,
            "unique_rows": 10,
            "duplicate_rows": 0,
        }

    async def broken_judge(**_: object) -> dict:
        raise RuntimeError("judge failed")

    async def fake_cohort_ratio(**_: object) -> dict[str, float]:
        return {}

    monkeypatch.setattr(live_eval, "_execute_deepsearch_task", fake_execute)
    monkeypatch.setattr(live_eval, "_collect_token_usage", fake_usage)
    monkeypatch.setattr(live_eval, "_audit_db_duplicate_ratio", fake_audit)
    monkeypatch.setattr(live_eval, "_run_pass_judge", broken_judge)
    monkeypatch.setattr(live_eval, "_compute_cohort_db_hit_ratio", fake_cohort_ratio)

    report = await live_eval.run_deepsearch_live_eval(
        student_id="00000000-0000-0000-0000-000000000001",
        dataset_path=dataset_path,
        second_pass=False,
        output_dir=tmp_path / "bench",
    )

    assert report.pass1.status == "partial"
    assert report.pass1.judge_calls == 0
    assert report.metrics["tokens_actual_search"] == 5000
    assert report.metrics["tokens_actual_judge"] == 0


@pytest.mark.asyncio
async def test_run_deepsearch_live_eval_no_judge_keeps_compat(monkeypatch, tmp_path: Path) -> None:
    dataset = {
        "dataset_id": "tiny",
        "version": "1.0",
        "required_fields_default": ["acceptance_rate"],
        "rules_default": {
            "acceptance_rate": {"kind": "numeric_range", "min": 0, "max": 100},
        },
        "schools": [
            {"school_name": "MIT", "aliases": ["Massachusetts Institute of Technology"]},
        ],
    }
    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(json.dumps(dataset), encoding="utf-8")

    async def fake_execute(**_: object):
        return (
            "local_fallback",
            {
                "search_metadata": {
                    "db_hit_ratio": 0.25,
                    "fallback_trigger_rate": 0.2,
                    "self_source_calls": 1,
                    "internal_websearch_calls": 0,
                    "persisted_data_points": 2,
                    "tokens_by_stage": {"total_estimated": 5000},
                    "raw_fact_count_before_merge": 5,
                    "unique_fact_count_after_merge": 4,
                    "dedupe_drop_count": 1,
                    "multi_source_agreement_count": 1,
                    "multi_source_conflict_count": 0,
                },
                "schools": [
                    {
                        "name": "Massachusetts Institute of Technology",
                        "aliases": ["MIT"],
                        "data": {"acceptance_rate": {"value": 5.0}},
                    },
                ],
                "errors": [],
            },
            [],
        )

    async def fake_usage(
        eval_run_id: str,
        caller_prefixes: tuple[str, ...] | None = None,
    ) -> dict:
        prefixes = caller_prefixes or ()
        if eval_run_id.endswith("-p1") and prefixes == ("search.",):
            return {"calls": 2, "errors": 0, "tokens": 4000, "p95_latency_ms": 800}
        if eval_run_id.endswith("-p1") and prefixes == ("eval.deepsearch.judge.",):
            return {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0}
        if eval_run_id.endswith("-p1"):
            return {"calls": 2, "errors": 0, "tokens": 4000, "p95_latency_ms": 800}
        return {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0}

    async def fake_audit(**_: object) -> dict:
        return {
            "status": "ok",
            "duplicate_ratio": 0.0,
            "total_rows": 10,
            "unique_rows": 10,
            "duplicate_rows": 0,
        }

    async def should_not_be_called(**_: object) -> dict:
        raise AssertionError("judge should not run when disabled")

    async def fake_cohort_ratio(**_: object) -> dict[str, float]:
        return {}

    monkeypatch.setattr(live_eval, "_execute_deepsearch_task", fake_execute)
    monkeypatch.setattr(live_eval, "_collect_token_usage", fake_usage)
    monkeypatch.setattr(live_eval, "_audit_db_duplicate_ratio", fake_audit)
    monkeypatch.setattr(live_eval, "_run_pass_judge", should_not_be_called)
    monkeypatch.setattr(live_eval, "_run_eval_judge_summary", should_not_be_called)
    monkeypatch.setattr(live_eval, "_compute_cohort_db_hit_ratio", fake_cohort_ratio)

    report = await live_eval.run_deepsearch_live_eval(
        student_id="00000000-0000-0000-0000-000000000001",
        dataset_path=dataset_path,
        second_pass=False,
        judge_enabled=False,
        output_dir=tmp_path / "bench",
    )

    assert report.pass1.judge_calls == 0
    assert report.metrics["tokens_actual"] == 4000
    assert report.metrics["tokens_actual_search"] == 4000
    assert report.metrics["tokens_actual_judge"] == 0


@pytest.mark.asyncio
async def test_run_deepsearch_live_eval_uses_pass1_when_pass2_failed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    dataset = {
        "dataset_id": "tiny",
        "version": "1.0",
        "required_fields_default": ["acceptance_rate"],
        "rules_default": {
            "acceptance_rate": {"kind": "numeric_range", "min": 0, "max": 100},
        },
        "schools": [
            {"school_name": "MIT", "aliases": ["Massachusetts Institute of Technology"]},
        ],
    }
    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(json.dumps(dataset), encoding="utf-8")

    async def fake_execute(**kwargs: object):
        eval_run_id = str(kwargs["eval_run_id"])
        if eval_run_id.endswith("-p1"):
            return (
                "local_fallback",
                {
                    "search_metadata": {
                        "db_hit_ratio": 0.35,
                        "fallback_trigger_rate": 0.2,
                        "self_source_calls": 2,
                        "internal_websearch_calls": 0,
                        "persisted_data_points": 3,
                        "tokens_by_stage": {"total_estimated": 5000},
                        "raw_fact_count_before_merge": 6,
                        "unique_fact_count_after_merge": 5,
                        "dedupe_drop_count": 1,
                        "multi_source_agreement_count": 1,
                        "multi_source_conflict_count": 0,
                    },
                    "schools": [
                        {
                            "name": "Massachusetts Institute of Technology",
                            "aliases": ["MIT"],
                            "data": {"acceptance_rate": {"value": 4.0}},
                        },
                    ],
                    "errors": [],
                },
                [],
            )
        return (
            "failed",
            {
                "search_metadata": {
                    "db_hit_ratio": 0.0,
                    "fallback_trigger_rate": 0.0,
                    "self_source_calls": 0,
                    "internal_websearch_calls": 0,
                    "persisted_data_points": 0,
                    "tokens_by_stage": {"total_estimated": 0},
                    "raw_fact_count_before_merge": 0,
                    "unique_fact_count_after_merge": 0,
                    "dedupe_drop_count": 0,
                    "multi_source_agreement_count": 0,
                    "multi_source_conflict_count": 0,
                },
                "schools": [],
                "errors": [{"stage": "local", "error": "pass2 failed"}],
            },
            [{"stage": "local", "error": "pass2 failed"}],
        )

    async def fake_usage(
        eval_run_id: str,
        caller_prefixes: tuple[str, ...] | None = None,
    ) -> dict:
        prefixes = caller_prefixes or ()
        if eval_run_id.endswith("-p1") and prefixes == ("search.",):
            return {"calls": 2, "errors": 0, "tokens": 4000, "p95_latency_ms": 800}
        if eval_run_id.endswith("-p2") and prefixes == ("search.",):
            return {"calls": 1, "errors": 1, "tokens": 500, "p95_latency_ms": 1200}
        if eval_run_id.endswith("-p1"):
            return {"calls": 2, "errors": 0, "tokens": 4000, "p95_latency_ms": 800}
        if eval_run_id.endswith("-p2"):
            return {"calls": 1, "errors": 1, "tokens": 500, "p95_latency_ms": 1200}
        return {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0}

    async def fake_audit(**_: object) -> dict:
        return {
            "status": "ok",
            "duplicate_ratio": 0.0,
            "total_rows": 10,
            "unique_rows": 10,
            "duplicate_rows": 0,
        }

    async def fake_cohort_ratio(**_: object) -> dict[str, float]:
        return {}

    monkeypatch.setattr(live_eval, "_execute_deepsearch_task", fake_execute)
    monkeypatch.setattr(live_eval, "_collect_token_usage", fake_usage)
    monkeypatch.setattr(live_eval, "_audit_db_duplicate_ratio", fake_audit)
    monkeypatch.setattr(live_eval, "_compute_cohort_db_hit_ratio", fake_cohort_ratio)

    report = await live_eval.run_deepsearch_live_eval(
        student_id="00000000-0000-0000-0000-000000000001",
        dataset_path=dataset_path,
        second_pass=True,
        judge_enabled=False,
        output_dir=tmp_path / "bench",
    )

    assert report.pass2 is not None
    assert report.pass2.status == "failed"
    assert report.metrics["second_pass_effective"] is False
    assert report.metrics["second_pass_status"] == "failed"
    assert report.metrics["required_slot_recall"] == pytest.approx(1.0, abs=1e-4)
    assert report.metrics["critical_slot_recall"] == pytest.approx(1.0, abs=1e-4)
    assert report.metrics["db_hit_uplift"] == pytest.approx(0.0, abs=1e-4)
    assert any("Pass2 failed" in item for item in report.recommendations)


@pytest.mark.asyncio
async def test_run_deepsearch_live_eval_cohort_validation_fail_fast(
    monkeypatch,
    tmp_path: Path,
) -> None:
    dataset = {
        "dataset_id": "coldmix-mini",
        "version": "1.0.0",
        "required_fields_default": ["acceptance_rate", "city"],
        "rules_default": {
            "acceptance_rate": {"kind": "numeric_range", "min": 0, "max": 100},
            "city": {"kind": "non_empty_text"},
        },
        "schools": [
            {"school_name": "Massachusetts Institute of Technology", "cohort": "in_db"},
            {"school_name": "Brown University", "cohort": "out_db"},
        ],
    }
    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(json.dumps(dataset), encoding="utf-8")

    async def fake_cold_reset(**_: object) -> dict[str, int]:
        return {
            "schools_targeted": 1,
            "schools_matched": 1,
            "deleted_conflicts": 5,
            "deleted_data_points": 10,
        }

    async def fake_cohort_ratio(**_: object) -> dict[str, float]:
        return {
            live_eval.COHORT_IN_DB: 0.65,
            live_eval.COHORT_OUT_DB: 0.05,
        }

    async def should_not_execute(**_: object) -> tuple[str, dict[str, object], list[dict[str, str]]]:
        raise AssertionError("pass should not start when cohort validation fails")

    monkeypatch.setattr(live_eval, "_cold_reset_out_group_data", fake_cold_reset)
    monkeypatch.setattr(live_eval, "_compute_cohort_db_hit_ratio", fake_cohort_ratio)
    monkeypatch.setattr(live_eval, "_execute_deepsearch_task", should_not_execute)

    report = await live_eval.run_deepsearch_live_eval(
        student_id="00000000-0000-0000-0000-000000000001",
        dataset_path=dataset_path,
        second_pass=True,
        judge_enabled=False,
        output_dir=tmp_path / "bench",
    )

    assert report.status == "failed"
    assert report.pass1.status == "failed"
    assert report.pass2 is None
    assert report.metrics["cohort_validation"]["ok"] is False
    assert report.metrics["cold_reset_deleted_rows"] == 10
    assert report.metrics["cohort_db_hit_ratio_pass1"][live_eval.COHORT_OUT_DB] == 0.05
    assert any("Cohort preflight validation failed" in item for item in report.recommendations)

    run_dir = Path(report.config["output_dir"])
    assert (run_dir / "pass1.json").exists()
    assert (run_dir / "report.json").exists()
    assert (run_dir / "summary.md").exists()
    assert (tmp_path / "bench" / "history.csv").exists()


@pytest.mark.asyncio
async def test_ensure_celery_queue_ready_detects_missing_queue(monkeypatch) -> None:
    import sys
    import types

    class _Inspect:
        def active_queues(self) -> dict[str, list[dict[str, str]]]:
            return {"worker@local": [{"name": "celery"}]}

    class _Control:
        def inspect(self, timeout: float = 1.0) -> _Inspect:
            return _Inspect()

    class _App:
        control = _Control()

    class _Task:
        app = _App()

    monkeypatch.setitem(
        sys.modules,
        "scholarpath.tasks",
        types.SimpleNamespace(run_deep_search=_Task()),
    )

    with pytest.raises(RuntimeError, match="deep_search"):
        await live_eval._ensure_celery_queue_ready(expected_queue="deep_search")
