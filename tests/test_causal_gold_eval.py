from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scholarpath.evals import causal_gold_live as live_eval

pytestmark = [
    pytest.mark.filterwarnings("error::RuntimeWarning"),
    pytest.mark.filterwarnings("error::ResourceWarning"),
]


def _base_case(
    *,
    case_id: str,
    cohort: str = "in_db",
    label_type: str = "true",
    include_all_outcomes: bool = True,
    include_selectivity: bool = True,
) -> dict:
    school_features = {
        "school_acceptance_rate": 0.2,
        "school_grad_rate": 0.9,
        "school_net_price_norm": 0.45,
        "school_endowment_norm": 0.7,
        "school_student_faculty_norm": 0.8,
        "school_location_tier": 0.7,
        "school_intl_pct_norm": 0.2,
    }
    if include_selectivity:
        school_features["school_selectivity"] = 0.8

    outcomes = {
        "admission_probability": 0.7,
        "academic_outcome": 0.8,
        "career_outcome": 0.75,
        "life_satisfaction": 0.72,
        "phd_probability": 0.62,
    }
    tolerances = {
        "admission_probability": 0.08,
        "academic_outcome": 0.11,
        "career_outcome": 0.11,
        "life_satisfaction": 0.12,
        "phd_probability": 0.12,
    }
    if not include_all_outcomes:
        outcomes = {"admission_probability": 0.7}
        tolerances = {"admission_probability": 0.08}

    return {
        "case_id": case_id,
        "cohort": cohort,
        "context": "causal_gold_eval_v1",
        "student_features": {
            "student_gpa_norm": 0.8,
            "student_sat_norm": 0.75,
            "student_budget_norm": 0.55,
            "student_act_norm": 0.65,
            "student_need_aid": 0.0,
            "student_profile_completed": 1.0,
        },
        "school_features": school_features,
        "offer_features": {
            "affordability_gap_norm": 0.2,
            "affordability_ratio_norm": 0.45,
            "academic_match": 0.78,
            "has_offer_signal": 1.0,
        },
        "gold_outcomes": outcomes,
        "gold_tolerance": tolerances,
        "label_type": label_type,
        "intervention_checks": [
            {
                "variable_name": "school_selectivity",
                "outcome_name": "admission_probability",
                "expected_direction": "increase",
                "delta": 0.05,
                "min_effect": 0.0,
            }
        ],
    }


def _write_dataset(path: Path, *, cases: list[dict]) -> None:
    payload = {"dataset_id": "tmp", "version": "1.0", "cases": cases}
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _make_pass_case(
    *,
    case_id: str,
    cohort: str,
    pred_admission: float,
    gold_admission: float,
    pred_career: float,
    gold_career: float,
    fallback_used: bool,
) -> live_eval.CausalGoldPassCase:
    abs_ad = round(abs(pred_admission - gold_admission), 6)
    abs_car = round(abs(pred_career - gold_career), 6)
    return live_eval.CausalGoldPassCase(
        case_id=case_id,
        cohort=cohort,
        context="ctx",
        predicted_outcomes={
            "admission_probability": pred_admission,
            "career_outcome": pred_career,
        },
        gold_outcomes={
            "admission_probability": gold_admission,
            "career_outcome": gold_career,
        },
        tolerance_by_outcome={
            "admission_probability": 0.1,
            "career_outcome": 0.15,
        },
        abs_errors={
            "admission_probability": abs_ad,
            "career_outcome": abs_car,
        },
        within_tolerance={
            "admission_probability": abs_ad <= 0.1,
            "career_outcome": abs_car <= 0.15,
        },
        estimate_confidence=0.8,
        label_type="true",
        label_confidence=0.8,
        fallback_used=fallback_used,
        fallback_reason=None,
        intervention_checks_total=1,
        intervention_checks_passed=1 if not fallback_used else 0,
        errors=[],
    )


def test_load_default_causal_dataset_schema() -> None:
    dataset = live_eval.load_causal_gold_dataset(live_eval.DEFAULT_CAUSAL_GOLD_DATASET_PATH)
    assert dataset.dataset_id == "causal_gold_v1"
    assert len(dataset.cases) == 40
    in_db = [c for c in dataset.cases if c.cohort == "in_db"]
    out_db = [c for c in dataset.cases if c.cohort == "out_db"]
    assert len(in_db) == 20
    assert len(out_db) == 20


def test_select_eval_cases_balanced_fixed_20() -> None:
    dataset = live_eval.load_causal_gold_dataset(live_eval.DEFAULT_CAUSAL_GOLD_DATASET_PATH)
    selected = live_eval._select_eval_cases(
        dataset.cases,
        sample_size=20,
        sample_strategy="balanced_fixed",
        case_ids=None,
    )
    assert len(selected) == 20
    assert sum(1 for case in selected if case.cohort == "in_db") == 10
    assert sum(1 for case in selected if case.cohort == "out_db") == 10
    assert selected[0].case_id == "cg-001"
    assert selected[-1].case_id == "cg-030"


def test_load_dataset_rejects_missing_school_selectivity(tmp_path: Path) -> None:
    cases = [
        _base_case(
            case_id=f"c{i:03d}",
            include_selectivity=False,
        )
        for i in range(40)
    ]
    path = tmp_path / "invalid_selectivity.json"
    _write_dataset(path, cases=cases)

    with pytest.raises(ValueError, match="school_features.school_selectivity"):
        live_eval.load_causal_gold_dataset(path)


def test_load_dataset_rejects_outcome_imbalance(tmp_path: Path) -> None:
    cases = [
        _base_case(
            case_id=f"c{i:03d}",
            include_all_outcomes=False,
        )
        for i in range(40)
    ]
    path = tmp_path / "invalid_outcome_balance.json"
    _write_dataset(path, cases=cases)

    with pytest.raises(ValueError, match="must appear in >=8"):
        live_eval.load_causal_gold_dataset(path)


def test_compute_pass_metrics_formulas() -> None:
    cases = [
        _make_pass_case(
            case_id="c1",
            cohort="in_db",
            pred_admission=0.8,
            gold_admission=0.6,
            pred_career=0.7,
            gold_career=0.6,
            fallback_used=False,
        ),
        _make_pass_case(
            case_id="c2",
            cohort="out_db",
            pred_admission=0.4,
            gold_admission=0.4,
            pred_career=0.5,
            gold_career=0.7,
            fallback_used=True,
        ),
    ]

    metrics = live_eval._compute_pass_metrics(
        cases,
        warning_mode="count_silent",
        pass_name="legacy",
    )
    assert metrics["mae_overall"] == pytest.approx(0.125, abs=1e-6)
    assert metrics["brier_admission"] == pytest.approx(0.02, abs=1e-6)
    assert metrics["fallback_rate"] == pytest.approx(0.5, abs=1e-6)
    assert metrics["intervention_direction_pass_rate"] == pytest.approx(0.5, abs=1e-6)


class _FakeSession:
    async def commit(self) -> None:
        return None


class _SessionFactory:
    def __call__(self) -> "_SessionFactory":
        return self

    async def __aenter__(self) -> _FakeSession:
        return _FakeSession()

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


@pytest.mark.asyncio
async def test_run_causal_gold_eval_writes_artifacts_and_history(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(live_eval, "async_session_factory", _SessionFactory())

    async def fake_has_active_pywhy_model(session, **kwargs) -> bool:
        return True

    monkeypatch.setattr(live_eval, "_has_active_pywhy_model", fake_has_active_pywhy_model)

    async def fake_run_engine_pass(**kwargs):
        run_id = str(kwargs["run_id"])
        pass_name = str(kwargs["pass_name"])
        eval_run_id = f"{run_id}-{pass_name}"
        cases = [
            _make_pass_case(
                case_id=f"{pass_name}-1",
                cohort="in_db",
                pred_admission=0.7 if pass_name == "legacy" else 0.72,
                gold_admission=0.68,
                pred_career=0.65 if pass_name == "legacy" else 0.69,
                gold_career=0.67,
                fallback_used=False,
            ),
            _make_pass_case(
                case_id=f"{pass_name}-2",
                cohort="out_db",
                pred_admission=0.56 if pass_name == "legacy" else 0.6,
                gold_admission=0.58,
                pred_career=0.6 if pass_name == "legacy" else 0.64,
                gold_career=0.62,
                fallback_used=False,
            ),
        ]
        metrics = live_eval._compute_pass_metrics(
            cases,
            warning_mode="count_silent",
            pass_name=pass_name,
        )
        now = datetime.now(timezone.utc).isoformat()
        return live_eval.CausalGoldPassReport(
            pass_name=pass_name,
            eval_run_id=eval_run_id,
            status="ok",
            started_at=now,
            ended_at=now,
            elapsed_seconds=0.1,
            case_count=len(cases),
            mae_by_outcome=metrics["mae_by_outcome"],
            mae_overall=metrics["mae_overall"],
            brier_admission=metrics["brier_admission"],
            ece_admission=metrics["ece_admission"],
            spearman_by_group=metrics["spearman_by_group"],
            intervention_direction_pass_rate=metrics["intervention_direction_pass_rate"],
            fallback_rate=metrics["fallback_rate"],
            avg_estimate_confidence=metrics["avg_estimate_confidence"],
            engine_case_concurrency=int(kwargs.get("case_concurrency", 1) or 1),
            engine_case_p95_ms=12.0,
            label_type_counts=metrics["label_type_counts"],
            warnings_total=int(metrics.get("warnings_total", 0) or 0),
            warnings_by_stage=metrics.get("warnings_by_stage", {}),
            errors=[],
            cases=cases,
        )

    monkeypatch.setattr(live_eval, "_run_engine_pass", fake_run_engine_pass)

    async def fake_collect_token_usage(*, eval_run_id: str, caller_prefixes=None):
        if eval_run_id.endswith("legacy-judge"):
            return {"calls": 2, "errors": 0, "tokens": 1200, "p95_latency_ms": 410.0, "rate_limit_errors": 0, "rpm_actual": 90.0}
        if eval_run_id.endswith("pywhy-judge"):
            return {"calls": 2, "errors": 0, "tokens": 1300, "p95_latency_ms": 430.0, "rate_limit_errors": 0, "rpm_actual": 91.0}
        if eval_run_id.endswith("judge-summary"):
            return {"calls": 1, "errors": 0, "tokens": 500, "p95_latency_ms": 350.0, "rate_limit_errors": 0, "rpm_actual": 88.0}
        return {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0.0, "rate_limit_errors": 0, "rpm_actual": 0.0}

    monkeypatch.setattr(live_eval, "_collect_token_usage", fake_collect_token_usage)

    class _JudgeResult:
        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def to_dict(self) -> dict:
            return dict(self._payload)

    class _FakeJudge:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        async def evaluate_pass(self, *, pass_name: str, eval_run_id: str, case_payloads, pass_metadata):
            return _JudgeResult(
                {
                    "pass_name": pass_name,
                    "eval_run_id": eval_run_id,
                    "status": "ok",
                    "case_results": [],
                    "case_count": len(case_payloads),
                    "avg_case_score": 75.0 if pass_name == "legacy" else 82.0,
                    "field_pass_rate": 0.8 if pass_name == "legacy" else 0.86,
                    "errors": [],
                },
            )

        async def evaluate_run(
            self,
            *,
            run_id: str,
            eval_run_id: str,
            legacy_summary: dict,
            pywhy_summary: dict | None,
            aggregate_metrics: dict,
        ):
            return _JudgeResult(
                {
                    "run_id": run_id,
                    "eval_run_id": eval_run_id,
                    "status": "good",
                    "overall_score": 84.0,
                    "score_uplift": 7.0,
                    "highlights": ["pywhy better on average"],
                    "risks": [],
                    "recommendations": ["continue shadow monitoring"],
                    "error": None,
                },
            )

    monkeypatch.setattr(live_eval, "CausalGoldJudge", _FakeJudge)

    report = await live_eval.run_causal_gold_eval(
        dataset_path=live_eval.DEFAULT_CAUSAL_GOLD_DATASET_PATH,
        output_dir=tmp_path,
        judge_enabled=True,
        judge_concurrency=2,
        judge_temperature=0.1,
        judge_max_tokens=1200,
        max_rpm_total=180,
        engine_case_concurrency=4,
        warning_mode="count_silent",
    )

    run_dir = Path(report.config["output_dir"])
    assert run_dir.exists()
    assert (run_dir / "legacy_pass.json").exists()
    assert (run_dir / "pywhy_pass.json").exists()
    assert (run_dir / "judge_cases_legacy.json").exists()
    assert (run_dir / "judge_cases_pywhy.json").exists()
    assert (run_dir / "judge_summary.json").exists()
    assert (run_dir / "report.json").exists()
    assert (run_dir / "summary.md").exists()

    history = tmp_path / "history.csv"
    assert history.exists()
    rows = list(csv.DictReader(history.read_text(encoding="utf-8").splitlines()))
    assert len(rows) == 1
    assert rows[0]["run_id"] == report.run_id
    assert report.metrics["judge_overall_score"] == pytest.approx(84.0, abs=1e-6)
    assert report.metrics["tokens_actual_judge"] == 3000
    assert report.metrics["sampled_case_count"] == 40
    assert report.metrics["rpm_actual_avg"] <= 180
    assert report.metrics["engine_case_concurrency"] == 4
    assert "warnings_total" in report.metrics
    assert "warnings_by_stage" in report.metrics


@pytest.mark.asyncio
async def test_run_causal_gold_eval_judge_failure_marks_partial(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(live_eval, "async_session_factory", _SessionFactory())

    async def fake_has_active_pywhy_model(session, **kwargs) -> bool:
        return True

    monkeypatch.setattr(live_eval, "_has_active_pywhy_model", fake_has_active_pywhy_model)

    async def fake_run_engine_pass(**kwargs):
        run_id = str(kwargs["run_id"])
        pass_name = str(kwargs["pass_name"])
        now = datetime.now(timezone.utc).isoformat()
        case = _make_pass_case(
            case_id=f"{pass_name}-1",
            cohort="in_db",
            pred_admission=0.7,
            gold_admission=0.68,
            pred_career=0.65,
            gold_career=0.67,
            fallback_used=False,
        )
        metrics = live_eval._compute_pass_metrics(
            [case],
            warning_mode="count_silent",
            pass_name=pass_name,
        )
        return live_eval.CausalGoldPassReport(
            pass_name=pass_name,
            eval_run_id=f"{run_id}-{pass_name}",
            status="ok",
            started_at=now,
            ended_at=now,
            elapsed_seconds=0.1,
            case_count=1,
            mae_by_outcome=metrics["mae_by_outcome"],
            mae_overall=metrics["mae_overall"],
            brier_admission=metrics["brier_admission"],
            ece_admission=metrics["ece_admission"],
            spearman_by_group=metrics["spearman_by_group"],
            intervention_direction_pass_rate=metrics["intervention_direction_pass_rate"],
            fallback_rate=metrics["fallback_rate"],
            avg_estimate_confidence=metrics["avg_estimate_confidence"],
            engine_case_concurrency=int(kwargs.get("case_concurrency", 1) or 1),
            engine_case_p95_ms=8.0,
            label_type_counts=metrics["label_type_counts"],
            warnings_total=int(metrics.get("warnings_total", 0) or 0),
            warnings_by_stage=metrics.get("warnings_by_stage", {}),
            errors=[],
            cases=[case],
        )

    monkeypatch.setattr(live_eval, "_run_engine_pass", fake_run_engine_pass)

    async def fake_collect_token_usage(*, eval_run_id: str, caller_prefixes=None):
        return {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0.0, "rate_limit_errors": 0, "rpm_actual": 0.0}

    monkeypatch.setattr(live_eval, "_collect_token_usage", fake_collect_token_usage)

    class _BrokenJudge:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        async def evaluate_pass(self, **kwargs):
            raise RuntimeError("judge unavailable")

        async def evaluate_run(self, **kwargs):
            raise RuntimeError("judge unavailable")

    monkeypatch.setattr(live_eval, "CausalGoldJudge", _BrokenJudge)

    report = await live_eval.run_causal_gold_eval(
        dataset_path=live_eval.DEFAULT_CAUSAL_GOLD_DATASET_PATH,
        output_dir=tmp_path,
        judge_enabled=True,
        max_rpm_total=180,
    )

    assert report.status == "partial"
    assert report.legacy_pass.status == "partial"
    assert report.pywhy_pass is not None
    assert report.pywhy_pass.status == "partial"
    assert "error" in report.judge_summary


@pytest.mark.asyncio
async def test_run_causal_gold_eval_enforces_rpm_limit(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="<= 200"):
        await live_eval.run_causal_gold_eval(
            dataset_path=live_eval.DEFAULT_CAUSAL_GOLD_DATASET_PATH,
            output_dir=tmp_path,
            judge_enabled=False,
            max_rpm_total=201,
        )


@pytest.mark.asyncio
async def test_run_causal_gold_eval_enforces_warning_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="warning_mode"):
        await live_eval.run_causal_gold_eval(
            dataset_path=live_eval.DEFAULT_CAUSAL_GOLD_DATASET_PATH,
            output_dir=tmp_path,
            judge_enabled=False,
            warning_mode="invalid_mode",
        )


@pytest.mark.asyncio
async def test_run_causal_gold_eval_balanced_sample_size_20(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(live_eval, "async_session_factory", _SessionFactory())

    async def fake_has_active_pywhy_model(session, **kwargs) -> bool:
        return True

    monkeypatch.setattr(live_eval, "_has_active_pywhy_model", fake_has_active_pywhy_model)

    async def fake_run_engine_pass(**kwargs):
        run_id = str(kwargs["run_id"])
        pass_name = str(kwargs["pass_name"])
        dataset = kwargs["dataset"]
        now = datetime.now(timezone.utc).isoformat()

        cases = [
            _make_pass_case(
                case_id=case.case_id,
                cohort=case.cohort,
                pred_admission=0.7,
                gold_admission=0.68,
                pred_career=0.65,
                gold_career=0.67,
                fallback_used=False,
            )
            for case in dataset.cases
        ]
        metrics = live_eval._compute_pass_metrics(
            cases,
            warning_mode="count_silent",
            pass_name=pass_name,
        )
        return live_eval.CausalGoldPassReport(
            pass_name=pass_name,
            eval_run_id=f"{run_id}-{pass_name}",
            status="ok",
            started_at=now,
            ended_at=now,
            elapsed_seconds=0.2,
            case_count=len(cases),
            mae_by_outcome=metrics["mae_by_outcome"],
            mae_overall=metrics["mae_overall"],
            brier_admission=metrics["brier_admission"],
            ece_admission=metrics["ece_admission"],
            spearman_by_group=metrics["spearman_by_group"],
            intervention_direction_pass_rate=metrics["intervention_direction_pass_rate"],
            fallback_rate=metrics["fallback_rate"],
            avg_estimate_confidence=metrics["avg_estimate_confidence"],
            engine_case_concurrency=int(kwargs.get("case_concurrency", 1) or 1),
            engine_case_p95_ms=15.0,
            label_type_counts=metrics["label_type_counts"],
            warnings_total=int(metrics.get("warnings_total", 0) or 0),
            warnings_by_stage=metrics.get("warnings_by_stage", {}),
            errors=[],
            cases=cases,
        )

    monkeypatch.setattr(live_eval, "_run_engine_pass", fake_run_engine_pass)

    async def fake_collect_token_usage(*, eval_run_id: str, caller_prefixes=None):
        return {
            "calls": 0,
            "errors": 0,
            "tokens": 0,
            "p95_latency_ms": 0.0,
            "rate_limit_errors": 0,
            "rpm_actual": 0.0,
        }

    monkeypatch.setattr(live_eval, "_collect_token_usage", fake_collect_token_usage)

    report = await live_eval.run_causal_gold_eval(
        dataset_path=live_eval.DEFAULT_CAUSAL_GOLD_DATASET_PATH,
        output_dir=tmp_path,
        judge_enabled=False,
        sample_size=20,
        sample_strategy="balanced_fixed",
    )

    sampled_ids = report.config["sampled_case_ids"]
    assert report.legacy_pass.case_count == 20
    assert report.pywhy_pass is not None
    assert report.pywhy_pass.case_count == 20
    assert report.metrics["sampled_case_count"] == 20
    assert len(sampled_ids) == 20
    assert sampled_ids[0] == "cg-001"
    assert sampled_ids[-1] == "cg-030"
