from __future__ import annotations

from pathlib import Path

import pytest

from scholarpath.evals.advisor_orchestrator_live import (
    load_advisor_reedit_dataset,
    load_advisor_orchestrator_dataset,
    run_advisor_orchestrator_eval,
    _write_json,
)

pytestmark = [
    pytest.mark.filterwarnings("error::RuntimeWarning"),
    pytest.mark.filterwarnings("error::ResourceWarning"),
]


class _StubRouterLLM:
    def set_caller_suffix(self, _: str | None):
        return object()

    def reset_caller_suffix(self, _) -> None:
        return None

    async def complete_json(self, messages, *, temperature=0.1, max_tokens=512, caller="unknown"):
        if caller == "advisor.router.plan":
            user_text = str(messages[-1]["content"]).lower()
            if "offer" in user_text:
                return {
                    "domain": "offer",
                    "domain_confidence": 0.95,
                    "intent_clarity": 0.9,
                    "candidates": [
                        {
                            "capability": "offer.compare",
                            "confidence": 0.92,
                            "conflict_group": "offer_decision",
                        }
                    ],
                }
            return {
                "domain": "undergrad",
                "domain_confidence": 0.95,
                "intent_clarity": 0.9,
                "candidates": [
                    {
                        "capability": "undergrad.school.recommend",
                        "confidence": 0.92,
                        "conflict_group": "undergrad_recommend",
                    }
                ],
            }
        if caller == "advisor.router.domain":
            user_text = str(messages[-1]["content"]).lower()
            if "offer" in user_text:
                return {"domain": "offer", "confidence": 0.95}
            return {"domain": "undergrad", "confidence": 0.95}
        if caller == "advisor.router.multi_intent":
            system_text = str(messages[0]["content"]).lower()
            if "domain: offer" in system_text:
                return {
                    "candidates": [
                        {
                            "capability": "offer.compare",
                            "confidence": 0.92,
                            "conflict_group": "offer_decision",
                        }
                    ]
                }
            return {
                "candidates": [
                    {
                        "capability": "undergrad.school.recommend",
                        "confidence": 0.92,
                        "conflict_group": "undergrad_recommend",
                    }
                ]
            }
        return {}


def test_load_default_dataset_schema_and_coverage() -> None:
    dataset = load_advisor_orchestrator_dataset()
    assert dataset.dataset_id == "advisor_orchestrator_gold_v1"
    assert len(dataset.cases) == 40
    assert dataset.thresholds["primary_hit_rate"] == 0.9
    assert dataset.thresholds["deepsearch_expectation_rate"] == 0.9
    assert dataset.thresholds["deepsearch_pair_uplift_pass_rate"] == 1.0
    assert sum(1 for case in dataset.cases if "cat_single_intent" in case.tags) == 12
    assert sum(1 for case in dataset.cases if "cat_multi_over_limit" in case.tags) == 10
    assert sum(1 for case in dataset.cases if "lang_zh" in case.tags) == 28
    assert sum(1 for case in dataset.cases if "lang_en" in case.tags) == 12

    reedit = load_advisor_reedit_dataset()
    assert reedit.dataset_id == "advisor_reedit_gold_v1"
    assert len(reedit.cases) == 12
    assert reedit.thresholds["reedit_overwrite_success_rate"] == 0.95


def test_load_v2_dataset_schema_and_coverage() -> None:
    base = Path("scholarpath/evals/datasets")
    dataset = load_advisor_orchestrator_dataset(base / "advisor_orchestrator_gold_v2.json")
    assert dataset.dataset_id == "advisor_orchestrator_gold_v2"
    assert len(dataset.cases) == 80

    reedit = load_advisor_reedit_dataset(base / "advisor_reedit_gold_v2.json")
    assert reedit.dataset_id == "advisor_reedit_gold_v2"
    assert len(reedit.cases) == 24


@pytest.mark.asyncio
async def test_run_eval_writes_artifacts_and_metrics(tmp_path: Path) -> None:
    report = await run_advisor_orchestrator_eval(
        output_dir=tmp_path,
        judge_enabled=False,
        sample_size=2,
        execution_lane="stub",
        usage_enabled=False,
        llm=_StubRouterLLM(),  # type: ignore[arg-type]
    )

    run_dir = tmp_path / report.run_id
    assert report.orchestrator_metrics["case_count"] == 2
    assert report.reedit_metrics["case_count"] == 12
    assert report.metrics["case_count"] == 14
    assert report.metrics["orchestrator_case_count"] == 2
    assert report.metrics["reedit_case_count"] == 12
    assert "primary_hit_rate" in report.metrics
    assert "clarify_precision" in report.metrics
    assert "clarify_recall" in report.metrics
    assert "task_count_total" in report.metrics
    assert "task_latency_p90_ms" in report.metrics
    assert "llm_tokens_per_task" in report.metrics
    assert "non_causal_p90_ms" in report.metrics
    assert "non_causal_p95_ms" in report.metrics
    assert "non_causal_task_p90_ms" in report.metrics
    assert "non_causal_task_p95_ms" in report.metrics
    assert "reedit_overwrite_success_rate" in report.metrics
    assert "reedit_truncation_correct_rate" in report.metrics
    assert "reedit_history_consistency_rate" in report.metrics
    assert "complex_output_polish_calls" in report.metrics
    assert "complex_output_polish_errors" in report.metrics
    assert "complex_output_render_pass_rate" in report.metrics
    assert "warning_counts_by_stage" in report.metrics
    assert isinstance(report.stub_metrics, dict)
    assert isinstance(report.real_metrics, dict)
    assert isinstance(report.gate_by_lane, dict)
    assert isinstance(report.warning_counts_by_stage, dict)
    assert (run_dir / "report.json").exists()
    assert (run_dir / "summary.md").exists()
    assert (run_dir / "cases.jsonl").exists()
    assert (run_dir / "reedit_cases.jsonl").exists()
    assert (run_dir / "merged_summary.json").exists()
    assert (run_dir / "judge_cases.json").exists()
    assert (run_dir / "judge_cases_reedit.json").exists()
    assert (run_dir / "judge_summary.json").exists()


@pytest.mark.asyncio
async def test_run_eval_accepts_v2_larger_sample(tmp_path: Path) -> None:
    base = Path("scholarpath/evals/datasets")
    report = await run_advisor_orchestrator_eval(
        output_dir=tmp_path,
        judge_enabled=False,
        dataset_path=base / "advisor_orchestrator_gold_v2.json",
        reedit_dataset_path=base / "advisor_reedit_gold_v2.json",
        sample_size=50,
        reedit_sample_size=20,
        include_reedit=True,
        execution_lane="stub",
        usage_enabled=False,
        llm=_StubRouterLLM(),  # type: ignore[arg-type]
    )

    assert report.orchestrator_metrics["case_count"] == 50
    assert report.reedit_metrics["case_count"] == 20
    assert report.metrics["case_count"] == 70


@pytest.mark.asyncio
async def test_memory_degraded_case_sets_signal_rate(tmp_path: Path) -> None:
    report = await run_advisor_orchestrator_eval(
        output_dir=tmp_path,
        judge_enabled=False,
        case_ids=["ao-039"],
        sample_size=1,
        include_reedit=False,
        execution_lane="stub",
        usage_enabled=False,
        llm=_StubRouterLLM(),  # type: ignore[arg-type]
    )

    assert report.metrics["case_count"] == 1
    assert report.metrics["memory_total"] == 1
    assert report.metrics["memory_degraded_signal_rate"] == pytest.approx(1.0, abs=1e-9)


@pytest.mark.asyncio
async def test_deepsearch_cold_warm_pair_metrics_are_tracked(tmp_path: Path) -> None:
    report = await run_advisor_orchestrator_eval(
        output_dir=tmp_path,
        judge_enabled=False,
        case_ids=["ao-002", "ao-008"],
        sample_size=2,
        include_reedit=False,
        execution_lane="stub",
        usage_enabled=False,
        llm=_StubRouterLLM(),  # type: ignore[arg-type]
    )

    assert report.metrics["deepsearch_expect_total"] == 2
    assert report.metrics["deepsearch_expectation_rate"] == pytest.approx(1.0, abs=1e-9)
    assert report.metrics["deepsearch_pair_total"] == 1
    assert report.metrics["deepsearch_pair_uplift_pass_rate"] == pytest.approx(1.0, abs=1e-9)
    assert report.metrics["deepsearch_pair_external_reduction_rate"] == pytest.approx(1.0, abs=1e-9)
    assert report.metrics["deepsearch_db_hit_uplift_avg"] > 0.0


@pytest.mark.asyncio
async def test_include_reedit_false_keeps_orchestrator_only_counts(tmp_path: Path) -> None:
    report = await run_advisor_orchestrator_eval(
        output_dir=tmp_path,
        judge_enabled=False,
        sample_size=3,
        include_reedit=False,
        execution_lane="stub",
        usage_enabled=False,
        llm=_StubRouterLLM(),  # type: ignore[arg-type]
    )
    assert report.orchestrator_metrics["case_count"] == 3
    assert report.reedit_metrics["case_count"] == 0
    assert report.metrics["case_count"] == 3


@pytest.mark.asyncio
async def test_mini_sampling_runs_10_plus_6_and_records_selected_ids(tmp_path: Path) -> None:
    report = await run_advisor_orchestrator_eval(
        output_dir=tmp_path,
        judge_enabled=False,
        sample_size=10,
        reedit_sample_size=6,
        include_reedit=True,
        execution_lane="stub",
        usage_enabled=False,
        llm=_StubRouterLLM(),  # type: ignore[arg-type]
    )

    assert report.orchestrator_metrics["case_count"] == 10
    assert report.reedit_metrics["case_count"] == 6
    assert report.metrics["case_count"] == 16
    assert report.config["selected_case_ids"] == [
        "ao-001",
        "ao-002",
        "ao-003",
        "ao-013",
        "ao-014",
        "ao-023",
        "ao-029",
        "ao-033",
        "ao-037",
        "ao-039",
    ]
    assert report.config["selected_reedit_case_ids"] == [
        "ar-001",
        "ar-003",
        "ar-005",
        "ar-007",
        "ar-008",
        "ar-010",
    ]
    assert report.config["reedit_sample_size"] == 6


@pytest.mark.asyncio
async def test_reedit_case_ids_override_sample_size_and_unknown_ids_raise(tmp_path: Path) -> None:
    report = await run_advisor_orchestrator_eval(
        output_dir=tmp_path,
        judge_enabled=False,
        sample_size=2,
        reedit_sample_size=6,
        reedit_case_ids=["ar-012", "ar-007"],
        include_reedit=True,
        execution_lane="stub",
        usage_enabled=False,
        llm=_StubRouterLLM(),  # type: ignore[arg-type]
    )
    assert report.reedit_metrics["case_count"] == 2
    assert report.config["selected_reedit_case_ids"] == ["ar-012", "ar-007"]

    with pytest.raises(ValueError, match="Unknown reedit_case_ids"):
        await run_advisor_orchestrator_eval(
            output_dir=tmp_path,
            judge_enabled=False,
            sample_size=1,
            reedit_case_ids=["ar-404"],
            include_reedit=True,
            execution_lane="stub",
            usage_enabled=False,
            llm=_StubRouterLLM(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_max_rpm_guard_validation(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="<= 200"):
        await run_advisor_orchestrator_eval(
            output_dir=tmp_path,
            judge_enabled=False,
            sample_size=1,
            max_rpm_total=201,
            execution_lane="stub",
            usage_enabled=False,
            llm=_StubRouterLLM(),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="> 0"):
        await run_advisor_orchestrator_eval(
            output_dir=tmp_path,
            judge_enabled=False,
            sample_size=1,
            max_rpm_total=0,
            execution_lane="stub",
            usage_enabled=False,
            llm=_StubRouterLLM(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_execution_lane_both_populates_stub_and_real_metrics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scholarpath.evals import advisor_orchestrator_live as live

    async def _fake_real_lane(**kwargs):  # type: ignore[no-untyped-def]
        report = live.AdvisorOrchestratorCaseReport(
            case_id="ao-real-001",
            tags=["cat_single_intent"],
            request={"message": "real lane"},
            response={
                "domain": "undergrad",
                "capability": "undergrad.school.recommend",
                "route_meta": {"executed_count": 1},
            },
            deterministic_checks={
                "primary_hit": True,
                "clarify_required": False,
                "clarify_correct": True,
                "execution_within_limit": True,
                "pending_reason_ok": True,
                "recoverability_ok": True,
                "contract_valid": True,
                "memory_degraded_signal": False,
                "deepsearch_expected_trigger": False,
                "deepsearch_trigger_observed": False,
                "deepsearch_expected_reuse": False,
                "deepsearch_reuse_observed": False,
                "deepsearch_expected_db_hit_range": None,
                "deepsearch_db_hit_range_observed": None,
                "deepsearch_expected_external_calls_max": None,
                "deepsearch_external_calls_observed": None,
                "deepsearch_pair_uplift_ok": True,
                "deepsearch_pair_external_reduction_ok": True,
                "error_contract_ok": True,
            },
            deterministic_score=1.0,
            final_score=1.0,
            errors=[],
        )
        return [report], [], []

    monkeypatch.setattr(live, "_run_orchestrator_cases_real", _fake_real_lane)

    report = await live.run_advisor_orchestrator_eval(
        output_dir=tmp_path,
        judge_enabled=False,
        include_reedit=False,
        sample_size=1,
        execution_lane="both",
        usage_enabled=False,
        llm=_StubRouterLLM(),  # type: ignore[arg-type]
    )
    assert report.metrics["execution_lane"] == "both"
    assert report.stub_metrics["case_count"] == 1
    assert report.real_metrics["case_count"] == 1
    assert "stub" in report.gate_by_lane
    assert "real" in report.gate_by_lane


def test_write_json_serializes_set_values(tmp_path: Path) -> None:
    output = tmp_path / "payload.json"
    _write_json(output, {"items": {"a", "b"}})
    text = output.read_text(encoding="utf-8")
    assert '"items"' in text
    assert "[" in text and "]" in text
