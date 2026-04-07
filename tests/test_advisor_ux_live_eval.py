from __future__ import annotations

from pathlib import Path

import pytest

from scholarpath.evals.advisor_ux_live import (
    AdvisorUXCaseExecution,
    AdvisorUXEvalReport,
    _build_by_skill_metrics,
    _build_judge_content,
    _build_low_score_case_study,
    _compute_degradation_intrusion_rate,
    _compute_mean_delta_by_dim,
    _compute_transparency_signal_density,
    _align_ab_cases,
    _is_generic_refusal_text,
    _merge_case_results_in_pair_order,
    _percentile,
    _resolve_unscored_buckets,
    _write_summary,
    load_advisor_ux_dataset,
)
from scholarpath.evals.advisor_ux_judge import AdvisorUXJudgeCaseResult, create_unscored_case_result


def _case(case_id: str) -> AdvisorUXCaseExecution:
    return AdvisorUXCaseExecution(
        case_id=case_id,
        bucket="test",
        tags=[],
        status="ok",
        turns_executed=2,
        duration_ms=100.0,
        final_content="ok content",
        final_blocks=[{"kind": "text", "capability_id": "general", "order": 0, "meta": {}}],
        final_usage={"wave_count": 1, "tool_steps_used": 1},
        trace_summary={"event_count": 3, "events": ["turn_started", "planning_done", "turn_completed"]},
        hard_check_passed=True,
        hard_check_results=[],
        soft_check_mean=1.0,
        soft_check_results=[],
        judge_payload={
            "status": "ok",
            "content": "hello",
            "block_kinds": ["text"],
            "execution_digest": {
                "what_done": "已完成本轮核心执行。",
                "why_next": "下一轮补齐预算上限可降低不确定性。",
                "needs_input": ["预算上限"],
            },
        },
        error=None,
        skill_id="default",
    )


def test_dataset_mini_full_counts_and_alignment():
    mini = load_advisor_ux_dataset("mini")
    full = load_advisor_ux_dataset("full")

    assert len(mini.cases) == 30
    assert len(full.cases) == 100
    assert [case.case_id for case in full.cases[:30]] == [case.case_id for case in mini.cases]


def test_dataset_low_score_smoke_loads():
    low = load_advisor_ux_dataset("low_score_smoke")
    assert low.dataset_id == "advisor_ux_low_score_smoke_v1"
    assert len(low.cases) >= 12


def test_align_ab_cases_marks_missing_and_missing_payload():
    candidates = [_case("ux_001"), _case("ux_002"), _case("ux_003")]
    candidates[0].bucket = "recommendation"
    baseline_map = {
        "ux_001": {"case_id": "ux_001", "judge_payload": {"status": "ok", "content": "b1"}},
        "ux_002": {"case_id": "ux_002"},
    }
    aligned, mismatches = _align_ab_cases(candidate_cases=candidates, baseline_map=baseline_map)

    assert [row["case_id"] for row in aligned] == ["ux_001"]
    assert aligned[0]["candidate_bucket"] == "recommendation"
    reason_by_case = {item["case_id"]: item["reason"] for item in mismatches}
    assert reason_by_case["ux_002"] == "baseline_missing_judge_payload"
    assert reason_by_case["ux_003"] == "baseline_missing_case"


def test_percentile_basic():
    values = [10, 20, 30, 40, 50]
    assert _percentile(values, 0.5) == 30.0
    assert _percentile(values, 0.9) == 50.0
    assert _percentile([], 0.95) == 0.0


def test_generic_refusal_detector():
    assert _is_generic_refusal_text("Please be more specific.")
    assert _is_generic_refusal_text("我没识别到具体字段，请更具体。")
    assert not _is_generic_refusal_text("结论：适合冲刺。下一步：补充活动细节并确认预算上限。")


def test_unscored_buckets_always_include_multi_intent():
    result = _resolve_unscored_buckets(["recommendation"])
    assert "recommendation" in result
    assert "multi_intent" in result


def test_merge_case_results_keeps_pair_order():
    aligned_pairs = [
        {"case_id": "ux_001"},
        {"case_id": "ux_002"},
    ]
    scored = [
        AdvisorUXJudgeCaseResult(
            case_id="ux_002",
            scoring_status="scored",
            unscored_reason=None,
            winner="candidate",
            candidate_scores={},
            baseline_scores={},
            candidate_mean=4.0,
            baseline_mean=3.0,
            mean_delta=1.0,
            confidence=0.9,
            reason_codes=[],
            notes="",
            error=None,
        )
    ]
    unscored = [create_unscored_case_result(case_id="ux_001", reason="unscored_bucket")]
    merged = _merge_case_results_in_pair_order(
        aligned_pairs=aligned_pairs,
        scored_results=scored,
        unscored_results=unscored,
    )
    assert [item.case_id for item in merged] == ["ux_001", "ux_002"]


def test_compute_mean_delta_by_dim_with_empty_input():
    deltas = _compute_mean_delta_by_dim([])
    assert set(deltas.keys()) == {
        "task_completion",
        "personalization_fit",
        "clarity_actionability",
        "interaction_fluency",
        "execution_chain_transparency",
        "robustness_consistency",
        "tone_experience",
    }
    assert all(value == 0.0 for value in deltas.values())


def test_write_summary_uses_new_metric_sections(tmp_path: Path):
    report = AdvisorUXEvalReport(
        run_id="r1",
        generated_at="2026-01-01T00:00:00Z",
        status="ok",
        config={"dataset_id": "advisor_ux_gold_mini_v1"},
        metrics={
            "scoring": {
                "scored_case_count": 10,
                "unscored_case_count": 5,
                "scoring_coverage_rate": 0.6667,
            },
            "scored_judge": {
                "candidate_win_rate": 0.7,
                "overall_user_feel_mean": 3.8,
                "mean_delta_by_dim": {},
            },
            "execution": {
                "candidate_case_count": 15,
                "aligned_case_count": 15,
                "mismatch_count": 0,
            },
            "experience_watch": {
                "generic_refusal_rate": 0.0,
                "transparency_low_score_rate": 0.2,
                "degradation_intrusion_rate": 0.0,
                "transparency_signal_density": 0.95,
            },
            "token_usage_by_stage": {
                "candidate": {"tokens": 100},
                "judge": {"tokens": 20},
                "total_tokens": 120,
            },
            "latency_ms_by_stage": {
                "candidate": {"median": 120, "p90": 250, "p95": 300},
                "judge": {"median": 80, "p90": 140, "p95": 180},
            },
        },
        judge_summary={
            "scored_case_count": 10,
            "unscored_case_count": 5,
            "overall_user_feel_mean": 3.8,
            "recommendations": ["x"],
        },
        mismatches=[],
        errors=[],
    )
    _write_summary(run_dir=tmp_path, report=report)
    text = (tmp_path / "summary.md").read_text(encoding="utf-8")
    assert "quality_mean" not in text
    assert "quality_score_100" not in text
    assert "b_win_rate" not in text
    assert "scored_case_count" in text
    assert "scored_candidate_win_rate" in text
    assert "degradation_intrusion_rate" in text
    assert "transparency_signal_density" in text


def test_compute_degradation_intrusion_rate_uses_judge_content():
    normal = _case("ux_normal")
    normal.judge_payload["content"] = "结论：继续推进。"
    noisy = _case("ux_noisy")
    noisy.judge_payload["content"] = "风险与缺失：降级原因：CAP_TIMEOUT"
    rate = _compute_degradation_intrusion_rate([normal, noisy])
    assert rate == pytest.approx(0.5)


def test_compute_transparency_signal_density_reads_execution_digest():
    complete = _case("ux_complete")
    incomplete = _case("ux_incomplete")
    incomplete.judge_payload["execution_digest"] = {
        "what_done": "",
        "why_next": "下一步补一个字段。",
        "needs_input": [],
    }
    density = _compute_transparency_signal_density([complete, incomplete])
    # complete contributes 3/3, incomplete contributes 1/3 -> 4/6
    assert density == pytest.approx(4 / 6)


def test_build_judge_content_prefers_answer_synthesis():
    synthesis_payload = {
        "summary": "结论：可执行",
        "conclusion": "先做匹配校清单。",
        "perspectives": [
            {"angle": "strategy", "claim": "先完成匹配档", "evidence": "预算与成绩匹配", "source_caps": ["strategy"], "confidence": 0.8}
        ],
        "actions": [{"step": "本周锁定学校清单", "rationale": "减少不确定性", "priority": "high"}],
        "risks_missing": ["缺少活动经历细节"],
        "degraded": {"has_degraded": False, "caps": [], "reason_codes": [], "retry_hint": ""},
    }
    text = _build_judge_content(
        synthesis_payload=synthesis_payload,
        fallback_content="fallback",
    )
    assert "结论：先做匹配校清单。" in text
    assert "行动：" in text
    assert "fallback" not in text


def test_low_score_case_study_groups_by_bucket_and_reason():
    cases = [_case("ux_001"), _case("ux_002")]
    cases[0].bucket = "what_if"
    cases[0].skill_id = "what_if"
    cases[0].degraded_caps = ["what_if"]
    cases[0].synthesis_present = True
    cases[0].primary_angle_covered = True
    cases[0].fallback_used = True
    cases[1].bucket = "strategy"
    cases[1].skill_id = "strategy"
    scored = [
        AdvisorUXJudgeCaseResult(
            case_id="ux_001",
            scoring_status="scored",
            unscored_reason=None,
            winner="baseline",
            candidate_scores={"execution_chain_transparency": 2.5},
            baseline_scores={"execution_chain_transparency": 3.5},
            candidate_mean=2.9,
            baseline_mean=3.2,
            mean_delta=-0.3,
            confidence=0.8,
            reason_codes=["low_transparency", "fallback_generic"],
            notes="",
            error=None,
        ),
        AdvisorUXJudgeCaseResult(
            case_id="ux_002",
            scoring_status="scored",
            unscored_reason=None,
            winner="candidate",
            candidate_scores={"execution_chain_transparency": 4.0},
            baseline_scores={"execution_chain_transparency": 3.0},
            candidate_mean=3.8,
            baseline_mean=3.0,
            mean_delta=0.8,
            confidence=0.9,
            reason_codes=[],
            notes="",
            error=None,
        ),
    ]
    report = _build_low_score_case_study(candidate_cases=cases, judge_case_results=scored)
    assert report["low_score_case_count"] == 1
    assert report["by_bucket"]["what_if"] == 1
    assert report["by_skill"]["what_if"] == 1
    assert report["by_reason_code"]["low_transparency"] == 1


def test_build_by_skill_metrics_groups_scored_cases():
    cases = [_case("ux_001"), _case("ux_002"), _case("ux_003")]
    cases[0].skill_id = "what_if"
    cases[1].skill_id = "what_if"
    cases[2].skill_id = "guided_intake"
    judged = [
        AdvisorUXJudgeCaseResult(
            case_id="ux_001",
            scoring_status="scored",
            unscored_reason=None,
            winner="candidate",
            candidate_scores={},
            baseline_scores={},
            candidate_mean=3.9,
            baseline_mean=3.2,
            mean_delta=0.7,
            confidence=0.9,
            reason_codes=[],
            notes="",
            error=None,
        ),
        AdvisorUXJudgeCaseResult(
            case_id="ux_002",
            scoring_status="scored",
            unscored_reason=None,
            winner="baseline",
            candidate_scores={},
            baseline_scores={},
            candidate_mean=2.8,
            baseline_mean=3.1,
            mean_delta=-0.3,
            confidence=0.7,
            reason_codes=["low_transparency"],
            notes="",
            error=None,
        ),
        create_unscored_case_result(case_id="ux_003", reason="unscored_bucket"),
    ]
    metrics = _build_by_skill_metrics(candidate_cases=cases, judge_case_results=judged)
    by_skill = {item.skill_id: item for item in metrics}
    assert by_skill["what_if"].case_count == 2
    assert by_skill["what_if"].scored_case_count == 2
    assert by_skill["what_if"].mean_score == pytest.approx(3.35)
    assert by_skill["what_if"].candidate_win_rate == pytest.approx(0.5)
    assert by_skill["what_if"].low_score_rate == pytest.approx(0.5)
