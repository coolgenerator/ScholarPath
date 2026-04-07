from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from scholarpath.evals.recommendation_judge import (
    RECOMMENDATION_RUBRIC_DIMENSIONS,
    RecommendationABJudge,
)
from scholarpath.evals.recommendation_ux_live import (
    RecommendationUXCaseExecution,
    _align_ab_cases,
    _budget_hard_gate_passed,
    _scenario_shape_complete,
    load_recommendation_persona_dataset,
    run_recommendation_ux_gold_eval,
)


class _FailingLLM:
    async def complete_json(self, *args, **kwargs):
        raise RuntimeError("forced failure")


class _StubLLM:
    def set_caller_suffix(self, suffix: str):
        self.suffix = suffix
        return "token"

    def reset_caller_suffix(self, token):
        return None


def _candidate_case(*, case_id: str) -> RecommendationUXCaseExecution:
    payload = {
        "prefilter_meta": {
            "budget_cap_used": 10_000,
            "eligible_count": 2,
            "stretch_count": 1,
            "excluded_count": 0,
        },
        "schools": [
            {
                "school_name": "A",
                "prefilter_tag": "eligible",
                "net_price": 9_000,
            },
            {
                "school_name": "B",
                "prefilter_tag": "stretch",
                "net_price": 12_000,
            },
        ],
        "scenario_pack": {
            "baseline": [
                {
                    "rank": 1,
                    "baseline_rank": 1,
                    "rank_delta": 0,
                    "prefilter_tag": "eligible",
                    "scenario_reason": "x",
                    "outcome_breakdown": {},
                }
            ],
            "scenarios": [
                {"id": "budget_first", "schools": [{"rank": 1, "baseline_rank": 1, "rank_delta": 0, "prefilter_tag": "eligible", "scenario_reason": "x", "outcome_breakdown": {}}]},
                {"id": "risk_first", "schools": [{"rank": 1, "baseline_rank": 1, "rank_delta": 0, "prefilter_tag": "eligible", "scenario_reason": "x", "outcome_breakdown": {}}]},
                {"id": "major_first", "schools": [{"rank": 1, "baseline_rank": 1, "rank_delta": 0, "prefilter_tag": "eligible", "scenario_reason": "x", "outcome_breakdown": {}}]},
                {"id": "geo_first", "schools": [{"rank": 1, "baseline_rank": 1, "rank_delta": 0, "prefilter_tag": "eligible", "scenario_reason": "x", "outcome_breakdown": {}}]},
                {"id": "roi_first", "schools": [{"rank": 1, "baseline_rank": 1, "rank_delta": 0, "prefilter_tag": "eligible", "scenario_reason": "x", "outcome_breakdown": {}}]},
            ],
        },
    }
    return RecommendationUXCaseExecution(
        case_id=case_id,
        bucket="budget_first",
        tags=["x"],
        status="ok",
        turns_executed=1,
        duration_ms=100.0,
        final_content="ok",
        final_blocks=[],
        final_usage={"active_skill_id": "recommendation"},
        trace_summary={},
        hard_check_passed=True,
        hard_check_results=[],
        soft_check_mean=1.0,
        soft_check_results=[],
        recommendation_payload=payload,
        prefilter_meta=payload["prefilter_meta"],
        scenario_pack=payload["scenario_pack"],
        judge_payload={"case_id": case_id, "summary": "x"},
        error=None,
    )


def test_load_recommendation_persona_dataset_mini() -> None:
    ds = load_recommendation_persona_dataset("mini")
    assert ds.dataset_id == "recommendation_persona_gold_mini_v1"
    assert len(ds.cases) == 30
    assert len({case.case_id for case in ds.cases}) == 30
    assert ds.rubric_dimensions == list(RECOMMENDATION_RUBRIC_DIMENSIONS)


@pytest.mark.asyncio
async def test_recommendation_ab_judge_case_fallback_on_error() -> None:
    judge = RecommendationABJudge(llm=_FailingLLM())
    result = await judge.judge_case(
        run_id="r1",
        case_id="c1",
        baseline_payload={"x": 1},
        candidate_payload={"x": 2},
    )
    assert result.winner == "tie"
    assert result.error is not None
    assert result.reason_codes == ["judge_call_failed"]


def test_budget_hard_gate_and_scenario_shape_checks() -> None:
    case = _candidate_case(case_id="c1")
    assert _budget_hard_gate_passed(case.recommendation_payload) is True
    assert _scenario_shape_complete(case.recommendation_payload) is True


def test_align_ab_cases_requires_baseline_judge_payload() -> None:
    candidate = _candidate_case(case_id="c1")
    aligned, mismatches = _align_ab_cases(
        candidate_cases=[candidate],
        baseline_map={"c1": {"judge_payload": {"summary": "baseline"}}},
    )
    assert len(aligned) == 1
    assert mismatches == []

    aligned2, mismatches2 = _align_ab_cases(
        candidate_cases=[candidate],
        baseline_map={"c1": {"other": 1}},
    )
    assert aligned2 == []
    assert mismatches2[0]["reason"] == "baseline_missing_judge_payload"


def test_recommendation_ux_eval_baseline_no_judge_writes_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def _fake_execute_cases(*, llm, run_id, cases, concurrency):
        return [_candidate_case(case_id="rec_001")]

    async def _fake_collect_token_usage(*, suffix, caller_prefixes=None):
        return {
            "calls": 0,
            "errors": 0,
            "tokens": 0,
            "median_latency_ms": 0.0,
            "p90_latency_ms": 0.0,
            "p95_latency_ms": 0.0,
        }

    monkeypatch.setattr("scholarpath.evals.recommendation_ux_live.get_llm_client", lambda: _StubLLM())
    monkeypatch.setattr("scholarpath.evals.recommendation_ux_live._execute_cases", _fake_execute_cases)
    monkeypatch.setattr("scholarpath.evals.recommendation_ux_live._collect_token_usage", _fake_collect_token_usage)

    report = asyncio.run(
        run_recommendation_ux_gold_eval(
            dataset="mini",
            judge_enabled=False,
            output_dir=tmp_path,
            candidate_run_id="recommendation-ux-test",
        )
    )
    assert report.status == "ok"
    assert report.metrics["scoring"]["hard_check_pass_rate"] == 1.0
    assert report.metrics["scoring"]["recommendation_route_hit_rate"] == 1.0
    assert report.metrics["scoring"]["recommendation_payload_exists_rate"] == 1.0
    assert (tmp_path / "recommendation-ux-test" / "report.json").exists()
    assert (tmp_path / "recommendation-ux-test" / "case_results.jsonl").exists()
