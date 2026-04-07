from __future__ import annotations

import pytest

from scholarpath.evals.advisor_ux_judge import (
    AdvisorUXABJudge,
    AdvisorUXJudgeCaseResult,
    RUBRIC_DIMENSIONS,
    create_unscored_case_result,
)


class _FakeLLM:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def complete_json(self, messages, *, schema=None, temperature=0.1, max_tokens=1200, caller="unknown"):
        self.calls.append(caller)
        if ".case#" in caller:
            return {
                "winner_option": "option_1",
                "scores_option_1": {dim: 4.0 for dim in RUBRIC_DIMENSIONS},
                "scores_option_2": {dim: 3.0 for dim in RUBRIC_DIMENSIONS},
                "confidence": 0.82,
                "reason_codes": ["BETTER_STRUCTURE"],
                "notes": "option_1 is better",
            }
        return {
            "risks": ["trace consistency may regress"],
            "recommendations": ["improve final actionability bullets"],
        }


@pytest.mark.asyncio
async def test_judge_case_maps_anonymous_options_back_to_candidate_or_baseline():
    llm = _FakeLLM()
    judge = AdvisorUXABJudge(llm=llm)

    # Use helper shuffle to know expected winner mapping.
    _a, _b, a_is_candidate = judge._shuffle_pair(  # noqa: SLF001
        run_id="r1",
        case_id="ux_001",
        baseline_payload={"status": "ok"},
        candidate_payload={"status": "ok"},
    )
    result = await judge.judge_case(
        run_id="r1",
        case_id="ux_001",
        baseline_payload={"status": "ok"},
        candidate_payload={"status": "ok"},
    )
    assert isinstance(result, AdvisorUXJudgeCaseResult)
    assert len(result.candidate_scores) == 7
    assert len(result.baseline_scores) == 7
    if a_is_candidate:
        assert result.winner == "candidate"
    else:
        assert result.winner == "baseline"


@pytest.mark.asyncio
async def test_judge_run_returns_structured_summary():
    llm = _FakeLLM()
    judge = AdvisorUXABJudge(llm=llm)
    case_results = [
        AdvisorUXJudgeCaseResult(
            case_id="ux_001",
            scoring_status="scored",
            unscored_reason=None,
            winner="candidate",
            candidate_scores={dim: 4.0 for dim in RUBRIC_DIMENSIONS},
            baseline_scores={dim: 3.0 for dim in RUBRIC_DIMENSIONS},
            candidate_mean=4.0,
            baseline_mean=3.0,
            mean_delta=1.0,
            confidence=0.8,
            reason_codes=["BETTER_STRUCTURE"],
            notes="ok",
            error=None,
        ),
        create_unscored_case_result(
            case_id="ux_002",
            reason="unscored_bucket",
        ),
    ]
    summary = await judge.judge_run(run_id="r1", case_results=case_results, metrics={"x": 1})
    assert summary.status == "ok"
    assert summary.candidate_win_rate == 1.0
    assert summary.overall_user_feel_mean == 4.0
    assert summary.scored_case_count == 1
    assert summary.unscored_case_count == 1
    assert summary.recommendations == ["improve final actionability bullets"]
