from __future__ import annotations

import pytest

from scholarpath.evals.advisor_orchestrator_judge import AdvisorOrchestratorJudge


class _FakeLLM:
    async def complete_json(self, messages, temperature, max_tokens, caller):  # noqa: D401
        if ".case#" in caller:
            return {
                "case_score": 88,
                "route_correct": True,
                "output_quality": 0.9,
                "notes": "ok",
            }
        return {"overall_score": 86, "recommendations": ["tighten format"]}


@pytest.mark.asyncio
async def test_advisor_judge_case_and_run():
    judge = AdvisorOrchestratorJudge(llm=_FakeLLM())
    case = await judge.judge_case(
        run_id="r1",
        lane="stub",
        case_payload={"case_id": "c1", "contract_valid": True},
    )
    assert case.case_id == "c1"
    assert case.case_score == 88
    assert case.route_correct is True

    summary = await judge.judge_run(
        run_id="r1",
        lane="stub",
        case_results=[case],
        metrics={"contract_valid_rate": 1.0},
    )
    assert summary.status == "ok"
    assert summary.overall_score == 86
