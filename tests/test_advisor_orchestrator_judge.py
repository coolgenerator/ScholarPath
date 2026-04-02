from __future__ import annotations

import pytest

from scholarpath.evals.advisor_orchestrator_judge import (
    AdvisorOrchestratorJudge,
    build_eval_llm_client,
)


class _FakeLLM:
    def __init__(self, responses: list[object] | None = None, raises: bool = False) -> None:
        self._responses = list(responses or [])
        self._raises = raises
        self.calls: list[dict[str, object]] = []

    def set_caller_suffix(self, _: str | None):
        return object()

    def reset_caller_suffix(self, _) -> None:
        return None

    async def complete_json(
        self,
        messages,
        *,
        temperature: float = 0.1,
        max_tokens: int = 1200,
        caller: str = "unknown",
    ):
        self.calls.append(
            {
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "caller": caller,
            }
        )
        if self._raises:
            raise RuntimeError("llm failure")
        if self._responses:
            return self._responses.pop(0)
        return {}


def _case_payload() -> dict:
    return {
        "case_id": "ao-001",
        "case_type": "orchestrator",
        "deterministic_checks": {
            "primary_hit": True,
            "clarify_correct": True,
            "max_execution_ok": True,
            "pending_reason_ok": True,
            "recoverable_ok": True,
        },
        "response": {
            "capability": "undergrad.school.recommend",
            "done": [{"capability": "undergrad.school.recommend", "status": "succeeded"}],
            "pending": [],
            "next_actions": [],
        },
    }


def _reedit_case_payload() -> dict:
    return {
        "case_id": "ar-001",
        "case_type": "reedit",
        "deterministic_checks": {
            "overwrite_success": True,
            "truncation_correct": True,
            "history_consistent": True,
            "contract_ok": True,
        },
        "response": {
            "capability": "undergrad.school.recommend",
            "done": [{"capability": "undergrad.school.recommend", "status": "succeeded"}],
            "pending": [],
            "next_actions": [],
        },
    }


def test_build_eval_llm_client_rpm_guard() -> None:
    with pytest.raises(ValueError, match="<= 200"):
        build_eval_llm_client(max_rpm_total=201)
    with pytest.raises(ValueError, match="> 0"):
        build_eval_llm_client(max_rpm_total=0)


@pytest.mark.asyncio
async def test_evaluate_cases_tolerates_non_structured_response() -> None:
    llm = _FakeLLM(responses=[{"message": "not structured"}])
    judge = AdvisorOrchestratorJudge(llm=llm, concurrency=1)

    result = await judge.evaluate_cases(
        pass_name="advisor_orchestrator",
        eval_run_id="run-judge-cases",
        case_payloads=[_case_payload()],
        run_metadata={"dataset_id": "advisor_orchestrator_gold_v1"},
    )

    assert result.status == "ok"
    assert result.case_count == 1
    assert result.avg_case_score >= 0.0
    assert result.case_results[0].case_id == "ao-001"
    assert result.case_results[0].dimension_scores
    assert llm.calls[0]["caller"] == "eval.advisor.orchestrator.judge.case"


@pytest.mark.asyncio
async def test_reedit_case_includes_timeline_integrity_dimension() -> None:
    llm = _FakeLLM(
        responses=[
            {
                "case_score": 92,
                "dimension_scores": {
                    "route_correctness": 90,
                    "coordination_discipline": 91,
                    "clarify_safety": 89,
                    "recoverability": 90,
                    "timeline_integrity": 94,
                },
                "strengths": ["timeline rebuilt"],
                "risks": [],
                "recommendation": "keep monitor",
            }
        ]
    )
    judge = AdvisorOrchestratorJudge(llm=llm, concurrency=1)

    result = await judge.evaluate_cases(
        pass_name="advisor_orchestrator_merged",
        eval_run_id="run-reedit-case",
        case_payloads=[_reedit_case_payload()],
        run_metadata={"dataset_id": "advisor_reedit_gold_v1"},
    )

    assert result.case_count == 1
    dims = result.case_results[0].dimension_scores
    assert "timeline_integrity" in dims
    assert dims["timeline_integrity"] == pytest.approx(94.0, abs=1e-6)
    assert llm.calls[0]["caller"] == "eval.advisor.orchestrator.judge.case"


@pytest.mark.asyncio
async def test_evaluate_run_fallback_on_llm_failure() -> None:
    llm = _FakeLLM(raises=True)
    judge = AdvisorOrchestratorJudge(llm=llm, concurrency=1)

    summary = await judge.evaluate_run(
        run_id="run-1",
        eval_run_id="run-1-summary",
        pass_summary={"avg_case_score": 82.0},
        aggregate_metrics={"deterministic_overall_score": 76.0},
    )

    assert summary.status in {"good", "watch", "bad"}
    assert summary.overall_score == pytest.approx(82.0, abs=1e-3)
    assert summary.score_uplift == pytest.approx(6.0, abs=1e-3)
    assert summary.error is not None
    assert llm.calls[0]["caller"] == "eval.advisor.orchestrator.judge.run"
