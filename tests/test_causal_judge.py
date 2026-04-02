from __future__ import annotations

import pytest

from scholarpath.evals.causal_judge import CausalGoldJudge, build_eval_llm_client


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
            },
        )
        if self._raises:
            raise RuntimeError("llm failure")
        if self._responses:
            return self._responses.pop(0)
        return {}


def _case_payload() -> dict:
    return {
        "case_id": "cg-001",
        "cohort": "in_db",
        "context": "causal_gold_eval_v1",
        "label_type": "true",
        "estimate_confidence": 0.8,
        "fallback_used": False,
        "fallback_reason": None,
        "fields": [
            {
                "outcome_name": "admission_probability",
                "predicted_value": 0.7,
                "gold_value": 0.65,
                "abs_error": 0.05,
                "tolerance": 0.08,
                "within_tolerance": True,
            },
            {
                "outcome_name": "career_outcome",
                "predicted_value": 0.6,
                "gold_value": 0.8,
                "abs_error": 0.2,
                "tolerance": 0.1,
                "within_tolerance": False,
            },
        ],
    }


def test_build_eval_llm_client_rpm_guard() -> None:
    with pytest.raises(ValueError, match="<= 200"):
        build_eval_llm_client(max_rpm_total=201)
    with pytest.raises(ValueError, match="> 0"):
        build_eval_llm_client(max_rpm_total=0)


@pytest.mark.asyncio
async def test_evaluate_pass_tolerates_non_structured_response() -> None:
    llm = _FakeLLM(responses=[{"message": "not_structured"}])
    judge = CausalGoldJudge(llm=llm, concurrency=1)

    result = await judge.evaluate_pass(
        pass_name="legacy",
        eval_run_id="run-legacy-judge",
        case_payloads=[_case_payload()],
        pass_metadata={"mae_overall": 0.1},
    )

    assert result.status == "ok"
    assert result.case_count == 1
    assert result.case_results[0].case_id == "cg-001"
    assert len(result.case_results[0].field_judgements) == 2
    assert llm.calls[0]["caller"] == "eval.causal.judge.case"


@pytest.mark.asyncio
async def test_evaluate_pass_on_llm_error_returns_partial() -> None:
    llm = _FakeLLM(raises=True)
    judge = CausalGoldJudge(llm=llm, concurrency=1)

    result = await judge.evaluate_pass(
        pass_name="legacy",
        eval_run_id="run-legacy-judge",
        case_payloads=[_case_payload()],
        pass_metadata={},
    )

    assert result.status == "partial"
    assert result.errors
    assert result.errors[0]["stage"] == "judge_case"
    assert result.case_results[0].error is not None
    assert llm.calls[0]["caller"] == "eval.causal.judge.case"


@pytest.mark.asyncio
async def test_evaluate_run_fallback_on_llm_failure() -> None:
    llm = _FakeLLM(raises=True)
    judge = CausalGoldJudge(llm=llm, concurrency=1)

    summary = await judge.evaluate_run(
        run_id="run-1",
        eval_run_id="run-1-summary",
        legacy_summary={"avg_case_score": 70.0},
        pywhy_summary={"avg_case_score": 81.0},
        aggregate_metrics={"mae_overall_legacy": 0.12, "mae_overall_pywhy": 0.1},
    )

    assert summary.status in {"good", "watch", "bad"}
    assert summary.overall_score == pytest.approx(81.0, abs=1e-3)
    assert summary.score_uplift == pytest.approx(11.0, abs=1e-3)
    assert summary.error is not None
    assert llm.calls[0]["caller"] == "eval.causal.judge.run"
