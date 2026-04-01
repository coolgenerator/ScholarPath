from __future__ import annotations

import pytest

from scholarpath.evals.deepsearch_judge import DeepSearchLiveJudge


class _FakeLLM:
    def __init__(self, responses: list[object] | None = None, raises: bool = False) -> None:
        self._responses = list(responses or [])
        self._raises = raises
        self.calls: list[dict[str, object]] = []

    def set_caller_suffix(self, _: str | None):  # pragma: no cover - simple stub
        return object()

    def reset_caller_suffix(self, _) -> None:  # pragma: no cover - simple stub
        return None

    async def complete_json(self, messages, *, temperature=0.1, max_tokens=1200, caller="unknown"):
        self.calls.append(
            {
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "caller": caller,
            },
        )
        if self._raises:
            raise RuntimeError("llm error")
        if self._responses:
            return self._responses.pop(0)
        return {}


def _valid_school_case() -> dict:
    return {
        "school_name": "MIT",
        "aliases": ["Massachusetts Institute of Technology"],
        "required_fields": ["acceptance_rate", "city"],
        "rules": {
            "acceptance_rate": {"kind": "numeric_range", "min": 0, "max": 100},
            "city": {"kind": "non_empty_text"},
        },
    }


@pytest.mark.asyncio
async def test_evaluate_pass_tolerates_non_structured_response() -> None:
    llm = _FakeLLM(responses=[{"message": "not structured"}])
    judge = DeepSearchLiveJudge(llm=llm, concurrency=1)

    result = await judge.evaluate_pass(
        pass_name="pass1",
        eval_run_id="run-p1",
        school_cases=[_valid_school_case()],
        schools_payload=[
            {
                "name": "Massachusetts Institute of Technology",
                "aliases": ["MIT"],
                "data": {
                    "acceptance_rate": {"value": "4.8%", "source": "scorecard", "confidence": 0.9},
                },
            },
        ],
        pass_metadata={"db_hit_ratio": 0.2},
        required_fields_override=None,
    )

    assert result.status == "ok"
    assert result.school_count == 1
    school = result.school_results[0]
    assert school.school_name == "MIT"
    assert 0.0 <= school.school_score <= 100.0
    assert len(school.field_judgements) == 2
    assert {item["variable_name"] for item in school.field_judgements} == {
        "acceptance_rate",
        "city",
    }
    assert llm.calls[0]["caller"] == "eval.deepsearch.judge.school"


@pytest.mark.asyncio
async def test_evaluate_pass_handles_empty_fields_and_missing_rules() -> None:
    llm = _FakeLLM(responses=[{}])
    judge = DeepSearchLiveJudge(llm=llm, concurrency=1)

    result = await judge.evaluate_pass(
        pass_name="pass1",
        eval_run_id="run-p1",
        school_cases=[
            {
                "school_name": "Stanford University",
                "aliases": ["Stanford"],
                "required_fields": ["acceptance_rate"],
                "rules": {},
            },
        ],
        schools_payload=[],
        pass_metadata={},
        required_fields_override=None,
    )

    assert result.school_count == 1
    school = result.school_results[0]
    assert school.matched_school is None
    assert len(school.field_judgements) == 1
    assert school.field_judgements[0]["variable_name"] == "acceptance_rate"
    assert school.field_judgements[0]["pass"] is False


@pytest.mark.asyncio
async def test_evaluate_run_fallback_on_llm_failure() -> None:
    llm = _FakeLLM(raises=True)
    judge = DeepSearchLiveJudge(llm=llm, concurrency=1)

    summary = await judge.evaluate_run(
        run_id="run-1",
        eval_run_id="run-1-summary",
        pass1_summary={"avg_school_score": 70.0},
        pass2_summary={"avg_school_score": 82.0},
        aggregate_metrics={"db_hit_uplift": 0.3},
    )

    assert summary.status in {"good", "watch", "bad"}
    assert summary.overall_score == pytest.approx(82.0, abs=1e-3)
    assert summary.score_uplift == pytest.approx(12.0, abs=1e-3)
    assert summary.error is not None
    assert llm.calls[0]["caller"] == "eval.deepsearch.judge.run"
