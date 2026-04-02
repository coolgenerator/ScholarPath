from __future__ import annotations

import pytest

from scholarpath.advisor.contracts import RecommendationData, RecommendedSchool
from scholarpath.advisor.output_polisher import AdvisorOutputPolisher


class _FakeLLM:
    def __init__(self, payload_by_caller: dict[str, object] | None = None, fail: bool = False) -> None:
        self._payload_by_caller = payload_by_caller or {}
        self._fail = fail

    async def complete_json(self, messages, *, temperature=0.1, max_tokens=512, caller="unknown"):  # noqa: ANN001, ANN002, ANN003
        if self._fail:
            raise RuntimeError("llm failed")
        payload = self._payload_by_caller.get(caller, {})
        if isinstance(payload, Exception):
            raise payload
        return payload


def _recommendation_data() -> RecommendationData:
    return RecommendationData(
        narrative="原始叙述",
        schools=[
            RecommendedSchool(
                school_name="MIT",
                tier="reach",
                overall_score=0.91,
                admission_probability=0.2,
                sub_scores={"academic": 0.95, "career": 0.82},
            )
        ],
        ed_recommendation="MIT",
        ea_recommendations=["CMU"],
        strategy_summary="原始策略摘要",
    )


@pytest.mark.asyncio
async def test_polish_recommendation_updates_text_only() -> None:
    llm = _FakeLLM(
        payload_by_caller={
            "advisor.style.recommendation": {
                "narrative": "润色后的叙述",
                "strategy_summary": "润色后的策略摘要",
            }
        }
    )
    polisher = AdvisorOutputPolisher(
        enabled=True,
        capabilities={"undergrad.school.recommend"},
        max_tokens=600,
        temperature=0.2,
    )
    source = _recommendation_data()

    polished = await polisher.polish_school_recommendation(llm=llm, data=source, locale="zh-CN")

    assert polished.narrative == "润色后的叙述"
    assert polished.strategy_summary == "润色后的策略摘要"
    assert polished.schools[0].school_name == "MIT"
    assert polished.schools[0].overall_score == pytest.approx(0.91)
    assert polished.schools[0].admission_probability == pytest.approx(0.2)


@pytest.mark.asyncio
async def test_polish_recommendation_fallback_on_error() -> None:
    llm = _FakeLLM(fail=True)
    polisher = AdvisorOutputPolisher(
        enabled=True,
        capabilities={"undergrad.school.recommend"},
        max_tokens=600,
        temperature=0.2,
    )
    source = _recommendation_data()

    polished = await polisher.polish_school_recommendation(llm=llm, data=source, locale="zh-CN")

    assert polished == source


@pytest.mark.asyncio
async def test_offer_and_what_if_fallback_when_payload_invalid() -> None:
    llm = _FakeLLM(
        payload_by_caller={
            "advisor.style.offer_compare": {},
            "advisor.style.offer_what_if": {"explanation": "x"},
        }
    )
    polisher = AdvisorOutputPolisher(
        enabled=True,
        capabilities={"offer.compare", "offer.what_if"},
        max_tokens=600,
        temperature=0.2,
    )

    recommendation = await polisher.polish_offer_recommendation(
        llm=llm,
        recommendation="Keep original recommendation",
        offers=[{"school": "A"}],
        locale="zh-CN",
    )
    explanation = await polisher.polish_what_if_explanation(
        llm=llm,
        explanation="Keep original explanation",
        interventions={"financial_aid": 0.9},
        deltas={"career_outcome": 0.04},
        locale="zh-CN",
    )

    assert recommendation == "Keep original recommendation"
    assert explanation == "Keep original explanation"
