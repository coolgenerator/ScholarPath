"""Advisor complex-output text polisher.

This module rewrites narrative text for selected capabilities while preserving
structured artifacts and numeric fields.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from scholarpath.advisor.contracts import RecommendationData
from scholarpath.config import settings
from scholarpath.llm.client import LLMClient
from scholarpath.observability import log_fallback

logger = logging.getLogger(__name__)

_CAP_RECOMMEND = "undergrad.school.recommend"
_CAP_OFFER_COMPARE = "offer.compare"
_CAP_OFFER_WHAT_IF = "offer.what_if"


def _prefer_chinese(locale: str | None) -> bool:
    if not locale:
        return True
    return locale.lower().startswith("zh")


def _non_empty_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if len(text) < 2:
        return None
    return text


@dataclass(slots=True)
class AdvisorOutputPolisher:
    """Text-only post-processor for rich advisor outputs."""

    enabled: bool = settings.ADVISOR_STYLE_POLISH_ENABLED
    capabilities: set[str] | None = None
    max_tokens: int = settings.ADVISOR_STYLE_POLISH_MAX_TOKENS
    temperature: float = settings.ADVISOR_STYLE_POLISH_TEMPERATURE

    def __post_init__(self) -> None:
        if self.capabilities is None:
            self.capabilities = set(settings.advisor_style_polish_capabilities)
        self.max_tokens = max(128, int(self.max_tokens))
        self.temperature = max(0.0, min(float(self.temperature), 1.0))

    def _supports(self, capability: str) -> bool:
        if not self.enabled:
            return False
        return capability in (self.capabilities or set())

    async def polish_school_recommendation(
        self,
        *,
        llm: LLMClient,
        data: RecommendationData,
        locale: str | None,
    ) -> RecommendationData:
        """Rewrite recommendation narrative/strategy text only."""
        if not self._supports(_CAP_RECOMMEND):
            return data
        if not data.narrative and not data.strategy_summary:
            return data

        language = "中文" if _prefer_chinese(locale) else "English"
        evidence = {
            "language": language,
            "narrative": data.narrative,
            "strategy_summary": data.strategy_summary,
            "ed_recommendation": data.ed_recommendation,
            "ea_recommendations": data.ea_recommendations,
            "schools": [
                {
                    "school_name": school.school_name,
                    "tier": school.tier,
                    "overall_score": school.overall_score,
                    "admission_probability": school.admission_probability,
                    "key_reasons": school.key_reasons[:2],
                }
                for school in data.schools[:12]
            ],
        }
        messages = [
            {
                "role": "system",
                "content": (
                    "You are ScholarPath output editor.\n"
                    "Rewrite text for readability only.\n"
                    "Hard rules:\n"
                    "1) Only rewrite `narrative` and `strategy_summary`.\n"
                    "2) Do NOT change facts, numbers, tiers, school names, or recommendations.\n"
                    "3) Keep concise: narrative 2 short paragraphs + optional bullets.\n"
                    "4) Output in requested language.\n"
                    "Return JSON only: {\"narrative\": string, \"strategy_summary\": string|null}."
                ),
            },
            {"role": "user", "content": json.dumps(evidence, ensure_ascii=False)},
        ]
        try:
            payload = await llm.complete_json(
                messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                caller="advisor.style.recommendation",
            )
        except Exception as exc:
            log_fallback(
                logger=logger,
                component="advisor.output_polisher",
                stage="recommendation",
                reason="llm_complete_json_failed",
                fallback_used=True,
                exc=exc,
            )
            return data

        if not isinstance(payload, dict):
            return data

        narrative = _non_empty_text(payload.get("narrative")) or data.narrative
        strategy_summary_raw = payload.get("strategy_summary")
        if strategy_summary_raw is None:
            strategy_summary = data.strategy_summary
        else:
            strategy_summary = _non_empty_text(strategy_summary_raw) or data.strategy_summary

        return data.model_copy(
            update={
                "narrative": narrative,
                "strategy_summary": strategy_summary,
            }
        )

    async def polish_offer_recommendation(
        self,
        *,
        llm: LLMClient,
        recommendation: str | None,
        offers: list[dict[str, Any]],
        locale: str | None,
    ) -> str | None:
        """Rewrite offer recommendation text only."""
        if not self._supports(_CAP_OFFER_COMPARE):
            return recommendation
        original = (recommendation or "").strip()
        if not original:
            return recommendation

        language = "中文" if _prefer_chinese(locale) else "English"
        evidence = {
            "language": language,
            "recommendation": original,
            "offers": [
                {
                    "school": str(row.get("school", "")),
                    "net_cost": row.get("net_cost"),
                    "total_aid": row.get("total_aid"),
                    "causal_scores": row.get("causal_scores", {}),
                    "decision_deadline": row.get("decision_deadline"),
                }
                for row in offers[:8]
                if isinstance(row, dict)
            ],
        }
        messages = [
            {
                "role": "system",
                "content": (
                    "You are ScholarPath output editor.\n"
                    "Rewrite the recommendation only, keeping all facts unchanged.\n"
                    "Use exactly this markdown structure:\n"
                    "### 结论\n"
                    "- ...\n"
                    "### 核心对比\n"
                    "- ...\n"
                    "### 下一步\n"
                    "- ...\n"
                    "Do NOT invent schools or numbers.\n"
                    "Return JSON only: {\"recommendation\": string}."
                ),
            },
            {"role": "user", "content": json.dumps(evidence, ensure_ascii=False)},
        ]
        try:
            payload = await llm.complete_json(
                messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                caller="advisor.style.offer_compare",
            )
        except Exception as exc:
            log_fallback(
                logger=logger,
                component="advisor.output_polisher",
                stage="offer_compare",
                reason="llm_complete_json_failed",
                fallback_used=True,
                exc=exc,
            )
            return recommendation

        if not isinstance(payload, dict):
            return recommendation
        polished = _non_empty_text(payload.get("recommendation"))
        return polished or recommendation

    async def polish_what_if_explanation(
        self,
        *,
        llm: LLMClient,
        explanation: str | None,
        interventions: dict[str, float],
        deltas: dict[str, float],
        locale: str | None,
    ) -> str | None:
        """Rewrite what-if explanation text only."""
        if not self._supports(_CAP_OFFER_WHAT_IF):
            return explanation
        original = (explanation or "").strip()
        if not original:
            return explanation

        language = "中文" if _prefer_chinese(locale) else "English"
        evidence = {
            "language": language,
            "explanation": original,
            "interventions": interventions,
            "deltas": deltas,
        }
        messages = [
            {
                "role": "system",
                "content": (
                    "You are ScholarPath output editor.\n"
                    "Rewrite explanation only, preserving all facts and numbers.\n"
                    "Use markdown with this structure:\n"
                    "### 变化概览\n"
                    "### 影响解释\n"
                    "### 下一步\n"
                    "Do NOT add external assumptions.\n"
                    "Return JSON only: {\"explanation\": string}."
                ),
            },
            {"role": "user", "content": json.dumps(evidence, ensure_ascii=False)},
        ]
        try:
            payload = await llm.complete_json(
                messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                caller="advisor.style.offer_what_if",
            )
        except Exception as exc:
            log_fallback(
                logger=logger,
                component="advisor.output_polisher",
                stage="offer_what_if",
                reason="llm_complete_json_failed",
                fallback_used=True,
                exc=exc,
            )
            return explanation

        if not isinstance(payload, dict):
            return explanation
        polished = _non_empty_text(payload.get("explanation"))
        return polished or explanation


_DEFAULT_POLISHER = AdvisorOutputPolisher()


def get_output_polisher() -> AdvisorOutputPolisher:
    """Return the shared output polisher instance."""
    return _DEFAULT_POLISHER
