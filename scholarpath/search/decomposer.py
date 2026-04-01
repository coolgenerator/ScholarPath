"""Query decomposition for Open DeepSearch."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from scholarpath.llm import LLMClient

logger = logging.getLogger(__name__)

_DECOMPOSE_SYSTEM_PROMPT = """\
You are a query decomposition engine for college admissions research.
Given a student profile and a research goal, break the goal down into
concrete sub-queries that can be sent to different data sources.

Available source types:
- college_scorecard: Official US government data (acceptance rate, SAT, cost, earnings)
- niche: School grades and student reviews
- web_search: General web search for supplementary data
- ugc: User-generated content (admission experiences, campus reviews)

Return a JSON object with key "sub_queries" containing an array. Each element:
{
  "query": "<search query string>",
  "target_sources": ["<source_name>", ...],
  "priority": <1-5, 1=highest>,
  "expected_fields": ["<variable_name>", ...]
}

Order by priority. Focus on data most relevant to the student's profile.
"""


@dataclass
class SubQuery:
    """A decomposed sub-query targeting specific sources."""

    query: str
    target_sources: list[str]
    priority: int = 3
    expected_fields: list[str] = field(default_factory=list)


class QueryDecomposer:
    """Decomposes a research goal into source-specific sub-queries using LLM."""

    def __init__(self, llm: LLMClient) -> None:
        self._llm = llm

    async def decompose(
        self,
        student_profile: dict,
        research_goal: str,
    ) -> list[SubQuery]:
        """Break *research_goal* into actionable sub-queries."""
        messages = [
            {"role": "system", "content": _DECOMPOSE_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Student profile:\n{json.dumps(student_profile, ensure_ascii=False, indent=2)}"
                    f"\n\nResearch goal: {research_goal}"
                ),
            },
        ]

        try:
            result = await self._llm.complete_json(messages, temperature=0.2)
        except Exception:
            logger.exception("LLM query decomposition failed; using fallback")
            return self._fallback_decompose(research_goal)

        raw_queries = result.get("sub_queries", [])
        sub_queries: list[SubQuery] = []
        for item in raw_queries:
            if not isinstance(item, dict) or "query" not in item:
                continue
            sub_queries.append(
                SubQuery(
                    query=item["query"],
                    target_sources=item.get("target_sources", ["college_scorecard"]),
                    priority=item.get("priority", 3),
                    expected_fields=item.get("expected_fields", []),
                )
            )
        sub_queries.sort(key=lambda sq: sq.priority)
        return sub_queries

    @staticmethod
    def _fallback_decompose(research_goal: str) -> list[SubQuery]:
        """Deterministic fallback when the LLM is unavailable."""
        return [
            SubQuery(
                query=research_goal,
                target_sources=["college_scorecard"],
                priority=1,
                expected_fields=["acceptance_rate", "sat_math_mid", "sat_reading_mid",
                                 "tuition_out_of_state", "median_earnings_10yr"],
            ),
            SubQuery(
                query=research_goal,
                target_sources=["niche"],
                priority=2,
                expected_fields=["overall_grade", "academics_grade"],
            ),
            SubQuery(
                query=research_goal,
                target_sources=["web_search"],
                priority=3,
                expected_fields=[],
            ),
            SubQuery(
                query=research_goal,
                target_sources=["ugc"],
                priority=4,
                expected_fields=["admission_experience", "campus_life_review"],
            ),
        ]
