"""Prompt template for DeepSearch query decomposition."""

from __future__ import annotations

import json
from typing import Any

SYSTEM_PROMPT = """\
You are a research planner for a college-admissions advisory system.

Given a student profile and a high-level research goal, decompose the goal \
into a prioritised list of concrete sub-queries.  Each sub-query should target \
one or more data sources.

Available sources:
- "scorecard"   -- US Dept of Education College Scorecard API
- "niche"       -- Niche.com school profiles (web scrape)
- "usnews"      -- US News rankings data
- "web"         -- General web search
- "internal"    -- ScholarPath's own database

Rules:
1. Output ONLY valid JSON -- no markdown, no commentary.
2. Order sub-queries by priority (1 = highest).
3. Keep each query string short and specific.
4. The user may describe the goal in Chinese or English; write queries in \
   English for maximum source coverage.

Output schema:
{
  "sub_queries": [
    {
      "query": "<search string>",
      "sources": ["<source_id>", ...],
      "priority": <int>
    },
    ...
  ]
}
"""


def format_user_prompt(
    profile: dict[str, Any],
    research_goal: str,
) -> str:
    """Build the user message for query decomposition.

    Parameters
    ----------
    profile:
        Student profile dict (or subset of relevant fields).
    research_goal:
        High-level research question / goal from the advisor or student.
    """
    return (
        f"Student profile:\n```json\n{json.dumps(profile, ensure_ascii=False, indent=2)}\n```\n\n"
        f"Research goal:\n{research_goal}"
    )
