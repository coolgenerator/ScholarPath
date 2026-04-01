"""Prompt template for generating ED/EA/RD application strategy."""

from __future__ import annotations

import json
from typing import Any

SYSTEM_PROMPT = """\
You are a strategic college-admissions advisor.  Given a student profile and \
a tiered school list (reach / match / safety), produce a concrete application \
strategy.

Your output must include:
1. **ED recommendation** -- which school (if any) to apply Early Decision and why.
2. **EA list** -- schools to apply Early Action, with brief rationale.
3. **RD list** -- remaining Regular Decision targets.
4. **Risk warnings** -- any portfolio-level risks (e.g. too top-heavy, \
   missing financial safeties, geographic concentration).

Rules:
1. Output ONLY valid JSON.
2. Respect the student's budget and preferences.
3. Respond in the same language as the input profile.
4. Be specific -- reference actual school names, not generic advice.

Output schema:
{
  "ed_recommendation": {
    "school": "<name or null>",
    "rationale": "<text>"
  },
  "ea_list": [
    {"school": "<name>", "rationale": "<text>"},
    ...
  ],
  "rd_list": [
    {"school": "<name>", "rationale": "<text>"},
    ...
  ],
  "risk_warnings": ["<warning>", ...],
  "overall_strategy_note": "<brief summary>"
}
"""


def format_user_prompt(
    student_profile: dict[str, Any],
    tiered_schools: dict[str, list[dict[str, Any]]],
) -> str:
    """Build the user message for strategy advice.

    Parameters
    ----------
    student_profile:
        Structured student profile dict.
    tiered_schools:
        Dict with keys "reach", "match", "safety", each containing a list of
        school dicts (at minimum ``{"name": ..., "composite_score": ...}``).
    """
    return (
        "Student profile:\n"
        f"```json\n{json.dumps(student_profile, ensure_ascii=False, indent=2)}\n```\n\n"
        "Tiered school list:\n"
        f"```json\n{json.dumps(tiered_schools, ensure_ascii=False, indent=2)}\n```"
    )
