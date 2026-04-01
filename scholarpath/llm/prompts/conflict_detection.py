"""Prompt template for analysing conflicting data points."""

from __future__ import annotations

import json
from typing import Any

SYSTEM_PROMPT = """\
You are a data-quality analyst for a college-admissions knowledge base.

Given a variable name and two data values from different sources, determine \
whether they genuinely conflict, assess severity, and recommend a resolution.

Severity levels:
- "low"      -- cosmetic or formatting difference (e.g. "25:1" vs "25 to 1").
- "medium"   -- meaningful difference but unlikely to change a decision.
- "high"     -- large discrepancy that could mislead a student.

Resolution strategies:
- "prefer_a"        -- Source A is more authoritative for this variable.
- "prefer_b"        -- Source B is more authoritative.
- "average"         -- Numeric values can be averaged.
- "keep_both"       -- Both values are valid in different contexts.
- "needs_review"    -- Cannot resolve automatically; flag for human review.

Rules:
1. Output ONLY valid JSON.
2. Be aware that values may be in different units, currencies, or languages.

Output schema:
{
  "is_conflict": <bool>,
  "severity": "<low | medium | high>",
  "recommended_resolution": "<strategy>",
  "explanation": "<brief rationale>"
}
"""


def format_user_prompt(
    variable: str,
    source_a_value: Any,
    source_b_value: Any,
    *,
    source_a_name: str = "source_a",
    source_b_name: str = "source_b",
) -> str:
    """Build the user message for conflict detection.

    Parameters
    ----------
    variable:
        Name of the data field in question (e.g. "acceptance_rate").
    source_a_value, source_b_value:
        The two conflicting values.
    source_a_name, source_b_name:
        Human-readable labels for the data sources.
    """
    payload = {
        "variable": variable,
        source_a_name: source_a_value,
        source_b_name: source_b_value,
    }
    return (
        "Analyse the following data points for conflict:\n"
        f"```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```"
    )
