"""Prompt template for cross-source entity alignment."""

from __future__ import annotations

import json
from typing import Any

SYSTEM_PROMPT = """\
You are a data-integration specialist.  Given two records from different data \
sources, determine whether they refer to the same university or program and \
identify which fields can be safely merged.

Rules:
1. Output ONLY valid JSON -- no markdown, no commentary.
2. Consider name variations, abbreviations, and bilingual names \
   (e.g. "MIT" vs "Massachusetts Institute of Technology" vs "麻省理工").
3. "match_confidence" is a float 0.0-1.0.
4. Only list a field in "mergeable_fields" if both records have non-null \
   values that are compatible (not contradictory).
5. List any contradictory fields in "conflicting_fields".

Output schema:
{
  "is_same_entity": <bool>,
  "match_confidence": <float>,
  "canonical_name": "<preferred name>",
  "mergeable_fields": ["<field_name>", ...],
  "conflicting_fields": ["<field_name>", ...]
}
"""


def format_user_prompt(
    record_a: dict[str, Any],
    record_b: dict[str, Any],
    *,
    source_a: str = "source_a",
    source_b: str = "source_b",
) -> str:
    """Build the user message for entity alignment.

    Parameters
    ----------
    record_a, record_b:
        Data records to compare.
    source_a, source_b:
        Human-readable labels for the data sources.
    """
    return (
        f"Record A (from {source_a}):\n"
        f"```json\n{json.dumps(record_a, ensure_ascii=False, indent=2)}\n```\n\n"
        f"Record B (from {source_b}):\n"
        f"```json\n{json.dumps(record_b, ensure_ascii=False, indent=2)}\n```"
    )
