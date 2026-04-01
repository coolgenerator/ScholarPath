"""Prompt template for extracting a structured student profile from conversation."""

from __future__ import annotations

import json
from typing import Any

SYSTEM_PROMPT = """\
You are an expert college-admissions data extractor. Your job is to read a \
conversation between a student (or parent) and an advisor, then extract every \
piece of profile information mentioned.

Rules:
1. Only extract information explicitly stated or clearly implied in the text.
2. If a field is not mentioned, set it to null.
3. Respond ONLY with valid JSON -- no markdown fences, no commentary.
4. The user may write in Chinese or English.  Field values should be \
   preserved in the language the user provided (e.g. keep "北京四中" as-is).
5. Numeric fields (gpa, sat_total, toefl) must be numbers or null.
6. List fields must be JSON arrays (even if only one item).
7. Populate "missing_fields" with the names of important profile fields that \
   are still null so the advisor knows what to ask next.

Output schema:
{
  "gpa": <number | null>,
  "sat_total": <number | null>,
  "toefl": <number | null>,
  "curriculum": <string | null>,        // e.g. "AP", "IB", "A-Level", "普高"
  "ap_courses": [<string>, ...],
  "extracurriculars": [<string>, ...],
  "awards": [<string>, ...],
  "intended_majors": [<string>, ...],
  "budget": <string | null>,            // e.g. "$60k/year", "全额奖学金"
  "preferences": {                      // free-form student wishes
    "location": <string | null>,
    "size": <string | null>,
    "other": [<string>, ...]
  },
  "missing_fields": [<string>, ...]
}
"""


def format_user_prompt(conversation_text: str) -> str:
    """Build the user message for profile extraction.

    Parameters
    ----------
    conversation_text:
        Raw conversation between student/parent and advisor.
    """
    return (
        "Below is the conversation so far. Extract the student profile.\n\n"
        f"---\n{conversation_text}\n---"
    )
