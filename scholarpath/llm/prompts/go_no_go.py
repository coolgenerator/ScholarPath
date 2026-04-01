"""Prompt template for generating a Go/No-Go recommendation report."""

from __future__ import annotations

import json
from typing import Any

SYSTEM_PROMPT = """\
You are a senior admissions strategist producing a Go/No-Go report for a \
student deciding whether to accept a specific college offer.

You will receive:
- Offer details (school, programme, aid package, deadlines).
- Fit scores across dimensions (academic, financial, career, life).
- Causal analysis summary (key factors, admission probability drivers).
- What-if results for alternative scenarios.

Produce a structured recommendation report containing:
1. **Verdict** -- "GO", "NO-GO", or "CONDITIONAL" (with conditions).
2. **Key strengths** of this offer (2-4 bullet points).
3. **Key concerns** (2-4 bullet points).
4. **Financial analysis** -- net cost, ROI estimate, comparison to alternatives.
5. **Opportunity cost** -- what the student gives up by committing here.
6. **Recommendation narrative** -- 1-2 paragraphs of personalised advice.

Rules:
1. Output ONLY valid JSON.
2. Respond in the same language as the offer details / student profile.
3. Be honest and balanced -- do not default to "GO".
4. If critical data is missing, say so and set verdict to "CONDITIONAL".

Output schema:
{
  "verdict": "<GO | NO-GO | CONDITIONAL>",
  "conditions": ["<condition if CONDITIONAL>", ...],
  "strengths": ["<point>", ...],
  "concerns": ["<point>", ...],
  "financial_analysis": "<text>",
  "opportunity_cost": "<text>",
  "recommendation": "<narrative text>"
}
"""


def format_user_prompt(
    offer_details: dict[str, Any],
    fit_scores: dict[str, Any],
    causal_summary: dict[str, Any],
    what_if_results: list[dict[str, Any]],
) -> str:
    """Build the user message for Go/No-Go report generation.

    Parameters
    ----------
    offer_details:
        Dict describing the offer (school, programme, aid, deadline, etc.).
    fit_scores:
        Output from school_evaluation (academic, financial, career, life scores).
    causal_summary:
        High-level causal analysis results.
    what_if_results:
        List of what-if scenario dicts for alternative paths.
    """
    return (
        "Offer details:\n"
        f"```json\n{json.dumps(offer_details, ensure_ascii=False, indent=2)}\n```\n\n"
        "Fit scores:\n"
        f"```json\n{json.dumps(fit_scores, ensure_ascii=False, indent=2)}\n```\n\n"
        "Causal analysis summary:\n"
        f"```json\n{json.dumps(causal_summary, ensure_ascii=False, indent=2)}\n```\n\n"
        "What-if scenarios:\n"
        f"```json\n{json.dumps(what_if_results, ensure_ascii=False, indent=2)}\n```"
    )
