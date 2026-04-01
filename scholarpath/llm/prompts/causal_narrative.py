"""Prompt template for generating natural-language causal analysis explanations."""

from __future__ import annotations

import json
from typing import Any

SYSTEM_PROMPT = """\
You are an expert at explaining causal-inference results in plain language \
for students and parents who are not statisticians.

You will receive:
- A summary of the causal graph (nodes, edges, key pathways).
- Mediation decomposition results (direct vs indirect effects).
- What-if scenario results (counterfactual estimates).

Your job is to produce a clear, conversational explanation that:
1. Explains the key causal relationships in everyday terms.
2. Highlights which factors have the biggest impact on admission probability.
3. Translates mediation results into actionable insights.
4. Describes what-if results as concrete "if you do X, then Y" statements.
5. Avoids jargon -- no "ATE", "ACME", "backdoor criterion" etc.

Rules:
1. Respond in the same language as the what-if results / user context.
2. Use short paragraphs and bullet points for readability.
3. Do NOT output JSON.  Output natural prose / markdown.
4. Keep the total response under 600 words.
"""


def format_user_prompt(
    causal_graph_summary: dict[str, Any],
    mediation_results: dict[str, Any],
    what_if_results: list[dict[str, Any]],
) -> str:
    """Build the user message for causal narrative generation.

    Parameters
    ----------
    causal_graph_summary:
        High-level description of the causal DAG (nodes, key edges).
    mediation_results:
        Output from MediationAnalyzer (direct/indirect effects).
    what_if_results:
        List of what-if scenario dicts with counterfactual estimates.
    """
    return (
        "Causal graph summary:\n"
        f"```json\n{json.dumps(causal_graph_summary, ensure_ascii=False, indent=2)}\n```\n\n"
        "Mediation decomposition:\n"
        f"```json\n{json.dumps(mediation_results, ensure_ascii=False, indent=2)}\n```\n\n"
        "What-if scenarios:\n"
        f"```json\n{json.dumps(what_if_results, ensure_ascii=False, indent=2)}\n```"
    )
