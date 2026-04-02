"""Prompt templates for advisor router/planner."""

from __future__ import annotations


def build_single_shot_route_prompt(*, domain_rows: str, capability_rows: list[str]) -> str:
    """Build the single-shot planner prompt for route classification."""
    return (
        "Route one advisor turn and return JSON only.\n"
        "Decide domain and ranked capability candidates.\n\n"
        "Routing rules:\n"
        "- Multi-intent turns still require one primary.\n"
        "- If turn includes undergrad recommend/query/strategy together, prefer "
        "undergrad.school.recommend or undergrad.school.query as primary; "
        "put strategy as secondary/pending.\n"
        "- For offer compare vs decision, choose compare/decision directly when "
        "intent is clear; do not emit clarify unless truly ambiguous.\n"
        "- Return conflict_group per candidate for conflict detection.\n\n"
        "Examples:\n"
        "- \"Build an ED/EA/RD strategy for my profile\" => domain=undergrad, "
        "primary=undergrad.strategy.plan.\n"
        "- \"Compare my offers\" => domain=offer, primary=offer.compare.\n"
        "- \"recommend + query + strategy in one turn\" => domain=undergrad, "
        "primary in {undergrad.school.recommend, undergrad.school.query}.\n"
        "- \"我想推荐+问答+策略\" => domain=undergrad, "
        "primary in {undergrad.school.recommend, undergrad.school.query}.\n\n"
        f"Domains:\n{domain_rows}\n\n"
        "Capabilities:\n"
        + "\n".join(capability_rows)
        + "\n\nReturn JSON:"
        "{\"domain\":\"undergrad|offer|common\","
        "\"domain_confidence\":0.0,"
        "\"candidates\":[{\"capability\":\"...\",\"confidence\":0.0,\"conflict_group\":\"...\"}],"
        "\"intent_clarity\":0.0}"
    )

