"""Handler for OFFER_DECISION intent -- compares admission offers."""

from __future__ import annotations

import json
import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient
from scholarpath.services.offer_service import compare_offers, list_offers

logger = logging.getLogger(__name__)


async def handle_offer_decision(
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    session_id: str,
    student_id: uuid.UUID,
    message: str,
) -> str:
    """Run a causal comparison of admission offers and return a recommendation.

    Returns
    -------
    str
        A structured comparison with a recommendation in conversational format.
    """
    # Check if the student has any offers
    offers = await list_offers(session, student_id)
    if not offers:
        return (
            "I don't see any admission offers on record yet. "
            "Would you like to add your offers so I can help you compare them? "
            "I'll need the school name, aid package details, and decision deadline."
        )

    actionable = [
        o for o in offers if o.status in ("admitted", "committed")
    ]
    if len(actionable) < 2:
        return (
            f"You currently have {len(actionable)} admitted offer(s). "
            "I need at least 2 admitted offers to run a comparison. "
            "Have you received any other decisions?"
        )

    # Run the comparison
    try:
        comparison = await compare_offers(session, llm, student_id)
    except Exception:
        logger.warning("Offer comparison failed", exc_info=True)
        return (
            "I had trouble running the full comparison. "
            "Could you tell me which two offers you're deciding between? "
            "I can do a simpler side-by-side analysis."
        )

    # Format response
    parts: list[str] = ["Here's how your offers compare:\n"]

    for offer_data in comparison.get("offers", []):
        school = offer_data.get("school", "Unknown")
        net_cost = offer_data.get("net_cost")
        total_aid = offer_data.get("total_aid")
        scores = offer_data.get("causal_scores", {})

        cost_str = f"${net_cost:,}/yr" if net_cost else "unknown"
        aid_str = f"${total_aid:,}" if total_aid else "unknown"

        parts.append(
            f"**{school}**\n"
            f"  Net cost: {cost_str} | Total aid: {aid_str}\n"
            f"  Career outlook: {scores.get('career_outcome', 'N/A'):.0%} | "
            f"Academic fit: {scores.get('academic_outcome', 'N/A'):.0%} | "
            f"Life satisfaction: {scores.get('life_satisfaction', 'N/A'):.0%}"
        )

    # Add recommendation
    if recommendation := comparison.get("recommendation"):
        parts.append(f"\n**My recommendation:**\n{recommendation}")

    # Store comparison in context
    await memory.save_context(
        session_id,
        "last_comparison",
        {"offers": [o.get("school", "") for o in comparison.get("offers", [])]},
        domain="offer",
    )

    return "\n\n".join(parts)
