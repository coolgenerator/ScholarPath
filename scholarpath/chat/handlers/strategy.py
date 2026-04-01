"""Handler for STRATEGY_ADVICE intent -- ED/EA/RD recommendations."""

from __future__ import annotations

import json
import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient
from scholarpath.services.evaluation_service import generate_strategy, get_tiered_list

logger = logging.getLogger(__name__)


async def handle_strategy(
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    student_id: uuid.UUID,
    message: str,
) -> str:
    """Generate application strategy advice using the evaluation service.

    Returns ED/EA/RD recommendations with risk analysis formatted for
    conversational delivery.

    Returns
    -------
    str
        Strategy advice response text.
    """
    session_id = str(student_id)

    # Check if the student has evaluations
    tiered = await get_tiered_list(session, student_id)
    total_schools = sum(len(v) for v in tiered.values())

    if total_schools == 0:
        return (
            "I don't have any school evaluations for you yet. "
            "Let's first build your school list so I can recommend an "
            "application strategy. Would you like me to generate school "
            "recommendations based on your profile?"
        )

    # Generate strategy via the evaluation service
    try:
        strategy = await generate_strategy(session, llm, student_id)
    except Exception:
        logger.warning("Strategy generation failed", exc_info=True)
        return (
            "I encountered an issue generating your strategy. "
            "Let me try a simpler analysis. Could you tell me which schools "
            "you're most interested in applying Early Decision to?"
        )

    # Format the strategy into a conversational response
    parts: list[str] = []

    # ED recommendation
    ed = strategy.get("ed_recommendation", {})
    if ed and ed.get("school"):
        parts.append(
            f"**Early Decision recommendation:** {ed['school']}\n"
            f"{ed.get('rationale', '')}"
        )

    # EA recommendations
    ea_list = strategy.get("ea_recommendations", [])
    if ea_list:
        ea_names = ", ".join(e.get("school", "?") for e in ea_list)
        parts.append(f"**Early Action suggestions:** {ea_names}")

    # RD recommendations
    rd_list = strategy.get("rd_recommendations", [])
    if rd_list:
        rd_names = ", ".join(r.get("school", "?") for r in rd_list)
        parts.append(f"**Regular Decision:** {rd_names}")

    # Risk analysis
    if risk := strategy.get("risk_analysis"):
        parts.append(f"**Risk analysis:** {risk}")

    # Timeline
    if timeline := strategy.get("timeline"):
        parts.append(f"**Timeline:** {timeline}")

    if not parts:
        return (
            "I generated a strategy but the results were inconclusive. "
            "Could you share more about your priorities -- is maximising "
            "admission chances or fit more important to you?"
        )

    # Store in context
    await memory.save_context(session_id, "last_strategy", strategy)

    return "\n\n".join(parts)
