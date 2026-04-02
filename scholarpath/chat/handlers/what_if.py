"""Handler for WHAT_IF intent -- parses and runs causal simulations."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient
from scholarpath.services.simulation_service import run_what_if

logger = logging.getLogger(__name__)

# Known intervention mappings from natural-language concepts to DAG nodes.
_INTERVENTION_CONCEPTS: dict[str, str] = {
    "sat": "student_ability",
    "sat_score": "student_ability",
    "gpa": "student_ability",
    "test_scores": "student_ability",
    "financial_aid": "financial_aid",
    "aid": "financial_aid",
    "scholarship": "financial_aid",
    "location": "location_effect",
    "research": "research_opportunities",
    "prestige": "brand_signal",
    "career_services": "career_services",
    "peer_network": "peer_network",
    "family_income": "family_ses",
}


async def handle_what_if(
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    session_id: str,
    student_id: uuid.UUID,
    message: str,
) -> str:
    """Parse a what-if scenario from natural language and run a simulation.

    The handler:
    1. Uses the LLM to extract intervention parameters from the message.
    2. Resolves the target school (from context or message).
    3. Runs the causal simulation.
    4. Returns results in conversational format.

    Returns
    -------
    str
        Simulation results in a conversational format.
    """
    # Get context for school resolution
    context = await memory.get_context(session_id, domain="undergrad")
    current_school_id = context.get("current_school_id")

    # Extract scenario from natural language
    scenario = await _parse_scenario(llm, message)
    interventions = scenario.get("interventions", {})
    school_name = scenario.get("school_name")

    if not interventions:
        return (
            "I understand you want to explore a hypothetical scenario, but I couldn't "
            "determine what changes to simulate. Could you be more specific?\n\n"
            "For example:\n"
            "- \"What if my SAT score were 1550?\"\n"
            "- \"What if I got a full scholarship?\"\n"
            "- \"What if I changed my major to CS?\""
        )

    # Resolve school_id
    school_id: uuid.UUID | None = None
    if current_school_id:
        try:
            school_id = uuid.UUID(current_school_id)
        except ValueError:
            logger.info(
                "Ignoring invalid school UUID in context: session=%s value=%s",
                session_id,
                current_school_id,
            )

    if school_id is None:
        return (
            "I need to know which school to run this simulation for. "
            "Could you mention the school name, or ask about a school first?"
        )

    # Run simulation
    try:
        result = await run_what_if(
            session, llm, student_id, school_id, interventions
        )
    except Exception:
        logger.warning("What-if simulation failed", exc_info=True)
        return (
            "I ran into an issue running the simulation. "
            "Could you try rephrasing your scenario?"
        )

    # Format results
    parts: list[str] = ["Here's what the simulation shows:\n"]

    deltas = result.get("deltas", {})
    for outcome, delta in deltas.items():
        direction = "increase" if delta > 0 else "decrease"
        label = outcome.replace("_", " ").title()
        parts.append(f"- **{label}**: {abs(delta):.1%} {direction}")

    if explanation := result.get("explanation"):
        parts.append(f"\n{explanation}")

    # Store in context
    await memory.save_context(
        session_id,
        "last_what_if",
        {
            "interventions": interventions,
            "deltas": deltas,
        },
        domain="offer",
    )

    return "\n".join(parts)


async def _parse_scenario(
    llm: LLMClient,
    message: str,
) -> dict[str, Any]:
    """Use the LLM to extract what-if interventions from natural language.

    Returns
    -------
    dict
        ``interventions``: mapping of DAG node ids to intervention values (0-1).
        ``school_name``: optional school name mentioned in the message.
    """
    messages = [
        {
            "role": "system",
            "content": (
                "You are parsing a what-if scenario for a college admissions tool.\n"
                "Extract the interventions the user wants to simulate.\n\n"
                "Available DAG nodes:\n"
                "- student_ability (GPA/SAT, 0-1 scale)\n"
                "- financial_aid (aid amount, 0-1 scale where 1=full ride)\n"
                "- location_effect (location benefit, 0-1)\n"
                "- research_opportunities (0-1)\n"
                "- brand_signal / prestige (0-1)\n"
                "- career_services (0-1)\n"
                "- peer_network (0-1)\n"
                "- family_ses (0-1)\n\n"
                "For SAT scores: map to student_ability. "
                "1600 -> 1.0, 1400 -> 0.83, 1200 -> 0.67, 1000 -> 0.5.\n"
                "For GPA: 4.0 -> 1.0, 3.5 -> 0.875.\n\n"
                "Return JSON: {\"interventions\": {\"node_id\": value}, "
                "\"school_name\": \"...\" or null}"
            ),
        },
        {"role": "user", "content": message},
    ]

    try:
        result = await llm.complete_json(messages, temperature=0.1, max_tokens=256)
        return result
    except Exception:
        logger.warning("Scenario parsing failed", exc_info=True)
        return {"interventions": {}, "school_name": None}
