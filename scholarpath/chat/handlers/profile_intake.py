"""Handler for PROFILE_INTAKE intent -- extracts profile data from conversation."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient
from scholarpath.llm.prompts import PROFILE_EXTRACTION_PROMPT, format_profile_extraction
from scholarpath.services.student_service import (
    check_profile_completeness,
    get_student,
    update_student,
)

logger = logging.getLogger(__name__)

# Mapping from LLM extraction keys to Student model fields.
_FIELD_MAP: dict[str, str] = {
    "gpa": "gpa",
    "sat_total": "sat_total",
    "toefl": "toefl_total",
    "curriculum": "curriculum_type",
    "ap_courses": "ap_courses",
    "extracurriculars": "extracurriculars",
    "awards": "awards",
    "intended_majors": "intended_majors",
    "budget": "budget_usd",
    "preferences": "preferences",
}


async def handle_profile_intake(
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    student_id: uuid.UUID,
    message: str,
) -> str:
    """Extract profile information from the user message and update the student record.

    The handler:
    1. Combines the current message with recent conversation history.
    2. Sends the conversation to the LLM for profile extraction.
    3. Updates the student record with any newly extracted fields.
    4. Checks for missing fields and asks follow-up questions.

    Returns
    -------
    str
        A response guiding the user through profile completion.
    """
    session_id = str(student_id)

    # Gather recent conversation for context
    history = await memory.get_history(session_id, limit=10)
    conversation_text = "\n".join(
        f"{m['role'].capitalize()}: {m['content']}" for m in history
    )
    conversation_text += f"\nUser: {message}"

    # Run LLM extraction
    user_prompt = format_profile_extraction(conversation_text)
    messages = [
        {"role": "system", "content": PROFILE_EXTRACTION_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    try:
        extracted = await llm.complete_json(messages, temperature=0.2, max_tokens=1024)
    except Exception:
        logger.warning("Profile extraction LLM call failed", exc_info=True)
        return (
            "I had trouble processing that. Could you tell me again? "
            "I need your GPA, test scores, intended major, and budget."
        )

    # Build update dict from extracted fields
    update_data: dict[str, Any] = {}
    for llm_key, model_key in _FIELD_MAP.items():
        value = extracted.get(llm_key)
        if value is not None:
            # Special handling for budget: parse string like "$60k/year" -> int
            if llm_key == "budget" and isinstance(value, str):
                parsed = _parse_budget(value)
                if parsed is not None:
                    update_data[model_key] = parsed
            else:
                update_data[model_key] = value

    # Apply updates
    if update_data:
        student = await update_student(session, student_id, update_data)
        # Store extracted data in context for future reference
        await memory.save_context(session_id, "last_extracted", update_data)
    else:
        student = await get_student(session, student_id)

    # Check completeness
    completeness = await check_profile_completeness(student)
    missing = completeness["missing_fields"]
    pct = completeness["completion_pct"]

    if completeness["completed"]:
        # Ensure profile embedding is up to date when profile is complete
        try:
            from scholarpath.llm.embeddings import get_embedding_service
            emb = get_embedding_service()
            profile_data = {
                "intended_majors": student.intended_majors,
                "gpa": student.gpa,
                "gpa_scale": student.gpa_scale,
                "sat_total": student.sat_total,
                "extracurriculars": student.extracurriculars,
                "awards": student.awards,
                "preferences": student.preferences,
                "budget_usd": student.budget_usd,
            }
            student.profile_embedding = await emb.embed_student_profile(profile_data)
            await session.flush()
        except Exception:
            logger.warning("Failed to embed profile on completion", exc_info=True)

        return (
            f"Your profile is complete ({pct:.0%})! "
            "I have all the information I need to start building your school list. "
            "Would you like me to generate personalized school recommendations?"
        )

    # Ask for the next missing fields
    missing_str = ", ".join(missing[:3])
    return (
        f"Thanks! I've updated your profile ({pct:.0%} complete). "
        f"I still need: {missing_str}. "
        "Could you share those details?"
    )


def _parse_budget(raw: str) -> int | None:
    """Best-effort parse of a budget string to annual USD integer."""
    import re

    raw = raw.replace(",", "").replace("$", "").strip()
    # Handle "60k" or "60K"
    match = re.search(r"(\d+(?:\.\d+)?)\s*[kK]", raw)
    if match:
        return int(float(match.group(1)) * 1000)
    # Handle plain number
    match = re.search(r"(\d+)", raw)
    if match:
        value = int(match.group(1))
        # Heuristic: if under 500, probably in thousands
        if value < 500:
            return value * 1000
        return value
    return None
