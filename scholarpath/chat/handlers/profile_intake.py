"""Handler for PROFILE_INTAKE intent -- extracts profile data from conversation."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.language import detect_response_language, select_localized_text
from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient
from scholarpath.llm.prompts import PROFILE_EXTRACTION_PROMPT, format_profile_extraction
from scholarpath.services.portfolio_service import (
    apply_portfolio_patch,
    canonicalize_preferences,
)
from scholarpath.services.student_service import (
    check_profile_completeness,
    get_student,
)

logger = logging.getLogger(__name__)

# Mapping from LLM extraction keys to Student model fields.
_FIELD_MAP: dict[str, tuple[str, str]] = {
    "gpa": ("academics", "gpa"),
    "sat_total": ("academics", "sat_total"),
    "toefl": ("academics", "toefl_total"),
    "curriculum": ("academics", "curriculum_type"),
    "ap_courses": ("academics", "ap_courses"),
    "extracurriculars": ("activities", "extracurriculars"),
    "awards": ("activities", "awards"),
    "intended_majors": ("academics", "intended_majors"),
    "budget": ("finance", "budget_usd"),
}


async def handle_profile_intake(
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    session_id: str,
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
    response_lang = detect_response_language(message)

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
        return select_localized_text(
            "我刚才没处理好这条信息。可以再说一遍吗？我需要你的 GPA、标化成绩、目标专业和预算。",
            "I had trouble processing that. Could you tell me again? I need your GPA, test scores, intended major, and budget.",
            response_lang,
            mixed=(
                "我刚才没处理好这条信息。可以再说一遍吗？我需要你的 GPA、标化成绩、目标专业和预算。\n"
                "I need your GPA, test scores, intended major, and budget."
            ),
        )

    # Build update dict from extracted fields
    patch_data: dict[str, Any] = {}
    for llm_key, (group_key, model_key) in _FIELD_MAP.items():
        value = extracted.get(llm_key)
        if value is not None:
            # Special handling for budget: parse string like "$60k/year" -> int
            if llm_key == "budget" and isinstance(value, str):
                parsed = _parse_budget(value)
                if parsed is not None:
                    patch_data.setdefault(group_key, {})[model_key] = parsed
            else:
                patch_data.setdefault(group_key, {})[model_key] = value

    preferences = extracted.get("preferences")
    if isinstance(preferences, dict):
        canonical = canonicalize_preferences(preferences)
        if canonical:
            patch_data["preferences"] = canonical

    # Apply updates
    if patch_data:
        await apply_portfolio_patch(session, student_id, patch_data)
        student = await get_student(session, student_id)
        # Store extracted data in context for future reference
        await memory.save_context(session_id, "last_extracted", patch_data)
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

        return select_localized_text(
            f"你的档案已经完整（{pct:.0%}）！我已经具备开始建立选校清单所需的信息。要不要我现在为你生成个性化学校推荐？",
            f"Your profile is complete ({pct:.0%})! I have all the information I need to start building your school list. Would you like me to generate personalized school recommendations?",
            response_lang,
            mixed=(
                f"你的档案已经完整（{pct:.0%}）！要不要我现在为你生成个性化学校推荐？\n"
                f"Your profile is complete ({pct:.0%}). Would you like me to generate personalized school recommendations?"
            ),
        )

    # Ask for the next missing fields
    missing_str = ", ".join(missing[:3])
    return select_localized_text(
        f"好的，我已经更新了你的档案（完成度 {pct:.0%}）。我还需要这些信息：{missing_str}。方便补充一下吗？",
        f"Thanks! I've updated your profile ({pct:.0%} complete). I still need: {missing_str}. Could you share those details?",
        response_lang,
        mixed=(
            f"好的，我已经更新了你的档案（完成度 {pct:.0%}）。我还需要这些信息：{missing_str}。\n"
            f"Thanks! I've updated your profile ({pct:.0%} complete). I still need: {missing_str}."
        ),
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
