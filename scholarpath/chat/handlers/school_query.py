"""Handler for SCHOOL_QUERY intent -- answers questions about specific schools."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.chat.memory import ChatMemory
from scholarpath.db.models import School
from scholarpath.llm.client import LLMClient
from scholarpath.services.school_service import get_school_detail, search_schools

logger = logging.getLogger(__name__)


async def handle_school_query(
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    student_id: uuid.UUID,
    message: str,
) -> str:
    """Identify the school being asked about, fetch data, and generate a response.

    The handler:
    1. Uses the LLM to identify which school the user is asking about.
    2. Searches the database for matching schools.
    3. Fetches detailed data (programs, data points, conflicts).
    4. Generates a Knowledge Card summary via the LLM.

    Returns
    -------
    str
        An informative response about the requested school.
    """
    session_id = str(student_id)

    # Step 1: Identify the school name from the message
    school_name = await _extract_school_name(llm, message)
    if not school_name:
        return (
            "I'm not sure which school you're asking about. "
            "Could you provide the full name of the university?"
        )

    # Step 2: Search the database
    results = await search_schools(session, {"q": school_name, "limit": 3})
    if not results:
        return (
            f"I couldn't find \"{school_name}\" in our database. "
            "Please double-check the name, or I can search with different terms."
        )

    school = results[0]
    await memory.save_context(session_id, "current_school_id", str(school.id))
    await memory.save_context(session_id, "current_school_name", school.name)

    # Step 3: Get detailed data
    detail = await get_school_detail(session, school.id)
    programs = detail["programs"]
    data_points = detail["data_points"]
    conflicts = detail["conflicts"]

    # Step 4: Build Knowledge Card and generate response
    knowledge_card = _build_knowledge_card(school, programs, data_points, conflicts)

    messages = [
        {
            "role": "system",
            "content": (
                "You are a college admissions advisor. Answer the student's "
                "question about a school using the provided Knowledge Card. "
                "Be informative, accurate, and conversational. If data has "
                "conflicts, mention the uncertainty. The student may write "
                "in Chinese or English -- respond in the same language."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Student question: {message}\n\n"
                f"Knowledge Card:\n{json.dumps(knowledge_card, ensure_ascii=False, indent=2)}"
            ),
        },
    ]
    response = await llm.complete(messages, temperature=0.6, max_tokens=1024, caller="chat.school_query")
    return response


async def _extract_school_name(llm: LLMClient, message: str) -> str | None:
    """Use the LLM to extract a school name from a user message."""
    messages = [
        {
            "role": "system",
            "content": (
                "Extract the school/university name from the user message. "
                "Return ONLY a JSON object: {\"school_name\": \"...\"} or "
                "{\"school_name\": null} if no school is mentioned. "
                "Handle Chinese names (e.g. 斯坦福 -> Stanford)."
            ),
        },
        {"role": "user", "content": message},
    ]
    try:
        result = await llm.complete_json(messages, temperature=0.1, max_tokens=128, caller="chat.extract_school")
        return result.get("school_name")
    except Exception:
        logger.warning("School name extraction failed", exc_info=True)
        return None


def _build_knowledge_card(
    school: School,
    programs: list,
    data_points: list,
    conflicts: list,
) -> dict[str, Any]:
    """Build a structured summary for the LLM to consume."""
    card: dict[str, Any] = {
        "name": school.name,
        "name_cn": school.name_cn,
        "location": f"{school.city}, {school.state}",
        "type": school.school_type,
        "us_news_rank": school.us_news_rank,
        "acceptance_rate": school.acceptance_rate,
        "sat_range": f"{school.sat_25}-{school.sat_75}" if school.sat_25 else None,
        "tuition_oos": school.tuition_oos,
        "avg_net_price": school.avg_net_price,
        "student_faculty_ratio": school.student_faculty_ratio,
        "graduation_rate_4yr": school.graduation_rate_4yr,
        "intl_student_pct": school.intl_student_pct,
        "campus_setting": school.campus_setting,
        "programs": [
            {
                "name": p.name,
                "department": p.department,
                "rank": p.us_news_rank,
                "has_research": p.has_research_opps,
                "has_coop": p.has_coop,
            }
            for p in programs[:10]
        ],
        "data_conflict_count": len(conflicts),
    }

    # Add a few key data points
    if data_points:
        card["key_data_points"] = [
            {
                "variable": dp.variable_name,
                "value": dp.value_text,
                "source": dp.source_name,
                "confidence": dp.confidence,
            }
            for dp in data_points[:5]
        ]

    if conflicts:
        card["data_conflicts"] = [
            {
                "variable": c.variable_name,
                "value_a": c.value_a,
                "value_b": c.value_b,
                "severity": c.severity,
            }
            for c in conflicts[:3]
        ]

    return card
