"""Handler for generating and formatting school recommendations as a chat response."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient
from scholarpath.services.recommendation_service import generate_recommendations

logger = logging.getLogger(__name__)


async def handle_recommendation(
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    student_id: uuid.UUID,
    message: str,
) -> str:
    """Generate and format school recommendations as a chat response.

    Steps
    -----
    1. Call ``generate_recommendations()`` for the full causal pipeline.
    2. Format the results into a readable tiered school list.
    3. Save recommendation context in memory for follow-up questions.

    Parameters
    ----------
    llm:
        LLM client for narrative generation.
    session:
        SQLAlchemy async session.
    memory:
        Redis-backed conversation memory.
    student_id:
        UUID of the student.
    message:
        The user's latest message (used for context).

    Returns
    -------
    str
        Formatted recommendation response.
    """
    session_id = str(student_id)

    try:
        results = await generate_recommendations(session, llm, student_id)
    except Exception:
        logger.exception("Recommendation generation failed for student %s", student_id)
        return (
            "I'm sorry, I wasn't able to generate recommendations right now. "
            "Could you make sure your profile is complete and try again? "
            "抱歉，暂时无法生成推荐。请确认你的资料已填写完整后重试。"
        )

    schools = results.get("schools", [])
    strategy = results.get("strategy", {})
    narrative = results.get("narrative", "")

    if not schools:
        return (
            "I couldn't find matching schools based on your profile. "
            "Let me know if you'd like to adjust your preferences. "
            "没有找到匹配的学校，要不要调整一下你的偏好？"
        )

    # Save context for follow-up
    await memory.save_context(
        session_id,
        "recommendations",
        {
            "school_names": [s["school_name"] for s in schools],
            "tier_counts": strategy.get("tier_counts", {}),
        },
    )

    # Build structured recommendation data
    structured_schools = []
    for s in schools:
        info = s.get("school_info", {})
        raw_sub = s.get("sub_scores", {})
        sub_scores = {
            "academic": raw_sub.get("academic", 0),
            "financial": raw_sub.get("financial", 0),
            "career": raw_sub.get("career", 0),
            "life": raw_sub.get("life", 0),
        }
        structured_schools.append({
            "school_name": s["school_name"],
            "school_name_cn": s.get("school_name_cn"),
            "tier": s["tier"],
            "rank": info.get("rank") or info.get("us_news_rank"),
            "overall_score": s["overall_score"],
            "admission_probability": s.get("admission_probability", 0),
            "acceptance_rate": info.get("acceptance_rate"),
            "net_price": info.get("avg_net_price"),
            "key_reasons": s.get("key_reasons", []),
            "sub_scores": sub_scores,
        })

    ed_rec = strategy.get("ed_recommendation")
    ea_recs = strategy.get("ea_recommendations", [])

    # Build strategy summary
    strategy_parts: list[str] = []
    if ed_rec:
        strategy_parts.append(f"ED: {ed_rec['school']}")
    if ea_recs:
        ea_names = ", ".join(r["school"] for r in ea_recs[:5])
        strategy_parts.append(f"EA: {ea_names}")
    strategy_summary = " | ".join(strategy_parts) if strategy_parts else None

    recommendation_data = {
        "narrative": narrative,
        "schools": structured_schools,
        "ed_recommendation": ed_rec["school"] if ed_rec else None,
        "ea_recommendations": [r["school"] for r in ea_recs],
        "strategy_summary": strategy_summary,
    }

    # Short text summary + structured data marker
    context = await memory.get_context(session_id)
    lang = context.get("user_language", "en")
    if lang == "zh":
        summary = f"基于你的背景分析，我为你推荐了 {len(schools)} 所学校。"
    else:
        summary = f"Based on your profile, I've recommended {len(schools)} schools for you."

    return f"{summary}\n[RECOMMENDATION]{json.dumps(recommendation_data, ensure_ascii=False)}"
