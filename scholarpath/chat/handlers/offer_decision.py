"""Handler for OFFER_DECISION intent -- compares admission offers."""

from __future__ import annotations

import json
import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.language import detect_response_language, select_localized_text
from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient
from scholarpath.services.offer_service import compare_offers, list_offers

logger = logging.getLogger(__name__)


async def handle_offer_decision(
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    student_id: uuid.UUID,
    message: str,
) -> str:
    """Run a causal comparison of admission offers and return a recommendation.

    Returns
    -------
    str
        A structured comparison with a recommendation in conversational format.
    """
    session_id = str(student_id)
    response_lang = detect_response_language(message)

    # Check if the student has any offers
    offers = await list_offers(session, student_id)
    if not offers:
        return select_localized_text(
            "我这边还没有看到你的录取 offer。你可以先把学校、奖助信息和截止日期录进去，我再帮你做比较。",
            "I don't see any admission offers on record yet. Would you like to add your offers so I can help you compare them? I'll need the school name, aid package details, and decision deadline.",
            response_lang,
            mixed=(
                "我这边还没有看到你的录取 offer。你可以先把学校、奖助信息和截止日期录进去，我再帮你做比较。\n"
                "I don't see any admission offers on record yet."
            ),
        )

    actionable = [
        o for o in offers if o.status in ("admitted", "committed")
    ]
    if len(actionable) < 2:
        return select_localized_text(
            f"你目前只有 {len(actionable)} 个可比较的录取 offer。我至少需要 2 个 admitted/committed offer 才能做横向比较。你还有其他录取结果吗？",
            f"You currently have {len(actionable)} admitted offer(s). I need at least 2 admitted offers to run a comparison. Have you received any other decisions?",
            response_lang,
            mixed=(
                f"你目前只有 {len(actionable)} 个可比较的录取 offer，我至少需要 2 个。\n"
                f"You currently have {len(actionable)} admitted offer(s)."
            ),
        )

    # Run the comparison
    try:
        comparison = await compare_offers(
            session,
            llm,
            student_id,
            response_language=response_lang,
        )
    except Exception:
        logger.warning("Offer comparison failed", exc_info=True)
        return select_localized_text(
            "我这次没能顺利跑完完整比较。你可以先告诉我你在纠结哪两个 offer，我先给你做一个简化版对比。",
            "I had trouble running the full comparison. Could you tell me which two offers you're deciding between? I can do a simpler side-by-side analysis.",
            response_lang,
            mixed=(
                "我这次没能顺利跑完完整比较。你可以先告诉我你在纠结哪两个 offer。\n"
                "I had trouble running the full comparison."
            ),
        )

    # Format response
    parts: list[str] = [
        select_localized_text(
            "下面是你的 offer 横向比较：\n",
            "Here's how your offers compare:\n",
            response_lang,
            mixed="下面是你的 offer 横向比较：\nHere's how your offers compare:\n",
        )
    ]

    for offer_data in comparison.get("offers", []):
        school = offer_data.get("school", "Unknown")
        net_cost = offer_data.get("net_cost")
        total_aid = offer_data.get("total_aid")
        scores = offer_data.get("causal_scores", {})

        cost_str = f"${net_cost:,}/yr" if net_cost else select_localized_text("未知", "unknown", response_lang, mixed="未知 / unknown")
        aid_str = f"${total_aid:,}" if total_aid else select_localized_text("未知", "unknown", response_lang, mixed="未知 / unknown")

        parts.append(
            f"**{school}**\n"
            f"  {select_localized_text('净成本', 'Net cost', response_lang, mixed='净成本 / Net cost')}: {cost_str} | "
            f"{select_localized_text('总资助', 'Total aid', response_lang, mixed='总资助 / Total aid')}: {aid_str}\n"
            f"  {select_localized_text('职业前景', 'Career outlook', response_lang, mixed='职业前景 / Career outlook')}: {scores.get('career_outcome', 0):.0%} | "
            f"{select_localized_text('学术匹配', 'Academic fit', response_lang, mixed='学术匹配 / Academic fit')}: {scores.get('academic_outcome', 0):.0%} | "
            f"{select_localized_text('生活满意度', 'Life satisfaction', response_lang, mixed='生活满意度 / Life satisfaction')}: {scores.get('life_satisfaction', 0):.0%}"
        )

    # Add recommendation
    if recommendation := comparison.get("recommendation"):
        parts.append(
            f"\n**{select_localized_text('我的建议', 'My recommendation', response_lang, mixed='我的建议 / My recommendation')}**\n{recommendation}"
        )

    # Store comparison in context
    await memory.save_context(session_id, "last_comparison", {
        "offers": [o.get("school", "") for o in comparison.get("offers", [])],
    })

    return "\n\n".join(parts)
