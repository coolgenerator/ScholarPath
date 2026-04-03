"""Handler for STRATEGY_ADVICE intent -- ED/EA/RD recommendations."""

from __future__ import annotations

import json
import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.language import detect_response_language, select_localized_text
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
    response_lang = detect_response_language(message)

    # Check if the student has evaluations
    tiered = await get_tiered_list(session, student_id)
    total_schools = sum(len(v) for v in tiered.values())

    if total_schools == 0:
        return select_localized_text(
            "我这边还没有你的学校评估结果。我们先把选校清单建起来，我才能进一步给你 ED/EA/RD 申请策略。要不要我先帮你做学校推荐？",
            "I don't have any school evaluations for you yet. Let's first build your school list so I can recommend an application strategy. Would you like me to generate school recommendations based on your profile?",
            response_lang,
            mixed=(
                "我这边还没有你的学校评估结果。我们先把选校清单建起来，再聊申请策略。\n"
                "I don't have any school evaluations for you yet."
            ),
        )

    # Generate strategy via the evaluation service
    try:
        strategy = await generate_strategy(
            session,
            llm,
            student_id,
            response_language=response_lang,
        )
    except Exception:
        logger.warning("Strategy generation failed", exc_info=True)
        return select_localized_text(
            "我这次没能顺利生成完整策略。你可以先告诉我你最想冲 ED 的学校，我先给你做一个简化判断。",
            "I encountered an issue generating your strategy. Let me try a simpler analysis. Could you tell me which schools you're most interested in applying Early Decision to?",
            response_lang,
            mixed=(
                "我这次没能顺利生成完整策略。你可以先告诉我你最想冲 ED 的学校。\n"
                "I encountered an issue generating your strategy."
            ),
        )

    # Format the strategy into a conversational response
    parts: list[str] = []

    # ED recommendation
    ed = strategy.get("ed_recommendation", {})
    if ed and ed.get("school"):
        parts.append(
            f"**{select_localized_text('Early Decision 建议', 'Early Decision recommendation', response_lang, mixed='Early Decision 建议 / Recommendation')}**: {ed['school']}\n"
            f"{ed.get('rationale', '')}"
        )

    # EA recommendations
    ea_list = strategy.get("ea_recommendations", [])
    if ea_list:
        ea_names = ", ".join(e.get("school", "?") for e in ea_list)
        parts.append(
            f"**{select_localized_text('Early Action 建议', 'Early Action suggestions', response_lang, mixed='Early Action 建议 / Suggestions')}**: {ea_names}"
        )

    # RD recommendations
    rd_list = strategy.get("rd_recommendations", [])
    if rd_list:
        rd_names = ", ".join(r.get("school", "?") for r in rd_list)
        parts.append(
            f"**{select_localized_text('Regular Decision 规划', 'Regular Decision', response_lang, mixed='Regular Decision 规划 / Plan')}**: {rd_names}"
        )

    # Risk analysis
    if risk := strategy.get("risk_analysis"):
        parts.append(
            f"**{select_localized_text('风险分析', 'Risk analysis', response_lang, mixed='风险分析 / Risk analysis')}**: {risk}"
        )

    # Timeline
    if timeline := strategy.get("timeline"):
        parts.append(
            f"**{select_localized_text('时间安排', 'Timeline', response_lang, mixed='时间安排 / Timeline')}**: {timeline}"
        )

    if not parts:
        return select_localized_text(
            "我已经生成了一版策略，但结论还不够明确。你可以再告诉我：你更看重录取概率，还是更看重学校匹配度？",
            "I generated a strategy but the results were inconclusive. Could you share more about your priorities -- is maximising admission chances or fit more important to you?",
            response_lang,
            mixed=(
                "我已经生成了一版策略，但结论还不够明确。你更看重录取概率，还是更看重学校匹配度？\n"
                "I generated a strategy but the results were inconclusive."
            ),
        )

    # Store in context
    await memory.save_context(session_id, "last_strategy", strategy)

    return "\n\n".join(parts)
