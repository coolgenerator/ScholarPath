"""Capability adapters wiring legacy handlers/services into advisor v1."""

from __future__ import annotations

import json
import logging
from typing import Any

from pydantic import ValidationError

from scholarpath.advisor.contracts import (
    AdvisorAction,
    GuidedIntakeArtifact,
    GuidedQuestion,
    InfoCardArtifact,
    OfferComparisonArtifact,
    RecommendationData,
    SchoolRecommendationArtifact,
    StrategyPlanArtifact,
    WhatIfResultArtifact,
)
from scholarpath.advisor.orchestration import (
    CapabilityContext,
    CapabilityDefinition,
    CapabilityRegistry,
    CapabilityResult,
)
from scholarpath.advisor.output_polisher import get_output_polisher
from scholarpath.chat.handlers.guided_intake import handle_guided_intake
from scholarpath.chat.handlers.offer_decision import handle_offer_decision
from scholarpath.chat.handlers.recommend import handle_recommendation
from scholarpath.chat.handlers.school_query import handle_school_query
from scholarpath.chat.handlers.strategy import handle_strategy
from scholarpath.chat.handlers.what_if import handle_what_if
from scholarpath.observability import log_fallback
from scholarpath.services.offer_service import compare_offers

logger = logging.getLogger(__name__)


def build_default_registry() -> CapabilityRegistry:
    """Construct the default advisor capability registry."""
    registry = CapabilityRegistry()
    registry.register(
        CapabilityDefinition(
            capability_id="undergrad.profile.intake",
            domain="undergrad",
            description="收集并更新本科申请画像信息。",
            handler=_handle_undergrad_profile_intake,
            requires_student=True,
            produces_artifacts=("guided_intake", "school_recommendation"),
        )
    )
    registry.register(
        CapabilityDefinition(
            capability_id="undergrad.school.recommend",
            domain="undergrad",
            description="生成本科选校推荐列表。",
            handler=_handle_undergrad_school_recommend,
            requires_student=True,
            produces_artifacts=("school_recommendation",),
        )
    )
    registry.register(
        CapabilityDefinition(
            capability_id="undergrad.school.query",
            domain="undergrad",
            description="回答本科院校详情和比较问题。",
            handler=_handle_undergrad_school_query,
            requires_student=True,
            produces_artifacts=("info_card",),
        )
    )
    registry.register(
        CapabilityDefinition(
            capability_id="undergrad.strategy.plan",
            domain="undergrad",
            description="给出 ED/EA/RD 本科申请策略。",
            handler=_handle_undergrad_strategy_plan,
            requires_student=True,
            produces_artifacts=("strategy_plan",),
        )
    )
    registry.register(
        CapabilityDefinition(
            capability_id="offer.compare",
            domain="offer",
            description="比较多个已录取 offer。",
            handler=_handle_offer_compare,
            requires_student=True,
            produces_artifacts=("offer_comparison",),
        )
    )
    registry.register(
        CapabilityDefinition(
            capability_id="offer.decision",
            domain="offer",
            description="给出录取 offer 取舍建议。",
            handler=_handle_offer_decision,
            requires_student=True,
            produces_artifacts=("offer_comparison",),
        )
    )
    registry.register(
        CapabilityDefinition(
            capability_id="offer.what_if",
            domain="offer",
            description="对 offer 决策做 what-if 模拟。",
            handler=_handle_offer_what_if,
            requires_student=True,
            produces_artifacts=("what_if_result",),
        )
    )
    registry.register(
        CapabilityDefinition(
            capability_id="common.general",
            domain="common",
            description="通用问答和闲聊。",
            handler=_handle_common_general,
            requires_student=False,
        )
    )
    registry.register(
        CapabilityDefinition(
            capability_id="common.emotional_support",
            domain="common",
            description="申请焦虑情绪支持与安抚。",
            handler=_handle_common_emotional_support,
            requires_student=False,
        )
    )
    registry.register(
        CapabilityDefinition(
            capability_id="common.clarify",
            domain="common",
            description="低置信度时请求用户澄清。",
            handler=_handle_common_clarify,
            requires_student=False,
        )
    )
    return registry


async def _handle_undergrad_profile_intake(ctx: CapabilityContext) -> CapabilityResult:
    assert ctx.student_id is not None
    raw = await handle_guided_intake(
        llm=ctx.llm,
        session=ctx.session,
        memory=ctx.memory,
        session_id=ctx.session_id,
        student_id=ctx.student_id,
        message=ctx.message,
    )
    intake_complete = "[INTAKE_COMPLETE]" in raw
    text = raw.replace("[INTAKE_COMPLETE]", "").strip()
    text, guided_payload = _extract_json_marker(text, "[GUIDED_OPTIONS]")

    artifacts: list[Any] = []
    if guided_payload is not None:
        if isinstance(guided_payload, dict):
            questions = guided_payload.get("questions", guided_payload)
        elif isinstance(guided_payload, list):
            questions = guided_payload
        else:
            questions = []
        parsed_questions: list[GuidedQuestion] = []
        if isinstance(questions, list):
            for question in questions:
                try:
                    parsed_questions.append(GuidedQuestion.model_validate(question))
                except ValidationError:
                    continue
        if parsed_questions:
            artifacts.append(GuidedIntakeArtifact(questions=parsed_questions))

    actions: list[AdvisorAction] = []
    if intake_complete:
        rec = await _run_recommendation(ctx)
        if rec.assistant_text:
            text = f"{text}\n\n{rec.assistant_text}".strip()
        artifacts.extend(rec.artifacts)
        actions.extend(rec.actions)
    return _capability_result(
        text=text,
        artifacts=artifacts,
        actions=actions,
        step_message="Updated undergrad intake profile and generated next guidance.",
    )


async def _handle_undergrad_school_recommend(ctx: CapabilityContext) -> CapabilityResult:
    assert ctx.student_id is not None
    return await _run_recommendation(ctx)


async def _handle_undergrad_school_query(ctx: CapabilityContext) -> CapabilityResult:
    assert ctx.student_id is not None
    query_result = await handle_school_query(
        llm=ctx.llm,
        session=ctx.session,
        memory=ctx.memory,
        session_id=ctx.session_id,
        student_id=ctx.student_id,
        message=ctx.message,
    )
    text = query_result.text
    artifact = InfoCardArtifact(
        title="School Query",
        summary=text[:300].strip() or "School information generated.",
        data={
            "query": ctx.message,
            "school_name": query_result.school_name,
            "extraction_source": query_result.extraction_source,
        },
    )
    return _capability_result(
        text=text,
        artifacts=[artifact],
        step_message="Answered school-level undergrad query.",
        llm_calls=query_result.llm_calls,
    )


async def _handle_undergrad_strategy_plan(ctx: CapabilityContext) -> CapabilityResult:
    assert ctx.student_id is not None
    text = await handle_strategy(
        llm=ctx.llm,
        session=ctx.session,
        memory=ctx.memory,
        session_id=ctx.session_id,
        student_id=ctx.student_id,
        message=ctx.message,
    )
    undergrad_ctx = await ctx.memory.get_context(ctx.session_id, domain="undergrad")
    strategy = undergrad_ctx.get("last_strategy") if isinstance(undergrad_ctx, dict) else None
    if not isinstance(strategy, dict):
        strategy = {"note": text}
    artifact = StrategyPlanArtifact(strategy=strategy)
    return _capability_result(
        text=text,
        artifacts=[artifact],
        step_message="Generated undergrad strategy plan.",
        llm_calls=1,
    )


async def _handle_offer_compare(ctx: CapabilityContext) -> CapabilityResult:
    assert ctx.student_id is not None
    return await _run_offer_comparison(ctx)


async def _handle_offer_decision(ctx: CapabilityContext) -> CapabilityResult:
    assert ctx.student_id is not None
    return await _run_offer_comparison(ctx)


async def _handle_offer_what_if(ctx: CapabilityContext) -> CapabilityResult:
    assert ctx.student_id is not None
    text = await handle_what_if(
        llm=ctx.llm,
        session=ctx.session,
        memory=ctx.memory,
        session_id=ctx.session_id,
        student_id=ctx.student_id,
        message=ctx.message,
    )
    offer_ctx = await ctx.memory.get_context(ctx.session_id, domain="offer")
    last_what_if = offer_ctx.get("last_what_if", {})
    interventions = last_what_if.get("interventions", {}) if isinstance(last_what_if, dict) else {}
    deltas = last_what_if.get("deltas", {}) if isinstance(last_what_if, dict) else {}
    text = (
        await get_output_polisher().polish_what_if_explanation(
            llm=ctx.llm,
            explanation=text,
            interventions=_coerce_float_map(interventions),
            deltas=_coerce_float_map(deltas),
            locale=ctx.locale,
        )
        or text
    )
    artifact = WhatIfResultArtifact(
        interventions=_coerce_float_map(interventions),
        deltas=_coerce_float_map(deltas),
        explanation=text,
    )
    return _capability_result(
        text=text,
        artifacts=[artifact],
        step_message="Completed offer what-if simulation.",
    )


async def _handle_common_general(ctx: CapabilityContext) -> CapabilityResult:
    is_portfolio = _is_portfolio_message(ctx.message)
    text = await ctx.llm.complete(
        [
            {
                "role": "system",
                "content": (
                    "You are ScholarPath advisor. Handle non-school requests with practical guidance. "
                    "If user asks for portfolio filling, give structured checklist and one concrete next step. "
                    "If user asks casual chat, keep it warm and concise. "
                    "Respond in the user's language."
                ),
            },
            {"role": "user", "content": ctx.message},
        ],
        temperature=0.6,
        max_tokens=512,
        caller="advisor.common.general",
    )
    artifacts: list[Any] = []
    actions: list[AdvisorAction] = []
    if is_portfolio:
        artifacts.append(
            InfoCardArtifact(
                title="Portfolio 填写清单",
                summary="先把素材补齐，再进入结构化画像填写。",
                data={
                    "sections": [
                        "学术成绩与标化",
                        "活动与领导力",
                        "科研/竞赛/项目",
                        "奖项与荣誉",
                        "推荐人候选",
                    ]
                },
            )
        )
        actions.append(
            AdvisorAction(
                action_id="common.start_portfolio",
                label="开始填写 Portfolio",
                payload={
                    "domain_hint": "undergrad",
                    "capability_hint": "undergrad.profile.intake",
                },
            )
        )
    return _capability_result(
        text=text,
        artifacts=artifacts,
        actions=actions,
        step_message="Responded to general user conversation.",
        llm_calls=1,
    )


async def _handle_common_emotional_support(ctx: CapabilityContext) -> CapabilityResult:
    text = await ctx.llm.complete(
        [
            {
                "role": "system",
                "content": (
                    "You are a warm admissions advisor. "
                    "Output should include: 1) acknowledge emotion, 2) one short grounding step, "
                    "3) one practical next action for today. "
                    "Keep tone calm and non-judgmental, respond in user's language."
                ),
            },
            {"role": "user", "content": ctx.message},
        ],
        temperature=0.55,
        max_tokens=512,
        caller="advisor.common.emotional_support",
    )
    actions = [
        AdvisorAction(
            action_id="support.switch_to_general",
            label="先轻聊再规划",
            payload={"capability_hint": "common.general"},
        ),
        AdvisorAction(
            action_id="route.clarify",
            label="澄清接下来重点",
            payload={"client_context": {"trigger": "route.clarify"}},
        ),
    ]
    artifact = InfoCardArtifact(
        title="2分钟稳定节奏",
        summary="先稳住情绪，再做一个最小行动。",
        data={
            "steps": [
                "30秒深呼吸（吸4秒/呼6秒）",
                "写下今天只做的一件事",
                "完成后再决定下一步",
            ]
        },
    )
    return _capability_result(
        text=text,
        artifacts=[artifact],
        actions=actions,
        step_message="Provided emotional support and next-step guidance.",
        llm_calls=1,
    )


async def _handle_common_clarify(ctx: CapabilityContext) -> CapabilityResult:
    text = (
        "我需要先确认你这轮的主要目标：本科择校、offer取舍、先聊聊、还是先做情绪支持？"
    )
    actions = [
        AdvisorAction(
            action_id="clarify.undergrad",
            label="本科择校",
            payload={"domain_hint": "undergrad"},
        ),
        AdvisorAction(
            action_id="clarify.offer",
            label="Offer取舍",
            payload={"domain_hint": "offer"},
        ),
        AdvisorAction(
            action_id="clarify.general",
            label="先聊聊",
            payload={"capability_hint": "common.general"},
        ),
        AdvisorAction(
            action_id="clarify.support",
            label="情绪支持",
            payload={"capability_hint": "common.emotional_support"},
        ),
    ]
    return _capability_result(
        text=text,
        actions=actions,
        step_message="Asked user to clarify priority and route.",
    )


async def _run_recommendation(ctx: CapabilityContext) -> CapabilityResult:
    assert ctx.student_id is not None
    raw = await handle_recommendation(
        llm=ctx.llm,
        session=ctx.session,
        memory=ctx.memory,
        session_id=ctx.session_id,
        student_id=ctx.student_id,
        message=ctx.message,
    )
    text, payload = _extract_json_marker(raw, "[RECOMMENDATION]")
    artifacts: list[Any] = []
    actions: list[AdvisorAction] = []
    if payload is not None:
        try:
            recommendation_data = RecommendationData.model_validate(payload)
            recommendation_data = await get_output_polisher().polish_school_recommendation(
                llm=ctx.llm,
                data=recommendation_data,
                locale=ctx.locale,
            )
            artifacts.append(SchoolRecommendationArtifact(data=recommendation_data))
            if recommendation_data.ed_recommendation:
                actions.append(
                    AdvisorAction(
                        action_id="recommendation.ed_focus",
                        label=f"查看ED建议：{recommendation_data.ed_recommendation}",
                        payload={"school_name": recommendation_data.ed_recommendation},
                    )
                )
        except ValidationError as exc:
            log_fallback(
                logger=logger,
                component="advisor.adapters",
                stage="run_recommendation.parse_artifact",
                reason="invalid_recommendation_payload",
                fallback_used=True,
                exc=exc,
                extra={"session_id": ctx.session_id},
            )
    return _capability_result(
        text=text,
        artifacts=artifacts,
        actions=actions,
        step_message="Generated undergrad school recommendation.",
    )


async def _run_offer_comparison(ctx: CapabilityContext) -> CapabilityResult:
    assert ctx.student_id is not None
    try:
        comparison = await compare_offers(ctx.session, ctx.llm, ctx.student_id)
    except Exception as exc:
        log_fallback(
            logger=logger,
            component="advisor.adapters",
            stage="run_offer_comparison.compare_offers",
            reason="compare_offers_failed",
            fallback_used=True,
            exc=exc,
            extra={"session_id": ctx.session_id, "student_id": str(ctx.student_id)},
        )
        fallback_text = await handle_offer_decision(
            llm=ctx.llm,
            session=ctx.session,
            memory=ctx.memory,
            session_id=ctx.session_id,
            student_id=ctx.student_id,
            message=ctx.message,
        )
        return _capability_result(
            text=fallback_text,
            step_message="Fallback offer decision response generated.",
        )

    offers = comparison.get("offers", [])
    recommendation = comparison.get("recommendation") or "已完成 offer 对比。"
    recommendation = (
        await get_output_polisher().polish_offer_recommendation(
            llm=ctx.llm,
            recommendation=recommendation,
            offers=offers if isinstance(offers, list) else [],
            locale=ctx.locale,
        )
        or recommendation
    )
    artifact = OfferComparisonArtifact(
        offers=offers,
        comparison_matrix=comparison.get("comparison_matrix", {}),
        recommendation=recommendation,
    )
    actions: list[AdvisorAction] = []
    for offer in comparison.get("offers", [])[:4]:
        school = offer.get("school")
        if school:
            actions.append(
                AdvisorAction(
                    action_id="offer.inspect",
                    label=f"查看 {school}",
                    payload={"school_name": school},
                )
            )
    return _capability_result(
        text=str(recommendation),
        artifacts=[artifact],
        actions=actions,
        step_message="Compared offers and generated decision guidance.",
    )


def _extract_json_marker(text: str, marker: str) -> tuple[str, Any]:
    if marker not in text:
        return text.strip(), None
    body, payload_raw = text.split(marker, 1)
    try:
        payload = json.loads(payload_raw.strip())
    except json.JSONDecodeError:
        payload = None
    return body.strip(), payload


def _coerce_float_map(data: Any) -> dict[str, float]:
    if not isinstance(data, dict):
        return {}
    out: dict[str, float] = {}
    for key, val in data.items():
        try:
            out[str(key)] = float(val)
        except (TypeError, ValueError):
            continue
    return out


def _capability_result(
    *,
    text: str,
    artifacts: list[Any] | None = None,
    actions: list[AdvisorAction] | None = None,
    step_message: str | None = None,
    llm_calls: int = 0,
    metadata: dict[str, Any] | None = None,
) -> CapabilityResult:
    summary = step_message or _compact_text(text) or "Capability completed."
    merged_meta: dict[str, Any] = dict(metadata or {})
    if llm_calls > 0:
        merged_meta["llm_calls"] = int(merged_meta.get("llm_calls", 0) or 0) + llm_calls
    return CapabilityResult(
        assistant_text=text,
        artifacts=artifacts or [],
        actions=actions or [],
        metadata=merged_meta,
        step_summary={"message": summary},
    )


def _compact_text(text: str, max_len: int = 160) -> str:
    stripped = " ".join(text.split())
    if len(stripped) <= max_len:
        return stripped
    return stripped[: max_len - 3] + "..."


def _is_portfolio_message(message: str) -> bool:
    zh_keywords = ("portfolio", "画像", "简历", "活动", "经历", "奖项", "科研", "文书素材")
    en_keywords = ("portfolio", "resume", "activities", "awards", "profile")
    lowered = message.lower()
    if any(keyword in message for keyword in zh_keywords):
        return True
    if any(keyword in lowered for keyword in en_keywords):
        return True
    return False
