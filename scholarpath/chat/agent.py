"""Main chat agent -- routes intents to handlers and manages conversation flow."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.chat.handlers import (
    handle_guided_intake,
    handle_offer_decision,
    handle_recommendation,
    handle_school_query,
    handle_strategy,
    handle_what_if,
)
from scholarpath.chat.intents import IntentType, classify_intent
from scholarpath.language import (
    detect_response_language,
    language_instruction,
    select_localized_text,
)
from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient

logger = logging.getLogger(__name__)
_RECOMMENDATION_MARKER = "[RECOMMENDATION]"


class ChatAgent:
    """Conversational agent that routes user messages to specialised handlers.

    Parameters
    ----------
    llm:
        LLM client instance for completions.
    session:
        SQLAlchemy async session for database access.
    redis:
        Redis connection for conversation memory.
    """

    def __init__(
        self,
        llm: LLMClient,
        session: AsyncSession,
        redis: aioredis.Redis,
    ) -> None:
        self._llm = llm
        self._session = session
        self._memory = ChatMemory(redis)

    async def process(
        self,
        session_id: str,
        student_id: uuid.UUID | None,
        message: str,
    ) -> str:
        """Process a user message and return a response.

        Pipeline
        --------
        1. Load conversation memory (history + context).
        2. Classify the user's intent via LLM.
        3. Route to the appropriate handler.
        4. Save the user message and assistant response to memory.
        5. Return the response text.

        Error handling returns a friendly message rather than propagating
        exceptions to the caller.

        Parameters
        ----------
        session_id:
            Unique conversation session identifier.
        student_id:
            UUID of the authenticated student, or ``None`` for anonymous.
        message:
            The user's message text.

        Returns
        -------
        str
            The assistant's response text.
        """
        result = await self.process_turn(
            session_id=session_id,
            student_id=student_id,
            message=message,
            route_plan=None,
            skill_id=None,
        )
        return result["response_text"]

    async def process_turn(
        self,
        *,
        session_id: str,
        student_id: uuid.UUID | None,
        message: str,
        route_plan: dict[str, Any] | None = None,
        skill_id: str | None = None,
    ) -> dict[str, Any]:
        """Process one turn and return response plus execution metadata."""
        try:
            history = await self._memory.get_history(session_id, limit=10)
            context = await self._memory.get_context(session_id)
            context["recent_messages"] = "\n".join(
                f"{m['role']}: {m['content']}" for m in history[-5:]
            )

            intent, confidence, route_source = await self._resolve_intent(
                message=message,
                context=context,
                route_plan=route_plan,
            )
            logger.info(
                "session=%s intent=%s confidence=%.2f route_source=%s",
                session_id,
                intent.value,
                confidence,
                route_source,
            )

            await self._memory.save_message(session_id, "user", message)

            route_bucket = None
            if route_plan:
                modifiers = route_plan.get("modifiers") or []
                if isinstance(modifiers, list):
                    route_bucket = next(
                        (str(item) for item in modifiers if isinstance(item, str) and item.endswith("_first")),
                        None,
                    )

            response = await self._route(
                intent,
                session_id,
                student_id,
                message,
                context,
                route_plan=route_plan,
                skill_id=skill_id,
                route_bucket=route_bucket,
            )
            executed_intent = intent

            execution_digest: dict[str, Any] = {
                "required_output_missing": False,
                "required_capability_missing": False,
                "forced_retry_count": 0,
                "cap_retry_count": 0,
                "cap_degraded": False,
                "reason_code": None,
                "failure_reason_code": None,
                "needs_input": [],
                "next_steps": [],
            }

            required_capabilities: list[str] = []
            if route_plan and isinstance(route_plan.get("required_capabilities"), list):
                required_capabilities = [
                    self._normalize_capability(str(item))
                    for item in route_plan["required_capabilities"]
                    if isinstance(item, str) and str(item).strip()
                ]
            required_outputs = []
            if route_plan and isinstance(route_plan.get("required_outputs"), list):
                required_outputs = [
                    str(item)
                    for item in route_plan["required_outputs"]
                    if isinstance(item, str) and str(item).strip()
                ]

            executed_capability = self._intent_to_capability(executed_intent)
            capability_missing = bool(
                required_capabilities
                and executed_capability not in required_capabilities
            )
            if capability_missing:
                execution_digest["required_capability_missing"] = True
                execution_digest["forced_retry_count"] = 1
                execution_digest["cap_retry_count"] = 1
                forced_intent = self._capability_to_intent(required_capabilities[0])
                if forced_intent is not None:
                    retried = await self._route(
                        forced_intent,
                        session_id,
                        student_id,
                        message,
                        context,
                        route_plan=route_plan,
                        skill_id=skill_id,
                        route_bucket=route_bucket,
                    )
                    response = retried
                    executed_intent = forced_intent
                    executed_capability = self._intent_to_capability(executed_intent)
                capability_missing = bool(
                    required_capabilities
                    and executed_capability not in required_capabilities
                )

            missing_required_outputs = self._missing_required_outputs(
                response_text=response,
                required_outputs=required_outputs,
            )
            if missing_required_outputs:
                execution_digest["required_output_missing"] = True
                if execution_digest["forced_retry_count"] == 0:
                    execution_digest["forced_retry_count"] = 1
                if execution_digest["cap_retry_count"] == 0:
                    execution_digest["cap_retry_count"] = 1
                retried = await self._route(
                    executed_intent,
                    session_id,
                    student_id,
                    message,
                    context,
                    route_plan=route_plan,
                    skill_id=skill_id,
                    route_bucket=route_bucket,
                )
                post_retry_missing = self._missing_required_outputs(
                    response_text=retried,
                    required_outputs=required_outputs,
                )
                if not post_retry_missing:
                    response = retried
                else:
                    execution_digest["cap_degraded"] = True
                    execution_digest["reason_code"] = "required_output_missing_after_retry"
                    execution_digest["failure_reason_code"] = "required_output_missing_after_retry"
                    execution_digest["needs_input"] = [
                        "budget_usd",
                        "intended_majors",
                        "preferred_region",
                    ]
                    execution_digest["next_steps"] = [
                        "Share budget range and intended major for a stricter shortlist.",
                        "Re-run recommendation with explicit scenario (budget/risk/major/geo/roi).",
                    ]
                    if "recommendation_payload" in post_retry_missing:
                        response = self._build_degraded_recommendation_response(
                            response_language=detect_response_language(message),
                            reason_code=str(execution_digest["reason_code"]),
                            needs_input=execution_digest["needs_input"],
                            next_steps=execution_digest["next_steps"],
                        )
                    else:
                        response = self._build_degraded_text_response(
                            response_language=detect_response_language(message),
                            reason_code=str(execution_digest["reason_code"]),
                            next_steps=execution_digest["next_steps"],
                        )
            elif capability_missing:
                execution_digest["cap_degraded"] = True
                execution_digest["reason_code"] = "required_capability_missing_after_retry"
                execution_digest["failure_reason_code"] = "required_capability_missing_after_retry"
                execution_digest["next_steps"] = [
                    "Retry with an explicit route_plan primary_task and required_capabilities.",
                ]
                response = self._build_degraded_text_response(
                    response_language=detect_response_language(message),
                    reason_code=str(execution_digest["reason_code"]),
                    next_steps=execution_digest["next_steps"],
                )

            await self._memory.save_message(session_id, "assistant", response)

            route_meta = {
                "route_source": route_source,
                "primary_task": self._route_plan_primary_task(route_plan),
                "skill_id": skill_id,
                "executed_capability": executed_capability,
            }
            return {
                "response_text": response,
                "intent": executed_intent.value,
                "route_meta": route_meta,
                "execution_digest": execution_digest,
            }
        except Exception:
            logger.exception("ChatAgent.process_turn failed for session %s", session_id)
            fallback = (
                "I'm sorry, I ran into an unexpected issue. "
                "Could you try rephrasing your question? "
                "If this keeps happening, please let us know."
            )
            return {
                "response_text": fallback,
                "intent": IntentType.GENERAL.value,
                "route_meta": {
                    "route_source": "error_fallback",
                    "primary_task": self._route_plan_primary_task(route_plan),
                    "skill_id": skill_id,
                },
                "execution_digest": {
                    "required_output_missing": False,
                    "required_capability_missing": False,
                    "forced_retry_count": 0,
                    "cap_retry_count": 0,
                    "cap_degraded": True,
                    "reason_code": "chat_agent_exception",
                    "failure_reason_code": "chat_agent_exception",
                    "needs_input": [],
                    "next_steps": [],
                },
            }

    async def _route(
        self,
        intent: IntentType,
        session_id: str,
        student_id: uuid.UUID | None,
        message: str,
        context: dict[str, Any],
        *,
        route_plan: dict[str, Any] | None = None,
        skill_id: str | None = None,
        route_bucket: str | None = None,
    ) -> str:
        """Route to the correct handler based on classified intent."""
        # Most handlers require a student_id
        if student_id is None and intent not in (
            IntentType.GENERAL,
            IntentType.EMOTIONAL_SUPPORT,
        ):
            return await self._handle_general(message)

        if intent == IntentType.PROFILE_INTAKE:
            response = await handle_guided_intake(
                self._llm, self._session, self._memory, session_id, student_id, message  # type: ignore[arg-type]
            )
            # If guided intake completed, automatically trigger recommendations
            if response.startswith("[INTAKE_COMPLETE]"):
                clean_msg = response.replace("[INTAKE_COMPLETE]", "")
                try:
                    rec_response = await handle_recommendation(
                        self._llm,
                        self._session,
                        self._memory,
                        session_id,
                        student_id,
                        message,  # type: ignore[arg-type]
                        skill_id=skill_id,
                        route_bucket=route_bucket,
                    )
                    return f"{clean_msg}\n\n{rec_response}"
                except Exception:
                    logger.warning(
                        "Auto-recommendation after intake failed for %s",
                        student_id,
                        exc_info=True,
                    )
                    return (
                        f"{clean_msg}\n\n"
                        "I had trouble generating recommendations automatically. "
                        "Type 'recommend' or '推荐学校' and I'll try again!"
                    )
            return response

        if intent == IntentType.RECOMMENDATION:
            if student_id is None:
                return await self._handle_general(message)
            return await handle_recommendation(
                self._llm,
                self._session,
                self._memory,
                session_id,
                student_id,
                message,  # type: ignore[arg-type]
                skill_id=skill_id,
                route_bucket=route_bucket,
            )

        if intent == IntentType.SCHOOL_QUERY:
            return await handle_school_query(
                self._llm, self._session, self._memory, student_id, message  # type: ignore[arg-type]
            )

        if intent == IntentType.STRATEGY_ADVICE:
            return await handle_strategy(
                self._llm, self._session, self._memory, student_id, message  # type: ignore[arg-type]
            )

        if intent == IntentType.OFFER_DECISION:
            return await handle_offer_decision(
                self._llm, self._session, self._memory, student_id, message  # type: ignore[arg-type]
            )

        if intent == IntentType.WHAT_IF:
            return await handle_what_if(
                self._llm, self._session, self._memory, student_id, message  # type: ignore[arg-type]
            )

        if intent == IntentType.EMOTIONAL_SUPPORT:
            return await self._handle_emotional_support(message)

        # IntentType.GENERAL or unrecognised
        return await self._handle_general(message)

    async def _handle_emotional_support(self, message: str) -> str:
        """Respond empathetically to stress or anxiety about admissions."""
        response_lang = detect_response_language(message)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a warm, empathetic college admissions advisor. "
                    "The student is feeling stressed or anxious about the "
                    "admissions process. Acknowledge their feelings, provide "
                    "encouragement, and gently offer to help with something "
                    "concrete. Keep it concise (2-3 paragraphs). "
                    f"{language_instruction(response_lang)}"
                ),
            },
            {"role": "user", "content": message},
        ]
        return await self._llm.complete(messages, temperature=0.7, max_tokens=512, caller="chat.emotional_support")

    async def _handle_general(self, message: str) -> str:
        """Handle greetings, off-topic questions, and meta-queries."""
        response_lang = detect_response_language(message)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are ScholarPath, a college admissions advisor chatbot. "
                    "Respond helpfully to general questions. If the message is "
                    "a greeting, introduce yourself briefly and ask how you can "
                    "help with college admissions. If off-topic, gently redirect "
                    "to admissions topics. "
                    f"{language_instruction(response_lang)}"
                ),
            },
            {"role": "user", "content": message},
        ]
        return await self._llm.complete(messages, temperature=0.7, max_tokens=512, caller="chat.general")

    async def _resolve_intent(
        self,
        *,
        message: str,
        context: dict[str, Any],
        route_plan: dict[str, Any] | None,
    ) -> tuple[IntentType, float, str]:
        if route_plan and bool(route_plan.get("route_lock", True)):
            primary = self._route_plan_primary_task(route_plan)
            mapped = self._intent_from_primary_task(primary)
            if mapped is not None:
                return mapped, 1.0, "route_plan"

        intent, confidence = await classify_intent(self._llm, message, context)
        return intent, confidence, "intent_classifier"

    @staticmethod
    def _route_plan_primary_task(route_plan: dict[str, Any] | None) -> str | None:
        if not route_plan:
            return None
        primary = route_plan.get("primary_task")
        if primary is None:
            return None
        return str(primary).strip() or None

    @staticmethod
    def _intent_from_primary_task(primary_task: str | None) -> IntentType | None:
        mapping = {
            "chat": IntentType.GENERAL,
            "recommendation": IntentType.RECOMMENDATION,
            "strategy": IntentType.STRATEGY_ADVICE,
            "what_if": IntentType.WHAT_IF,
            "offer_compare": IntentType.OFFER_DECISION,
            "intake": IntentType.PROFILE_INTAKE,
        }
        if not primary_task:
            return None
        return mapping.get(primary_task)

    @staticmethod
    def _intent_to_capability(intent: IntentType) -> str:
        mapping = {
            IntentType.GENERAL: "chat",
            IntentType.RECOMMENDATION: "recommendation",
            IntentType.STRATEGY_ADVICE: "strategy",
            IntentType.WHAT_IF: "what_if",
            IntentType.OFFER_DECISION: "offer_compare",
            IntentType.PROFILE_INTAKE: "intake",
            IntentType.SCHOOL_QUERY: "school_query",
            IntentType.EMOTIONAL_SUPPORT: "chat",
        }
        return mapping.get(intent, "chat")

    @staticmethod
    def _normalize_capability(capability: str) -> str:
        raw = (capability or "").strip().lower()
        aliases = {
            "recommendation_subagent": "recommendation",
            "memory_followup": "chat",
        }
        return aliases.get(raw, raw)

    @staticmethod
    def _capability_to_intent(capability: str | None) -> IntentType | None:
        mapping = {
            "chat": IntentType.GENERAL,
            "recommendation": IntentType.RECOMMENDATION,
            "strategy": IntentType.STRATEGY_ADVICE,
            "what_if": IntentType.WHAT_IF,
            "offer_compare": IntentType.OFFER_DECISION,
            "intake": IntentType.PROFILE_INTAKE,
            "school_query": IntentType.SCHOOL_QUERY,
        }
        if not capability:
            return None
        return mapping.get(ChatAgent._normalize_capability(capability))

    def _missing_required_outputs(
        self,
        *,
        response_text: str,
        required_outputs: list[str],
    ) -> list[str]:
        missing: list[str] = []
        for output_name in required_outputs:
            if not self._response_has_required_output(
                response_text=response_text,
                output_name=output_name,
            ):
                missing.append(output_name)
        return missing

    @staticmethod
    def _response_has_required_output(*, response_text: str, output_name: str) -> bool:
        normalized = (output_name or "").strip()
        if not normalized:
            return True
        if normalized == "recommendation_payload":
            return ChatAgent._has_recommendation_payload(response_text)
        if normalized == "guided_questions":
            return "[GUIDED_OPTIONS]" in response_text
        if normalized == "text":
            return bool((response_text or "").strip())
        return False

    @staticmethod
    def _has_recommendation_payload(response_text: str) -> bool:
        if _RECOMMENDATION_MARKER not in response_text:
            return False
        _, json_part = response_text.split(_RECOMMENDATION_MARKER, 1)
        payload_raw = (json_part or "").strip()
        if not payload_raw:
            return False
        try:
            payload = json.loads(payload_raw)
        except Exception:
            return False
        return isinstance(payload, dict) and isinstance(payload.get("schools"), list)

    @staticmethod
    def _build_degraded_recommendation_response(
        *,
        response_language: str,
        reason_code: str,
        needs_input: list[str],
        next_steps: list[str],
    ) -> str:
        summary = select_localized_text(
            "我暂时无法稳定生成结构化推荐，先给你一个可执行补救步骤。",
            "I couldn't produce a stable structured recommendation just now. Here is a recovery path.",
            response_language,
            mixed=(
                "我暂时无法稳定生成结构化推荐，先给你一个可执行补救步骤。\n"
                "I couldn't produce a stable structured recommendation just now."
            ),
        )
        payload = {
            "narrative": "",
            "schools": [],
            "ed_recommendation": None,
            "ea_recommendations": [],
            "strategy_summary": "degraded_fallback",
            "prefilter_meta": None,
            "skill_id_used": None,
            "degraded": True,
            "reason_code": reason_code,
            "needs_input": needs_input,
            "next_steps": next_steps,
        }
        return f"{summary}\n{_RECOMMENDATION_MARKER}{json.dumps(payload, ensure_ascii=False)}"

    @staticmethod
    def _build_degraded_text_response(
        *,
        response_language: str,
        reason_code: str,
        next_steps: list[str],
    ) -> str:
        summary = select_localized_text(
            "这轮我没能完成你要求的结构化输出。我先给你可执行的下一步。",
            "I couldn't complete the required structured output for this turn. Here are actionable next steps.",
            response_language,
            mixed=(
                "这轮我没能完成你要求的结构化输出。\n"
                "I couldn't complete the required structured output for this turn."
            ),
        )
        steps = "\n".join(f"- {step}" for step in next_steps if step)
        if steps:
            return f"{summary}\n{steps}\n(reason_code={reason_code})"
        return f"{summary}\n(reason_code={reason_code})"
