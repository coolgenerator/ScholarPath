"""Main chat agent -- routes intents to handlers and manages conversation flow."""

from __future__ import annotations

import logging
import uuid
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.chat.handlers import (
    handle_guided_intake,
    handle_offer_decision,
    handle_profile_intake,
    handle_recommendation,
    handle_school_query,
    handle_strategy,
    handle_what_if,
)
from scholarpath.chat.intents import IntentType, classify_intent
from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient

logger = logging.getLogger(__name__)


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
        try:
            # Step 1: Load memory
            history = await self._memory.get_history(session_id, limit=10)
            context = await self._memory.get_context(session_id)
            context["recent_messages"] = "\n".join(
                f"{m['role']}: {m['content']}" for m in history[-5:]
            )

            # Step 2: Classify intent
            intent, confidence = await classify_intent(self._llm, message, context)
            logger.info(
                "session=%s intent=%s confidence=%.2f",
                session_id,
                intent.value,
                confidence,
            )

            # Save user message
            await self._memory.save_message(session_id, "user", message)

            # Step 3: Route to handler
            response = await self._route(
                intent, session_id, student_id, message, context
            )

            # Step 4: Save assistant response
            await self._memory.save_message(session_id, "assistant", response)

            return response

        except Exception:
            logger.exception("ChatAgent.process failed for session %s", session_id)
            return (
                "I'm sorry, I ran into an unexpected issue. "
                "Could you try rephrasing your question? "
                "If this keeps happening, please let us know."
            )

    async def _route(
        self,
        intent: IntentType,
        session_id: str,
        student_id: uuid.UUID | None,
        message: str,
        context: dict[str, Any],
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
                self._llm, self._session, self._memory, student_id, message  # type: ignore[arg-type]
            )
            # If guided intake completed, automatically trigger recommendations
            if response.startswith("[INTAKE_COMPLETE]"):
                clean_msg = response.replace("[INTAKE_COMPLETE]", "")
                try:
                    rec_response = await handle_recommendation(
                        self._llm, self._session, self._memory, student_id, message  # type: ignore[arg-type]
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
                self._llm, self._session, self._memory, student_id, message  # type: ignore[arg-type]
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
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a warm, empathetic college admissions advisor. "
                    "The student is feeling stressed or anxious about the "
                    "admissions process. Acknowledge their feelings, provide "
                    "encouragement, and gently offer to help with something "
                    "concrete. Keep it concise (2-3 paragraphs). "
                    "Respond in the same language the student uses."
                ),
            },
            {"role": "user", "content": message},
        ]
        return await self._llm.complete(messages, temperature=0.7, max_tokens=512, caller="chat.emotional_support")

    async def _handle_general(self, message: str) -> str:
        """Handle greetings, off-topic questions, and meta-queries."""
        messages = [
            {
                "role": "system",
                "content": (
                    "You are ScholarPath, a college admissions advisor chatbot. "
                    "Respond helpfully to general questions. If the message is "
                    "a greeting, introduce yourself briefly and ask how you can "
                    "help with college admissions. If off-topic, gently redirect "
                    "to admissions topics. Respond in the user's language."
                ),
            },
            {"role": "user", "content": message},
        ]
        return await self._llm.complete(messages, temperature=0.7, max_tokens=512, caller="chat.general")
