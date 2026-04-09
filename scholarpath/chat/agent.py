"""Advisor chat runtime entrypoint using the ReAct tool-use agent."""

from __future__ import annotations

import logging
import uuid
from typing import Awaitable, Callable

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.api.models.chat import RoutePlan, TurnEvent, TurnResult
from scholarpath.chat.memory import ChatMemory
from scholarpath.chat.react_advisor import ReactAdvisor
from scholarpath.llm.client import LLMClient

logger = logging.getLogger(__name__)


class ChatAgent:
    """Conversational agent backed by a ReAct async-generator loop.

    The advisor yields ``TurnEvent`` objects for progress and a final
    ``TurnResult``.  ``run_turn`` forwards each event through the
    ``emit_event`` callback so callers (WebSocket handler, HTTP endpoint)
    can stream them to the client in real time.
    """

    def __init__(
        self,
        llm: LLMClient,
        session: AsyncSession,
        redis: aioredis.Redis | None = None,
        memory: ChatMemory | None = None,
    ) -> None:
        self._llm = llm
        self._session = session
        if memory is not None:
            self._memory = memory
        else:
            if redis is None:
                raise ValueError("Either redis or memory must be provided")
            self._memory = ChatMemory(redis)
        self._advisor = ReactAdvisor(
            llm=llm,
            session=session,
            memory=self._memory,
        )

    async def run_turn(
        self,
        *,
        session_id: str,
        student_id: uuid.UUID | None,
        message: str,
        route_plan: RoutePlan | None = None,
        emit_event: Callable[[TurnEvent], Awaitable[None]],
    ) -> TurnResult:
        """Execute one user turn, streaming events via *emit_event*."""
        try:
            await self._memory.save_message(session_id, "user", message)

            result: TurnResult | None = None

            async for item in self._advisor.run_turn(
                session_id=session_id,
                student_id=student_id,
                message=message,
            ):
                if isinstance(item, TurnEvent):
                    await emit_event(item)
                elif isinstance(item, TurnResult):
                    result = item

            if result is None:
                result = TurnResult(
                    trace_id=str(uuid.uuid4()),
                    status="error",
                    content="No result produced. Please try again.",
                    blocks=[],
                    actions=[],
                    usage={},
                )

            await self._memory.save_assistant_turn(
                session_id,
                content=result.content,
                status=result.status,
                trace_id=result.trace_id,
                blocks=[item.model_dump(mode="json") for item in result.blocks],
                actions=result.actions,
                execution_digest=result.execution_digest,
            )
            return result

        except Exception:
            logger.exception("ChatAgent.run_turn failed for session %s", session_id)
            try:
                await self._session.rollback()
            except Exception:
                logger.warning(
                    "ChatAgent rollback after failure failed for session %s",
                    session_id,
                    exc_info=True,
                )
            fallback = TurnResult(
                trace_id=str(uuid.uuid4()),
                status="error",
                content=(
                    "I ran into an unexpected issue. "
                    "Please try rephrasing your request."
                ),
                blocks=[],
                actions=[],
                usage={},
            )
            await self._memory.save_assistant_turn(
                session_id,
                content=fallback.content,
                status=fallback.status,
                trace_id=fallback.trace_id,
                blocks=[],
                actions=[],
                execution_digest=None,
            )
            return fallback

    async def process(
        self,
        session_id: str,
        student_id: uuid.UUID | None,
        message: str,
    ) -> str:
        """Compatibility wrapper returning plain text only."""

        async def _noop(_: TurnEvent) -> None:
            return None

        result = await self.run_turn(
            session_id=session_id,
            student_id=student_id,
            message=message,
            route_plan=None,
            emit_event=_noop,
        )
        return result.content
