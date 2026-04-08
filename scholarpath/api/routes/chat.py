"""Chat WebSocket route + REST history endpoint."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from scholarpath.api.deps import RedisDep
from scholarpath.api.models.chat import ChatMessage, ChatResponse, RouteTurnRequest

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["chat"])


# ── REST: load session history ──────────────────────────────────────

class HistoryEntry(BaseModel):
    role: str
    content: str


@router.get("/history/{session_id}", response_model=list[HistoryEntry])
async def get_chat_history(session_id: str, redis: RedisDep) -> list[dict]:
    """Return stored conversation history for a session from Redis."""
    from scholarpath.chat.memory import ChatMemory

    memory = ChatMemory(redis)
    history = await memory.get_history(session_id, limit=50)
    return history


@router.post("/route-turn", response_model=ChatResponse)
async def route_turn(payload: RouteTurnRequest) -> ChatResponse:
    """Execute one turn with optional external route_plan + skill_id."""
    try:
        from scholarpath.chat.agent import ChatAgent
        from scholarpath.llm.client import get_llm_client
        from scholarpath.db.session import async_session_factory
        from scholarpath.db.redis import redis_pool

        llm = get_llm_client()
        student_id = None
        if payload.student_id:
            import uuid as _uuid
            student_id = _uuid.UUID(payload.student_id)

        async with async_session_factory() as session:
            agent = ChatAgent(llm=llm, session=session, redis=redis_pool)
            result = await agent.process_turn(
                session_id=payload.session_id,
                student_id=student_id,
                message=payload.message,
                route_plan=payload.route_plan.model_dump() if payload.route_plan else None,
                skill_id=payload.skill_id,
            )
            response_text = str(result.get("response_text") or "")
            response_text, recommendation, guided_questions = _parse_response_markers(response_text)
            await session.commit()
    except Exception:
        logger.exception("route-turn failed")
        return ChatResponse(
            content="I ran into an error processing your request. Please try again.",
            intent="error",
            suggested_actions=None,
            recommendation=None,
            guided_questions=None,
            route_meta=None,
            execution_digest=None,
        )

    return ChatResponse(
        content=response_text,
        intent=str(result.get("intent") or "general"),
        suggested_actions=None,
        recommendation=recommendation,
        guided_questions=guided_questions,
        route_meta=result.get("route_meta"),
        execution_digest=result.get("execution_digest"),
    )


# ── WebSocket: real-time chat ───────────────────────────────────────

@router.websocket("/chat/{session_id}")
async def chat_websocket(websocket: WebSocket, session_id: str) -> None:
    """WebSocket endpoint for conversational chat."""
    await websocket.accept()
    logger.info("Chat session %s connected", session_id)

    # Lazily initialise dependencies
    try:
        from scholarpath.chat.agent import ChatAgent
        from scholarpath.llm.client import get_llm_client
        from scholarpath.db.session import async_session_factory
        from scholarpath.db.redis import redis_pool

        llm = get_llm_client()
    except ImportError:
        llm = None
        redis_pool = None
        async_session_factory = None

    try:
        while True:
            raw = await websocket.receive_text()

            try:
                data = json.loads(raw)
                student_id_str = data.pop("student_id", None)
                student_id = None
                if student_id_str:
                    import uuid as _uuid
                    try:
                        student_id = _uuid.UUID(student_id_str)
                    except (ValueError, TypeError):
                        pass
                message = ChatMessage(**data)
            except (json.JSONDecodeError, Exception) as exc:
                await websocket.send_json(
                    {"content": f"Invalid message format: {exc}", "intent": "error", "suggested_actions": None}
                )
                continue

            if llm is not None and async_session_factory is not None:
                try:
                    async with async_session_factory() as session:
                        # Auto-create/update ChatSession record
                        if student_id:
                            await _ensure_chat_session(session, student_id, session_id, message.content)

                        agent = ChatAgent(llm=llm, session=session, redis=redis_pool)
                        response_text = await agent.process(
                            session_id=session_id,
                            student_id=student_id,
                            message=message.content,
                        )

                        # Update session metadata
                        if student_id:
                            await _update_session_preview(session, session_id, message.content)

                        await session.commit()

                    # Parse structured data markers from response
                    logger.info("Response length=%d, has_RECOMMENDATION=%s, has_GUIDED=%s",
                        len(response_text),
                        "[RECOMMENDATION]" in response_text,
                        "[GUIDED_OPTIONS]" in response_text,
                    )
                    response_text, recommendation, guided_questions = _parse_response_markers(response_text)

                    resp = ChatResponse(
                        content=response_text,
                        intent="general",
                        suggested_actions=None,
                        guided_questions=guided_questions,
                        recommendation=recommendation,
                        route_meta=None,
                        execution_digest=None,
                    )
                    await websocket.send_json(resp.model_dump(mode="json"))
                except Exception:
                    logger.exception("Chat agent error in session %s", session_id)
                    await websocket.send_json(
                        {"content": "I ran into an error processing your request. Please try again.",
                         "intent": "error", "suggested_actions": None}
                    )
            else:
                await websocket.send_json(
                    {"content": "Chat agent is not configured.", "intent": "system", "suggested_actions": None}
                )

    except WebSocketDisconnect:
        logger.info("Chat session %s disconnected", session_id)


def _parse_response_markers(response_text: str) -> tuple[str, dict | None, list | None]:
    guided_questions = None
    recommendation = None
    text = response_text

    if "[GUIDED_OPTIONS]" in text:
        text_part, json_part = text.split("[GUIDED_OPTIONS]", 1)
        text = text_part.strip()
        try:
            options_data = json.loads(json_part.strip())
            guided_questions = options_data.get("questions", [])
        except Exception:
            guided_questions = None

    if "[RECOMMENDATION]" in text:
        text_part, json_part = text.split("[RECOMMENDATION]", 1)
        text = text_part.strip()
        try:
            recommendation = json.loads(json_part.strip())
        except Exception:
            recommendation = None
    return text, recommendation, guided_questions


async def _ensure_chat_session(session, student_id, session_id: str, first_message: str) -> None:
    """Create a ChatSession record if it doesn't exist yet."""
    from sqlalchemy import select
    from scholarpath.db.models.chat_session import ChatSession

    stmt = select(ChatSession).where(ChatSession.session_id == session_id)
    result = await session.execute(stmt)
    if result.scalars().first() is None:
        # Use first message (truncated) as title
        title = first_message[:60].strip()
        if len(first_message) > 60:
            title += "..."
        chat_session = ChatSession(
            student_id=student_id,
            session_id=session_id,
            title=title,
            preview=first_message[:200],
            message_count=1,
        )
        session.add(chat_session)
        await session.flush()


async def _update_session_preview(session, session_id: str, message: str) -> None:
    """Increment message count and update preview."""
    from sqlalchemy import select
    from scholarpath.db.models.chat_session import ChatSession

    stmt = select(ChatSession).where(ChatSession.session_id == session_id)
    result = await session.execute(stmt)
    chat_session = result.scalars().first()
    if chat_session:
        chat_session.message_count = (chat_session.message_count or 0) + 1
        chat_session.preview = message[:200]
