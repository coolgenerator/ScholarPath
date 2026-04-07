"""Chat WebSocket route + structured history endpoint."""

from __future__ import annotations

import json
import logging
import time
import uuid as _uuid

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect

from scholarpath.api.deps import AppLLMDep, RedisDep
from scholarpath.api.models.chat import (
    ChatHistoryEntry,
    ChatMessage,
    RouteTurnRequest,
    SessionTraceListResponse,
    TurnEvent,
    TurnResult,
    TurnTraceResponse,
)
from scholarpath.chat.trace import MAX_SESSION_TRACES, TurnTraceRecorder

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["chat"])

_TURN_LOCK_TTL_SECONDS = 120
_LOCK_RELEASE_SCRIPT = (
    "if redis.call('GET', KEYS[1]) == ARGV[1] then "
    "return redis.call('DEL', KEYS[1]) "
    "else return 0 end"
)


@router.get("/history/{session_id}", response_model=list[ChatHistoryEntry])
async def get_chat_history(session_id: str, redis: RedisDep) -> list[ChatHistoryEntry]:
    """Return stored structured conversation history for a session from Redis."""
    from scholarpath.chat.memory import ChatMemory

    memory = ChatMemory(redis)
    history = await memory.get_history(session_id, limit=50)
    parsed: list[ChatHistoryEntry] = []
    for item in history:
        try:
            parsed.append(ChatHistoryEntry.model_validate(item))
        except Exception:
            # Keep endpoint resilient when old entries do not match the new schema.
            parsed.append(
                ChatHistoryEntry(
                    role=str(item.get("role", "assistant")),
                    content=str(item.get("content", "")),
                )
            )
    return parsed


@router.get(
    "/traces/{trace_id}",
    response_model=TurnTraceResponse,
    response_model_exclude_none=True,
)
async def get_turn_trace(
    trace_id: str,
    redis: RedisDep,
    view: str = Query(default="compact", pattern="^(compact|full)$"),
) -> TurnTraceResponse:
    """Return persisted step trace for one assistant turn."""
    recorder = TurnTraceRecorder(redis)
    payload = await recorder.get_trace(trace_id, view=view)
    if payload is None:
        raise HTTPException(status_code=404, detail="Trace not found")
    return TurnTraceResponse.model_validate(payload)


@router.get(
    "/traces/session/{session_id}",
    response_model=SessionTraceListResponse,
    response_model_exclude_none=True,
)
async def list_session_traces(
    session_id: str,
    redis: RedisDep,
    limit: int = Query(default=50, ge=1, le=MAX_SESSION_TRACES),
    view: str = Query(default="compact", pattern="^(compact|full)$"),
) -> SessionTraceListResponse:
    """Return recent turn trace summaries for one chat session."""
    recorder = TurnTraceRecorder(redis)
    payload = await recorder.list_session_traces(session_id=session_id, limit=limit, view=view)
    return SessionTraceListResponse.model_validate(payload)


@router.post("/route-turn", response_model=TurnResult)
async def route_turn_http(
    request: RouteTurnRequest,
    llm: AppLLMDep,
    redis: RedisDep,
) -> TurnResult:
    """HTTP turn execution endpoint with optional route plan contract."""
    try:
        from scholarpath.chat.agent import ChatAgent
        from scholarpath.chat.memory import ChatMemory
        from scholarpath.db.session import async_session_factory
    except ImportError as exc:  # pragma: no cover - runtime safety
        raise HTTPException(status_code=500, detail=f"Chat runtime unavailable: {exc}") from exc

    session_id = str(request.session_id)
    student_id = request.student_id
    message = str(request.message or "")
    route_plan = request.route_plan
    trace_recorder = TurnTraceRecorder(redis)

    lock_wait_started = time.perf_counter()
    lock_scope = "student" if student_id is not None else "session"
    lock = await _acquire_turn_lock(
        redis=redis,
        student_id=student_id,
        session_id=session_id,
    )
    lock_wait_ms = int((time.perf_counter() - lock_wait_started) * 1000)
    if lock is None:
        lock_trace_id = str(_uuid.uuid4())
        lock_usage = {
            "rejected_by_lock": True,
            "lock_scope": lock_scope,
            "lock_ttl_seconds": _TURN_LOCK_TTL_SECONDS,
            "lock_wait_ms": lock_wait_ms,
            "tool_steps_used": 0,
            "wave_count": 0,
            "duration_ms": 0,
            "guardrail_triggered": False,
        }
        try:
            await trace_recorder.record_lock_rejection(
                trace_id=lock_trace_id,
                session_id=session_id,
                student_id=str(student_id) if student_id is not None else None,
                usage=lock_usage,
            )
        except Exception:
            logger.warning("Failed to persist lock rejection trace", exc_info=True)
        return TurnResult(
            trace_id=lock_trace_id,
            status="error",
            content="Another turn is running for this student. Please retry in a moment.",
            blocks=[],
            actions=[],
            usage=lock_usage,
        )

    logger.info(
        "HTTP route-turn lock acquired scope=%s session_id=%s student_id=%s wait_ms=%d",
        lock_scope,
        session_id,
        str(student_id) if student_id is not None else "anonymous",
        lock_wait_ms,
    )

    turn_trace_id: str | None = None
    try:
        async with async_session_factory() as session:
            base_memory = ChatMemory(redis)
            journal = base_memory.begin_turn_journal()
            try:
                if student_id:
                    await _ensure_chat_session(
                        session,
                        student_id=student_id,
                        session_id=session_id,
                        first_message=message,
                    )

                agent = ChatAgent(llm=llm, session=session, memory=journal)

                async def _emit(event: TurnEvent) -> None:
                    nonlocal turn_trace_id
                    turn_trace_id = event.trace_id
                    try:
                        await trace_recorder.append_event(
                            trace_id=event.trace_id,
                            session_id=session_id,
                            student_id=str(student_id) if student_id is not None else None,
                            event=event,
                        )
                    except Exception:
                        logger.warning("Failed to append turn trace event", exc_info=True)

                result = await agent.run_turn(
                    session_id=session_id,
                    student_id=student_id,
                    message=message,
                    route_plan=route_plan,
                    emit_event=_emit,
                )
                turn_trace_id = result.trace_id
                result.usage["rejected_by_lock"] = False
                result.usage["lock_scope"] = lock_scope
                result.usage["lock_ttl_seconds"] = _TURN_LOCK_TTL_SECONDS
                result.usage["lock_wait_ms"] = lock_wait_ms
                await _finalize_turn_transaction(
                    session=session,
                    journal=journal,
                    result_status=result.status,
                    session_id=session_id,
                    student_id=student_id,
                    message=message,
                )
                try:
                    await trace_recorder.finalize_result(
                        trace_id=result.trace_id,
                        session_id=session_id,
                        student_id=str(student_id) if student_id is not None else None,
                        result=result,
                    )
                except Exception:
                    logger.warning("Failed to finalize turn trace", exc_info=True)
                return result
            except Exception:
                await session.rollback()
                await journal.discard()
                raise
    except Exception:
        logger.exception("HTTP route-turn failed for session %s", session_id)
        fallback_trace_id = turn_trace_id or str(_uuid.uuid4())
        fallback = TurnResult(
            trace_id=fallback_trace_id,
            status="error",
            content="I ran into an error processing your request. Please try again.",
            blocks=[],
            actions=[],
            usage={
                "rejected_by_lock": False,
                "lock_scope": lock_scope,
                "lock_ttl_seconds": _TURN_LOCK_TTL_SECONDS,
                "lock_wait_ms": lock_wait_ms,
                "tool_steps_used": 0,
                "wave_count": 0,
                "duration_ms": 0,
            },
        )
        try:
            await trace_recorder.finalize_result(
                trace_id=fallback.trace_id,
                session_id=session_id,
                student_id=str(student_id) if student_id is not None else None,
                result=fallback,
            )
        except Exception:
            logger.warning("Failed to persist fallback error trace", exc_info=True)
        return fallback
    finally:
        await _release_turn_lock(redis, lock_key=lock["key"], token=lock["token"])


@router.websocket("/chat/{session_id}")
async def chat_websocket(
    websocket: WebSocket,
    session_id: str,
    llm: AppLLMDep,
) -> None:
    """WebSocket endpoint for conversational chat (turn.event + turn.result protocol)."""
    await websocket.accept()
    logger.info("Chat session %s connected", session_id)

    try:
        from scholarpath.chat.agent import ChatAgent
        from scholarpath.db.redis import redis_pool
        from scholarpath.db.session import async_session_factory
    except ImportError:
        redis_pool = None
        async_session_factory = None
    trace_recorder = TurnTraceRecorder(redis_pool)

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
                student_id = None
                student_id_raw = data.pop("student_id", None)
                if student_id_raw:
                    try:
                        student_id = _uuid.UUID(str(student_id_raw))
                    except (ValueError, TypeError):
                        student_id = None
                message = ChatMessage(**data)
            except Exception as exc:
                await websocket.send_json(
                    {
                        "type": "turn.result",
                        "trace_id": str(_uuid.uuid4()),
                        "status": "error",
                        "content": f"Invalid message format: {exc}",
                        "blocks": [],
                        "actions": [],
                        "usage": {},
                    }
                )
                continue

            if async_session_factory is None:
                await websocket.send_json(
                    {
                        "type": "turn.result",
                        "trace_id": str(_uuid.uuid4()),
                        "status": "error",
                        "content": "Chat agent is not configured.",
                        "blocks": [],
                        "actions": [],
                        "usage": {},
                    }
                )
                continue

            lock_wait_started = time.perf_counter()
            lock_scope = "student" if student_id is not None else "session"
            lock = await _acquire_turn_lock(
                redis=redis_pool,
                student_id=student_id,
                session_id=session_id,
            )
            lock_wait_ms = int((time.perf_counter() - lock_wait_started) * 1000)
            if lock is None:
                logger.info(
                    "Turn rejected by lock scope=%s session_id=%s student_id=%s wait_ms=%d",
                    lock_scope,
                    session_id,
                    str(student_id) if student_id is not None else "anonymous",
                    lock_wait_ms,
                )
                lock_trace_id = str(_uuid.uuid4())
                lock_usage = {
                    "rejected_by_lock": True,
                    "lock_scope": lock_scope,
                    "lock_ttl_seconds": _TURN_LOCK_TTL_SECONDS,
                    "lock_wait_ms": lock_wait_ms,
                    "tool_steps_used": 0,
                    "wave_count": 0,
                    "duration_ms": 0,
                    "guardrail_triggered": False,
                }
                try:
                    await trace_recorder.record_lock_rejection(
                        trace_id=lock_trace_id,
                        session_id=session_id,
                        student_id=str(student_id) if student_id is not None else None,
                        usage=lock_usage,
                    )
                except Exception:
                    logger.warning("Failed to persist lock rejection trace", exc_info=True)
                lock_result = TurnResult(
                    trace_id=lock_trace_id,
                    status="error",
                    content="Another turn is running for this student. Please retry in a moment.",
                    blocks=[],
                    actions=[],
                    usage=lock_usage,
                )
                await websocket.send_json(lock_result.model_dump(mode="json"))
                continue

            logger.info(
                "Turn lock acquired scope=%s session_id=%s student_id=%s wait_ms=%d",
                lock_scope,
                session_id,
                str(student_id) if student_id is not None else "anonymous",
                lock_wait_ms,
            )
            turn_trace_id: str | None = None
            try:
                async with async_session_factory() as session:
                    from scholarpath.chat.memory import ChatMemory

                    base_memory = ChatMemory(redis_pool)
                    journal = base_memory.begin_turn_journal()
                    try:
                        if student_id:
                            await _ensure_chat_session(
                                session,
                                student_id=student_id,
                                session_id=session_id,
                                first_message=message.content,
                            )

                        agent = ChatAgent(llm=llm, session=session, memory=journal)

                        async def _emit(event: TurnEvent) -> None:
                            nonlocal turn_trace_id
                            turn_trace_id = event.trace_id
                            payload = event.model_dump(mode="json")
                            if event.event == "turn_completed":
                                data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
                                data["rejected_by_lock"] = False
                                data["lock_scope"] = lock_scope
                                data["lock_wait_ms"] = lock_wait_ms
                                data["lock_ttl_seconds"] = _TURN_LOCK_TTL_SECONDS
                                payload["data"] = data
                            try:
                                await trace_recorder.append_event(
                                    trace_id=event.trace_id,
                                    session_id=session_id,
                                    student_id=str(student_id) if student_id is not None else None,
                                    event=event,
                                )
                            except Exception:
                                logger.warning("Failed to append turn trace event", exc_info=True)
                            await websocket.send_json(payload)

                        result = await agent.run_turn(
                            session_id=session_id,
                            student_id=student_id,
                            message=message.content,
                            emit_event=_emit,
                        )
                        turn_trace_id = result.trace_id
                        result.usage["rejected_by_lock"] = False
                        result.usage["lock_scope"] = lock_scope
                        result.usage["lock_ttl_seconds"] = _TURN_LOCK_TTL_SECONDS
                        result.usage["lock_wait_ms"] = lock_wait_ms
                        await _finalize_turn_transaction(
                            session=session,
                            journal=journal,
                            result_status=result.status,
                            session_id=session_id,
                            student_id=student_id,
                            message=message.content,
                        )
                        try:
                            await trace_recorder.finalize_result(
                                trace_id=result.trace_id,
                                session_id=session_id,
                                student_id=str(student_id) if student_id is not None else None,
                                result=result,
                            )
                        except Exception:
                            logger.warning("Failed to finalize turn trace", exc_info=True)
                    except Exception:
                        await session.rollback()
                        await journal.discard()
                        raise

                await websocket.send_json(result.model_dump(mode="json"))
                logger.info(
                    "Turn finished session_id=%s student_id=%s status=%s trace_id=%s lock_scope=%s wait_ms=%d",
                    session_id,
                    str(student_id) if student_id is not None else "anonymous",
                    result.status,
                    result.trace_id,
                    lock_scope,
                    lock_wait_ms,
                )
            except Exception:
                logger.exception("Chat turn failed in session %s", session_id)
                fallback_trace_id = turn_trace_id or str(_uuid.uuid4())
                fallback = TurnResult(
                    trace_id=fallback_trace_id,
                    status="error",
                    content="I ran into an error processing your request. Please try again.",
                    blocks=[],
                    actions=[],
                    usage={
                        "rejected_by_lock": False,
                        "lock_scope": lock_scope,
                        "lock_ttl_seconds": _TURN_LOCK_TTL_SECONDS,
                        "lock_wait_ms": lock_wait_ms,
                        "tool_steps_used": 0,
                        "wave_count": 0,
                        "duration_ms": 0,
                    },
                )
                try:
                    await trace_recorder.finalize_result(
                        trace_id=fallback.trace_id,
                        session_id=session_id,
                        student_id=str(student_id) if student_id is not None else None,
                        result=fallback,
                    )
                except Exception:
                    logger.warning("Failed to persist fallback error trace", exc_info=True)
                await websocket.send_json(fallback.model_dump(mode="json"))
            finally:
                await _release_turn_lock(redis_pool, lock_key=lock["key"], token=lock["token"])
    except WebSocketDisconnect:
        logger.info("Chat session %s disconnected", session_id)


async def _ensure_chat_session(session, student_id, session_id: str, first_message: str) -> None:
    """Create a ChatSession record if it doesn't exist yet."""
    from sqlalchemy import select

    from scholarpath.db.models.chat_session import ChatSession

    stmt = select(ChatSession).where(ChatSession.session_id == session_id)
    result = await session.execute(stmt)
    if result.scalars().first() is not None:
        return

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


async def _finalize_turn_transaction(
    *,
    session,
    journal,
    result_status: str,
    session_id: str,
    student_id,
    message: str,
) -> None:
    if result_status != "ok":
        await session.rollback()
        await journal.discard()
        return
    if student_id:
        await _update_session_preview(
            session,
            session_id=session_id,
            message=message,
        )
    try:
        await session.commit()
    except Exception:
        await journal.discard()
        raise
    try:
        await journal.commit()
    except Exception:
        await journal.discard()
        raise


async def _acquire_turn_lock(
    *,
    redis,
    student_id: _uuid.UUID | None,
    session_id: str,
) -> dict[str, str] | None:
    if redis is None:
        return {"key": "", "token": ""}
    scope = "student" if student_id is not None else "session"
    entity = str(student_id) if student_id is not None else session_id
    lock_key = f"scholarpath:chat:student_turn_lock:{entity}"
    token = str(_uuid.uuid4())
    acquired = await redis.set(lock_key, token, ex=_TURN_LOCK_TTL_SECONDS, nx=True)
    if not acquired:
        logger.info(
            "Turn lock conflict: scope=%s entity=%s session_id=%s",
            scope,
            entity,
            session_id,
        )
        return None
    return {"key": lock_key, "token": token}


async def _release_turn_lock(redis, *, lock_key: str, token: str) -> None:
    if redis is None or not lock_key:
        return
    eval_fn = getattr(redis, "eval", None)
    if callable(eval_fn):
        try:
            await eval_fn(_LOCK_RELEASE_SCRIPT, 1, lock_key, token)
            return
        except Exception:
            logger.warning("Turn lock release via Lua failed for %s", lock_key, exc_info=True)
    current = await redis.get(lock_key)
    if isinstance(current, bytes):
        current = current.decode("utf-8", errors="ignore")
    if current == token:
        await redis.delete(lock_key)
