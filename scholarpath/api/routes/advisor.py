"""Advisor v1 routes: unified domain+capability stream endpoint."""

from __future__ import annotations

import json
import logging
import uuid

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import delete, func, select

from scholarpath.advisor.adapters import build_default_registry
from scholarpath.advisor.contracts import AdvisorHistoryEntry, AdvisorRequest, AdvisorResponse
from scholarpath.advisor.orchestrator import AdvisorOrchestrator
from scholarpath.api.deps import RedisDep
from scholarpath.chat.memory import ChatMemory
from scholarpath.db.models.advisor_memory import (
    AdvisorMemoryItem,
    AdvisorMessage,
    AdvisorMessageChunk,
)
from scholarpath.db.models.chat_session import ChatSession

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/advisor", tags=["advisor"])


@router.get("/v1/sessions/{session_id}/history", response_model=list[AdvisorHistoryEntry])
async def get_advisor_history(session_id: str, redis: RedisDep) -> list[dict]:
    """Return stored conversation history for one advisor session.

    DB is the source of truth for editable timelines. Redis fallback is
    retained for legacy sessions that do not have advisor_messages rows.
    """
    from scholarpath.db.session import async_session_factory

    async with async_session_factory() as session:
        db_history = await _load_db_history_entries(session=session, session_id=session_id)
        if db_history:
            return db_history

    memory = ChatMemory(redis)
    legacy = await memory.get_history(session_id, limit=50)
    return [
        AdvisorHistoryEntry(
            role=item.get("role", "assistant"),
            content=item.get("content", ""),
            editable=False,
            edited=False,
        ).model_dump(mode="json")
        for item in legacy
    ]


@router.websocket("/v1/sessions/{session_id}/stream")
async def advisor_stream(websocket: WebSocket, session_id: str) -> None:
    """Advisor v1 streaming endpoint."""
    await websocket.accept()
    logger.info("Advisor session %s connected", session_id)

    try:
        from scholarpath.db.redis import redis_pool
        from scholarpath.db.session import async_session_factory
        from scholarpath.llm.client import get_llm_client

        llm = get_llm_client()
        registry = build_default_registry()
    except Exception:
        logger.exception("Advisor dependency initialization failed")
        await websocket.send_json(
            _error_response(
                turn_id=str(uuid.uuid4()),
                session_id=session_id,
                message="Advisor service is not configured.",
                code="DEPENDENCY_UNAVAILABLE",
                capability="common.general",
                guard_result="pass",
                guard_reason="none",
                retriable=True,
            )
        )
        await websocket.close()
        return

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise ValueError("payload must be an object")
                data["session_id"] = session_id
                request = AdvisorRequest.model_validate(data)
            except Exception as exc:
                await websocket.send_json(
                    _error_response(
                        turn_id=str(uuid.uuid4()),
                        session_id=session_id,
                        message=f"Invalid request payload: {exc}",
                        code="INVALID_INPUT",
                        capability="common.general",
                        guard_result="invalid_input",
                        guard_reason="invalid_input",
                        retriable=False,
                    )
                )
                continue

            async with async_session_factory() as session:
                if request.edit is not None:
                    rewritten, edit_error = await _apply_edit_overwrite(
                        session=session,
                        redis=redis_pool,
                        request=request,
                    )
                    if edit_error is not None:
                        await websocket.send_json(
                            _error_response(
                                turn_id=request.turn_id or str(uuid.uuid4()),
                                session_id=session_id,
                                message=edit_error,
                                code="INVALID_INPUT",
                                capability="common.general",
                                guard_result="invalid_input",
                                guard_reason="invalid_input",
                                retriable=False,
                            )
                        )
                        await session.rollback()
                        continue
                    request = rewritten

                if request.student_id:
                    try:
                        student_uuid = uuid.UUID(request.student_id)
                    except ValueError:
                        student_uuid = None
                    if student_uuid is not None:
                        await _ensure_chat_session(
                            session=session,
                            student_id=student_uuid,
                            session_id=session_id,
                            first_message=request.message,
                        )

                orchestrator = AdvisorOrchestrator(
                    llm=llm,
                    session=session,
                    redis=redis_pool,
                    registry=registry,
                )
                response = await orchestrator.process(request)

                if request.student_id:
                    await _update_session_preview(session, session_id, request.message)
                await session.commit()

            await websocket.send_json(response.model_dump(mode="json"))
    except WebSocketDisconnect:
        logger.info("Advisor session %s disconnected", session_id)


def _error_response(
    *,
    turn_id: str,
    session_id: str,
    message: str,
    code: str,
    capability: str = "common.general",
    guard_result: str = "pass",
    guard_reason: str = "none",
    retriable: bool = False,
) -> dict:
    recover_actions = [
        {
            "action_id": "route.clarify",
            "label": "澄清当前任务",
            "payload": {"client_context": {"trigger": "route.clarify"}},
        }
    ]
    response = AdvisorResponse(
        turn_id=turn_id,
        domain="common",
        capability=capability,  # type: ignore[arg-type]
        assistant_text=message,
        artifacts=[],
        actions=recover_actions,
        done=[],
        pending=[],
        next_actions=recover_actions,
        route_meta={
            "domain_confidence": 0.0,
            "capability_confidence": 0.0,
            "router_model": "n/a",
            "latency_ms": 0,
            "fallback_used": True,
            "guard_result": guard_result,
            "guard_reason": guard_reason,
        },
        error={
            "code": code,
            "message": message,
            "retriable": retriable,
            "detail": {"session_id": session_id},
        },
    )
    return response.model_dump(mode="json")


async def _ensure_chat_session(session, student_id, session_id: str, first_message: str) -> None:
    """Create a ChatSession record if missing."""
    from sqlalchemy import select

    from scholarpath.db.models.chat_session import ChatSession

    stmt = select(ChatSession).where(ChatSession.session_id == session_id)
    result = await session.execute(stmt)
    if result.scalars().first() is None:
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
    """Recompute message_count from DB active timeline and refresh preview."""
    stmt = select(ChatSession).where(ChatSession.session_id == session_id)
    result = await session.execute(stmt)
    chat_session = result.scalars().first()
    if chat_session:
        count_stmt = select(func.count(AdvisorMessage.id)).where(
            AdvisorMessage.session_id == session_id
        )
        count = await session.scalar(count_stmt)
        chat_session.message_count = int(count or 0)
        chat_session.preview = message[:200]


async def _load_db_history_entries(*, session, session_id: str) -> list[dict]:
    stmt = (
        select(AdvisorMessage)
        .where(AdvisorMessage.session_id == session_id)
        .order_by(AdvisorMessage.created_at.asc(), AdvisorMessage.id.asc())
        .limit(500)
    )
    rows = (await session.execute(stmt)).scalars().all()
    if not rows:
        return []

    output: list[dict] = []
    for row in rows:
        edited = False
        if row.updated_at is not None and row.created_at is not None:
            edited = row.updated_at > row.created_at
        output.append(
            AdvisorHistoryEntry(
                role=row.role,
                content=row.content,
                message_id=str(row.id),
                turn_id=row.turn_id,
                created_at=row.created_at.isoformat() if row.created_at else None,
                editable=(row.role == "user"),
                edited=edited,
            ).model_dump(mode="json")
        )
    return output


async def _apply_edit_overwrite(
    *,
    session,
    redis,
    request: AdvisorRequest,
) -> tuple[AdvisorRequest, str | None]:
    edit_payload = request.edit
    if edit_payload is None:
        return request, None
    if edit_payload.mode != "overwrite":
        return request, "Unsupported edit.mode; only overwrite is allowed."

    target_turn_id = edit_payload.target_turn_id.strip()
    if not target_turn_id:
        return request, "edit.target_turn_id is required."

    stmt = (
        select(AdvisorMessage)
        .where(AdvisorMessage.session_id == request.session_id)
        .order_by(AdvisorMessage.created_at.asc(), AdvisorMessage.id.asc())
    )
    timeline = (await session.execute(stmt)).scalars().all()
    if not timeline:
        return request, "No editable timeline found for this session."

    target_idx: int | None = None
    for idx, row in enumerate(timeline):
        if row.turn_id == target_turn_id and row.role == "user":
            target_idx = idx
            break
    if target_idx is None:
        return request, "edit.target_turn_id does not point to an editable user message."

    target_row = timeline[target_idx]
    target_row.content = request.message
    target_row.ingestion_status = "pending"

    rows_before_target = timeline[:target_idx]
    rows_after_target = timeline[target_idx + 1 :]
    delete_turn_ids = {row.turn_id for row in rows_after_target}
    delete_turn_ids.add(target_turn_id)

    if rows_after_target:
        for row in rows_after_target:
            await session.delete(row)
        await session.flush()

    await session.execute(
        delete(AdvisorMemoryItem).where(
            AdvisorMemoryItem.session_id == request.session_id,
            AdvisorMemoryItem.source_turn_id.in_(delete_turn_ids),
        )
    )
    await session.execute(
        delete(AdvisorMessageChunk).where(
            AdvisorMessageChunk.session_id == request.session_id,
            AdvisorMessageChunk.turn_id.in_(delete_turn_ids),
        )
    )

    memory = ChatMemory(redis)
    await memory.clear(request.session_id)
    for row in rows_before_target:
        await memory.save_message(request.session_id, row.role, row.content)

    rewritten = request.model_copy(
        update={
            "turn_id": target_turn_id,
            "edit": None,
        }
    )
    return rewritten, None
