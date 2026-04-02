"""Advisor layered memory ingestion, retrieval, and context assembly."""

from __future__ import annotations

import ast
import asyncio
import logging
import math
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

from sqlalchemy import and_, desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.advisor.contracts import MemoryIngestEvent, MemoryItem, RetrievedChunk
from scholarpath.chat.memory import ChatMemory
from scholarpath.config import settings
from scholarpath.db.models.advisor_memory import (
    AdvisorMemoryItem,
    AdvisorMessage,
    AdvisorMessageChunk,
)
from scholarpath.llm.embeddings import EmbeddingService, get_embedding_service

logger = logging.getLogger(__name__)

_MEMORY_ITEM_ACTIVE = "active"
_MEMORY_ITEM_PENDING_CONFLICT = "pending_conflict"
_MEMORY_ITEM_SUPERSEDED = "superseded"

_ALLOWED_DOMAINS = {"undergrad", "offer", "graduate", "summer", "common"}
_MAX_CHUNK_CHARS = 450
_CHUNK_OVERLAP_CHARS = 90
_CHUNK_TOKEN_LIMIT = 220
_RAG_TIMEOUT_SECONDS = 0.12
_RAG_CANDIDATE_LIMIT = 60


@dataclass(slots=True)
class ContextMetrics:
    """Observability metrics emitted by context assembly."""

    context_tokens: int = 0
    memory_hits: int = 0
    rag_hits: int = 0
    rag_latency_ms: int = 0
    memory_degraded: bool = False
    memory_conflicts: int = 0


class ContextAssembler:
    """Builds stage-specific prompt contexts from layered memory."""

    def __init__(
        self,
        *,
        session: AsyncSession,
        memory: ChatMemory,
        embedding_service: EmbeddingService | None = None,
    ) -> None:
        self._session = session
        self._memory = memory
        self._embedding_service = embedding_service

    async def assemble(
        self,
        *,
        stage: str,
        session_id: str,
        student_id: uuid.UUID | None,
        message: str,
        domain: str | None,
    ) -> tuple[dict[str, Any], ContextMetrics]:
        """Assemble context for routing or execution stage."""
        is_route_stage = stage == "route"
        budget = 900 if stage == "route" else 1800
        history_keep = 3 if is_route_stage else 8
        rag_limit = 4 if is_route_stage else 6
        memory_limit = 0 if is_route_stage else 8
        metrics = ContextMetrics()

        history = await self._memory.get_history(session_id, limit=max(history_keep + 8, 16))
        history_msgs = history[-history_keep:]
        history_lines = [_history_line(m) for m in history_msgs if m.get("content")]

        undergrad_ctx: dict[str, Any] = {}
        offer_ctx: dict[str, Any] = {}
        if not is_route_stage:
            undergrad_ctx = await self._memory.get_context(session_id, domain="undergrad")
            offer_ctx = await self._memory.get_context(session_id, domain="offer")
        common_ctx = await self._memory.get_context(session_id, domain="common")

        memory_items: list[AdvisorMemoryItem] = []
        memory_conflicts: list[AdvisorMemoryItem] = []
        if not is_route_stage:
            try:
                loaded_items = await load_memory_items(
                    session=self._session,
                    session_id=session_id,
                    student_id=student_id,
                    domain=domain if domain in _ALLOWED_DOMAINS else None,
                    limit=40,
                )
                memory_items = loaded_items["active"]
                memory_conflicts = loaded_items["conflicts"]
            except Exception:
                logger.warning("Memory item retrieval failed; falling back to Redis-only", exc_info=True)
                metrics.memory_degraded = True

        selected_memory_items = _select_memory_items(memory_items, stage=stage, limit=memory_limit)
        memory_lines = [_memory_line(item) for item in selected_memory_items]
        conflict_lines = [_memory_line(item) for item in memory_conflicts[:4]]

        rag_chunks: list[RetrievedChunk] = []
        rag_lines: list[str] = []
        if not is_route_stage:
            try:
                rag_chunks, rag_meta = await retrieve_message_chunks(
                    session=self._session,
                    query=message,
                    session_id=session_id,
                    student_id=student_id,
                    domain=domain if domain in _ALLOWED_DOMAINS else None,
                    limit=rag_limit,
                    embedding_service=self._embedding_service,
                )
                metrics.rag_latency_ms = rag_meta["latency_ms"]
                metrics.memory_degraded = metrics.memory_degraded or rag_meta["memory_degraded"]
            except Exception:
                logger.warning("RAG retrieval failed; falling back to history-only", exc_info=True)
                metrics.memory_degraded = True

        for chunk in rag_chunks:
            rag_lines.append(_trim_to_tokens(chunk.text, _CHUNK_TOKEN_LIMIT))

        pending_lines = _pending_lines_from_context(common_ctx)
        policy_lines = _policy_lines(stage=stage)

        while True:
            assembled = _compose_prompt_context(
                policy_lines=policy_lines,
                history_lines=history_lines,
                memory_lines=memory_lines,
                rag_lines=rag_lines,
                pending_lines=pending_lines,
                conflict_lines=conflict_lines,
            )
            tokens = estimate_tokens(assembled)
            if tokens <= budget:
                metrics.context_tokens = tokens
                break
            if rag_lines:
                rag_lines.pop()
                continue
            if len(history_lines) > 1:
                history_lines.pop(0)
                continue
            metrics.context_tokens = tokens
            break

        metrics.memory_hits = len(selected_memory_items)
        metrics.rag_hits = len(rag_lines)
        metrics.memory_conflicts = len(memory_conflicts)

        context = {
            "recent_messages": "\n".join(history_lines),
            "route_prompt_context": assembled,
            "undergrad": undergrad_ctx,
            "offer": offer_ctx,
            "common": common_ctx,
            "memory_items": [_memory_item_dump(item) for item in selected_memory_items],
            "memory_conflicts": [_memory_item_dump(item) for item in memory_conflicts[:8]],
            "retrieved_chunks": [chunk.model_dump(mode="json") for chunk in rag_chunks[: len(rag_lines)]],
        }
        return context, metrics


async def persist_turn_message(
    *,
    session: AsyncSession,
    event: MemoryIngestEvent,
) -> AdvisorMessage:
    """Persist one advisor message with idempotency on turn+role."""
    stmt = select(AdvisorMessage).where(
        AdvisorMessage.session_id == event.session_id,
        AdvisorMessage.turn_id == event.turn_id,
        AdvisorMessage.role == event.role,
    )
    existing = (await session.execute(stmt)).scalars().first()
    student_uuid = _parse_uuid(event.student_id)

    if existing is not None:
        existing.student_id = student_uuid
        existing.domain = event.domain
        existing.capability = event.capability
        existing.content = event.content
        existing.artifacts_json = event.artifacts or []
        existing.done_json = [d.model_dump(mode="json") for d in event.done]
        existing.pending_json = [p.model_dump(mode="json") for p in event.pending]
        existing.next_actions_json = [a.model_dump(mode="json") for a in event.next_actions]
        existing.ingestion_status = "pending"
        await session.flush()
        return existing

    row = AdvisorMessage(
        turn_id=event.turn_id,
        session_id=event.session_id,
        student_id=student_uuid,
        role=event.role,
        domain=event.domain,
        capability=event.capability,
        content=event.content,
        artifacts_json=event.artifacts or [],
        done_json=[d.model_dump(mode="json") for d in event.done],
        pending_json=[p.model_dump(mode="json") for p in event.pending],
        next_actions_json=[a.model_dump(mode="json") for a in event.next_actions],
        ingestion_status="pending",
    )
    session.add(row)
    await session.flush()
    return row


async def ingest_memory_event(
    *,
    session: AsyncSession,
    event: MemoryIngestEvent | dict[str, Any] | None = None,
    message_id: str | None = None,
    embedding_service: EmbeddingService | None = None,
) -> dict[str, Any]:
    """Ingest one memory event into message chunks + structured memory."""
    if message_id is not None:
        parsed_message_id = _parse_uuid(message_id)
        if parsed_message_id is None:
            return {
                "skipped": True,
                "reason": "invalid_message_id",
                "message_id": message_id,
            }
        message_row = await session.get(AdvisorMessage, parsed_message_id)
        if message_row is None:
            return {
                "skipped": True,
                "reason": "message_not_found",
                "message_id": message_id,
            }
        parsed = _event_from_message_row(message_row)
    else:
        if event is None:
            raise ValueError("event or message_id is required")
        parsed = event if isinstance(event, MemoryIngestEvent) else MemoryIngestEvent.model_validate(event)
        message_row = await persist_turn_message(session=session, event=parsed)

    existing_idx = await _load_existing_chunk_indexes(session=session, message_id=message_row.id)

    chunks = chunk_text(parsed.content, max_chars=_MAX_CHUNK_CHARS, overlap_chars=_CHUNK_OVERLAP_CHARS)
    new_chunks = [(idx, text) for idx, text in enumerate(chunks) if idx not in existing_idx]

    embedded_vectors: list[list[float] | None] = [None for _ in new_chunks]
    memory_degraded = False
    if new_chunks:
        try:
            embedded_vectors = await _embed_chunks(
                [text for _, text in new_chunks],
                embedding_service=embedding_service,
            )
        except Exception:
            logger.warning("Chunk embedding failed; storing chunks without vectors", exc_info=True)
            memory_degraded = True
            embedded_vectors = [None for _ in new_chunks]

    for pos, (chunk_index, text) in enumerate(new_chunks):
        session.add(
            AdvisorMessageChunk(
                message_id=message_row.id,
                turn_id=parsed.turn_id,
                session_id=parsed.session_id,
                student_id=_parse_uuid(parsed.student_id),
                domain=parsed.domain,
                chunk_index=chunk_index,
                content=text,
                token_count=estimate_tokens(text),
                score_meta={"source": "advisor_message"},
                embedding=embedded_vectors[pos],
            )
        )

    memory_candidates = _build_memory_candidates(parsed)
    writes = await _upsert_memory_candidates(session=session, candidates=memory_candidates)

    message_row.ingestion_status = "ready"
    await session.flush()
    return {
        "message_id": str(message_row.id),
        "chunk_count": len(chunks),
        "new_chunk_count": len(new_chunks),
        "memory_inserted": writes["active_inserted"],
        "memory_conflicts": writes["pending_conflicts"],
        "memory_degraded": memory_degraded,
    }


def _event_from_message_row(row: AdvisorMessage) -> MemoryIngestEvent:
    done_raw = row.done_json if isinstance(row.done_json, list) else []
    pending_raw = row.pending_json if isinstance(row.pending_json, list) else []
    actions_raw = row.next_actions_json if isinstance(row.next_actions_json, list) else []
    done = [item for item in done_raw if isinstance(item, dict)]
    pending = [item for item in pending_raw if isinstance(item, dict)]
    actions = [item for item in actions_raw if isinstance(item, dict)]
    return MemoryIngestEvent.model_validate(
        {
            "turn_id": row.turn_id,
            "session_id": row.session_id,
            "student_id": str(row.student_id) if row.student_id is not None else None,
            "domain": row.domain,
            "capability": row.capability,
            "role": row.role,
            "content": row.content,
            "artifacts": row.artifacts_json if isinstance(row.artifacts_json, list) else [],
            "done": done,
            "pending": pending,
            "next_actions": actions,
        }
    )


async def load_memory_items(
    *,
    session: AsyncSession,
    session_id: str,
    student_id: uuid.UUID | None,
    domain: str | None,
    limit: int = 20,
) -> dict[str, list[AdvisorMemoryItem]]:
    """Load active and conflict memory items with layered filters."""
    now = datetime.now(tz=UTC)
    filters = [
        AdvisorMemoryItem.status.in_([_MEMORY_ITEM_ACTIVE, _MEMORY_ITEM_PENDING_CONFLICT]),
        or_(AdvisorMemoryItem.expires_at.is_(None), AdvisorMemoryItem.expires_at >= now),
    ]

    scope_filters: list[Any] = [AdvisorMemoryItem.session_id == session_id]
    if student_id is not None:
        scope_filters.append(AdvisorMemoryItem.student_id == student_id)
        if domain in _ALLOWED_DOMAINS:
            scope_filters.append(
                and_(
                    AdvisorMemoryItem.student_id == student_id,
                    AdvisorMemoryItem.domain == domain,
                )
            )
    filters.append(or_(*scope_filters))

    stmt = (
        select(AdvisorMemoryItem)
        .where(*filters)
        .order_by(desc(AdvisorMemoryItem.confidence), desc(AdvisorMemoryItem.created_at))
        .limit(limit)
    )
    rows = (await session.execute(stmt)).scalars().all()
    active = [row for row in rows if row.status == _MEMORY_ITEM_ACTIVE]
    conflicts = [row for row in rows if row.status == _MEMORY_ITEM_PENDING_CONFLICT]
    return {"active": active, "conflicts": conflicts}


async def retrieve_message_chunks(
    *,
    session: AsyncSession,
    query: str,
    session_id: str,
    student_id: uuid.UUID | None,
    domain: str | None,
    limit: int = 6,
    embedding_service: EmbeddingService | None = None,
) -> tuple[list[RetrievedChunk], dict[str, Any]]:
    """Hybrid-retrieve message chunks with vector + keyword + recency scoring."""
    started = datetime.now(tz=UTC)
    session_rows = await _load_chunk_candidates(
        session=session,
        session_id=session_id,
        student_id=None,
        domain=domain,
        limit=_RAG_CANDIDATE_LIMIT,
    )
    fallback_rows: list[AdvisorMessageChunk] = []
    if student_id is not None:
        fallback_rows = await _load_chunk_candidates(
            session=session,
            session_id=None,
            student_id=student_id,
            domain=domain,
            limit=_RAG_CANDIDATE_LIMIT,
        )

    all_rows = _dedupe_chunks(session_rows + fallback_rows)
    query_vector: list[float] | None = None
    memory_degraded = False
    if query.strip() and settings.GOOGLE_API_KEY:
        try:
            emb = embedding_service or get_embedding_service()
            query_vector = await asyncio.wait_for(emb.embed_query(query), timeout=_RAG_TIMEOUT_SECONDS)
        except Exception:
            memory_degraded = True
            logger.warning("Query embedding unavailable; using keyword+recency retrieval", exc_info=True)
    else:
        memory_degraded = True

    query_terms = _tokenize(query)
    scored: list[tuple[float, float, float, float, str, AdvisorMessageChunk]] = []
    for row in all_rows:
        vector_sim = _cosine_similarity(query_vector, _coerce_vector(row.embedding))
        keyword_score = _keyword_overlap(query_terms, row.content)
        recency = _recency_decay(row.created_at)
        score = 0.70 * vector_sim + 0.20 * keyword_score + 0.10 * recency
        source = "session" if row.session_id == session_id else "student_domain"
        if source == "session":
            score += 0.02
        scored.append((score, vector_sim, keyword_score, recency, source, row))

    scored.sort(key=lambda x: x[0], reverse=True)
    selected = scored[:limit]
    result = [
        RetrievedChunk(
            chunk_id=str(row.id),
            text=row.content,
            score=round(score, 6),
            source=source,
            session_id=row.session_id,
            domain=row.domain if row.domain in _ALLOWED_DOMAINS else None,
        )
        for score, _vec, _kw, _rec, source, row in selected
    ]
    latency_ms = int((datetime.now(tz=UTC) - started).total_seconds() * 1000)
    return result, {"latency_ms": latency_ms, "memory_degraded": memory_degraded}


async def cleanup_memory_records(
    *,
    session: AsyncSession,
    now: datetime | None = None,
    batch_size: int = 1000,
) -> dict[str, int]:
    """Expire stale memory items and clean orphan chunks."""
    current = now or datetime.now(tz=UTC)
    expire_stmt = select(AdvisorMemoryItem).where(
        AdvisorMemoryItem.status.in_([_MEMORY_ITEM_ACTIVE, _MEMORY_ITEM_PENDING_CONFLICT]),
        AdvisorMemoryItem.expires_at.is_not(None),
        AdvisorMemoryItem.expires_at < current,
    )
    expired_rows = (await session.execute(expire_stmt)).scalars().all()
    for row in expired_rows:
        row.status = "expired"
        row.deleted_at = current

    prune_before = current - timedelta(days=14)
    delete_stmt = select(AdvisorMemoryItem).where(
        AdvisorMemoryItem.status.in_(["expired", "deleted", _MEMORY_ITEM_SUPERSEDED]),
        AdvisorMemoryItem.deleted_at.is_not(None),
        AdvisorMemoryItem.deleted_at < prune_before,
    ).limit(batch_size)
    prunable = (await session.execute(delete_stmt)).scalars().all()
    for row in prunable:
        await session.delete(row)

    orphan_stmt = (
        select(AdvisorMessageChunk)
        .outerjoin(AdvisorMessage, AdvisorMessageChunk.message_id == AdvisorMessage.id)
        .where(AdvisorMessage.id.is_(None))
        .limit(batch_size)
    )
    orphan_rows = (await session.execute(orphan_stmt)).scalars().all()
    for row in orphan_rows:
        await session.delete(row)

    await session.flush()
    return {
        "expired": len(expired_rows),
        "deleted": len(prunable),
        "orphan_chunks_deleted": len(orphan_rows),
    }


def chunk_text(text: str, *, max_chars: int = _MAX_CHUNK_CHARS, overlap_chars: int = _CHUNK_OVERLAP_CHARS) -> list[str]:
    """Split text into overlapping fixed-size chunks."""
    raw = text.strip()
    if not raw:
        return []
    if len(raw) <= max_chars:
        return [raw]

    chunks: list[str] = []
    start = 0
    step = max(max_chars - overlap_chars, 1)
    while start < len(raw):
        chunks.append(raw[start : start + max_chars])
        start += step
    return chunks


def estimate_tokens(text: str) -> int:
    """Cheap token estimator for budgeting."""
    return max(len(text) // 4, 1) if text else 0


async def _load_existing_chunk_indexes(*, session: AsyncSession, message_id: uuid.UUID) -> set[int]:
    stmt = select(AdvisorMessageChunk.chunk_index).where(AdvisorMessageChunk.message_id == message_id)
    rows = (await session.execute(stmt)).scalars().all()
    return {int(v) for v in rows}


async def _embed_chunks(
    chunks: list[str],
    *,
    embedding_service: EmbeddingService | None = None,
) -> list[list[float] | None]:
    if not chunks:
        return []
    if not settings.GOOGLE_API_KEY:
        return [None for _ in chunks]
    emb = embedding_service or get_embedding_service()
    vectors = await emb.embed_batch(chunks, task_type="RETRIEVAL_DOCUMENT")
    return [list(v) if isinstance(v, Iterable) else None for v in vectors]


def _build_memory_candidates(event: MemoryIngestEvent) -> list[MemoryItem]:
    """Build structured memory candidates from white-listed structured signals."""
    candidates: list[MemoryItem] = []
    student_uuid = event.student_id

    for idx, artifact in enumerate(event.artifacts):
        artifact_type = str(artifact.get("type", "unknown"))
        candidates.append(
            MemoryItem(
                scope="domain",
                type="fact",
                key=f"artifact:{artifact_type}:{idx}",
                value=artifact,
                confidence=0.9,
                session_id=event.session_id,
                student_id=student_uuid,
                domain=event.domain,
                source_turn_id=event.turn_id,
                expires_at=_expires_at_iso(days=90),
            )
        )

    for done in event.done:
        confidence = 0.85 if done.status == "succeeded" else 0.75
        candidates.append(
            MemoryItem(
                scope="session",
                type="decision",
                key=f"done:{done.capability}",
                value={
                    "status": done.status,
                    "message": done.message,
                    "retry_count": done.retry_count,
                },
                confidence=confidence,
                session_id=event.session_id,
                student_id=student_uuid,
                domain=event.domain,
                source_turn_id=event.turn_id,
                expires_at=_expires_at_iso(days=14),
            )
        )

    for pending in event.pending:
        candidates.append(
            MemoryItem(
                scope="session",
                type="queue_step",
                key=f"pending:{pending.capability}",
                value={
                    "reason": pending.reason,
                    "message": pending.message,
                },
                confidence=0.8,
                session_id=event.session_id,
                student_id=student_uuid,
                domain=event.domain,
                source_turn_id=event.turn_id,
                expires_at=_expires_at_iso(days=30),
            )
        )

    for action in event.next_actions:
        candidates.append(
            MemoryItem(
                scope="session",
                type="plan",
                key=f"action:{action.action_id}",
                value={"label": action.label, "payload": action.payload},
                confidence=0.7,
                session_id=event.session_id,
                student_id=student_uuid,
                domain=event.domain,
                source_turn_id=event.turn_id,
                expires_at=_expires_at_iso(days=14),
            )
        )

    return candidates


async def _upsert_memory_candidates(
    *,
    session: AsyncSession,
    candidates: list[MemoryItem],
) -> dict[str, int]:
    active_inserted = 0
    pending_conflicts = 0
    for candidate in candidates:
        key_stmt = select(AdvisorMemoryItem).where(
            AdvisorMemoryItem.scope == candidate.scope,
            AdvisorMemoryItem.item_type == candidate.type,
            AdvisorMemoryItem.item_key == candidate.key,
            AdvisorMemoryItem.status == _MEMORY_ITEM_ACTIVE,
        )
        if candidate.session_id:
            key_stmt = key_stmt.where(AdvisorMemoryItem.session_id == candidate.session_id)
        if candidate.student_id:
            key_stmt = key_stmt.where(AdvisorMemoryItem.student_id == _parse_uuid(candidate.student_id))
        if candidate.domain:
            key_stmt = key_stmt.where(AdvisorMemoryItem.domain == candidate.domain)
        key_stmt = key_stmt.order_by(desc(AdvisorMemoryItem.created_at)).limit(1)

        existing = (await session.execute(key_stmt)).scalars().first()
        status = _MEMORY_ITEM_ACTIVE
        confidence = _bound_confidence(candidate.confidence)
        if existing is not None:
            if confidence < (float(existing.confidence) + 0.1):
                status = _MEMORY_ITEM_PENDING_CONFLICT
                pending_conflicts += 1
            else:
                existing.status = _MEMORY_ITEM_SUPERSEDED
                existing.deleted_at = datetime.now(tz=UTC)

        item = AdvisorMemoryItem(
            session_id=candidate.session_id,
            student_id=_parse_uuid(candidate.student_id),
            domain=candidate.domain,
            scope=candidate.scope,
            item_type=candidate.type,
            item_key=candidate.key,
            item_value=candidate.value,
            confidence=confidence,
            status=status,
            source_turn_id=candidate.source_turn_id,
            expires_at=_parse_datetime(candidate.expires_at),
        )
        session.add(item)
        if status == _MEMORY_ITEM_ACTIVE:
            active_inserted += 1
    await session.flush()
    return {"active_inserted": active_inserted, "pending_conflicts": pending_conflicts}


async def _load_chunk_candidates(
    *,
    session: AsyncSession,
    session_id: str | None,
    student_id: uuid.UUID | None,
    domain: str | None,
    limit: int,
) -> list[AdvisorMessageChunk]:
    stmt = select(AdvisorMessageChunk)
    if session_id is not None:
        stmt = stmt.where(AdvisorMessageChunk.session_id == session_id)
    if student_id is not None:
        stmt = stmt.where(AdvisorMessageChunk.student_id == student_id)
    if domain in _ALLOWED_DOMAINS:
        stmt = stmt.where(or_(AdvisorMessageChunk.domain == domain, AdvisorMessageChunk.domain.is_(None)))
    stmt = stmt.order_by(desc(AdvisorMessageChunk.created_at)).limit(limit)
    return (await session.execute(stmt)).scalars().all()


def _dedupe_chunks(chunks: list[AdvisorMessageChunk]) -> list[AdvisorMessageChunk]:
    out: list[AdvisorMessageChunk] = []
    seen: set[str] = set()
    for row in chunks:
        key = str(row.id)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _coerce_vector(raw: Any) -> list[float] | None:
    if raw is None:
        return None
    if isinstance(raw, list):
        out: list[float] = []
        for val in raw:
            try:
                out.append(float(val))
            except (TypeError, ValueError):
                return None
        return out
    if isinstance(raw, str):
        try:
            parsed = ast.literal_eval(raw)
        except Exception:
            return None
        return _coerce_vector(parsed)
    return None


def _tokenize(text: str) -> set[str]:
    return {tok for tok in re.findall(r"[A-Za-z0-9_\u4e00-\u9fff]+", text.lower()) if len(tok) >= 2}


def _keyword_overlap(query_terms: set[str], text: str) -> float:
    if not query_terms:
        return 0.0
    text_terms = _tokenize(text)
    if not text_terms:
        return 0.0
    return len(query_terms & text_terms) / len(query_terms)


def _recency_decay(created_at: datetime | None) -> float:
    if created_at is None:
        return 0.0
    now = datetime.now(tz=UTC)
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    age_days = max((now - created_at).total_seconds() / 86400.0, 0.0)
    return math.exp(-age_days / 30.0)


def _cosine_similarity(a: list[float] | None, b: list[float] | None) -> float:
    if not a or not b:
        return 0.0
    if len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    raw = dot / (na * nb)
    return max(0.0, min(1.0, (raw + 1.0) / 2.0))


def _compose_prompt_context(
    *,
    policy_lines: list[str],
    history_lines: list[str],
    memory_lines: list[str],
    rag_lines: list[str],
    pending_lines: list[str],
    conflict_lines: list[str],
) -> str:
    parts: list[str] = []
    parts.append("policy:")
    parts.extend(f"- {line}" for line in policy_lines)
    if history_lines:
        parts.append("recent_history:")
        parts.extend(f"- {line}" for line in history_lines)
    if memory_lines:
        parts.append("memory_items:")
        parts.extend(f"- {line}" for line in memory_lines)
    if conflict_lines:
        parts.append("memory_conflicts:")
        parts.extend(f"- {line}" for line in conflict_lines)
    if rag_lines:
        parts.append("retrieved_history:")
        parts.extend(f"- {line}" for line in rag_lines)
    if pending_lines:
        parts.append("pending_recovery:")
        parts.extend(f"- {line}" for line in pending_lines)
    return "\n".join(parts)


def _policy_lines(*, stage: str) -> list[str]:
    base = [
        "Prefer structured memory and retrieved history over guesswork.",
        "Use only context relevant to the current request.",
    ]
    if stage == "route":
        base.append("Prioritize domain and intent disambiguation.")
    else:
        base.append("Prioritize execution details and recovery continuity.")
    return base


def _pending_lines_from_context(common_ctx: dict[str, Any]) -> list[str]:
    out: list[str] = []
    pending_raw = common_ctx.get("advisor_pending_queue")
    failed_raw = common_ctx.get("advisor_failed_steps")
    if isinstance(pending_raw, list):
        for row in pending_raw[:8]:
            if not isinstance(row, dict):
                continue
            cap = row.get("capability")
            reason = row.get("reason")
            if cap:
                out.append(f"{cap} ({reason})")
    if isinstance(failed_raw, list):
        for row in failed_raw[:6]:
            if not isinstance(row, dict):
                continue
            cap = row.get("capability")
            status = row.get("status")
            if cap:
                out.append(f"{cap} [{status}]")
    return out


def _select_memory_items(items: list[AdvisorMemoryItem], *, stage: str, limit: int) -> list[AdvisorMemoryItem]:
    if stage == "route":
        preferred = [row for row in items if row.item_type in {"constraint", "decision"}]
        if preferred:
            return preferred[:limit]
    return items[:limit]


def _memory_line(item: AdvisorMemoryItem) -> str:
    value_preview = str(item.item_value)
    if len(value_preview) > 180:
        value_preview = value_preview[:177] + "..."
    return f"{item.item_type}:{item.item_key} conf={item.confidence:.2f} {value_preview}"


def _memory_item_dump(item: AdvisorMemoryItem) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "scope": item.scope,
        "type": item.item_type,
        "key": item.item_key,
        "value": item.item_value,
        "confidence": float(item.confidence),
        "status": item.status,
        "source_turn_id": item.source_turn_id,
        "expires_at": item.expires_at.isoformat() if item.expires_at else None,
    }


def _history_line(message: dict[str, Any]) -> str:
    role = str(message.get("role", "unknown"))
    content = str(message.get("content", "")).strip()
    if len(content) > 280:
        content = content[:277] + "..."
    return f"{role}: {content}"


def _trim_to_tokens(text: str, token_limit: int) -> str:
    max_chars = token_limit * 4
    stripped = " ".join(text.split())
    if len(stripped) <= max_chars:
        return stripped
    return stripped[: max_chars - 3] + "..."


def _bound_confidence(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _expires_at_iso(*, days: int) -> str:
    return (datetime.now(tz=UTC) + timedelta(days=days)).isoformat()


def _parse_datetime(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    try:
        value = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _parse_uuid(raw: str | None) -> uuid.UUID | None:
    if not raw:
        return None
    try:
        return uuid.UUID(raw)
    except (TypeError, ValueError):
        return None
