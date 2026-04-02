"""Tests for advisor layered memory ingestion and context assembly."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from scholarpath.advisor.contracts import AdvisorAction, DoneStep, MemoryIngestEvent, PendingStep
from scholarpath.advisor.memory_context import ContextAssembler, cleanup_memory_records, ingest_memory_event
from scholarpath.chat.memory import ChatMemory
from scholarpath.db.models.advisor_memory import AdvisorMemoryItem, AdvisorMessage, AdvisorMessageChunk
from tests.fake_redis import FakeRedis


@pytest.mark.asyncio
async def test_ingest_event_is_idempotent(session) -> None:
    event = MemoryIngestEvent(
        turn_id="turn-1",
        session_id="sess-1",
        student_id=None,
        domain="undergrad",
        capability="undergrad.school.recommend",
        role="assistant",
        content="这是一段较长的推荐解释。" * 50,
        artifacts=[{"type": "info_card", "title": "A", "summary": "B"}],
        done=[
            DoneStep(
                capability="undergrad.school.recommend",
                status="succeeded",
                message="done",
                retry_count=0,
            )
        ],
        pending=[
            PendingStep(
                capability="undergrad.strategy.plan",
                reason="over_limit",
                message="queued",
            )
        ],
        next_actions=[AdvisorAction(action_id="queue.run_pending", label="继续", payload={})],
    )

    first = await ingest_memory_event(session=session, event=event)
    second = await ingest_memory_event(session=session, event=event)
    await session.commit()

    msg_count = await session.scalar(select(func.count(AdvisorMessage.id)))
    chunk_count = await session.scalar(select(func.count(AdvisorMessageChunk.id)))
    memory_count = await session.scalar(select(func.count(AdvisorMemoryItem.id)))

    assert first["new_chunk_count"] > 0
    assert second["new_chunk_count"] == 0
    assert msg_count == 1
    assert chunk_count == first["chunk_count"]
    assert memory_count and memory_count >= 2


@pytest.mark.asyncio
async def test_retrieval_prioritizes_same_session(session) -> None:
    event_a = MemoryIngestEvent(
        turn_id="turn-a",
        session_id="sess-a",
        student_id=None,
        domain="undergrad",
        capability="undergrad.school.query",
        role="assistant",
        content="MIT 计算机科学项目课程和录取偏好说明",
    )
    event_b = MemoryIngestEvent(
        turn_id="turn-b",
        session_id="sess-b",
        student_id=None,
        domain="undergrad",
        capability="undergrad.school.query",
        role="assistant",
        content="同样包含 MIT 关键词但来自另一个会话",
    )
    await ingest_memory_event(session=session, event=event_a)
    await ingest_memory_event(session=session, event=event_b)
    await session.commit()

    redis = FakeRedis()
    memory = ChatMemory(redis)
    assembler = ContextAssembler(session=session, memory=memory)
    ctx, metrics = await assembler.assemble(
        stage="execution",
        session_id="sess-a",
        student_id=None,
        message="MIT 课程和录取",
        domain="undergrad",
    )

    chunks = ctx["retrieved_chunks"]
    assert chunks
    assert chunks[0]["session_id"] == "sess-a"
    assert metrics.rag_hits >= 1


@pytest.mark.asyncio
async def test_context_budget_and_non_trim_sections(session) -> None:
    redis = FakeRedis()
    memory = ChatMemory(redis)
    for i in range(20):
        await memory.save_message("sess-c", "user", f"历史消息{i} " + ("x" * 220))
    await memory.save_context(
        "sess-c",
        "advisor_pending_queue",
        [
            {
                "capability": "undergrad.strategy.plan",
                "reason": "over_limit",
                "message": "queued",
            }
        ],
        domain="common",
    )

    now = datetime.now(tz=UTC)
    session.add(
        AdvisorMemoryItem(
            session_id="sess-c",
            student_id=None,
            domain="undergrad",
            scope="session",
            item_type="constraint",
            item_key="constraint:budget",
            item_value={"budget_usd": 50000},
            confidence=0.95,
            status="active",
            source_turn_id="seed-1",
            expires_at=now + timedelta(days=90),
        )
    )
    session.add(
        AdvisorMessage(
            turn_id="turn-c",
            session_id="sess-c",
            student_id=None,
            role="assistant",
            domain="undergrad",
            capability="undergrad.school.query",
            content="RAG 文本 " + ("z" * 1500),
            ingestion_status="ready",
        )
    )
    await session.flush()
    row = (
        await session.execute(
            select(AdvisorMessage).where(
                AdvisorMessage.session_id == "sess-c",
                AdvisorMessage.turn_id == "turn-c",
            )
        )
    ).scalars().first()
    assert row is not None
    session.add(
        AdvisorMessageChunk(
            message_id=row.id,
            turn_id=row.turn_id,
            session_id=row.session_id,
            student_id=None,
            domain="undergrad",
            chunk_index=0,
            content="RAG " + ("k" * 2400),
            token_count=900,
            score_meta={"source": "seed"},
            embedding=None,
        )
    )
    await session.commit()

    assembler = ContextAssembler(session=session, memory=memory)
    ctx, metrics = await assembler.assemble(
        stage="execution",
        session_id="sess-c",
        student_id=None,
        message="帮我做本科择校",
        domain="undergrad",
    )

    prompt = ctx["route_prompt_context"]
    assert metrics.context_tokens <= 1800
    assert "pending_recovery:" in prompt
    assert "memory_items:" in prompt
    assert "constraint:budget" in prompt


@pytest.mark.asyncio
async def test_route_stage_uses_lightweight_context_without_rag_or_memory_items(session) -> None:
    redis = FakeRedis()
    memory = ChatMemory(redis)
    await memory.save_message("sess-light", "user", "hello")
    await memory.save_message("sess-light", "assistant", "world")
    await memory.save_message("sess-light", "user", "portfolio")
    await memory.save_context(
        "sess-light",
        "advisor_pending_queue",
        [
            {
                "capability": "undergrad.strategy.plan",
                "reason": "over_limit",
                "message": "queued",
            }
        ],
        domain="common",
    )
    assembler = ContextAssembler(session=session, memory=memory)
    ctx, metrics = await assembler.assemble(
        stage="route",
        session_id="sess-light",
        student_id=None,
        message="继续",
        domain="undergrad",
    )
    assert metrics.rag_hits == 0
    assert metrics.memory_hits == 0
    assert ctx["memory_items"] == []
    assert ctx["retrieved_chunks"] == []
    assert "pending_recovery:" in ctx["route_prompt_context"]


@pytest.mark.asyncio
async def test_cleanup_expires_old_memory_items(session) -> None:
    session.add(
        AdvisorMemoryItem(
            session_id="sess-x",
            student_id=None,
            domain="undergrad",
            scope="session",
            item_type="queue_step",
            item_key="pending:offer.compare",
            item_value={"reason": "over_limit"},
            confidence=0.8,
            status="active",
            source_turn_id="turn-old",
            expires_at=datetime.now(tz=UTC) - timedelta(days=1),
        )
    )
    await session.commit()

    result = await cleanup_memory_records(session=session, batch_size=100)
    await session.commit()

    refreshed = (
        await session.execute(
            select(AdvisorMemoryItem).where(AdvisorMemoryItem.item_key == "pending:offer.compare")
        )
    ).scalars().first()
    assert result["expired"] >= 1
    assert refreshed is not None
    assert refreshed.status == "expired"


@pytest.mark.asyncio
async def test_ingest_message_id_path_skips_missing_message(session) -> None:
    result = await ingest_memory_event(
        session=session,
        message_id="f7f7e12c-c177-4f8c-8d2e-4e72888e8cc0",
    )
    assert result["skipped"] is True
    assert result["reason"] == "message_not_found"
