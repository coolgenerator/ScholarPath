"""Tests for advisor overwrite-edit timeline behavior."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from scholarpath.advisor.contracts import AdvisorRequest
from scholarpath.api.routes.advisor import _apply_edit_overwrite, _load_db_history_entries
from scholarpath.chat.memory import ChatMemory
from scholarpath.db.models.advisor_memory import (
    AdvisorMemoryItem,
    AdvisorMessage,
    AdvisorMessageChunk,
)
from tests.fake_redis import FakeRedis


def _msg(
    *,
    turn_id: str,
    role: str,
    content: str,
    created_at: datetime,
) -> AdvisorMessage:
    return AdvisorMessage(
        turn_id=turn_id,
        session_id="sess-edit",
        student_id=None,
        role=role,
        domain="undergrad",
        capability="undergrad.school.recommend",
        content=content,
        artifacts_json=[],
        done_json=[],
        pending_json=[],
        next_actions_json=[],
        ingestion_status="ready",
        created_at=created_at,
        updated_at=created_at,
    )


@pytest.mark.asyncio
async def test_apply_edit_overwrite_truncates_timeline_and_rebuilds_runtime_cache(session) -> None:
    t0 = datetime.now(tz=UTC)
    rows = [
        _msg(turn_id="turn-1", role="user", content="u1", created_at=t0 + timedelta(seconds=1)),
        _msg(turn_id="turn-1", role="assistant", content="a1", created_at=t0 + timedelta(seconds=2)),
        _msg(turn_id="turn-2", role="user", content="u2", created_at=t0 + timedelta(seconds=3)),
        _msg(turn_id="turn-2", role="assistant", content="a2", created_at=t0 + timedelta(seconds=4)),
        _msg(turn_id="turn-3", role="user", content="u3", created_at=t0 + timedelta(seconds=5)),
    ]
    session.add_all(rows)
    await session.flush()

    for row in rows[2:]:
        session.add(
            AdvisorMessageChunk(
                message_id=row.id,
                turn_id=row.turn_id,
                session_id=row.session_id,
                student_id=None,
                domain="undergrad",
                chunk_index=0,
                content=f"chunk-{row.turn_id}-{row.role}",
                token_count=12,
                score_meta={"source": "test"},
                embedding=None,
            )
        )

    session.add_all(
        [
            AdvisorMemoryItem(
                session_id="sess-edit",
                student_id=None,
                domain="undergrad",
                scope="session",
                item_type="decision",
                item_key="done:turn-1",
                item_value={"v": 1},
                confidence=0.9,
                status="active",
                source_turn_id="turn-1",
            ),
            AdvisorMemoryItem(
                session_id="sess-edit",
                student_id=None,
                domain="undergrad",
                scope="session",
                item_type="decision",
                item_key="done:turn-2",
                item_value={"v": 2},
                confidence=0.9,
                status="active",
                source_turn_id="turn-2",
            ),
            AdvisorMemoryItem(
                session_id="sess-edit",
                student_id=None,
                domain="undergrad",
                scope="session",
                item_type="decision",
                item_key="done:turn-3",
                item_value={"v": 3},
                confidence=0.9,
                status="active",
                source_turn_id="turn-3",
            ),
        ]
    )
    await session.flush()

    redis = FakeRedis()
    memory = ChatMemory(redis)
    for row in rows:
        await memory.save_message("sess-edit", row.role, row.content)

    request = AdvisorRequest(
        turn_id="new-turn",
        session_id="sess-edit",
        message="u2-edited",
        edit={"target_turn_id": "turn-2", "mode": "overwrite"},
    )

    rewritten, err = await _apply_edit_overwrite(
        session=session,
        redis=redis,
        request=request,
    )
    assert err is None
    assert rewritten.turn_id == "turn-2"
    assert rewritten.edit is None

    timeline = (
        await session.execute(
            select(AdvisorMessage)
            .where(AdvisorMessage.session_id == "sess-edit")
            .order_by(AdvisorMessage.created_at.asc(), AdvisorMessage.id.asc())
        )
    ).scalars().all()
    assert [(row.turn_id, row.role, row.content) for row in timeline] == [
        ("turn-1", "user", "u1"),
        ("turn-1", "assistant", "a1"),
        ("turn-2", "user", "u2-edited"),
    ]
    assert timeline[-1].ingestion_status == "pending"

    remaining_items = (
        await session.execute(
            select(AdvisorMemoryItem.source_turn_id).where(
                AdvisorMemoryItem.session_id == "sess-edit"
            )
        )
    ).scalars().all()
    assert set(remaining_items) == {"turn-1"}

    remaining_chunks = (
        await session.execute(
            select(AdvisorMessageChunk.turn_id).where(
                AdvisorMessageChunk.session_id == "sess-edit"
            )
        )
    ).scalars().all()
    assert set(remaining_chunks) == set()

    rebuilt = await memory.get_history("sess-edit", limit=20)
    assert rebuilt == [
        {"role": "user", "content": "u1"},
        {"role": "assistant", "content": "a1"},
    ]


@pytest.mark.asyncio
async def test_load_db_history_entries_returns_editability_flags(session) -> None:
    base = datetime.now(tz=UTC)
    user_row = _msg(
        turn_id="turn-1",
        role="user",
        content="hello",
        created_at=base,
    )
    user_row.updated_at = base + timedelta(minutes=5)
    assistant_row = _msg(
        turn_id="turn-1",
        role="assistant",
        content="world",
        created_at=base + timedelta(seconds=2),
    )

    session.add_all([user_row, assistant_row])
    await session.flush()

    history = await _load_db_history_entries(session=session, session_id="sess-edit")
    assert len(history) == 2
    assert history[0]["role"] == "user"
    assert history[0]["editable"] is True
    assert history[0]["edited"] is True
    assert history[0]["turn_id"] == "turn-1"
    assert history[0]["message_id"]
    assert history[1]["role"] == "assistant"
    assert history[1]["editable"] is False
