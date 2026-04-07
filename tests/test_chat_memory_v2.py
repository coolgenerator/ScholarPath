"""Tests for structured chat history persistence in ChatMemory."""

from __future__ import annotations

import json
import uuid

import pytest

from scholarpath.chat.memory import ChatMemory


class _InMemoryRedis:
    def __init__(self):
        self._lists: dict[str, list[str]] = {}
        self._hashes: dict[str, dict[str, str]] = {}

    async def rpush(self, key: str, value: str) -> None:
        self._lists.setdefault(key, []).append(value)

    async def ltrim(self, key: str, start: int, end: int) -> None:
        items = self._lists.get(key, [])
        n = len(items)
        start_idx = n + start if start < 0 else start
        end_idx = n + end if end < 0 else end
        start_idx = max(start_idx, 0)
        end_idx = min(end_idx, n - 1)
        if start_idx > end_idx:
            self._lists[key] = []
        else:
            self._lists[key] = items[start_idx : end_idx + 1]

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        items = self._lists.get(key, [])
        n = len(items)
        start_idx = n + start if start < 0 else start
        end_idx = n + end if end < 0 else end
        start_idx = max(start_idx, 0)
        end_idx = min(end_idx, n - 1)
        if start_idx > end_idx:
            return []
        return items[start_idx : end_idx + 1]

    async def hset(self, key: str, field: str, value: str) -> None:
        self._hashes.setdefault(key, {})[field] = value

    async def hdel(self, key: str, field: str) -> None:
        mapping = self._hashes.get(key)
        if mapping is None:
            return
        mapping.pop(field, None)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._hashes.get(key, {}))

    async def expire(self, key: str, _ttl_seconds: int) -> None:
        # TTL is ignored in this in-memory fake.
        if key in self._lists or key in self._hashes:
            return

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self._lists.pop(key, None)
            self._hashes.pop(key, None)


@pytest.mark.asyncio
async def test_save_assistant_turn_persists_structured_fields():
    redis = _InMemoryRedis()
    memory = ChatMemory(redis)

    await memory.save_message("session-1", "user", "hello")
    await memory.save_assistant_turn(
        "session-1",
        content="final content",
        status="ok",
        trace_id="trace-123",
        blocks=[
            {
                "id": "b1",
                "kind": "recommendation",
                "capability_id": "recommendation_subagent",
                "order": 0,
                "payload": {"schools": []},
                "meta": {"subagent": True},
            }
        ],
        actions=["next_step"],
    )

    history = await memory.get_history("session-1", limit=10)
    assert len(history) == 2
    assert history[0]["role"] == "user"
    assert history[1]["role"] == "assistant"
    assert history[1]["status"] == "ok"
    assert history[1]["trace_id"] == "trace-123"
    assert history[1]["blocks"][0]["kind"] == "recommendation"
    assert history[1]["actions"] == ["next_step"]


@pytest.mark.asyncio
async def test_context_layering_routes_keys_and_returns_merged_view():
    redis = _InMemoryRedis()
    memory = ChatMemory(redis)

    session_id = "session-layered"
    legacy_key = f"scholarpath:chat:context:{session_id}"
    await redis.hset(legacy_key, "legacy_only", json.dumps("old-value", ensure_ascii=False))

    await memory.save_context(session_id, "current_school_name", "MIT")
    await memory.save_context(session_id, "intake_step", 2)
    await memory.save_context(session_id, "advisor_tone", "concise")
    await memory.save_context(session_id, "preferred_regions", ["US", "UK"], layer="long_term")

    layers = await memory.get_context_layers(session_id)
    merged = await memory.get_context(session_id)

    assert layers["short_term"]["current_school_name"] == "MIT"
    assert layers["working"]["intake_step"] == 2
    assert layers["working"]["advisor_tone"] == "concise"
    assert layers["long_term"]["preferred_regions"] == ["US", "UK"]
    assert merged["legacy_only"] == "old-value"
    assert merged["current_school_name"] == "MIT"


@pytest.mark.asyncio
async def test_save_context_none_clears_key_from_all_layers_and_legacy():
    redis = _InMemoryRedis()
    memory = ChatMemory(redis)

    session_id = "session-clear-key"
    legacy_key = f"scholarpath:chat:context:{session_id}"
    await redis.hset(legacy_key, "pending_profile_patch", json.dumps({"x": 1}))
    await memory.save_context(session_id, "pending_profile_patch", {"proposal_id": "p1"})
    await memory.save_context(session_id, "pending_profile_patch", None)

    layers = await memory.get_context_layers(session_id)
    merged = await memory.get_context(session_id)
    assert "pending_profile_patch" not in merged
    assert "pending_profile_patch" not in layers["legacy"]
    assert "pending_profile_patch" not in layers["short_term"]
    assert "pending_profile_patch" not in layers["working"]


@pytest.mark.asyncio
async def test_clear_removes_history_and_all_context_layers():
    redis = _InMemoryRedis()
    memory = ChatMemory(redis)

    session_id = "session-clear"
    await memory.save_message(session_id, "user", "hello")
    await memory.save_context(session_id, "intake_step", 1)
    await memory.save_context(session_id, "current_school_name", "Stanford")
    await memory.save_context(session_id, "preferred_regions", ["US"], layer="long_term")

    assert await memory.get_history(session_id, limit=10)
    assert await memory.get_context(session_id)

    await memory.clear(session_id)

    assert await memory.get_history(session_id, limit=10) == []
    assert await memory.get_context(session_id) == {}


@pytest.mark.asyncio
async def test_student_context_layers_persist_cross_session_memory():
    redis = _InMemoryRedis()
    memory = ChatMemory(redis)

    student_id = uuid.uuid4()
    await memory.save_student_context(student_id, "profile_budget_usd", 65000, layer="long_term")
    await memory.save_student_context(student_id, "last_profile_changed_fields", ["academics.gpa"], layer="short_term")

    layers = await memory.get_student_context_layers(student_id)
    merged = await memory.get_student_context(student_id)
    assert layers["long_term"]["profile_budget_usd"] == 65000
    assert layers["short_term"]["last_profile_changed_fields"] == ["academics.gpa"]
    assert merged["profile_budget_usd"] == 65000
    assert merged["last_profile_changed_fields"] == ["academics.gpa"]


@pytest.mark.asyncio
async def test_clear_student_context_removes_all_layers():
    redis = _InMemoryRedis()
    memory = ChatMemory(redis)

    student_id = uuid.uuid4()
    await memory.save_student_context(student_id, "profile_gpa", 3.9, layer="long_term")
    await memory.save_student_context(student_id, "intake_complete", True, layer="working")
    assert await memory.get_student_context(student_id)

    await memory.clear_student_context(student_id)
    assert await memory.get_student_context(student_id) == {}


@pytest.mark.asyncio
async def test_turn_memory_journal_commit_persists_staged_writes():
    redis = _InMemoryRedis()
    memory = ChatMemory(redis)
    journal = memory.begin_turn_journal()
    student_id = uuid.uuid4()

    await journal.save_message("session-journal", "user", "hello")
    await journal.save_context("session-journal", "intake_step", 3)
    await journal.save_student_context(student_id, "profile_budget_usd", 70000, layer="long_term")

    assert await memory.get_history("session-journal", limit=10) == []
    assert await memory.get_context("session-journal") == {}
    assert await memory.get_student_context(student_id) == {}

    await journal.commit()

    history = await memory.get_history("session-journal", limit=10)
    assert len(history) == 1
    assert history[0]["content"] == "hello"
    assert (await memory.get_context("session-journal"))["intake_step"] == 3
    assert (await memory.get_student_context(student_id))["profile_budget_usd"] == 70000


@pytest.mark.asyncio
async def test_turn_memory_journal_discard_drops_staged_writes():
    redis = _InMemoryRedis()
    memory = ChatMemory(redis)
    journal = memory.begin_turn_journal()
    student_id = uuid.uuid4()

    await journal.save_message("session-discard", "user", "hello")
    await journal.save_context("session-discard", "intake_step", 4)
    await journal.save_student_context(student_id, "profile_gpa", 3.95, layer="long_term")
    await journal.discard()

    assert await memory.get_history("session-discard", limit=10) == []
    assert await memory.get_context("session-discard") == {}
    assert await memory.get_student_context(student_id) == {}


@pytest.mark.asyncio
async def test_turn_memory_journal_supports_read_your_writes():
    redis = _InMemoryRedis()
    memory = ChatMemory(redis)
    student_id = uuid.uuid4()
    await memory.save_message("session-ryw", "user", "base")
    await memory.save_context("session-ryw", "intake_step", 1)
    await memory.save_student_context(student_id, "profile_budget_usd", 65000, layer="long_term")

    journal = memory.begin_turn_journal()
    await journal.save_message("session-ryw", "assistant", "staged")
    await journal.save_context("session-ryw", "intake_step", 2)
    await journal.save_context("session-ryw", "advisor_tone", "concise")
    await journal.save_student_context(student_id, "profile_budget_usd", None, layer="long_term")
    await journal.save_student_context(student_id, "profile_need_financial_aid", True, layer="working")

    staged_history = await journal.get_history("session-ryw", limit=10)
    staged_context = await journal.get_context("session-ryw")
    staged_student_context = await journal.get_student_context(student_id)
    assert [item["content"] for item in staged_history] == ["base", "staged"]
    assert staged_context["intake_step"] == 2
    assert staged_context["advisor_tone"] == "concise"
    assert "profile_budget_usd" not in staged_student_context
    assert staged_student_context["profile_need_financial_aid"] is True

    base_context = await memory.get_context("session-ryw")
    base_student_context = await memory.get_student_context(student_id)
    assert base_context["intake_step"] == 1
    assert "advisor_tone" not in base_context
    assert base_student_context["profile_budget_usd"] == 65000
