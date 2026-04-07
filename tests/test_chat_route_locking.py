"""Unit tests for chat route lock/transaction helpers."""

from __future__ import annotations

import uuid

import pytest

from scholarpath.api.routes.chat import (
    _TURN_LOCK_TTL_SECONDS,
    _acquire_turn_lock,
    _finalize_turn_transaction,
    _release_turn_lock,
)


class _FakeRedisLock:
    def __init__(self) -> None:
        self.store: dict[str, str | bytes] = {}

    async def set(self, key: str, token: str, ex: int, nx: bool) -> bool:
        assert ex == _TURN_LOCK_TTL_SECONDS
        if nx and key in self.store:
            return False
        self.store[key] = token
        return True

    async def get(self, key: str):
        return self.store.get(key)

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)

    async def eval(self, _script: str, _key_count: int, key: str, token: str) -> int:
        if self.store.get(key) == token:
            self.store.pop(key, None)
            return 1
        return 0


class _FakeRedisNoEval:
    def __init__(self) -> None:
        self.store: dict[str, str | bytes] = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)


class _FakeSession:
    def __init__(self, *, fail_commit: bool = False) -> None:
        self.rollback_called = 0
        self.commit_called = 0
        self.fail_commit = fail_commit

    async def rollback(self) -> None:
        self.rollback_called += 1

    async def commit(self) -> None:
        self.commit_called += 1
        if self.fail_commit:
            raise RuntimeError("commit failed")


class _FakeJournal:
    def __init__(self) -> None:
        self.commit_called = 0
        self.discard_called = 0

    async def commit(self) -> None:
        self.commit_called += 1

    async def discard(self) -> None:
        self.discard_called += 1


@pytest.mark.asyncio
async def test_acquire_turn_lock_rejects_same_student_immediately():
    redis = _FakeRedisLock()
    student_id = uuid.uuid4()

    first = await _acquire_turn_lock(redis=redis, student_id=student_id, session_id="s-1")
    second = await _acquire_turn_lock(redis=redis, student_id=student_id, session_id="s-2")
    third = await _acquire_turn_lock(redis=redis, student_id=uuid.uuid4(), session_id="s-3")

    assert first is not None
    assert second is None
    assert third is not None
    assert f"scholarpath:chat:student_turn_lock:{student_id}" == first["key"]


@pytest.mark.asyncio
async def test_acquire_turn_lock_uses_session_scope_when_student_missing():
    redis = _FakeRedisLock()
    lock = await _acquire_turn_lock(redis=redis, student_id=None, session_id="sid-anon")
    assert lock is not None
    assert lock["key"] == "scholarpath:chat:student_turn_lock:sid-anon"


@pytest.mark.asyncio
async def test_release_turn_lock_uses_compare_and_delete():
    redis = _FakeRedisLock()
    lock = await _acquire_turn_lock(redis=redis, student_id=uuid.uuid4(), session_id="sid-lock")
    assert lock is not None
    redis.store[lock["key"]] = lock["token"]

    await _release_turn_lock(redis, lock_key=lock["key"], token="wrong-token")
    assert lock["key"] in redis.store

    await _release_turn_lock(redis, lock_key=lock["key"], token=lock["token"])
    assert lock["key"] not in redis.store


@pytest.mark.asyncio
async def test_release_turn_lock_fallback_decodes_bytes_value():
    redis = _FakeRedisNoEval()
    redis.store["k"] = b"token-1"
    await _release_turn_lock(redis, lock_key="k", token="token-1")
    assert "k" not in redis.store


@pytest.mark.asyncio
async def test_finalize_turn_transaction_rolls_back_and_discards_on_error_result():
    session = _FakeSession()
    journal = _FakeJournal()

    await _finalize_turn_transaction(
        session=session,
        journal=journal,
        result_status="error",
        session_id="sid-error",
        student_id=None,
        message="irrelevant",
    )

    assert session.rollback_called == 1
    assert session.commit_called == 0
    assert journal.discard_called == 1
    assert journal.commit_called == 0


@pytest.mark.asyncio
async def test_finalize_turn_transaction_discards_journal_when_db_commit_fails():
    session = _FakeSession(fail_commit=True)
    journal = _FakeJournal()

    with pytest.raises(RuntimeError, match="commit failed"):
        await _finalize_turn_transaction(
            session=session,
            journal=journal,
            result_status="ok",
            session_id="sid-commit-fail",
            student_id=None,
            message="hello",
        )

    assert session.commit_called == 1
    assert journal.discard_called == 1
    assert journal.commit_called == 0
