"""Tests for Redis-backed turn trace recorder."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scholarpath.api.models.chat import TurnEvent, TurnResult
from scholarpath.chat.trace import TurnTraceRecorder


class _InMemoryRedis:
    def __init__(self) -> None:
        self._strings: dict[str, str] = {}
        self._lists: dict[str, list[str]] = {}
        self._sets: dict[str, set[str]] = {}

    async def get(self, key: str):
        return self._strings.get(key)

    async def set(self, key: str, value: str):
        self._strings[key] = value

    async def delete(self, key: str):
        self._strings.pop(key, None)
        self._lists.pop(key, None)
        self._sets.pop(key, None)

    async def expire(self, _key: str, _seconds: int):
        return 1

    async def rpush(self, key: str, value: str):
        self._lists.setdefault(key, []).append(value)

    async def ltrim(self, key: str, start: int, end: int):
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

    async def lrange(self, key: str, start: int, end: int):
        items = self._lists.get(key, [])
        n = len(items)
        start_idx = n + start if start < 0 else start
        end_idx = n + end if end < 0 else end
        start_idx = max(start_idx, 0)
        end_idx = min(end_idx, n - 1)
        if start_idx > end_idx:
            return []
        return items[start_idx : end_idx + 1]

    async def sadd(self, key: str, value: str):
        bucket = self._sets.setdefault(key, set())
        before = len(bucket)
        bucket.add(value)
        return 0 if len(bucket) == before else 1


@pytest.mark.asyncio
async def test_trace_recorder_persists_steps_and_finalize_usage():
    redis = _InMemoryRedis()
    recorder = TurnTraceRecorder(redis, max_trace_steps=10, max_session_traces=10)
    trace_id = "trace-1"
    now = datetime.now(UTC)
    event = TurnEvent(
        trace_id=trace_id,
        event="capability_started",
        data={
            "step_id": "step-1",
            "step_kind": "capability",
            "step_status": "running",
            "phase": "execution",
            "wave_index": 1,
            "capability_id": "strategy",
        },
        timestamp=now,
    )
    await recorder.append_event(
        trace_id=trace_id,
        session_id="sid-1",
        student_id="stu-1",
        event=event,
    )

    result = TurnResult(
        trace_id=trace_id,
        status="ok",
        content="done",
        blocks=[],
        actions=[],
        usage={"tool_steps_used": 1, "wave_count": 1},
    )
    await recorder.finalize_result(
        trace_id=trace_id,
        session_id="sid-1",
        student_id="stu-1",
        result=result,
    )

    trace = await recorder.get_trace(trace_id)
    assert trace is not None
    assert trace["status"] == "ok"
    assert trace["usage"]["tool_steps_used"] == 1
    assert trace["step_count"] >= 2
    capability = next(step for step in trace["steps"] if step.get("step_kind") == "capability")
    assert capability["step_id"] == "step-1"


@pytest.mark.asyncio
async def test_trace_recorder_trims_steps_and_session_index():
    redis = _InMemoryRedis()
    recorder = TurnTraceRecorder(redis, max_trace_steps=3, max_session_traces=2)

    for idx in range(5):
        trace_id = f"trace-{idx}"
        await recorder.append_event(
            trace_id=trace_id,
            session_id="sid-x",
            student_id="stu-x",
            event=TurnEvent(
                trace_id=trace_id,
                event="planning_done",
                data={
                    "step_id": f"step-{idx}",
                    "step_kind": "checkpoint",
                    "step_status": "noop",
                    "phase": "checkpoint",
                    "wave_index": idx,
                },
                timestamp=datetime.now(UTC),
            ),
        )
        await recorder.finalize_trace(
            trace_id=trace_id,
            session_id="sid-x",
            student_id="stu-x",
            status="ok",
            usage={"wave_count": idx + 1},
        )

    last_trace = await recorder.get_trace("trace-4")
    assert last_trace is not None
    for idx in range(4):
        await recorder.append_event(
            trace_id="trace-4",
            session_id="sid-x",
            student_id="stu-x",
            event=TurnEvent(
                trace_id="trace-4",
                event="capability_finished",
                data={
                    "step_id": f"late-{idx}",
                    "step_kind": "capability",
                    "step_status": "completed",
                    "phase": "execution",
                },
                timestamp=datetime.now(UTC),
            ),
        )
    trimmed_trace = await recorder.get_trace("trace-4")
    assert trimmed_trace is not None
    assert len(trimmed_trace["steps"]) == 3
    session_list = await recorder.list_session_traces(session_id="sid-x", limit=50)
    assert session_list["total"] == 2
    assert [item["trace_id"] for item in session_list["items"]] == ["trace-4", "trace-3"]


@pytest.mark.asyncio
async def test_trace_recorder_records_lock_rejection():
    redis = _InMemoryRedis()
    recorder = TurnTraceRecorder(redis)
    usage = {
        "rejected_by_lock": True,
        "lock_scope": "student",
        "lock_ttl_seconds": 75,
        "lock_wait_ms": 3,
    }
    await recorder.record_lock_rejection(
        trace_id="lock-trace",
        session_id="sid-lock",
        student_id="stu-lock",
        usage=usage,
    )
    trace = await recorder.get_trace("lock-trace")
    assert trace is not None
    assert trace["status"] == "error"
    assert trace["usage"]["rejected_by_lock"] is True
    assert trace["step_count"] >= 1
    assert any(step.get("compact_reason_code") == "LOCK_REJECTED" for step in trace["steps"])


@pytest.mark.asyncio
async def test_trace_recorder_dedupes_by_step_id_and_event_seq():
    redis = _InMemoryRedis()
    recorder = TurnTraceRecorder(redis)
    trace_id = "trace-dedupe"
    base_ts = datetime.now(UTC)
    duplicate_event = TurnEvent(
        trace_id=trace_id,
        event="capability_started",
        data={
            "step_id": "step-1",
            "event_seq": 7,
            "step_kind": "capability",
            "step_status": "running",
            "phase": "execution",
            "display": {"title": "cap", "badge": "running", "severity": "info"},
        },
        timestamp=base_ts,
    )
    await recorder.append_event(
        trace_id=trace_id,
        session_id="sid-dedupe",
        student_id="stu-dedupe",
        event=duplicate_event,
    )
    await recorder.append_event(
        trace_id=trace_id,
        session_id="sid-dedupe",
        student_id="stu-dedupe",
        event=duplicate_event,
    )

    newer = TurnEvent(
        trace_id=trace_id,
        event="capability_finished",
        data={
            "step_id": "step-1",
            "event_seq": 8,
            "step_kind": "capability",
            "step_status": "completed",
            "phase": "execution",
        },
        timestamp=base_ts,
    )
    await recorder.append_event(
        trace_id=trace_id,
        session_id="sid-dedupe",
        student_id="stu-dedupe",
        event=newer,
    )

    compact = await recorder.get_trace(trace_id, view="compact")
    full = await recorder.get_trace(trace_id, view="full")
    assert compact is not None and full is not None
    cap_compact = next(step for step in compact["steps"] if step.get("step_kind") == "capability")
    cap_full = next(step for step in full["steps"] if step.get("step_kind") == "capability")
    assert cap_compact["step_status"] == "completed"
    assert cap_compact["event_seq"] == 8
    assert "data" not in cap_compact
    assert "metrics" not in cap_compact
    assert "data" in cap_full
