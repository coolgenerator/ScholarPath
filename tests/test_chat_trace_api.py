"""API tests for chat trace query endpoints."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scholarpath.api.models.chat import TurnEvent
from scholarpath.chat.trace import TurnTraceRecorder


class _InMemoryRedis:
    def __init__(self) -> None:
        self._strings: dict[str, str] = {}
        self._lists: dict[str, list[str]] = {}

    async def get(self, key: str):
        return self._strings.get(key)

    async def set(self, key: str, value: str):
        self._strings[key] = value

    async def delete(self, key: str):
        self._strings.pop(key, None)
        self._lists.pop(key, None)

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

    async def sadd(self, _key: str, _value: str):
        return 1


@pytest.mark.asyncio
async def test_chat_trace_endpoints_return_trace_and_session_list(client):
    from scholarpath.db.redis import get_redis
    from scholarpath.main import app

    fake_redis = _InMemoryRedis()
    recorder = TurnTraceRecorder(fake_redis)
    await recorder.append_event(
        trace_id="trace-api-1",
        session_id="sid-api-1",
        student_id="stu-api-1",
        event=TurnEvent(
            trace_id="trace-api-1",
            event="planning_done",
            data={
                "step_id": "step-1",
                "event_seq": 3,
                "step_kind": "wave",
                "step_status": "completed",
                "phase": "planning",
                "wave_index": 0,
                "metrics": {"pending_count": 2},
            },
            timestamp=datetime.now(UTC),
        ),
    )
    await recorder.finalize_trace(
        trace_id="trace-api-1",
        session_id="sid-api-1",
        student_id="stu-api-1",
        status="ok",
        usage={"tool_steps_used": 1, "wave_count": 1},
    )

    async def _override_redis():
        yield fake_redis

    app.dependency_overrides[get_redis] = _override_redis
    try:
        trace_resp = await client.get("/api/chat/traces/trace-api-1", params={"view": "compact"})
        assert trace_resp.status_code == 200
        trace_payload = trace_resp.json()
        assert trace_payload["trace_id"] == "trace-api-1"
        assert trace_payload["status"] == "ok"
        assert trace_payload["step_count"] >= 1
        step_payload = next(step for step in trace_payload["steps"] if step["step_id"] == "step-1")
        assert "metrics" not in step_payload
        assert "data" not in step_payload

        full_resp = await client.get("/api/chat/traces/trace-api-1", params={"view": "full"})
        assert full_resp.status_code == 200
        full_payload = full_resp.json()
        full_step = next(step for step in full_payload["steps"] if step["step_id"] == "step-1")
        assert "metrics" in full_step
        assert full_step["event_seq"] == 3

        list_resp = await client.get(
            "/api/chat/traces/session/sid-api-1",
            params={"limit": 50, "view": "compact"},
        )
        assert list_resp.status_code == 200
        list_payload = list_resp.json()
        assert list_payload["total"] == 1
        assert list_payload["items"][0]["trace_id"] == "trace-api-1"
    finally:
        app.dependency_overrides.pop(get_redis, None)


@pytest.mark.asyncio
async def test_chat_trace_endpoint_returns_404_when_missing(client):
    from scholarpath.db.redis import get_redis
    from scholarpath.main import app

    fake_redis = _InMemoryRedis()

    async def _override_redis():
        yield fake_redis

    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get("/api/chat/traces/not-found")
        assert response.status_code == 404
    finally:
        app.dependency_overrides.pop(get_redis, None)
