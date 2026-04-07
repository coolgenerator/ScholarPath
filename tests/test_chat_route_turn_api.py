"""API tests for POST /api/chat/route-turn."""

from __future__ import annotations

import uuid

import pytest

from scholarpath.api.models.chat import ChatBlock, RoutePlan, TurnResult


class _InMemoryRedis:
    def __init__(self) -> None:
        self._strings: dict[str, str] = {}
        self._lists: dict[str, list[str]] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        self._sets: dict[str, set[str]] = {}

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False):
        if nx and key in self._strings:
            return False
        self._strings[key] = value
        return True

    async def get(self, key: str):
        return self._strings.get(key)

    async def delete(self, *keys: str):
        removed = 0
        for key in keys:
            removed += int(key in self._strings)
            self._strings.pop(key, None)
            self._lists.pop(key, None)
            self._hashes.pop(key, None)
            self._sets.pop(key, None)
        return removed

    async def eval(self, _script: str, _key_count: int, key: str, token: str):
        if self._strings.get(key) == token:
            self._strings.pop(key, None)
            return 1
        return 0

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

    async def expire(self, _key: str, _seconds: int):
        return 1

    async def sadd(self, key: str, value: str):
        bucket = self._sets.setdefault(key, set())
        if value in bucket:
            return 0
        bucket.add(value)
        return 1

    async def hset(self, key: str, field: str, value: str):
        self._hashes.setdefault(key, {})[field] = value
        return 1

    async def hgetall(self, key: str):
        return dict(self._hashes.get(key, {}))

    async def hdel(self, key: str, *fields: str):
        bucket = self._hashes.get(key, {})
        removed = 0
        for field in fields:
            if field in bucket:
                removed += 1
                bucket.pop(field, None)
        return removed


@pytest.mark.asyncio
async def test_route_turn_http_accepts_route_plan_and_returns_turn_result(client, monkeypatch):
    from scholarpath.api.deps import get_redis
    from scholarpath.main import app

    fake_redis = _InMemoryRedis()
    captured: dict[str, object] = {}

    async def _override_redis():
        yield fake_redis

    async def _fake_run_turn(self, *, session_id, student_id, message, route_plan, emit_event):
        captured["session_id"] = session_id
        captured["student_id"] = student_id
        captured["message"] = message
        captured["route_plan"] = route_plan
        return TurnResult(
            trace_id=str(uuid.uuid4()),
            status="ok",
            content="ok",
            blocks=[
                ChatBlock(
                    id=str(uuid.uuid4()),
                    kind="recommendation",
                    capability_id="recommendation_subagent",
                    order=0,
                    payload={"schools": [], "prefilter_meta": {}, "scenario_pack": {"baseline": [], "scenarios": []}},
                    meta={},
                )
            ],
            actions=[],
            usage={},
        )

    app.dependency_overrides[get_redis] = _override_redis
    monkeypatch.setattr("scholarpath.chat.agent.ChatAgent.run_turn", _fake_run_turn)
    try:
        payload = {
            "session_id": "sid-route-turn-1",
            "student_id": None,
            "message": "预算1万给我推荐学校",
            "route_plan": RoutePlan(
                primary_task="recommendation",
                modifiers=["memory_followup"],
                required_capabilities=["recommendation_subagent"],
                required_outputs=["recommendation_payload"],
                route_lock=True,
            ).model_dump(mode="json"),
        }
        response = await client.post("/api/chat/route-turn", json=payload)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["status"] == "ok"
        assert body["type"] == "turn.result"
        assert body["usage"]["lock_scope"] == "session"
        route_plan = captured.get("route_plan")
        assert isinstance(route_plan, RoutePlan)
        assert route_plan.primary_task == "recommendation"
        assert captured.get("message") == "预算1万给我推荐学校"
    finally:
        app.dependency_overrides.pop(get_redis, None)
