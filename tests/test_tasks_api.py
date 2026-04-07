from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_task_status_route_uses_normalized_path(client):
    resp = await client.get("/api/tasks/test-task-id")
    assert resp.status_code in (200, 501), resp.text
    payload = resp.json()
    if resp.status_code == 200:
        assert payload["task_id"] == "test-task-id"
        assert "status" in payload
    else:
        assert payload["detail"] == "Task queue not configured"


@pytest.mark.asyncio
async def test_legacy_task_status_path_removed(client):
    resp = await client.get("/api/tasks/tasks/test-task-id")
    assert resp.status_code == 404, resp.text
