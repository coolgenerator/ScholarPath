from __future__ import annotations

import uuid

import pytest


@pytest.mark.asyncio
async def test_route_turn_returns_route_meta_and_execution_digest(client, monkeypatch):
    from scholarpath.chat import agent as agent_module
    import scholarpath.llm.client as llm_client_module

    class DummyAgent:
        def __init__(self, llm, session, redis):  # noqa: ANN001
            self._llm = llm
            self._session = session
            self._redis = redis

        async def process_turn(self, **kwargs):  # noqa: ANN003
            return {
                "response_text": (
                    "ok\n[RECOMMENDATION]"
                    "{\"narrative\":\"\",\"schools\":[],\"ed_recommendation\":null,"
                    "\"ea_recommendations\":[],\"strategy_summary\":\"\"}"
                ),
                "intent": "recommendation",
                "route_meta": {
                    "route_source": "route_plan",
                    "primary_task": "recommendation",
                    "skill_id": "recommendation.budget_first",
                },
                "execution_digest": {
                    "required_output_missing": False,
                    "forced_retry_count": 0,
                    "cap_degraded": False,
                    "reason_code": None,
                    "needs_input": [],
                    "next_steps": [],
                },
            }

    monkeypatch.setattr(agent_module, "ChatAgent", DummyAgent)
    monkeypatch.setattr(llm_client_module, "get_llm_client", lambda: object())

    payload = {
        "session_id": "test-route-turn-001",
        "student_id": str(uuid.uuid4()),
        "message": "Budget is 10000, recommend schools.",
        "route_plan": {
            "primary_task": "recommendation",
            "modifiers": ["budget_first"],
            "required_capabilities": ["recommendation"],
            "required_outputs": ["recommendation_payload"],
            "route_lock": True,
        },
        "skill_id": "recommendation.budget_first",
    }
    resp = await client.post("/api/chat/route-turn", json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["intent"] == "recommendation"
    assert body["route_meta"]["route_source"] == "route_plan"
    assert body["route_meta"]["skill_id"] == "recommendation.budget_first"
    assert body["execution_digest"]["required_output_missing"] is False
    assert body["recommendation"] is not None
