"""Adapter tests for structured artifact normalization."""

from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock

import pytest

from scholarpath.advisor.adapters import build_default_registry
from scholarpath.advisor.orchestration import CapabilityContext
from scholarpath.chat.memory import ChatMemory
from tests.fake_redis import FakeRedis


class _StubLLM:
    async def complete(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return "ok"

    async def complete_json(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return {}


def _ctx(*, domain: str, capability: str, memory: ChatMemory, message: str = "test") -> CapabilityContext:
    return CapabilityContext(
        turn_id="turn-1",
        session_id="session-1",
        student_id=uuid.UUID("9b46c72c-d57b-48e5-8409-4c9770db0f2c"),
        message=message,
        locale="zh",
        domain=domain,  # type: ignore[arg-type]
        capability=capability,  # type: ignore[arg-type]
        client_context={},
        llm=_StubLLM(),  # type: ignore[arg-type]
        session=MagicMock(),
        memory=memory,
        conversation_context={},
    )


@pytest.mark.asyncio
async def test_recommend_adapter_emits_school_recommendation(monkeypatch) -> None:
    payload = {
        "narrative": "n",
        "schools": [
            {
                "school_name": "MIT",
                "tier": "reach",
                "overall_score": 0.9,
                "admission_probability": 0.2,
                "sub_scores": {"academic": 0.95},
            }
        ],
        "ea_recommendations": [],
    }

    async def _fake_recommend(*args, **kwargs):  # noqa: ANN002, ANN003
        return f"推荐完成\n[RECOMMENDATION]{json.dumps(payload, ensure_ascii=False)}"

    monkeypatch.setattr("scholarpath.advisor.adapters.handle_recommendation", _fake_recommend)

    redis = FakeRedis()
    memory = ChatMemory(redis)
    registry = build_default_registry()
    handler = registry.get("undergrad.school.recommend")
    assert handler is not None

    result = await handler.handler(_ctx(domain="undergrad", capability=handler.capability_id, memory=memory))

    assert "RECOMMENDATION" not in result.assistant_text
    assert len(result.artifacts) == 1
    assert result.artifacts[0].type == "school_recommendation"


@pytest.mark.asyncio
async def test_profile_intake_adapter_emits_guided_and_recommendation(monkeypatch) -> None:
    guided = {
        "questions": [
            {
                "id": "major",
                "title": "选择专业",
                "options": [{"label": "CS", "value": "CS"}],
                "allow_custom": True,
                "custom_placeholder": "",
                "multi_select": False,
            }
        ]
    }
    recommendation_payload = {
        "narrative": "done",
        "schools": [
            {
                "school_name": "CMU",
                "tier": "target",
                "overall_score": 0.8,
                "admission_probability": 0.5,
                "sub_scores": {"academic": 0.8},
            }
        ],
        "ea_recommendations": [],
    }

    async def _fake_guided(*args, **kwargs):  # noqa: ANN002, ANN003
        return (
            "[INTAKE_COMPLETE]资料已完整\n"
            f"[GUIDED_OPTIONS]{json.dumps(guided, ensure_ascii=False)}"
        )

    async def _fake_recommend(*args, **kwargs):  # noqa: ANN002, ANN003
        return f"推荐已生成\n[RECOMMENDATION]{json.dumps(recommendation_payload, ensure_ascii=False)}"

    monkeypatch.setattr("scholarpath.advisor.adapters.handle_guided_intake", _fake_guided)
    monkeypatch.setattr("scholarpath.advisor.adapters.handle_recommendation", _fake_recommend)

    redis = FakeRedis()
    memory = ChatMemory(redis)
    registry = build_default_registry()
    handler = registry.get("undergrad.profile.intake")
    assert handler is not None

    result = await handler.handler(_ctx(domain="undergrad", capability=handler.capability_id, memory=memory))

    assert "INTAKE_COMPLETE" not in result.assistant_text
    assert "GUIDED_OPTIONS" not in result.assistant_text
    artifact_types = sorted(a.type for a in result.artifacts)
    assert artifact_types == ["guided_intake", "school_recommendation"]


@pytest.mark.asyncio
async def test_offer_decision_adapter_emits_offer_comparison(monkeypatch) -> None:
    async def _fake_compare_offers(*args, **kwargs):  # noqa: ANN002, ANN003
        return {
            "offers": [{"school": "A"}],
            "comparison_matrix": {"net_cost": {"A": 30000}},
            "recommendation": "Choose A",
        }

    monkeypatch.setattr("scholarpath.advisor.adapters.compare_offers", _fake_compare_offers)

    redis = FakeRedis()
    memory = ChatMemory(redis)
    registry = build_default_registry()
    handler = registry.get("offer.decision")
    assert handler is not None

    result = await handler.handler(_ctx(domain="offer", capability=handler.capability_id, memory=memory))

    assert result.assistant_text == "Choose A"
    assert len(result.artifacts) == 1
    assert result.artifacts[0].type == "offer_comparison"


@pytest.mark.asyncio
async def test_offer_what_if_adapter_emits_what_if_result(monkeypatch) -> None:
    async def _fake_what_if(*args, **kwargs):  # noqa: ANN002, ANN003
        return "模拟结果：录取概率上升。"

    monkeypatch.setattr("scholarpath.advisor.adapters.handle_what_if", _fake_what_if)

    redis = FakeRedis()
    memory = ChatMemory(redis)
    await memory.save_context(
        "session-1",
        "last_what_if",
        {"interventions": {"student_ability": 0.9}, "deltas": {"admission_probability": 0.1}},
        domain="offer",
    )
    registry = build_default_registry()
    handler = registry.get("offer.what_if")
    assert handler is not None

    result = await handler.handler(_ctx(domain="offer", capability=handler.capability_id, memory=memory))

    assert result.assistant_text.startswith("模拟结果")
    assert len(result.artifacts) == 1
    assert result.artifacts[0].type == "what_if_result"
    assert result.artifacts[0].deltas["admission_probability"] == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_common_general_portfolio_message_emits_info_card_and_action() -> None:
    redis = FakeRedis()
    memory = ChatMemory(redis)
    registry = build_default_registry()
    handler = registry.get("common.general")
    assert handler is not None

    result = await handler.handler(
        _ctx(
            domain="common",
            capability=handler.capability_id,
            memory=memory,
            message="我想先把portfolio素材整理一下",
        )
    )

    assert any(getattr(artifact, "type", "") == "info_card" for artifact in result.artifacts)
    assert any(action.action_id == "common.start_portfolio" for action in result.actions)


@pytest.mark.asyncio
async def test_common_emotional_support_emits_support_artifact_and_actions() -> None:
    redis = FakeRedis()
    memory = ChatMemory(redis)
    registry = build_default_registry()
    handler = registry.get("common.emotional_support")
    assert handler is not None

    result = await handler.handler(
        _ctx(
            domain="common",
            capability=handler.capability_id,
            memory=memory,
            message="最近压力很大，感觉有点崩溃",
        )
    )

    assert any(getattr(artifact, "type", "") == "info_card" for artifact in result.artifacts)
    assert any(action.action_id == "support.switch_to_general" for action in result.actions)
    assert any(action.action_id == "route.clarify" for action in result.actions)


@pytest.mark.asyncio
async def test_recommend_adapter_polisher_updates_text_fields_only(monkeypatch) -> None:
    payload = {
        "narrative": "原始叙述",
        "schools": [
            {
                "school_name": "MIT",
                "tier": "reach",
                "overall_score": 0.91,
                "admission_probability": 0.21,
                "sub_scores": {"academic": 0.95},
            }
        ],
        "strategy_summary": "原始策略",
        "ea_recommendations": [],
    }

    async def _fake_recommend(*args, **kwargs):  # noqa: ANN002, ANN003
        return f"推荐完成\n[RECOMMENDATION]{json.dumps(payload, ensure_ascii=False)}"

    class _Polisher:
        async def polish_school_recommendation(self, *, llm, data, locale):  # noqa: ANN001
            return data.model_copy(
                update={
                    "narrative": "润色后的叙述",
                    "strategy_summary": "润色后的策略",
                }
            )

        async def polish_offer_recommendation(self, **kwargs):  # noqa: ANN003
            return kwargs.get("recommendation")

        async def polish_what_if_explanation(self, **kwargs):  # noqa: ANN003
            return kwargs.get("explanation")

    monkeypatch.setattr("scholarpath.advisor.adapters.handle_recommendation", _fake_recommend)
    monkeypatch.setattr("scholarpath.advisor.adapters.get_output_polisher", lambda: _Polisher())

    redis = FakeRedis()
    memory = ChatMemory(redis)
    registry = build_default_registry()
    handler = registry.get("undergrad.school.recommend")
    assert handler is not None

    result = await handler.handler(_ctx(domain="undergrad", capability=handler.capability_id, memory=memory))
    artifact = result.artifacts[0]
    assert artifact.type == "school_recommendation"
    assert artifact.data.narrative == "润色后的叙述"
    assert artifact.data.strategy_summary == "润色后的策略"
    assert artifact.data.schools[0].overall_score == pytest.approx(0.91)
    assert artifact.data.schools[0].admission_probability == pytest.approx(0.21)


@pytest.mark.asyncio
async def test_offer_compare_polisher_failure_falls_back(monkeypatch) -> None:
    async def _fake_compare_offers(*args, **kwargs):  # noqa: ANN002, ANN003
        return {
            "offers": [{"school": "A"}],
            "comparison_matrix": {"net_cost": {"A": 30000}},
            "recommendation": "Choose A",
        }

    class _FailingPolisher:
        async def polish_school_recommendation(self, *, llm, data, locale):  # noqa: ANN001
            return data

        async def polish_offer_recommendation(self, **kwargs):  # noqa: ANN003
            raise RuntimeError("style failed")

        async def polish_what_if_explanation(self, **kwargs):  # noqa: ANN003
            return kwargs.get("explanation")

    monkeypatch.setattr("scholarpath.advisor.adapters.compare_offers", _fake_compare_offers)
    monkeypatch.setattr("scholarpath.advisor.adapters.get_output_polisher", lambda: _FailingPolisher())

    redis = FakeRedis()
    memory = ChatMemory(redis)
    registry = build_default_registry()
    handler = registry.get("offer.compare")
    assert handler is not None

    result = await handler.handler(_ctx(domain="offer", capability=handler.capability_id, memory=memory))
    assert result.assistant_text == "Choose A"
    assert len(result.artifacts) == 1
    assert result.artifacts[0].type == "offer_comparison"
    assert result.artifacts[0].recommendation == "Choose A"


@pytest.mark.asyncio
async def test_offer_what_if_polisher_updates_assistant_and_artifact(monkeypatch) -> None:
    async def _fake_what_if(*args, **kwargs):  # noqa: ANN002, ANN003
        return "模拟结果：录取概率上升。"

    class _Polisher:
        async def polish_school_recommendation(self, *, llm, data, locale):  # noqa: ANN001
            return data

        async def polish_offer_recommendation(self, **kwargs):  # noqa: ANN003
            return kwargs.get("recommendation")

        async def polish_what_if_explanation(self, **kwargs):  # noqa: ANN003
            return "### 变化概览\n- 录取概率提升"

    monkeypatch.setattr("scholarpath.advisor.adapters.handle_what_if", _fake_what_if)
    monkeypatch.setattr("scholarpath.advisor.adapters.get_output_polisher", lambda: _Polisher())

    redis = FakeRedis()
    memory = ChatMemory(redis)
    await memory.save_context(
        "session-1",
        "last_what_if",
        {"interventions": {"student_ability": 0.9}, "deltas": {"admission_probability": 0.1}},
        domain="offer",
    )
    registry = build_default_registry()
    handler = registry.get("offer.what_if")
    assert handler is not None

    result = await handler.handler(_ctx(domain="offer", capability=handler.capability_id, memory=memory))
    assert result.assistant_text.startswith("### 变化概览")
    assert result.artifacts[0].type == "what_if_result"
    assert result.artifacts[0].explanation.startswith("### 变化概览")
    assert result.artifacts[0].deltas["admission_probability"] == pytest.approx(0.1)
