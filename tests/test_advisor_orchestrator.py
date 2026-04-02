"""Coordinator tests for advisor v1.1 orchestrator."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scholarpath.advisor.contracts import AdvisorRequest
from scholarpath.advisor.orchestration import (
    CapabilityContext,
    CapabilityDefinition,
    CapabilityRegistry,
    CapabilityResult,
)
from scholarpath.advisor.orchestrator import AdvisorOrchestrator
from tests.fake_redis import FakeRedis


class _StubLLM:
    def __init__(self, responses: list[dict]) -> None:
        self._responses = list(responses)

    async def complete_json(self, *args, **kwargs):  # noqa: ANN002, ANN003
        caller = str(kwargs.get("caller", ""))
        if caller == "advisor.router.plan":
            if len(self._responses) >= 2 and "domain" in self._responses[0]:
                domain_row = self._responses.pop(0)
                intent_row = self._responses.pop(0)
                payload = {
                    "domain": domain_row.get("domain", "common"),
                    "domain_confidence": domain_row.get(
                        "domain_confidence",
                        domain_row.get("confidence", 0.0),
                    ),
                    "intent_clarity": intent_row.get("intent_clarity", 0.9),
                }
                if isinstance(intent_row.get("candidates"), list):
                    payload["candidates"] = intent_row["candidates"]
                elif intent_row.get("capability"):
                    payload["candidates"] = [
                        {
                            "capability": intent_row.get("capability"),
                            "confidence": intent_row.get("confidence", 0.0),
                            "conflict_group": intent_row.get("conflict_group", "default"),
                        }
                    ]
                else:
                    payload["candidates"] = []
                return payload
            if self._responses:
                return self._responses.pop(0)
            return {"domain": "common", "domain_confidence": 0.0, "candidates": [], "intent_clarity": 0.0}
        if self._responses:
            return self._responses.pop(0)
        return {"domain": "common", "confidence": 0.0}


async def _ok_handler(ctx: CapabilityContext) -> CapabilityResult:
    return CapabilityResult(
        assistant_text=f"{ctx.capability} ok",
        step_summary={"message": f"{ctx.capability} completed"},
    )


async def _clarify_handler(_: CapabilityContext) -> CapabilityResult:
    return CapabilityResult(
        assistant_text="need clarification",
        step_summary={"message": "clarify requested"},
    )


async def _boom_handler(_: CapabilityContext) -> CapabilityResult:
    raise RuntimeError("boom")


def _make_registry(*, fail_capability: str | None = None) -> CapabilityRegistry:
    registry = CapabilityRegistry()
    for capability_id, domain, requires_student, description in (
        ("undergrad.school.recommend", "undergrad", True, "本科推荐"),
        ("undergrad.school.query", "undergrad", True, "院校问答"),
        ("undergrad.strategy.plan", "undergrad", True, "策略规划"),
        ("offer.compare", "offer", True, "Offer对比"),
        ("offer.decision", "offer", True, "Offer决策"),
        ("common.general", "common", False, "通用对话"),
        ("common.emotional_support", "common", False, "情绪支持"),
        ("common.clarify", "common", False, "澄清"),
    ):
        handler = _boom_handler if capability_id == fail_capability else _ok_handler
        if capability_id == "common.clarify":
            handler = _clarify_handler
        registry.register(
            CapabilityDefinition(
                capability_id=capability_id,  # type: ignore[arg-type]
                domain=domain,  # type: ignore[arg-type]
                description=description,
                requires_student=requires_student,
                handler=handler,
            )
        )
    return registry


@pytest.mark.asyncio
async def test_multi_intent_executes_max_two_and_queues_rest() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "undergrad", "confidence": 0.93},
            {
                "candidates": [
                    {
                        "capability": "undergrad.school.recommend",
                        "confidence": 0.94,
                        "conflict_group": "undergrad_recommend",
                    },
                    {
                        "capability": "undergrad.school.query",
                        "confidence": 0.81,
                        "conflict_group": "undergrad_query",
                    },
                    {
                        "capability": "undergrad.strategy.plan",
                        "confidence": 0.79,
                        "conflict_group": "undergrad_strategy",
                    },
                ]
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        turn_id="turn-1",
        session_id="session-1",
        student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
        message="给我推荐学校并顺便说说学校差异和申请策略",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "undergrad"
    assert resp.capability == "undergrad.school.recommend"
    assert len(resp.done) == 2
    assert all(step.status == "succeeded" for step in resp.done)
    assert len(resp.pending) == 1
    assert resp.pending[0].capability == "undergrad.strategy.plan"
    assert resp.pending[0].reason == "over_limit"
    assert any(action.action_id == "queue.run_pending" for action in resp.next_actions)
    assert "done:" in resp.assistant_text
    assert "pending:" in resp.assistant_text
    assert "next_actions:" in resp.assistant_text


@pytest.mark.asyncio
async def test_low_confidence_routes_to_clarify_without_business_execution() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "undergrad", "confidence": 0.9},
            {
                "candidates": [
                    {
                        "capability": "undergrad.school.recommend",
                        "confidence": 0.1,
                        "conflict_group": "undergrad_recommend",
                    }
                ]
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(session_id="session-2", message="你好，帮我看看？")

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "common"
    assert resp.capability == "common.clarify"
    assert resp.route_meta.fallback_used is True
    assert len(resp.done) == 1
    assert resp.done[0].capability == "common.clarify"
    assert resp.pending
    assert all(step.reason == "low_confidence" for step in resp.pending)


@pytest.mark.asyncio
async def test_conflict_routes_to_clarify_and_preserves_pending() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "offer", "confidence": 0.96},
            {
                "candidates": [
                    {
                        "capability": "offer.compare",
                        "confidence": 0.88,
                        "conflict_group": "offer_decision",
                    },
                    {
                        "capability": "offer.decision",
                        "confidence": 0.8,
                        "conflict_group": "offer_decision",
                    },
                ]
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-3",
        student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
        message="帮我比较这两个offer并直接给最终决策",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.capability == "common.clarify"
    assert resp.route_meta.fallback_used is True
    assert len(resp.done) == 1
    assert all(step.reason == "conflict" for step in resp.pending)


@pytest.mark.asyncio
async def test_capability_failure_retries_once_and_returns_recover_actions() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "undergrad", "confidence": 0.9},
            {
                "candidates": [
                    {
                        "capability": "undergrad.school.recommend",
                        "confidence": 0.95,
                        "conflict_group": "undergrad_recommend",
                    }
                ]
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(fail_capability="undergrad.school.recommend"),
    )
    req = AdvisorRequest(
        session_id="session-4",
        student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
        message="推荐学校",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert len(resp.done) == 1
    assert resp.done[0].capability == "undergrad.school.recommend"
    assert resp.done[0].status == "failed"
    assert resp.done[0].retry_count == 1
    assert resp.pending
    assert resp.pending[0].reason == "requires_user_trigger"
    assert any(action.action_id == "step.retry" for action in resp.next_actions)
    assert any(action.action_id == "queue.run_pending" for action in resp.next_actions)


@pytest.mark.asyncio
async def test_queue_run_pending_trigger_executes_specified_capability() -> None:
    redis = FakeRedis()
    llm = _StubLLM(
        responses=[
            {"domain": "undergrad", "confidence": 0.93},
            {
                "candidates": [
                    {
                        "capability": "undergrad.school.recommend",
                        "confidence": 0.94,
                        "conflict_group": "undergrad_recommend",
                    },
                    {
                        "capability": "undergrad.school.query",
                        "confidence": 0.81,
                        "conflict_group": "undergrad_query",
                    },
                    {
                        "capability": "undergrad.strategy.plan",
                        "confidence": 0.79,
                        "conflict_group": "undergrad_strategy",
                    },
                ]
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=redis,
        registry=_make_registry(),
    )

    first = await orchestrator.process(
        AdvisorRequest(
            turn_id="turn-1",
            session_id="session-5",
            student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
            message="推荐+问答+策略都做",
        )
    )
    assert any(step.capability == "undergrad.strategy.plan" for step in first.pending)

    second = await orchestrator.process(
        AdvisorRequest(
            turn_id="turn-2",
            session_id="session-5",
            student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
            message="继续",
            capability_hint="undergrad.strategy.plan",
            client_context={"trigger": "queue.run_pending"},
        )
    )

    assert second.error is None
    assert second.capability == "undergrad.strategy.plan"
    assert len(second.done) == 1
    assert second.done[0].capability == "undergrad.strategy.plan"
    assert all(step.capability != "undergrad.strategy.plan" for step in second.pending)


@pytest.mark.asyncio
async def test_capability_hint_has_primary_priority() -> None:
    llm = _StubLLM(
        responses=[
            {
                "candidates": [
                    {
                        "capability": "undergrad.school.query",
                        "confidence": 0.65,
                        "conflict_group": "undergrad_query",
                    }
                ]
            }
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-6",
        student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
        message="继续",
        capability_hint="undergrad.school.recommend",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "undergrad"
    assert resp.capability == "undergrad.school.recommend"
    assert resp.done
    assert resp.done[0].capability == "undergrad.school.recommend"


@pytest.mark.asyncio
async def test_invalid_student_id_returns_common_general_with_recover_actions() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "undergrad", "confidence": 0.9},
            {
                "candidates": [
                    {
                        "capability": "undergrad.school.recommend",
                        "confidence": 0.92,
                        "conflict_group": "undergrad_recommend",
                    }
                ]
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-7",
        student_id="not-a-uuid",
        message="给我推荐学校",
    )

    resp = await orchestrator.process(req)

    assert resp.capability == "common.general"
    assert resp.error is not None
    assert resp.error.code == "INVALID_INPUT"
    assert resp.route_meta.guard_result == "invalid_input"
    assert resp.route_meta.guard_reason == "invalid_input"
    assert any(action.action_id == "input.fix_student_id" for action in resp.next_actions)
    assert any(action.action_id == "route.clarify" for action in resp.next_actions)


@pytest.mark.asyncio
async def test_invalid_trigger_capability_returns_common_general_with_fix_action() -> None:
    llm = _StubLLM(responses=[])
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-8",
        message="继续",
        capability_hint="non.existent.capability",
        client_context={"trigger": "queue.run_pending"},
    )

    resp = await orchestrator.process(req)

    assert resp.capability == "common.general"
    assert resp.error is not None
    assert resp.error.code == "INVALID_INPUT"
    assert resp.route_meta.guard_result == "invalid_input"
    assert resp.route_meta.guard_reason == "trigger_invalid"
    assert any(action.action_id == "input.fix_capability_hint" for action in resp.next_actions)


@pytest.mark.asyncio
async def test_non_school_chat_routes_to_common_general() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "common", "confidence": 0.93},
            {"capability": "common.general", "confidence": 0.88, "intent_clarity": 0.9},
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-9",
        message="我们先简单聊聊怎么安排这一周的时间吧",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "common"
    assert resp.capability == "common.general"
    assert resp.route_meta.guard_result == "pass"
    assert resp.done
    assert resp.done[0].capability == "common.general"


@pytest.mark.asyncio
async def test_emotional_message_routes_to_common_emotional_support() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "common", "confidence": 0.95},
            {"capability": "common.emotional_support", "confidence": 0.91, "intent_clarity": 0.92},
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-10",
        message="最近申请压力很大，我有点焦虑",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "common"
    assert resp.capability == "common.emotional_support"
    assert resp.route_meta.guard_result == "pass"
    assert resp.done
    assert resp.done[0].capability == "common.emotional_support"


@pytest.mark.asyncio
async def test_common_multi_intent_executes_support_and_general_same_turn() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "common", "confidence": 0.96},
            {
                "candidates": [
                    {
                        "capability": "common.emotional_support",
                        "confidence": 0.91,
                        "conflict_group": "common_support",
                    },
                    {
                        "capability": "common.general",
                        "confidence": 0.86,
                        "conflict_group": "common_general",
                    },
                ],
                "intent_clarity": 0.92,
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-11",
        message="我最近压力很大，也想把portfolio素材整理一下",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "common"
    assert resp.capability == "common.emotional_support"
    assert len(resp.done) == 2
    assert {step.capability for step in resp.done} == {"common.emotional_support", "common.general"}
    assert resp.pending == []


@pytest.mark.asyncio
async def test_common_classifier_fallback_still_covers_emotion_plus_portfolio() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "common", "confidence": 0.9},
            {},
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-12",
        message="I feel anxious and want to organize my portfolio tonight.",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "common"
    assert len(resp.done) == 2
    assert {step.capability for step in resp.done} == {"common.emotional_support", "common.general"}


@pytest.mark.asyncio
async def test_offer_turn_can_attach_common_emotional_support_as_secondary() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "offer", "confidence": 0.96},
            {
                "candidates": [
                    {
                        "capability": "offer.compare",
                        "confidence": 0.93,
                        "conflict_group": "offer_decision",
                    }
                ],
                "intent_clarity": 0.9,
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-13",
        student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
        message="我有点焦虑，但也想先比较一下这两个offer",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "offer"
    assert resp.capability == "offer.compare"
    assert len(resp.done) == 2
    assert {step.capability for step in resp.done} == {"offer.compare", "common.emotional_support"}


@pytest.mark.asyncio
async def test_undergrad_turn_can_attach_common_general_for_portfolio_guidance() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "undergrad", "confidence": 0.95},
            {
                "candidates": [
                    {
                        "capability": "undergrad.school.recommend",
                        "confidence": 0.92,
                        "conflict_group": "undergrad_recommend",
                    }
                ],
                "intent_clarity": 0.92,
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-14",
        student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
        message="先给我推荐学校，并顺便说下我该怎么整理portfolio素材",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "undergrad"
    assert resp.capability == "undergrad.school.recommend"
    assert len(resp.done) == 2
    assert {step.capability for step in resp.done} == {"undergrad.school.recommend", "common.general"}


@pytest.mark.asyncio
async def test_ambiguous_emotional_message_prefers_clarify() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "common", "confidence": 0.94},
            {},
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-15",
        message="最近申请压力很大，不确定先做哪一步",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "common"
    assert resp.capability == "common.clarify"


@pytest.mark.asyncio
async def test_memory_word_does_not_trigger_emotional_secondary() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "offer", "confidence": 0.96},
            {
                "candidates": [
                    {
                        "capability": "offer.compare",
                        "confidence": 0.93,
                        "conflict_group": "offer_decision",
                    }
                ],
                "intent_clarity": 0.9,
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-16",
        student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
        message="Compare my offers under degraded memory context.",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "offer"
    assert resp.capability == "offer.compare"
    assert len(resp.done) == 1
    assert resp.done[0].capability == "offer.compare"


@pytest.mark.asyncio
async def test_multi_intent_prefers_school_primary_over_strategy_even_when_strategy_confidence_higher() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "undergrad", "confidence": 0.97},
            {
                "candidates": [
                    {
                        "capability": "undergrad.strategy.plan",
                        "confidence": 0.96,
                        "conflict_group": "undergrad_strategy",
                    },
                    {
                        "capability": "undergrad.school.recommend",
                        "confidence": 0.91,
                        "conflict_group": "undergrad_recommend",
                    },
                    {
                        "capability": "undergrad.school.query",
                        "confidence": 0.89,
                        "conflict_group": "undergrad_query",
                    },
                ],
                "intent_clarity": 0.92,
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-17",
        student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
        message="推荐+问答+策略一起做",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "undergrad"
    assert resp.capability in {"undergrad.school.recommend", "undergrad.school.query"}
    assert len(resp.done) == 2
    assert any(step.capability == "undergrad.strategy.plan" for step in resp.done)
    assert resp.pending


@pytest.mark.asyncio
async def test_offer_compare_vs_decision_large_gap_should_not_force_clarify() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "offer", "confidence": 0.97},
            {
                "candidates": [
                    {
                        "capability": "offer.compare",
                        "confidence": 0.96,
                        "conflict_group": "offer_decision",
                    },
                    {
                        "capability": "offer.decision",
                        "confidence": 0.78,
                        "conflict_group": "offer_decision",
                    },
                ],
                "intent_clarity": 0.9,
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-18",
        student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
        message="Compare my offers under degraded memory context.",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.domain == "offer"
    assert resp.capability in {"offer.compare", "offer.decision"}
    assert resp.route_meta.guard_result == "pass"


@pytest.mark.asyncio
async def test_strategy_message_with_profile_phrase_should_not_inject_common_general() -> None:
    llm = _StubLLM(
        responses=[
            {"domain": "undergrad", "confidence": 0.98},
            {
                "candidates": [
                    {
                        "capability": "undergrad.strategy.plan",
                        "confidence": 0.88,
                        "conflict_group": "undergrad_strategy",
                    }
                ],
                "intent_clarity": 0.9,
            },
        ]
    )
    orchestrator = AdvisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        redis=FakeRedis(),
        registry=_make_registry(),
    )
    req = AdvisorRequest(
        session_id="session-19",
        student_id="9b46c72c-d57b-48e5-8409-4c9770db0f2c",
        message="Build an ED/EA/RD application strategy for my profile.",
    )

    resp = await orchestrator.process(req)

    assert resp.error is None
    assert resp.capability == "undergrad.strategy.plan"
    assert resp.route_meta.guard_result == "pass"
    assert all(step.capability != "common.general" for step in resp.pending)
