"""Tests for Advisor Orchestrator V2 runtime behavior."""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import MagicMock

import pytest

import scholarpath.chat.orchestrator_v2 as orchestrator_module
from scholarpath.api.models.chat import RoutePlan
from scholarpath.chat.handlers.profile_ops import (
    PENDING_PROFILE_PATCH_KEY,
    PROFILE_CONFIRM_COMMAND_PREFIX,
    PROFILE_PATCH_TTL_USER_TURNS,
)
from scholarpath.chat.orchestrator_v2 import (
    AdvisorOrchestratorV2,
    CapabilityResult,
    CapabilitySpec,
    PlannedCapability,
)
from scholarpath.services.student_service import create_student, get_student


class _StubLLM:
    async def complete_json(self, _messages, **_kwargs):
        return {"capabilities": ["general"]}

    async def complete(self, _messages, **_kwargs):
        return "ok"


class _StubMemory:
    async def get_history(self, _session_id: str, limit: int = 20):
        return []

    async def get_context(self, _session_id: str):
        return {}

    async def save_context(self, _session_id: str, _key: str, _value):
        return None


class _LayeredStubMemory(_StubMemory):
    async def get_context_layers(self, _session_id: str):
        return {
            "legacy": {"legacy_only": "x"},
            "long_term": {"preferred_regions": ["US"]},
            "working": {"intake_step": 1},
            "short_term": {"current_school_name": "MIT"},
            "merged": {
                "legacy_only": "x",
                "preferred_regions": ["US"],
                "intake_step": 1,
                "current_school_name": "MIT",
            },
        }

    async def get_student_context_layers(self, _student_id):
        return {
            "long_term": {"profile_budget_usd": 70000, "preferred_regions": ["US", "UK"]},
            "working": {"intake_complete": False},
            "short_term": {"last_profile_changed_fields": ["academics.gpa"]},
            "merged": {
                "profile_budget_usd": 70000,
                "preferred_regions": ["US", "UK"],
                "intake_complete": False,
                "last_profile_changed_fields": ["academics.gpa"],
            },
        }


class _StateMemory:
    def __init__(self):
        self.history: dict[str, list[dict[str, str]]] = {}
        self.context: dict[str, dict[str, object]] = {}

    async def get_history(self, session_id: str, limit: int = 20):
        return self.history.get(session_id, [])[-limit:]

    async def get_context(self, session_id: str):
        return dict(self.context.get(session_id, {}))

    async def save_context(self, session_id: str, key: str, value):
        self.context.setdefault(session_id, {})[key] = value

    def push_user(self, session_id: str, content: str) -> None:
        self.history.setdefault(session_id, []).append({"role": "user", "content": content})


def _spec(
    capability_id: str,
    *,
    deps: list[str],
    execute,
    requires_db_session: bool = False,
    failure_policy: str = "fatal",
) -> CapabilitySpec:
    return CapabilitySpec(
        id=capability_id,
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        dependencies=deps,
        cost_class="test",
        execute=execute,
        requires_db_session=requires_db_session,
        failure_policy=failure_policy,  # type: ignore[arg-type]
    )


def _planned(
    capability_id: str,
    *,
    order: int,
    deps: list[str] | None = None,
) -> PlannedCapability:
    return PlannedCapability(
        id=capability_id,
        is_primary=(order == 0),
        dependencies=deps or [],
        plan_order=order,
    )


def test_capability_timeout_uses_soft_override_for_strategy_and_recommendation():
    strategy_spec = CapabilitySpec(
        id="strategy",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        dependencies=[],
        cost_class="test",
        execute=lambda _ctx: asyncio.sleep(0),  # type: ignore[arg-type]
        failure_policy="best_effort",
    )
    recommendation_spec = CapabilitySpec(
        id="recommendation_subagent",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        dependencies=[],
        cost_class="test",
        execute=lambda _ctx: asyncio.sleep(0),  # type: ignore[arg-type]
        failure_policy="best_effort",
    )
    generic_spec = CapabilitySpec(
        id="what_if",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        dependencies=[],
        cost_class="test",
        execute=lambda _ctx: asyncio.sleep(0),  # type: ignore[arg-type]
        failure_policy="best_effort",
    )

    assert AdvisorOrchestratorV2._capability_timeout_seconds(strategy_spec) == pytest.approx(8.0)
    assert AdvisorOrchestratorV2._capability_timeout_seconds(recommendation_spec) == pytest.approx(8.0)
    assert AdvisorOrchestratorV2._capability_timeout_seconds(generic_spec) == pytest.approx(6.0)


@pytest.mark.asyncio
async def test_execute_plan_runs_ready_nodes_in_parallel_then_unlocks_dependency():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    starts: dict[str, float] = {}
    finishes: dict[str, float] = {}

    def _runner(name: str, delay: float):
        async def _execute(_ctx) -> CapabilityResult:
            loop_time = asyncio.get_running_loop().time()
            starts[name] = loop_time
            await asyncio.sleep(delay)
            finishes[name] = asyncio.get_running_loop().time()
            return CapabilityResult(
                content=f"{name} done",
                blocks=[{"kind": "text", "payload": {"text": name}, "meta": {}}],
                meta={},
            )

        return _execute

    orchestrator._registry = {
        "alpha": _spec("alpha", deps=[], execute=_runner("alpha", 0.05)),
        "beta": _spec("beta", deps=[], execute=_runner("beta", 0.05)),
        "gamma": _spec("gamma", deps=["alpha"], execute=_runner("gamma", 0.0)),
    }

    planned = [
        _planned("alpha", order=0),
        _planned("beta", order=1),
        _planned("gamma", order=2, deps=["alpha"]),
    ]

    events = []

    async def _emit(event):
        events.append(event)

    execution = await orchestrator._execute_plan(
        planned=planned,
        message="test",
        session_id="sid-1",
        student_id=uuid.uuid4(),
        context={},
        shared={},
        trace_id="trace-1",
        emit_event=_emit,
    )
    results = execution["results"]

    assert set(results.keys()) == {"alpha", "beta", "gamma"}
    assert abs(starts["alpha"] - starts["beta"]) < 0.06
    assert starts["gamma"] >= finishes["alpha"]
    assert results["alpha"][1] == 1
    assert results["beta"][1] == 2
    assert results["gamma"][1] == 3

    started = [event for event in events if event.event == "capability_started"]
    finished = [event for event in events if event.event == "capability_finished"]
    assert len(started) == 6
    assert len(finished) == 3
    assert {item.data["step_status"] for item in started} == {"queued", "running"}


@pytest.mark.asyncio
async def test_execute_plan_serializes_db_capabilities():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    active = 0
    max_active = 0
    state_lock = asyncio.Lock()

    async def _db_runner(_ctx) -> CapabilityResult:
        nonlocal active, max_active
        async with state_lock:
            active += 1
            max_active = max(max_active, active)
        await asyncio.sleep(0.04)
        async with state_lock:
            active -= 1
        return CapabilityResult(
            content="db ok",
            blocks=[{"kind": "text", "payload": {"text": "db"}, "meta": {}}],
            meta={},
        )

    orchestrator._registry = {
        "db_a": _spec("db_a", deps=[], execute=_db_runner, requires_db_session=True),
        "db_b": _spec("db_b", deps=[], execute=_db_runner, requires_db_session=True),
    }

    await orchestrator._execute_plan(
        planned=[_planned("db_a", order=0), _planned("db_b", order=1)],
        message="run db caps",
        session_id="sid-db-serial",
        student_id=uuid.uuid4(),
        context={},
        shared={},
        trace_id="trace-db-serial",
        emit_event=lambda _event: asyncio.sleep(0),
    )

    assert max_active == 1


@pytest.mark.asyncio
async def test_execute_plan_keeps_non_db_capabilities_parallel():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    active = 0
    max_active = 0
    state_lock = asyncio.Lock()

    async def _non_db_runner(_ctx) -> CapabilityResult:
        nonlocal active, max_active
        async with state_lock:
            active += 1
            max_active = max(max_active, active)
        await asyncio.sleep(0.04)
        async with state_lock:
            active -= 1
        return CapabilityResult(
            content="ok",
            blocks=[{"kind": "text", "payload": {"text": "ok"}, "meta": {}}],
            meta={},
        )

    orchestrator._registry = {
        "a": _spec("a", deps=[], execute=_non_db_runner, requires_db_session=False),
        "b": _spec("b", deps=[], execute=_non_db_runner, requires_db_session=False),
    }

    await orchestrator._execute_plan(
        planned=[_planned("a", order=0), _planned("b", order=1)],
        message="run non db caps",
        session_id="sid-non-db-parallel",
        student_id=uuid.uuid4(),
        context={},
        shared={},
        trace_id="trace-non-db-parallel",
        emit_event=lambda _event: asyncio.sleep(0),
    )

    assert max_active >= 2


@pytest.mark.asyncio
async def test_execute_plan_wave_checkpoint_can_add_followup_capability(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _guided(_ctx) -> CapabilityResult:
        return CapabilityResult(
            content="intake done",
            blocks=[{"kind": "text", "payload": {"text": "done"}, "meta": {}}],
            meta={"intake_complete": True},
        )

    async def _recommend(_ctx) -> CapabilityResult:
        return CapabilityResult(
            content="recommendations",
            blocks=[{"kind": "recommendation", "payload": {"schools": []}, "meta": {}}],
            meta={},
        )

    orchestrator._registry = {
        "guided_intake": _spec("guided_intake", deps=[], execute=_guided),
        "recommendation_subagent": _spec(
            "recommendation_subagent",
            deps=["guided_intake"],
            execute=_recommend,
        ),
    }

    async def _noop_llm_checkpoint(**_kwargs):
        return {"add": [], "drop": [], "reprioritize": []}

    monkeypatch.setattr(orchestrator, "_checkpoint_with_llm", _noop_llm_checkpoint)

    execution = await orchestrator._execute_plan(
        planned=[_planned("guided_intake", order=0)],
        message="complete intake then recommend",
        session_id="sid-wave",
        student_id=uuid.uuid4(),
        context={},
        shared={},
        trace_id="trace-wave",
        emit_event=lambda _event: asyncio.sleep(0),
    )

    assert set(execution["results"].keys()) == {"guided_intake", "recommendation_subagent"}
    assert execution["wave_count"] >= 2


def test_aggregate_result_keeps_stable_block_order():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    planned = [
        _planned("first", order=0),
        _planned("second", order=1),
    ]
    node_results = {
        "second": (
            CapabilityResult(
                content="second content",
                blocks=[{"kind": "offer_compare", "payload": {"schools": []}, "meta": {}}],
                meta={},
            ),
            2,
        ),
        "first": (
            CapabilityResult(
                content="first content",
                blocks=[{"kind": "recommendation", "payload": {"schools": []}, "meta": {}}],
                meta={},
            ),
            1,
        ),
    }

    result, aggregate_stats = orchestrator._aggregate_result(
        trace_id="trace-order",
        planned=planned,
        node_results=node_results,
    )

    assert result.status == "ok"
    assert [block.capability_id for block in result.blocks] == ["answer_synthesis", "first", "second"]
    assert [block.order for block in result.blocks] == [0, 1, 2]
    assert result.blocks[0].kind == "answer_synthesis"
    assert isinstance(result.blocks[0].payload.get("conclusion"), str)
    assert result.content
    assert aggregate_stats["output_compacted"] is False


def test_aggregate_result_compacts_long_output_payload():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    planned = [_planned("verbose", order=0)]
    long_text = "关键结论: 录取概率 42%。预算 70000。下一步行动。" + (" details " * 250)
    node_results = {
        "verbose": (
            CapabilityResult(
                content=long_text,
                blocks=[
                    {
                        "kind": "text",
                        "payload": {"text": "x" * 4200, "details": {"raw": "y" * 4200}},
                        "meta": {},
                    }
                ],
                meta={},
            ),
            1,
        )
    }

    result, aggregate_stats = orchestrator._aggregate_result(
        trace_id="trace-compact",
        planned=planned,
        node_results=node_results,
    )

    assert result.status == "ok"
    assert aggregate_stats["output_compacted"] is True
    assert len(result.content) < len(long_text)
    synthesis_block = result.blocks[0]
    assert synthesis_block.kind == "answer_synthesis"
    assert synthesis_block.meta is not None
    assert synthesis_block.meta.get("compacted") is True


def test_aggregate_result_memory_followup_skill_uses_constraints():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    planned = [_planned("recommendation_subagent", order=0)]
    node_results = {
        "recommendation_subagent": (
            CapabilityResult(
                content="ok",
                blocks=[{"kind": "recommendation", "payload": {"schools": []}, "meta": {}}],
                meta={
                    "what_done": "Generated recommendation payload and narrative from current profile context.",
                    "personalization_evidence": {
                        "facts_used": ["GPA: 3.6", "SAT: 1480"],
                        "constraints_used": [],
                        "missing_fields": [],
                        "confidence": 0.8,
                    },
                },
            ),
            1,
        )
    }
    result, _stats = orchestrator._aggregate_result(
        trace_id="trace-memory-followup",
        planned=planned,
        node_results=node_results,
        message="基于我刚才的偏好（预算上限45000美元、偏好大城市、CS+AI）给我下一步建议",
        context={
            "profile_budget_usd": 45000,
            "profile_intended_majors": ["Computer Science", "AI"],
        },
    )

    synthesis_block = next(block for block in result.blocks if block.kind == "answer_synthesis")
    assert synthesis_block.meta is not None
    assert synthesis_block.meta.get("task_skill") == "memory_followup"
    conclusion = str(synthesis_block.payload.get("conclusion", ""))
    assert "payload" not in conclusion.lower()
    actions = synthesis_block.payload.get("actions") or []
    assert len(actions) >= 2
    action_text = " ".join(str(item.get("step", "")) for item in actions if isinstance(item, dict))
    assert "预算" in action_text or "CS" in action_text or "城市" in action_text


def test_aggregate_result_guided_intake_skill_outputs_questions():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    planned = [_planned("guided_intake", order=0)]
    node_results = {
        "guided_intake": (
            CapabilityResult(
                content="ok",
                blocks=[{"kind": "guided_questions", "payload": {"questions": []}, "meta": {}}],
                meta={
                    "what_done": "Collected missing intake fields and advanced the intake workflow.",
                    "personalization_evidence": {
                        "facts_used": [],
                        "constraints_used": [],
                        "missing_fields": ["academics.gpa", "finance.budget_usd", "academics.intended_majors"],
                        "confidence": 0.7,
                    },
                },
            ),
            1,
        )
    }
    result, _stats = orchestrator._aggregate_result(
        trace_id="trace-guided-intake",
        planned=planned,
        node_results=node_results,
        message="继续下一组问题，尽量具体",
        context={"profile_missing_fields": ["academics.gpa", "finance.budget_usd", "academics.intended_majors"]},
    )

    synthesis_block = next(block for block in result.blocks if block.kind == "answer_synthesis")
    assert synthesis_block.meta is not None
    assert synthesis_block.meta.get("task_skill") == "guided_intake"
    actions = synthesis_block.payload.get("actions") or []
    assert len(actions) == 1
    assert any("？" in str(item.get("step", "")) for item in actions if isinstance(item, dict))


def test_aggregate_result_profile_update_skill_outputs_confirm_flow():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    planned = [_planned("profile_update", order=0)]
    node_results = {
        "profile_update": (
            CapabilityResult(
                content="proposal ready",
                blocks=[{"kind": "profile_patch_proposal", "payload": {"proposal_id": "p1"}, "meta": {}}],
                meta={"what_done": "Prepared profile patch proposal for confirmation."},
            ),
            1,
        )
    }
    result, _stats = orchestrator._aggregate_result(
        trace_id="trace-profile-update",
        planned=planned,
        node_results=node_results,
        message="请更新我的gpa为3.9",
        context={
            PENDING_PROFILE_PATCH_KEY: {
                "confirm_command": "confirm_profile_patch:p1",
                "reedit_command": "reedit_profile_patch:p1",
            }
        },
    )
    synthesis_block = next(block for block in result.blocks if block.kind == "answer_synthesis")
    assert synthesis_block.meta is not None
    assert synthesis_block.meta.get("task_skill") == "profile_update"
    actions = synthesis_block.payload.get("actions") or []
    joined = " ".join(str(item.get("step", "")) for item in actions if isinstance(item, dict))
    assert "confirm_profile_patch:p1" in joined


def test_aggregate_result_multi_intent_skill_adds_focus_risk():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    planned = [
        _planned("recommendation_subagent", order=0),
        _planned("strategy", order=1),
    ]
    node_results = {
        "recommendation_subagent": (
            CapabilityResult(
                content="recommendation ready",
                blocks=[{"kind": "recommendation", "payload": {"schools": []}, "meta": {}}],
                meta={},
            ),
            1,
        ),
        "strategy": (
            CapabilityResult(
                content="strategy ready",
                blocks=[{"kind": "text", "payload": {"text": "strategy ready"}, "meta": {}}],
                meta={},
            ),
            2,
        ),
    }
    result, _stats = orchestrator._aggregate_result(
        trace_id="trace-multi-intent",
        planned=planned,
        node_results=node_results,
        message="同时做选校推荐和策略规划",
        context={},
    )
    synthesis_block = next(block for block in result.blocks if block.kind == "answer_synthesis")
    assert synthesis_block.meta is not None
    assert synthesis_block.meta.get("task_skill") == "multi_intent"
    risks = synthesis_block.payload.get("risks_missing") or []
    assert any("聚焦" in str(item) for item in risks)


@pytest.mark.asyncio
async def test_run_guided_intake_returns_single_question_payload(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _fake_handle_guided_intake(*_args, **_kwargs):
        return {
            "content": "请先回答一个问题",
            "guided_questions": [
                {"id": "q1", "title": "Q1"},
                {"id": "q2", "title": "Q2"},
                {"id": "q3", "title": "Q3"},
            ],
            "intake_complete": False,
        }

    monkeypatch.setattr(orchestrator_module, "handle_guided_intake", _fake_handle_guided_intake)

    result = await orchestrator._run_guided_intake(
        orchestrator_module.CapabilityContext(
            llm=_StubLLM(),
            session=MagicMock(),
            memory=_StubMemory(),
            session_id="sid-guided-single",
            student_id=uuid.uuid4(),
            message="继续提问",
            conversation_context={},
            shared={},
        )
    )
    guided_block = next(block for block in result.blocks if block["kind"] == "guided_questions")
    payload = guided_block.get("payload") or {}
    assert len(payload.get("questions") or []) == 1
    assert len(payload.get("next_turn_candidates") or []) == 2


def test_resolve_synthesis_skill_prefers_what_if_message_signal():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    skill = orchestrator._resolve_synthesis_skill(
        planned=[_planned("guided_intake", order=0)],
        ordered_capabilities=[
            ("guided_intake", (CapabilityResult(content="ok", blocks=[], meta={}), 1))
        ],
        message="如果我把SAT从1380提到1460，录取概率会怎么变化？",
        context={},
    )
    assert skill == "what_if"


def test_resolve_synthesis_skill_uses_history_for_what_if_followup():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    skill = orchestrator._resolve_synthesis_skill(
        planned=[_planned("guided_intake", order=0)],
        ordered_capabilities=[
            ("guided_intake", (CapabilityResult(content="ok", blocks=[], meta={}), 1))
        ],
        message="请给最值得做的两个投入动作。",
        context={"recent_messages": "user: 如果我把SAT从1380提到1460，录取概率和策略会怎么变化？"},
    )
    assert skill == "what_if"


@pytest.mark.parametrize(
    ("message", "capability_ids", "history_text", "expected_skill"),
    [
        ("confirm_profile_patch:abc123ef", ["profile_update"], "", "profile_update"),
        ("这轮又超时了，先重试", ["strategy"], "", "robustness"),
        ("基于我刚才的偏好继续给建议", ["recommendation_subagent"], "user: 预算 45000", "recommendation"),
        ("请继续提问，把信息补齐", ["guided_intake"], "", "guided_intake"),
        ("如果我把SAT从1380提到1460会怎样", ["what_if"], "", "what_if"),
        ("帮我比较这两个offer", ["offer_compare"], "", "offer_compare"),
        ("给我选校推荐清单", ["recommendation_subagent"], "", "recommendation"),
        ("帮我规划ED/EA/RD时间线", ["strategy"], "", "strategy"),
        ("MIT的CS本科怎么样", ["school_query"], "", "school_query"),
        ("同时做选校推荐和策略规划", ["recommendation_subagent", "strategy"], "", "multi_intent"),
        ("我最近很焦虑，怕申请来不及", ["emotional_support"], "", "emotional_support"),
        ("随便聊聊", ["general"], "", "default"),
    ],
)
def test_resolve_active_skill_routes_expected(
    message: str,
    capability_ids: list[str],
    history_text: str,
    expected_skill: str,
):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    decision = orchestrator._resolve_active_skill(
        message=message,
        context={"recent_messages": history_text, "intent_split_source": "heuristic"},
        capability_ids=capability_ids,
    )
    assert decision.active_skill_id == expected_skill
    assert decision.source


def test_resolve_active_skill_keeps_memory_followup_as_modifier_only():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    decision = orchestrator._resolve_active_skill(
        message="基于我刚才的偏好继续给建议",
        context={"recent_messages": "user: 预算 45000", "intent_split_source": "heuristic"},
        capability_ids=["recommendation_subagent"],
    )
    assert decision.active_skill_id == "recommendation"
    assert "memory_followup" in set(decision.modifiers)


def test_resolve_active_skill_obeys_route_plan_lock():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    route_plan = orchestrator._normalize_route_plan(
        RoutePlan(
            primary_task="recommendation",
            modifiers=["memory_followup"],
            required_capabilities=["recommendation_subagent"],
            required_outputs=["recommendation_payload"],
            route_lock=True,
        )
    )
    decision = orchestrator._resolve_active_skill(
        message="随便聊聊",
        context={"recent_messages": "", "intent_split_source": "heuristic"},
        capability_ids=["general", "recommendation_subagent"],
        route_plan=route_plan,
    )
    assert decision.active_skill_id == "recommendation"
    assert decision.source == "route_plan_lock"
    assert "memory_followup" in set(decision.modifiers)


@pytest.mark.asyncio
async def test_run_turn_rolls_back_on_capability_failure(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _split(*, message: str, context: dict):
        return ["boom"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    async def _boom(_ctx):
        raise RuntimeError("boom")

    orchestrator._registry = {
        "boom": _spec("boom", deps=[], execute=_boom),
    }

    events = []

    async def _emit(event):
        events.append(event)

    result = await orchestrator.run_turn(
        session_id="sid-fail",
        student_id=uuid.uuid4(),
        message="trigger failure",
        emit_event=_emit,
    )

    assert result.status == "error"
    assert len(result.blocks) == 1
    assert result.blocks[0].kind == "error"
    assert any(event.event == "rollback" for event in events)
    assert events[-1].event == "turn_completed"


@pytest.mark.asyncio
async def test_run_turn_emits_protocol_events_and_result_schema(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _split(*, message: str, context: dict):
        return ["first", "second"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    async def _first(_ctx):
        return CapabilityResult(
            content="first answer",
            blocks=[{"kind": "text", "payload": {"text": "first answer"}, "meta": {}}],
            meta={},
        )

    async def _second(_ctx):
        return CapabilityResult(
            content="second answer",
            blocks=[{"kind": "what_if", "payload": {"deltas": []}, "meta": {}}],
            meta={},
        )

    orchestrator._registry = {
        "first": _spec("first", deps=[], execute=_first),
        "second": _spec("second", deps=[], execute=_second),
    }

    events = []

    async def _emit(event):
        events.append(event)

    result = await orchestrator.run_turn(
        session_id="sid-ok",
        student_id=uuid.uuid4(),
        message="run two capabilities",
        emit_event=_emit,
    )

    assert result.type == "turn.result"
    assert result.status == "ok"
    assert len(result.blocks) >= 2
    assert all(block.order >= 0 for block in result.blocks)
    assert result.usage["tool_steps_used"] == 2
    assert result.usage["tool_step_budget"] == orchestrator_module.MAX_TOOL_STEPS_PER_TURN
    assert result.usage["wave_count"] >= 1
    assert events[0].type == "turn.event"
    assert events[0].event == "turn_started"
    assert events[1].event == "planning_done"
    assert events[-1].event == "turn_completed"
    assert sum(1 for event in events if event.event == "capability_started") == 4
    assert sum(1 for event in events if event.event == "capability_finished") == 2
    planning_event = events[1]
    assert planning_event.data["trace_id"] == result.trace_id
    assert isinstance(planning_event.data["event_seq"], int)
    assert planning_event.data["step_id"]
    assert planning_event.data["step_kind"] == "wave"
    assert planning_event.data["step_status"] == "completed"
    assert planning_event.data["phase"] == "planning"
    assert planning_event.data["wave_index"] == 0
    assert "context_chars_before" in planning_event.data
    assert "context_chars_after" in planning_event.data
    assert "compression_level" in planning_event.data
    assert "summary_node_count" in planning_event.data
    assert "profile_fact_count" in planning_event.data
    assert "active_skill_id" in planning_event.data
    assert "skill_route_source" in planning_event.data
    assert "skill_contract_version" in planning_event.data
    completed_event = events[-1]
    assert completed_event.data["tool_steps_used"] == 2
    assert completed_event.data["tool_step_budget"] == orchestrator_module.MAX_TOOL_STEPS_PER_TURN
    assert "input_compacted" in completed_event.data
    assert "context_compacted" in completed_event.data
    assert "output_compacted" in completed_event.data
    assert "compression_passes" in completed_event.data
    assert "context_chars" in completed_event.data
    assert "active_skill_id" in completed_event.data
    assert "skill_route_source" in completed_event.data
    assert "skill_contract_version" in completed_event.data
    assert completed_event.data["step_kind"] == "turn"
    assert completed_event.data["phase"] == "finalization"
    assert completed_event.data["step_status"] == "completed"
    assert isinstance(completed_event.data["event_seq"], int)
    assert "display" in completed_event.data
    assert "metrics" in completed_event.data
    assert "input_compacted" in result.usage
    assert "context_compacted" in result.usage
    assert "output_compacted" in result.usage
    assert "compression_passes" in result.usage
    assert "context_chars" in result.usage
    assert "duration_ms" in result.usage
    assert "active_skill_id" in result.usage
    assert "skill_route_source" in result.usage
    assert "skill_contract_version" in result.usage
    cap_started = [event for event in events if event.event == "capability_started"]
    cap_finished = [event for event in events if event.event == "capability_finished"]
    assert cap_started[0].data["step_kind"] == "capability"
    assert {item.data["step_status"] for item in cap_started} == {"queued", "running"}
    assert cap_finished[0].data["step_kind"] == "capability"
    assert cap_finished[0].data["step_status"] == "completed"
    assert "prompt_chars" in cap_finished[0].data
    assert "max_tokens" in cap_finished[0].data
    assert "db_session_serialized" in cap_finished[0].data
    assert "metrics" in cap_finished[0].data
    assert "prompt_chars" in cap_finished[0].data["metrics"]
    assert "max_tokens" in cap_finished[0].data["metrics"]
    assert "db_session_serialized" in cap_finished[0].data["metrics"]
    started_step_ids = {item.data["step_id"] for item in cap_started}
    finished_step_ids = {item.data["step_id"] for item in cap_finished}
    assert finished_step_ids.issubset(started_step_ids)


@pytest.mark.asyncio
async def test_run_turn_prefers_layered_memory_snapshot(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_LayeredStubMemory(),
    )

    captured: dict[str, object] = {}

    async def _split(*, message: str, context: dict):
        captured["message"] = message
        captured["memory_layers"] = context.get("memory_layers")
        captured["preferred_regions"] = context.get("preferred_regions")
        captured["current_school_name"] = context.get("current_school_name")
        captured["profile_budget_usd"] = context.get("profile_budget_usd")
        captured["compressed_user_message"] = context.get("compressed_user_message")
        captured["context_management"] = context.get("context_management")
        return ["general"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    result = await orchestrator.run_turn(
        session_id="sid-layered",
        student_id=uuid.uuid4(),
        message="hello",
        emit_event=lambda _event: asyncio.sleep(0),
    )

    assert result.status == "ok"
    memory_layers = captured.get("memory_layers")
    assert isinstance(memory_layers, dict)
    assert memory_layers["short_term"]["current_school_name"] == "MIT"
    assert memory_layers["working"]["intake_step"] == 1
    assert memory_layers["long_term"]["preferred_regions"] == ["US"]
    assert captured["preferred_regions"] in (["US"], ["US", "UK"])
    assert captured["current_school_name"] == "MIT"
    assert captured["profile_budget_usd"] in (70000, None)
    assert isinstance(captured["compressed_user_message"], str)
    context_management = captured["context_management"]
    assert isinstance(context_management, dict)
    assert "context_budget_chars" in context_management


@pytest.mark.asyncio
async def test_checkpoint_soft_intake_gate_keeps_recommendation_when_not_hard_missing(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    plan_map = {
        "guided_intake": _planned("guided_intake", order=0),
        "recommendation_subagent": _planned("recommendation_subagent", order=1, deps=["guided_intake"]),
    }
    pending = {"recommendation_subagent"}
    completed = {"guided_intake"}
    results = {
        "guided_intake": (
            CapabilityResult(
                content="collecting",
                blocks=[],
                meta={"intake_complete": False},
            ),
            1,
        )
    }

    async def _noop_checkpoint(**_kwargs):
        return {"add": [], "drop": [], "reprioritize": []}

    monkeypatch.setattr(orchestrator, "_checkpoint_with_llm", _noop_checkpoint)
    await orchestrator._checkpoint_replan(
        message="继续推荐",
        context={
            "profile_budget_usd": 45000,
            "profile_intended_majors": [],
            "profile_structured": {"finance": {"budget_usd": 45000}},
        },
        shared={},
        trace_id="trace-soft-gate",
        wave_index=1,
        plan_map=plan_map,
        pending=pending,
        completed=completed,
        results=results,
        emit_event=lambda _event: asyncio.sleep(0),
        just_completed=[],
    )
    assert "recommendation_subagent" in pending


@pytest.mark.asyncio
async def test_run_turn_retries_required_output_and_recovers(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    calls = {"recommendation": 0}

    async def _split(*, message: str, context: dict):
        return ["recommendation_subagent"]

    async def _profile_read(_ctx):
        return CapabilityResult(content="profile ok", blocks=[], meta={})

    async def _recommend(_ctx):
        calls["recommendation"] += 1
        if calls["recommendation"] == 1:
            return CapabilityResult(content="narrative only", blocks=[], meta={})
        return CapabilityResult(
            content="recovered",
            blocks=[{"kind": "recommendation", "payload": {"schools": []}, "meta": {}}],
            meta={},
        )

    monkeypatch.setattr(orchestrator, "_split_intents", _split)
    orchestrator._registry = {
        "profile_read": _spec("profile_read", deps=[], execute=_profile_read, requires_db_session=True),
        "recommendation_subagent": _spec(
            "recommendation_subagent",
            deps=["profile_read"],
            execute=_recommend,
            requires_db_session=True,
            failure_policy="best_effort",
        ),
    }

    result = await orchestrator.run_turn(
        session_id="sid-required-output-retry",
        student_id=uuid.uuid4(),
        message="给我预算内推荐",
        route_plan=RoutePlan(primary_task="recommendation", route_lock=True),
        emit_event=lambda _event: asyncio.sleep(0),
    )

    assert result.status == "ok"
    assert calls["recommendation"] == 2
    assert result.usage.get("cap_retry_count") == 1
    assert any(block.kind == "recommendation" for block in result.blocks)


@pytest.mark.asyncio
async def test_run_turn_degrades_when_required_capability_fails_twice(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _split(*, message: str, context: dict):
        return ["recommendation_subagent"]

    async def _profile_read(_ctx):
        return CapabilityResult(content="profile ok", blocks=[], meta={})

    async def _boom(_ctx):
        raise RuntimeError("subagent failed")

    monkeypatch.setattr(orchestrator, "_split_intents", _split)
    orchestrator._registry = {
        "profile_read": _spec("profile_read", deps=[], execute=_profile_read, requires_db_session=True),
        "recommendation_subagent": _spec(
            "recommendation_subagent",
            deps=["profile_read"],
            execute=_boom,
            requires_db_session=True,
            failure_policy="fatal",
        ),
    }

    result = await orchestrator.run_turn(
        session_id="sid-required-cap-degrade",
        student_id=uuid.uuid4(),
        message="给我推荐",
        route_plan=RoutePlan(primary_task="recommendation", route_lock=True),
        emit_event=lambda _event: asyncio.sleep(0),
    )

    assert result.status == "ok"
    assert result.usage.get("cap_retry_count") == 1
    assert result.usage.get("cap_degraded") is True
    digest = result.execution_digest or {}
    assert digest.get("cap_retry_count") == 1
    assert digest.get("cap_degraded") is True


@pytest.mark.asyncio
async def test_required_output_not_retried_when_recommendation_blocked_by_intake(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )
    calls = {"recommendation": 0}

    async def _split(*, message: str, context: dict):
        return ["guided_intake", "recommendation_subagent"]

    async def _profile_read(_ctx):
        return CapabilityResult(content="profile ok", blocks=[], meta={})

    async def _guided(_ctx):
        return CapabilityResult(content="need more info", blocks=[], meta={"intake_complete": False})

    async def _recommend(_ctx):
        calls["recommendation"] += 1
        return CapabilityResult(content="should not run", blocks=[], meta={})

    monkeypatch.setattr(orchestrator, "_split_intents", _split)
    orchestrator._registry = {
        "profile_read": _spec("profile_read", deps=[], execute=_profile_read, requires_db_session=True),
        "guided_intake": _spec("guided_intake", deps=["profile_read"], execute=_guided, requires_db_session=True),
        "recommendation_subagent": _spec(
            "recommendation_subagent",
            deps=["guided_intake", "profile_read"],
            execute=_recommend,
            requires_db_session=True,
            failure_policy="best_effort",
        ),
    }

    result = await orchestrator.run_turn(
        session_id="sid-hard-intake-block",
        student_id=uuid.uuid4(),
        message="给我选校推荐清单",
        route_plan=RoutePlan(primary_task="recommendation", route_lock=False),
        emit_event=lambda _event: asyncio.sleep(0),
    )
    assert result.status == "ok"
    assert calls["recommendation"] == 0
    assert result.usage.get("required_output_missing") is True
    assert result.usage.get("failure_reason_code") == "RECOMMENDATION_BLOCKED_BY_INTAKE"


@pytest.mark.asyncio
async def test_run_one_capability_normalizes_meta_contract_fields():
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _exec(_ctx):
        return CapabilityResult(
            content="done",
            blocks=[{"kind": "text", "payload": {"text": "done"}, "meta": {}}],
            meta={},
        )

    spec = _spec("general", deps=[], execute=_exec)
    shared = {
        "trace_id": "trace-meta",
        "turn_step_id": "turn-trace-meta",
        "step_seq": 0,
        "event_seq": 0,
        "active_skill_id": "default",
    }
    events = []

    async def _emit(event):
        events.append(event)

    result = await orchestrator._run_one_capability(
        spec=spec,
        ctx=orchestrator_module.CapabilityContext(
            llm=_StubLLM(),
            session=MagicMock(),
            memory=_StubMemory(),
            session_id="sid-meta",
            student_id=uuid.uuid4(),
            message="hi",
            conversation_context={},
            shared=shared,
        ),
        wave_index=1,
        step_id="step-meta",
        parent_step_id="wave-1",
        trace_id="trace-meta",
        emit_event=_emit,
    )
    assert isinstance(result.meta, dict)
    assert "what_done" in result.meta
    assert "why_next" in result.meta
    assert "needs_input" in result.meta
    assert "action_hints" in result.meta
    assert "risks_missing" in result.meta
    assert "personalization_evidence" in result.meta
    assert len(result.meta.get("needs_input") or []) <= 2


@pytest.mark.asyncio
async def test_run_turn_compacts_oversized_user_message(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    captured: dict[str, object] = {}

    async def _split(*, message: str, context: dict):
        captured["original_len"] = len(message)
        compressed = str(context.get("compressed_user_message", ""))
        captured["compressed_len"] = len(compressed)
        captured["input_compacted"] = bool(context.get("context_management", {}).get("input_compacted"))
        return ["general"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    long_message = "请帮我改档案并做选校 " + ("SAT 1550, GPA 3.95, budget 70000. " * 120)
    result = await orchestrator.run_turn(
        session_id="sid-long-input",
        student_id=uuid.uuid4(),
        message=long_message,
        emit_event=lambda _event: asyncio.sleep(0),
    )

    assert result.status == "ok"
    assert captured["input_compacted"] is True
    assert int(captured["compressed_len"]) < int(captured["original_len"])
    assert result.usage.get("input_compacted") is True


@pytest.mark.asyncio
async def test_run_turn_rolls_back_on_invalid_capability_schema(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _split(*, message: str, context: dict):
        return ["invalid"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    async def _invalid(_ctx):
        return CapabilityResult(
            content="bad block",
            blocks=[{"kind": "not_supported", "payload": {"x": 1}, "meta": {}}],
            meta={},
        )

    orchestrator._registry = {
        "invalid": _spec("invalid", deps=[], execute=_invalid),
    }

    result = await orchestrator.run_turn(
        session_id="sid-invalid",
        student_id=uuid.uuid4(),
        message="invalid output",
        emit_event=lambda _event: asyncio.sleep(0),
    )

    assert result.status == "error"
    assert len(result.blocks) == 1
    assert result.blocks[0].kind == "error"
    assert result.usage.get("rolled_back") is True
    assert result.usage.get("tool_step_budget") == orchestrator_module.MAX_TOOL_STEPS_PER_TURN


@pytest.mark.asyncio
async def test_run_turn_rolls_back_on_capability_timeout(monkeypatch):
    monkeypatch.setattr(orchestrator_module, "CAPABILITY_TIMEOUT_SECONDS", 0.01)

    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _split(*, message: str, context: dict):
        return ["slow"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    async def _slow(_ctx):
        await asyncio.sleep(0.05)
        return CapabilityResult(content="slow", blocks=[], meta={})

    orchestrator._registry = {
        "slow": _spec("slow", deps=[], execute=_slow),
    }

    events = []

    async def _emit(event):
        events.append(event)

    result = await orchestrator.run_turn(
        session_id="sid-timeout",
        student_id=uuid.uuid4(),
        message="timeout me",
        emit_event=_emit,
    )

    assert result.status == "error"
    assert len(result.blocks) == 1
    assert result.blocks[0].kind == "error"
    assert any(event.event == "rollback" for event in events)
    assert result.usage.get("guardrail_triggered") is True


@pytest.mark.asyncio
async def test_run_turn_degrades_best_effort_timeout_without_rollback(monkeypatch):
    monkeypatch.setattr(orchestrator_module, "BEST_EFFORT_CAPABILITY_TIMEOUT_SECONDS", 0.01)

    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _split(*, message: str, context: dict):
        return ["strategy"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    async def _slow(_ctx):
        await asyncio.sleep(0.05)
        return CapabilityResult(content="slow", blocks=[], meta={})

    async def _profile_read(_ctx):
        return CapabilityResult(content="profile ok", blocks=[], meta={})

    orchestrator._registry = {
        "profile_read": _spec(
            "profile_read",
            deps=[],
            execute=_profile_read,
            requires_db_session=True,
        ),
        "strategy": _spec(
            "strategy",
            deps=["profile_read"],
            execute=_slow,
            requires_db_session=True,
            failure_policy="best_effort",
        ),
    }

    events = []

    async def _emit(event):
        events.append(event)

    result = await orchestrator.run_turn(
        session_id="sid-timeout-best-effort",
        student_id=uuid.uuid4(),
        message="timeout strategy",
        emit_event=_emit,
    )

    assert result.status == "ok"
    assert result.usage.get("guardrail_triggered") is not True
    assert result.usage.get("best_effort_degraded_count") == 1
    assert "strategy" in (result.usage.get("best_effort_degraded_caps") or [])
    assert any(event.event == "capability_finished" and event.data.get("step_status") == "timeout" for event in events)
    assert not any(event.event == "rollback" for event in events)
    synthesis_block = next((block for block in result.blocks if block.kind == "answer_synthesis"), None)
    assert synthesis_block is not None
    assert isinstance(synthesis_block.payload.get("actions"), list)
    assert len(synthesis_block.payload.get("actions") or []) >= 1
    degraded = synthesis_block.payload.get("degraded", {})
    assert degraded.get("has_degraded") is True
    assert "strategy" in (degraded.get("caps") or [])
    assert "CAP_TIMEOUT" in (degraded.get("reason_codes") or [])
    summary_text = str(synthesis_block.payload.get("summary") or "")
    conclusion_text = str(synthesis_block.payload.get("conclusion") or "")
    risks_text = " ".join(str(item) for item in (synthesis_block.payload.get("risks_missing") or []))
    assert "CAP_TIMEOUT" not in summary_text
    assert "CAP_TIMEOUT" not in conclusion_text
    assert "CAP_TIMEOUT" not in risks_text


@pytest.mark.asyncio
async def test_run_turn_rolls_back_when_step_budget_exceeded(monkeypatch):
    monkeypatch.setattr(orchestrator_module, "MAX_TOOL_STEPS_PER_TURN", 2)

    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _split(*, message: str, context: dict):
        return ["a", "b", "c"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    async def _ok(_ctx):
        await asyncio.sleep(0)
        return CapabilityResult(content="ok", blocks=[], meta={})

    orchestrator._registry = {
        "a": _spec("a", deps=[], execute=_ok),
        "b": _spec("b", deps=[], execute=_ok),
        "c": _spec("c", deps=[], execute=_ok),
    }

    events = []

    async def _emit(event):
        events.append(event)

    result = await orchestrator.run_turn(
        session_id="sid-budget",
        student_id=uuid.uuid4(),
        message="trigger step budget",
        emit_event=_emit,
    )

    rollback = next(event for event in events if event.event == "rollback")
    assert "tool step budget exceeded" in str(rollback.data["reason"])
    assert result.status == "error"
    assert result.usage["tool_steps_used"] == 2
    assert result.usage["tool_step_budget"] == 2
    assert result.usage["guardrail_triggered"] is True


@pytest.mark.asyncio
async def test_checkpoint_timeout_falls_back_to_noop_then_blocked(monkeypatch):
    monkeypatch.setattr(orchestrator_module, "CHECKPOINT_TIMEOUT_SECONDS", 0.01)

    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _slow_checkpoint(**_kwargs):
        await asyncio.sleep(0.05)
        return {"add": [], "drop": [], "reprioritize": []}

    monkeypatch.setattr(orchestrator, "_checkpoint_with_llm", _slow_checkpoint)

    async def _split(*, message: str, context: dict):
        return ["blocked_a", "blocked_b"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    async def _ready_never(_ctx):
        return CapabilityResult(content="x", blocks=[], meta={})

    orchestrator._registry = {
        "blocked_a": _spec("blocked_a", deps=["blocked_b"], execute=_ready_never),
        "blocked_b": _spec("blocked_b", deps=["blocked_a"], execute=_ready_never),
    }

    events = []

    async def _emit(event):
        events.append(event)

    result = await orchestrator.run_turn(
        session_id="sid-checkpoint-timeout",
        student_id=uuid.uuid4(),
        message="blocked graph",
        emit_event=_emit,
    )

    rollback = next(event for event in events if event.event == "rollback")
    assert "Capability graph is blocked" in str(rollback.data["reason"])
    assert result.status == "error"


@pytest.mark.asyncio
async def test_checkpoint_planning_event_includes_observability_fields(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _guided(_ctx) -> CapabilityResult:
        return CapabilityResult(
            content="intake done",
            blocks=[{"kind": "text", "payload": {"text": "done"}, "meta": {}}],
            meta={"intake_complete": True},
        )

    async def _recommend(_ctx) -> CapabilityResult:
        return CapabilityResult(
            content="recommendations",
            blocks=[{"kind": "recommendation", "payload": {"schools": []}, "meta": {}}],
            meta={},
        )

    orchestrator._registry = {
        "guided_intake": _spec("guided_intake", deps=[], execute=_guided),
        "recommendation_subagent": _spec(
            "recommendation_subagent",
            deps=["guided_intake"],
            execute=_recommend,
        ),
    }

    async def _noop_llm_checkpoint(**_kwargs):
        return {"add": [], "drop": [], "reprioritize": []}

    monkeypatch.setattr(orchestrator, "_checkpoint_with_llm", _noop_llm_checkpoint)
    events = []

    async def _emit(event):
        events.append(event)

    await orchestrator._execute_plan(
        planned=[_planned("guided_intake", order=0)],
        message="complete intake then recommend",
        session_id="sid-wave-observability",
        student_id=uuid.uuid4(),
        context={},
        shared={},
        trace_id="trace-wave-observability",
        emit_event=_emit,
    )

    checkpoint_event = next(
        event
        for event in events
        if event.event == "planning_done" and bool((event.data or {}).get("checkpoint"))
    )
    assert "checkpoint_kind" in checkpoint_event.data
    assert "checkpoint_status" in checkpoint_event.data
    assert "checkpoint_summary" in checkpoint_event.data
    assert checkpoint_event.data["step_kind"] == "checkpoint"
    assert checkpoint_event.data["phase"] == "checkpoint"
    assert "llm_reasoning" not in checkpoint_event.data
    assert "context_chars_before" in checkpoint_event.data
    assert "context_chars_after" in checkpoint_event.data
    assert "compression_level" in checkpoint_event.data


@pytest.mark.asyncio
async def test_plan_auto_injects_profile_read_for_profile_dependent_capability(monkeypatch):
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=MagicMock(),
        memory=_StubMemory(),
    )

    async def _split(*, message: str, context: dict):
        return ["strategy"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    planned = await orchestrator._plan_capabilities(
        message="give me strategy",
        context={},
        student_id=uuid.uuid4(),
    )
    ids = [item.id for item in planned]
    assert ids[0] == "profile_read"
    assert "strategy" in ids
    strategy = next(item for item in planned if item.id == "strategy")
    assert "profile_read" in strategy.dependencies


class _ProfilePatchLLM(_StubLLM):
    async def complete_json(self, _messages, **kwargs):
        caller = kwargs.get("caller")
        if caller == "chat.profile_update_extract":
            return {
                "patch": {"academics": {"gpa": 3.95}},
                "summary": "Proposed updates: academics.gpa",
            }
        if caller == "chat.wave_checkpoint":
            return {"add": [], "drop": [], "reprioritize": []}
        return {"capabilities": ["profile_update"]}


@pytest.mark.asyncio
async def test_profile_update_proposal_then_confirm_apply(session, monkeypatch):
    memory = _StateMemory()
    orchestrator = AdvisorOrchestratorV2(
        llm=_ProfilePatchLLM(),
        session=session,
        memory=memory,
    )

    student = await create_student(
        session,
        {
            "name": "Patch Student",
            "gpa": 3.8,
            "gpa_scale": "4.0",
            "sat_total": 1500,
            "curriculum_type": "AP",
            "intended_majors": ["CS"],
            "budget_usd": 60000,
            "target_year": 2028,
            "need_financial_aid": False,
        },
    )

    async def _split(*, message: str, context: dict):
        return ["profile_update"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    sid = "sid-profile-proposal"
    memory.push_user(sid, "please update my gpa to 3.95")
    proposal_result = await orchestrator.run_turn(
        session_id=sid,
        student_id=student.id,
        message="please update my gpa to 3.95",
        emit_event=lambda _event: asyncio.sleep(0),
    )

    assert proposal_result.status == "ok"
    proposal_block = next(block for block in proposal_result.blocks if block.kind == "profile_patch_proposal")
    confirm_command = str(proposal_block.payload["confirm_command"])

    unchanged = await get_student(session, student.id)
    assert unchanged.gpa == pytest.approx(3.8)

    memory.push_user(sid, confirm_command)
    confirm_result = await orchestrator.run_turn(
        session_id=sid,
        student_id=student.id,
        message=confirm_command,
        emit_event=lambda _event: asyncio.sleep(0),
    )
    assert confirm_result.status == "ok"
    assert any(block.kind == "profile_patch_result" for block in confirm_result.blocks)

    updated = await get_student(session, student.id)
    assert updated.gpa == pytest.approx(3.95)


@pytest.mark.asyncio
async def test_profile_update_confirm_fails_when_pending_patch_expired(session, monkeypatch):
    memory = _StateMemory()
    orchestrator = AdvisorOrchestratorV2(
        llm=_ProfilePatchLLM(),
        session=session,
        memory=memory,
    )

    student = await create_student(
        session,
        {
            "name": "TTL Student",
            "gpa": 3.7,
            "gpa_scale": "4.0",
            "sat_total": 1450,
            "curriculum_type": "AP",
            "intended_majors": ["Economics"],
            "budget_usd": 50000,
            "target_year": 2028,
            "need_financial_aid": False,
        },
    )

    async def _split(*, message: str, context: dict):
        return ["profile_update"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    sid = "sid-profile-ttl"
    memory.push_user(sid, "update my gpa to 3.95")
    proposal_result = await orchestrator.run_turn(
        session_id=sid,
        student_id=student.id,
        message="update my gpa to 3.95",
        emit_event=lambda _event: asyncio.sleep(0),
    )
    proposal_block = next(block for block in proposal_result.blocks if block.kind == "profile_patch_proposal")
    confirm_command = str(proposal_block.payload["confirm_command"])

    pending = memory.context[sid][PENDING_PROFILE_PATCH_KEY]
    assert isinstance(pending, dict)
    pending["created_user_turn_index"] = int(pending["created_user_turn_index"]) - (
        PROFILE_PATCH_TTL_USER_TURNS + 1
    )
    memory.context[sid][PENDING_PROFILE_PATCH_KEY] = pending

    events = []

    async def _emit(event):
        events.append(event)

    memory.push_user(sid, confirm_command)
    expired_result = await orchestrator.run_turn(
        session_id=sid,
        student_id=student.id,
        message=confirm_command,
        emit_event=_emit,
    )
    assert expired_result.status == "error"
    rollback = next(event for event in events if event.event == "rollback")
    assert "expired" in str(rollback.data["reason"])


@pytest.mark.asyncio
async def test_profile_update_confirm_fails_without_pending_patch(session, monkeypatch):
    memory = _StateMemory()
    orchestrator = AdvisorOrchestratorV2(
        llm=_ProfilePatchLLM(),
        session=session,
        memory=memory,
    )

    student = await create_student(
        session,
        {
            "name": "No Pending Student",
            "gpa": 3.6,
            "gpa_scale": "4.0",
            "sat_total": 1400,
            "curriculum_type": "AP",
            "intended_majors": ["Math"],
            "budget_usd": 48000,
            "target_year": 2028,
            "need_financial_aid": False,
        },
    )

    async def _split(*, message: str, context: dict):
        return ["profile_update"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    events = []

    async def _emit(event):
        events.append(event)

    bad_confirm = f"{PROFILE_CONFIRM_COMMAND_PREFIX}11111111-1111-1111-1111-111111111111"
    memory.push_user("sid-no-pending", bad_confirm)
    result = await orchestrator.run_turn(
        session_id="sid-no-pending",
        student_id=student.id,
        message=bad_confirm,
        emit_event=_emit,
    )
    assert result.status == "error"
    rollback = next(event for event in events if event.event == "rollback")
    assert "no pending profile patch" in str(rollback.data["reason"])


@pytest.mark.asyncio
async def test_run_turn_rolls_back_db_writes_when_later_capability_fails(session, monkeypatch):
    memory = _StateMemory()
    orchestrator = AdvisorOrchestratorV2(
        llm=_StubLLM(),
        session=session,
        memory=memory,
    )

    student = await create_student(
        session,
        {
            "name": "Rollback Student",
            "gpa": 3.6,
            "gpa_scale": "4.0",
            "sat_total": 1420,
            "curriculum_type": "AP",
            "intended_majors": ["Physics"],
            "budget_usd": 55000,
            "target_year": 2028,
            "need_financial_aid": False,
        },
    )

    async def _split(*, message: str, context: dict):
        return ["writer", "boom"]

    monkeypatch.setattr(orchestrator, "_split_intents", _split)

    async def _writer(ctx) -> CapabilityResult:
        target = await get_student(ctx.session, student.id)
        target.gpa = 4.0
        await ctx.session.flush()
        return CapabilityResult(content="writer done", blocks=[], meta={})

    async def _boom(_ctx):
        await asyncio.sleep(0.02)
        raise RuntimeError("boom")

    orchestrator._registry = {
        "writer": _spec("writer", deps=[], execute=_writer),
        "boom": _spec("boom", deps=[], execute=_boom),
    }

    memory.push_user("sid-rollback-db", "trigger rollback")
    result = await orchestrator.run_turn(
        session_id="sid-rollback-db",
        student_id=student.id,
        message="trigger rollback",
        emit_event=lambda _event: asyncio.sleep(0),
    )
    assert result.status == "error"

    await session.refresh(student)
    assert student.gpa == pytest.approx(3.6)
