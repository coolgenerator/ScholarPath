"""Advisor V2 orchestrator runtime.

Single-agent loop:
intent split -> plan graph -> parallel capability execution -> aggregate blocks.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, Awaitable, Callable, Literal

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.api.models.chat import ChatBlock, RoutePlan, TurnEvent, TurnResult
from scholarpath.chat.handlers import (
    PENDING_PROFILE_PATCH_KEY,
    apply_pending_profile_patch,
    build_profile_snapshot,
    clear_pending_profile_patch,
    create_profile_patch_proposal,
    handle_guided_intake,
    handle_offer_decision,
    handle_recommendation,
    resolve_profile_update_gate,
    handle_school_query,
    handle_strategy,
    handle_what_if,
)
from scholarpath.chat.memory import ChatMemory
from scholarpath.language import detect_response_language, language_instruction
from scholarpath.llm.client import LLMClient

logger = logging.getLogger(__name__)

_TURN_TIMEOUT_SECONDS = 90
_CAPABILITY_MAX_CONCURRENCY = 8
_MAX_WAVES_PER_TURN = 8
MAX_TOOL_STEPS_PER_TURN = 12
CAPABILITY_TIMEOUT_SECONDS = 60
BEST_EFFORT_CAPABILITY_TIMEOUT_SECONDS = 12
BEST_EFFORT_SOFT_TIMEOUT_SECONDS = 6
_BEST_EFFORT_SOFT_TIMEOUT_BY_CAPABILITY: dict[str, float] = {
    "strategy": 8.0,
    "recommendation_subagent": 8.0,
}
CHECKPOINT_TIMEOUT_SECONDS = 2

# Context management budgets (character-based proxy for token budgets).
_INPUT_COMPACT_TRIGGER_CHARS = 1600
_INPUT_COMPACT_TARGET_CHARS = 700
_RECENT_MESSAGES_BUDGET_CHARS = 2200
_CONTEXT_ACTIVE_BUDGET_CHARS = 12000
_CONTEXT_HARD_BUDGET_CHARS = 16000

_LAYER_QUOTAS_DEFAULT: dict[str, tuple[int, int]] = {
    "short_term": (10, 260),
    "working": (14, 320),
    "long_term": (18, 420),
}
_LAYER_QUOTAS_TIGHT: dict[str, tuple[int, int]] = {
    "short_term": (8, 200),
    "working": (10, 260),
    "long_term": (12, 320),
}
_LAYER_QUOTAS_HARD: dict[str, tuple[int, int]] = {
    "short_term": (6, 160),
    "working": (8, 220),
    "long_term": (10, 260),
}
_CONTEXT_CRITICAL_KEYS = {
    "pending_profile_patch",
    "current_school_id",
    "current_school_name",
    "intake_step",
    "intake_complete",
    "profile_completion_pct",
    "profile_budget_usd",
    "profile_need_financial_aid",
}

_BLOCK_KINDS = {
    "answer_synthesis",
    "recommendation",
    "offer_compare",
    "what_if",
    "guided_questions",
    "profile_snapshot",
    "profile_patch_proposal",
    "profile_patch_result",
    "text",
    "error",
}

_PROFILE_DEPENDENT_CAPABILITIES = {
    "recommendation_subagent",
    "school_query",
    "strategy",
    "offer_compare",
    "what_if",
}

_SYNTHESIS_BASE_WEIGHTS: dict[str, float] = {
    "recommendation_subagent": 1.0,
    "offer_compare": 0.92,
    "what_if": 0.9,
    "strategy": 0.88,
    "school_query": 0.86,
    "profile_update": 0.78,
    "profile_read": 0.74,
    "guided_intake": 0.72,
    "general": 0.62,
    "emotional_support": 0.58,
}

_ANGLE_BY_CAPABILITY: dict[str, str] = {
    "recommendation_subagent": "recommendation",
    "offer_compare": "comparison",
    "what_if": "scenario",
    "strategy": "timeline",
    "school_query": "school_facts",
    "profile_update": "profile",
    "profile_read": "profile",
    "guided_intake": "intake",
    "general": "general",
    "emotional_support": "support",
}

_SYNTHESIS_SKILLS = {
    "default",
    "recommendation",
    "strategy",
    "school_query",
    "offer_compare",
    "what_if",
    "profile_update",
    "guided_intake",
    "memory_followup",
    "multi_intent",
    "robustness",
    "emotional_support",
}
_SKILL_CONTRACT_VERSION = "skill_contract_v1"

_SKILL_PRIMARY_CAPABILITY: dict[str, tuple[str, ...]] = {
    "recommendation": ("recommendation_subagent",),
    "strategy": ("strategy",),
    "school_query": ("school_query",),
    "offer_compare": ("offer_compare",),
    "what_if": ("what_if",),
    "profile_update": ("profile_update", "profile_read"),
    "guided_intake": ("guided_intake",),
    "memory_followup": ("recommendation_subagent", "strategy", "school_query"),
    "emotional_support": ("emotional_support",),
    "default": ("general",),
}

_SKILL_CAPABILITY_PRIORITY: dict[str, int] = {
    "profile_update": 0,
    "guided_intake": 1,
    "offer_compare": 2,
    "what_if": 3,
    "recommendation_subagent": 4,
    "strategy": 5,
    "school_query": 6,
    "profile_read": 7,
    "emotional_support": 8,
    "general": 9,
}

_ROUTE_TASK_TO_SKILL: dict[str, str] = {
    "chat": "default",
    "recommendation": "recommendation",
    "strategy": "strategy",
    "what_if": "what_if",
    "offer_compare": "offer_compare",
    "intake": "guided_intake",
}

_ROUTE_TASK_TO_PRIMARY_CAPABILITY: dict[str, str] = {
    "chat": "general",
    "recommendation": "recommendation_subagent",
    "strategy": "strategy",
    "what_if": "what_if",
    "offer_compare": "offer_compare",
    "intake": "guided_intake",
}

_ROUTE_ALLOWED_MODIFIERS = {"memory_followup"}
_ROUTE_ALLOWED_REQUIRED_OUTPUTS = {"recommendation_payload"}

_INTERNAL_JARGON_PATTERNS = (
    re.compile(r"\bgenerated\b", re.IGNORECASE),
    re.compile(r"\bpayload\b", re.IGNORECASE),
    re.compile(r"\bworkflow\b", re.IGNORECASE),
    re.compile(r"\badvanced the intake workflow\b", re.IGNORECASE),
)

_INTERNAL_REASON_CODE_PATTERN = re.compile(
    r"\b(?:CAP_TIMEOUT|CAP_FAILED|CAP_SCHEMA_INVALID|CAP_DEGRADED|STEP_BUDGET_EXCEEDED|LOCK_REJECTED|PROFILE_GATE_BLOCKED)\b",
    re.IGNORECASE,
)

_INTERNAL_REASON_TEXT_PATTERN = re.compile(
    r"(?:降级原因|原因码|reason\s*code(?:s)?)[:：]?\s*([A-Z_][A-Z0-9_]*(?:\s*,\s*[A-Z_][A-Z0-9_]*)*)",
    re.IGNORECASE,
)

_GENERIC_REFUSAL_PATTERNS = (
    re.compile(r"\b(i (?:can(?:not|'t)|am unable)|cannot help)\b", re.IGNORECASE),
    re.compile(r"\b(please be more specific|need more details|not detect concrete)\b", re.IGNORECASE),
    re.compile(r"(无法处理|无法判断|请更具体|没识别到|不清楚你的意思)"),
)

_RESPONSE_CONTRACT_SECTION_HINTS: dict[str, tuple[str, ...]] = {
    "conclusion": ("结论", "Conclusion", "建议"),
    "evidence": ("依据", "Evidence", "原因", "理由"),
    "next_step": ("下一步", "Next", "行动"),
    "missing_info": ("缺失信息", "Missing", "还需要", "待补充"),
}


@dataclass(slots=True)
class CapabilityContext:
    llm: LLMClient
    session: AsyncSession
    memory: ChatMemory
    session_id: str
    student_id: uuid.UUID | None
    message: str
    conversation_context: dict[str, Any]
    shared: dict[str, Any]


@dataclass(slots=True)
class CapabilityResult:
    content: str
    blocks: list[dict[str, Any]]
    meta: dict[str, Any]


CapabilityRunner = Callable[[CapabilityContext], Awaitable[CapabilityResult]]
CapabilityFailurePolicy = Literal["fatal", "best_effort"]


@dataclass(slots=True)
class CapabilitySpec:
    id: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    dependencies: list[str]
    cost_class: str
    execute: CapabilityRunner
    requires_db_session: bool = False
    failure_policy: CapabilityFailurePolicy = "fatal"


@dataclass(slots=True)
class PlannedCapability:
    id: str
    is_primary: bool
    dependencies: list[str]
    plan_order: int


@dataclass(slots=True, frozen=True)
class SkillSpec:
    id: str
    priority: int
    capability_hints: tuple[str, ...] = ()
    message_patterns: tuple[str, ...] = ()
    history_patterns: tuple[str, ...] = ()
    bucket: str = "general"


@dataclass(slots=True)
class SkillRouteDecision:
    active_skill_id: str
    source: str
    confidence: float
    matched_signals: list[str]
    modifiers: tuple[str, ...] = ()


_SKILL_SPECS: dict[str, SkillSpec] = {
    "profile_update": SkillSpec(
        id="profile_update",
        priority=120,
        capability_hints=("profile_update",),
        message_patterns=(
            r"\bconfirm_profile_patch:[a-f0-9-]+\b",
            r"(确认修改|确认提交|confirm.*patch|更新档案|修改档案|修改gpa|更新预算|改专业)",
        ),
        bucket="profile_update",
    ),
    "robustness": SkillSpec(
        id="robustness",
        priority=110,
        message_patterns=(
            r"(报错|失败|超时|卡住|不稳定|重试|retry|timeout|failed|rollback|回滚)",
            r"(为什么.*(慢|失败|reject)|请求.*reject)",
        ),
        bucket="robustness",
    ),
    "memory_followup": SkillSpec(
        id="memory_followup",
        priority=100,
        capability_hints=(),
        message_patterns=(
            r"(基于我刚才|按我上条|沿用刚才|继续上次|follow[\s-]?up|记住我刚说)",
            r"(延续|继续)我(的)?(偏好|约束|档案)",
        ),
        history_patterns=(
            r"(预算|专业|城市|tier|冲刺|匹配|保底)",
        ),
        bucket="memory_followup",
    ),
    "guided_intake": SkillSpec(
        id="guided_intake",
        priority=95,
        capability_hints=("guided_intake",),
        message_patterns=(
            r"(继续提问|继续问我|信息不全|需要补充|先问我问题|补齐信息|怎么补资料)",
            r"(我不知道怎么选|先收集我的信息)",
        ),
        bucket="guided_intake",
    ),
    "what_if": SkillSpec(
        id="what_if",
        priority=90,
        capability_hints=("what_if",),
        message_patterns=(
            r"(如果|假设|what[\s-]?if|会怎么变化|会怎样变化|提到\d|提高到|from\s*\d+\s*to\s*\d+)",
            r"(两个投入动作|最值得做的两个动作)",
        ),
        bucket="what_if",
    ),
    "offer_compare": SkillSpec(
        id="offer_compare",
        priority=85,
        capability_hints=("offer_compare",),
        message_patterns=(
            r"(offer|录取对比|比较学校|compare|哪个更好|二选一)",
        ),
        bucket="offer_compare",
    ),
    "recommendation": SkillSpec(
        id="recommendation",
        priority=80,
        capability_hints=("recommendation_subagent",),
        message_patterns=(
            r"(选校推荐|推荐学校|shortlist|school list|推荐清单)",
        ),
        bucket="recommendation",
    ),
    "strategy": SkillSpec(
        id="strategy",
        priority=78,
        capability_hints=("strategy",),
        message_patterns=(
            r"(申请策略|时间线|timeline|ed|ea|rd|早申|节奏)",
        ),
        bucket="strategy",
    ),
    "school_query": SkillSpec(
        id="school_query",
        priority=76,
        capability_hints=("school_query",),
        message_patterns=(
            r"(学校信息|学校数据|学校怎么样|university|college|mit|harvard|stanford)",
        ),
        bucket="school_query",
    ),
    "multi_intent": SkillSpec(
        id="multi_intent",
        priority=74,
        message_patterns=(
            r"(同时|并且|另外|然后再|并行|一轮里|多用途)",
        ),
        bucket="multi_intent",
    ),
    "emotional_support": SkillSpec(
        id="emotional_support",
        priority=70,
        capability_hints=("emotional_support",),
        message_patterns=(
            r"(焦虑|紧张|压力|panic|anxious|stress|怕来不及)",
        ),
        bucket="emotional_support",
    ),
    "default": SkillSpec(
        id="default",
        priority=10,
        capability_hints=("general",),
        bucket="default",
    ),
}


class CapabilityExecutionError(RuntimeError):
    """Raised when capability output is invalid or execution fails."""


class AdvisorOrchestratorV2:
    """Main orchestrator for one turn execution."""

    def __init__(
        self,
        *,
        llm: LLMClient,
        session: AsyncSession,
        memory: ChatMemory,
    ) -> None:
        self._llm = llm
        self._session = session
        self._memory = memory
        self._capability_semaphore = asyncio.Semaphore(_CAPABILITY_MAX_CONCURRENCY)
        self._db_session_semaphore = asyncio.Semaphore(1)
        self._subagent_semaphore = asyncio.Semaphore(1)
        self._registry = self._build_registry()

    async def run_turn(
        self,
        *,
        session_id: str,
        student_id: uuid.UUID | None,
        message: str,
        route_plan: RoutePlan | dict[str, Any] | None = None,
        emit_event: Callable[[TurnEvent], Awaitable[None]],
    ) -> TurnResult:
        trace_id = str(uuid.uuid4())
        logger.info(
            "Advisor turn started trace=%s session=%s student=%s message_chars=%d",
            trace_id,
            session_id,
            str(student_id) if student_id is not None else "anonymous",
            len(message),
        )
        history = await self._memory.get_history(session_id, limit=12)
        context_layers = await self._load_context_layers(session_id)
        student_context_layers = await self._load_student_context_layers(student_id)
        profile_facts = await self._load_profile_structured_memory(student_id)
        layered_context = self._merge_context_sources(
            session_layers=context_layers,
            student_layers=student_context_layers,
            profile_facts=profile_facts,
        )
        session_summary_source = {
            **context_layers.get("short_term", {}),
            **context_layers.get("working", {}),
            **context_layers.get("long_term", {}),
        }
        student_summary_source = {
            **student_context_layers.get("short_term", {}),
            **student_context_layers.get("working", {}),
            **student_context_layers.get("long_term", {}),
        }
        session_summary_nodes = self._build_summary_nodes(
            source=session_summary_source,
            max_nodes=8,
            node_prefix="session",
        )
        student_summary_nodes = self._build_summary_nodes(
            source=student_summary_source,
            max_nodes=6,
            node_prefix="student",
        )
        compressed_message, input_compacted = self._compact_user_message(message)
        layered_context, context_compaction = self._compact_layered_context(layered_context)

        context = dict(layered_context["merged"])
        context["memory_layers"] = {
            "short_term": dict(layered_context["short_term"]),
            "working": dict(layered_context["working"]),
            "long_term": dict(layered_context["long_term"]),
            "session": {
                "short_term": dict(context_layers["short_term"]),
                "working": dict(context_layers["working"]),
                "long_term": dict(context_layers["long_term"]),
            },
            "student": {
                "short_term": dict(student_context_layers["short_term"]),
                "working": dict(student_context_layers["working"]),
                "long_term": dict(student_context_layers["long_term"]),
            },
            "session_summary_nodes": session_summary_nodes,
            "student_summary_nodes": student_summary_nodes,
        }
        context["compressed_user_message"] = compressed_message
        context["recent_messages"] = self._clip_text(
            "\n".join(f"{m.get('role', 'unknown')}: {m.get('content', '')}" for m in history[-6:]),
            _RECENT_MESSAGES_BUDGET_CHARS,
        )
        context["profile_structured"] = profile_facts
        context["history_user_turn_count"] = sum(
            1 for item in history if str(item.get("role", "")).lower() == "user"
        )
        context["_profile_update_gate"] = None
        compression_passes = int(context_compaction.get("passes", 0)) + (1 if input_compacted else 0)
        context["context_management"] = {
            "input_compacted": input_compacted,
            "context_compacted": bool(context_compaction.get("compacted", False)),
            "compression_passes": compression_passes,
            "context_chars": int(context_compaction.get("chars", 0)),
            "context_chars_before": int(context_compaction.get("before_chars", 0)),
            "context_chars_after": int(context_compaction.get("chars", 0)),
            "context_budget_chars": _CONTEXT_ACTIVE_BUDGET_CHARS,
            "hard_budget_chars": _CONTEXT_HARD_BUDGET_CHARS,
            "compression_level": str(context_compaction.get("quota_profile", "default")),
            "summary_node_count": len(session_summary_nodes) + len(student_summary_nodes),
            "profile_fact_count": len(profile_facts),
        }
        shared: dict[str, Any] = {
            "trace_id": trace_id,
            "turn_step_id": f"turn-{trace_id}",
            "step_seq": 0,
            "event_seq": 0,
            "wave_step_ids": {},
        }

        await emit_event(
            TurnEvent(
                trace_id=trace_id,
                event="turn_started",
                data={"session_id": session_id},
            )
        )

        planned = await self._plan_capabilities(
            message=message,
            context=context,
            student_id=student_id,
            route_plan=route_plan,
        )
        active_skill_id = str(context.get("active_skill_id") or "default")
        skill_route_source = str(context.get("skill_route_source") or "default")
        skill_contract_version = str(context.get("skill_contract_version") or _SKILL_CONTRACT_VERSION)
        skill_route_signals = [
            str(item).strip()
            for item in (context.get("skill_route_signals") or [])
            if str(item).strip()
        ] if isinstance(context.get("skill_route_signals"), list) else []
        skill_route_modifiers = [
            str(item).strip()
            for item in (context.get("skill_route_modifiers") or [])
            if str(item).strip()
        ] if isinstance(context.get("skill_route_modifiers"), list) else []
        shared["active_skill_id"] = active_skill_id
        shared["skill_route_source"] = skill_route_source
        shared["skill_contract_version"] = skill_contract_version
        shared["skill_route_signals"] = skill_route_signals
        shared["skill_route_modifiers"] = skill_route_modifiers
        await emit_event(
            TurnEvent(
                trace_id=trace_id,
                event="planning_done",
                data=self._with_step_fields(
                    shared=shared,
                    data={
                        "checkpoint_kind": "initial_plan",
                        "checkpoint_status": "completed",
                        "capabilities": [
                            {
                                "id": node.id,
                                "is_primary": node.is_primary,
                                "dependencies": node.dependencies,
                                "order": node.plan_order,
                            }
                            for node in planned
                        ],
                        "context_chars_before": int(context["context_management"]["context_chars_before"]),
                        "context_chars_after": int(context["context_management"]["context_chars_after"]),
                        "compression_level": str(context["context_management"]["compression_level"]),
                        "summary_node_count": int(context["context_management"]["summary_node_count"]),
                        "profile_fact_count": int(context["context_management"]["profile_fact_count"]),
                        "active_skill_id": active_skill_id,
                        "skill_route_source": skill_route_source,
                        "skill_contract_version": skill_contract_version,
                        "skill_route_signals": skill_route_signals[:6],
                        "skill_route_modifiers": skill_route_modifiers[:4],
                        "what_done": f"Split intents and built an executable plan with {len(planned)} nodes.",
                        "why_next": "Run ready capabilities in parallel for the first wave.",
                        "needs_input": [],
                    },
                    step_kind="wave",
                    step_status="completed",
                    phase="planning",
                    wave_index=0,
                    parent_step_id=str(shared.get("turn_step_id")),
                    checkpoint_summary={
                        "changed": True,
                        "added_count": len(planned),
                        "dropped_count": 0,
                        "reprioritized_count": 0,
                        "profile_gate_status": "not_applicable",
                    },
                    metrics={
                        "tool_step_budget": MAX_TOOL_STEPS_PER_TURN,
                        "context_chars": int(context["context_management"]["context_chars"]),
                    },
                    display={
                        "title": "Plan Ready",
                        "badge": "planning",
                        "severity": "info",
                    },
                ),
            )
        )

        shared.update(
            {
                "started_at": datetime.now(UTC).isoformat(),
                "tool_steps_used": 0,
                "tool_step_budget": MAX_TOOL_STEPS_PER_TURN,
                "wave_count": 0,
                "guardrail_triggered": False,
                "context_compacted": bool(context["context_management"]["context_compacted"]),
                "input_compacted": bool(context["context_management"]["input_compacted"]),
                "context_chars": int(context["context_management"]["context_chars"]),
                "compression_passes": int(context["context_management"]["compression_passes"]),
                "compression_level": str(context["context_management"]["compression_level"]),
                "summary_node_count": int(context["context_management"]["summary_node_count"]),
                "profile_fact_count": int(context["context_management"]["profile_fact_count"]),
                "output_compacted": False,
                "best_effort_degraded_count": 0,
                "best_effort_degraded_caps": [],
                "active_skill_id": active_skill_id,
                "skill_route_source": skill_route_source,
                "skill_contract_version": skill_contract_version,
                "skill_route_modifiers": skill_route_modifiers,
                "required_capabilities": list(context.get("required_capabilities") or []),
                "required_outputs": list(context.get("required_outputs") or []),
                "cap_retry_count": 0,
                "cap_degraded": False,
                "required_output_missing": False,
                "failure_reason_code": "",
            }
        )
        turn_tx = None
        begin_nested = getattr(self._session, "begin_nested", None)
        if callable(begin_nested):
            maybe_turn_tx = begin_nested()
            if inspect.isawaitable(maybe_turn_tx):
                turn_tx = await maybe_turn_tx
        try:
            node_results = await asyncio.wait_for(
                self._execute_plan(
                    planned=planned,
                    message=message,
                    session_id=session_id,
                    student_id=student_id,
                    context=context,
                    shared=shared,
                    trace_id=trace_id,
                    emit_event=emit_event,
                ),
                timeout=_TURN_TIMEOUT_SECONDS,
            )
            planned = self._materialize_plan_list(node_results["plan_map"])
            result, aggregate_stats = self._aggregate_result(
                trace_id=trace_id,
                planned=planned,
                node_results=node_results["results"],
                message=message,
                context=context,
            )
            result, contract_stats = await self._apply_user_visible_contract(
                result=result,
                message=message,
                context=context,
                node_results=node_results["results"],
                planned=planned,
            )
            aggregate_stats.update(contract_stats)
            execution_digest = self._build_execution_digest(
                planned=planned,
                node_results=node_results["results"],
                wave_count=int(node_results["wave_count"]),
                tool_steps_used=int(node_results["tool_steps_used"]),
                active_skill_id=str(shared.get("active_skill_id", "default")),
                cap_retry_count=int(shared.get("cap_retry_count", 0)),
                cap_degraded=bool(shared.get("cap_degraded", False)),
                failure_reason_code=str(shared.get("failure_reason_code", "") or ""),
                required_output_missing=bool(shared.get("required_output_missing", False)),
            )
            result.execution_digest = execution_digest
            started_at = str(shared.get("started_at", ""))
            ended_at = datetime.now(UTC)
            duration_ms = 0
            try:
                if started_at:
                    duration_ms = max(0, int((ended_at - datetime.fromisoformat(started_at)).total_seconds() * 1000))
            except Exception:
                duration_ms = 0
            result.usage["tool_steps_used"] = int(node_results["tool_steps_used"])
            result.usage["tool_step_budget"] = int(MAX_TOOL_STEPS_PER_TURN)
            result.usage["wave_count"] = int(node_results["wave_count"])
            result.usage["duration_ms"] = duration_ms
            result.usage["context_compacted"] = bool(shared.get("context_compacted", False))
            result.usage["input_compacted"] = bool(shared.get("input_compacted", False))
            result.usage["output_compacted"] = bool(aggregate_stats.get("output_compacted", False))
            result.usage["compression_passes"] = int(shared.get("compression_passes", 0))
            result.usage["compression_level"] = str(shared.get("compression_level", "default"))
            result.usage["context_chars"] = int(shared.get("context_chars", 0))
            result.usage["summary_node_count"] = int(shared.get("summary_node_count", 0))
            result.usage["profile_fact_count"] = int(shared.get("profile_fact_count", 0))
            result.usage["best_effort_degraded_count"] = int(shared.get("best_effort_degraded_count", 0))
            result.usage["best_effort_degraded_caps"] = list(shared.get("best_effort_degraded_caps", []))
            result.usage["active_skill_id"] = str(shared.get("active_skill_id", "default"))
            result.usage["skill_route_source"] = str(shared.get("skill_route_source", "default"))
            result.usage["skill_contract_version"] = str(shared.get("skill_contract_version", _SKILL_CONTRACT_VERSION))
            result.usage["skill_route_modifiers"] = list(shared.get("skill_route_modifiers", []))
            result.usage["response_contract_repaired"] = bool(aggregate_stats.get("response_contract_repaired", False))
            result.usage["generic_refusal_repaired"] = bool(aggregate_stats.get("generic_refusal_repaired", False))
            result.usage["cap_retry_count"] = int(shared.get("cap_retry_count", 0))
            result.usage["cap_degraded"] = bool(shared.get("cap_degraded", False))
            result.usage["failure_reason_code"] = str(shared.get("failure_reason_code", "") or "")
            result.usage["required_output_missing"] = bool(shared.get("required_output_missing", False))
            if shared.get("guardrail_triggered"):
                result.usage["guardrail_triggered"] = True
            await emit_event(
                TurnEvent(
                    trace_id=trace_id,
                    event="turn_completed",
                    data=self._with_step_fields(
                        shared=shared,
                        data={
                            "status": "ok",
                            "block_count": len(result.blocks),
                            "wave_count": node_results["wave_count"],
                            "tool_steps_used": node_results["tool_steps_used"],
                            "tool_step_budget": MAX_TOOL_STEPS_PER_TURN,
                            "duration_ms": duration_ms,
                            "context_compacted": bool(shared.get("context_compacted", False)),
                            "input_compacted": bool(shared.get("input_compacted", False)),
                            "output_compacted": bool(aggregate_stats.get("output_compacted", False)),
                            "compression_passes": int(shared.get("compression_passes", 0)),
                            "context_chars": int(shared.get("context_chars", 0)),
                            "best_effort_degraded_count": int(shared.get("best_effort_degraded_count", 0)),
                            "best_effort_degraded_caps": list(shared.get("best_effort_degraded_caps", [])),
                            "active_skill_id": str(shared.get("active_skill_id", "default")),
                            "skill_route_source": str(shared.get("skill_route_source", "default")),
                            "skill_contract_version": str(shared.get("skill_contract_version", _SKILL_CONTRACT_VERSION)),
                            "skill_route_modifiers": list(shared.get("skill_route_modifiers", [])),
                            "cap_retry_count": int(shared.get("cap_retry_count", 0)),
                            "cap_degraded": bool(shared.get("cap_degraded", False)),
                            "failure_reason_code": str(shared.get("failure_reason_code", "") or ""),
                            "required_output_missing": bool(shared.get("required_output_missing", False)),
                            "what_done": str(execution_digest.get("what_done", "")),
                            "why_next": str(execution_digest.get("why_next", "")),
                            "needs_input": execution_digest.get("needs_input", []),
                        },
                        step_kind="turn",
                        step_status="completed",
                        phase="finalization",
                        step_id=str(shared.get("turn_step_id")),
                        duration_ms=duration_ms,
                        display={
                            "title": "Turn Completed",
                            "badge": "ok",
                            "severity": "success",
                        },
                        metrics={
                            "tool_steps_used": node_results["tool_steps_used"],
                            "wave_count": node_results["wave_count"],
                        },
                    ),
                )
            )
            if turn_tx is not None and bool(getattr(turn_tx, "is_active", False)):
                maybe_commit = turn_tx.commit()
                if inspect.isawaitable(maybe_commit):
                    await maybe_commit
            logger.info(
                "Advisor turn completed trace=%s session=%s student=%s status=ok waves=%s tool_steps=%s input_compacted=%s context_compacted=%s output_compacted=%s context_chars=%s",
                trace_id,
                session_id,
                str(student_id) if student_id is not None else "anonymous",
                node_results["wave_count"],
                node_results["tool_steps_used"],
                bool(shared.get("input_compacted", False)),
                bool(shared.get("context_compacted", False)),
                bool(aggregate_stats.get("output_compacted", False)),
                int(shared.get("context_chars", 0)),
            )
            return result
        except Exception as exc:
            logger.exception("Advisor turn failed and was rolled back: trace=%s", trace_id)
            if isinstance(exc, asyncio.TimeoutError):
                shared["guardrail_triggered"] = True
                shared["failure_reason_code"] = "TURN_TIMEOUT"
            elif not shared.get("failure_reason_code"):
                shared["failure_reason_code"] = "TURN_FAILED"
            started_at = str(shared.get("started_at", ""))
            ended_at = datetime.now(UTC)
            duration_ms = 0
            try:
                if started_at:
                    duration_ms = max(0, int((ended_at - datetime.fromisoformat(started_at)).total_seconds() * 1000))
            except Exception:
                duration_ms = 0
            if turn_tx is not None and bool(getattr(turn_tx, "is_active", False)):
                maybe_rollback = turn_tx.rollback()
                if inspect.isawaitable(maybe_rollback):
                    await maybe_rollback
            failure_reason_code = str(shared.get("failure_reason_code", "TURN_FAILED"))
            failed_capability = self._extract_failed_capability(str(exc))
            recovery_hint = self._build_recovery_hint(
                reason_code=failure_reason_code,
                failed_capability=failed_capability,
            )
            await emit_event(
                TurnEvent(
                    trace_id=trace_id,
                    event="rollback",
                    data=self._with_step_fields(
                        shared=shared,
                        data={
                            "reason": str(exc),
                            "active_skill_id": str(shared.get("active_skill_id", "default")),
                            "skill_route_source": str(shared.get("skill_route_source", "default")),
                            "what_done": (
                                f"Turn failed during {failed_capability} and database changes were rolled back."
                            ),
                            "why_next": "Need one corrected input so the next turn can re-run safely.",
                            "needs_input": [recovery_hint],
                            "recovery_hint": recovery_hint,
                        },
                        step_kind="rollback",
                        step_status="failed",
                        phase="rollback",
                        wave_index=int(shared.get("wave_count", 0)) if shared.get("wave_count") else None,
                        compact_reason_code=failure_reason_code,
                        display={
                            "title": "Rollback",
                            "badge": "rollback",
                            "severity": "error",
                        },
                        metrics={
                            "tool_steps_used": int(shared.get("tool_steps_used", 0)),
                            "wave_count": int(shared.get("wave_count", 0)),
                        },
                    ),
                )
            )
            error_block = ChatBlock(
                id=str(uuid.uuid4()),
                kind="error",
                capability_id="orchestrator",
                order=0,
                payload={
                    "message": self._build_failure_message(
                        reason_code=failure_reason_code,
                        failed_capability=failed_capability,
                        recovery_hint=recovery_hint,
                    )
                },
                meta={"trace_id": trace_id},
            )
            result = TurnResult(
                trace_id=trace_id,
                status="error",
                content=error_block.payload["message"],
                blocks=[error_block],
                actions=[],
                execution_digest={
                    "summary": f"本轮在 {failed_capability} 阶段失败并已回滚。",
                    "what_done": "Rollback completed and no partial business blocks were persisted.",
                    "why_next": "Collect one corrected input and retry.",
                    "needs_input": [recovery_hint],
                    "wave_count": int(shared.get("wave_count", 0)),
                    "tool_steps_used": int(shared.get("tool_steps_used", 0)),
                    "steps": [],
                },
                usage={
                    "rolled_back": True,
                    "tool_steps_used": int(shared.get("tool_steps_used", 0)),
                    "tool_step_budget": int(MAX_TOOL_STEPS_PER_TURN),
                    "wave_count": int(shared.get("wave_count", 0)),
                    "duration_ms": duration_ms,
                    "guardrail_triggered": bool(shared.get("guardrail_triggered", False)),
                    "context_compacted": bool(shared.get("context_compacted", False)),
                    "input_compacted": bool(shared.get("input_compacted", False)),
                    "output_compacted": False,
                    "compression_passes": int(shared.get("compression_passes", 0)),
                    "context_chars": int(shared.get("context_chars", 0)),
                    "active_skill_id": str(shared.get("active_skill_id", "default")),
                    "skill_route_source": str(shared.get("skill_route_source", "default")),
                    "skill_contract_version": str(shared.get("skill_contract_version", _SKILL_CONTRACT_VERSION)),
                    "skill_route_modifiers": list(shared.get("skill_route_modifiers", [])),
                    "cap_retry_count": int(shared.get("cap_retry_count", 0)),
                    "cap_degraded": bool(shared.get("cap_degraded", False)),
                    "failure_reason_code": str(shared.get("failure_reason_code", "") or ""),
                    "required_output_missing": bool(shared.get("required_output_missing", False)),
                },
            )
            await emit_event(
                TurnEvent(
                    trace_id=trace_id,
                    event="turn_completed",
                    data=self._with_step_fields(
                        shared=shared,
                        data={
                            "status": "error",
                            "block_count": 1,
                            "wave_count": int(shared.get("wave_count", 0)),
                            "tool_steps_used": int(shared.get("tool_steps_used", 0)),
                            "tool_step_budget": int(MAX_TOOL_STEPS_PER_TURN),
                            "duration_ms": duration_ms,
                            "context_compacted": bool(shared.get("context_compacted", False)),
                            "input_compacted": bool(shared.get("input_compacted", False)),
                            "output_compacted": False,
                            "compression_passes": int(shared.get("compression_passes", 0)),
                            "context_chars": int(shared.get("context_chars", 0)),
                            "active_skill_id": str(shared.get("active_skill_id", "default")),
                            "skill_route_source": str(shared.get("skill_route_source", "default")),
                            "skill_contract_version": str(shared.get("skill_contract_version", _SKILL_CONTRACT_VERSION)),
                            "skill_route_modifiers": list(shared.get("skill_route_modifiers", [])),
                            "cap_retry_count": int(shared.get("cap_retry_count", 0)),
                            "cap_degraded": bool(shared.get("cap_degraded", False)),
                            "failure_reason_code": str(shared.get("failure_reason_code", "") or ""),
                            "required_output_missing": bool(shared.get("required_output_missing", False)),
                            "what_done": (
                                f"Turn failed during {failed_capability}; state was rolled back."
                            ),
                            "why_next": "Provide one corrected follow-up input to restart this turn.",
                            "needs_input": [recovery_hint],
                            "recovery_hint": recovery_hint,
                        },
                        step_kind="turn",
                        step_status="failed",
                        phase="finalization",
                        step_id=str(shared.get("turn_step_id")),
                        duration_ms=duration_ms,
                        compact_reason_code=failure_reason_code,
                        display={
                            "title": "Turn Failed",
                            "badge": "error",
                            "severity": "error",
                        },
                        metrics={
                            "tool_steps_used": int(shared.get("tool_steps_used", 0)),
                            "wave_count": int(shared.get("wave_count", 0)),
                        },
                    ),
                )
            )
            logger.info(
                "Advisor turn completed trace=%s session=%s student=%s status=error waves=%s tool_steps=%s guardrail=%s input_compacted=%s context_compacted=%s",
                trace_id,
                session_id,
                str(student_id) if student_id is not None else "anonymous",
                int(shared.get("wave_count", 0)),
                int(shared.get("tool_steps_used", 0)),
                bool(shared.get("guardrail_triggered", False)),
                bool(shared.get("input_compacted", False)),
                bool(shared.get("context_compacted", False)),
            )
            return result

    async def _plan_capabilities(
        self,
        *,
        message: str,
        context: dict[str, Any],
        student_id: uuid.UUID | None,
        route_plan: RoutePlan | dict[str, Any] | None = None,
    ) -> list[PlannedCapability]:
        normalized_route_plan = self._normalize_route_plan(route_plan)
        context["route_plan"] = normalized_route_plan
        split = await self._split_intents(message=message, context=context)
        if not split:
            split = ["general"]

        if normalized_route_plan is not None:
            split = self._merge_route_plan_capabilities(
                split=split,
                route_plan=normalized_route_plan,
            )

        # Student-auth-required capabilities are downgraded when anonymous.
        gated = {
            "guided_intake",
            "recommendation_subagent",
            "school_query",
            "strategy",
            "offer_compare",
            "what_if",
            "profile_read",
            "profile_update",
        }
        if student_id is None:
            split = [item for item in split if item not in gated]
            if not split:
                split = ["general"]

        unique: list[str] = []
        for item in split:
            if item not in self._registry:
                continue
            if item in unique:
                continue
            unique.append(item)
        if not unique:
            unique = ["general"]
        route_decision = self._resolve_active_skill(
            message=message,
            context=context,
            capability_ids=unique,
            route_plan=normalized_route_plan,
        )
        required_capabilities = self._resolve_required_capabilities(
            active_skill_id=route_decision.active_skill_id,
            route_plan=normalized_route_plan,
        )
        required_outputs = self._resolve_required_outputs(
            active_skill_id=route_decision.active_skill_id,
            route_plan=normalized_route_plan,
        )
        unique = self._order_capabilities_by_skill(
            capability_ids=unique,
            active_skill_id=route_decision.active_skill_id,
        )
        unique = self._inject_required_capabilities(
            capability_ids=unique,
            required_capabilities=required_capabilities,
            active_skill_id=route_decision.active_skill_id,
        )
        unique = self._inject_profile_read_capability(unique)
        context["active_skill_id"] = route_decision.active_skill_id
        context["skill_route_source"] = route_decision.source
        context["skill_route_confidence"] = route_decision.confidence
        context["skill_route_signals"] = list(route_decision.matched_signals)
        context["skill_route_modifiers"] = list(route_decision.modifiers)
        context["required_capabilities"] = list(required_capabilities)
        context["required_outputs"] = list(required_outputs)
        context["skill_contract_version"] = _SKILL_CONTRACT_VERSION

        planned: list[PlannedCapability] = []
        for idx, capability_id in enumerate(unique):
            spec = self._registry[capability_id]
            deps = [dep for dep in spec.dependencies if dep in unique]
            planned.append(
                PlannedCapability(
                    id=capability_id,
                    is_primary=(idx == 0),
                    dependencies=deps,
                    plan_order=idx,
                )
            )
        return planned

    async def _split_intents(self, *, message: str, context: dict[str, Any]) -> list[str]:
        layered_memory = self._format_layered_memory(context)
        compacted_message = str(context.get("compressed_user_message", message))
        prompt = (
            "You are an intent splitter for a college-admissions advisor.\n"
            "Return JSON only.\n"
            "Pick one or more capabilities in execution priority order.\n"
            "Allowed capability ids:\n"
            "- profile_read\n"
            "- profile_update\n"
            "- guided_intake\n"
            "- recommendation_subagent\n"
            "- school_query\n"
            "- strategy\n"
            "- offer_compare\n"
            "- what_if\n"
            "- emotional_support\n"
            "- general\n"
            "Output schema:\n"
            "{\"capabilities\": [\"id1\", \"id2\", ...]}\n"
        )
        messages = [
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": (
                    f"Recent context:\n{context.get('recent_messages', '')}\n\n"
                    f"Layered memory:\n{layered_memory}\n\n"
                    f"User message:\n{compacted_message}"
                ),
            },
        ]
        try:
            result = await self._llm.complete_json(
                messages,
                temperature=0.1,
                max_tokens=256,
                caller="chat.intent_split",
            )
            capabilities = result.get("capabilities", [])
            if isinstance(capabilities, list):
                picked = [str(item).strip() for item in capabilities if str(item).strip()]
                if picked:
                    context["intent_split_source"] = "llm"
                    return picked
        except Exception:
            logger.warning("Intent split failed; falling back to heuristics", exc_info=True)

        context["intent_split_source"] = "heuristic"
        return self._heuristic_split(message=message, context=context)

    def _heuristic_split(self, *, message: str, context: dict[str, Any]) -> list[str]:
        raw = message.lower()
        out: list[str] = []
        if re.search(r"\b(show|view|read|get).*(profile|portfolio)\b|查看档案|读取档案|看下我的档案", raw):
            out.append("profile_read")
        if re.search(r"\b(update|change|edit|set).*(profile|gpa|sat|act|toefl|major|budget|aid)\b|更新档案|修改档案|修改gpa|更新预算|改专业", raw):
            out.append("profile_update")
        if re.search(r"\bconfirm_profile_patch:[a-f0-9-]+\b|\bconfirm\b.*\b(profile|patch)\b|确认修改|确认提交", raw):
            out.append("profile_update")
        if re.search(r"\b(recommend|shortlist|school list)\b|推荐|选校|学校清单", raw):
            out.append("recommendation_subagent")
        if re.search(r"\b(compare|offer|admitted)\b|offer|录取|对比|比较", raw):
            out.append("offer_compare")
        if re.search(r"\b(what if|hypothetical)\b|如果|假设|模拟", raw):
            out.append("what_if")
        if re.search(r"\b(ed|ea|rd|timeline|strategy)\b|早申|申请策略|时间安排", raw):
            out.append("strategy")
        if re.search(r"\b(university|college|school)\b|大学|学校|斯坦福|mit|harvard", raw):
            out.append("school_query")
        if re.search(r"\b(gpa|sat|act|toefl|budget|major|活动)\b|gpa|sat|预算|专业|活动", raw):
            out.append("guided_intake")
        if re.search(r"\b(stress|anxious|panic|worried)\b|焦虑|紧张|压力", raw):
            out.append("emotional_support")

        if not out:
            # Keep backward compatibility for simple prompts.
            # Use the old single-intent classifier as a fallback signal.
            out = []
        return out or ["general"]

    def _normalize_route_plan(
        self,
        route_plan: RoutePlan | dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if route_plan is None:
            return None
        data = (
            route_plan.model_dump(mode="json")
            if isinstance(route_plan, RoutePlan)
            else dict(route_plan)
        )
        primary_task_raw = str(data.get("primary_task") or "").strip()
        if primary_task_raw not in _ROUTE_TASK_TO_SKILL:
            raise ValueError(f"Unsupported route_plan.primary_task: {primary_task_raw!r}")
        route_lock = bool(data.get("route_lock", True))
        modifiers: list[str] = []
        for item in data.get("modifiers", []) if isinstance(data.get("modifiers"), list) else []:
            modifier = str(item).strip()
            if modifier in _ROUTE_ALLOWED_MODIFIERS and modifier not in modifiers:
                modifiers.append(modifier)
        required_capabilities: list[str] = []
        for item in data.get("required_capabilities", []) if isinstance(data.get("required_capabilities"), list) else []:
            cap_id = str(item).strip()
            if cap_id and cap_id in self._registry and cap_id not in required_capabilities:
                required_capabilities.append(cap_id)
        required_outputs: list[str] = []
        for item in data.get("required_outputs", []) if isinstance(data.get("required_outputs"), list) else []:
            output_id = str(item).strip()
            if output_id in _ROUTE_ALLOWED_REQUIRED_OUTPUTS and output_id not in required_outputs:
                required_outputs.append(output_id)
        return {
            "primary_task": primary_task_raw,
            "primary_skill_id": _ROUTE_TASK_TO_SKILL[primary_task_raw],
            "primary_capability_id": _ROUTE_TASK_TO_PRIMARY_CAPABILITY[primary_task_raw],
            "route_lock": route_lock,
            "modifiers": modifiers,
            "required_capabilities": required_capabilities,
            "required_outputs": required_outputs,
        }

    def _merge_route_plan_capabilities(
        self,
        *,
        split: list[str],
        route_plan: dict[str, Any],
    ) -> list[str]:
        out: list[str] = []
        primary_capability = str(route_plan.get("primary_capability_id") or "").strip()
        route_lock = bool(route_plan.get("route_lock", True))
        if primary_capability and primary_capability in self._registry:
            out.append(primary_capability)
        if not route_lock:
            for capability_id in split:
                if capability_id in self._registry and capability_id not in out:
                    out.append(capability_id)
        required_capabilities = route_plan.get("required_capabilities")
        if isinstance(required_capabilities, list):
            for item in required_capabilities:
                capability_id = str(item).strip()
                if capability_id in self._registry and capability_id not in out:
                    out.append(capability_id)
        if not out:
            out = [item for item in split if item in self._registry]
        return out or ["general"]

    def _resolve_required_capabilities(
        self,
        *,
        active_skill_id: str,
        route_plan: dict[str, Any] | None,
    ) -> list[str]:
        out: list[str] = []
        if route_plan and isinstance(route_plan.get("required_capabilities"), list):
            for item in route_plan.get("required_capabilities", []):
                capability_id = str(item).strip()
                if capability_id in self._registry and capability_id not in out:
                    out.append(capability_id)
        if active_skill_id == "recommendation" and "recommendation_subagent" in self._registry:
            if "recommendation_subagent" not in out:
                out.append("recommendation_subagent")
        return out

    @staticmethod
    def _resolve_required_outputs(
        *,
        active_skill_id: str,
        route_plan: dict[str, Any] | None,
    ) -> list[str]:
        out: list[str] = []
        if route_plan and isinstance(route_plan.get("required_outputs"), list):
            for item in route_plan.get("required_outputs", []):
                output_id = str(item).strip()
                if output_id in _ROUTE_ALLOWED_REQUIRED_OUTPUTS and output_id not in out:
                    out.append(output_id)
        if active_skill_id == "recommendation" and "recommendation_payload" not in out:
            out.append("recommendation_payload")
        return out

    def _inject_required_capabilities(
        self,
        *,
        capability_ids: list[str],
        required_capabilities: list[str],
        active_skill_id: str,
    ) -> list[str]:
        if not required_capabilities:
            return capability_ids
        out: list[str] = []
        primary = list(_SKILL_PRIMARY_CAPABILITY.get(active_skill_id, ()))
        for capability_id in primary:
            if capability_id in capability_ids and capability_id not in out:
                out.append(capability_id)
        for capability_id in required_capabilities:
            if capability_id in self._registry and capability_id not in out:
                out.append(capability_id)
        for capability_id in capability_ids:
            if capability_id not in out:
                out.append(capability_id)
        return out

    def _resolve_active_skill(
        self,
        *,
        message: str,
        context: dict[str, Any],
        capability_ids: list[str],
        route_plan: dict[str, Any] | None = None,
    ) -> SkillRouteDecision:
        message_text = str(message or "")
        history_text = str(context.get("recent_messages", ""))
        split_source = str(context.get("intent_split_source") or "heuristic")
        modifiers: list[str] = []

        memory_spec = _SKILL_SPECS.get("memory_followup")
        if memory_spec is not None:
            memory_matched = self._match_skill_spec(
                spec=memory_spec,
                capability_ids=capability_ids,
                message_text=message_text,
                history_text=history_text,
            )
            has_message_followup_signal = any(item.startswith("msg:") for item in memory_matched)
            # Keep memory_followup as an explicit continuation modifier only.
            if has_message_followup_signal and "memory_followup" not in modifiers:
                modifiers.append("memory_followup")
        if route_plan and isinstance(route_plan.get("modifiers"), list):
            for item in route_plan.get("modifiers", []):
                modifier = str(item).strip()
                if modifier in _ROUTE_ALLOWED_MODIFIERS and modifier not in modifiers:
                    modifiers.append(modifier)

        if route_plan and bool(route_plan.get("route_lock", True)):
            forced_skill = str(route_plan.get("primary_skill_id") or "").strip()
            if forced_skill in _SKILL_SPECS:
                return SkillRouteDecision(
                    active_skill_id=forced_skill,
                    source="route_plan_lock",
                    confidence=0.99,
                    matched_signals=["route_plan.primary_task"],
                    modifiers=tuple(modifiers),
                )

        if re.search(r"\bconfirm_profile_patch:[a-f0-9-]+\b|确认修改|确认提交", message_text, re.IGNORECASE):
            return SkillRouteDecision(
                active_skill_id="profile_update",
                source="explicit_command",
                confidence=0.99,
                matched_signals=["confirm_profile_patch"],
                modifiers=tuple(modifiers),
            )

        if re.search(
            r"(报错|失败|超时|卡住|不稳定|重试|retry|timeout|failed|rollback|回滚|reject)",
            message_text,
            re.IGNORECASE,
        ):
            return SkillRouteDecision(
                active_skill_id="robustness",
                source="robustness_signal",
                confidence=0.95,
                matched_signals=["error_or_retry_signal"],
                modifiers=tuple(modifiers),
            )

        domain_caps = [
            item for item in capability_ids
            if item not in {"general", "emotional_support", "profile_read"}
        ]
        multi_domain_caps = [item for item in domain_caps if item != "guided_intake"]
        multi_markers = bool(re.search(r"(同时|并且|另外|然后再|并行|一轮里|多用途)", message_text, re.IGNORECASE))
        is_multi_intent_candidate = len(multi_domain_caps) >= 2 or multi_markers

        recommendation_spec = _SKILL_SPECS.get("recommendation")
        if recommendation_spec is not None and not is_multi_intent_candidate:
            recommendation_matched = self._match_skill_spec(
                spec=recommendation_spec,
                capability_ids=capability_ids,
                message_text=message_text,
                history_text=history_text,
            )
            if any(item.startswith("msg:") for item in recommendation_matched):
                return SkillRouteDecision(
                    active_skill_id="recommendation",
                    source=f"explicit_recommendation_signal:{split_source}",
                    confidence=0.93,
                    matched_signals=recommendation_matched,
                    modifiers=tuple(modifiers),
                )

        low_score_order = (
            "what_if",
            "guided_intake",
            "profile_update",
            "offer_compare",
        )
        for skill_id in low_score_order:
            spec = _SKILL_SPECS.get(skill_id)
            if spec is None:
                continue
            matched = self._match_skill_spec(
                spec=spec,
                capability_ids=capability_ids,
                message_text=message_text,
                history_text=history_text,
            )
            if matched:
                return SkillRouteDecision(
                    active_skill_id=skill_id,
                    source=f"priority_low_score:{split_source}",
                    confidence=0.9,
                    matched_signals=matched,
                    modifiers=tuple(modifiers),
                )

        if len(capability_ids) >= 2:
            if is_multi_intent_candidate:
                signals = [f"capability_count={len(multi_domain_caps)}"]
                if multi_markers:
                    signals.append("multi_intent_marker")
                return SkillRouteDecision(
                    active_skill_id="multi_intent",
                    source=f"planner_multi_intent:{split_source}",
                    confidence=0.84,
                    matched_signals=signals,
                    modifiers=tuple(modifiers),
                )

        scored_candidates: list[tuple[float, SkillSpec, list[str]]] = []
        for skill_id, spec in _SKILL_SPECS.items():
            if skill_id in {"default", "multi_intent", "robustness", "memory_followup"}:
                continue
            matched = self._match_skill_spec(
                spec=spec,
                capability_ids=capability_ids,
                message_text=message_text,
                history_text=history_text,
            )
            if not matched:
                continue
            score = float(spec.priority) + (0.6 * len(matched))
            scored_candidates.append((score, spec, matched))

        if scored_candidates:
            scored_candidates.sort(key=lambda item: item[0], reverse=True)
            _score, best_spec, matched = scored_candidates[0]
            return SkillRouteDecision(
                active_skill_id=best_spec.id,
                source=f"skill_spec:{split_source}",
                confidence=0.8,
                matched_signals=matched,
                modifiers=tuple(modifiers),
            )

        return SkillRouteDecision(
            active_skill_id="default",
            source=f"default:{split_source}",
            confidence=0.6,
            matched_signals=[],
            modifiers=tuple(modifiers),
        )

    @staticmethod
    def _match_skill_spec(
        *,
        spec: SkillSpec,
        capability_ids: list[str],
        message_text: str,
        history_text: str,
    ) -> list[str]:
        matched: list[str] = []
        capability_set = set(capability_ids)
        for hint in spec.capability_hints:
            if hint in capability_set and f"cap:{hint}" not in matched:
                matched.append(f"cap:{hint}")
        for pattern in spec.message_patterns:
            if re.search(pattern, message_text, re.IGNORECASE):
                key = f"msg:{pattern}"
                if key not in matched:
                    matched.append(key)
        for pattern in spec.history_patterns:
            if re.search(pattern, history_text, re.IGNORECASE):
                key = f"history:{pattern}"
                if key not in matched:
                    matched.append(key)
        return matched

    def _order_capabilities_by_skill(
        self,
        *,
        capability_ids: list[str],
        active_skill_id: str,
    ) -> list[str]:
        unique: list[str] = []
        for item in capability_ids:
            if item not in unique:
                unique.append(item)
        if not unique:
            return ["general"]

        if active_skill_id == "multi_intent":
            ordered = sorted(
                unique,
                key=lambda item: (
                    _SKILL_CAPABILITY_PRIORITY.get(item, 99),
                    capability_ids.index(item),
                ),
            )
            return ordered

        preferred = list(_SKILL_PRIMARY_CAPABILITY.get(active_skill_id, ()))
        if not preferred:
            return unique
        out: list[str] = []
        for cap in preferred:
            if cap in unique and cap not in out:
                out.append(cap)
        for cap in unique:
            if cap not in out:
                out.append(cap)
        return out

    @staticmethod
    def _inject_profile_read_capability(unique: list[str]) -> list[str]:
        has_dependent = any(item in _PROFILE_DEPENDENT_CAPABILITIES for item in unique)
        if not has_dependent:
            return unique
        if "profile_read" in unique:
            return unique
        first_dependent = next(
            (idx for idx, item in enumerate(unique) if item in _PROFILE_DEPENDENT_CAPABILITIES),
            0,
        )
        out = list(unique)
        out.insert(first_dependent, "profile_read")
        return out

    @staticmethod
    def _get_nested_value(source: dict[str, Any], path: tuple[str, ...]) -> Any:
        current: Any = source
        for key in path:
            if not isinstance(current, dict):
                return None
            if key not in current:
                return None
            current = current.get(key)
        return current

    def _has_hard_intake_missing_fields(self, *, context: dict[str, Any]) -> bool:
        profile_structured = (
            context.get("profile_structured")
            if isinstance(context.get("profile_structured"), dict)
            else {}
        )

        budget = context.get("profile_budget_usd")
        if budget in (None, ""):
            budget = self._get_nested_value(profile_structured, ("finance", "budget_usd"))
        majors = context.get("profile_intended_majors")
        if not isinstance(majors, list):
            majors = self._get_nested_value(profile_structured, ("academics", "intended_majors"))
        gpa = context.get("profile_gpa")
        if gpa in (None, ""):
            gpa = self._get_nested_value(profile_structured, ("academics", "gpa"))
        sat = context.get("profile_sat_total")
        if sat in (None, ""):
            sat = self._get_nested_value(profile_structured, ("academics", "sat_total"))
        act = context.get("profile_act_composite")
        if act in (None, ""):
            act = self._get_nested_value(profile_structured, ("academics", "act_composite"))

        budget_missing = budget in (None, "")
        majors_missing = not isinstance(majors, list) or not any(str(item).strip() for item in majors)
        academics_all_missing = all(value in (None, "") for value in (gpa, sat, act))
        return bool(budget_missing and majors_missing and academics_all_missing)

    async def _load_context_layers(self, session_id: str) -> dict[str, dict[str, Any]]:
        get_layers = getattr(self._memory, "get_context_layers", None)
        if callable(get_layers):
            raw = await get_layers(session_id)
            if isinstance(raw, dict):
                merged = raw.get("merged")
                short_term = raw.get("short_term")
                working = raw.get("working")
                long_term = raw.get("long_term")
                if all(isinstance(item, dict) for item in (merged, short_term, working, long_term)):
                    return {
                        "merged": dict(merged),
                        "short_term": dict(short_term),
                        "working": dict(working),
                        "long_term": dict(long_term),
                    }

        fallback = await self._memory.get_context(session_id)
        fallback_context = fallback if isinstance(fallback, dict) else {}
        return {
            "merged": dict(fallback_context),
            "short_term": {},
            "working": dict(fallback_context),
            "long_term": {},
        }

    async def _load_student_context_layers(
        self,
        student_id: uuid.UUID | None,
    ) -> dict[str, dict[str, Any]]:
        empty = {"merged": {}, "short_term": {}, "working": {}, "long_term": {}}
        if student_id is None:
            return empty

        get_student_layers = getattr(self._memory, "get_student_context_layers", None)
        if callable(get_student_layers):
            raw = await get_student_layers(student_id)
            if isinstance(raw, dict):
                merged = raw.get("merged")
                short_term = raw.get("short_term")
                working = raw.get("working")
                long_term = raw.get("long_term")
                if all(isinstance(item, dict) for item in (merged, short_term, working, long_term)):
                    return {
                        "merged": dict(merged),
                        "short_term": dict(short_term),
                        "working": dict(working),
                        "long_term": dict(long_term),
                    }

        # Backward compatibility: historically some handlers wrote to context using str(student_id).
        fallback = await self._memory.get_context(str(student_id))
        fallback_context = fallback if isinstance(fallback, dict) else {}
        return {
            "merged": dict(fallback_context),
            "short_term": {},
            "working": dict(fallback_context),
            "long_term": {},
        }

    async def _load_profile_structured_memory(
        self,
        student_id: uuid.UUID | None,
    ) -> dict[str, Any]:
        if student_id is None:
            return {}
        try:
            snapshot = await build_profile_snapshot(
                session=self._session,
                student_id=student_id,
            )
        except Exception:
            logger.warning(
                "Failed to load profile structured memory for context assembly",
                exc_info=True,
            )
            return {}

        payload = snapshot.get("payload") if isinstance(snapshot.get("payload"), dict) else {}
        portfolio = payload.get("portfolio") if isinstance(payload.get("portfolio"), dict) else {}
        completion = payload.get("completion") if isinstance(payload.get("completion"), dict) else {}
        return self._extract_profile_facts(portfolio=portfolio, completion=completion)

    @staticmethod
    def _merge_context_sources(
        *,
        session_layers: dict[str, dict[str, Any]],
        student_layers: dict[str, dict[str, Any]],
        profile_facts: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        short_term = {
            **student_layers.get("short_term", {}),
            **session_layers.get("short_term", {}),
        }
        working = {
            **student_layers.get("working", {}),
            **session_layers.get("working", {}),
        }
        long_term = {
            **student_layers.get("long_term", {}),
            **session_layers.get("long_term", {}),
            **profile_facts,
        }
        merged = {
            **long_term,
            **working,
            **short_term,
        }
        return {
            "short_term": short_term,
            "working": working,
            "long_term": long_term,
            "merged": merged,
        }

    @staticmethod
    def _compact_user_message(message: str) -> tuple[str, bool]:
        if len(message) <= _INPUT_COMPACT_TRIGGER_CHARS:
            return message, False
        head = message[: int(_INPUT_COMPACT_TARGET_CHARS * 0.65)]
        tail = message[-int(_INPUT_COMPACT_TARGET_CHARS * 0.25) :]
        lines = [line.strip() for line in message.splitlines() if line.strip()]
        salient = [
            line
            for line in lines
            if re.search(r"\d|gpa|sat|act|budget|aid|major|offer|ed|ea|rd|profile|确认|修改", line, re.IGNORECASE)
        ]
        middle = "\n".join(salient[:4]).strip()
        compacted = f"{head}\n...\n{middle}\n...\n{tail}".strip()
        return compacted[:_INPUT_COMPACT_TARGET_CHARS], True

    @classmethod
    def _compact_layered_context(
        cls,
        layered_context: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        before_chars = cls._estimate_context_chars(
            layered_context.get("short_term", {}) if isinstance(layered_context.get("short_term"), dict) else {},
            layered_context.get("working", {}) if isinstance(layered_context.get("working"), dict) else {},
            layered_context.get("long_term", {}) if isinstance(layered_context.get("long_term"), dict) else {},
        )
        compacted = False
        pass_count = 1
        quotas = _LAYER_QUOTAS_DEFAULT
        short_term = cls._compact_layer_map(layered_context.get("short_term", {}), *quotas["short_term"])
        working = cls._compact_layer_map(layered_context.get("working", {}), *quotas["working"])
        long_term = cls._compact_layer_map(layered_context.get("long_term", {}), *quotas["long_term"])
        chars = cls._estimate_context_chars(short_term, working, long_term)

        if chars > _CONTEXT_ACTIVE_BUDGET_CHARS:
            compacted = True
            pass_count += 1
            quotas = _LAYER_QUOTAS_TIGHT
            short_term = cls._compact_layer_map(layered_context.get("short_term", {}), *quotas["short_term"])
            working = cls._compact_layer_map(layered_context.get("working", {}), *quotas["working"])
            long_term = cls._compact_layer_map(layered_context.get("long_term", {}), *quotas["long_term"])
            chars = cls._estimate_context_chars(short_term, working, long_term)

        if chars > _CONTEXT_HARD_BUDGET_CHARS:
            compacted = True
            pass_count += 1
            quotas = _LAYER_QUOTAS_HARD
            short_term = cls._compact_layer_map(layered_context.get("short_term", {}), *quotas["short_term"])
            working = cls._compact_layer_map(layered_context.get("working", {}), *quotas["working"])
            long_term = cls._compact_layer_map(layered_context.get("long_term", {}), *quotas["long_term"])
            chars = cls._estimate_context_chars(short_term, working, long_term)

        merged = {**long_term, **working, **short_term}
        return (
            {
                "short_term": short_term,
                "working": working,
                "long_term": long_term,
                "merged": merged,
            },
            {
                "before_chars": before_chars,
                "chars": chars,
                "compacted": compacted,
                "passes": pass_count,
                "quota_profile": "hard" if quotas is _LAYER_QUOTAS_HARD else ("tight" if quotas is _LAYER_QUOTAS_TIGHT else "default"),
            },
        )

    @classmethod
    def _compact_layer_map(
        cls,
        values: dict[str, Any],
        max_items: int,
        max_value_chars: int,
    ) -> dict[str, Any]:
        if not isinstance(values, dict):
            return {}
        ordered_keys = []
        for key in values:
            if key in _CONTEXT_CRITICAL_KEYS:
                ordered_keys.append(key)
        for key in values:
            if key not in ordered_keys:
                ordered_keys.append(key)

        compacted: dict[str, Any] = {}
        for key in ordered_keys:
            if len(compacted) >= max_items:
                break
            value = values.get(key)
            compacted[key] = cls._clip_value(value, max_chars=max_value_chars)
        return compacted

    @classmethod
    def _clip_value(cls, value: Any, *, max_chars: int) -> Any:
        if isinstance(value, str):
            return cls._clip_text(value, max_chars)
        if isinstance(value, (int, float, bool)) or value is None:
            return value
        if isinstance(value, list):
            out: list[Any] = []
            for item in value[:8]:
                out.append(cls._clip_value(item, max_chars=max_chars // 2))
            return out
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for idx, (key, item) in enumerate(value.items()):
                if idx >= 8:
                    break
                out[str(key)] = cls._clip_value(item, max_chars=max_chars // 2)
            return out
        return cls._clip_text(str(value), max_chars)

    @staticmethod
    def _clip_text(text: str, max_chars: int) -> str:
        if len(text) <= max_chars:
            return text
        head = text[: int(max_chars * 0.7)]
        tail = text[-int(max_chars * 0.2) :]
        return f"{head}\n...\n{tail}"

    @staticmethod
    def _estimate_context_chars(short_term: dict[str, Any], working: dict[str, Any], long_term: dict[str, Any]) -> int:
        return len(str({"short_term": short_term, "working": working, "long_term": long_term}))

    @staticmethod
    def _extract_profile_facts(
        *,
        portfolio: dict[str, Any],
        completion: dict[str, Any],
    ) -> dict[str, Any]:
        academics = portfolio.get("academics") if isinstance(portfolio.get("academics"), dict) else {}
        finance = portfolio.get("finance") if isinstance(portfolio.get("finance"), dict) else {}
        strategy = portfolio.get("strategy") if isinstance(portfolio.get("strategy"), dict) else {}
        preferences = portfolio.get("preferences") if isinstance(portfolio.get("preferences"), dict) else {}
        facts: dict[str, Any] = {
            "profile_completion_pct": completion.get("completion_pct"),
            "profile_missing_fields": completion.get("missing_fields") or [],
            "profile_gpa": academics.get("gpa"),
            "profile_sat_total": academics.get("sat_total"),
            "profile_budget_usd": finance.get("budget_usd"),
            "profile_need_financial_aid": finance.get("need_financial_aid"),
            "profile_target_year": strategy.get("target_year"),
            "profile_intended_majors": academics.get("intended_majors") or preferences.get("interests") or [],
        }
        return {key: value for key, value in facts.items() if value not in (None, "", [])}

    @staticmethod
    def _next_step_id(shared: dict[str, Any]) -> str:
        seq = int(shared.get("step_seq", 0)) + 1
        shared["step_seq"] = seq
        return f"step-{seq}"

    @staticmethod
    def _next_event_seq(shared: dict[str, Any]) -> int:
        seq = int(shared.get("event_seq", 0)) + 1
        shared["event_seq"] = seq
        return seq

    @classmethod
    def _get_wave_step_id(cls, shared: dict[str, Any], wave_index: int) -> str:
        wave_ids = shared.get("wave_step_ids")
        if not isinstance(wave_ids, dict):
            wave_ids = {}
            shared["wave_step_ids"] = wave_ids
        key = str(wave_index)
        existing = wave_ids.get(key)
        if isinstance(existing, str) and existing:
            return existing
        wave_step_id = cls._next_step_id(shared)
        wave_ids[key] = wave_step_id
        return wave_step_id

    @classmethod
    def _with_step_fields(
        cls,
        *,
        shared: dict[str, Any],
        data: dict[str, Any],
        step_kind: str,
        step_status: str,
        phase: str,
        wave_index: int | None = None,
        capability_id: str | None = None,
        duration_ms: int | None = None,
        checkpoint_summary: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        parent_step_id: str | None = None,
        step_id: str | None = None,
        compact_reason_code: str | None = None,
        display: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = dict(data)
        payload["trace_id"] = str(shared.get("trace_id", payload.get("trace_id", "")))
        if step_id is None:
            if step_kind == "wave" and wave_index is not None:
                step_id = cls._get_wave_step_id(shared, wave_index)
            else:
                step_id = cls._next_step_id(shared)
        payload["step_id"] = step_id
        payload["event_seq"] = cls._next_event_seq(shared)
        if parent_step_id is None and step_kind != "turn":
            turn_step_id = shared.get("turn_step_id")
            if isinstance(turn_step_id, str) and turn_step_id:
                parent_step_id = turn_step_id
        payload["parent_step_id"] = parent_step_id
        payload["step_kind"] = step_kind
        payload["step_status"] = step_status
        payload["phase"] = phase
        if wave_index is not None:
            payload["wave_index"] = int(wave_index)
        if capability_id:
            payload["capability_id"] = capability_id
        if duration_ms is not None:
            payload["duration_ms"] = int(duration_ms)
        if checkpoint_summary is not None:
            payload["checkpoint_summary"] = checkpoint_summary
        if compact_reason_code:
            payload["compact_reason_code"] = compact_reason_code
        if display is not None:
            payload["display"] = display
        if metrics is not None:
            payload["metrics"] = metrics
        return payload

    async def _save_student_memory_entries(
        self,
        *,
        student_id: uuid.UUID | None,
        values: dict[str, Any],
        layer: str = "long_term",
    ) -> None:
        if student_id is None or not values:
            return
        saver = getattr(self._memory, "save_student_contexts", None)
        if callable(saver):
            await saver(student_id, values, layer=layer)  # type: ignore[arg-type]

    @classmethod
    def _build_summary_nodes(
        cls,
        *,
        source: dict[str, Any],
        max_nodes: int,
        node_prefix: str,
    ) -> list[dict[str, Any]]:
        nodes: list[dict[str, Any]] = []
        for idx, (key, value) in enumerate(source.items()):
            snippet = cls._clip_text(str(value), 200).replace("\n", " ").strip()
            nodes.append(
                {
                    "id": f"{node_prefix}-{idx}",
                    "summary": cls._clip_text(f"{key}: {snippet}", 240),
                }
            )
        if len(nodes) <= max_nodes:
            return nodes
        kept = nodes[: max_nodes - 1]
        merged = " | ".join(item["summary"] for item in nodes[max_nodes - 1 :])
        kept.append(
            {
                "id": f"{node_prefix}-rollup",
                "summary": cls._clip_text(merged, 240),
            }
        )
        return kept

    def _apply_checkpoint_compaction(
        self,
        *,
        context: dict[str, Any],
        shared: dict[str, Any],
    ) -> None:
        memory_layers = context.get("memory_layers")
        if not isinstance(memory_layers, dict):
            return
        layered_context = {
            "short_term": memory_layers.get("short_term") if isinstance(memory_layers.get("short_term"), dict) else {},
            "working": memory_layers.get("working") if isinstance(memory_layers.get("working"), dict) else {},
            "long_term": memory_layers.get("long_term") if isinstance(memory_layers.get("long_term"), dict) else {},
        }
        compacted_layers, compaction = self._compact_layered_context(layered_context)
        memory_layers["short_term"] = dict(compacted_layers["short_term"])
        memory_layers["working"] = dict(compacted_layers["working"])
        memory_layers["long_term"] = dict(compacted_layers["long_term"])
        context["memory_layers"] = memory_layers
        context.update(compacted_layers["merged"])

        management = context.get("context_management")
        if not isinstance(management, dict):
            management = {}
        management["context_chars_before"] = int(compaction.get("before_chars", 0))
        management["context_chars_after"] = int(compaction.get("chars", 0))
        management["context_chars"] = int(compaction.get("chars", 0))
        management["compression_level"] = str(compaction.get("quota_profile", "default"))
        management["context_compacted"] = bool(
            management.get("context_compacted", False) or compaction.get("compacted", False)
        )
        management["compression_passes"] = int(management.get("compression_passes", 0)) + int(
            compaction.get("passes", 0)
        )
        context["context_management"] = management

        shared["context_chars"] = int(compaction.get("chars", 0))
        shared["context_compacted"] = bool(
            shared.get("context_compacted", False) or compaction.get("compacted", False)
        )
        shared["compression_passes"] = int(management.get("compression_passes", 0))

    @classmethod
    def _compress_block_payload(
        cls,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any], bool, int, int]:
        before_chars = len(str(payload))
        if before_chars <= 3000:
            return payload, False, before_chars, before_chars
        compacted_payload = cls._clip_value(payload, max_chars=420)
        after_chars = len(str(compacted_payload))
        return (
            compacted_payload if isinstance(compacted_payload, dict) else {},
            True,
            before_chars,
            after_chars,
        )

    @classmethod
    def _compress_text_content(cls, text: str, *, threshold: int = 900) -> tuple[str, bool]:
        cleaned = text.strip()
        if len(cleaned) <= threshold:
            return cleaned, False
        lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
        signal_lines = [
            line
            for line in lines
            if re.search(r"\d|%|gpa|sat|budget|aid|ed|ea|rd|offer|risk|建议|结论|next", line, re.IGNORECASE)
        ]
        head = cleaned[: int(threshold * 0.5)]
        middle = "\n".join(signal_lines[:4]).strip()
        tail = cleaned[-int(threshold * 0.2) :]
        merged = f"{head}\n...\n{middle}\n...\n{tail}".strip()
        return cls._clip_text(merged, threshold), True

    @staticmethod
    def _extract_failed_capability(error_text: str) -> str:
        match = re.search(r"Capability '([^']+)' failed", error_text)
        if match:
            return str(match.group(1)).strip()
        timeout_match = re.search(r"Capability '([^']+)' timed out", error_text)
        if timeout_match:
            return str(timeout_match.group(1)).strip()
        return "orchestrator"

    @staticmethod
    def _is_best_effort(spec: CapabilitySpec) -> bool:
        return str(spec.failure_policy).lower() == "best_effort"

    @classmethod
    def _capability_timeout_seconds(cls, spec: CapabilitySpec) -> float:
        if str(spec.failure_policy).lower() == "best_effort":
            override = _BEST_EFFORT_SOFT_TIMEOUT_BY_CAPABILITY.get(
                spec.id,
                BEST_EFFORT_SOFT_TIMEOUT_SECONDS,
            )
            return min(float(override), float(BEST_EFFORT_CAPABILITY_TIMEOUT_SECONDS))
        return CAPABILITY_TIMEOUT_SECONDS

    @staticmethod
    def _record_best_effort_degraded(shared: dict[str, Any], capability_id: str) -> None:
        shared["best_effort_degraded_count"] = int(shared.get("best_effort_degraded_count", 0)) + 1
        caps_raw = shared.get("best_effort_degraded_caps")
        caps = list(caps_raw) if isinstance(caps_raw, list) else []
        if capability_id not in caps:
            caps.append(capability_id)
        shared["best_effort_degraded_caps"] = caps

    def _build_best_effort_fallback_result(
        self,
        *,
        spec: CapabilitySpec,
        ctx: CapabilityContext,
        reason_code: str,
    ) -> CapabilityResult:
        recovery_hint = self._build_recovery_hint(reason_code=reason_code, failed_capability=spec.id)
        heuristic = self._build_best_effort_heuristic(
            capability_id=spec.id,
            reason_code=reason_code,
            recovery_hint=recovery_hint,
            message=ctx.message,
        )
        content = "\n".join(
            [
                heuristic["claim"],
                f"依据：{heuristic['evidence']}",
                f"下一步：{heuristic['actions'][0] if heuristic['actions'] else recovery_hint}",
                f"风险：{heuristic['risks'][0] if heuristic['risks'] else '请补充更明确的范围后重试。'}",
            ]
        )
        return CapabilityResult(
            content=content,
            blocks=[
                {
                    "kind": "text",
                    "payload": {"text": content},
                    "meta": {
                        "degraded": True,
                        "degraded_reason": reason_code,
                        "capability_id": spec.id,
                    },
                }
            ],
            meta={
                "degraded": True,
                "degraded_reason": reason_code,
                "what_done": heuristic["claim"],
                "why_next": heuristic["actions"][0] if heuristic["actions"] else recovery_hint,
                "synthesis_claim": heuristic["claim"],
                "synthesis_evidence": heuristic["evidence"],
                "action_hints": heuristic["actions"],
                "risks_missing": heuristic["risks"],
                "personalization_evidence": self._build_personalization_evidence(
                    ctx=ctx,
                    confidence=0.55,
                ),
            },
        )

    def _build_best_effort_heuristic(
        self,
        *,
        capability_id: str,
        reason_code: str,
        recovery_hint: str,
        message: str,
    ) -> dict[str, Any]:
        _ = detect_response_language(message or "")
        if capability_id == "what_if":
            return {
                "claim": "基于当前信息，关键指标上调通常会带来录取概率的正向变化，但幅度需结合目标校分层确认。",
                "evidence": "已保留你本轮问题中的变量变化方向，并沿用当前档案约束做方向性判断。",
                "actions": [
                    "先选 1 个可量化变量（SAT/GPA/活动）做两周冲刺，并记录变化前后数据。",
                    "同步准备 1 条风险兜底路径（保底校或预算替代方案），避免单点投入。",
                ],
                "risks": [
                    "缺少目标学校分层与当前基线分位，当前结论以方向性为主。",
                    "本轮采用了降级兜底，建议下一轮缩小输入范围以获得更完整分析。",
                ],
            }
        if capability_id == "strategy":
            return {
                "claim": "建议先按冲刺/匹配/保底三档推进，优先保证匹配与保底档按期提交。",
                "evidence": "策略节点已读取现有档案信息并保留申请周期约束，当前提供最小可执行节奏。",
                "actions": [
                    "本周先锁定三档学校清单（每档 2-3 所），并标注 ED/EA/RD 选择。",
                    "按截止时间倒排材料任务，优先完成公共文书与推荐信请求。",
                ],
                "risks": [
                    "若未补齐截止日期与预算边界，优先级仍可能波动。",
                    "本轮采用了降级兜底，建议下一轮聚焦单一策略目标。",
                ],
            }
        if capability_id in {"school_query", "offer_compare"}:
            return {
                "claim": "当前可先按学术匹配、成本资助、就业结果三维做初步比较，再补关键字段收敛结论。",
                "evidence": "比较框架已建立，后续通过补充缺失字段可以显著提高结论稳定性。",
                "actions": [
                    "先确认每个学校的总成本、净价和奖助结构，统一口径后再横向对比。",
                    "补齐至少 1 条非财务约束（专业方向或城市偏好），用于排序裁剪。",
                ],
                "risks": [
                    "缺少标准化比较字段时，当前结论仅适合作为筛选草案。",
                    "本轮采用了降级兜底，建议下一轮补齐 1-2 个关键对比字段。",
                ],
            }
        return {
            "claim": f"`{capability_id}` 本轮进入降级路径，已保留上下文并继续推进其他节点。",
            "evidence": "系统优先保证整轮响应时延与可用性，未中断会话。",
            "actions": [recovery_hint],
            "risks": ["本轮采用了降级兜底，建议下一轮补充更具体输入以恢复完整分析。"],
        }

    @staticmethod
    def _build_recovery_hint(*, reason_code: str, failed_capability: str) -> str:
        reason = reason_code.strip().upper()
        if reason == "CAP_TIMEOUT":
            return f"请缩小 `{failed_capability}` 的输入范围，例如只问一个学校或一个策略问题。"
        if reason == "STEP_BUDGET_EXCEEDED":
            return "请把需求拆成两条消息发送，先完成关键任务再继续。"
        if reason == "PROFILE_GATE_BLOCKED":
            return "请先发送确认命令（或重新编辑命令）再触发档案写入。"
        if reason == "LOCK_REJECTED":
            return "当前学生会话已有回合在运行，稍后重试同一条请求即可。"
        return f"请补充一个更具体的输入（目标学校/预算/成绩）以重试 `{failed_capability}`。"

    @staticmethod
    def _build_failure_message(
        *,
        reason_code: str,
        failed_capability: str,
        recovery_hint: str,
    ) -> str:
        return (
            f"本轮在 `{failed_capability}` 阶段失败（{reason_code}），已按事务策略回滚。\n"
            f"下一步建议：{recovery_hint}\n"
            "你已提供的上下文仍会保留到下一轮，可以直接继续。"
        )

    @staticmethod
    def _contains_contract_sections(text: str) -> bool:
        hits = 0
        for hints in _RESPONSE_CONTRACT_SECTION_HINTS.values():
            if any(marker in text for marker in hints):
                hits += 1
        return hits >= 2

    @staticmethod
    def _looks_generic_refusal(text: str) -> bool:
        cleaned = text.strip()
        if len(cleaned) > 320:
            return False
        if any(pattern.search(cleaned) for pattern in _GENERIC_REFUSAL_PATTERNS):
            return True
        return False

    @staticmethod
    def _extract_message_constraints(message: str) -> list[str]:
        constraints: list[str] = []
        gpa_match = re.search(r"\bgpa\s*[:=]?\s*(\d(?:\.\d+)?)", message, re.IGNORECASE)
        if gpa_match:
            constraints.append(f"GPA {gpa_match.group(1)}")
        sat_match = re.search(r"\bsat\s*[:=]?\s*(\d{3,4})", message, re.IGNORECASE)
        if sat_match:
            constraints.append(f"SAT {sat_match.group(1)}")
        budget_match = re.search(r"(budget|预算)\s*[:=]?\s*\$?(\d{4,6})", message, re.IGNORECASE)
        if budget_match:
            constraints.append(f"Budget {budget_match.group(2)}")
        if re.search(r"\b(cs|computer science|data science|专业)\b", message, re.IGNORECASE):
            constraints.append("Major preference mentioned")
        return constraints

    @classmethod
    def _collect_personalization_signals(
        cls,
        *,
        context: dict[str, Any],
        node_results: dict[str, tuple[CapabilityResult, int]],
        message: str,
    ) -> dict[str, Any]:
        fact_keys = [
            ("profile_gpa", "GPA"),
            ("profile_sat_total", "SAT"),
            ("profile_budget_usd", "Budget"),
            ("profile_target_year", "Target year"),
            ("profile_intended_majors", "Intended majors"),
            ("current_school_name", "Current school"),
        ]
        facts_used: list[str] = []
        for key, label in fact_keys:
            value = context.get(key)
            if value in (None, "", []):
                continue
            facts_used.append(f"{label}: {value}")

        constraints_used = cls._extract_message_constraints(message)
        missing_fields_raw = context.get("profile_missing_fields")
        missing_fields = (
            [str(item) for item in missing_fields_raw if str(item).strip()]
            if isinstance(missing_fields_raw, list)
            else []
        )

        for cap_result, _seq in node_results.values():
            evidence = (
                cap_result.meta.get("personalization_evidence")
                if isinstance(cap_result.meta, dict)
                else None
            )
            if not isinstance(evidence, dict):
                continue
            for item in evidence.get("facts_used", []):
                text = str(item).strip()
                if text and text not in facts_used:
                    facts_used.append(text)
            for item in evidence.get("constraints_used", []):
                text = str(item).strip()
                if text and text not in constraints_used:
                    constraints_used.append(text)
            for item in evidence.get("missing_fields", []):
                text = str(item).strip()
                if text and text not in missing_fields:
                    missing_fields.append(text)
        return {
            "facts_used": facts_used[:8],
            "constraints_used": constraints_used[:8],
            "missing_fields": missing_fields[:8],
        }

    async def _repair_generic_refusal_content(
        self,
        *,
        original_content: str,
        message: str,
        signals: dict[str, Any],
    ) -> dict[str, str] | None:
        schema = {
            "type": "object",
            "properties": {
                "conclusion": {"type": "string"},
                "evidence": {"type": "string"},
                "next_step": {"type": "string"},
                "missing_info": {"type": "string"},
            },
            "required": ["conclusion", "evidence", "next_step", "missing_info"],
        }
        try:
            result = await self._llm.complete_json(
                [
                    {
                        "role": "system",
                        "content": (
                            "You rewrite a generic refusal into an actionable advisor response. "
                            "Return strict JSON only. Do not expose chain-of-thought."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Original response:\n{original_content}\n\n"
                            f"User message:\n{message}\n\n"
                            f"Signals:\n{signals}\n\n"
                            "Write concise Chinese output sections."
                        ),
                    },
                ],
                schema=schema,
                temperature=0.1,
                max_tokens=220,
                caller="chat.output_repair",
            )
        except Exception:
            logger.warning("Generic-refusal repair call failed", exc_info=True)
            return None

        sections: dict[str, str] = {}
        for key in ("conclusion", "evidence", "next_step", "missing_info"):
            value = str(result.get(key, "")).strip()
            sections[key] = value
        if not sections["conclusion"]:
            return None
        return sections

    @staticmethod
    def _build_structured_content(*, sections: dict[str, str]) -> str:
        return (
            f"结论：{sections.get('conclusion', '已收到你的请求。')}\n"
            f"依据：{sections.get('evidence', '基于你当前提供的档案与约束。')}\n"
            f"下一步：{sections.get('next_step', '请确认后我继续执行下一轮。')}\n"
            f"缺失信息：{sections.get('missing_info', '暂无。')}"
        ).strip()

    async def _apply_user_visible_contract(
        self,
        *,
        result: TurnResult,
        message: str,
        context: dict[str, Any],
        node_results: dict[str, tuple[CapabilityResult, int]],
        planned: list[PlannedCapability],
    ) -> tuple[TurnResult, dict[str, Any]]:
        stats = {
            "response_contract_repaired": False,
            "generic_refusal_repaired": False,
        }
        if result.status != "ok":
            return result, stats

        synthesis_block = next((block for block in result.blocks if block.kind == "answer_synthesis"), None)
        if synthesis_block is not None:
            summary = ""
            if isinstance(synthesis_block.payload, dict):
                summary = str(synthesis_block.payload.get("summary", "")).strip()
            base = result.content.strip() or summary or "已完成本轮分析，并整理了可执行建议。"
            result.content = self._clip_text(base, 180)
            return result, stats

        original_content = result.content.strip()
        has_contract = self._contains_contract_sections(original_content)
        looks_refusal = self._looks_generic_refusal(original_content)
        if has_contract and not looks_refusal:
            return result, stats

        signals = self._collect_personalization_signals(
            context=context,
            node_results=node_results,
            message=message,
        )
        repaired_sections: dict[str, str] | None = None
        if looks_refusal:
            repaired_sections = await self._repair_generic_refusal_content(
                original_content=original_content,
                message=message,
                signals=signals,
            )
            if repaired_sections is not None:
                stats["generic_refusal_repaired"] = True

        if repaired_sections is None:
            capability_labels = [item.id for item in planned]
            missing_fields = signals.get("missing_fields", [])
            repaired_sections = {
                "conclusion": self._clip_text(original_content or "已完成本轮执行，并得到初步结论。", 180),
                "evidence": (
                    "；".join(signals.get("facts_used", [])[:3])
                    or "已结合你的历史上下文与本轮能力执行结果。"
                ),
                "next_step": (
                    "请回复一个更具体目标（学校/专业/预算）继续优化。"
                    if looks_refusal
                    else f"下一轮可继续细化：{', '.join(capability_labels[:3]) or 'general'}。"
                ),
                "missing_info": (
                    "、".join(missing_fields[:4]) if missing_fields else "暂无必须补充字段。"
                ),
            }

        result.content = self._build_structured_content(sections=repaired_sections)
        stats["response_contract_repaired"] = True
        return result, stats

    def _build_execution_digest(
        self,
        *,
        planned: list[PlannedCapability],
        node_results: dict[str, tuple[CapabilityResult, int]],
        wave_count: int,
        tool_steps_used: int,
        active_skill_id: str = "default",
        cap_retry_count: int = 0,
        cap_degraded: bool = False,
        failure_reason_code: str = "",
        required_output_missing: bool = False,
    ) -> dict[str, Any]:
        plan_order = {item.id: item.plan_order for item in planned}
        ordered = sorted(
            node_results.items(),
            key=lambda item: (item[1][1], plan_order.get(item[0], 10_000)),
        )
        steps: list[dict[str, Any]] = []
        needs_input: list[str] = []
        degraded_count = 0
        for capability_id, (cap_result, _seq) in ordered:
            summary = self._build_capability_event_summary(spec_id=capability_id, result=cap_result)
            for item in summary["needs_input"]:
                if item not in needs_input:
                    needs_input.append(item)
            cap_meta = cap_result.meta if isinstance(cap_result.meta, dict) else {}
            degraded = bool(cap_meta.get("degraded", False))
            if degraded:
                degraded_count += 1
            steps.append(
                {
                    "capability_id": capability_id,
                    "status": "degraded" if degraded else "completed",
                    "degraded_reason": str(cap_meta.get("degraded_reason", "")) if degraded else "",
                    "what_done": summary["what_done"],
                    "why_next": summary["why_next"],
                    "needs_input": summary["needs_input"],
                }
            )
        summary = f"本轮完成 {len(steps)} 个能力节点，执行 {wave_count} 个波次。"
        if degraded_count:
            summary += f" 其中 {degraded_count} 个节点降级执行。"
        return {
            "summary": summary,
            "what_done": f"本轮完成 {len(steps)} 个能力节点，覆盖 {wave_count} 个波次。",
            "why_next": "下一轮补齐缺失输入可显著降低不确定性。",
            "needs_input": needs_input[:6],
            "wave_count": wave_count,
            "tool_steps_used": tool_steps_used,
            "degraded_count": degraded_count,
            "active_skill_id": active_skill_id,
            "cap_retry_count": int(cap_retry_count),
            "cap_degraded": bool(cap_degraded),
            "failure_reason_code": str(failure_reason_code or ""),
            "required_output_missing": bool(required_output_missing),
            "steps": steps,
        }

    @staticmethod
    def _build_capability_event_summary(
        *,
        spec_id: str,
        result: CapabilityResult,
    ) -> dict[str, Any]:
        meta = result.meta if isinstance(result.meta, dict) else {}
        evidence = (
            meta.get("personalization_evidence")
            if isinstance(meta.get("personalization_evidence"), dict)
            else {}
        )
        missing = [
            str(item).strip()
            for item in evidence.get("missing_fields", [])
            if str(item).strip()
        ]
        what_done = str(meta.get("what_done", "")).strip()
        if not what_done:
            what_done = f"{spec_id} 已完成，并产出 {len(result.blocks)} 个结构化结果。"
        why_next = str(meta.get("why_next", "")).strip()
        if not why_next:
            why_next = "继续执行下游能力，或先补齐关键缺失字段。"
        return {
            "what_done": what_done,
            "why_next": why_next,
            "needs_input": missing[:2],
        }

    def _build_personalization_evidence(
        self,
        *,
        ctx: CapabilityContext,
        required_fields: list[str] | None = None,
        confidence: float = 0.72,
        extra_facts: list[str] | None = None,
    ) -> dict[str, Any]:
        context = ctx.conversation_context if isinstance(ctx.conversation_context, dict) else {}
        facts_used: list[str] = []
        field_map: list[tuple[str, str]] = [
            ("profile_gpa", "GPA"),
            ("profile_sat_total", "SAT"),
            ("profile_budget_usd", "Budget"),
            ("profile_target_year", "Target year"),
            ("profile_intended_majors", "Intended majors"),
            ("current_school_name", "Current school"),
        ]
        for key, label in field_map:
            value = context.get(key)
            if value in (None, "", []):
                continue
            facts_used.append(f"{label}: {value}")
        if extra_facts:
            for item in extra_facts:
                text = str(item).strip()
                if text and text not in facts_used:
                    facts_used.append(text)

        constraints_used = self._extract_message_constraints(ctx.message)
        missing_fields = [
            str(item).strip()
            for item in (context.get("profile_missing_fields") or [])
            if str(item).strip()
        ] if isinstance(context.get("profile_missing_fields"), list) else []
        if required_fields:
            required_set = {str(item).strip() for item in required_fields if str(item).strip()}
            provided_hints = set()
            for fact in facts_used:
                provided_hints.add(fact.lower())
            for constraint in constraints_used:
                provided_hints.add(constraint.lower())
            for field_name in required_set:
                if any(field_name.lower() in hint for hint in provided_hints):
                    continue
                if field_name not in missing_fields:
                    missing_fields.append(field_name)
        return {
            "facts_used": facts_used[:8],
            "constraints_used": constraints_used[:8],
            "missing_fields": missing_fields[:8],
            "confidence": round(max(0.0, min(1.0, confidence)), 2),
        }

    @staticmethod
    def _format_layered_memory(context: dict[str, Any]) -> str:
        layers = context.get("memory_layers")
        if not isinstance(layers, dict):
            return "{}"

        def _clip(value: Any) -> dict[str, Any]:
            if not isinstance(value, dict):
                return {}
            clipped: dict[str, Any] = {}
            for idx, (key, item) in enumerate(value.items()):
                if idx >= 8:
                    break
                clipped[str(key)] = item
            return clipped

        summary = {
            "short_term": _clip(layers.get("short_term")),
            "working": _clip(layers.get("working")),
            "long_term": _clip(layers.get("long_term")),
        }
        return str(summary)

    async def _execute_plan(
        self,
        *,
        planned: list[PlannedCapability],
        message: str,
        session_id: str,
        student_id: uuid.UUID | None,
        context: dict[str, Any],
        shared: dict[str, Any],
        trace_id: str,
        emit_event: Callable[[TurnEvent], Awaitable[None]],
    ) -> dict[str, Any]:
        shared.setdefault("trace_id", trace_id)
        shared.setdefault("turn_step_id", f"turn-{trace_id}")
        shared.setdefault("step_seq", 0)
        shared.setdefault("event_seq", 0)
        shared.setdefault("wave_step_ids", {})
        plan_map = {
            item.id: PlannedCapability(
                id=item.id,
                is_primary=item.is_primary,
                dependencies=list(item.dependencies),
                plan_order=item.plan_order,
            )
            for item in planned
        }
        pending = {item.id for item in planned}
        completed: set[str] = set()
        results: dict[str, tuple[CapabilityResult, int]] = {}
        start_seq = 0
        wave_count = 0

        while pending:
            wave_count += 1
            shared["wave_count"] = wave_count
            shared["current_wave"] = wave_count
            if wave_count > _MAX_WAVES_PER_TURN:
                shared["guardrail_triggered"] = True
                shared["failure_reason_code"] = "MAX_WAVE_EXCEEDED"
                raise CapabilityExecutionError(
                    f"Exceeded max wave count {_MAX_WAVES_PER_TURN} while pending={sorted(pending)}"
                )
            ready = [
                node_id
                for node_id in sorted(pending, key=lambda item: plan_map[item].plan_order)
                if set(plan_map[node_id].dependencies).issubset(completed)
            ]
            if not ready:
                changed = await self._checkpoint_replan(
                    message=message,
                    context=context,
                    shared=shared,
                    trace_id=trace_id,
                    wave_index=wave_count,
                    plan_map=plan_map,
                    pending=pending,
                    completed=completed,
                    results=results,
                    emit_event=emit_event,
                    just_completed=[],
                )
                if changed:
                    pending = {node_id for node_id in plan_map if node_id not in completed}
                    continue
                unresolved = ", ".join(sorted(pending))
                shared["failure_reason_code"] = "CAP_GRAPH_BLOCKED"
                raise CapabilityExecutionError(f"Capability graph is blocked: {unresolved}")

            wave_tasks: dict[asyncio.Task[CapabilityResult], tuple[str, int]] = {}
            for node_id in ready:
                if node_id == "profile_update":
                    gate = resolve_profile_update_gate(message=message, context=context)
                    context["_profile_update_gate"] = {
                        "action": gate.action,
                        "can_commit": gate.can_commit,
                        "reason": gate.reason,
                        "proposal_id": gate.proposal_id,
                    }
                    if gate.action == "commit" and not gate.can_commit:
                        shared["guardrail_triggered"] = True
                        shared["failure_reason_code"] = "PROFILE_GATE_BLOCKED"
                        for task in wave_tasks:
                            task.cancel()
                        if wave_tasks:
                            await asyncio.gather(*wave_tasks.keys(), return_exceptions=True)
                        raise CapabilityExecutionError(
                            f"profile_update pre-write checkpoint failed: {gate.reason}"
                        )
                if int(shared.get("tool_steps_used", 0)) + 1 > MAX_TOOL_STEPS_PER_TURN:
                    shared["guardrail_triggered"] = True
                    shared["failure_reason_code"] = "STEP_BUDGET_EXCEEDED"
                    for task in wave_tasks:
                        task.cancel()
                    if wave_tasks:
                        await asyncio.gather(*wave_tasks.keys(), return_exceptions=True)
                    raise CapabilityExecutionError(
                        f"tool step budget exceeded ({MAX_TOOL_STEPS_PER_TURN})"
                    )
                pending.remove(node_id)
                spec = self._registry[node_id]
                start_seq += 1
                shared["tool_steps_used"] = int(shared.get("tool_steps_used", 0)) + 1
                capability_step_id = self._next_step_id(shared)
                wave_step_id = self._get_wave_step_id(shared, wave_count)
                await emit_event(
                    TurnEvent(
                        trace_id=trace_id,
                        event="capability_started",
                        data=self._with_step_fields(
                            shared=shared,
                            data={"capability_id": spec.id},
                            step_kind="capability",
                            step_status="queued",
                            phase="execution",
                            wave_index=wave_count,
                            capability_id=spec.id,
                            parent_step_id=wave_step_id,
                            step_id=capability_step_id,
                            display={
                                "title": f"{spec.id}",
                                "badge": "queued",
                                "severity": "info",
                            },
                            metrics={
                                "tool_steps_used": int(shared.get("tool_steps_used", 0)),
                                "tool_step_budget": int(shared.get("tool_step_budget", MAX_TOOL_STEPS_PER_TURN)),
                            },
                        ),
                    )
                )
                task = asyncio.create_task(
                    self._run_one_capability(
                        spec=spec,
                        ctx=CapabilityContext(
                            llm=self._llm,
                            session=self._session,
                            memory=self._memory,
                            session_id=session_id,
                            student_id=student_id,
                            message=message,
                            conversation_context=context,
                            shared=shared,
                        ),
                        wave_index=wave_count,
                        step_id=capability_step_id,
                        parent_step_id=wave_step_id,
                        trace_id=trace_id,
                        emit_event=emit_event,
                    )
                )
                wave_tasks[task] = (node_id, start_seq)

            done, running = await asyncio.wait(
                wave_tasks.keys(),
                return_when=asyncio.FIRST_EXCEPTION,
            )
            failed: tuple[str, Exception] | None = None
            for task in done:
                node_id, _ = wave_tasks[task]
                try:
                    task.result()
                except Exception as exc:
                    failed = (node_id, exc)
                    break

            if failed is not None:
                for task in running:
                    task.cancel()
                if running:
                    await asyncio.gather(*running, return_exceptions=True)
                node_id, exc = failed
                just_completed: list[str] = []
                for task in done:
                    failed_node_id, sequence = wave_tasks[task]
                    if failed_node_id == node_id:
                        continue
                    cap_result = task.result()
                    completed.add(failed_node_id)
                    results[failed_node_id] = (cap_result, sequence)
                    just_completed.append(failed_node_id)

                recovered = await self._retry_or_degrade_required_capability(
                    capability_id=node_id,
                    failure_exc=exc,
                    message=message,
                    session_id=session_id,
                    student_id=student_id,
                    context=context,
                    shared=shared,
                    trace_id=trace_id,
                    emit_event=emit_event,
                    wave_index=wave_count,
                )
                if recovered is None:
                    shared["failure_reason_code"] = "CAP_FAILED"
                    raise CapabilityExecutionError(
                        f"Capability '{node_id}' failed: {exc}"
                    ) from exc
                completed.add(node_id)
                results[node_id] = (recovered, max(start_seq + 1, len(results) + 1))
                start_seq += 1
                just_completed.append(node_id)
                changed = await self._checkpoint_replan(
                    message=message,
                    context=context,
                    shared=shared,
                    trace_id=trace_id,
                    wave_index=wave_count,
                    plan_map=plan_map,
                    pending=pending,
                    completed=completed,
                    results=results,
                    emit_event=emit_event,
                    just_completed=just_completed,
                )
                if changed:
                    pending = {node_id for node_id in plan_map if node_id not in completed}
                continue

            if running:
                done_rest, _ = await asyncio.wait(running, return_when=asyncio.ALL_COMPLETED)
                done = done.union(done_rest)

            just_completed: list[str] = []
            for task in done:
                node_id, sequence = wave_tasks[task]
                cap_result = task.result()
                completed.add(node_id)
                results[node_id] = (cap_result, sequence)
                just_completed.append(node_id)

            changed = await self._checkpoint_replan(
                message=message,
                context=context,
                shared=shared,
                trace_id=trace_id,
                wave_index=wave_count,
                plan_map=plan_map,
                pending=pending,
                completed=completed,
                results=results,
                emit_event=emit_event,
                just_completed=just_completed,
            )
            if changed:
                pending = {node_id for node_id in plan_map if node_id not in completed}

        await self._enforce_required_outputs(
            message=message,
            session_id=session_id,
            student_id=student_id,
            context=context,
            shared=shared,
            trace_id=trace_id,
            emit_event=emit_event,
            wave_index=wave_count + 1,
            results=results,
        )

        return {
            "results": results,
            "plan_map": plan_map,
            "wave_count": wave_count,
            "tool_steps_used": int(shared.get("tool_steps_used", 0)),
        }

    @staticmethod
    def _is_required_capability(*, capability_id: str, shared: dict[str, Any]) -> bool:
        required = shared.get("required_capabilities")
        if not isinstance(required, list):
            return False
        return capability_id in {str(item).strip() for item in required if str(item).strip()}

    async def _retry_capability_same_task_once(
        self,
        *,
        capability_id: str,
        message: str,
        session_id: str,
        student_id: uuid.UUID | None,
        context: dict[str, Any],
        shared: dict[str, Any],
        trace_id: str,
        emit_event: Callable[[TurnEvent], Awaitable[None]],
        wave_index: int,
    ) -> CapabilityResult:
        retry_key = f"cap_retry:{capability_id}"
        retries_used = int(shared.get(retry_key, 0))
        if retries_used >= 1:
            raise CapabilityExecutionError(
                f"Required capability '{capability_id}' exceeded retry budget"
            )
        shared[retry_key] = retries_used + 1
        shared["cap_retry_count"] = int(shared.get("cap_retry_count", 0)) + 1
        spec = self._registry[capability_id]
        if int(shared.get("tool_steps_used", 0)) + 1 > MAX_TOOL_STEPS_PER_TURN:
            shared["guardrail_triggered"] = True
            shared["failure_reason_code"] = "STEP_BUDGET_EXCEEDED"
            raise CapabilityExecutionError(
                f"tool step budget exceeded ({MAX_TOOL_STEPS_PER_TURN})"
            )
        shared["tool_steps_used"] = int(shared.get("tool_steps_used", 0)) + 1
        capability_step_id = self._next_step_id(shared)
        wave_step_id = self._get_wave_step_id(shared, wave_index)
        await emit_event(
            TurnEvent(
                trace_id=trace_id,
                event="capability_started",
                data=self._with_step_fields(
                    shared=shared,
                    data={
                        "capability_id": capability_id,
                        "retry": True,
                        "retry_count": shared[retry_key],
                    },
                    step_kind="capability",
                    step_status="queued",
                    phase="execution",
                    wave_index=wave_index,
                    capability_id=capability_id,
                    parent_step_id=wave_step_id,
                    step_id=capability_step_id,
                    display={
                        "title": f"{capability_id}",
                        "badge": "retry",
                        "severity": "warning",
                    },
                    metrics={
                        "tool_steps_used": int(shared.get("tool_steps_used", 0)),
                        "tool_step_budget": int(shared.get("tool_step_budget", MAX_TOOL_STEPS_PER_TURN)),
                    },
                ),
            )
        )
        return await self._run_one_capability(
            spec=spec,
            ctx=CapabilityContext(
                llm=self._llm,
                session=self._session,
                memory=self._memory,
                session_id=session_id,
                student_id=student_id,
                message=message,
                conversation_context=context,
                shared=shared,
            ),
            wave_index=wave_index,
            step_id=capability_step_id,
            parent_step_id=wave_step_id,
            trace_id=trace_id,
            emit_event=emit_event,
        )

    def _build_required_capability_degraded_result(
        self,
        *,
        capability_id: str,
        reason_code: str,
        failure_message: str,
    ) -> CapabilityResult:
        recovery_hint = self._build_recovery_hint(
            reason_code=reason_code,
            failed_capability=capability_id,
        )
        degraded_message = self._sanitize_user_facing_text(
            f"{capability_id} 本轮执行受限，已降级为可执行建议。"
        )
        return CapabilityResult(
            content=degraded_message,
            blocks=[
                {
                    "kind": "text",
                    "payload": {
                        "text": degraded_message,
                        "next_steps": [
                            "先按当前预算/专业/地域约束收敛候选学校范围。",
                            "补充一条最关键约束后重试推荐，可显著提升稳定性。",
                        ],
                    },
                    "meta": {
                        "degraded": True,
                        "reason_code": reason_code,
                        "needs_input": [recovery_hint],
                    },
                }
            ],
            meta={
                "degraded": True,
                "degraded_reason": reason_code,
                "what_done": "Required capability degraded after one same-task retry.",
                "why_next": "Use fallback next steps and retry with one clarified constraint.",
                "needs_input": [recovery_hint],
                "failure_message": self._clip_text(failure_message, 180),
            },
        )

    async def _retry_or_degrade_required_capability(
        self,
        *,
        capability_id: str,
        failure_exc: Exception,
        message: str,
        session_id: str,
        student_id: uuid.UUID | None,
        context: dict[str, Any],
        shared: dict[str, Any],
        trace_id: str,
        emit_event: Callable[[TurnEvent], Awaitable[None]],
        wave_index: int,
    ) -> CapabilityResult | None:
        if not self._is_required_capability(capability_id=capability_id, shared=shared):
            return None
        try:
            retried = await self._retry_capability_same_task_once(
                capability_id=capability_id,
                message=message,
                session_id=session_id,
                student_id=student_id,
                context=context,
                shared=shared,
                trace_id=trace_id,
                emit_event=emit_event,
                wave_index=wave_index + 1,
            )
            return retried
        except Exception as retry_exc:
            logger.warning(
                "Required capability retry failed, degrading capability result cap=%s",
                capability_id,
                exc_info=True,
            )
            shared["cap_degraded"] = True
            shared["failure_reason_code"] = "CAP_FAILED"
            degraded = self._build_required_capability_degraded_result(
                capability_id=capability_id,
                reason_code="CAP_FAILED",
                failure_message=str(retry_exc or failure_exc),
            )
            # Preserve meta contract consistency for synthesis/execution digest.
            return self._ensure_capability_meta_contract(
                spec_id=capability_id,
                ctx=CapabilityContext(
                    llm=self._llm,
                    session=self._session,
                    memory=self._memory,
                    session_id=session_id,
                    student_id=student_id,
                    message=message,
                    conversation_context=context,
                    shared=shared,
                ),
                result=degraded,
            )

    @staticmethod
    def _has_recommendation_payload_in_results(
        *,
        results: dict[str, tuple[CapabilityResult, int]],
    ) -> bool:
        for cap_result, _sequence in results.values():
            for block in cap_result.blocks or []:
                if str(block.get("kind") or "") != "recommendation":
                    continue
                payload = block.get("payload")
                if isinstance(payload, dict):
                    return True
        return False

    def _missing_required_outputs(
        self,
        *,
        shared: dict[str, Any],
        results: dict[str, tuple[CapabilityResult, int]],
    ) -> list[str]:
        required_outputs = shared.get("required_outputs")
        if not isinstance(required_outputs, list):
            return []
        missing: list[str] = []
        required_set = {str(item).strip() for item in required_outputs if str(item).strip()}
        if "recommendation_payload" in required_set and not self._has_recommendation_payload_in_results(results=results):
            missing.append("recommendation_payload")
        return missing

    async def _enforce_required_outputs(
        self,
        *,
        message: str,
        session_id: str,
        student_id: uuid.UUID | None,
        context: dict[str, Any],
        shared: dict[str, Any],
        trace_id: str,
        emit_event: Callable[[TurnEvent], Awaitable[None]],
        wave_index: int,
        results: dict[str, tuple[CapabilityResult, int]],
    ) -> None:
        missing = self._missing_required_outputs(shared=shared, results=results)
        if not missing:
            return
        shared["required_output_missing"] = True
        shared["failure_reason_code"] = "REQUIRED_OUTPUT_MISSING"
        if (
            "recommendation_payload" in missing
            and bool(shared.get("recommendation_blocked_by_intake", False))
        ):
            shared["failure_reason_code"] = "RECOMMENDATION_BLOCKED_BY_INTAKE"
            return

        if "recommendation_payload" in missing and "recommendation_subagent" in self._registry:
            try:
                retried = await self._retry_capability_same_task_once(
                    capability_id="recommendation_subagent",
                    message=message,
                    session_id=session_id,
                    student_id=student_id,
                    context=context,
                    shared=shared,
                    trace_id=trace_id,
                    emit_event=emit_event,
                    wave_index=wave_index,
                )
                results["recommendation_subagent"] = (
                    retried,
                    max(sequence for _result, sequence in results.values()) + 1 if results else 1,
                )
            except Exception:
                logger.warning("Required output retry failed for recommendation_payload", exc_info=True)

        missing_after_retry = self._missing_required_outputs(shared=shared, results=results)
        if not missing_after_retry:
            return

        shared["cap_degraded"] = True
        shared["failure_reason_code"] = "REQUIRED_OUTPUT_MISSING"
        degraded = self._build_required_capability_degraded_result(
            capability_id="recommendation_subagent",
            reason_code="REQUIRED_OUTPUT_MISSING",
            failure_message="recommendation_payload missing after one same-task retry",
        )
        results["recommendation_subagent"] = (
            self._ensure_capability_meta_contract(
                spec_id="recommendation_subagent",
                ctx=CapabilityContext(
                    llm=self._llm,
                    session=self._session,
                    memory=self._memory,
                    session_id=session_id,
                    student_id=student_id,
                    message=message,
                    conversation_context=context,
                    shared=shared,
                ),
                result=degraded,
            ),
            max(sequence for _result, sequence in results.values()) + 1 if results else 1,
        )

    async def _checkpoint_replan(
        self,
        *,
        message: str,
        context: dict[str, Any],
        shared: dict[str, Any],
        trace_id: str,
        wave_index: int,
        plan_map: dict[str, PlannedCapability],
        pending: set[str],
        completed: set[str],
        results: dict[str, tuple[CapabilityResult, int]],
        emit_event: Callable[[TurnEvent], Awaitable[None]],
        just_completed: list[str],
    ) -> bool:
        changed = False
        add: list[str] = []
        drop: list[str] = []
        checkpoint_kind = "wave_replan"
        checkpoint_status = "noop"

        self._apply_checkpoint_compaction(context=context, shared=shared)
        context_management = context.get("context_management")
        if isinstance(context_management, dict):
            shared["compression_level"] = str(context_management.get("compression_level", "default"))
            shared["summary_node_count"] = int(context_management.get("summary_node_count", 0))
            shared["profile_fact_count"] = int(context_management.get("profile_fact_count", 0))

        profile_gate = resolve_profile_update_gate(message=message, context=context)
        context["_profile_update_gate"] = {
            "action": profile_gate.action,
            "can_commit": profile_gate.can_commit,
            "reason": profile_gate.reason,
            "proposal_id": profile_gate.proposal_id,
        }
        if "profile_update" in pending and profile_gate.action == "commit" and not profile_gate.can_commit:
            shared["failure_reason_code"] = "PROFILE_GATE_BLOCKED"
            raise CapabilityExecutionError(
                f"profile_update pre-write checkpoint failed: {profile_gate.reason}"
            )
        shared["recommendation_blocked_by_intake"] = False

        # Guided intake soft gate:
        # recommendation_subagent is blocked only when intake is incomplete
        # and hard-required intake fields are all missing.
        guided_entry = results.get("guided_intake")
        if guided_entry is not None:
            guided_meta = guided_entry[0].meta or {}
            intake_complete = bool(guided_meta.get("intake_complete"))
            intake_hard_missing = self._has_hard_intake_missing_fields(context=context)
            if (
                not intake_complete
                and intake_hard_missing
                and "recommendation_subagent" in pending
            ):
                shared["recommendation_blocked_by_intake"] = True
                drop.append("recommendation_subagent")
            if intake_complete:
                if (
                    "recommendation_subagent" not in completed
                    and "recommendation_subagent" not in plan_map
                ):
                    add.append("recommendation_subagent")

        changed = self._ensure_profile_read_dependencies(
            plan_map=plan_map,
            pending=pending,
            completed=completed,
            force_refresh=False,
        ) or changed

        if "profile_update" in just_completed:
            profile_update_entry = results.get("profile_update")
            profile_update_meta = (profile_update_entry[0].meta or {}) if profile_update_entry else {}
            if bool(profile_update_meta.get("applied")):
                changed = self._ensure_profile_read_dependencies(
                    plan_map=plan_map,
                    pending=pending,
                    completed=completed,
                    force_refresh=True,
                ) or changed

        if add or drop:
            changed = self._apply_plan_delta(
                plan_map=plan_map,
                completed=completed,
                add=add,
                drop=drop,
                reprioritize=[],
            ) or changed
            if changed:
                pending.clear()
                pending.update(node_id for node_id in plan_map if node_id not in completed)

        try:
            llm_delta = await asyncio.wait_for(
                self._checkpoint_with_llm(
                    message=message,
                    context=context,
                    plan_map=plan_map,
                    pending=pending,
                    completed=completed,
                    results=results,
                    just_completed=just_completed,
                    wave_index=wave_index,
                ),
                timeout=CHECKPOINT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Wave checkpoint timed out after %.1fs; using no-op delta",
                CHECKPOINT_TIMEOUT_SECONDS,
            )
            shared["failure_reason_code"] = "CHECKPOINT_TIMEOUT"
            llm_delta = {"add": [], "drop": [], "reprioritize": []}
        if llm_delta["add"] or llm_delta["drop"] or llm_delta["reprioritize"]:
            llm_changed = self._apply_plan_delta(
                plan_map=plan_map,
                completed=completed,
                add=llm_delta["add"],
                drop=llm_delta["drop"],
                reprioritize=llm_delta["reprioritize"],
            )
            changed = llm_changed or changed
            if llm_changed:
                pending.clear()
                pending.update(node_id for node_id in plan_map if node_id not in completed)
                checkpoint_status = "changed"

        changed = self._ensure_profile_read_dependencies(
            plan_map=plan_map,
            pending=pending,
            completed=completed,
            force_refresh=False,
        ) or changed

        if changed and checkpoint_status == "noop":
            checkpoint_status = "changed"

        checkpoint_summary = {
            "changed": bool(changed),
            "added_count": len(add) + len(llm_delta.get("add", [])),
            "dropped_count": len(drop) + len(llm_delta.get("drop", [])),
            "reprioritized_count": len(llm_delta.get("reprioritize", [])),
            "profile_gate_status": (
                "blocked"
                if profile_gate.action == "commit" and not profile_gate.can_commit
                else ("can_commit" if profile_gate.action == "commit" else profile_gate.action)
            ),
        }
        await emit_event(
            TurnEvent(
                trace_id=trace_id,
                event="planning_done",
                data=self._with_step_fields(
                    shared=shared,
                    data={
                        "checkpoint": True,
                        "wave": wave_index,
                        "checkpoint_kind": checkpoint_kind,
                        "checkpoint_status": checkpoint_status,
                        "context_chars_before": int(
                            (context.get("context_management") or {}).get("context_chars_before", 0)
                        ),
                        "context_chars_after": int(
                            (context.get("context_management") or {}).get("context_chars_after", 0)
                        ),
                        "compression_level": str(
                            (context.get("context_management") or {}).get("compression_level", "default")
                        ),
                        "summary_node_count": int(
                            (context.get("context_management") or {}).get("summary_node_count", 0)
                        ),
                        "profile_fact_count": int(
                            (context.get("context_management") or {}).get("profile_fact_count", 0)
                        ),
                        "active_skill_id": str(shared.get("active_skill_id", "default")),
                        "skill_route_source": str(shared.get("skill_route_source", "default")),
                        "skill_contract_version": str(shared.get("skill_contract_version", _SKILL_CONTRACT_VERSION)),
                        "what_done": (
                            "Checkpoint reviewed completed nodes and refreshed scheduling priorities."
                        ),
                        "why_next": (
                            "Proceed to next wave with updated ready set."
                            if changed
                            else "No schedule delta; continue current execution order."
                        ),
                        "needs_input": [],
                        "capabilities": [
                            {
                                "id": item.id,
                                "is_primary": item.is_primary,
                                "dependencies": item.dependencies,
                                "order": item.plan_order,
                            }
                            for item in self._materialize_plan_list(plan_map)
                        ],
                    },
                    step_kind="checkpoint",
                    step_status="completed",
                    phase="checkpoint",
                    wave_index=wave_index,
                    parent_step_id=self._get_wave_step_id(shared, wave_index),
                    checkpoint_summary=checkpoint_summary,
                    display={
                        "title": "Checkpoint",
                        "badge": checkpoint_status,
                        "severity": "info",
                    },
                    metrics={
                        "pending_count": len(pending),
                        "completed_count": len(completed),
                    },
                ),
            )
        )
        return changed

    async def _checkpoint_with_llm(
        self,
        *,
        message: str,
        context: dict[str, Any],
        plan_map: dict[str, PlannedCapability],
        pending: set[str],
        completed: set[str],
        results: dict[str, tuple[CapabilityResult, int]],
        just_completed: list[str],
        wave_index: int,
    ) -> dict[str, list[str]]:
        if len(pending) < 2:
            return {"add": [], "drop": [], "reprioritize": []}

        ordered_pending = [item.id for item in self._materialize_plan_list(plan_map) if item.id in pending]
        completed_summary = [
            {
                "id": cap_id,
                "meta": cap_result.meta,
                "block_kinds": [str(block.get("kind")) for block in cap_result.blocks],
            }
            for cap_id, (cap_result, _sequence) in sorted(results.items(), key=lambda item: item[1][1])
        ]
        prompt = (
            "You are a scheduling checkpoint for an advisor orchestrator.\n"
            "Return JSON only. Decide optional plan deltas for the next wave.\n"
            "Allowed capability ids:\n"
            + "\n".join(f"- {cap_id}" for cap_id in sorted(self._registry))
            + "\nOutput schema:\n"
            "{\"add\": [\"cap_id\"], \"drop\": [\"cap_id\"], \"reprioritize\": [\"cap_id\"], \"reason\": \"...\"}\n"
            "Rules:\n"
            "- Keep it conservative. Prefer no-op unless there is a clear gain.\n"
            "- Never drop completed capabilities.\n"
            "- reprioritize only reorders pending capabilities.\n"
            "- recommendation_subagent can run before intake_complete when hard required fields are not all missing.\n"
        )
        messages = [
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": (
                    f"Wave index: {wave_index}\n"
                    f"User message: {context.get('compressed_user_message', message)}\n"
                    f"Recent context:\n{context.get('recent_messages', '')}\n\n"
                    f"Layered memory:\n{self._format_layered_memory(context)}\n\n"
                    f"Pending: {ordered_pending}\n"
                    f"Just completed: {just_completed}\n"
                    f"Completed summary: {completed_summary}\n"
                ),
            },
        ]
        try:
            result = await self._llm.complete_json(
                messages,
                temperature=0.0,
                max_tokens=256,
                caller="chat.wave_checkpoint",
            )
        except Exception:
            logger.warning("Wave checkpoint LLM call failed; using no-op", exc_info=True)
            return {"add": [], "drop": [], "reprioritize": []}

        def _normalize_list(value: Any) -> list[str]:
            if not isinstance(value, list):
                return []
            out: list[str] = []
            for item in value:
                capability_id = str(item).strip()
                if not capability_id or capability_id not in self._registry:
                    continue
                if capability_id in out:
                    continue
                out.append(capability_id)
            return out

        add = [
            cap_id
            for cap_id in _normalize_list(result.get("add"))
            if cap_id not in completed and cap_id not in plan_map
        ]
        drop = [
            cap_id
            for cap_id in _normalize_list(result.get("drop"))
            if cap_id in pending
        ]
        reprioritize = [
            cap_id
            for cap_id in _normalize_list(result.get("reprioritize"))
            if cap_id in pending
        ]
        return {
            "add": add,
            "drop": drop,
            "reprioritize": reprioritize,
        }

    def _apply_plan_delta(
        self,
        *,
        plan_map: dict[str, PlannedCapability],
        completed: set[str],
        add: list[str],
        drop: list[str],
        reprioritize: list[str],
    ) -> bool:
        changed = False
        drop_set = {cap_id for cap_id in drop if cap_id in plan_map and cap_id not in completed}
        if drop_set:
            # Protect dependency integrity for still-pending nodes.
            protected_drop: set[str] = set()
            for candidate in drop_set:
                for node_id, node in plan_map.items():
                    if node_id in completed or node_id in drop_set:
                        continue
                    if candidate in node.dependencies:
                        protected_drop.add(candidate)
                        break
            drop_set -= protected_drop
            if drop_set:
                for cap_id in drop_set:
                    plan_map.pop(cap_id, None)
                changed = True

        if add:
            for cap_id in add:
                changed = self._ensure_capability_with_dependencies(
                    plan_map=plan_map,
                    cap_id=cap_id,
                ) or changed

        if reprioritize:
            pending_ids = [
                item.id
                for item in self._materialize_plan_list(plan_map)
                if item.id not in completed
            ]
            if pending_ids:
                prefix: list[str] = []
                seen: set[str] = set()
                for cap_id in reprioritize:
                    if cap_id in pending_ids and cap_id not in seen:
                        prefix.append(cap_id)
                        seen.add(cap_id)
                if prefix:
                    ordered_pending = prefix + [cap_id for cap_id in pending_ids if cap_id not in seen]
                    changed = self._reassign_pending_order(
                        plan_map=plan_map,
                        completed=completed,
                        ordered_pending=ordered_pending,
                    ) or changed

        if changed:
            self._normalize_primary(plan_map)
        return changed

    def _ensure_profile_read_dependencies(
        self,
        *,
        plan_map: dict[str, PlannedCapability],
        pending: set[str],
        completed: set[str],
        force_refresh: bool,
    ) -> bool:
        pending_dependents = [
            cap_id
            for cap_id in pending
            if cap_id in _PROFILE_DEPENDENT_CAPABILITIES and cap_id in plan_map
        ]
        if not pending_dependents:
            return False

        changed = self._ensure_capability_with_dependencies(
            plan_map=plan_map,
            cap_id="profile_read",
        )
        profile_node = plan_map.get("profile_read")
        if profile_node is None:
            return changed

        if force_refresh and "profile_read" in completed:
            completed.remove("profile_read")
            pending.add("profile_read")
            changed = True

        if "profile_read" not in completed:
            pending.add("profile_read")

        for cap_id in pending_dependents:
            deps = plan_map[cap_id].dependencies
            if "profile_read" not in deps:
                deps.append("profile_read")
                changed = True

        return changed

    def _ensure_capability_with_dependencies(
        self,
        *,
        plan_map: dict[str, PlannedCapability],
        cap_id: str,
    ) -> bool:
        if cap_id in plan_map:
            return False
        spec = self._registry.get(cap_id)
        if spec is None:
            return False
        changed = False
        for dep_id in spec.dependencies:
            if dep_id in self._registry:
                changed = self._ensure_capability_with_dependencies(
                    plan_map=plan_map,
                    cap_id=dep_id,
                ) or changed
        next_order = (
            max((item.plan_order for item in plan_map.values()), default=-1) + 1
        )
        plan_map[cap_id] = PlannedCapability(
            id=cap_id,
            is_primary=False,
            dependencies=list(spec.dependencies),
            plan_order=next_order,
        )
        return True

    def _reassign_pending_order(
        self,
        *,
        plan_map: dict[str, PlannedCapability],
        completed: set[str],
        ordered_pending: list[str],
    ) -> bool:
        base = max(
            (plan_map[node_id].plan_order for node_id in completed if node_id in plan_map),
            default=-1,
        )
        changed = False
        for index, cap_id in enumerate(ordered_pending):
            if cap_id not in plan_map:
                continue
            new_order = base + index + 1
            if plan_map[cap_id].plan_order != new_order:
                plan_map[cap_id].plan_order = new_order
                changed = True
        return changed

    def _normalize_primary(self, plan_map: dict[str, PlannedCapability]) -> None:
        ordered = self._materialize_plan_list(plan_map)
        for item in ordered:
            item.is_primary = False
        if ordered:
            ordered[0].is_primary = True
        for item in ordered:
            plan_map[item.id] = item

    @staticmethod
    def _materialize_plan_list(plan_map: dict[str, PlannedCapability]) -> list[PlannedCapability]:
        return sorted(plan_map.values(), key=lambda item: item.plan_order)

    async def _run_one_capability(
        self,
        *,
        spec: CapabilitySpec,
        ctx: CapabilityContext,
        wave_index: int,
        step_id: str,
        parent_step_id: str | None,
        trace_id: str,
        emit_event: Callable[[TurnEvent], Awaitable[None]],
    ) -> CapabilityResult:
        started = datetime.now(UTC)
        prompt_chars = len(ctx.message) + len(str(ctx.conversation_context.get("recent_messages", "")))
        max_tokens = int(ctx.shared.get(f"{spec.id}:max_tokens_hint", 0) or 0)
        db_session_serialized = bool(spec.requires_db_session)
        timeout_seconds = self._capability_timeout_seconds(spec)
        await emit_event(
            TurnEvent(
                trace_id=trace_id,
                event="capability_started",
                data=self._with_step_fields(
                    shared=ctx.shared,
                    data={
                        "capability_id": spec.id,
                        "started_at": started.isoformat(),
                        "active_skill_id": str(ctx.shared.get("active_skill_id", "default")),
                    },
                    step_kind="capability",
                    step_status="running",
                    phase="execution",
                    wave_index=wave_index,
                    capability_id=spec.id,
                    parent_step_id=parent_step_id,
                    step_id=step_id,
                    display={
                        "title": f"{spec.id}",
                        "badge": "running",
                        "severity": "info",
                    },
                    metrics={
                        "tool_steps_used": int(ctx.shared.get("tool_steps_used", 0)),
                        "tool_step_budget": int(ctx.shared.get("tool_step_budget", MAX_TOOL_STEPS_PER_TURN)),
                        "prompt_chars": prompt_chars,
                        "max_tokens": max_tokens,
                        "db_session_serialized": db_session_serialized,
                        "timeout_seconds": timeout_seconds,
                    },
                ),
            )
        )

        async def _execute_capability_call() -> CapabilityResult:
            if spec.id == "recommendation_subagent":
                async with self._subagent_semaphore:
                    return await asyncio.wait_for(
                        spec.execute(ctx),
                        timeout=timeout_seconds,
                    )
            return await asyncio.wait_for(
                spec.execute(ctx),
                timeout=timeout_seconds,
            )

        try:
            async with self._capability_semaphore:
                if spec.requires_db_session:
                    async with self._db_session_semaphore:
                        result = await _execute_capability_call()
                else:
                    result = await _execute_capability_call()
        except asyncio.TimeoutError as exc:
            best_effort = self._is_best_effort(spec)
            if not best_effort:
                ctx.shared["guardrail_triggered"] = True
                ctx.shared["failure_reason_code"] = "CAP_TIMEOUT"
            finished = datetime.now(UTC)
            duration_ms = int((finished - started).total_seconds() * 1000)
            why_next = (
                "Capability is best-effort; orchestrator will continue with a degraded result."
                if best_effort
                else "Need a narrower request or lower payload size before retry."
            )
            what_done = (
                f"{spec.id} timed out and was degraded to preserve turn responsiveness."
                if best_effort
                else f"{spec.id} timed out before producing a valid result."
            )
            await emit_event(
                TurnEvent(
                    trace_id=trace_id,
                    event="capability_finished",
                    data=self._with_step_fields(
                        shared=ctx.shared,
                        data={
                            "capability_id": spec.id,
                            "duration_ms": duration_ms,
                            "prompt_chars": prompt_chars,
                            "max_tokens": max_tokens,
                            "db_session_serialized": db_session_serialized,
                            "timeout_seconds": timeout_seconds,
                            "active_skill_id": str(ctx.shared.get("active_skill_id", "default")),
                            "what_done": what_done,
                            "why_next": why_next,
                            "needs_input": [self._build_recovery_hint(reason_code="CAP_TIMEOUT", failed_capability=spec.id)],
                        },
                        step_kind="capability",
                        step_status="timeout",
                        phase="execution",
                        wave_index=wave_index,
                        capability_id=spec.id,
                        duration_ms=duration_ms,
                        parent_step_id=parent_step_id,
                        step_id=step_id,
                        compact_reason_code="CAP_TIMEOUT",
                        display={
                            "title": f"{spec.id}",
                            "badge": "timeout",
                            "severity": "warning",
                        },
                        metrics={
                            "duration_ms": duration_ms,
                            "prompt_chars": prompt_chars,
                            "max_tokens": max_tokens,
                            "db_session_serialized": db_session_serialized,
                            "timeout_seconds": timeout_seconds,
                        },
                    ),
                )
            )
            logger.warning(
                "Capability timed out trace=%s cap=%s wave=%s duration_ms=%s prompt_chars=%s max_tokens=%s db_session_serialized=%s",
                trace_id,
                spec.id,
                wave_index,
                duration_ms,
                prompt_chars,
                max_tokens,
                db_session_serialized,
            )
            if best_effort:
                self._record_best_effort_degraded(ctx.shared, spec.id)
                fallback = self._build_best_effort_fallback_result(
                    spec=spec,
                    ctx=ctx,
                    reason_code="CAP_TIMEOUT",
                )
                return self._ensure_capability_meta_contract(
                    spec_id=spec.id,
                    ctx=ctx,
                    result=fallback,
                )
            raise CapabilityExecutionError(
                f"Capability '{spec.id}' timed out after {timeout_seconds}s"
            ) from exc
        except Exception as exc:
            best_effort = self._is_best_effort(spec)
            finished = datetime.now(UTC)
            duration_ms = int((finished - started).total_seconds() * 1000)
            why_next = (
                "Capability is best-effort; orchestrator will continue with a degraded result."
                if best_effort
                else "Need corrected constraints or fewer simultaneous asks before retry."
            )
            what_done = (
                f"{spec.id} failed and was degraded to keep the turn progressing."
                if best_effort
                else f"{spec.id} failed before completion."
            )
            await emit_event(
                TurnEvent(
                    trace_id=trace_id,
                    event="capability_finished",
                    data=self._with_step_fields(
                        shared=ctx.shared,
                        data={
                            "capability_id": spec.id,
                            "duration_ms": duration_ms,
                            "prompt_chars": prompt_chars,
                            "max_tokens": max_tokens,
                            "db_session_serialized": db_session_serialized,
                            "timeout_seconds": timeout_seconds,
                            "active_skill_id": str(ctx.shared.get("active_skill_id", "default")),
                            "what_done": what_done,
                            "why_next": why_next,
                            "needs_input": [self._build_recovery_hint(reason_code="CAP_FAILED", failed_capability=spec.id)],
                        },
                        step_kind="capability",
                        step_status="failed",
                        phase="execution",
                        wave_index=wave_index,
                        capability_id=spec.id,
                        duration_ms=duration_ms,
                        parent_step_id=parent_step_id,
                        step_id=step_id,
                        compact_reason_code="CAP_FAILED",
                        display={
                            "title": f"{spec.id}",
                            "badge": "failed",
                            "severity": "error",
                        },
                        metrics={
                            "duration_ms": duration_ms,
                            "prompt_chars": prompt_chars,
                            "max_tokens": max_tokens,
                            "db_session_serialized": db_session_serialized,
                            "timeout_seconds": timeout_seconds,
                        },
                    ),
                )
            )
            logger.warning(
                "Capability failed trace=%s cap=%s wave=%s duration_ms=%s prompt_chars=%s max_tokens=%s db_session_serialized=%s",
                trace_id,
                spec.id,
                wave_index,
                duration_ms,
                prompt_chars,
                max_tokens,
                db_session_serialized,
                exc_info=True,
            )
            if best_effort:
                self._record_best_effort_degraded(ctx.shared, spec.id)
                fallback = self._build_best_effort_fallback_result(
                    spec=spec,
                    ctx=ctx,
                    reason_code="CAP_FAILED",
                )
                return self._ensure_capability_meta_contract(
                    spec_id=spec.id,
                    ctx=ctx,
                    result=fallback,
                )
            raise
        result = self._ensure_capability_meta_contract(
            spec_id=spec.id,
            ctx=ctx,
            result=result,
        )
        try:
            self._validate_capability_output(spec=spec, result=result)
        except Exception:
            best_effort = self._is_best_effort(spec)
            finished = datetime.now(UTC)
            duration_ms = int((finished - started).total_seconds() * 1000)
            why_next = (
                "Capability output was downgraded; continue with other successful capability outputs."
                if best_effort
                else "Capability output contract must be fixed before retry."
            )
            await emit_event(
                TurnEvent(
                    trace_id=trace_id,
                    event="capability_finished",
                    data=self._with_step_fields(
                        shared=ctx.shared,
                        data={
                            "capability_id": spec.id,
                            "duration_ms": duration_ms,
                            "prompt_chars": prompt_chars,
                            "max_tokens": max_tokens,
                            "db_session_serialized": db_session_serialized,
                            "timeout_seconds": timeout_seconds,
                            "active_skill_id": str(ctx.shared.get("active_skill_id", "default")),
                            "what_done": f"{spec.id} returned payload but failed schema validation.",
                            "why_next": why_next,
                            "needs_input": [self._build_recovery_hint(reason_code="CAP_SCHEMA_INVALID", failed_capability=spec.id)],
                        },
                        step_kind="capability",
                        step_status="failed",
                        phase="execution",
                        wave_index=wave_index,
                        capability_id=spec.id,
                        duration_ms=duration_ms,
                        parent_step_id=parent_step_id,
                        step_id=step_id,
                        compact_reason_code="CAP_SCHEMA_INVALID",
                        display={
                            "title": f"{spec.id}",
                            "badge": "failed",
                            "severity": "error",
                        },
                        metrics={
                            "duration_ms": duration_ms,
                            "prompt_chars": prompt_chars,
                            "max_tokens": max_tokens,
                            "db_session_serialized": db_session_serialized,
                            "timeout_seconds": timeout_seconds,
                        },
                    ),
                )
            )
            if best_effort:
                self._record_best_effort_degraded(ctx.shared, spec.id)
                fallback = self._build_best_effort_fallback_result(
                    spec=spec,
                    ctx=ctx,
                    reason_code="CAP_SCHEMA_INVALID",
                )
                return self._ensure_capability_meta_contract(
                    spec_id=spec.id,
                    ctx=ctx,
                    result=fallback,
                )
            raise
        finished = datetime.now(UTC)
        duration_ms = int((finished - started).total_seconds() * 1000)
        event_summary = self._build_capability_event_summary(spec_id=spec.id, result=result)
        await emit_event(
            TurnEvent(
                trace_id=trace_id,
                event="capability_finished",
                data=self._with_step_fields(
                    shared=ctx.shared,
                    data={
                        "capability_id": spec.id,
                        "duration_ms": duration_ms,
                        "block_count": len(result.blocks),
                        "prompt_chars": prompt_chars,
                        "max_tokens": max_tokens,
                        "db_session_serialized": db_session_serialized,
                            "timeout_seconds": timeout_seconds,
                            "active_skill_id": str(ctx.shared.get("active_skill_id", "default")),
                            "what_done": event_summary["what_done"],
                            "why_next": event_summary["why_next"],
                        "needs_input": event_summary["needs_input"],
                    },
                    step_kind="capability",
                    step_status="completed",
                    phase="execution",
                    wave_index=wave_index,
                    capability_id=spec.id,
                    duration_ms=duration_ms,
                    parent_step_id=parent_step_id,
                    step_id=step_id,
                    display={
                        "title": f"{spec.id}",
                        "badge": "completed",
                        "severity": "success",
                    },
                    metrics={
                        "block_count": len(result.blocks),
                        "tool_steps_used": int(ctx.shared.get("tool_steps_used", 0)),
                        "duration_ms": duration_ms,
                        "prompt_chars": prompt_chars,
                        "max_tokens": max_tokens,
                        "db_session_serialized": db_session_serialized,
                        "timeout_seconds": timeout_seconds,
                    },
                ),
            )
        )
        logger.info(
            "Capability finished trace=%s cap=%s wave=%s duration_ms=%s prompt_chars=%s max_tokens=%s db_session_serialized=%s",
            trace_id,
            spec.id,
            wave_index,
            duration_ms,
            prompt_chars,
            max_tokens,
            db_session_serialized,
        )
        return result

    def _ensure_capability_meta_contract(
        self,
        *,
        spec_id: str,
        ctx: CapabilityContext,
        result: CapabilityResult,
    ) -> CapabilityResult:
        meta = dict(result.meta) if isinstance(result.meta, dict) else {}
        evidence = (
            dict(meta.get("personalization_evidence"))
            if isinstance(meta.get("personalization_evidence"), dict)
            else self._build_personalization_evidence(ctx=ctx, confidence=0.72)
        )
        facts_used = [
            str(item).strip()
            for item in evidence.get("facts_used", [])
            if str(item).strip()
        ] if isinstance(evidence.get("facts_used"), list) else []
        constraints_used = [
            str(item).strip()
            for item in evidence.get("constraints_used", [])
            if str(item).strip()
        ] if isinstance(evidence.get("constraints_used"), list) else []
        missing_fields = [
            str(item).strip()
            for item in evidence.get("missing_fields", [])
            if str(item).strip()
        ] if isinstance(evidence.get("missing_fields"), list) else []

        action_hints = [
            str(item).strip()
            for item in (meta.get("action_hints") or [])
            if str(item).strip()
        ] if isinstance(meta.get("action_hints"), list) else []
        if not action_hints:
            action_hints = self._default_actions_for_capability(spec_id)

        risks_missing = [
            str(item).strip()
            for item in (meta.get("risks_missing") or [])
            if str(item).strip()
        ] if isinstance(meta.get("risks_missing"), list) else []

        active_skill_id = str(ctx.shared.get("active_skill_id") or "default")
        route_modifiers = {
            str(item).strip()
            for item in (ctx.shared.get("skill_route_modifiers") or [])
            if str(item).strip()
        } if isinstance(ctx.shared.get("skill_route_modifiers"), list) else set()
        if active_skill_id == "guided_intake" and spec_id == "guided_intake":
            questions = self._build_guided_intake_questions(
                context=ctx.conversation_context,
                evidence_units=[],
            )
            if questions:
                action_hints = questions[:1]
        elif (
            active_skill_id == "memory_followup"
            or "memory_followup" in route_modifiers
        ) and spec_id in {"recommendation_subagent", "strategy", "school_query"}:
            constraints = self._extract_memory_followup_constraints(
                context=ctx.conversation_context,
                message=ctx.message,
                evidence_units=[],
            )
            if constraints:
                action_hints = [
                    f"先按“{constraints[0]}”收敛候选，再继续细化推荐。",
                    "补齐另一条关键偏好（预算/城市/专业）以减少下一轮分歧。",
                ]
        elif active_skill_id == "what_if" and spec_id == "what_if":
            if "结论以方向性为主，建议补齐目标学校分层后再量化。" not in risks_missing:
                risks_missing.insert(0, "结论以方向性为主，建议补齐目标学校分层后再量化。")
        elif active_skill_id == "profile_update" and spec_id == "profile_update":
            pending_patch = ctx.conversation_context.get(PENDING_PROFILE_PATCH_KEY)
            if isinstance(pending_patch, dict):
                confirm_command = str(pending_patch.get("confirm_command") or "").strip()
                reedit_command = str(pending_patch.get("reedit_command") or "").strip()
                action_hints = [
                    hint
                    for hint in [
                        f"发送 `{confirm_command}` 以提交档案修改。" if confirm_command else "",
                        f"发送 `{reedit_command}` 以重新编辑本次补丁。" if reedit_command else "",
                    ]
                    if hint
                ] or action_hints
            if not risks_missing:
                risks_missing.append("若字段值不够具体，系统会继续保留为待确认补丁而不直接写库。")
        elif active_skill_id == "offer_compare" and spec_id == "offer_compare":
            action_hints = [
                "先统一比较口径（总成本/净价/奖助），再做排序。",
                "补齐一个非财务偏好（专业或城市）用于打破接近选项。",
            ]

        if not risks_missing and missing_fields:
            risks_missing = [f"仍缺少关键信息：{', '.join(missing_fields[:3])}。"]

        what_done = str(meta.get("what_done") or "").strip()
        if not what_done:
            what_done = f"{spec_id} completed and produced {len(result.blocks)} block(s)."
        why_next = str(meta.get("why_next") or "").strip()
        if not why_next:
            why_next = action_hints[0] if action_hints else "继续执行下一能力或补充关键字段。"
        needs_input = [
            str(item).strip()
            for item in (meta.get("needs_input") or [])
            if str(item).strip()
        ] if isinstance(meta.get("needs_input"), list) else []
        if not needs_input:
            needs_input = missing_fields[:2]

        synthesis_claim = str(meta.get("synthesis_claim") or "").strip() or what_done
        synthesis_evidence = str(meta.get("synthesis_evidence") or "").strip()
        if not synthesis_evidence:
            evidence_parts: list[str] = []
            if facts_used:
                evidence_parts.append("；".join(facts_used[:2]))
            if constraints_used:
                evidence_parts.append("；".join(constraints_used[:2]))
            if not evidence_parts:
                evidence_parts.append(self._extract_capability_claim(capability_id=spec_id, content=result.content))
            synthesis_evidence = "；".join(part for part in evidence_parts if part).strip() or "基于本轮能力执行结果。"

        meta.update(
            {
                "what_done": self._sanitize_user_facing_text(what_done),
                "why_next": self._sanitize_user_facing_text(why_next),
                "needs_input": needs_input[:2],
                "action_hints": [self._sanitize_user_facing_text(item) for item in action_hints if item][:6],
                "risks_missing": [self._sanitize_user_facing_text(item) for item in risks_missing if item][:8],
                "synthesis_claim": self._sanitize_user_facing_text(synthesis_claim),
                "synthesis_evidence": self._sanitize_user_facing_text(synthesis_evidence),
                "personalization_evidence": evidence,
            }
        )
        return CapabilityResult(
            content=result.content,
            blocks=list(result.blocks or []),
            meta=meta,
        )

    def _validate_capability_output(
        self,
        *,
        spec: CapabilitySpec,
        result: CapabilityResult,
    ) -> None:
        if not isinstance(result.content, str):
            raise CapabilityExecutionError(
                f"Capability '{spec.id}' returned non-string content"
            )
        if not isinstance(result.blocks, list):
            raise CapabilityExecutionError(
                f"Capability '{spec.id}' returned non-list blocks"
            )
        for block in result.blocks:
            kind = block.get("kind")
            if kind not in _BLOCK_KINDS:
                raise CapabilityExecutionError(
                    f"Capability '{spec.id}' produced unsupported block kind '{kind}'"
                )
            if not isinstance(block.get("payload", {}), dict):
                raise CapabilityExecutionError(
                    f"Capability '{spec.id}' produced non-dict block payload"
                )

    def _aggregate_result(
        self,
        *,
        trace_id: str,
        planned: list[PlannedCapability],
        node_results: dict[str, tuple[CapabilityResult, int]],
        message: str = "",
        context: dict[str, Any] | None = None,
    ) -> tuple[TurnResult, dict[str, Any]]:
        plan_order = {item.id: item.plan_order for item in planned}
        ordered_capabilities = sorted(
            node_results.items(),
            key=lambda item: (item[1][1], plan_order.get(item[0], 10_000)),
        )
        blocks: list[ChatBlock] = []
        block_order = 0
        source_content_compacted = any(
            len((cap_result.content or "").strip()) > 900
            for _, (cap_result, _seq) in ordered_capabilities
        )
        output_compacted = source_content_compacted

        synthesis_payload, synthesis_meta, synthesis_summary = self._build_answer_synthesis_payload(
            ordered_capabilities=ordered_capabilities,
            planned=planned,
            message=message,
            context=context or {},
        )
        if synthesis_payload:
            compacted_payload, payload_compacted, _before_chars, _after_chars = self._compress_block_payload(
                dict(synthesis_payload),
            )
            output_compacted = output_compacted or payload_compacted
            synthesis_block_meta = dict(synthesis_meta)
            if source_content_compacted:
                synthesis_block_meta["compacted"] = True
            if payload_compacted:
                synthesis_block_meta["compacted"] = True
            blocks.append(
                ChatBlock(
                    id=str(uuid.uuid4()),
                    kind="answer_synthesis",
                    capability_id="answer_synthesis",
                    order=block_order,
                    payload=compacted_payload,
                    meta=synthesis_block_meta or None,
                )
            )
            block_order += 1

        for capability_id, (cap_result, _) in ordered_capabilities:
            cap_meta = cap_result.meta if isinstance(cap_result.meta, dict) else {}
            degraded = bool(cap_meta.get("degraded", False))
            personalization_evidence = (
                cap_meta.get("personalization_evidence")
                if isinstance(cap_meta.get("personalization_evidence"), dict)
                else None
            )
            cap_blocks = cap_result.blocks or []
            normalized_content = cap_result.content.strip()
            if not cap_blocks and normalized_content and not degraded and not synthesis_payload:
                fallback_meta: dict[str, Any] = {}
                if output_compacted:
                    fallback_meta["compacted"] = True
                if personalization_evidence:
                    fallback_meta["personalization_evidence"] = personalization_evidence
                cap_blocks = [
                    {
                        "kind": "text",
                        "payload": {"text": normalized_content},
                        "meta": fallback_meta,
                    }
                ]
            for raw in cap_blocks:
                raw_payload = raw.get("payload", {})
                payload_dict = dict(raw_payload) if isinstance(raw_payload, dict) else {}
                block_kind = str(raw.get("kind", "text"))
                if synthesis_payload and block_kind == "text":
                    continue
                if degraded and block_kind == "text":
                    continue
                compacted_payload = payload_dict
                payload_compacted = False
                if block_kind != "error":
                    compacted_payload, payload_compacted, _before, _after = self._compress_block_payload(payload_dict)
                output_compacted = output_compacted or payload_compacted
                block_meta = raw.get("meta", {})
                meta_dict = dict(block_meta) if isinstance(block_meta, dict) else {}
                if payload_compacted:
                    meta_dict["compacted"] = True
                if personalization_evidence:
                    meta_dict["personalization_evidence"] = personalization_evidence
                blocks.append(
                    ChatBlock(
                        id=str(uuid.uuid4()),
                        kind=block_kind,
                        capability_id=capability_id,
                        order=block_order,
                        payload=compacted_payload,
                        meta=meta_dict or None,
                    )
                )
                block_order += 1

        content = synthesis_summary.strip()
        if not content:
            content_parts: list[str] = []
            for _, (cap_result, _) in ordered_capabilities:
                text = cap_result.content.strip()
                if text:
                    compacted_text, content_compacted = self._compress_text_content(text)
                    output_compacted = output_compacted or content_compacted
                    content_parts.append(compacted_text)
            content = "\n\n".join(content_parts).strip()
        if not content:
            content = "已完成本轮执行。"
        content = self._clip_text(content, 220)
        return (
            TurnResult(
                trace_id=trace_id,
                status="ok",
                content=content,
                blocks=blocks,
                actions=[],
                usage={
                    "capability_count": len(node_results),
                    "block_count": len(blocks),
                },
            ),
            {
                "output_compacted": output_compacted,
                "synthesis_present": bool(synthesis_payload),
            },
        )

    def _build_answer_synthesis_payload(
        self,
        *,
        ordered_capabilities: list[tuple[str, tuple[CapabilityResult, int]]],
        planned: list[PlannedCapability],
        message: str,
        context: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], str]:
        evidence_units: list[dict[str, Any]] = []
        suppressed_caps: list[str] = []
        fusion_scores: dict[str, float] = {}
        degraded_caps: list[str] = []
        degraded_reason_codes: list[str] = []

        for capability_id, (cap_result, seq) in ordered_capabilities:
            cap_meta = cap_result.meta if isinstance(cap_result.meta, dict) else {}
            evidence = (
                cap_meta.get("personalization_evidence")
                if isinstance(cap_meta.get("personalization_evidence"), dict)
                else {}
            )
            facts_used = [
                str(item).strip()
                for item in evidence.get("facts_used", [])
                if str(item).strip()
            ] if isinstance(evidence.get("facts_used"), list) else []
            constraints_used = [
                str(item).strip()
                for item in evidence.get("constraints_used", [])
                if str(item).strip()
            ] if isinstance(evidence.get("constraints_used"), list) else []
            missing_fields = [
                str(item).strip()
                for item in evidence.get("missing_fields", [])
                if str(item).strip()
            ] if isinstance(evidence.get("missing_fields"), list) else []
            degraded = bool(cap_meta.get("degraded", False))
            degraded_reason = str(cap_meta.get("degraded_reason", "")).strip()
            if degraded and capability_id not in degraded_caps:
                degraded_caps.append(capability_id)
            if degraded_reason and degraded_reason not in degraded_reason_codes:
                degraded_reason_codes.append(degraded_reason)

            claim_seed = (
                str(cap_meta.get("synthesis_claim", "")).strip()
                or str(cap_meta.get("what_done", "")).strip()
                or self._extract_capability_claim(capability_id=capability_id, content=cap_result.content)
            )
            if not claim_seed:
                claim_seed = f"{capability_id} 已完成。"
            claim_seed = self._sanitize_user_facing_text(claim_seed)

            evidence_seed = str(cap_meta.get("synthesis_evidence", "")).strip()
            if not evidence_seed:
                evidence_parts: list[str] = []
                if facts_used:
                    evidence_parts.append("；".join(facts_used[:2]))
                if constraints_used:
                    evidence_parts.append("；".join(constraints_used[:2]))
                if not evidence_parts:
                    evidence_parts.append(self._clip_text(self._extract_capability_claim(capability_id=capability_id, content=cap_result.content), 120))
                evidence_seed = "；".join(part for part in evidence_parts if part).strip("； ") or "基于本轮能力执行结果。"
            evidence_seed = self._sanitize_user_facing_text(evidence_seed)

            if degraded:
                heuristic = self._build_best_effort_heuristic(
                    capability_id=capability_id,
                    reason_code=degraded_reason or "CAP_DEGRADED",
                    recovery_hint=self._build_recovery_hint(
                        reason_code=degraded_reason or "CAP_DEGRADED",
                        failed_capability=capability_id,
                    ),
                    message=cap_result.content,
                )
                claim_seed = heuristic["claim"]
                evidence_seed = heuristic["evidence"]
                action_hints = list(heuristic["actions"])
                risks_missing = list(heuristic["risks"])
            else:
                action_hints = [
                    str(item).strip()
                    for item in cap_meta.get("action_hints", [])
                    if str(item).strip()
                ] if isinstance(cap_meta.get("action_hints"), list) else []
                if not action_hints:
                    action_hints = self._default_actions_for_capability(capability_id)
                action_hints = [self._sanitize_user_facing_text(item) for item in action_hints if str(item).strip()]
                risks_missing = [
                    str(item).strip()
                    for item in cap_meta.get("risks_missing", [])
                    if str(item).strip()
                ] if isinstance(cap_meta.get("risks_missing"), list) else []

            base_weight = float(_SYNTHESIS_BASE_WEIGHTS.get(capability_id, 0.65))
            completeness_bonus = min(0.22, 0.05 * len(facts_used) + 0.03 * len(constraints_used))
            degraded_penalty = 0.24 if degraded else 0.0
            missing_penalty = min(0.2, 0.04 * len(missing_fields))
            score = round(base_weight + completeness_bonus - degraded_penalty - missing_penalty, 4)
            fusion_scores[capability_id] = score

            confidence_raw = evidence.get("confidence")
            try:
                confidence = float(confidence_raw)
            except Exception:
                confidence = 0.72
            if degraded:
                confidence = max(0.4, confidence - 0.16)

            evidence_units.append(
                {
                    "capability_id": capability_id,
                    "seq": int(seq),
                    "angle": _ANGLE_BY_CAPABILITY.get(capability_id, "general"),
                    "claim": self._clip_text(claim_seed, 150),
                    "evidence": self._clip_text(evidence_seed, 180),
                    "score": score,
                    "confidence": round(max(0.0, min(1.0, confidence)), 2),
                    "missing_fields": missing_fields,
                    "risks_missing": risks_missing,
                    "actions": action_hints[:4],
                }
            )

        selected_by_key: dict[str, dict[str, Any]] = {}
        for unit in evidence_units:
            dedupe_key = f"{unit['angle']}::{unit['claim']}"
            prev = selected_by_key.get(dedupe_key)
            if prev is None or float(unit["score"]) > float(prev["score"]):
                selected_by_key[dedupe_key] = unit
            else:
                capability_id = str(unit["capability_id"])
                if capability_id not in suppressed_caps:
                    suppressed_caps.append(capability_id)

        selected_units = sorted(
            selected_by_key.values(),
            key=lambda item: (-float(item["score"]), int(item["seq"])),
        )
        perspectives: list[dict[str, Any]] = []
        actions: list[dict[str, Any]] = []
        action_seen: set[str] = set()
        risks_missing: list[str] = []
        missing_seen: set[str] = set()
        for unit in selected_units[:6]:
            perspectives.append(
                {
                    "angle": unit["angle"],
                    "claim": unit["claim"],
                    "evidence": unit["evidence"],
                    "source_caps": [unit["capability_id"]],
                    "confidence": unit["confidence"],
                }
            )
            priority = "high" if float(unit["score"]) >= 0.9 else "medium" if float(unit["score"]) >= 0.76 else "low"
            for idx, step in enumerate(unit["actions"]):
                clean_step = str(step).strip()
                if not clean_step or clean_step in action_seen:
                    continue
                action_seen.add(clean_step)
                actions.append(
                    {
                        "step": clean_step,
                        "rationale": unit["claim"],
                        "priority": priority if idx == 0 else ("medium" if priority == "high" else priority),
                    }
                )
                if len(actions) >= 6:
                    break
            for missing in [*unit["missing_fields"], *unit["risks_missing"]]:
                clean_missing = str(missing).strip()
                if clean_missing and clean_missing not in missing_seen:
                    missing_seen.add(clean_missing)
                    risks_missing.append(clean_missing)
            if len(actions) >= 6:
                continue

        if not actions:
            actions = [
                {
                    "step": "先确认本轮最优先目标（选校/策略/对比），下一轮聚焦一个目标继续细化。",
                    "rationale": "当前信息已可继续推进，但聚焦单目标可显著提升答案质量。",
                    "priority": "high",
                }
            ]

        if not risks_missing:
            risks_missing.append("暂无关键缺失项，可继续补充目标学校、预算上限或专业偏好以提升稳定性。")

        conclusion = perspectives[0]["claim"] if perspectives else "已完成本轮分析，并整理出可执行建议。"
        summary = self._clip_text(f"结论：{conclusion} 下一步优先：{actions[0]['step']}", 190)
        degraded_retry_hint = ""
        if degraded_caps:
            degraded_retry_hint = self._build_recovery_hint(
                reason_code=degraded_reason_codes[0] if degraded_reason_codes else "CAP_TIMEOUT",
                failed_capability=degraded_caps[0],
            )

        payload = {
            "summary": summary,
            "conclusion": conclusion,
            "perspectives": perspectives,
            "actions": actions,
            "risks_missing": risks_missing[:8],
            "degraded": {
                "has_degraded": bool(degraded_caps),
                "caps": degraded_caps,
                "reason_codes": degraded_reason_codes,
                "retry_hint": degraded_retry_hint,
            },
        }
        skill_id = self._resolve_synthesis_skill(
            planned=planned,
            ordered_capabilities=ordered_capabilities,
            message=message,
            context=context,
        )
        payload = self._apply_synthesis_skill_contract(
            skill_id=skill_id,
            payload=payload,
            context=context,
            message=message,
            evidence_units=selected_units,
        )
        payload = self._sanitize_synthesis_payload(payload)
        payload["summary"] = self._clip_text(
            str(payload.get("summary") or f"结论：{payload.get('conclusion', conclusion)} 下一步优先：{(payload.get('actions') or actions)[0].get('step', '') if (payload.get('actions') or actions) else ''}"),
            190,
        )
        payload["conclusion"] = self._clip_text(str(payload.get("conclusion") or conclusion), 160)

        meta = {
            "stitch_version": "rule_weighted_v1",
            "fusion_scores": fusion_scores,
            "suppressed_caps": suppressed_caps,
            "task_skill": skill_id,
            "active_skill_id": str(context.get("active_skill_id") or skill_id),
            "skill_route_source": str(context.get("skill_route_source") or "unknown"),
            "skill_contract_version": str(context.get("skill_contract_version") or _SKILL_CONTRACT_VERSION),
        }
        return payload, meta, str(payload.get("summary") or summary)

    def _resolve_synthesis_skill(
        self,
        *,
        planned: list[PlannedCapability],
        ordered_capabilities: list[tuple[str, tuple[CapabilityResult, int]]],
        message: str,
        context: dict[str, Any],
    ) -> str:
        active_skill = str(context.get("active_skill_id") or "").strip()
        modifiers = context.get("skill_route_modifiers")
        if (
            isinstance(modifiers, list)
            and "memory_followup" in {str(item).strip() for item in modifiers if str(item).strip()}
            and active_skill in {"recommendation", "strategy", "school_query", "default"}
        ):
            cap_ids = {cap_id for cap_id, _cap_result in ordered_capabilities}
            if cap_ids.intersection({"recommendation_subagent", "strategy", "school_query"}):
                return "memory_followup"
        if active_skill in _SYNTHESIS_SKILLS:
            return active_skill

        capability_ids: list[str] = []
        for item in planned:
            if item.id not in capability_ids:
                capability_ids.append(item.id)
        for capability_id, _ in ordered_capabilities:
            if capability_id not in capability_ids:
                capability_ids.append(capability_id)

        decision = self._resolve_active_skill(
            message=message,
            context=context,
            capability_ids=capability_ids,
        )
        if "memory_followup" in set(decision.modifiers):
            return "memory_followup"
        if decision.active_skill_id in _SYNTHESIS_SKILLS:
            return decision.active_skill_id
        return "default"

    @staticmethod
    def _looks_like_what_if_text(text: str) -> bool:
        return bool(
            re.search(
                r"(如果|假设|what[\s-]?if|会怎么变化|会怎样变化|提到\d|提高到|from\s*\d+\s*to\s*\d+)",
                text or "",
                re.IGNORECASE,
            )
        )

    def _apply_synthesis_skill_contract(
        self,
        *,
        skill_id: str,
        payload: dict[str, Any],
        context: dict[str, Any],
        message: str,
        evidence_units: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if skill_id not in _SYNTHESIS_SKILLS:
            skill_id = "default"
        output = dict(payload)
        perspectives = list(output.get("perspectives") or [])
        actions = list(output.get("actions") or [])
        risks = [str(item).strip() for item in (output.get("risks_missing") or []) if str(item).strip()]

        if skill_id == "what_if":
            delta_match = re.search(
                r"(?P<metric>sat|gpa|预算|budget)[^\d]*(?P<before>\d+(?:\.\d+)?)\D+(?:提到|提高到|to)\D*(?P<after>\d+(?:\.\d+)?)",
                message or "",
                re.IGNORECASE,
            )
            if delta_match:
                metric = delta_match.group("metric").upper()
                before = delta_match.group("before")
                after = delta_match.group("after")
                conclusion = f"若 {metric} 从 {before} 提升到 {after}，录取概率通常会上行，但幅度取决于目标校分层。"
                evidence = f"已识别你提供的变量变化：{metric} {before}->{after}，并结合当前档案给出方向判断。"
            else:
                conclusion = "当前变量上调通常会带来正向录取变化，但需要目标校分层数据来量化幅度。"
                evidence = "已使用你本轮 what-if 问题与当前档案约束做方向性判断。"
            output["conclusion"] = conclusion
            output["summary"] = f"结论：{conclusion} 下一步优先：先验证最关键变量提升是否可落地。"
            if not perspectives:
                perspectives = []
            perspectives.insert(
                0,
                {
                    "angle": "scenario",
                    "claim": conclusion,
                    "evidence": evidence,
                    "source_caps": ["what_if"],
                    "confidence": 0.78,
                },
            )
            actions = [
                {
                    "step": "先聚焦一个最可控变量（SAT/GPA/活动）做两周冲刺并记录变化前后结果。",
                    "rationale": "把 what-if 预测变成可验证动作，避免只停留在假设。",
                    "priority": "high",
                },
                {
                    "step": "同步准备一个风险兜底方案（匹配校或预算替代路径），避免单点策略失效。",
                    "rationale": "保持策略稳健性，降低波动风险。",
                    "priority": "medium",
                },
            ]
            if "缺少目标学校分层与当前基线分位，结论目前以方向性为主。" not in risks:
                risks.insert(0, "缺少目标学校分层与当前基线分位，结论目前以方向性为主。")

        elif skill_id == "guided_intake":
            questions = self._build_guided_intake_questions(context=context, evidence_units=evidence_units)
            output["conclusion"] = "为给你更贴身的建议，先补齐下面几条关键信息。"
            output["summary"] = "结论：先补齐关键信息。下一步优先：按问题顺序回复，我会据此继续推进。"
            perspectives = [
                {
                    "angle": "intake",
                    "claim": "当前阶段以信息补齐为主，先问高影响字段可显著提升下一轮建议质量。",
                    "evidence": "问题顺序按申请影响度排序：学术基线 -> 预算与地区 -> 专业与目标。",
                    "source_caps": ["guided_intake"],
                    "confidence": 0.82,
                }
            ]
            actions = [
                {
                    "step": question,
                    "rationale": "补齐该字段后可减少下一轮不确定性。",
                    "priority": "high" if idx == 0 else "medium",
                }
                for idx, question in enumerate(questions)
            ][:1]
            risks = [item for item in risks if item]
            if "关键信息未补齐前，推荐与策略会偏保守。" not in risks:
                risks.insert(0, "关键信息未补齐前，推荐与策略会偏保守。")
            if "其余问题会在后续轮次按信息增益顺序继续提问。" not in risks:
                risks.append("其余问题会在后续轮次按信息增益顺序继续提问。")

        elif skill_id == "memory_followup":
            constraints = self._extract_memory_followup_constraints(context=context, message=message, evidence_units=evidence_units)
            picked = constraints[:3]
            if picked:
                evidence_text = "；".join(picked)
                output["conclusion"] = f"已基于你刚确认的偏好生成下一步建议：{evidence_text}。"
                output["summary"] = f"结论：已按你的偏好推进。下一步优先：先执行与“{picked[0]}”最相关的动作。"
            else:
                output["conclusion"] = "已读取你上条偏好，先给出保守可执行方案，后续可继续细化。"
                output["summary"] = "结论：已按历史偏好继续推进。下一步优先：补充预算/城市/专业三项中的至少两项。"
            perspectives = [
                {
                    "angle": "profile",
                    "claim": output["conclusion"],
                    "evidence": "已优先使用你最近一轮的偏好与档案约束。",
                    "source_caps": ["profile_read", "recommendation_subagent"],
                    "confidence": 0.83,
                }
            ]
            action_items: list[dict[str, Any]] = []
            for item in picked:
                if "预算" in item or "$" in item:
                    action_items.append(
                        {
                            "step": f"先把学校净价筛到不超过你的预算上限（{item}），再做冲刺/匹配/保底分层。",
                            "rationale": "先控制财务边界可避免后续方案不可执行。",
                            "priority": "high",
                        }
                    )
                elif "城市" in item:
                    action_items.append(
                        {
                            "step": "优先保留大城市圈学校作为第一批候选，再补 1-2 所同梯度非大城市备选。",
                            "rationale": "同时满足偏好与稳健性，避免过度集中。",
                            "priority": "high",
                        }
                    )
                elif "专业" in item or "CS" in item.upper() or "AI" in item.upper():
                    action_items.append(
                        {
                            "step": "优先选择含 CS+AI 方向课程与科研资源的项目，再对比就业去向。",
                            "rationale": "保证建议与专业目标直接对齐。",
                            "priority": "high",
                        }
                    )
            if len(action_items) < 2:
                action_items.append(
                    {
                        "step": "补充你最看重的学校类型（公立/私立）或地区范围，我会据此收敛推荐。",
                        "rationale": "提高下一轮匹配度。",
                        "priority": "medium",
                    }
                )
            actions = action_items[:4]
            if len(picked) < 2:
                risks.insert(0, "已识别的偏好约束不足 2 项，建议补充预算/城市/专业偏好后再细化。")

        elif skill_id == "recommendation":
            output["conclusion"] = str(output.get("conclusion") or "已整理出与你档案约束匹配的学校推荐方向。")
            output["summary"] = f"结论：{output['conclusion']} 下一步优先：先锁定冲刺/匹配/保底三档的候选。"
            if not actions:
                actions = [
                    {
                        "step": "先确认冲刺/匹配/保底每档 2-3 所，再进入文书与时间线执行。",
                        "rationale": "先定盘再执行，避免反复重排。",
                        "priority": "high",
                    },
                    {
                        "step": "补充预算上限或奖助偏好，缩小不可执行候选。",
                        "rationale": "提高推荐可落地性。",
                        "priority": "medium",
                    },
                ]
            if "若预算与专业约束未补齐，推荐清单仍会有波动。" not in risks:
                risks.insert(0, "若预算与专业约束未补齐，推荐清单仍会有波动。")

        elif skill_id == "strategy":
            output["conclusion"] = str(output.get("conclusion") or "已生成可执行的申请节奏与轮次策略。")
            output["summary"] = f"结论：{output['conclusion']} 下一步优先：先锁定最早截止轮次任务。"
            actions = actions or [
                {
                    "step": "按 ED/EA/RD 建立倒排清单，本周先完成最早截止材料。",
                    "rationale": "先处理时间敏感项可降低延期风险。",
                    "priority": "high",
                },
                {
                    "step": "每档学校至少保留 2 所，避免单档过度集中。",
                    "rationale": "保持申请组合稳健。",
                    "priority": "medium",
                },
            ]

        elif skill_id == "school_query":
            output["conclusion"] = str(output.get("conclusion") or "已返回学校关键信息并结合你的档案做了解读。")
            output["summary"] = f"结论：{output['conclusion']} 下一步优先：把结果用于对比或策略细化。"
            actions = actions or [
                {
                    "step": "先确定 1-2 所目标学校，统一比较口径后再扩展候选。",
                    "rationale": "降低检索噪声。",
                    "priority": "high",
                }
            ]

        elif skill_id == "offer_compare":
            output["conclusion"] = str(output.get("conclusion") or "已完成 offer 的成本与结果维度比较。")
            output["summary"] = f"结论：{output['conclusion']} 下一步优先：补一条关键偏好打破近似选项。"
            actions = [
                {
                    "step": "按总成本/净价/奖助统一口径重排，再看学术与就业维度。",
                    "rationale": "避免口径不一致导致误判。",
                    "priority": "high",
                },
                {
                    "step": "补充一个强偏好（城市/专业方向），用于 close-call 决策。",
                    "rationale": "快速缩小近似选项。",
                    "priority": "medium",
                },
            ]

        elif skill_id == "profile_update":
            gate = context.get("_profile_update_gate") if isinstance(context.get("_profile_update_gate"), dict) else {}
            pending_patch = context.get(PENDING_PROFILE_PATCH_KEY)
            action = str(gate.get("action") or "").strip()
            if isinstance(pending_patch, dict):
                confirm_command = str(pending_patch.get("confirm_command") or "").strip()
                reedit_command = str(pending_patch.get("reedit_command") or "").strip()
                output["conclusion"] = "已生成档案修改提案，等待你确认后再写入。"
                output["summary"] = "结论：档案修改提案已就绪。下一步优先：确认提交或重新编辑。"
                actions = [
                    {
                        "step": f"发送 `{confirm_command}` 提交补丁。" if confirm_command else "发送确认命令提交补丁。",
                        "rationale": "保持写入前可核对，避免误改。",
                        "priority": "high",
                    },
                    {
                        "step": f"发送 `{reedit_command}` 重新编辑。" if reedit_command else "若需改动，直接补充更具体字段值。",
                        "rationale": "保证落库内容与预期一致。",
                        "priority": "medium",
                    },
                ]
            elif action == "commit":
                output["conclusion"] = "已检测到档案写入确认意图，正在按校验规则提交更新。"
            else:
                output["conclusion"] = str(output.get("conclusion") or "已识别档案更新意图，但仍需更具体字段值。")
            if "档案写入采用先提案后确认，未确认前不会落库。" not in risks:
                risks.insert(0, "档案写入采用先提案后确认，未确认前不会落库。")

        elif skill_id == "multi_intent":
            output["conclusion"] = "已将你的多目标请求拆分并按优先级编排执行。"
            output["summary"] = "结论：多目标已拆分并执行。下一步优先：先确认本轮第一优先目标。"
            actions = actions or [
                {
                    "step": "先确认本轮第一优先目标（选校/策略/对比），下一轮再扩展第二目标。",
                    "rationale": "降低同轮目标冲突，提升答案稳定性。",
                    "priority": "high",
                }
            ]
            if "同轮目标越多，单目标深度会下降；建议下一轮聚焦。" not in risks:
                risks.insert(0, "同轮目标越多，单目标深度会下降；建议下一轮聚焦。")

        elif skill_id == "robustness":
            output["conclusion"] = str(output.get("conclusion") or "系统已保留上下文并进入稳健恢复路径。")
            output["summary"] = f"结论：{output['conclusion']} 下一步优先：按恢复提示提供一个更具体输入。"
            degraded = output.get("degraded") if isinstance(output.get("degraded"), dict) else {}
            retry_hint = str(degraded.get("retry_hint") or "").strip()
            actions = [
                {
                    "step": retry_hint or "把请求拆成更小的单目标后重试。",
                    "rationale": "降低失败链路复发概率。",
                    "priority": "high",
                }
            ]

        elif skill_id == "emotional_support":
            output["conclusion"] = str(output.get("conclusion") or "先稳住情绪，再回到一个可执行的小目标。")
            output["summary"] = "结论：先稳定状态。下一步优先：执行一个最小可控动作。"
            actions = actions or [
                {
                    "step": "先完成一个 10 分钟内可完成的小任务（如确认 1 所目标校）。",
                    "rationale": "降低焦虑并恢复掌控感。",
                    "priority": "high",
                }
            ]

        output["perspectives"] = perspectives[:6]
        output["actions"] = actions[:6]
        output["risks_missing"] = risks[:8]
        return output

    def _extract_memory_followup_constraints(
        self,
        *,
        context: dict[str, Any],
        message: str,
        evidence_units: list[dict[str, Any]],
    ) -> list[str]:
        constraints: list[str] = []
        budget = context.get("profile_budget_usd")
        if isinstance(budget, (int, float)) and budget > 0:
            constraints.append(f"预算上限 ${int(budget)}")
        majors = context.get("profile_intended_majors")
        if isinstance(majors, list) and majors:
            major_text = ", ".join(str(item).strip() for item in majors[:2] if str(item).strip())
            if major_text:
                constraints.append(f"专业偏好 {major_text}")
        preferred_regions = context.get("preferred_regions")
        if isinstance(preferred_regions, list) and preferred_regions:
            constraints.append(f"地区偏好 {', '.join(str(item).strip() for item in preferred_regions[:2] if str(item).strip())}")
        if re.search(r"(大城市|metro|urban|city)", message or "", re.IGNORECASE):
            constraints.append("城市偏好 大城市")
        budget_match = re.search(r"(预算上限|预算|budget)[^\d]*(\d{4,6})", message or "", re.IGNORECASE)
        if budget_match:
            constraints.append(f"预算上限 ${budget_match.group(2)}")
        if re.search(r"(cs\+?ai|cs|computer science|data science|ai方向|专业方向)", message or "", re.IGNORECASE):
            constraints.append("专业偏好 CS+AI")
        for unit in evidence_units[:4]:
            evidence = str(unit.get("evidence", "")).strip()
            if "Budget" in evidence and "$" not in evidence:
                num = re.search(r"Budget[:\s]*(\d{4,6})", evidence, re.IGNORECASE)
                if num:
                    constraints.append(f"预算上限 ${num.group(1)}")
        deduped: list[str] = []
        for item in constraints:
            text = str(item).strip()
            if text and text not in deduped:
                deduped.append(text)
        return deduped

    def _build_guided_intake_questions(
        self,
        *,
        context: dict[str, Any],
        evidence_units: list[dict[str, Any]],
    ) -> list[str]:
        missing_fields_raw = context.get("profile_missing_fields")
        missing_fields = [
            str(item).strip()
            for item in missing_fields_raw
            if str(item).strip()
        ] if isinstance(missing_fields_raw, list) else []
        if not missing_fields:
            for unit in evidence_units:
                for item in unit.get("missing_fields", []):
                    text = str(item).strip()
                    if text and text not in missing_fields:
                        missing_fields.append(text)
        questions: list[str] = []
        for field in missing_fields:
            mapped = self._map_missing_field_to_question(field)
            if mapped and mapped not in questions:
                questions.append(mapped)
            if len(questions) >= 4:
                break
        if not questions:
            questions = [
                "你更倾向申请哪个国家或地区？为什么？",
                "你目前的预算上限大概是多少（每年美元）？",
                "你更想走 CS 纯技术方向，还是 CS+AI/数据交叉方向？",
            ]
        if len(questions) < 2:
            for fallback in [
                "你目前的预算上限大概是多少（每年美元）？",
                "你更想走 CS 纯技术方向，还是 CS+AI/数据交叉方向？",
                "你偏好的国家/地区和城市类型是什么？",
            ]:
                if fallback not in questions:
                    questions.append(fallback)
                if len(questions) >= 3:
                    break
        return questions[:4]

    @staticmethod
    def _map_missing_field_to_question(field_name: str) -> str:
        normalized = field_name.lower()
        if "gpa" in normalized:
            return "你目前 GPA 是多少（以及满分制）？"
        if "sat" in normalized or "act" in normalized:
            return "你的 SAT/ACT 当前分数是多少？计划是否还会重考？"
        if "budget" in normalized or "finance" in normalized:
            return "你的年度预算上限大概是多少（美元）？是否需要奖助学金？"
        if "major" in normalized:
            return "你最想申请的专业方向是什么？是否接受相关交叉专业？"
        if "target_year" in normalized or "cycle" in normalized:
            return "你计划申请的入学年份是？是否有 ED/EA 时间偏好？"
        if "region" in normalized or "country" in normalized:
            return "你偏好的国家/地区和城市类型是什么？"
        return ""

    @classmethod
    def _sanitize_user_facing_text(cls, text: str) -> str:
        cleaned = str(text or "").strip()
        if not cleaned:
            return ""
        replacements = {
            "Generated recommendation payload and narrative from current profile context.": "已结合你的当前档案生成推荐方向与结论。",
            "Collected missing intake fields and advanced the intake workflow.": "已识别当前仍需补齐的关键信息。",
            "Loaded and synchronized the latest student portfolio snapshot.": "已读取并同步你的最新学生档案。",
            "Detected update intent but lacked concrete patchable field-value pairs.": "识别到你想更新档案，但还缺少可直接写入的字段值。",
            "Generated application strategy sequencing and timeline suggestions.": "已生成申请节奏与时间线建议。",
            "Returned school-specific facts and advisor interpretation.": "已返回学校关键事实并结合你的背景做了解读。",
            "Compared available offers across cost and outcome dimensions.": "已按成本与结果维度完成 offer 对比。",
            "Ran hypothetical scenario analysis and summarized delta impacts.": "已完成情景变化分析并总结关键影响。",
            "Returned a general advisor response.": "已给出本轮通用顾问回复。",
        }
        for source, target in replacements.items():
            cleaned = cleaned.replace(source, target)
        cleaned = re.sub(r"\bpayload\b", "结果", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bworkflow\b", "流程", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bgenerated\b", "已生成", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    @classmethod
    def _strip_internal_reason_text(cls, text: str) -> str:
        cleaned = str(text or "").strip()
        if not cleaned:
            return ""
        cleaned = _INTERNAL_REASON_TEXT_PATTERN.sub("当前采用降级兜底", cleaned)
        cleaned = _INTERNAL_REASON_CODE_PATTERN.sub("系统降级", cleaned)
        cleaned = re.sub(r"(?:当前采用降级兜底[\s,，;；]*){2,}", "当前采用降级兜底 ", cleaned)
        return re.sub(r"\s+", " ", cleaned).strip(" .，；;")

    @classmethod
    def _sanitize_synthesis_payload(cls, payload: dict[str, Any]) -> dict[str, Any]:
        output = dict(payload)
        output["summary"] = cls._strip_internal_reason_text(
            cls._sanitize_user_facing_text(str(output.get("summary", "")))
        )
        output["conclusion"] = cls._strip_internal_reason_text(
            cls._sanitize_user_facing_text(str(output.get("conclusion", "")))
        )
        perspectives = []
        for item in output.get("perspectives", []):
            if not isinstance(item, dict):
                continue
            normalized = dict(item)
            normalized["claim"] = cls._strip_internal_reason_text(
                cls._sanitize_user_facing_text(str(item.get("claim", "")))
            )
            normalized["evidence"] = cls._strip_internal_reason_text(
                cls._sanitize_user_facing_text(str(item.get("evidence", "")))
            )
            perspectives.append(normalized)
        output["perspectives"] = perspectives
        actions = []
        for item in output.get("actions", []):
            if not isinstance(item, dict):
                continue
            normalized = dict(item)
            normalized["step"] = cls._strip_internal_reason_text(
                cls._sanitize_user_facing_text(str(item.get("step", "")))
            )
            normalized["rationale"] = cls._strip_internal_reason_text(
                cls._sanitize_user_facing_text(str(item.get("rationale", "")))
            )
            actions.append(normalized)
        output["actions"] = actions
        risks = [
            cls._strip_internal_reason_text(cls._sanitize_user_facing_text(str(item)))
            for item in output.get("risks_missing", [])
        ]
        output["risks_missing"] = [
            item
            for item in risks
            if item
            and not re.search(r"(降级原因|原因码|系统降级|degraded|fallback)", item, re.IGNORECASE)
        ][:8]
        for field in ("summary", "conclusion"):
            text = str(output.get(field, ""))
            for pattern in _INTERNAL_JARGON_PATTERNS:
                if pattern.search(text):
                    text = pattern.sub("", text).strip()
            output[field] = re.sub(r"\s+", " ", text).strip(" .")
        return output

    @classmethod
    def _extract_capability_claim(cls, *, capability_id: str, content: str) -> str:
        cleaned = re.sub(r"`{1,3}", "", str(content or "").strip())
        if not cleaned:
            return f"{capability_id} completed."
        for segment in re.split(r"[\n。.!?；;]", cleaned):
            text = segment.strip()
            if not text:
                continue
            if len(text) > 6:
                return cls._clip_text(text, 140)
        return cls._clip_text(cleaned, 140)

    @staticmethod
    def _default_actions_for_capability(capability_id: str) -> list[str]:
        if capability_id == "what_if":
            return [
                "把最有把握提升的单一变量拆成 2 周执行任务，并在下一轮反馈结果。",
                "同步准备风险兜底方案，避免单一假设失效后无备选路径。",
            ]
        if capability_id == "offer_compare":
            return [
                "先统一比较口径（总成本、净价、奖助），再做横向排序。",
                "补充一条非财务约束（专业匹配或城市偏好）提升决策稳定性。",
            ]
        if capability_id == "strategy":
            return [
                "按 ED/EA/RD 建立月度倒排清单，先完成最早截止项。",
                "将学校池按冲刺/匹配/保底重新校准，控制每档数量。",
            ]
        if capability_id == "school_query":
            return [
                "先限定目标学校或项目范围，减少检索噪声。",
                "补充预算上限与专业方向，用于筛选优先级。",
            ]
        if capability_id in {"profile_update", "profile_read"}:
            return [
                "先确认档案关键信息是否准确，再触发下一轮推荐。",
            ]
        if capability_id == "guided_intake":
            return [
                "优先补齐高影响字段（GPA/SAT/预算/目标专业），缩短后续问答轮数。",
            ]
        return ["按本轮结论执行第一个动作，并在下一轮反馈结果继续收敛。"]

    def _build_registry(self) -> dict[str, CapabilitySpec]:
        return {
            "profile_read": CapabilitySpec(
                id="profile_read",
                input_schema={"type": "object", "required": ["message"]},
                output_schema={"type": "object", "required": ["content", "blocks"]},
                dependencies=[],
                cost_class="low",
                execute=self._run_profile_read,
                requires_db_session=True,
            ),
            "profile_update": CapabilitySpec(
                id="profile_update",
                input_schema={"type": "object", "required": ["message"]},
                output_schema={"type": "object", "required": ["content", "blocks"]},
                dependencies=[],
                cost_class="medium",
                execute=self._run_profile_update,
                requires_db_session=True,
            ),
            "guided_intake": CapabilitySpec(
                id="guided_intake",
                input_schema={"type": "object", "required": ["message"]},
                output_schema={"type": "object", "required": ["content", "blocks"]},
                dependencies=[],
                cost_class="medium",
                execute=self._run_guided_intake,
                requires_db_session=True,
            ),
            "recommendation_subagent": CapabilitySpec(
                id="recommendation_subagent",
                input_schema={"type": "object", "required": ["message"]},
                output_schema={"type": "object", "required": ["content", "blocks"]},
                dependencies=["guided_intake", "profile_read"],
                cost_class="high",
                execute=self._run_recommendation_subagent,
                requires_db_session=True,
                failure_policy="best_effort",
            ),
            "school_query": CapabilitySpec(
                id="school_query",
                input_schema={"type": "object", "required": ["message"]},
                output_schema={"type": "object", "required": ["content", "blocks"]},
                dependencies=["profile_read"],
                cost_class="medium",
                execute=self._run_school_query,
                requires_db_session=True,
                failure_policy="best_effort",
            ),
            "strategy": CapabilitySpec(
                id="strategy",
                input_schema={"type": "object", "required": ["message"]},
                output_schema={"type": "object", "required": ["content", "blocks"]},
                dependencies=["profile_read"],
                cost_class="medium",
                execute=self._run_strategy,
                requires_db_session=True,
                failure_policy="best_effort",
            ),
            "offer_compare": CapabilitySpec(
                id="offer_compare",
                input_schema={"type": "object", "required": ["message"]},
                output_schema={"type": "object", "required": ["content", "blocks"]},
                dependencies=["profile_read"],
                cost_class="high",
                execute=self._run_offer_compare,
                requires_db_session=True,
                failure_policy="best_effort",
            ),
            "what_if": CapabilitySpec(
                id="what_if",
                input_schema={"type": "object", "required": ["message"]},
                output_schema={"type": "object", "required": ["content", "blocks"]},
                dependencies=["profile_read", "school_query"],
                cost_class="high",
                execute=self._run_what_if,
                requires_db_session=True,
                failure_policy="best_effort",
            ),
            "emotional_support": CapabilitySpec(
                id="emotional_support",
                input_schema={"type": "object", "required": ["message"]},
                output_schema={"type": "object", "required": ["content", "blocks"]},
                dependencies=[],
                cost_class="low",
                execute=self._run_emotional_support,
                requires_db_session=False,
            ),
            "general": CapabilitySpec(
                id="general",
                input_schema={"type": "object", "required": ["message"]},
                output_schema={"type": "object", "required": ["content", "blocks"]},
                dependencies=[],
                cost_class="low",
                execute=self._run_general,
                requires_db_session=False,
            ),
        }

    async def _run_profile_read(self, ctx: CapabilityContext) -> CapabilityResult:
        if ctx.student_id is None:
            return await self._run_general(ctx)
        snapshot = await build_profile_snapshot(
            session=ctx.session,
            student_id=ctx.student_id,
        )
        content = str(snapshot.get("content", "")).strip()
        payload = snapshot.get("payload") if isinstance(snapshot.get("payload"), dict) else {}
        blocks: list[dict[str, Any]] = []
        evidence_facts: list[str] = []
        if payload:
            portfolio = payload.get("portfolio") if isinstance(payload.get("portfolio"), dict) else {}
            completion = payload.get("completion") if isinstance(payload.get("completion"), dict) else {}
            facts = self._extract_profile_facts(portfolio=portfolio, completion=completion)
            if completion:
                evidence_facts.append(
                    f"Profile completion: {completion.get('completion_pct', 0)}"
                )
            await self._save_student_memory_entries(
                student_id=ctx.student_id,
                values=facts,
                layer="long_term",
            )
            blocks.append(
                {
                    "kind": "profile_snapshot",
                    "payload": payload,
                    "meta": {"source": "portfolio"},
                }
            )
        if content:
            blocks.append({"kind": "text", "payload": {"text": content}, "meta": {}})
        evidence = self._build_personalization_evidence(
            ctx=ctx,
            required_fields=["gpa", "sat", "budget"],
            confidence=0.86,
            extra_facts=evidence_facts,
        )
        return CapabilityResult(
            content=content or "Profile snapshot loaded.",
            blocks=blocks,
            meta={
                "profile_read": True,
                "what_done": "Loaded and synchronized the latest student portfolio snapshot.",
                "why_next": "Downstream capabilities should consume this snapshot to stay consistent.",
                "personalization_evidence": evidence,
            },
        )

    async def _run_profile_update(self, ctx: CapabilityContext) -> CapabilityResult:
        if ctx.student_id is None:
            return await self._run_general(ctx)
        base_evidence = self._build_personalization_evidence(
            ctx=ctx,
            required_fields=["gpa", "sat", "budget", "intended_majors"],
            confidence=0.74,
        )
        gate_raw = ctx.conversation_context.get("_profile_update_gate")
        if isinstance(gate_raw, dict):
            gate_action = str(gate_raw.get("action", "propose")).strip()
            can_commit = bool(gate_raw.get("can_commit", False))
            gate_reason = str(gate_raw.get("reason", "")).strip()
        else:
            gate = resolve_profile_update_gate(
                message=ctx.message,
                context=ctx.conversation_context,
            )
            gate_action = gate.action
            can_commit = gate.can_commit
            gate_reason = gate.reason

        if gate_action == "commit":
            if not can_commit:
                raise CapabilityExecutionError(
                    f"profile_update pre-write checkpoint failed: {gate_reason}"
                )
            pending = ctx.conversation_context.get(PENDING_PROFILE_PATCH_KEY)
            if not isinstance(pending, dict):
                raise CapabilityExecutionError("pending profile patch missing during commit")
            applied = await apply_pending_profile_patch(
                session=ctx.session,
                memory=ctx.memory,
                session_id=ctx.session_id,
                student_id=ctx.student_id,
                context=ctx.conversation_context,
                pending=pending,
            )
            content = str(applied.get("content", "")).strip()
            payload = applied.get("payload") if isinstance(applied.get("payload"), dict) else {}
            blocks: list[dict[str, Any]] = []
            if payload:
                portfolio = payload.get("portfolio") if isinstance(payload.get("portfolio"), dict) else {}
                completion = portfolio.get("completion") if isinstance(portfolio.get("completion"), dict) else {}
                facts = self._extract_profile_facts(portfolio=portfolio, completion=completion)
                await self._save_student_memory_entries(
                    student_id=ctx.student_id,
                    values=facts,
                    layer="long_term",
                )
                changed_fields = payload.get("changed_fields")
                if isinstance(changed_fields, list) and changed_fields:
                    await self._save_student_memory_entries(
                        student_id=ctx.student_id,
                        values={"last_profile_changed_fields": [str(item) for item in changed_fields[:12]]},
                        layer="short_term",
                    )
                blocks.append(
                    {
                        "kind": "profile_patch_result",
                        "payload": payload,
                        "meta": {"applied": True},
                    }
                )
            if content:
                blocks.append({"kind": "text", "payload": {"text": content}, "meta": {}})
            changed_fields = payload.get("changed_fields")
            changed_labels = (
                [str(item) for item in changed_fields if str(item).strip()]
                if isinstance(changed_fields, list)
                else []
            )
            commit_evidence = dict(base_evidence)
            commit_evidence["missing_fields"] = []
            if changed_labels:
                commit_evidence["facts_used"] = [
                    *commit_evidence.get("facts_used", []),
                    f"Changed fields: {', '.join(changed_labels[:4])}",
                ][:8]
            return CapabilityResult(
                content=content or "Profile updated.",
                blocks=blocks,
                meta={
                    "applied": True,
                    "proposal_id": payload.get("proposal_id"),
                    "what_done": "Applied the confirmed profile patch and persisted portfolio changes.",
                    "why_next": "Run profile-dependent planning with the refreshed profile snapshot.",
                    "personalization_evidence": commit_evidence,
                },
            )

        if gate_action == "reedit":
            await clear_pending_profile_patch(
                memory=ctx.memory,
                session_id=ctx.session_id,
                context=ctx.conversation_context,
            )
            content = "Cleared the pending profile update. Share the corrected fields and I will prepare a new patch."
            return CapabilityResult(
                content=content,
                blocks=[{"kind": "text", "payload": {"text": content}, "meta": {}}],
                meta={
                    "applied": False,
                    "cleared": True,
                    "what_done": "Cleared stale pending patch state.",
                    "why_next": "Need a new concrete field/value request to create the next proposal.",
                    "personalization_evidence": base_evidence,
                },
            )

        if gate_action == "noop":
            content = (
                "我没识别到明确可写入的档案字段。"
                "你可以按这个格式发：`把 GPA 改成 3.85，SAT 改成 1500，预算改成 70000，专业加 Data Science`。"
            )
            noop_evidence = dict(base_evidence)
            noop_evidence["missing_fields"] = [
                "academics.gpa",
                "academics.sat_total",
                "finance.budget_usd",
                "academics.intended_majors",
            ]
            return CapabilityResult(
                content=content,
                blocks=[{"kind": "text", "payload": {"text": content}, "meta": {}}],
                meta={
                    "applied": False,
                    "proposal_created": False,
                    "what_done": "Detected update intent but lacked concrete patchable field-value pairs.",
                    "why_next": "Need one explicit patch instruction to proceed.",
                    "personalization_evidence": noop_evidence,
                },
            )

        proposed = await create_profile_patch_proposal(
            llm=ctx.llm,
            session=ctx.session,
            memory=ctx.memory,
            session_id=ctx.session_id,
            student_id=ctx.student_id,
            message=ctx.message,
            context=ctx.conversation_context,
        )
        content = str(proposed.get("content", "")).strip()
        proposal_payload = proposed.get("proposal") if isinstance(proposed.get("proposal"), dict) else None
        blocks: list[dict[str, Any]] = []
        if proposal_payload is not None:
            blocks.append(
                {
                    "kind": "profile_patch_proposal",
                    "payload": proposal_payload,
                    "meta": {"applied": False},
                }
            )
        if content:
            blocks.append({"kind": "text", "payload": {"text": content}, "meta": {}})
        proposal_meta = proposed.get("meta") if isinstance(proposed.get("meta"), dict) else {}
        proposal_evidence = dict(base_evidence)
        proposal_missing = proposal_meta.get("missing_fields")
        if isinstance(proposal_missing, list):
            proposal_evidence["missing_fields"] = [
                str(item) for item in proposal_missing if str(item).strip()
            ][:8]
        return CapabilityResult(
            content=content or "Prepared profile update proposal.",
            blocks=blocks,
            meta={
                **proposal_meta,
                "what_done": "Prepared a profile patch proposal for confirmation.",
                "why_next": "Confirm the proposal command to apply the patch safely.",
                "personalization_evidence": proposal_evidence,
            },
        )

    async def _run_guided_intake(self, ctx: CapabilityContext) -> CapabilityResult:
        if ctx.student_id is None:
            return await self._run_general(ctx)
        response = await handle_guided_intake(
            ctx.llm,
            ctx.session,
            ctx.memory,
            ctx.session_id,
            ctx.student_id,
            ctx.message,
        )
        cleaned = str(response.get("content", "")).strip()
        guided_questions = response.get("guided_questions")
        intake_complete = bool(response.get("intake_complete"))
        await self._save_student_memory_entries(
            student_id=ctx.student_id,
            values={"intake_complete": intake_complete},
            layer="working",
        )
        blocks: list[dict[str, Any]] = []
        if cleaned:
            blocks.append({"kind": "text", "payload": {"text": cleaned}, "meta": {}})
        if isinstance(guided_questions, list) and guided_questions:
            primary_question = guided_questions[0]
            next_turn_candidates = [
                item for item in guided_questions[1:4]
                if isinstance(item, dict)
            ]
            payload: dict[str, Any] = {"questions": [primary_question]}
            if next_turn_candidates:
                payload["next_turn_candidates"] = next_turn_candidates
            blocks.append(
                {
                    "kind": "guided_questions",
                    "payload": payload,
                    "meta": {"interactive": True},
                }
            )
        evidence = self._build_personalization_evidence(
            ctx=ctx,
            required_fields=["gpa", "sat", "budget", "intended_majors"],
            confidence=0.7 if not intake_complete else 0.82,
        )
        return CapabilityResult(
            content=cleaned,
            blocks=blocks,
            meta={
                "intake_complete": intake_complete,
                "what_done": (
                    "Collected missing intake fields and advanced the intake workflow."
                    if not intake_complete
                    else "Finished intake collection for this student."
                ),
                "why_next": (
                    "Continue asking only missing fields."
                    if not intake_complete
                    else "Recommendation and strategy capabilities can now run with better context."
                ),
                "personalization_evidence": evidence,
            },
        )

    async def _run_recommendation_subagent(self, ctx: CapabilityContext) -> CapabilityResult:
        if ctx.student_id is None:
            return await self._run_general(ctx)
        response = await handle_recommendation(
            ctx.llm,
            ctx.session,
            ctx.memory,
            ctx.session_id,
            ctx.student_id,
            ctx.message,
        )
        content = str(response.get("content", "")).strip()
        recommendation_payload = response.get("recommendation")
        blocks: list[dict[str, Any]] = []
        if isinstance(recommendation_payload, dict):
            blocks.append(
                {
                    "kind": "recommendation",
                    "payload": recommendation_payload,
                    "meta": {"subagent": True},
                }
            )
        if content:
            blocks.append(
                {
                    "kind": "text",
                    "payload": {"text": content},
                    "meta": {},
                }
            )
        evidence = self._build_personalization_evidence(
            ctx=ctx,
            required_fields=["gpa", "sat", "budget", "intended_majors"],
            confidence=0.83,
        )
        return CapabilityResult(
            content=content,
            blocks=blocks,
            meta={
                "subagent": "recommendation_subagent",
                "what_done": "Generated recommendation payload and narrative from current profile context.",
                "why_next": "User can refine constraints (tier/budget/major) for a tighter shortlist.",
                "personalization_evidence": evidence,
            },
        )

    async def _run_school_query(self, ctx: CapabilityContext) -> CapabilityResult:
        if ctx.student_id is None:
            return await self._run_general(ctx)
        ctx.shared["school_query:max_tokens_hint"] = 640
        content = await handle_school_query(
            ctx.llm,
            ctx.session,
            ctx.memory,
            ctx.student_id,
            ctx.message,
            max_tokens=640,
        )
        evidence = self._build_personalization_evidence(
            ctx=ctx,
            required_fields=["budget", "intended_majors"],
            confidence=0.76,
        )
        return CapabilityResult(
            content=content,
            blocks=[{"kind": "text", "payload": {"text": content}, "meta": {}}],
            meta={
                "what_done": "Returned school-specific facts and advisor interpretation.",
                "why_next": "Use this school context for comparison, what-if, or application strategy.",
                "personalization_evidence": evidence,
            },
        )

    async def _run_strategy(self, ctx: CapabilityContext) -> CapabilityResult:
        if ctx.student_id is None:
            return await self._run_general(ctx)
        ctx.shared["strategy:max_tokens_hint"] = 640
        content = await handle_strategy(
            ctx.llm,
            ctx.session,
            ctx.memory,
            ctx.student_id,
            ctx.message,
            max_tokens=640,
            per_tier_limit=6,
        )
        evidence = self._build_personalization_evidence(
            ctx=ctx,
            required_fields=["target_year", "ed_preference", "budget"],
            confidence=0.8,
        )
        return CapabilityResult(
            content=content,
            blocks=[{"kind": "text", "payload": {"text": content}, "meta": {}}],
            meta={
                "what_done": "Generated application strategy sequencing and timeline suggestions.",
                "why_next": "Confirm target schools and early-round preference to lock execution order.",
                "personalization_evidence": evidence,
            },
        )

    async def _run_offer_compare(self, ctx: CapabilityContext) -> CapabilityResult:
        if ctx.student_id is None:
            return await self._run_general(ctx)
        content = await handle_offer_decision(
            ctx.llm,
            ctx.session,
            ctx.memory,
            ctx.student_id,
            ctx.message,
        )
        structured = _parse_offer_compare_from_text(content)
        blocks: list[dict[str, Any]] = []
        if structured is not None:
            blocks.append(
                {
                    "kind": "offer_compare",
                    "payload": structured,
                    "meta": {},
                }
            )
        blocks.append({"kind": "text", "payload": {"text": content}, "meta": {}})
        evidence = self._build_personalization_evidence(
            ctx=ctx,
            required_fields=["budget", "financial_aid_type"],
            confidence=0.79,
        )
        return CapabilityResult(
            content=content,
            blocks=blocks,
            meta={
                "what_done": "Compared available offers across cost and outcome dimensions.",
                "why_next": "User can provide one stronger preference to break close ties.",
                "personalization_evidence": evidence,
            },
        )

    async def _run_what_if(self, ctx: CapabilityContext) -> CapabilityResult:
        if ctx.student_id is None:
            return await self._run_general(ctx)
        content = await handle_what_if(
            ctx.llm,
            ctx.session,
            ctx.memory,
            ctx.student_id,
            ctx.message,
        )
        structured = _parse_what_if_from_text(content)
        blocks: list[dict[str, Any]] = []
        if structured is not None:
            blocks.append(
                {
                    "kind": "what_if",
                    "payload": structured,
                    "meta": {},
                }
            )
        blocks.append({"kind": "text", "payload": {"text": content}, "meta": {}})
        evidence = self._build_personalization_evidence(
            ctx=ctx,
            required_fields=["gpa", "sat", "budget"],
            confidence=0.77,
        )
        return CapabilityResult(
            content=content,
            blocks=blocks,
            meta={
                "what_done": "Ran hypothetical scenario analysis and summarized delta impacts.",
                "why_next": "Pick one feasible intervention and re-run to narrow uncertainty.",
                "personalization_evidence": evidence,
            },
        )

    async def _run_emotional_support(self, ctx: CapabilityContext) -> CapabilityResult:
        response_lang = detect_response_language(ctx.message)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a warm, empathetic college admissions advisor. "
                    "The student is feeling stressed or anxious about admissions. "
                    "Acknowledge feelings and offer one concrete next action. "
                    f"{language_instruction(response_lang)}"
                ),
            },
            {"role": "user", "content": ctx.message},
        ]
        content = await ctx.llm.complete(
            messages,
            temperature=0.7,
            max_tokens=512,
            caller="chat.emotional_support",
        )
        evidence = self._build_personalization_evidence(
            ctx=ctx,
            required_fields=[],
            confidence=0.68,
        )
        return CapabilityResult(
            content=content,
            blocks=[{"kind": "text", "payload": {"text": content}, "meta": {}}],
            meta={
                "what_done": "Delivered emotional support and one concrete calming action.",
                "why_next": "Transition back to one actionable admissions task.",
                "personalization_evidence": evidence,
            },
        )

    async def _run_general(self, ctx: CapabilityContext) -> CapabilityResult:
        response_lang = detect_response_language(ctx.message)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are ScholarPath, a college admissions advisor chatbot. "
                    "Respond helpfully to general requests and redirect to admissions tasks when needed. "
                    f"{language_instruction(response_lang)}"
                ),
            },
            {"role": "user", "content": ctx.message},
        ]
        content = await ctx.llm.complete(
            messages,
            temperature=0.7,
            max_tokens=512,
            caller="chat.general",
        )
        evidence = self._build_personalization_evidence(
            ctx=ctx,
            required_fields=[],
            confidence=0.66,
        )
        return CapabilityResult(
            content=content,
            blocks=[{"kind": "text", "payload": {"text": content}, "meta": {}}],
            meta={
                "what_done": "Returned a general advisor response.",
                "why_next": "Ask one concrete admissions intent for higher-quality planning.",
                "personalization_evidence": evidence,
            },
        )

def _parse_money(raw: str) -> float | None:
    if not raw:
        return None
    if re.search(r"unknown|n/a|暂无|未知|—", raw, re.IGNORECASE):
        return None
    numeric = re.sub(r"[^0-9.\-]", "", raw)
    if not numeric:
        return None
    try:
        return float(numeric)
    except ValueError:
        return None


def _parse_percent(raw: str) -> float | None:
    if not raw:
        return None
    match = re.search(r"(-?\d+(?:\.\d+)?)\s*%", raw)
    if not match:
        return None
    try:
        return float(match.group(1)) / 100.0
    except ValueError:
        return None


def _parse_offer_compare_from_text(content: str) -> dict[str, Any] | None:
    if not re.search(
        r"(net cost|total aid|career outlook|academic fit|life satisfaction|offer|对比|比较)",
        content,
        re.IGNORECASE,
    ):
        return None

    summary = None
    summary_match = re.search(
        r"\*\*(?:My recommendation|Recommendation|建议|我的建议)\*\*:?\s*([\s\S]+)$",
        content,
        re.IGNORECASE,
    )
    compare_body = content.strip()
    if summary_match:
        summary = summary_match.group(1).strip()
        compare_body = content[: summary_match.start()].strip()

    school_pattern = re.compile(
        r"^\*\*(.+?)\*\*\s*([\s\S]*?)(?=^\*\*.+?\*\*\s*$|$)",
        re.MULTILINE,
    )
    schools: list[dict[str, Any]] = []
    for idx, match in enumerate(school_pattern.finditer(compare_body)):
        school_name = match.group(1).strip()
        block = match.group(2)
        net_cost_match = re.search(
            r"(?:Net cost|净成本|净费用)\s*:\s*([^\n|]+)",
            block,
            re.IGNORECASE,
        )
        total_aid_match = re.search(
            r"(?:Total aid|总资助)\s*:\s*([^\n|]+)",
            block,
            re.IGNORECASE,
        )
        career_match = re.search(
            r"(?:Career outlook|Career outcome|职业前景|职业结果)\s*:\s*([^\n|]+)",
            block,
            re.IGNORECASE,
        )
        academic_match = re.search(
            r"(?:Academic fit|Academic outcome|学术匹配|学术结果)\s*:\s*([^\n|]+)",
            block,
            re.IGNORECASE,
        )
        life_match = re.search(
            r"(?:Life satisfaction|Life fit|生活满意度|生活匹配)\s*:\s*([^\n|]+)",
            block,
            re.IGNORECASE,
        )
        metrics = {
            "net_cost": _parse_money(net_cost_match.group(1) if net_cost_match else ""),
            "total_aid": _parse_money(total_aid_match.group(1) if total_aid_match else ""),
            "career_outlook": _parse_percent(career_match.group(1) if career_match else ""),
            "academic_fit": _parse_percent(academic_match.group(1) if academic_match else ""),
            "life_satisfaction": _parse_percent(life_match.group(1) if life_match else ""),
        }
        if not any(value is not None for value in metrics.values()):
            continue
        schools.append(
            {
                "id": f"chat-offer-{idx}",
                "schoolName": school_name,
                "metrics": metrics,
            }
        )

    if len(schools) < 2:
        return None
    metric_order = [
        key
        for key in ["net_cost", "total_aid", "career_outlook", "academic_fit", "life_satisfaction"]
        if any(school["metrics"].get(key) is not None for school in schools)
    ]
    return {
        "source": "chat",
        "title": "Offer Comparison",
        "description": "Structured side-by-side comparison extracted from advisor output.",
        "summary": summary,
        "schools": schools,
        "metricOrder": metric_order,
    }


def _normalize_outcome_key(label: str) -> str:
    normalized = re.sub(r"[_\s-]+", " ", label.lower()).strip()
    if re.search(r"admission|录取", normalized):
        return "admission_probability"
    if re.search(r"academic|学术", normalized):
        return "academic_outcome"
    if re.search(r"career|职业|就业", normalized):
        return "career_outcome"
    if re.search(r"life|生活", normalized):
        return "life_satisfaction"
    if re.search(r"phd|博士", normalized):
        return "phd_probability"
    return normalized.replace(" ", "_")


def _parse_what_if_from_text(content: str) -> dict[str, Any] | None:
    if not re.search(r"simulation|what-if|scenario|模拟", content, re.IGNORECASE):
        return None

    pattern = re.compile(
        r"^-+\s*\*\*(.+?)\*\*\s*:\s*(\d+(?:\.\d+)?)%\s*(increase|decrease|提升|下降)",
        re.IGNORECASE | re.MULTILINE,
    )
    deltas: list[dict[str, Any]] = []
    for match in pattern.finditer(content):
        key = _normalize_outcome_key(match.group(1))
        value = float(match.group(2)) / 100.0
        if re.search(r"decrease|下降", match.group(3), re.IGNORECASE):
            value = -value
        deltas.append({"key": key, "value": value})

    if not deltas:
        return None
    explanation = pattern.sub("", content).strip()
    return {
        "title": "What-If Analysis",
        "deltas": deltas,
        "explanation": explanation,
        "suggestions": [],
    }
