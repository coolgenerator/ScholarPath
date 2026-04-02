"""Core datatypes shared by advisor orchestration components."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.advisor.contracts import (
    AdvisorAction,
    AdvisorCapability,
    AdvisorDomain,
    DoneStep,
    PendingStep,
)
from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient


@dataclass(slots=True)
class CapabilityContext:
    """Runtime context passed to capability handlers."""

    turn_id: str
    session_id: str
    student_id: uuid.UUID | None
    message: str
    locale: str | None
    domain: AdvisorDomain
    capability: AdvisorCapability
    client_context: dict[str, Any]
    llm: LLMClient
    session: AsyncSession
    memory: ChatMemory
    conversation_context: dict[str, Any]


@dataclass(slots=True)
class CapabilityResult:
    """Normalized output from a capability handler."""

    assistant_text: str
    artifacts: list[Any] = field(default_factory=list)
    actions: list[AdvisorAction] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    step_summary: dict[str, Any] = field(default_factory=dict)


CapabilityHandler = Callable[[CapabilityContext], Awaitable[CapabilityResult]]


@dataclass(slots=True)
class CapabilityDefinition:
    """Capability registry row."""

    capability_id: AdvisorCapability
    domain: AdvisorDomain
    description: str
    handler: CapabilityHandler
    requires_student: bool = False
    produces_artifacts: tuple[str, ...] = ()


@dataclass(slots=True)
class IntentCandidate:
    """One routed intent candidate."""

    capability: AdvisorCapability
    confidence: float
    conflict_group: str
    source: str = "llm"


@dataclass(slots=True)
class RouteDecision:
    """Planner output consumed by clarify gate and coordinator."""

    domain: AdvisorDomain
    candidates: list[IntentCandidate]
    primary: IntentCandidate | None
    domain_confidence: float
    capability_confidence: float
    intent_clarity: float
    unresolved_conflict: bool
    ambiguous_expression: bool
    explicit_run: bool = False
    explicit_definition: CapabilityDefinition | None = None
    llm_calls: int = 0


@dataclass(slots=True)
class GuardDecision:
    """Clarify gate outcome."""

    result: Literal["pass", "clarify", "invalid_input"]
    reason: Literal["low_confidence", "conflict", "invalid_input", "trigger_invalid", "none"]
    message: str | None = None
    requested_definition: CapabilityDefinition | None = None


@dataclass(slots=True)
class ExecutionResult:
    """Executor output per turn."""

    done_steps: list[DoneStep]
    artifacts: list[Any]
    compatibility_actions: list[AdvisorAction]
    step_outputs: list[str]
    failed_for_retry: list[PendingStep]
    llm_calls: int = 0


@dataclass(slots=True)
class RouteDiagnostics:
    """Per-turn latency and call-count diagnostics for route_meta."""

    planner_ms: int = 0
    route_context_ms: int = 0
    execution_context_ms: int = 0
    capability_exec_ms: int = 0
    llm_calls: int = 0
