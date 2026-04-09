"""Pydantic schemas for the chat interface."""

from __future__ import annotations

from datetime import datetime
import uuid
from typing import Any, Literal

from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    """A single chat message."""

    role: str = Field(..., examples=["user", "assistant", "system"])
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class QuestionOption(BaseModel):
    """A single selectable option in a guided question."""

    label: str
    value: str
    icon: str | None = None  # material icon name


class GuidedQuestion(BaseModel):
    """A structured question with selectable options."""

    id: str
    title: str
    description: str | None = None
    options: list[QuestionOption] = []
    allow_custom: bool = True  # show text input for custom answer
    custom_placeholder: str = ""
    multi_select: bool = False  # allow multiple selections


class RecommendedSchool(BaseModel):
    """A single school recommendation with scores and reasons."""

    school_name: str
    school_name_cn: str | None = None
    tier: str  # reach/target/safety/likely
    rank: int | None = None
    overall_score: float
    admission_probability: float
    acceptance_rate: float | None = None
    net_price: int | None = None
    key_reasons: list[str] = []
    sub_scores: dict[str, float] = {}  # academic, financial, career, life
    prefilter_tag: str | None = None
    is_stretch: bool = False
    rank_delta: int | None = None


class RecommendationData(BaseModel):
    """Full recommendation payload for rich rendering."""

    narrative: str
    schools: list[RecommendedSchool]
    ed_recommendation: str | None = None
    ea_recommendations: list[str] = []
    strategy_summary: str | None = None
    prefilter_meta: dict[str, Any] | None = None
    scenario_pack: dict[str, Any] | None = None


class ChatBlock(BaseModel):
    """Structured rich-render block produced by a capability execution."""

    id: str
    kind: Literal[
        "answer_synthesis",
        "recommendation",
        "offer_compare",
        "what_if",
        "guided_questions",
        "disambiguation",
        "profile_snapshot",
        "profile_patch_proposal",
        "profile_patch_result",
        "text",
        "error",
    ]
    capability_id: str
    order: int = Field(..., ge=0)
    payload: dict[str, Any] = Field(default_factory=dict)
    meta: dict[str, Any] | None = None


class TurnEvent(BaseModel):
    """Streaming execution event emitted during one assistant turn."""

    type: Literal["turn.event"] = "turn.event"
    trace_id: str
    event: Literal[
        "turn_started",
        "planning_done",
        "capability_started",
        "capability_finished",
        "rollback",
        "turn_completed",
    ]
    data: dict[str, Any] | None = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class TurnResult(BaseModel):
    """Final turn payload sent after orchestration completes."""

    type: Literal["turn.result"] = "turn.result"
    trace_id: str
    status: Literal["ok", "error"]
    content: str
    blocks: list[ChatBlock] = Field(default_factory=list)
    actions: list[str] = Field(default_factory=list)
    execution_digest: dict[str, Any] | None = None
    usage: dict[str, Any] = Field(default_factory=dict)


class RoutePlan(BaseModel):
    """Externally supplied routing contract for one chat turn."""

    primary_task: Literal[
        "chat",
        "recommendation",
        "strategy",
        "what_if",
        "offer_compare",
        "intake",
    ]
    modifiers: list[str] = Field(default_factory=list)
    required_capabilities: list[str] = Field(default_factory=list)
    required_outputs: list[str] = Field(default_factory=list)
    route_lock: bool = True


class RouteTurnRequest(BaseModel):
    """HTTP request payload for orchestrated chat turn with optional route plan."""

    session_id: str
    student_id: uuid.UUID | None = None
    message: str
    route_plan: RoutePlan | None = None


class TurnTraceStep(BaseModel):
    """One normalized execution step in a turn trace."""

    trace_id: str
    event: Literal[
        "turn_started",
        "planning_done",
        "capability_started",
        "capability_finished",
        "rollback",
        "turn_completed",
    ]
    timestamp: datetime
    step_id: str
    parent_step_id: str | None = None
    step_kind: Literal["turn", "wave", "checkpoint", "capability", "rollback"] | None = None
    step_status: Literal[
        "queued",
        "running",
        "completed",
        "failed",
        "blocked",
        "cancelled",
        "timeout",
        "retrying",
    ] | None = None
    phase: str | None = None
    wave_index: int | None = None
    capability_id: str | None = None
    duration_ms: int | None = None
    checkpoint_summary: dict[str, Any] | None = None
    compact_reason_code: str | None = None
    event_seq: int | None = None
    display: dict[str, Any] | None = None
    metrics: dict[str, Any] | None = None
    data: dict[str, Any] | None = None


class TurnTraceSummary(BaseModel):
    """Session-level summary row for one turn trace."""

    trace_id: str
    session_id: str
    student_id: str | None = None
    status: Literal["running", "ok", "error"] = "running"
    started_at: datetime
    ended_at: datetime | None = None
    usage: dict[str, Any] = Field(default_factory=dict)
    step_count: int = 0


class TurnTraceResponse(BaseModel):
    """Full trace payload with all normalized steps."""

    trace_id: str
    session_id: str
    student_id: str | None = None
    status: Literal["running", "ok", "error"] = "running"
    started_at: datetime
    ended_at: datetime | None = None
    usage: dict[str, Any] = Field(default_factory=dict)
    steps: list[TurnTraceStep] = Field(default_factory=list)
    step_count: int = 0


class SessionTraceListResponse(BaseModel):
    """Paginated list response for recent session traces."""

    items: list[TurnTraceSummary] = Field(default_factory=list)
    total: int = 0


class ChatHistoryEntry(BaseModel):
    """Stored turn/message entry used by the history endpoint."""

    role: str
    content: str
    status: Literal["ok", "error"] | None = None
    trace_id: str | None = None
    blocks: list[ChatBlock] | None = None
    actions: list[str] | None = None
    execution_digest: dict[str, Any] | None = None
