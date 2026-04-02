"""Advisor v1 wire contracts and typed artifacts."""

from __future__ import annotations

from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BaseModel, Field

AdvisorDomain: TypeAlias = Literal["undergrad", "offer", "graduate", "summer", "common"]

AdvisorCapability: TypeAlias = Literal[
    "undergrad.profile.intake",
    "undergrad.school.recommend",
    "undergrad.school.query",
    "undergrad.strategy.plan",
    "offer.compare",
    "offer.decision",
    "offer.what_if",
    "graduate.program.recommend",
    "summer.program.recommend",
    "common.general",
    "common.emotional_support",
    "common.clarify",
]

AdvisorErrorCode: TypeAlias = Literal[
    "ROUTE_LOW_CONFIDENCE",
    "INVALID_INPUT",
    "CAPABILITY_FAILED",
    "DEPENDENCY_UNAVAILABLE",
]
AdvisorGuardResult: TypeAlias = Literal["pass", "clarify", "invalid_input"]
AdvisorGuardReason: TypeAlias = Literal[
    "low_confidence",
    "conflict",
    "invalid_input",
    "trigger_invalid",
    "none",
]

AdvisorStepStatus: TypeAlias = Literal["succeeded", "degraded", "failed"]
PendingReason: TypeAlias = Literal[
    "over_limit",
    "conflict",
    "low_confidence",
    "requires_user_trigger",
    "dependency_wait",
]
MemoryScope: TypeAlias = Literal["session", "domain", "user"]
MemoryItemType: TypeAlias = Literal[
    "fact",
    "preference",
    "constraint",
    "decision",
    "plan",
    "queue_step",
    "emotion_signal",
]
MemoryItemStatus: TypeAlias = Literal[
    "active",
    "pending_conflict",
    "superseded",
    "expired",
    "deleted",
]


class AdvisorEditPayload(BaseModel):
    """Optional edit request for overwriting one prior user turn."""

    target_turn_id: str
    mode: Literal["overwrite"] = "overwrite"


class AdvisorRequest(BaseModel):
    """Inbound advisor turn payload."""

    turn_id: str | None = None
    session_id: str
    student_id: str | None = None
    message: str
    locale: str | None = None
    domain_hint: AdvisorDomain | None = None
    capability_hint: str | None = None
    client_context: dict[str, Any] | None = None
    edit: AdvisorEditPayload | None = None


class QuestionOption(BaseModel):
    """A single selectable option in a guided question."""

    label: str
    value: str
    icon: str | None = None


class GuidedQuestion(BaseModel):
    """A structured question with selectable options."""

    id: str
    title: str
    description: str | None = None
    options: list[QuestionOption] = []
    allow_custom: bool = True
    custom_placeholder: str = ""
    multi_select: bool = False


class RecommendedSchool(BaseModel):
    """A single school recommendation."""

    school_name: str
    school_name_cn: str | None = None
    tier: str
    rank: int | None = None
    overall_score: float
    admission_probability: float
    acceptance_rate: float | None = None
    net_price: int | None = None
    key_reasons: list[str] = []
    sub_scores: dict[str, float] = {}


class RecommendationData(BaseModel):
    """Recommendation payload for rich rendering."""

    narrative: str
    schools: list[RecommendedSchool]
    ed_recommendation: str | None = None
    ea_recommendations: list[str] = []
    strategy_summary: str | None = None


class GuidedIntakeArtifact(BaseModel):
    """Structured guided-intake questions for card rendering."""

    type: Literal["guided_intake"] = "guided_intake"
    questions: list[GuidedQuestion] = Field(default_factory=list)


class SchoolRecommendationArtifact(BaseModel):
    """Structured recommendation card payload."""

    type: Literal["school_recommendation"] = "school_recommendation"
    data: RecommendationData


class OfferComparisonArtifact(BaseModel):
    """Structured offer comparison artifact."""

    type: Literal["offer_comparison"] = "offer_comparison"
    offers: list[dict[str, Any]] = Field(default_factory=list)
    comparison_matrix: dict[str, dict[str, Any]] = Field(default_factory=dict)
    recommendation: str | None = None


class StrategyPlanArtifact(BaseModel):
    """Structured strategy artifact."""

    type: Literal["strategy_plan"] = "strategy_plan"
    strategy: dict[str, Any] = Field(default_factory=dict)


class WhatIfResultArtifact(BaseModel):
    """Structured what-if simulation artifact."""

    type: Literal["what_if_result"] = "what_if_result"
    interventions: dict[str, float] = Field(default_factory=dict)
    deltas: dict[str, float] = Field(default_factory=dict)
    explanation: str | None = None


class InfoCardArtifact(BaseModel):
    """Generic informational card artifact."""

    type: Literal["info_card"] = "info_card"
    title: str
    summary: str
    data: dict[str, Any] = Field(default_factory=dict)


AdvisorArtifact: TypeAlias = Annotated[
    (
        GuidedIntakeArtifact
        | SchoolRecommendationArtifact
        | OfferComparisonArtifact
        | StrategyPlanArtifact
        | WhatIfResultArtifact
        | InfoCardArtifact
    ),
    Field(discriminator="type"),
]


class AdvisorAction(BaseModel):
    """Client-invokable action button."""

    action_id: str
    label: str
    payload: dict[str, Any] = Field(default_factory=dict)


class DoneStep(BaseModel):
    """One executed capability step in this turn."""

    capability: AdvisorCapability
    status: AdvisorStepStatus
    message: str | None = None
    retry_count: int = 0


class PendingStep(BaseModel):
    """One deferred capability step."""

    capability: AdvisorCapability
    reason: PendingReason
    message: str | None = None


class AdvisorRouteMeta(BaseModel):
    """Routing and observability metadata."""

    domain_confidence: float = 0.0
    capability_confidence: float = 0.0
    router_model: str
    latency_ms: int
    fallback_used: bool = False
    context_tokens: int = 0
    memory_hits: int = 0
    rag_hits: int = 0
    rag_latency_ms: int = 0
    memory_degraded: bool = False
    guard_result: AdvisorGuardResult = "pass"
    guard_reason: AdvisorGuardReason = "none"
    primary_capability: AdvisorCapability | None = None
    executed_count: int = 0
    pending_count: int = 0
    planner_ms: int = 0
    route_context_ms: int = 0
    execution_context_ms: int = 0
    capability_exec_ms: int = 0
    llm_calls: int = 0


class AdvisorError(BaseModel):
    """Normalized advisor error payload."""

    code: AdvisorErrorCode
    message: str
    retriable: bool = False
    detail: dict[str, Any] | None = None


class AdvisorResponse(BaseModel):
    """Outbound advisor turn payload."""

    turn_id: str
    domain: AdvisorDomain
    capability: AdvisorCapability
    assistant_text: str
    artifacts: list[AdvisorArtifact] = Field(default_factory=list)
    actions: list[AdvisorAction] = Field(default_factory=list)
    done: list[DoneStep] = Field(default_factory=list)
    pending: list[PendingStep] = Field(default_factory=list)
    next_actions: list[AdvisorAction] = Field(default_factory=list)
    route_meta: AdvisorRouteMeta
    error: AdvisorError | None = None


class AdvisorHistoryEntry(BaseModel):
    """Chat history entry for advisor sessions."""

    role: str
    content: str
    message_id: str | None = None
    turn_id: str | None = None
    created_at: str | None = None
    editable: bool = False
    edited: bool = False


class MemoryIngestEvent(BaseModel):
    """Internal event payload for async advisor memory ingestion."""

    turn_id: str
    session_id: str
    student_id: str | None = None
    domain: AdvisorDomain
    capability: AdvisorCapability
    role: Literal["user", "assistant"]
    content: str
    artifacts: list[dict[str, Any]] = Field(default_factory=list)
    done: list[DoneStep] = Field(default_factory=list)
    pending: list[PendingStep] = Field(default_factory=list)
    next_actions: list[AdvisorAction] = Field(default_factory=list)


class MemoryItem(BaseModel):
    """Internal structured memory row payload."""

    scope: MemoryScope
    type: MemoryItemType
    key: str
    value: dict[str, Any]
    confidence: float
    status: MemoryItemStatus = "active"
    session_id: str | None = None
    student_id: str | None = None
    domain: AdvisorDomain | None = None
    source_turn_id: str
    expires_at: str | None = None


class RetrievedChunk(BaseModel):
    """Internal retrieval hit payload for context assembly."""

    chunk_id: str
    text: str
    score: float
    source: str
    session_id: str
    domain: AdvisorDomain | None = None
