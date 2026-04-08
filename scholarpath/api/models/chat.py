"""Pydantic schemas for the chat interface."""

from __future__ import annotations

from datetime import datetime

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
    acceptance_rate_effective: float | None = None
    acceptance_rate_capped: bool | None = None
    net_price: int | None = None
    major_match: float | None = None
    geo_match: float | None = None
    major_match_evidence: bool | None = None
    tier_cap_triggered: bool | None = None
    key_reasons: list[str] = []
    sub_scores: dict[str, float] = {}  # academic, financial, career, life


class RecommendationData(BaseModel):
    """Full recommendation payload for rich rendering."""

    narrative: str
    schools: list[RecommendedSchool]
    ed_recommendation: str | None = None
    ea_recommendations: list[str] = []
    strategy_summary: str | None = None
    prefilter_meta: dict | None = None
    top_n_used: int | None = None
    skill_id_used: str | None = None
    scenario_validation: dict | None = None
    constraint_status: str | None = None
    constraint_fail_reasons: list[str] = []
    next_steps: list[str] = []
    deepsearch_pending: bool | None = None
    deepsearch_fallback_triggered: bool | None = None
    deepsearch_fallback_reasons: list[str] = []
    deepsearch_fallback_task_id: str | None = None
    deepsearch_debounced: bool | None = None
    deepsearch_enqueue_error: str | None = None


class RoutePlan(BaseModel):
    """Optional external route plan for route-turn endpoint."""

    primary_task: str
    modifiers: list[str] = []
    required_capabilities: list[str] = []
    required_outputs: list[str] = []
    route_lock: bool = True


class RouteTurnRequest(BaseModel):
    """Request payload for route-turn endpoint."""

    session_id: str
    message: str
    student_id: str | None = None
    route_plan: RoutePlan | None = None
    skill_id: str | None = None


class RouteMeta(BaseModel):
    route_source: str
    primary_task: str | None = None
    skill_id: str | None = None
    executed_capability: str | None = None


class ExecutionDigest(BaseModel):
    required_output_missing: bool = False
    required_capability_missing: bool = False
    forced_retry_count: int = 0
    cap_retry_count: int = 0
    cap_degraded: bool = False
    reason_code: str | None = None
    failure_reason_code: str | None = None
    needs_input: list[str] = []
    next_steps: list[str] = []


class ChatResponse(BaseModel):
    """Response from the chat agent."""

    content: str
    intent: str = Field(
        ...,
        description="Classified intent of the user message",
        examples=["school_search", "evaluation", "general"],
    )
    suggested_actions: list[str] | None = Field(
        None,
        description="Optional follow-up actions the user can take",
    )
    guided_questions: list[GuidedQuestion] | None = Field(
        None,
        description="Structured questions with selectable options for guided intake",
    )
    recommendation: RecommendationData | None = Field(
        None,
        description="Structured recommendation data for rich rendering",
    )
    route_meta: RouteMeta | None = Field(
        None,
        description="Optional route metadata for route-turn execution.",
    )
    execution_digest: ExecutionDigest | None = Field(
        None,
        description="Execution diagnostics for forced retry and graceful degradation.",
    )
