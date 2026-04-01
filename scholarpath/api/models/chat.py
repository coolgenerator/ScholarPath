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
    net_price: int | None = None
    key_reasons: list[str] = []
    sub_scores: dict[str, float] = {}  # academic, financial, career, life


class RecommendationData(BaseModel):
    """Full recommendation payload for rich rendering."""

    narrative: str
    schools: list[RecommendedSchool]
    ed_recommendation: str | None = None
    ea_recommendations: list[str] = []
    strategy_summary: str | None = None


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
