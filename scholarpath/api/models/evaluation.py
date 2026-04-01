"""Pydantic schemas for school evaluations."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EvaluationRequest(BaseModel):
    """Request body for triggering an evaluation."""

    school_id: uuid.UUID


class EvaluationResponse(BaseModel):
    """Schema for a school evaluation result."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    created_at: datetime
    student_id: uuid.UUID
    school_id: uuid.UUID

    tier: str
    academic_fit: float
    financial_fit: float
    career_fit: float
    life_fit: float
    overall_score: float

    admission_probability: float | None = None
    ed_ea_recommendation: str | None = None

    reasoning: str
    fit_details: dict[str, Any] | None = None


class TieredSchoolList(BaseModel):
    """Schools organised by admission tier."""

    reach: list[EvaluationResponse] = Field(default_factory=list)
    target: list[EvaluationResponse] = Field(default_factory=list)
    safety: list[EvaluationResponse] = Field(default_factory=list)
    likely: list[EvaluationResponse] = Field(default_factory=list)
