"""Pydantic schemas for school evaluations."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


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
    causal_engine_version: str | None = None
    causal_model_version: str | None = None
    estimate_confidence: float | None = None
    label_type: str | None = None
    fallback_used: bool | None = None

    @model_validator(mode="before")
    @classmethod
    def _inject_causal_fields(cls, data: Any) -> Any:
        if hasattr(data, "fit_details") and not isinstance(data, dict):
            obj = data
            details = obj.fit_details or {}
            meta = details.get("causal_metadata", {})
            return {
                "id": obj.id,
                "created_at": obj.created_at,
                "student_id": obj.student_id,
                "school_id": obj.school_id,
                "tier": obj.tier,
                "academic_fit": obj.academic_fit,
                "financial_fit": obj.financial_fit,
                "career_fit": obj.career_fit,
                "life_fit": obj.life_fit,
                "overall_score": obj.overall_score,
                "admission_probability": obj.admission_probability,
                "ed_ea_recommendation": obj.ed_ea_recommendation,
                "reasoning": obj.reasoning,
                "fit_details": details,
                "causal_engine_version": meta.get("causal_engine_version"),
                "causal_model_version": meta.get("causal_model_version"),
                "estimate_confidence": meta.get("estimate_confidence"),
                "label_type": meta.get("label_type"),
                "fallback_used": meta.get("fallback_used"),
            }
        return data


class TieredSchoolList(BaseModel):
    """Schools organised by admission tier."""

    reach: list[EvaluationResponse] = Field(default_factory=list)
    target: list[EvaluationResponse] = Field(default_factory=list)
    safety: list[EvaluationResponse] = Field(default_factory=list)
    likely: list[EvaluationResponse] = Field(default_factory=list)
