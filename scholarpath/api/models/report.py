"""Pydantic schemas for Go/No-Go reports."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class GoNoGoResponse(BaseModel):
    """Schema for a Go/No-Go recommendation report."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: uuid.UUID
    created_at: datetime
    student_id: uuid.UUID
    offer_id: uuid.UUID

    overall_score: float
    ci_lower: float = Field(..., alias="confidence_lower")
    ci_upper: float = Field(..., alias="confidence_upper")

    sub_scores: dict[str, float] = Field(default_factory=dict)
    recommendation: str
    top_factors: list[Any]
    risks: list[Any]
    narrative: str
    what_if_results: dict[str, Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def _build_sub_scores(cls, data: Any) -> Any:
        """Populate ``sub_scores`` from the ORM model's individual score columns."""
        # When constructing from an ORM object, data is the object itself.
        if hasattr(data, "academic_score") and not isinstance(data, dict):
            obj = data
            sub = {
                "academic": obj.academic_score,
                "financial": obj.financial_score,
                "career": obj.career_score,
                "life": obj.life_score,
            }
            # Convert ORM to a dict so Pydantic can process it
            d = {
                "id": obj.id,
                "created_at": obj.created_at,
                "student_id": obj.student_id,
                "offer_id": obj.offer_id,
                "overall_score": obj.overall_score,
                "confidence_lower": obj.confidence_lower,
                "confidence_upper": obj.confidence_upper,
                "sub_scores": sub,
                "recommendation": obj.recommendation,
                "top_factors": obj.top_factors,
                "risks": obj.risks,
                "narrative": obj.narrative,
                "what_if_results": obj.what_if_results,
            }
            return d
        return data
