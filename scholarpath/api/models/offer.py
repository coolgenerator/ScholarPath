"""Pydantic schemas for admission offers."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class OfferCreate(BaseModel):
    """Schema for recording a new admission offer."""

    school_id: uuid.UUID
    status: str = Field(..., examples=["admitted", "waitlisted", "denied", "deferred"])

    # Cost of Attendance (user-reported)
    tuition: int | None = None
    room_and_board: int | None = None
    books_supplies: int | None = None
    personal_expenses: int | None = None
    transportation: int | None = None

    # Financial aid
    merit_scholarship: int = Field(0, ge=0)
    need_based_grant: int = Field(0, ge=0)
    loan_offered: int = Field(0, ge=0)
    work_study: int = Field(0, ge=0)

    honors_program: bool = False
    conditions: str | None = None
    decision_deadline: date | None = None
    notes: str | None = None


class OfferUpdate(BaseModel):
    """Schema for updating an existing offer. All fields optional."""

    status: str | None = None

    tuition: int | None = None
    room_and_board: int | None = None
    books_supplies: int | None = None
    personal_expenses: int | None = None
    transportation: int | None = None

    merit_scholarship: int | None = Field(None, ge=0)
    need_based_grant: int | None = Field(None, ge=0)
    loan_offered: int | None = Field(None, ge=0)
    work_study: int | None = Field(None, ge=0)

    honors_program: bool | None = None
    conditions: str | None = None
    decision_deadline: date | None = None
    notes: str | None = None


class OfferResponse(BaseModel):
    """Schema for an offer response."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    created_at: datetime
    student_id: uuid.UUID
    school_id: uuid.UUID
    school_name: str | None = None

    status: str

    # Cost of Attendance
    tuition: int | None = None
    room_and_board: int | None = None
    books_supplies: int | None = None
    personal_expenses: int | None = None
    transportation: int | None = None
    total_cost: int | None = None

    # Financial aid
    merit_scholarship: int
    need_based_grant: int
    loan_offered: int
    work_study: int = 0
    total_aid: int
    net_cost: int | None = None

    honors_program: bool
    conditions: str | None = None
    decision_deadline: date | None = None
    notes: str | None = None


class OfferComparisonResponse(BaseModel):
    """Side-by-side comparison of admitted offers."""

    offers: list[OfferResponse]
    comparison_scores: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Per-offer scoring breakdown for side-by-side comparison",
    )
