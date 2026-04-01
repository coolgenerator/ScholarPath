"""Pydantic schemas for student profiles."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class StudentCreate(BaseModel):
    """Schema for creating a new student profile."""

    name: str = Field(..., min_length=1, max_length=200)
    gpa: float = Field(..., ge=0)
    gpa_scale: str = Field(..., max_length=20, examples=["4.0", "5.0", "100"])
    sat_total: int | None = Field(None, ge=400, le=1600)
    act_composite: int | None = Field(None, ge=1, le=36)
    toefl_total: int | None = Field(None, ge=0, le=120)
    curriculum_type: str = Field(..., max_length=20, examples=["AP", "IB", "A-Level", "other"])
    ap_courses: list[str] | None = None
    extracurriculars: list[Any] | dict[str, Any] | None = None
    awards: list[Any] | dict[str, Any] | None = None
    intended_majors: list[str] = Field(..., min_length=1)
    budget_usd: int | None = Field(None, ge=0)
    need_financial_aid: bool | None = None
    preferences: dict[str, Any] | None = None
    ed_preference: str | None = Field(None, max_length=20, examples=["ed", "ea", "rea", "rd"])
    target_year: int = Field(..., ge=2024, le=2035)


class StudentUpdate(BaseModel):
    """Schema for updating a student profile. All fields optional."""

    name: str | None = Field(None, min_length=1, max_length=200)
    gpa: float | None = Field(None, ge=0)
    gpa_scale: str | None = Field(None, max_length=20)
    sat_total: int | None = Field(None, ge=400, le=1600)
    act_composite: int | None = Field(None, ge=1, le=36)
    toefl_total: int | None = Field(None, ge=0, le=120)
    curriculum_type: str | None = Field(None, max_length=20)
    ap_courses: list[str] | None = None
    extracurriculars: list[Any] | dict[str, Any] | None = None
    awards: list[Any] | dict[str, Any] | None = None
    intended_majors: list[str] | None = None
    budget_usd: int | None = Field(None, ge=0)
    need_financial_aid: bool | None = None
    preferences: dict[str, Any] | None = None
    ed_preference: str | None = Field(None, max_length=20)
    target_year: int | None = Field(None, ge=2024, le=2035)


class StudentResponse(BaseModel):
    """Schema for student profile responses."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    created_at: datetime
    name: str
    gpa: float
    gpa_scale: str
    sat_total: int | None = None
    act_composite: int | None = None
    toefl_total: int | None = None
    curriculum_type: str
    ap_courses: list[str] | None = None
    extracurriculars: list[Any] | dict[str, Any] | None = None
    awards: list[Any] | dict[str, Any] | None = None
    intended_majors: list[str] | None = None
    budget_usd: int
    need_financial_aid: bool
    preferences: dict[str, Any] | None = None
    ed_preference: str | None = None
    target_year: int
    profile_completed: bool
