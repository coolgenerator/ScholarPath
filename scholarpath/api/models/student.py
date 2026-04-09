"""Pydantic schemas for student profiles."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class StudentCreate(BaseModel):
    """Schema for creating a new student profile."""

    name: str = Field(..., min_length=1, max_length=200)
    gpa: float | None = Field(None, ge=0)
    gpa_scale: str | None = Field(None, max_length=20, examples=["4.0", "5.0", "100"])
    sat_total: int | None = Field(None, ge=400, le=1600)
    act_composite: int | None = Field(None, ge=1, le=36)
    toefl_total: int | None = Field(None, ge=0, le=120)
    curriculum_type: str | None = Field(None, max_length=20, examples=["AP", "IB", "A-Level", "other"])
    ap_courses: list[str] | None = None
    extracurriculars: list[Any] | dict[str, Any] | None = None
    awards: list[Any] | dict[str, Any] | None = None
    intended_majors: list[str] = Field(..., min_length=1)
    citizenship: str | None = Field(None, description="ISO country code", examples=["CN", "US", "IN", "KR"])
    residency_state: str | None = Field(None, max_length=10, examples=["CA", "NY"])
    budget_usd: int | None = Field(None, ge=0)
    need_financial_aid: bool | None = None
    preferences: dict[str, Any] | None = None
    ed_preference: str | None = Field(None, max_length=20, examples=["ed", "ea", "rea", "rd"])
    degree_level: str | None = Field(None, pattern="^(undergraduate|masters|phd)$")
    target_year: int | None = Field(None, ge=2024, le=2035)


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
    citizenship: str | None = Field(None, description="ISO country code", examples=["CN", "US", "IN", "KR"])
    residency_state: str | None = Field(None, max_length=10)
    budget_usd: int | None = Field(None, ge=0)
    need_financial_aid: bool | None = None
    preferences: dict[str, Any] | None = None
    degree_level: str | None = Field(None, pattern="^(undergraduate|masters|phd)$")
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
    citizenship: str | None = None
    residency_state: str | None = None
    budget_usd: int
    need_financial_aid: bool
    preferences: dict[str, Any] | None = None
    degree_level: str = "undergraduate"
    ed_preference: str | None = None
    target_year: int
    profile_completed: bool


class PortfolioIdentityPatch(BaseModel):
    """Patch payload for identity fields."""

    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(None, min_length=1, max_length=200)
    degree_level: str | None = Field(None, pattern="^(undergraduate|masters|phd)$")
    target_year: int | None = Field(None, ge=2024, le=2035)


class PortfolioAcademicsPatch(BaseModel):
    """Patch payload for academic fields."""

    model_config = ConfigDict(extra="forbid")

    gpa: float | None = Field(None, ge=0)
    gpa_scale: str | None = Field(None, max_length=20)
    sat_total: int | None = Field(None, ge=400, le=1600)
    act_composite: int | None = Field(None, ge=1, le=36)
    toefl_total: int | None = Field(None, ge=0, le=120)
    curriculum_type: str | None = Field(None, max_length=20)
    ap_courses: list[str] | None = None
    intended_majors: list[str] | None = None


class PortfolioActivitiesPatch(BaseModel):
    """Patch payload for activities and honors."""

    model_config = ConfigDict(extra="forbid")

    extracurriculars: list[Any] | dict[str, Any] | None = None
    awards: list[Any] | dict[str, Any] | None = None


class PortfolioFinancePatch(BaseModel):
    """Patch payload for financial fields."""

    model_config = ConfigDict(extra="forbid")

    citizenship: str | None = Field(None, description="ISO country code", examples=["CN", "US", "IN", "KR"])
    residency_state: str | None = Field(None, max_length=10)
    budget_usd: int | None = Field(None, ge=0)
    need_financial_aid: bool | None = None


class PortfolioStrategyPatch(BaseModel):
    """Patch payload for strategy fields."""

    model_config = ConfigDict(extra="forbid")

    ed_preference: str | None = Field(None, max_length=20)


class PortfolioPreferencesPatch(BaseModel):
    """Patch payload for canonical preference keys."""

    model_config = ConfigDict(extra="forbid")

    interests: list[str] | None = None
    risk_preference: str | None = None
    cost_priority: str | None = None
    location: list[str] | None = None
    size: list[str] | None = None
    culture: list[str] | None = None
    career_goal: str | None = None
    research_vs_teaching: str | None = None
    target_schools: list[str] | None = None
    financial_aid_type: str | None = None
    ui_preference_tags: list[str] | None = None
    application_level: str | None = None      # undergrad / graduate
    application_stage: str | None = None       # researching / applying / admitted


class StudentPortfolioPatch(BaseModel):
    """Grouped patch contract for student portfolio updates."""

    model_config = ConfigDict(extra="forbid")

    identity: PortfolioIdentityPatch | None = None
    academics: PortfolioAcademicsPatch | None = None
    activities: PortfolioActivitiesPatch | None = None
    finance: PortfolioFinancePatch | None = None
    strategy: PortfolioStrategyPatch | None = None
    preferences: PortfolioPreferencesPatch | None = None


class PortfolioIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    degree_level: str = "undergraduate"
    target_year: int


class PortfolioAcademics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    gpa: float
    gpa_scale: str
    sat_total: int | None = None
    act_composite: int | None = None
    toefl_total: int | None = None
    curriculum_type: str
    ap_courses: list[str] | None = None
    intended_majors: list[str] | None = None


class PortfolioActivities(BaseModel):
    model_config = ConfigDict(extra="forbid")

    extracurriculars: list[Any] | dict[str, Any] | None = None
    awards: list[Any] | dict[str, Any] | None = None


class PortfolioFinance(BaseModel):
    model_config = ConfigDict(extra="forbid")

    citizenship: str | None = None
    residency_state: str | None = None
    budget_usd: int
    need_financial_aid: bool


class PortfolioStrategy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ed_preference: str | None = None


class PortfolioPreferences(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interests: list[str] | None = None
    risk_preference: str | None = None
    cost_priority: str | None = None
    location: list[str] | None = None
    size: list[str] | None = None
    culture: list[str] | None = None
    career_goal: str | None = None
    research_vs_teaching: str | None = None
    target_schools: list[str] | None = None
    financial_aid_type: str | None = None
    ui_preference_tags: list[str] | None = None
    application_level: str | None = None      # undergrad / graduate
    application_stage: str | None = None       # researching / applying / admitted


class PortfolioCompletion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    profile_completed: bool
    completion_pct: float
    missing_fields: list[str]


class StudentPortfolioResponse(BaseModel):
    """Strongly-typed grouped portfolio response contract."""

    model_config = ConfigDict(extra="forbid")

    student_id: uuid.UUID
    identity: PortfolioIdentity
    academics: PortfolioAcademics
    activities: PortfolioActivities
    finance: PortfolioFinance
    strategy: PortfolioStrategy
    preferences: PortfolioPreferences
    completion: PortfolioCompletion
