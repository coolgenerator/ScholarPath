"""Pydantic schemas for schools and programs."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ProgramResponse(BaseModel):
    """Schema for an academic program."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    school_id: uuid.UUID
    name: str
    department: str
    us_news_rank: int | None = None
    avg_class_size: int | None = None
    has_research_opps: bool
    has_coop: bool
    description: str | None = None


class SchoolResponse(BaseModel):
    """Schema for a school with full details."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    created_at: datetime
    name: str
    name_cn: str | None = None
    city: str
    state: str
    school_type: str
    size_category: str

    us_news_rank: int | None = None
    qs_world_rank: int | None = None
    forbes_rank: int | None = None
    acceptance_rate: float | None = None

    sat_25: int | None = None
    sat_75: int | None = None
    act_25: int | None = None
    act_75: int | None = None

    tuition_oos: int | None = None
    avg_net_price: int | None = None

    intl_student_pct: float | None = None
    student_faculty_ratio: float | None = None
    graduation_rate_4yr: float | None = None
    endowment_per_student: int | None = None

    campus_setting: str | None = None
    website_url: str | None = None

    programs: list[ProgramResponse] = []


class SchoolListResponse(BaseModel):
    """Paginated list of schools."""

    items: list[SchoolResponse]
    total: int
    page: int
    per_page: int


class SchoolSearchParams(BaseModel):
    """Query parameters for school search."""

    query: str | None = None
    state: str | None = None
    min_rank: int | None = Field(None, ge=1)
    max_rank: int | None = Field(None, ge=1)
    max_tuition: int | None = Field(None, ge=0)
    school_type: str | None = None
    page: int = Field(1, ge=1)
    per_page: int = Field(20, ge=1, le=100)
