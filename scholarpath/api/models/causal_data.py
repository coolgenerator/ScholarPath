"""Schemas for causal data ingestion and dataset registry APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class AdmissionEvidenceCreate(BaseModel):
    school_id: UUID | None = None
    cycle_year: int | None = None
    source_name: str = Field(min_length=1, max_length=80)
    source_type: str = Field(default="user_upload", min_length=1, max_length=30)
    source_url: str | None = None
    content_text: str | None = None
    metadata: dict[str, Any] | None = None


class AdmissionEvidenceResponse(BaseModel):
    id: UUID
    student_id: UUID | None
    school_id: UUID | None
    cycle_year: int | None
    source_name: str
    source_type: str
    source_url: str | None
    source_hash: str | None
    captured_at: datetime
    redaction_status: str
    metadata: dict[str, Any] | None = None


class AdmissionEventCreate(BaseModel):
    school_id: UUID
    cycle_year: int
    major_bucket: str | None = Field(default=None, max_length=100)
    stage: Literal[
        "submitted",
        "interview",
        "waitlist",
        "deferred",
        "admit",
        "reject",
        "declined",
        "commit",
    ]
    happened_at: datetime | None = None
    evidence_ref: UUID | None = None
    source_name: str = Field(default="manual", min_length=1, max_length=80)
    metadata: dict[str, Any] | None = None


class AdmissionEventResponse(BaseModel):
    id: UUID
    student_id: UUID
    school_id: UUID
    cycle_year: int
    major_bucket: str | None
    stage: str
    happened_at: datetime
    evidence_ref: UUID | None
    source_name: str
    metadata: dict[str, Any] | None = None


class CausalDatasetVersionResponse(BaseModel):
    version: str
    status: str
    config_json: dict[str, Any]
    stats_json: dict[str, Any] | None
    truth_ratio_by_outcome: dict[str, Any] | None
    training_window_start: datetime | None
    training_window_end: datetime | None
    mini_gate_passed: bool
    created_at: datetime
    updated_at: datetime
