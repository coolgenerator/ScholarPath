"""Pydantic schemas for multi-school comparison reports."""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, Field


class CompareReportRequest(BaseModel):
    """Request body for generating a multi-school comparison report."""

    school_ids: list[uuid.UUID] = Field(..., min_length=2, max_length=4)
    orientations: list[str] | None = Field(
        None,
        description="Career orientations to include. Defaults to all 7.",
    )


class OrientationLayerDetail(BaseModel):
    value: float
    confidence: float
    signals: dict[str, Any]


class SchoolOrientationScore(BaseModel):
    school_id: uuid.UUID
    school_name: str
    score: float
    l1: OrientationLayerDetail
    l2: OrientationLayerDetail
    l3: OrientationLayerDetail


class OrientationComparison(BaseModel):
    orientation: str
    schools: list[SchoolOrientationScore]
    narrative: str


class CausalFactorNode(BaseModel):
    id: str
    label: str
    layer: str  # "l3_environment" | "l2_school" | "l1_outcome"
    values: dict[str, float]  # {school_id: normalized_value}


class CausalFactorEdge(BaseModel):
    source: str
    target: str
    strength: float
    mechanism: str


class OrientationCausalGraph(BaseModel):
    orientation: str
    nodes: list[CausalFactorNode]
    edges: list[CausalFactorEdge]


class CompareReportResponse(BaseModel):
    student_id: uuid.UUID
    school_ids: list[uuid.UUID]
    orientations: list[OrientationComparison]
    causal_graphs: list[OrientationCausalGraph]
    recommendation: str
    confidence: float
