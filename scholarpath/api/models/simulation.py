"""Pydantic schemas for what-if simulations."""

from __future__ import annotations

import math
import uuid
from typing import Any

from pydantic import BaseModel, Field, field_validator


class WhatIfRequest(BaseModel):
    """Request body for a what-if simulation.

    ``interventions`` maps variable names to hypothetical values,
    e.g. {"scholarship": 15000, "major": "data_science"}.
    """

    interventions: dict[str, float] = Field(
        ...,
        min_length=1,
        examples=[{"scholarship": 15000, "research_opportunities": 0.8}],
    )

    @field_validator("interventions")
    @classmethod
    def _validate_interventions(
        cls,
        value: dict[str, float],
    ) -> dict[str, float]:
        cleaned: dict[str, float] = {}
        for raw_key, raw_val in value.items():
            key = str(raw_key or "").strip()
            if not key:
                raise ValueError("intervention variable name cannot be empty")
            val = float(raw_val)
            if not math.isfinite(val):
                raise ValueError(f"intervention '{key}' must be a finite number")
            if abs(val) > 1_000_000:
                raise ValueError(f"intervention '{key}' is out of allowed range")
            cleaned[key] = val
        return cleaned


class WhatIfResponse(BaseModel):
    """Result of a single what-if simulation."""

    original_scores: dict[str, float]
    modified_scores: dict[str, float]
    deltas: dict[str, float]
    explanation: str
    causal_engine_version: str | None = None
    causal_model_version: str | None = None
    estimate_confidence: float | None = None
    label_type: str | None = None
    fallback_used: bool | None = None
    fallback_reason: str | None = None


class ScenarioRequest(BaseModel):
    """One scenario in a cross-school what-if comparison."""

    school_id: uuid.UUID
    interventions: dict[str, float] = Field(..., min_length=1)
    label: str | None = None

    @field_validator("interventions")
    @classmethod
    def _validate_scenario_interventions(
        cls,
        value: dict[str, float],
    ) -> dict[str, float]:
        return WhatIfRequest._validate_interventions(value)


class ScenarioCompareRequest(BaseModel):
    """Request body for comparing multiple scenarios (can span schools)."""

    scenarios: list[ScenarioRequest] = Field(..., min_length=2)


class ScenarioResult(WhatIfResponse):
    """One scenario result with scenario metadata."""

    school_id: uuid.UUID
    label: str


class ScenarioCompareResponse(BaseModel):
    """Side-by-side comparison of multiple scenarios."""

    results: list[ScenarioResult]
    summary: str = Field(
        ..., description="Narrative summary comparing all scenarios"
    )
