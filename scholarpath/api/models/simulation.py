"""Pydantic schemas for what-if simulations."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class WhatIfRequest(BaseModel):
    """Request body for a what-if simulation.

    ``interventions`` maps variable names to hypothetical values,
    e.g. {"scholarship": 15000, "major": "data_science"}.
    """

    interventions: dict[str, float | str] = Field(
        ...,
        min_length=1,
        examples=[{"scholarship": 15000, "major": "data_science"}],
    )


class WhatIfResponse(BaseModel):
    """Result of a single what-if simulation."""

    original_scores: dict[str, float]
    modified_scores: dict[str, float]
    deltas: dict[str, float]
    explanation: str


class ScenarioCompareRequest(BaseModel):
    """Request body for comparing multiple what-if scenarios."""

    scenarios: list[WhatIfRequest] = Field(..., min_length=2)


class ScenarioCompareResponse(BaseModel):
    """Side-by-side comparison of multiple scenarios."""

    results: list[WhatIfResponse]
    summary: str = Field(
        ..., description="Narrative summary comparing all scenarios"
    )
