"""Causal engine protocol used by runtime and services."""

from __future__ import annotations

from typing import Protocol

from scholarpath.causal_engine.types import (
    CausalEstimateResult,
    CausalExplainResult,
    CausalInterventionResult,
    CausalRequestContext,
)


class CausalEngine(Protocol):
    """Engine contract for causal estimate/intervene/explain."""

    engine_name: str

    async def estimate(self, request: CausalRequestContext) -> CausalEstimateResult:
        """Return causal outcome estimates for the request context."""

    async def intervene(
        self,
        request: CausalRequestContext,
        interventions: dict[str, float],
    ) -> CausalInterventionResult:
        """Return post-intervention deltas."""

    async def explain(self, request: CausalRequestContext) -> CausalExplainResult:
        """Return user-facing explanation for the estimate."""
