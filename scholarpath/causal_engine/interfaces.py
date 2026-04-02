"""Engine interfaces for causal estimation and interventions."""

from __future__ import annotations

from abc import ABC, abstractmethod

from scholarpath.causal_engine.types import (
    CausalEstimateResult,
    CausalExplainResult,
    CausalInterventionResult,
    CausalRequestContext,
)


class CausalEngine(ABC):
    """Abstract contract implemented by all causal engines."""

    engine_version: str

    @abstractmethod
    async def estimate(
        self,
        ctx: CausalRequestContext,
        outcomes: list[str],
    ) -> CausalEstimateResult:
        """Estimate requested outcomes for a single context."""

    @abstractmethod
    async def intervene(
        self,
        ctx: CausalRequestContext,
        interventions: dict[str, float],
        outcomes: list[str],
    ) -> CausalInterventionResult:
        """Apply do-style interventions and return deltas."""

    @abstractmethod
    async def explain(
        self,
        ctx: CausalRequestContext,
        result: CausalEstimateResult,
    ) -> CausalExplainResult:
        """Return structured explanation text/factors."""
