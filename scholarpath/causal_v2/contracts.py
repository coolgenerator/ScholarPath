"""Contracts for the standalone causal inference V2 module.

The V2 package is intentionally not wired into the production service
routes yet. These contracts define a stable surface so V2 can replace
the current engine by switching one integration seam in the future.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(slots=True)
class CausalStudentProfile:
    """Normalized student profile used by Causal V2."""

    gpa: float | None = None
    sat: int | None = None
    family_income: float | None = None


@dataclass(slots=True)
class CausalSchoolProfile:
    """Normalized school profile used by Causal V2."""

    acceptance_rate: float | None = None
    research_expenditure: float | None = None
    avg_aid: float | None = None
    location_tier: float | None = None
    career_services_rating: float | None = None


@dataclass(slots=True)
class CausalDimensionScores:
    """Aggregated dimensions used by product-level recommendation logic."""

    academic: float
    financial: float
    career: float
    life: float
    overall: float

    def as_dict(self) -> dict[str, float]:
        return {
            "academic": self.academic,
            "financial": self.financial,
            "career": self.career,
            "life": self.life,
            "overall": self.overall,
        }


@dataclass(slots=True)
class CausalOutcomeScores:
    """Raw causal outcome nodes from the DAG."""

    admission_probability: float
    academic_outcome: float
    career_outcome: float
    phd_probability: float
    life_satisfaction: float
    financial_stress: float

    def as_dict(self) -> dict[str, float]:
        return {
            "admission_probability": self.admission_probability,
            "academic_outcome": self.academic_outcome,
            "career_outcome": self.career_outcome,
            "phd_probability": self.phd_probability,
            "life_satisfaction": self.life_satisfaction,
            "financial_stress": self.financial_stress,
        }


@dataclass(slots=True)
class CausalEvaluationResult:
    """Evaluation output produced by Causal V2."""

    dimensions: CausalDimensionScores
    outcomes: CausalOutcomeScores
    tier: str
    confidence_interval: dict[str, dict[str, float]] | None = None


@dataclass(slots=True)
class CausalWhatIfResult:
    """Single scenario what-if output."""

    original_scores: dict[str, float]
    modified_scores: dict[str, float]
    deltas: dict[str, float]


@dataclass(slots=True)
class CausalScenarioInput:
    """Scenario input with explicit school profile (no implicit defaults)."""

    school_profile: CausalSchoolProfile
    interventions: Mapping[str, float]
    label: str | None = None


@dataclass(slots=True)
class CausalScenarioResult:
    """Scenario result with deterministic ranking signal."""

    label: str
    school_profile: CausalSchoolProfile
    what_if: CausalWhatIfResult
    delta_score: float


@dataclass(slots=True)
class CausalScenarioComparisonResult:
    """Side-by-side scenario comparison output."""

    scenarios: list[CausalScenarioResult]
    best_scenario_label: str
    summary: str

