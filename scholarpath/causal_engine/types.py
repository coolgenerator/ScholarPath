"""Shared causal runtime data structures."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class CausalRequestContext:
    """Inputs shared by estimate/intervention/explain calls."""

    context: str
    student_id: str
    school_id: str | None = None
    offer_id: str | None = None
    student_features: dict[str, float] = field(default_factory=dict)
    school_features: dict[str, float] = field(default_factory=dict)
    interaction_features: dict[str, float] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CausalEstimateResult:
    """Outcome scores and metadata from a causal estimate."""

    scores: dict[str, float]
    confidence: float
    engine_used: str
    model_version: str | None = None
    fallback_used: bool = False
    fallback_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CausalInterventionResult:
    """Result of a what-if intervention."""

    original_scores: dict[str, float]
    modified_scores: dict[str, float]
    deltas: dict[str, float]
    engine_used: str
    model_version: str | None = None
    fallback_used: bool = False
    fallback_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CausalExplainResult:
    """Textual explanation + machine-readable rationale payload."""

    explanation: str
    reasons: list[dict[str, Any]] = field(default_factory=list)
    engine_used: str = "legacy"
    model_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
