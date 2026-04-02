"""Shared request/response contracts for causal engines."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class CausalRequestContext:
    """Normalized input context for causal inference calls."""

    request_id: str
    context: str
    student_id: uuid.UUID
    school_id: uuid.UUID | None
    offer_id: uuid.UUID | None
    student_features: dict[str, float]
    school_features: dict[str, float]
    interaction_features: dict[str, float]
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def all_features(self) -> dict[str, float]:
        merged = dict(self.student_features)
        merged.update(self.school_features)
        merged.update(self.interaction_features)
        return merged


@dataclass(slots=True)
class CausalEstimateResult:
    """Outcome estimates plus rollout metadata."""

    scores: dict[str, float]
    confidence_by_outcome: dict[str, float]
    estimate_confidence: float
    label_type: str
    label_confidence: float
    causal_engine_version: str
    causal_model_version: str | None
    fallback_used: bool = False
    fallback_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CausalInterventionResult:
    """Result for what-if interventions."""

    original_scores: dict[str, float]
    modified_scores: dict[str, float]
    deltas: dict[str, float]
    estimate_confidence: float
    label_type: str
    label_confidence: float
    causal_engine_version: str
    causal_model_version: str | None
    fallback_used: bool = False
    fallback_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CausalExplainResult:
    """Human-readable explanation metadata."""

    summary: str
    key_factors: list[str]
    causal_engine_version: str
    causal_model_version: str | None
    metadata: dict[str, Any] = field(default_factory=dict)
