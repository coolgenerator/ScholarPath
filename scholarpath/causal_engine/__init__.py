"""Unified causal engine package for legacy and PyWhy runtimes."""

from scholarpath.causal_engine.feature_builder import FeatureBuilder
from scholarpath.causal_engine.interfaces import CausalEngine
from scholarpath.causal_engine.legacy_engine import LegacyCausalEngine
from scholarpath.causal_engine.pywhy_engine import PyWhyCausalEngine, PyWhyUnavailableError
from scholarpath.causal_engine.runtime import CausalRuntime
from scholarpath.causal_engine.training import (
    TrainingResult,
    promote_model,
    shadow_audit,
    train_full_graph_model,
)
from scholarpath.causal_engine.types import (
    CausalEstimateResult,
    CausalExplainResult,
    CausalInterventionResult,
    CausalRequestContext,
)

__all__ = [
    "CausalEngine",
    "CausalRuntime",
    "LegacyCausalEngine",
    "PyWhyCausalEngine",
    "PyWhyUnavailableError",
    "FeatureBuilder",
    "CausalRequestContext",
    "CausalEstimateResult",
    "CausalInterventionResult",
    "CausalExplainResult",
    "TrainingResult",
    "train_full_graph_model",
    "promote_model",
    "shadow_audit",
]
