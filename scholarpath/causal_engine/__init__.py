"""Causal engine runtime and training interfaces.

This package restores maintainable source code for the causal stack.
The implementation is intentionally conservative and backward compatible:
legacy DAG estimation remains available while the pywhy path can be
progressively enabled through runtime configuration.
"""

from scholarpath.causal_engine.interfaces import CausalEngine
from scholarpath.causal_engine.runtime import CausalRuntime, get_causal_runtime
from scholarpath.causal_engine.training import train_full_graph_model
from scholarpath.causal_engine.types import (
    CausalEstimateResult,
    CausalExplainResult,
    CausalInterventionResult,
    CausalRequestContext,
)

__all__ = [
    "CausalEngine",
    "CausalRuntime",
    "get_causal_runtime",
    "train_full_graph_model",
    "CausalRequestContext",
    "CausalEstimateResult",
    "CausalInterventionResult",
    "CausalExplainResult",
]
