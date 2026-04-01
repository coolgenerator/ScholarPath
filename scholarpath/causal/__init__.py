"""CurioCat Causal Inference Engine for ScholarPath.

Provides domain-constrained causal DAGs, belief propagation, do-calculus,
mediation analysis, backdoor adjustment, and Go/No-Go scoring for
college admissions decision-making.
"""

from scholarpath.causal.dag_builder import AdmissionDAGBuilder
from scholarpath.causal.belief_propagation import NoisyORPropagator
from scholarpath.causal.do_calculus import DoCalculusEngine
from scholarpath.causal.mediation import MediationAnalyzer
from scholarpath.causal.backdoor import BackdoorAdjuster
from scholarpath.causal.go_no_go import GoNoGoScorer
from scholarpath.causal.graph_store import (
    serialize_graph,
    deserialize_graph,
    graph_to_cytoscape,
    graph_diff,
)

__all__ = [
    "AdmissionDAGBuilder",
    "NoisyORPropagator",
    "DoCalculusEngine",
    "MediationAnalyzer",
    "BackdoorAdjuster",
    "GoNoGoScorer",
    "serialize_graph",
    "deserialize_graph",
    "graph_to_cytoscape",
    "graph_diff",
]
