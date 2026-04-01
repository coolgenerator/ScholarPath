"""Mediation analysis for causal pathway decomposition.

Decomposes the total causal effect of a treatment on an outcome into
direct and indirect (mediated) components, enumerating all causal
pathways and their relative contributions.
"""

from __future__ import annotations

import copy
import logging
from typing import Any

import networkx as nx
import numpy as np

from scholarpath.causal.belief_propagation import NoisyORPropagator

logger = logging.getLogger(__name__)


class MediationAnalyzer:
    """Decomposes causal effects into direct, indirect, and pathway-level contributions."""

    def __init__(self, propagator: NoisyORPropagator | None = None) -> None:
        self.propagator = propagator or NoisyORPropagator()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def total_effect(
        self, dag: nx.DiGraph, treatment: str, outcome: str
    ) -> float:
        """Compute the total causal effect of treatment on outcome.

        TE = E[Y | do(X=1)] - E[Y | do(X=0)]

        Parameters
        ----------
        dag:
            The causal DAG with prior beliefs set.
        treatment:
            Treatment node id.
        outcome:
            Outcome node id.

        Returns
        -------
        float
            The total effect (can be negative if the treatment hurts the outcome).
        """
        self._validate_nodes(dag, treatment, outcome)

        y_high = self._intervene_and_read(dag, treatment, 1.0, outcome)
        y_low = self._intervene_and_read(dag, treatment, 0.0, outcome)
        return float(y_high - y_low)

    def direct_effect(
        self,
        dag: nx.DiGraph,
        treatment: str,
        outcome: str,
        mediators: list[str],
    ) -> float:
        """Compute the controlled direct effect (CDE) not through mediators.

        Fixes all mediators at their natural level under do(X=0), then
        computes the effect of varying treatment.

        CDE = E[Y | do(X=1, M=m_0)] - E[Y | do(X=0, M=m_0)]

        Parameters
        ----------
        dag:
            The causal DAG.
        treatment:
            Treatment node id.
        outcome:
            Outcome node id.
        mediators:
            List of mediator node ids to hold fixed.

        Returns
        -------
        float
            The controlled direct effect.
        """
        self._validate_nodes(dag, treatment, outcome)
        for m in mediators:
            if m not in dag:
                raise KeyError(f"Mediator node '{m}' not found in DAG")

        # Get natural mediator values under do(X=0)
        dag_baseline = self._do(dag, treatment, 0.0)
        mediator_vals = {
            m: dag_baseline.nodes[m].get("propagated_belief", 0.5)
            for m in mediators
        }

        # E[Y | do(X=1, M=m_0)]
        dag_high = copy.deepcopy(dag)
        interventions = {treatment: 1.0, **mediator_vals}
        for node_id, val in interventions.items():
            for parent in list(dag_high.predecessors(node_id)):
                dag_high.remove_edge(parent, node_id)
            dag_high.nodes[node_id]["prior_belief"] = val
            dag_high.nodes[node_id]["confidence"] = 1.0
            dag_high.nodes[node_id]["observed"] = True
        self.propagator.propagate(dag_high)
        y_high = dag_high.nodes[outcome].get("propagated_belief", 0.5)

        # E[Y | do(X=0, M=m_0)]
        dag_low = copy.deepcopy(dag)
        interventions_low = {treatment: 0.0, **mediator_vals}
        for node_id, val in interventions_low.items():
            for parent in list(dag_low.predecessors(node_id)):
                dag_low.remove_edge(parent, node_id)
            dag_low.nodes[node_id]["prior_belief"] = val
            dag_low.nodes[node_id]["confidence"] = 1.0
            dag_low.nodes[node_id]["observed"] = True
        self.propagator.propagate(dag_low)
        y_low = dag_low.nodes[outcome].get("propagated_belief", 0.5)

        return float(y_high - y_low)

    def indirect_effect(
        self, dag: nx.DiGraph, treatment: str, mediator: str, outcome: str
    ) -> float:
        """Compute the natural indirect effect (NIE) through a specific mediator.

        NIE = TE - CDE (where CDE controls for this mediator).
        Equivalently, the portion of the total effect that flows through
        the given mediator.

        Parameters
        ----------
        dag:
            The causal DAG.
        treatment:
            Treatment node id.
        mediator:
            The mediator to measure the indirect effect through.
        outcome:
            Outcome node id.

        Returns
        -------
        float
            The natural indirect effect through the mediator.
        """
        te = self.total_effect(dag, treatment, outcome)
        cde = self.direct_effect(dag, treatment, outcome, mediators=[mediator])
        return float(te - cde)

    def decompose_pathways(
        self,
        dag: nx.DiGraph,
        treatment: str,
        outcome: str,
        max_length: int = 5,
    ) -> list[dict[str, Any]]:
        """Decompose the total effect into all causal pathways with contributions.

        Each pathway's contribution is estimated by blocking all *other*
        pathways and measuring the remaining effect.

        Parameters
        ----------
        dag:
            The causal DAG.
        treatment:
            Treatment node id.
        outcome:
            Outcome node id.
        max_length:
            Maximum path length for DFS enumeration.

        Returns
        -------
        list[dict]
            One entry per pathway:
            ``{path, effect, percentage, mechanism}``.
        """
        self._validate_nodes(dag, treatment, outcome)

        paths = self.find_all_paths(dag, treatment, outcome, max_length)
        if not paths:
            logger.warning(
                "No causal paths found from '%s' to '%s'.", treatment, outcome
            )
            return []

        te = self.total_effect(dag, treatment, outcome)
        if abs(te) < 1e-10:
            # No total effect; return paths with zero contribution
            return [
                {
                    "path": p,
                    "effect": 0.0,
                    "percentage": 0.0,
                    "mechanism": self._describe_path(dag, p),
                }
                for p in paths
            ]

        # Estimate each pathway's contribution
        raw_effects: list[float] = []
        for path in paths:
            effect = self._estimate_path_effect(dag, path, treatment, outcome)
            raw_effects.append(effect)

        # Normalize so percentages sum to 100
        total_raw = sum(abs(e) for e in raw_effects)
        if total_raw < 1e-10:
            total_raw = 1.0

        results: list[dict[str, Any]] = []
        for path, raw in zip(paths, raw_effects):
            pct = (abs(raw) / total_raw) * 100.0
            results.append({
                "path": path,
                "effect": float(raw * abs(te) / total_raw) if total_raw > 0 else 0.0,
                "percentage": float(pct),
                "mechanism": self._describe_path(dag, path),
            })

        # Sort by percentage descending
        results.sort(key=lambda x: x["percentage"], reverse=True)
        return results

    def find_all_paths(
        self,
        dag: nx.DiGraph,
        source: str,
        target: str,
        max_length: int = 5,
    ) -> list[list[str]]:
        """Enumerate all directed paths from source to target via DFS.

        Parameters
        ----------
        dag:
            The directed graph.
        source:
            Start node id.
        target:
            End node id.
        max_length:
            Maximum number of edges in a path.

        Returns
        -------
        list[list[str]]
            Each inner list is an ordered sequence of node ids.
        """
        if source not in dag or target not in dag:
            return []

        all_paths: list[list[str]] = []

        def _dfs(current: str, path: list[str], visited: set[str]) -> None:
            if len(path) - 1 > max_length:
                return
            if current == target:
                all_paths.append(list(path))
                return
            for neighbor in dag.successors(current):
                if neighbor not in visited:
                    visited.add(neighbor)
                    path.append(neighbor)
                    _dfs(neighbor, path, visited)
                    path.pop()
                    visited.discard(neighbor)

        _dfs(source, [source], {source})
        return all_paths

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_nodes(self, dag: nx.DiGraph, *node_ids: str) -> None:
        for nid in node_ids:
            if nid not in dag:
                raise KeyError(f"Node '{nid}' not found in DAG")

    def _do(
        self, dag: nx.DiGraph, node_id: str, value: float
    ) -> nx.DiGraph:
        """Apply a single do-intervention and propagate."""
        mutilated = copy.deepcopy(dag)
        for parent in list(mutilated.predecessors(node_id)):
            mutilated.remove_edge(parent, node_id)
        mutilated.nodes[node_id]["prior_belief"] = value
        mutilated.nodes[node_id]["confidence"] = 1.0
        mutilated.nodes[node_id]["observed"] = True
        self.propagator.propagate(mutilated)
        return mutilated

    def _intervene_and_read(
        self, dag: nx.DiGraph, treatment: str, value: float, outcome: str
    ) -> float:
        result = self._do(dag, treatment, value)
        return result.nodes[outcome].get("propagated_belief", 0.5)

    def _estimate_path_effect(
        self,
        dag: nx.DiGraph,
        path: list[str],
        treatment: str,
        outcome: str,
    ) -> float:
        """Estimate a single pathway's effect by edge-strength product."""
        product = 1.0
        for i in range(len(path) - 1):
            u, v = path[i], path[i + 1]
            if dag.has_edge(u, v):
                product *= dag.edges[u, v].get("strength", 0.5)
            else:
                product = 0.0
                break
        return product

    def _describe_path(self, dag: nx.DiGraph, path: list[str]) -> str:
        """Build a human-readable mechanism description for a path."""
        labels = []
        for node_id in path:
            label = dag.nodes[node_id].get("label", node_id)
            labels.append(label)
        return " -> ".join(labels)
