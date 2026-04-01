"""Noisy-OR belief propagation over causal DAGs.

Implements forward belief propagation using Noisy-OR (and AND-gate)
combination rules, with bootstrap-based confidence interval estimation.
"""

from __future__ import annotations

import logging
from typing import Any

import networkx as nx
import numpy as np

logger = logging.getLogger(__name__)


class NoisyORPropagator:
    """Propagates beliefs through a causal DAG using Noisy-OR combination.

    For each node the updated belief is computed as:

        belief = 1 - prod(1 - w_i * parent_belief_i)

    where *w_i* is the edge strength and *parent_belief_i* is the
    propagated belief of parent *i*.  Nodes flagged with
    ``gate_type="and"`` use a product (AND) rule instead.
    """

    def __init__(self, leak_probability: float = 0.01) -> None:
        """
        Parameters
        ----------
        leak_probability:
            Background "leak" probability representing unmeasured causes.
            Applied additively before the Noisy-OR combination.
        """
        if not 0.0 <= leak_probability <= 1.0:
            raise ValueError("leak_probability must be in [0, 1]")
        self.leak_probability = leak_probability

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def propagate(self, dag: nx.DiGraph) -> nx.DiGraph:
        """Run belief propagation on the DAG using Noisy-OR model.

        Processes nodes in topological order. Root nodes (no parents)
        retain their ``prior_belief``.  All other nodes are updated
        according to their ``gate_type`` (default ``noisy_or``).

        Parameters
        ----------
        dag:
            A directed acyclic graph with ``prior_belief`` on each node
            and ``strength`` on each edge.

        Returns
        -------
        nx.DiGraph
            The same DAG instance with ``propagated_belief`` set on every node.

        Raises
        ------
        ValueError
            If the graph contains cycles.
        """
        dag = self._ensure_acyclic(dag)
        topo_order = list(nx.topological_sort(dag))

        for node in topo_order:
            parents = list(dag.predecessors(node))
            if not parents:
                # Root node: propagated belief equals prior
                dag.nodes[node]["propagated_belief"] = dag.nodes[node].get(
                    "prior_belief", 0.5
                )
                continue

            gate = dag.nodes[node].get("gate_type", "noisy_or")
            if gate == "and":
                belief = self._and_gate(dag, node, parents)
            else:
                belief = self._noisy_or_gate(dag, node, parents)

            dag.nodes[node]["propagated_belief"] = float(np.clip(belief, 0, 1))

        return dag

    def propagate_with_evidence(
        self, dag: nx.DiGraph, evidence: dict[str, float]
    ) -> nx.DiGraph:
        """Propagate beliefs after clamping observed evidence.

        Parameters
        ----------
        dag:
            The causal DAG.
        evidence:
            Mapping from node id to observed belief value in [0, 1].

        Returns
        -------
        nx.DiGraph
            DAG with updated ``propagated_belief`` values.
        """
        for node_id, value in evidence.items():
            if node_id not in dag:
                logger.warning("Evidence node '%s' not found in DAG; skipping.", node_id)
                continue
            if not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"Evidence value for '{node_id}' must be in [0, 1], got {value}"
                )
            dag.nodes[node_id]["prior_belief"] = value
            dag.nodes[node_id]["confidence"] = 1.0
            # Mark as observed so propagation respects the clamp
            dag.nodes[node_id]["observed"] = True

        dag = self._ensure_acyclic(dag)
        topo_order = list(nx.topological_sort(dag))

        for node in topo_order:
            if dag.nodes[node].get("observed", False):
                dag.nodes[node]["propagated_belief"] = dag.nodes[node]["prior_belief"]
                continue

            parents = list(dag.predecessors(node))
            if not parents:
                dag.nodes[node]["propagated_belief"] = dag.nodes[node].get(
                    "prior_belief", 0.5
                )
                continue

            gate = dag.nodes[node].get("gate_type", "noisy_or")
            if gate == "and":
                belief = self._and_gate(dag, node, parents)
            else:
                belief = self._noisy_or_gate(dag, node, parents)

            dag.nodes[node]["propagated_belief"] = float(np.clip(belief, 0, 1))

        return dag

    def compute_confidence_intervals(
        self, dag: nx.DiGraph, n_samples: int = 1000
    ) -> dict[str, dict[str, float]]:
        """Bootstrap-based confidence interval computation.

        Perturbs each node's ``prior_belief`` by sampling from a Beta
        distribution centred on the belief with spread controlled by the
        node's ``confidence``, then re-propagates.

        Parameters
        ----------
        dag:
            The causal DAG (must already have ``propagated_belief``).
        n_samples:
            Number of bootstrap iterations.

        Returns
        -------
        dict
            Mapping from node id to ``{mean, ci_lower, ci_upper, std}``.
        """
        rng = np.random.default_rng(42)
        all_nodes = list(dag.nodes)
        samples: dict[str, list[float]] = {n: [] for n in all_nodes}

        for _ in range(n_samples):
            # Perturb priors
            sim_dag = dag.copy()
            for node in all_nodes:
                belief = sim_dag.nodes[node].get("prior_belief", 0.5)
                conf = sim_dag.nodes[node].get("confidence", 0.5)
                # Higher confidence -> tighter distribution
                concentration = max(2.0, conf * 20.0)
                alpha = belief * concentration
                beta_param = (1.0 - belief) * concentration
                # Avoid degenerate parameters
                alpha = max(alpha, 0.1)
                beta_param = max(beta_param, 0.1)
                sim_dag.nodes[node]["prior_belief"] = float(
                    rng.beta(alpha, beta_param)
                )

            sim_dag = self.propagate(sim_dag)
            for node in all_nodes:
                samples[node].append(
                    sim_dag.nodes[node].get("propagated_belief", 0.5)
                )

        results: dict[str, dict[str, float]] = {}
        for node in all_nodes:
            arr = np.array(samples[node])
            results[node] = {
                "mean": float(np.mean(arr)),
                "ci_lower": float(np.percentile(arr, 2.5)),
                "ci_upper": float(np.percentile(arr, 97.5)),
                "std": float(np.std(arr)),
            }

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _noisy_or_gate(
        self, dag: nx.DiGraph, node: str, parents: list[str]
    ) -> float:
        """Compute belief using Noisy-OR: 1 - prod(1 - w_i * b_i)."""
        product = 1.0 - self.leak_probability
        for parent in parents:
            edge_data = dag.edges[parent, node]
            w = abs(edge_data.get("strength", 0.5))
            parent_belief = dag.nodes[parent].get("propagated_belief", dag.nodes[parent].get("prior_belief", 0.5))

            # Handle negative causal effects: invert parent belief
            if edge_data.get("strength", 0.5) < 0:
                parent_belief = 1.0 - parent_belief

            product *= 1.0 - w * parent_belief

        return 1.0 - product

    def _and_gate(
        self, dag: nx.DiGraph, node: str, parents: list[str]
    ) -> float:
        """Compute belief using AND gate (product rule)."""
        result = 1.0
        for parent in parents:
            edge_data = dag.edges[parent, node]
            w = abs(edge_data.get("strength", 0.5))
            parent_belief = dag.nodes[parent].get("propagated_belief", dag.nodes[parent].get("prior_belief", 0.5))

            if edge_data.get("strength", 0.5) < 0:
                parent_belief = 1.0 - parent_belief

            result *= w * parent_belief

        return result

    @staticmethod
    def _ensure_acyclic(dag: nx.DiGraph) -> nx.DiGraph:
        """Verify the DAG is acyclic; break cycles by removing weakest edge."""
        while not nx.is_directed_acyclic_graph(dag):
            try:
                cycle = next(nx.simple_cycles(dag))
            except StopIteration:
                break
            # Find weakest edge in cycle
            weakest_edge = None
            weakest_strength = float("inf")
            for i in range(len(cycle)):
                u, v = cycle[i], cycle[(i + 1) % len(cycle)]
                if dag.has_edge(u, v):
                    strength = abs(dag.edges[u, v].get("strength", 0.5))
                    if strength < weakest_strength:
                        weakest_strength = strength
                        weakest_edge = (u, v)
            if weakest_edge:
                logger.warning(
                    "Breaking cycle by removing weakest edge: %s -> %s (strength=%.3f)",
                    weakest_edge[0],
                    weakest_edge[1],
                    weakest_strength,
                )
                dag.remove_edge(*weakest_edge)

        return dag
