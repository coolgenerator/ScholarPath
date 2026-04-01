"""Backdoor adjustment for confounding control.

Implements the backdoor criterion to identify valid adjustment sets
and compute adjusted causal effects in the admissions DAG.
"""

from __future__ import annotations

import copy
import itertools
import logging
from typing import Any

import networkx as nx
import numpy as np

from scholarpath.causal.belief_propagation import NoisyORPropagator

logger = logging.getLogger(__name__)


class BackdoorAdjuster:
    """Identifies and applies backdoor adjustment sets for causal effect estimation."""

    def __init__(self, propagator: NoisyORPropagator | None = None) -> None:
        self.propagator = propagator or NoisyORPropagator()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def find_backdoor_set(
        self, dag: nx.DiGraph, treatment: str, outcome: str
    ) -> set[str]:
        """Find a minimal sufficient adjustment set using the backdoor criterion.

        A set Z satisfies the backdoor criterion relative to (X, Y) if:
        1. No node in Z is a descendant of X.
        2. Z blocks every path between X and Y that has an arrow into X
           (i.e., every backdoor path).

        This implementation finds a minimal such set.

        Parameters
        ----------
        dag:
            The causal DAG.
        treatment:
            Treatment node id.
        outcome:
            Outcome node id.

        Returns
        -------
        set[str]
            A minimal sufficient adjustment set. May be empty if no
            confounding exists.
        """
        self._validate_nodes(dag, treatment, outcome)

        # Find all ancestors of treatment that are also ancestors of outcome
        # (potential confounders)
        confounders = self.identify_confounders(dag, treatment, outcome)
        if not confounders:
            return set()

        # Get descendants of treatment (cannot be in adjustment set)
        descendants_of_treatment = nx.descendants(dag, treatment)

        # Candidate nodes: confounders that are not descendants of treatment
        candidates = [c for c in confounders if c not in descendants_of_treatment]

        if not candidates:
            return set()

        # Try to find minimal set: start with all candidates and try removing
        full_set = set(candidates)
        if self.is_valid_adjustment_set(dag, treatment, outcome, full_set):
            # Try to minimise by removing one at a time
            minimal = set(full_set)
            for node in candidates:
                test_set = minimal - {node}
                if self.is_valid_adjustment_set(dag, treatment, outcome, test_set):
                    minimal = test_set
            return minimal

        # If full set doesn't work, try subsets of increasing size
        for size in range(1, len(candidates) + 1):
            for subset in itertools.combinations(candidates, size):
                s = set(subset)
                if self.is_valid_adjustment_set(dag, treatment, outcome, s):
                    return s

        logger.warning(
            "No valid backdoor adjustment set found for %s -> %s",
            treatment,
            outcome,
        )
        return set()

    def is_valid_adjustment_set(
        self,
        dag: nx.DiGraph,
        treatment: str,
        outcome: str,
        adjustment_set: set[str],
    ) -> bool:
        """Verify whether an adjustment set satisfies the backdoor criterion.

        Parameters
        ----------
        dag:
            The causal DAG.
        treatment:
            Treatment node id.
        outcome:
            Outcome node id.
        adjustment_set:
            Set of node ids to condition on.

        Returns
        -------
        bool
            True if the set is a valid backdoor adjustment set.
        """
        self._validate_nodes(dag, treatment, outcome)

        # Criterion 1: No node in Z is a descendant of X
        descendants = nx.descendants(dag, treatment)
        if adjustment_set & descendants:
            return False

        # Criterion 2: Z blocks all backdoor paths
        # A backdoor path is any path from treatment to outcome that starts
        # with an arrow INTO treatment (i.e., paths through parents of treatment).
        backdoor_paths = self._find_backdoor_paths(dag, treatment, outcome)

        for path in backdoor_paths:
            if not self._is_path_blocked(dag, path, adjustment_set):
                return False

        return True

    def adjusted_effect(
        self,
        dag: nx.DiGraph,
        treatment: str,
        outcome: str,
        adjustment_set: set[str],
        data: dict[str, float],
    ) -> dict[str, float]:
        """Compute the adjusted causal effect using backdoor adjustment.

        For each stratum defined by the adjustment set, computes the
        interventional effect and averages over strata.

        Parameters
        ----------
        dag:
            The causal DAG.
        treatment:
            Treatment node id.
        outcome:
            Outcome node id.
        adjustment_set:
            Set of nodes to adjust for.
        data:
            Mapping from node id to observed value for the adjustment
            variables.

        Returns
        -------
        dict
            ``{adjusted_effect, adjustment_set, strata_effects}``
        """
        self._validate_nodes(dag, treatment, outcome)

        if not self.is_valid_adjustment_set(dag, treatment, outcome, adjustment_set):
            logger.warning(
                "Provided adjustment set %s may not satisfy the backdoor criterion.",
                adjustment_set,
            )

        # Fix adjustment variables to their observed values
        evidence_high = {treatment: 1.0}
        evidence_low = {treatment: 0.0}
        for adj_node in adjustment_set:
            val = data.get(adj_node, dag.nodes[adj_node].get("prior_belief", 0.5))
            evidence_high[adj_node] = val
            evidence_low[adj_node] = val

        # Compute do(X=1) conditioned on Z=z
        dag_high = copy.deepcopy(dag)
        for node_id, val in evidence_high.items():
            for parent in list(dag_high.predecessors(node_id)):
                dag_high.remove_edge(parent, node_id)
            dag_high.nodes[node_id]["prior_belief"] = val
            dag_high.nodes[node_id]["confidence"] = 1.0
            dag_high.nodes[node_id]["observed"] = True
        self.propagator.propagate(dag_high)
        y_high = dag_high.nodes[outcome].get("propagated_belief", 0.5)

        # Compute do(X=0) conditioned on Z=z
        dag_low = copy.deepcopy(dag)
        for node_id, val in evidence_low.items():
            for parent in list(dag_low.predecessors(node_id)):
                dag_low.remove_edge(parent, node_id)
            dag_low.nodes[node_id]["prior_belief"] = val
            dag_low.nodes[node_id]["confidence"] = 1.0
            dag_low.nodes[node_id]["observed"] = True
        self.propagator.propagate(dag_low)
        y_low = dag_low.nodes[outcome].get("propagated_belief", 0.5)

        adj_effect = float(y_high - y_low)

        return {
            "adjusted_effect": adj_effect,
            "adjustment_set": sorted(adjustment_set),
            "y_do_high": float(y_high),
            "y_do_low": float(y_low),
        }

    def identify_confounders(
        self, dag: nx.DiGraph, treatment: str, outcome: str
    ) -> list[str]:
        """Find all confounders (common causes) of treatment and outcome.

        A confounder is a node that is an ancestor of both the treatment
        and the outcome (a common cause).

        Parameters
        ----------
        dag:
            The causal DAG.
        treatment:
            Treatment node id.
        outcome:
            Outcome node id.

        Returns
        -------
        list[str]
            Sorted list of confounder node ids.
        """
        self._validate_nodes(dag, treatment, outcome)

        ancestors_of_treatment = nx.ancestors(dag, treatment)
        ancestors_of_outcome = nx.ancestors(dag, outcome)

        # Common ancestors are confounders
        common_ancestors = ancestors_of_treatment & ancestors_of_outcome

        # Also include nodes explicitly typed as confounders that are
        # connected to both treatment and outcome
        for node in dag.nodes:
            if dag.nodes[node].get("node_type") == "confounder":
                # Check if this node has a directed path to both treatment and outcome
                is_ancestor_of_t = node in ancestors_of_treatment or node == treatment
                is_ancestor_of_o = node in ancestors_of_outcome or node == outcome
                if is_ancestor_of_t and is_ancestor_of_o:
                    common_ancestors.add(node)

        # Exclude treatment and outcome themselves
        common_ancestors.discard(treatment)
        common_ancestors.discard(outcome)

        return sorted(common_ancestors)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_nodes(self, dag: nx.DiGraph, *node_ids: str) -> None:
        for nid in node_ids:
            if nid not in dag:
                raise KeyError(f"Node '{nid}' not found in DAG")

    def _find_backdoor_paths(
        self, dag: nx.DiGraph, treatment: str, outcome: str
    ) -> list[list[str]]:
        """Find all backdoor paths from treatment to outcome.

        Backdoor paths are paths that begin with an arrow INTO the treatment
        node (i.e., via a parent of treatment). We search in the underlying
        undirected graph but only consider paths starting via a parent.
        """
        paths: list[list[str]] = []
        parents_of_treatment = list(dag.predecessors(treatment))

        undirected = dag.to_undirected()

        for parent in parents_of_treatment:
            # Find all simple paths from parent to outcome not going through treatment
            try:
                for path in nx.all_simple_paths(
                    undirected, parent, outcome, cutoff=8
                ):
                    if treatment not in path:
                        full_path = [treatment] + list(path)
                        paths.append(full_path)
            except nx.NetworkXError:
                continue

        return paths

    def _is_path_blocked(
        self,
        dag: nx.DiGraph,
        path: list[str],
        conditioning_set: set[str],
    ) -> bool:
        """Check if a path is blocked (d-separated) given a conditioning set.

        A path is blocked if there exists a non-collider in the conditioning
        set, or a collider NOT in the conditioning set (and no descendant
        of the collider is in the conditioning set).
        """
        if len(path) < 3:
            # A path of length 2 is just treatment -> parent; blocked if
            # the parent is in the conditioning set
            return len(set(path[1:]) & conditioning_set) > 0

        for i in range(1, len(path) - 1):
            prev_node, curr_node, next_node = path[i - 1], path[i], path[i + 1]

            # Determine if curr_node is a collider on this path
            # A collider has arrows pointing IN from both sides
            arrow_in_from_prev = dag.has_edge(prev_node, curr_node)
            arrow_in_from_next = dag.has_edge(next_node, curr_node)
            is_collider = arrow_in_from_prev and arrow_in_from_next

            if is_collider:
                # Collider blocks UNLESS it (or a descendant) is conditioned on
                curr_descendants = nx.descendants(dag, curr_node)
                if curr_node not in conditioning_set and not (
                    curr_descendants & conditioning_set
                ):
                    return True  # Blocked by unactivated collider
            else:
                # Non-collider blocks IF it is conditioned on
                if curr_node in conditioning_set:
                    return True  # Blocked by conditioned non-collider

        return False
