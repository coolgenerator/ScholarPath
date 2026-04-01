"""Interventional analysis using the do-operator.

Implements Pearl's do-calculus for computing causal effects via graph
surgery and counterfactual reasoning over admission DAGs.
"""

from __future__ import annotations

import copy
import logging
from typing import Any

import networkx as nx
import numpy as np

from scholarpath.causal.belief_propagation import NoisyORPropagator
from scholarpath.causal.dag_builder import AdmissionDAGBuilder

logger = logging.getLogger(__name__)


class DoCalculusEngine:
    """Engine for interventional (do-operator) causal queries.

    Supports single and multi-variable interventions, average treatment
    effect estimation, school comparisons, and sensitivity analysis.
    """

    def __init__(self, propagator: NoisyORPropagator | None = None) -> None:
        self.propagator = propagator or NoisyORPropagator()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def do_intervention(
        self, dag: nx.DiGraph, node_id: str, value: float
    ) -> nx.DiGraph:
        """Apply do(node_id = value): graph surgery + re-propagation.

        Removes all incoming edges to ``node_id``, sets its belief to
        ``value``, and re-propagates through the rest of the graph.

        Parameters
        ----------
        dag:
            The causal DAG.
        node_id:
            The node to intervene on.
        value:
            The value to set in [0, 1].

        Returns
        -------
        nx.DiGraph
            A new DAG with the intervention applied and beliefs propagated.

        Raises
        ------
        KeyError
            If ``node_id`` is not in the DAG.
        """
        if node_id not in dag:
            raise KeyError(f"Node '{node_id}' not found in DAG")
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"Intervention value must be in [0, 1], got {value}")

        mutilated = copy.deepcopy(dag)

        # Graph surgery: remove all incoming edges
        incoming = list(mutilated.predecessors(node_id))
        for parent in incoming:
            mutilated.remove_edge(parent, node_id)

        # Set the intervened node's belief
        mutilated.nodes[node_id]["prior_belief"] = value
        mutilated.nodes[node_id]["confidence"] = 1.0
        mutilated.nodes[node_id]["observed"] = True
        mutilated.nodes[node_id]["intervened"] = True

        # Re-propagate
        self.propagator.propagate(mutilated)

        return mutilated

    def average_treatment_effect(
        self,
        dag: nx.DiGraph,
        treatment_node: str,
        outcome_node: str,
        treatment_val_high: float = 1.0,
        treatment_val_low: float = 0.0,
        n_bootstrap: int = 500,
    ) -> dict[str, float]:
        """Compute the average treatment effect (ATE).

        ATE = E[Y | do(X=high)] - E[Y | do(X=low)]

        Parameters
        ----------
        dag:
            The causal DAG.
        treatment_node:
            The treatment variable.
        outcome_node:
            The outcome variable.
        treatment_val_high:
            High treatment value (default 1.0).
        treatment_val_low:
            Low treatment value (default 0.0).
        n_bootstrap:
            Number of bootstrap samples for confidence intervals.

        Returns
        -------
        dict
            ``{ate, ci_lower, ci_upper}``
        """
        for node_id in (treatment_node, outcome_node):
            if node_id not in dag:
                raise KeyError(f"Node '{node_id}' not found in DAG")

        dag_high = self.do_intervention(dag, treatment_node, treatment_val_high)
        dag_low = self.do_intervention(dag, treatment_node, treatment_val_low)

        y_high = dag_high.nodes[outcome_node].get("propagated_belief", 0.5)
        y_low = dag_low.nodes[outcome_node].get("propagated_belief", 0.5)
        ate = y_high - y_low

        # Bootstrap CI
        rng = np.random.default_rng(42)
        ate_samples = []
        for _ in range(n_bootstrap):
            perturbed = copy.deepcopy(dag)
            for node in perturbed.nodes:
                belief = perturbed.nodes[node].get("prior_belief", 0.5)
                conf = perturbed.nodes[node].get("confidence", 0.5)
                noise = rng.normal(0, max(0.01, (1.0 - conf) * 0.15))
                perturbed.nodes[node]["prior_belief"] = float(
                    np.clip(belief + noise, 0, 1)
                )

            dh = self.do_intervention(perturbed, treatment_node, treatment_val_high)
            dl = self.do_intervention(perturbed, treatment_node, treatment_val_low)
            yh = dh.nodes[outcome_node].get("propagated_belief", 0.5)
            yl = dl.nodes[outcome_node].get("propagated_belief", 0.5)
            ate_samples.append(yh - yl)

        arr = np.array(ate_samples)
        return {
            "ate": float(ate),
            "ci_lower": float(np.percentile(arr, 2.5)),
            "ci_upper": float(np.percentile(arr, 97.5)),
        }

    def compare_schools(
        self,
        dag: nx.DiGraph,
        student: dict[str, Any],
        school_a_data: dict[str, Any],
        school_b_data: dict[str, Any],
        outcome_node: str = "career_outcome",
    ) -> dict[str, Any]:
        """Compare do(School=A) vs do(School=B) for a given student.

        Builds two personalized DAGs and compares the outcome under each
        school's data.

        Parameters
        ----------
        dag:
            Base causal DAG (will be deep-copied).
        student:
            Student profile dict.
        school_a_data:
            School A attributes.
        school_b_data:
            School B attributes.
        outcome_node:
            Which outcome to compare (default ``career_outcome``).

        Returns
        -------
        dict
            ``{school_a_outcome, school_b_outcome, difference,
              favoured, all_outcomes_a, all_outcomes_b}``
        """
        if outcome_node not in dag:
            raise KeyError(f"Outcome node '{outcome_node}' not found in DAG")

        builder = AdmissionDAGBuilder()

        dag_a = copy.deepcopy(dag)
        builder.personalize_dag(dag_a, student, school_a_data)
        self.propagator.propagate(dag_a)

        dag_b = copy.deepcopy(dag)
        builder.personalize_dag(dag_b, student, school_b_data)
        self.propagator.propagate(dag_b)

        outcome_a = dag_a.nodes[outcome_node].get("propagated_belief", 0.5)
        outcome_b = dag_b.nodes[outcome_node].get("propagated_belief", 0.5)

        # Collect all outcome nodes
        outcome_nodes = [
            n for n in dag.nodes
            if dag.nodes[n].get("node_type") == "outcome"
        ]
        all_a = {
            n: dag_a.nodes[n].get("propagated_belief", 0.5) for n in outcome_nodes
        }
        all_b = {
            n: dag_b.nodes[n].get("propagated_belief", 0.5) for n in outcome_nodes
        }

        diff = outcome_a - outcome_b
        return {
            "school_a_outcome": float(outcome_a),
            "school_b_outcome": float(outcome_b),
            "difference": float(diff),
            "favoured": "school_a" if diff > 0 else "school_b" if diff < 0 else "equal",
            "all_outcomes_a": {k: float(v) for k, v in all_a.items()},
            "all_outcomes_b": {k: float(v) for k, v in all_b.items()},
        }

    def what_if(
        self, dag: nx.DiGraph, interventions: dict[str, float]
    ) -> dict[str, float]:
        """Apply multiple simultaneous interventions and return all beliefs.

        Parameters
        ----------
        dag:
            The causal DAG.
        interventions:
            Mapping from node id to intervention value.

        Returns
        -------
        dict
            Mapping from every node id to its propagated belief after
            all interventions.
        """
        mutilated = copy.deepcopy(dag)

        for node_id, value in interventions.items():
            if node_id not in mutilated:
                raise KeyError(f"Node '{node_id}' not found in DAG")
            if not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"Intervention value for '{node_id}' must be in [0, 1], got {value}"
                )
            # Graph surgery
            incoming = list(mutilated.predecessors(node_id))
            for parent in incoming:
                mutilated.remove_edge(parent, node_id)
            mutilated.nodes[node_id]["prior_belief"] = value
            mutilated.nodes[node_id]["confidence"] = 1.0
            mutilated.nodes[node_id]["observed"] = True
            mutilated.nodes[node_id]["intervened"] = True

        self.propagator.propagate(mutilated)

        return {
            node: float(mutilated.nodes[node].get("propagated_belief", 0.5))
            for node in mutilated.nodes
        }

    def sensitivity_analysis(
        self,
        dag: nx.DiGraph,
        node_id: str,
        value_range: tuple[float, float] | list[float] | None = None,
        outcome_node: str = "career_outcome",
        n_steps: int = 20,
    ) -> list[dict[str, float]]:
        """Sweep a variable across a range and record outcome changes.

        Parameters
        ----------
        dag:
            The causal DAG.
        node_id:
            The node to sweep.
        value_range:
            Either a (low, high) tuple or an explicit list of values.
            Defaults to (0.0, 1.0).
        outcome_node:
            The outcome to track.
        n_steps:
            Number of evenly-spaced steps when ``value_range`` is a tuple.

        Returns
        -------
        list[dict]
            One entry per step: ``{input_value, outcome_value}``.
        """
        if node_id not in dag:
            raise KeyError(f"Node '{node_id}' not found in DAG")
        if outcome_node not in dag:
            raise KeyError(f"Outcome node '{outcome_node}' not found in DAG")

        if value_range is None:
            values = np.linspace(0.0, 1.0, n_steps).tolist()
        elif isinstance(value_range, (list, np.ndarray)):
            values = list(value_range)
        else:
            values = np.linspace(value_range[0], value_range[1], n_steps).tolist()

        results: list[dict[str, float]] = []
        for val in values:
            intervened = self.do_intervention(dag, node_id, val)
            outcome = intervened.nodes[outcome_node].get("propagated_belief", 0.5)
            results.append({
                "input_value": float(val),
                "outcome_value": float(outcome),
            })

        return results
