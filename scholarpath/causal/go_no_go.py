"""Go/No-Go scoring engine for college admission offers.

Combines causal inference outputs into an actionable recommendation
score using Noisy-OR aggregation across outcome dimensions.
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

# Default outcome dimension weights
_DEFAULT_WEIGHTS: dict[str, float] = {
    "academic": 0.30,
    "financial": 0.25,
    "career": 0.25,
    "life": 0.20,
}

# Tier thresholds and labels
_TIER_THRESHOLDS: list[tuple[float, str]] = [
    (0.78, "strongly_recommend"),
    (0.58, "recommend"),
    (0.38, "neutral"),
    (0.00, "not_recommend"),
]

# Mapping from dimension keys to DAG outcome nodes
_DIMENSION_NODE_MAP: dict[str, str] = {
    "academic": "academic_outcome",
    "financial": "financial_stress",  # Inverted: lower stress = better
    "career": "career_outcome",
    "life": "life_satisfaction",
}


def _score_to_tier(score: float) -> str:
    """Map a score in [0, 1] to a recommendation tier."""
    for threshold, tier in _TIER_THRESHOLDS:
        if score >= threshold:
            return tier
    return "not_recommend"


def _score_to_recommendation(score: float, tier: str) -> str:
    """Generate a human-readable recommendation string."""
    recommendations = {
        "strongly_recommend": "This school is an excellent match. Strong alignment across academic, career, and personal dimensions.",
        "recommend": "This school is a good fit. Positive indicators outweigh concerns.",
        "neutral": "This school has mixed signals. Consider your personal priorities carefully.",
        "not_recommend": "This school may not be the best fit. Significant concerns in one or more dimensions.",
    }
    return recommendations.get(tier, "Unable to generate recommendation.")


class GoNoGoScorer:
    """Computes Go/No-Go scores for college admission offers.

    Aggregates causal-graph-derived outcome probabilities into a single
    actionable score using Noisy-OR combination, with configurable
    dimension weights.
    """

    def __init__(self, propagator: NoisyORPropagator | None = None) -> None:
        self.propagator = propagator or NoisyORPropagator()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_score(
        self,
        evaluation_data: dict[str, float],
        causal_graph: nx.DiGraph,
        weights: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        """Compute the overall Go/No-Go score for a school offer.

        Uses Noisy-OR aggregation:
            Score = 1 - product(1 - w_i * p_i)

        Parameters
        ----------
        evaluation_data:
            Mapping from dimension key (``academic``, ``financial``,
            ``career``, ``life``) to probability/score in [0, 1].
            If missing, values are read from the propagated causal graph.
        causal_graph:
            The propagated causal DAG.
        weights:
            Optional custom weights per dimension. Defaults to
            academic=0.30, financial=0.25, career=0.25, life=0.20.

        Returns
        -------
        dict
            ``{overall_score, confidence_interval, sub_scores,
              recommendation, tier}``
        """
        w = weights or _DEFAULT_WEIGHTS.copy()

        # Normalise weights to sum to 1
        w_total = sum(w.values())
        if w_total > 0:
            w = {k: v / w_total for k, v in w.items()}

        # Gather sub-scores from evaluation_data or the DAG
        sub_scores: dict[str, float] = {}
        for dim, weight in w.items():
            if dim in evaluation_data:
                p = evaluation_data[dim]
            else:
                node_id = _DIMENSION_NODE_MAP.get(dim)
                if node_id and node_id in causal_graph:
                    p = causal_graph.nodes[node_id].get(
                        "propagated_belief",
                        causal_graph.nodes[node_id].get("prior_belief", 0.5),
                    )
                    # Invert financial_stress (lower stress = better score)
                    if dim == "financial":
                        p = 1.0 - p
                else:
                    p = 0.5
            sub_scores[dim] = float(np.clip(p, 0, 1))

        # Noisy-OR aggregation
        product = 1.0
        for dim, weight in w.items():
            p = sub_scores.get(dim, 0.5)
            product *= 1.0 - weight * p

        overall_score = float(1.0 - product)

        # Bootstrap CI
        ci = self._bootstrap_ci(sub_scores, w)

        tier = _score_to_tier(overall_score)
        recommendation = _score_to_recommendation(overall_score, tier)

        return {
            "overall_score": overall_score,
            "confidence_interval": ci,
            "sub_scores": sub_scores,
            "recommendation": recommendation,
            "tier": tier,
        }

    def compare_offers(
        self,
        offers_data: list[dict[str, Any]],
        causal_graph: nx.DiGraph,
    ) -> list[dict[str, Any]]:
        """Score and rank multiple admission offers.

        Parameters
        ----------
        offers_data:
            List of dicts, each with ``school_name`` and either
            sub-score values or ``school_data`` for DAG personalisation.
        causal_graph:
            The base causal DAG.

        Returns
        -------
        list[dict]
            Sorted list (best first) of scored offers, each with
            ``school_name``, ``score``, ``tier``, ``rank``.
        """
        scored: list[dict[str, Any]] = []

        for offer in offers_data:
            school_name = offer.get("school_name", "Unknown")

            # If school_data is provided, personalize a copy of the DAG
            if "school_data" in offer and "student" in offer:
                builder = AdmissionDAGBuilder()
                dag_copy = copy.deepcopy(causal_graph)
                builder.personalize_dag(dag_copy, offer["student"], offer["school_data"])
                self.propagator.propagate(dag_copy)
                eval_data = {}  # Will pull from DAG
                graph = dag_copy
            else:
                eval_data = {
                    k: offer[k]
                    for k in ("academic", "financial", "career", "life")
                    if k in offer
                }
                graph = causal_graph

            result = self.compute_score(eval_data, graph)
            result["school_name"] = school_name
            scored.append(result)

        # Sort by overall_score descending
        scored.sort(key=lambda x: x["overall_score"], reverse=True)

        # Add ranks
        for i, entry in enumerate(scored, 1):
            entry["rank"] = i

        return scored

    def generate_key_factors(
        self,
        scores: dict[str, Any],
        dag: nx.DiGraph,
    ) -> list[dict[str, Any]]:
        """Extract the top factors driving the Go/No-Go score.

        Parameters
        ----------
        scores:
            The output of ``compute_score``.
        dag:
            The propagated causal DAG.

        Returns
        -------
        list[dict]
            Sorted list of factors with ``{node_id, label, belief,
            impact, direction}``.
        """
        sub_scores = scores.get("sub_scores", {})
        overall = scores.get("overall_score", 0.5)

        factors: list[dict[str, Any]] = []

        for node_id in dag.nodes:
            node_data = dag.nodes[node_id]
            belief = node_data.get(
                "propagated_belief", node_data.get("prior_belief", 0.5)
            )
            label = node_data.get("label", node_id)
            node_type = node_data.get("node_type", "observed")

            # Estimate impact: how much does this node's belief deviate from
            # neutral and how connected is it to outcomes?
            out_degree = dag.out_degree(node_id)
            deviation = abs(belief - 0.5)
            impact = deviation * (1 + 0.1 * out_degree)

            direction = "positive" if belief > 0.5 else "negative" if belief < 0.5 else "neutral"

            factors.append({
                "node_id": node_id,
                "label": label,
                "belief": float(belief),
                "impact": float(impact),
                "direction": direction,
                "node_type": node_type,
            })

        # Sort by impact descending
        factors.sort(key=lambda x: x["impact"], reverse=True)
        return factors[:10]

    def run_automatic_what_ifs(
        self,
        dag: nx.DiGraph,
        student: dict[str, Any],
        school: dict[str, Any],
        base_score: float,
    ) -> list[dict[str, Any]]:
        """Auto-run standard what-if scenarios.

        Scenarios include SAT +50, financial aid +$15K, and major change.

        Parameters
        ----------
        dag:
            The propagated causal DAG.
        student:
            Student profile dict.
        school:
            School data dict.
        base_score:
            The baseline Go/No-Go score for comparison.

        Returns
        -------
        list[dict]
            Each entry: ``{scenario, description, new_score, delta,
            direction}``.
        """
        builder = AdmissionDAGBuilder()
        scenarios: list[dict[str, Any]] = []

        # Scenario 1: SAT +50
        student_sat_up = {**student, "sat": student.get("sat", 1100) + 50}
        dag_s1 = copy.deepcopy(dag)
        builder.personalize_dag(dag_s1, student_sat_up, school)
        self.propagator.propagate(dag_s1)
        score_s1 = self.compute_score({}, dag_s1)
        delta_s1 = score_s1["overall_score"] - base_score
        scenarios.append({
            "scenario": "sat_plus_50",
            "description": f"SAT score increases by 50 points (to {student_sat_up['sat']})",
            "new_score": score_s1["overall_score"],
            "delta": float(delta_s1),
            "direction": "positive" if delta_s1 > 0 else "negative",
        })

        # Scenario 2: Financial aid +$15K
        school_aid_up = {**school, "avg_aid": school.get("avg_aid", 20_000) + 15_000}
        dag_s2 = copy.deepcopy(dag)
        builder.personalize_dag(dag_s2, student, school_aid_up)
        self.propagator.propagate(dag_s2)
        score_s2 = self.compute_score({}, dag_s2)
        delta_s2 = score_s2["overall_score"] - base_score
        scenarios.append({
            "scenario": "aid_plus_15k",
            "description": f"Financial aid increases by $15,000 (to ${school_aid_up['avg_aid']:,})",
            "new_score": score_s2["overall_score"],
            "delta": float(delta_s2),
            "direction": "positive" if delta_s2 > 0 else "negative",
        })

        # Scenario 3: GPA +0.3
        student_gpa_up = {**student, "gpa": min(4.0, student.get("gpa", 3.0) + 0.3)}
        dag_s3 = copy.deepcopy(dag)
        builder.personalize_dag(dag_s3, student_gpa_up, school)
        self.propagator.propagate(dag_s3)
        score_s3 = self.compute_score({}, dag_s3)
        delta_s3 = score_s3["overall_score"] - base_score
        scenarios.append({
            "scenario": "gpa_plus_0.3",
            "description": f"GPA increases by 0.3 (to {student_gpa_up['gpa']:.1f})",
            "new_score": score_s3["overall_score"],
            "delta": float(delta_s3),
            "direction": "positive" if delta_s3 > 0 else "negative",
        })

        # Scenario 4: Location tier upgrade
        school_loc_up = {**school, "location_tier": min(5, school.get("location_tier", 3) + 1)}
        dag_s4 = copy.deepcopy(dag)
        builder.personalize_dag(dag_s4, student, school_loc_up)
        self.propagator.propagate(dag_s4)
        score_s4 = self.compute_score({}, dag_s4)
        delta_s4 = score_s4["overall_score"] - base_score
        scenarios.append({
            "scenario": "location_tier_up",
            "description": f"Location tier improves by 1 (to {school_loc_up['location_tier']})",
            "new_score": score_s4["overall_score"],
            "delta": float(delta_s4),
            "direction": "positive" if delta_s4 > 0 else "negative",
        })

        # Sort by absolute delta descending
        scenarios.sort(key=lambda x: abs(x["delta"]), reverse=True)
        return scenarios

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _bootstrap_ci(
        self,
        sub_scores: dict[str, float],
        weights: dict[str, float],
        n_samples: int = 500,
    ) -> dict[str, float]:
        """Bootstrap confidence interval for the overall score."""
        rng = np.random.default_rng(42)
        samples: list[float] = []

        for _ in range(n_samples):
            product = 1.0
            for dim, w in weights.items():
                p = sub_scores.get(dim, 0.5)
                # Perturb
                noise = rng.normal(0, 0.05)
                p_noisy = float(np.clip(p + noise, 0, 1))
                product *= 1.0 - w * p_noisy
            samples.append(1.0 - product)

        arr = np.array(samples)
        return {
            "ci_lower": float(np.percentile(arr, 2.5)),
            "ci_upper": float(np.percentile(arr, 97.5)),
        }
