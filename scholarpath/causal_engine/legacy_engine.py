"""Adapter around the existing DAG + Noisy-OR causal stack."""

from __future__ import annotations

from typing import Any

from scholarpath.causal import AdmissionDAGBuilder, NoisyORPropagator
from scholarpath.causal_engine.interfaces import CausalEngine
from scholarpath.causal_engine.types import (
    CausalEstimateResult,
    CausalExplainResult,
    CausalInterventionResult,
    CausalRequestContext,
)

_DEFAULT_OUTCOMES = (
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
)


class LegacyCausalEngine(CausalEngine):
    """Legacy engine wrapper exposing the new interface."""

    engine_version = "legacy_dag_v1"

    def __init__(self) -> None:
        self._builder = AdmissionDAGBuilder()
        self._propagator = NoisyORPropagator()

    async def estimate(
        self,
        ctx: CausalRequestContext,
        outcomes: list[str],
    ) -> CausalEstimateResult:
        dag = self._build_graph(ctx)
        dag = self._propagator.propagate(dag)

        requested = outcomes or list(_DEFAULT_OUTCOMES)
        scores: dict[str, float] = {}
        for node_id in requested:
            if node_id in dag:
                scores[node_id] = float(dag.nodes[node_id].get("propagated_belief", 0.5))

        ci = self._propagator.compute_confidence_intervals(dag, n_samples=200)
        conf = {
            key: float(ci.get(key, {}).get("ci_upper", 0.5) - ci.get(key, {}).get("ci_lower", 0.5))
            for key in scores
        }
        # Smaller confidence interval width -> higher confidence.
        estimate_confidence = max(0.0, min(1.0, 1.0 - (sum(conf.values()) / max(len(conf), 1))))

        return CausalEstimateResult(
            scores=scores,
            confidence_by_outcome={k: max(0.0, min(1.0, 1.0 - v)) for k, v in conf.items()},
            estimate_confidence=estimate_confidence,
            label_type="proxy",
            label_confidence=0.6,
            causal_engine_version=self.engine_version,
            causal_model_version="legacy",
        )

    async def intervene(
        self,
        ctx: CausalRequestContext,
        interventions: dict[str, float],
        outcomes: list[str],
    ) -> CausalInterventionResult:
        baseline = await self.estimate(ctx, outcomes)
        dag = self._build_graph(ctx)
        for node_id, value in interventions.items():
            if node_id not in dag:
                continue
            dag.nodes[node_id]["prior_belief"] = max(0.0, min(1.0, float(value)))
            dag.nodes[node_id]["confidence"] = 1.0
            parents = list(dag.predecessors(node_id))
            for parent in parents:
                dag.remove_edge(parent, node_id)

        dag = self._propagator.propagate(dag)
        requested = outcomes or list(_DEFAULT_OUTCOMES)
        modified = {
            k: float(dag.nodes[k].get("propagated_belief", 0.5))
            for k in requested
            if k in dag
        }
        deltas = {k: round(modified.get(k, 0.0) - baseline.scores.get(k, 0.0), 4) for k in modified}

        return CausalInterventionResult(
            original_scores=baseline.scores,
            modified_scores=modified,
            deltas=deltas,
            estimate_confidence=baseline.estimate_confidence,
            label_type=baseline.label_type,
            label_confidence=baseline.label_confidence,
            causal_engine_version=self.engine_version,
            causal_model_version=baseline.causal_model_version,
        )

    async def explain(
        self,
        ctx: CausalRequestContext,
        result: CausalEstimateResult,
    ) -> CausalExplainResult:
        top = sorted(result.scores.items(), key=lambda kv: kv[1], reverse=True)[:3]
        factors = [f"{name}: {value:.1%}" for name, value in top]
        return CausalExplainResult(
            summary="Legacy DAG explanation generated from propagated node beliefs.",
            key_factors=factors,
            causal_engine_version=self.engine_version,
            causal_model_version=result.causal_model_version,
        )

    def _build_graph(self, ctx: CausalRequestContext):
        student_profile = {
            "gpa": 4.0 * ctx.student_features.get("student_gpa_norm", 0.75),
            "sat": 400 + 1200 * ctx.student_features.get("student_sat_norm", 0.58),
            "family_income": 100_000 * max(ctx.student_features.get("student_budget_norm", 0.4), 0.1),
        }
        school_data = {
            "acceptance_rate": ctx.school_features.get("school_acceptance_rate", 0.5),
            "research_expenditure": 300_000_000 * ctx.school_features.get("school_endowment_norm", 0.3),
            "avg_aid": 80_000 * (1.0 - ctx.school_features.get("school_net_price_norm", 0.5)),
            "location_tier": 5.0 * ctx.school_features.get("school_location_tier", 0.5),
            "career_services_rating": ctx.school_features.get("school_grad_rate", 0.5),
        }
        return self._builder.build_admission_dag(student_profile, school_data)
