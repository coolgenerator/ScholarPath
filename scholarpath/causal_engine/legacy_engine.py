"""Legacy causal adapter built on the in-repo DAG engine."""

from __future__ import annotations

from scholarpath.causal import AdmissionDAGBuilder, NoisyORPropagator
from scholarpath.causal_engine.types import (
    CausalEstimateResult,
    CausalExplainResult,
    CausalInterventionResult,
    CausalRequestContext,
)

_OUTCOMES = [
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
]


class LegacyCausalEngine:
    """Compatibility adapter around the existing DAG implementation."""

    engine_name = "legacy"

    def __init__(self) -> None:
        self._builder = AdmissionDAGBuilder()
        self._propagator = NoisyORPropagator()

    async def estimate(self, request: CausalRequestContext) -> CausalEstimateResult:
        dag = self._build(request)
        scores = _extract_scores(dag)
        return CausalEstimateResult(
            scores=scores,
            confidence=0.85,
            engine_used=self.engine_name,
            model_version="legacy",
            metadata={"context": request.context},
        )

    async def intervene(
        self,
        request: CausalRequestContext,
        interventions: dict[str, float],
    ) -> CausalInterventionResult:
        base = self._build(request)
        mod = self._build(request, interventions=interventions)
        original = _extract_scores(base)
        modified = _extract_scores(mod)
        deltas = {
            key: round(modified.get(key, 0.0) - original.get(key, 0.0), 6)
            for key in original
        }
        return CausalInterventionResult(
            original_scores=original,
            modified_scores=modified,
            deltas=deltas,
            engine_used=self.engine_name,
            model_version="legacy",
            metadata={"context": request.context},
        )

    async def explain(self, request: CausalRequestContext) -> CausalExplainResult:
        estimate = await self.estimate(request)
        top = sorted(estimate.scores.items(), key=lambda item: item[1], reverse=True)
        text = (
            "Legacy causal estimate generated. "
            f"Top outcomes: {', '.join(f'{k}={v:.2f}' for k, v in top[:3])}."
        )
        return CausalExplainResult(
            explanation=text,
            reasons=[{"type": "legacy_summary", "scores": estimate.scores}],
            engine_used=self.engine_name,
            model_version="legacy",
        )

    def _build(
        self,
        request: CausalRequestContext,
        *,
        interventions: dict[str, float] | None = None,
    ):
        student_profile = {
            "gpa": request.student_features.get("student_gpa_norm", 0.0) * 4.0,
            "sat": int(request.student_features.get("student_sat_norm", 0.0) * 1600),
            "family_income": int(request.student_features.get("student_budget_norm", 0.0) * 200000),
        }
        school_data = {
            "acceptance_rate": request.school_features.get("school_acceptance_rate", 0.5),
            "avg_aid": max(0.0, 60000.0 * (1 - request.school_features.get("school_net_price_norm", 0.5))),
            "research_expenditure": request.school_features.get("school_endowment_norm", 0.0) * 1_000_000,
            "location_tier": request.school_features.get("school_location_tier", 0.6) * 5,
        }
        dag = self._builder.build_admission_dag(student_profile, school_data)
        if interventions:
            for node, value in interventions.items():
                if node not in dag:
                    continue
                dag.nodes[node]["prior_belief"] = max(0.0, min(1.0, float(value)))
                parents = list(dag.predecessors(node))
                for parent in parents:
                    dag.remove_edge(parent, node)
        return self._propagator.propagate(dag)


def _extract_scores(dag) -> dict[str, float]:
    out: dict[str, float] = {}
    for key in _OUTCOMES:
        out[key] = round(
            float(dag.nodes.get(key, {}).get("propagated_belief", 0.5)),
            6,
        )
    return out
