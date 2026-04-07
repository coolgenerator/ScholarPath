"""Standalone Causal Inference Engine V2.

This module is intentionally not wired into the current production routes.
It is built as a drop-in future replacement surface.
"""

from __future__ import annotations

from collections.abc import Mapping

import networkx as nx

from scholarpath.causal import AdmissionDAGBuilder, NoisyORPropagator
from scholarpath.causal_v2.contracts import (
    CausalDimensionScores,
    CausalEvaluationResult,
    CausalOutcomeScores,
    CausalScenarioComparisonResult,
    CausalScenarioInput,
    CausalScenarioResult,
    CausalSchoolProfile,
    CausalStudentProfile,
    CausalWhatIfResult,
)

_OUTCOME_NODES = (
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "phd_probability",
    "life_satisfaction",
    "financial_stress",
)

_DIMENSION_WEIGHTS: dict[str, float] = {
    "academic": 0.30,
    "financial": 0.25,
    "career": 0.25,
    "life": 0.20,
}

_TIER_THRESHOLDS: list[tuple[float, str]] = [
    (0.85, "likely"),
    (0.60, "safety"),
    (0.20, "target"),
    (0.00, "reach"),
]

_SCENARIO_SCORING_WEIGHTS: dict[str, float] = {
    "admission_probability": 0.30,
    "academic_outcome": 0.20,
    "career_outcome": 0.25,
    "life_satisfaction": 0.20,
    "phd_probability": 0.05,
    # financial_stress is penalized separately with minus sign.
}


class CausalEngineV2:
    """Future-replacement causal engine with explicit typed contracts."""

    def __init__(
        self,
        *,
        builder: AdmissionDAGBuilder | None = None,
        propagator: NoisyORPropagator | None = None,
        ci_samples: int = 300,
    ) -> None:
        self.builder = builder or AdmissionDAGBuilder()
        self.propagator = propagator or NoisyORPropagator()
        self.ci_samples = ci_samples

    def evaluate(
        self,
        student_profile: CausalStudentProfile,
        school_profile: CausalSchoolProfile,
        *,
        include_confidence: bool = True,
    ) -> CausalEvaluationResult:
        """Evaluate one student-school pair and return typed scores."""
        dag = self._build_propagated_dag(student_profile, school_profile)
        outcomes = self._extract_outcomes(dag)
        dimensions = self._compute_dimensions(outcomes)
        tier = self._overall_to_tier(dimensions.overall)

        ci: dict[str, dict[str, float]] | None = None
        if include_confidence:
            raw_ci = self.propagator.compute_confidence_intervals(
                dag,
                n_samples=self.ci_samples,
            )
            ci = {node: raw_ci[node] for node in _OUTCOME_NODES if node in raw_ci}

        return CausalEvaluationResult(
            dimensions=dimensions,
            outcomes=outcomes,
            tier=tier,
            confidence_interval=ci,
        )

    def what_if(
        self,
        student_profile: CausalStudentProfile,
        school_profile: CausalSchoolProfile,
        interventions: dict[str, float],
    ) -> CausalWhatIfResult:
        """Run one what-if scenario with explicit interventions."""
        base_dag = self._build_propagated_dag(student_profile, school_profile)
        original = self._extract_outcomes(base_dag).as_dict()

        modified_dag = base_dag.copy()
        self._apply_interventions(modified_dag, interventions)
        modified_dag = self.propagator.propagate(modified_dag)
        modified = self._extract_outcomes(modified_dag).as_dict()

        deltas = {
            node: round(modified[node] - original[node], 4)
            for node in _OUTCOME_NODES
        }
        return CausalWhatIfResult(
            original_scores=original,
            modified_scores=modified,
            deltas=deltas,
        )

    def compare_scenarios(
        self,
        student_profile: CausalStudentProfile,
        scenarios: list[CausalScenarioInput],
    ) -> CausalScenarioComparisonResult:
        """Compare two or more scenarios with explicit school mapping."""
        if len(scenarios) < 2:
            raise ValueError("compare_scenarios requires at least 2 scenarios")

        scenario_results: list[CausalScenarioResult] = []
        for idx, scenario in enumerate(scenarios):
            label = scenario.label or f"Scenario {idx + 1}"
            what_if = self.what_if(
                student_profile,
                scenario.school_profile,
                dict(scenario.interventions),
            )
            delta_score = self._score_scenario_delta(what_if.deltas)
            scenario_results.append(
                CausalScenarioResult(
                    label=label,
                    school_profile=scenario.school_profile,
                    what_if=what_if,
                    delta_score=delta_score,
                )
            )

        best = max(scenario_results, key=lambda item: item.delta_score)
        summary = (
            f"{best.label} has the strongest aggregate causal uplift "
            f"(delta_score={best.delta_score:.4f}) across key outcomes."
        )
        return CausalScenarioComparisonResult(
            scenarios=scenario_results,
            best_scenario_label=best.label,
            summary=summary,
        )

    def _build_propagated_dag(
        self,
        student_profile: CausalStudentProfile,
        school_profile: CausalSchoolProfile,
    ) -> nx.DiGraph:
        student_raw = {
            "gpa": student_profile.gpa,
            "sat": student_profile.sat,
            "family_income": student_profile.family_income,
        }
        school_raw = {
            "acceptance_rate": school_profile.acceptance_rate,
            "research_expenditure": school_profile.research_expenditure,
            "avg_aid": school_profile.avg_aid,
            "location_tier": school_profile.location_tier,
            "career_services_rating": school_profile.career_services_rating,
        }
        student = {key: value for key, value in student_raw.items() if value is not None}
        school = {key: value for key, value in school_raw.items() if value is not None}
        dag = self.builder.build_admission_dag(student, school)
        return self.propagator.propagate(dag)

    def _extract_outcomes(self, dag: nx.DiGraph) -> CausalOutcomeScores:
        payload = {
            node: float(
                dag.nodes[node].get(
                    "propagated_belief",
                    dag.nodes[node].get("prior_belief", 0.5),
                )
            )
            for node in _OUTCOME_NODES
        }
        return CausalOutcomeScores(**payload)

    def _compute_dimensions(self, outcomes: CausalOutcomeScores) -> CausalDimensionScores:
        academic = outcomes.academic_outcome
        financial = 1.0 - outcomes.financial_stress
        career = outcomes.career_outcome
        life = outcomes.life_satisfaction
        overall = (
            _DIMENSION_WEIGHTS["academic"] * academic
            + _DIMENSION_WEIGHTS["financial"] * financial
            + _DIMENSION_WEIGHTS["career"] * career
            + _DIMENSION_WEIGHTS["life"] * life
        )
        return CausalDimensionScores(
            academic=round(academic, 4),
            financial=round(financial, 4),
            career=round(career, 4),
            life=round(life, 4),
            overall=round(overall, 4),
        )

    def _overall_to_tier(self, overall_score: float) -> str:
        for threshold, tier in _TIER_THRESHOLDS:
            if overall_score >= threshold:
                return tier
        return "reach"

    def _apply_interventions(
        self,
        dag: nx.DiGraph,
        interventions: Mapping[str, float],
    ) -> None:
        for node, value in interventions.items():
            if node not in dag:
                raise KeyError(f"Intervention node '{node}' not found in DAG")
            if not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"Intervention value for '{node}' must be in [0, 1], got {value}"
                )

            parents = list(dag.predecessors(node))
            for parent in parents:
                dag.remove_edge(parent, node)

            dag.nodes[node]["prior_belief"] = float(value)
            dag.nodes[node]["confidence"] = 1.0
            dag.nodes[node]["observed"] = True
            dag.nodes[node]["intervened"] = True

    def _score_scenario_delta(self, deltas: Mapping[str, float]) -> float:
        positive = sum(
            _SCENARIO_SCORING_WEIGHTS[node] * float(deltas.get(node, 0.0))
            for node in _SCENARIO_SCORING_WEIGHTS
        )
        # Lower financial stress is better, so subtract delta(financial_stress).
        stress_penalty = float(deltas.get("financial_stress", 0.0)) * 0.25
        return round(positive - stress_penalty, 4)
