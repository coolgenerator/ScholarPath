"""Domain-constrained DAG builder for college admissions causal reasoning.

Encodes expert knowledge about the causal structure of college admissions
outcomes as a directed acyclic graph with typed nodes and weighted edges.
"""

from __future__ import annotations

import logging
from typing import Any

import networkx as nx
import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Domain knowledge constants
# ---------------------------------------------------------------------------

_DOMAIN_NODES: list[dict[str, Any]] = [
    {
        "id": "student_ability",
        "label": "Student Ability (GPA, SAT)",
        "node_type": "confounder",
        "prior_belief": 0.5,
        "confidence": 0.7,
        "evidence_sources": ["college_scorecard", "nces"],
    },
    {
        "id": "school_selectivity",
        "label": "School Selectivity",
        "node_type": "treatment",
        "prior_belief": 0.5,
        "confidence": 0.6,
        "evidence_sources": ["usnews", "ipeds"],
    },
    {
        "id": "research_opportunities",
        "label": "Research Opportunities",
        "node_type": "mediator",
        "prior_belief": 0.4,
        "confidence": 0.5,
        "evidence_sources": ["nsf_herd", "faculty_data"],
    },
    {
        "id": "peer_network",
        "label": "Peer Network Quality",
        "node_type": "mediator",
        "prior_belief": 0.4,
        "confidence": 0.5,
        "evidence_sources": ["alumni_surveys"],
    },
    {
        "id": "brand_signal",
        "label": "Brand / Prestige Signal",
        "node_type": "mediator",
        "prior_belief": 0.4,
        "confidence": 0.5,
        "evidence_sources": ["employer_surveys", "linkedin"],
    },
    {
        "id": "career_services",
        "label": "Career Services Quality",
        "node_type": "mediator",
        "prior_belief": 0.4,
        "confidence": 0.4,
        "evidence_sources": ["nace_outcomes"],
    },
    {
        "id": "location_effect",
        "label": "Location / Regional Effect",
        "node_type": "mediator",
        "prior_belief": 0.4,
        "confidence": 0.5,
        "evidence_sources": ["bls", "census"],
    },
    {
        "id": "financial_aid",
        "label": "Financial Aid Package",
        "node_type": "observed",
        "prior_belief": 0.5,
        "confidence": 0.8,
        "evidence_sources": ["offer_letter", "ipeds"],
    },
    {
        "id": "financial_stress",
        "label": "Financial Stress",
        "node_type": "mediator",
        "prior_belief": 0.5,
        "confidence": 0.5,
        "evidence_sources": ["student_surveys"],
    },
    {
        "id": "family_ses",
        "label": "Family Socioeconomic Status",
        "node_type": "confounder",
        "prior_belief": 0.5,
        "confidence": 0.6,
        "evidence_sources": ["fafsa", "census"],
    },
    {
        "id": "school_choice",
        "label": "School Choice",
        "node_type": "treatment",
        "prior_belief": 0.5,
        "confidence": 0.5,
        "evidence_sources": [],
    },
    {
        "id": "admission_probability",
        "label": "Admission Probability",
        "node_type": "outcome",
        "prior_belief": 0.3,
        "confidence": 0.6,
        "evidence_sources": ["historical_admissions"],
    },
    {
        "id": "academic_outcome",
        "label": "Academic Outcome (GPA, Graduation)",
        "node_type": "outcome",
        "prior_belief": 0.5,
        "confidence": 0.5,
        "evidence_sources": ["college_scorecard"],
    },
    {
        "id": "career_outcome",
        "label": "Career Outcome (Earnings, Employment)",
        "node_type": "outcome",
        "prior_belief": 0.5,
        "confidence": 0.5,
        "evidence_sources": ["college_scorecard", "linkedin", "bls"],
    },
    {
        "id": "phd_probability",
        "label": "PhD / Graduate School Probability",
        "node_type": "outcome",
        "prior_belief": 0.2,
        "confidence": 0.5,
        "evidence_sources": ["nsf_sed", "college_scorecard"],
    },
    {
        "id": "life_satisfaction",
        "label": "Life Satisfaction",
        "node_type": "outcome",
        "prior_belief": 0.5,
        "confidence": 0.3,
        "evidence_sources": ["gallup_alumni"],
    },
]

_DOMAIN_EDGES: list[dict[str, Any]] = [
    # Required causal edges (domain knowledge)
    {
        "source": "student_ability",
        "target": "admission_probability",
        "mechanism": "Higher ability increases admission chances",
        "strength": 0.7,
        "evidence_score": 0.9,
        "causal_type": "direct",
    },
    {
        "source": "school_selectivity",
        "target": "admission_probability",
        "mechanism": "More selective schools have lower base admission rates",
        "strength": -0.6,
        "evidence_score": 0.95,
        "causal_type": "direct",
    },
    {
        "source": "research_opportunities",
        "target": "phd_probability",
        "mechanism": "Undergraduate research exposure increases graduate school interest and preparedness",
        "strength": 0.6,
        "evidence_score": 0.7,
        "causal_type": "direct",
    },
    {
        "source": "peer_network",
        "target": "career_outcome",
        "mechanism": "Strong peer networks provide referrals, co-founding opportunities, and information advantages",
        "strength": 0.5,
        "evidence_score": 0.6,
        "causal_type": "direct",
    },
    {
        "source": "brand_signal",
        "target": "career_outcome",
        "mechanism": "Employer screening uses school prestige as a quality signal",
        "strength": 0.4,
        "evidence_score": 0.7,
        "causal_type": "direct",
    },
    {
        "source": "career_services",
        "target": "career_outcome",
        "mechanism": "Career services facilitate employer connections and interview preparation",
        "strength": 0.3,
        "evidence_score": 0.5,
        "causal_type": "direct",
    },
    {
        "source": "location_effect",
        "target": "career_outcome",
        "mechanism": "Geographic proximity to industry hubs increases internship and job access",
        "strength": 0.4,
        "evidence_score": 0.6,
        "causal_type": "direct",
    },
    {
        "source": "financial_aid",
        "target": "financial_stress",
        "mechanism": "More financial aid reduces student debt burden and financial anxiety",
        "strength": -0.7,
        "evidence_score": 0.8,
        "causal_type": "direct",
    },
    {
        "source": "financial_stress",
        "target": "academic_outcome",
        "mechanism": "Financial stress diverts attention, forces excessive work hours, and hurts academic performance",
        "strength": -0.5,
        "evidence_score": 0.7,
        "causal_type": "direct",
    },
    {
        "source": "family_ses",
        "target": "school_choice",
        "mechanism": "Family SES constrains and shapes the set of schools a student considers (confounder)",
        "strength": 0.5,
        "evidence_score": 0.8,
        "causal_type": "confounding",
    },
    {
        "source": "student_ability",
        "target": "career_outcome",
        "mechanism": "Innate ability contributes to career success independent of school attended (confounder)",
        "strength": 0.5,
        "evidence_score": 0.8,
        "causal_type": "confounding",
    },
    # Additional structural edges
    {
        "source": "school_selectivity",
        "target": "research_opportunities",
        "mechanism": "More selective schools tend to have greater research infrastructure",
        "strength": 0.5,
        "evidence_score": 0.7,
        "causal_type": "mediated",
    },
    {
        "source": "school_selectivity",
        "target": "peer_network",
        "mechanism": "Selective admissions concentrate high-ability peers",
        "strength": 0.6,
        "evidence_score": 0.7,
        "causal_type": "mediated",
    },
    {
        "source": "school_selectivity",
        "target": "brand_signal",
        "mechanism": "Selectivity is a primary driver of perceived prestige",
        "strength": 0.7,
        "evidence_score": 0.8,
        "causal_type": "mediated",
    },
    {
        "source": "school_selectivity",
        "target": "career_services",
        "mechanism": "Well-resourced schools invest more in career services",
        "strength": 0.4,
        "evidence_score": 0.5,
        "causal_type": "mediated",
    },
    {
        "source": "academic_outcome",
        "target": "career_outcome",
        "mechanism": "Academic performance affects graduate school and first job prospects",
        "strength": 0.4,
        "evidence_score": 0.6,
        "causal_type": "direct",
    },
    {
        "source": "academic_outcome",
        "target": "phd_probability",
        "mechanism": "Strong academic performance is prerequisite for PhD admission",
        "strength": 0.5,
        "evidence_score": 0.8,
        "causal_type": "direct",
    },
    {
        "source": "career_outcome",
        "target": "life_satisfaction",
        "mechanism": "Career success is a component of overall life satisfaction",
        "strength": 0.4,
        "evidence_score": 0.5,
        "causal_type": "direct",
    },
    {
        "source": "financial_stress",
        "target": "life_satisfaction",
        "mechanism": "Debt burden and financial anxiety reduce well-being",
        "strength": -0.5,
        "evidence_score": 0.7,
        "causal_type": "direct",
    },
    {
        "source": "student_ability",
        "target": "academic_outcome",
        "mechanism": "Student ability predicts academic performance",
        "strength": 0.6,
        "evidence_score": 0.9,
        "causal_type": "direct",
    },
    {
        "source": "family_ses",
        "target": "financial_aid",
        "mechanism": "Need-based aid is determined by family income",
        "strength": -0.6,
        "evidence_score": 0.9,
        "causal_type": "direct",
    },
    {
        "source": "family_ses",
        "target": "student_ability",
        "mechanism": "SES correlates with educational resources and test prep access",
        "strength": 0.4,
        "evidence_score": 0.7,
        "causal_type": "confounding",
    },
]

# Edges that should never appear (nonsensical causal claims)
_FORBIDDEN_EDGES: set[tuple[str, str]] = {
    ("weather", "gpa"),
    ("campus_beauty", "career_outcome"),
}


class AdmissionDAGBuilder:
    """Constructs a domain-constrained causal DAG for college admissions.

    The builder encodes expert knowledge about the causal relationships
    between student characteristics, school attributes, and long-term
    outcomes. The resulting DAG can be personalized for individual students
    and used with the belief propagation and do-calculus engines.
    """

    def __init__(
        self,
        custom_nodes: list[dict[str, Any]] | None = None,
        custom_edges: list[dict[str, Any]] | None = None,
        forbidden_edges: set[tuple[str, str]] | None = None,
    ) -> None:
        self._custom_nodes = custom_nodes or []
        self._custom_edges = custom_edges or []
        self._forbidden_edges = _FORBIDDEN_EDGES | (forbidden_edges or set())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_admission_dag(
        self,
        student_profile: dict[str, Any] | None = None,
        school_data: dict[str, Any] | None = None,
    ) -> nx.DiGraph:
        """Build a causal DAG with domain knowledge priors.

        Parameters
        ----------
        student_profile:
            Optional student-specific data to personalize node beliefs.
            Keys may include ``gpa``, ``sat``, ``family_income``, ``major``, etc.
        school_data:
            Optional school-specific data. Keys may include
            ``acceptance_rate``, ``research_expenditure``, ``avg_aid``,
            ``location_tier``, etc.

        Returns
        -------
        nx.DiGraph
            A directed acyclic graph with typed, weighted nodes and edges.
        """
        dag = nx.DiGraph()

        # Add domain nodes
        for node_spec in _DOMAIN_NODES + self._custom_nodes:
            node_id = node_spec["id"]
            attrs = {k: v for k, v in node_spec.items() if k != "id"}
            # Default gate type based on node_type
            attrs.setdefault("gate_type", "noisy_or")
            dag.add_node(node_id, **attrs)

        # Add domain edges
        for edge_spec in _DOMAIN_EDGES + self._custom_edges:
            src, tgt = edge_spec["source"], edge_spec["target"]
            if (src, tgt) in self._forbidden_edges:
                logger.warning(
                    "Skipping forbidden edge %s -> %s", src, tgt
                )
                continue
            attrs = {
                k: v
                for k, v in edge_spec.items()
                if k not in ("source", "target")
            }
            dag.add_edge(src, tgt, **attrs)

        # Validate acyclicity
        if not nx.is_directed_acyclic_graph(dag):
            cycles = list(nx.simple_cycles(dag))
            raise ValueError(
                f"Domain DAG contains cycles, which violates causal assumptions: {cycles}"
            )

        # Personalize if data provided
        if student_profile or school_data:
            self.personalize_dag(dag, student_profile or {}, school_data or {})

        return dag

    def personalize_dag(
        self,
        dag: nx.DiGraph,
        student_profile: dict[str, Any],
        school_data: dict[str, Any] | None = None,
    ) -> nx.DiGraph:
        """Adjust node beliefs based on specific student and school data.

        Parameters
        ----------
        dag:
            The causal DAG to personalize (modified in-place and returned).
        student_profile:
            Student data dict. Recognised keys: ``gpa`` (0-4), ``sat`` (400-1600),
            ``family_income`` (USD), ``intended_major``, ``state``.
        school_data:
            School data dict. Recognised keys: ``acceptance_rate`` (0-1),
            ``research_expenditure`` (USD), ``avg_aid`` (USD),
            ``location_tier`` (1-5), ``career_services_rating`` (0-1).

        Returns
        -------
        nx.DiGraph
            The personalized DAG.
        """
        school_data = school_data or {}

        # --- Student ability ---
        if "gpa" in student_profile or "sat" in student_profile:
            gpa_norm = student_profile.get("gpa", 3.0) / 4.0
            sat_norm = (student_profile.get("sat", 1100) - 400) / 1200
            ability = float(np.clip(0.6 * gpa_norm + 0.4 * sat_norm, 0, 1))
            if dag.has_node("student_ability"):
                dag.nodes["student_ability"]["prior_belief"] = ability
                dag.nodes["student_ability"]["confidence"] = 0.85

        # --- Family SES ---
        if "family_income" in student_profile:
            income = student_profile["family_income"]
            # Sigmoid-style mapping: $50k -> ~0.35, $100k -> ~0.55, $200k -> ~0.75
            ses = float(np.clip(1.0 / (1.0 + np.exp(-0.00003 * (income - 100_000))), 0, 1))
            if dag.has_node("family_ses"):
                dag.nodes["family_ses"]["prior_belief"] = ses
                dag.nodes["family_ses"]["confidence"] = 0.8

        # --- School selectivity ---
        if "acceptance_rate" in school_data:
            selectivity = float(np.clip(1.0 - school_data["acceptance_rate"], 0, 1))
            if dag.has_node("school_selectivity"):
                dag.nodes["school_selectivity"]["prior_belief"] = selectivity
                dag.nodes["school_selectivity"]["confidence"] = 0.9

        # --- Research opportunities ---
        if "research_expenditure" in school_data:
            # Normalize: $100M -> ~0.5, $500M -> ~0.8
            exp = school_data["research_expenditure"]
            research = float(np.clip(1.0 / (1.0 + np.exp(-0.000000005 * (exp - 200_000_000))), 0, 1))
            if dag.has_node("research_opportunities"):
                dag.nodes["research_opportunities"]["prior_belief"] = research
                dag.nodes["research_opportunities"]["confidence"] = 0.7

        # --- Financial aid ---
        if "avg_aid" in school_data:
            aid_norm = float(np.clip(school_data["avg_aid"] / 80_000, 0, 1))
            if dag.has_node("financial_aid"):
                dag.nodes["financial_aid"]["prior_belief"] = aid_norm
                dag.nodes["financial_aid"]["confidence"] = 0.85

        # --- Location effect ---
        if "location_tier" in school_data:
            loc = float(np.clip(school_data["location_tier"] / 5.0, 0, 1))
            if dag.has_node("location_effect"):
                dag.nodes["location_effect"]["prior_belief"] = loc
                dag.nodes["location_effect"]["confidence"] = 0.6

        # --- Career services ---
        if "career_services_rating" in school_data:
            if dag.has_node("career_services"):
                dag.nodes["career_services"]["prior_belief"] = float(
                    np.clip(school_data["career_services_rating"], 0, 1)
                )
                dag.nodes["career_services"]["confidence"] = 0.5

        return dag
