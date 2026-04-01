"""Tests for the ScholarPath causal reasoning engine.

Covers: DAG builder, belief propagation, do-calculus, mediation analysis,
backdoor adjustment, Go/No-Go scorer, and graph serialization.
"""

from __future__ import annotations

import copy

import networkx as nx
import numpy as np
import pytest

from scholarpath.causal.dag_builder import AdmissionDAGBuilder
from scholarpath.causal.belief_propagation import NoisyORPropagator
from scholarpath.causal.do_calculus import DoCalculusEngine
from scholarpath.causal.mediation import MediationAnalyzer
from scholarpath.causal.backdoor import BackdoorAdjuster
from scholarpath.causal.go_no_go import GoNoGoScorer, _score_to_tier
from scholarpath.causal.graph_store import (
    serialize_graph,
    deserialize_graph,
    graph_to_cytoscape,
    graph_diff,
)


# =========================================================================
# Fixtures
# =========================================================================

@pytest.fixture
def builder():
    return AdmissionDAGBuilder()


@pytest.fixture
def base_dag(builder):
    """Build a default (un-personalized) DAG."""
    return builder.build_admission_dag()


@pytest.fixture
def personalized_dag(builder):
    """Build a DAG personalized for a specific student + school."""
    student = {"gpa": 3.8, "sat": 1480, "family_income": 80_000}
    school = {
        "acceptance_rate": 0.12,
        "research_expenditure": 300_000_000,
        "avg_aid": 35_000,
        "location_tier": 4,
        "career_services_rating": 0.75,
    }
    return builder.build_admission_dag(student, school)


@pytest.fixture
def propagator():
    return NoisyORPropagator()


@pytest.fixture
def propagated_dag(base_dag, propagator):
    """A DAG that has already been through belief propagation."""
    return propagator.propagate(base_dag)


@pytest.fixture
def do_engine():
    return DoCalculusEngine()


@pytest.fixture
def mediation():
    return MediationAnalyzer()


@pytest.fixture
def backdoor():
    return BackdoorAdjuster()


@pytest.fixture
def scorer():
    return GoNoGoScorer()


# =========================================================================
# 1. DAG Builder
# =========================================================================

class TestDAGBuilder:
    def test_builds_valid_dag(self, base_dag):
        assert isinstance(base_dag, nx.DiGraph)
        assert nx.is_directed_acyclic_graph(base_dag)

    def test_has_expected_nodes(self, base_dag):
        expected = {
            "student_ability", "school_selectivity", "research_opportunities",
            "peer_network", "brand_signal", "career_services",
            "location_effect", "financial_aid", "financial_stress",
            "family_ses", "school_choice", "admission_probability",
            "academic_outcome", "career_outcome", "phd_probability",
            "life_satisfaction",
        }
        assert expected == set(base_dag.nodes)

    def test_node_attributes(self, base_dag):
        for node in base_dag.nodes:
            attrs = base_dag.nodes[node]
            assert "prior_belief" in attrs, f"Node {node} missing prior_belief"
            assert "node_type" in attrs, f"Node {node} missing node_type"
            assert 0 <= attrs["prior_belief"] <= 1

    def test_edge_attributes(self, base_dag):
        for u, v in base_dag.edges:
            attrs = base_dag.edges[u, v]
            assert "strength" in attrs, f"Edge {u}->{v} missing strength"
            assert "mechanism" in attrs, f"Edge {u}->{v} missing mechanism"

    def test_personalization_adjusts_beliefs(self, builder):
        dag = builder.build_admission_dag()
        orig_ability = dag.nodes["student_ability"]["prior_belief"]

        student = {"gpa": 4.0, "sat": 1600}
        builder.personalize_dag(dag, student)

        # Perfect scores should produce high ability belief
        assert dag.nodes["student_ability"]["prior_belief"] > orig_ability
        assert dag.nodes["student_ability"]["prior_belief"] > 0.8

    def test_personalization_school_selectivity(self, builder):
        dag = builder.build_admission_dag()
        school = {"acceptance_rate": 0.05}
        builder.personalize_dag(dag, {}, school)

        # 5% acceptance rate → very high selectivity
        assert dag.nodes["school_selectivity"]["prior_belief"] > 0.9

    def test_custom_nodes_and_edges(self):
        custom_node = {
            "id": "test_node",
            "label": "Test Node",
            "node_type": "mediator",
            "prior_belief": 0.5,
            "confidence": 0.5,
            "evidence_sources": [],
        }
        custom_edge = {
            "source": "test_node",
            "target": "career_outcome",
            "mechanism": "Test mechanism",
            "strength": 0.3,
            "evidence_score": 0.5,
            "causal_type": "direct",
        }
        builder = AdmissionDAGBuilder(
            custom_nodes=[custom_node],
            custom_edges=[custom_edge],
        )
        dag = builder.build_admission_dag()
        assert "test_node" in dag
        assert dag.has_edge("test_node", "career_outcome")

    def test_forbidden_edges_are_skipped(self):
        forbidden_edge = {
            "source": "weather",
            "target": "gpa",
            "mechanism": "Nonsense",
            "strength": 0.5,
            "evidence_score": 0.5,
            "causal_type": "direct",
        }
        builder = AdmissionDAGBuilder(custom_edges=[forbidden_edge])
        dag = builder.build_admission_dag()
        assert not dag.has_edge("weather", "gpa")

    def test_cycle_detection(self):
        """Adding edges that would create a cycle should raise."""
        edges = [
            {"source": "career_outcome", "target": "student_ability",
             "mechanism": "Loop", "strength": 0.5, "evidence_score": 0.5,
             "causal_type": "direct"},
        ]
        builder = AdmissionDAGBuilder(custom_edges=edges)
        with pytest.raises(ValueError, match="cycles"):
            builder.build_admission_dag()


# =========================================================================
# 2. Belief Propagation
# =========================================================================

class TestBeliefPropagation:
    def test_root_nodes_keep_prior(self, base_dag, propagator):
        dag = propagator.propagate(base_dag)
        for node in dag.nodes:
            parents = list(dag.predecessors(node))
            if not parents:
                assert dag.nodes[node]["propagated_belief"] == dag.nodes[node]["prior_belief"]

    def test_all_nodes_have_propagated_belief(self, propagated_dag):
        for node in propagated_dag.nodes:
            assert "propagated_belief" in propagated_dag.nodes[node]
            belief = propagated_dag.nodes[node]["propagated_belief"]
            assert 0 <= belief <= 1, f"Node {node} belief {belief} out of range"

    def test_propagate_with_evidence(self, base_dag, propagator):
        evidence = {"student_ability": 0.9, "financial_aid": 0.8}
        dag = propagator.propagate_with_evidence(base_dag, evidence)

        assert dag.nodes["student_ability"]["propagated_belief"] == 0.9
        assert dag.nodes["financial_aid"]["propagated_belief"] == 0.8

    def test_high_ability_increases_outcomes(self, builder, propagator):
        """A high-ability student should have better outcome beliefs."""
        dag_low = builder.build_admission_dag({"gpa": 2.0, "sat": 800})
        dag_high = builder.build_admission_dag({"gpa": 4.0, "sat": 1600})

        propagator.propagate(dag_low)
        propagator.propagate(dag_high)

        # student_ability directly affects admission_probability and career_outcome
        for outcome in ["career_outcome", "admission_probability"]:
            low_b = dag_low.nodes[outcome]["propagated_belief"]
            high_b = dag_high.nodes[outcome]["propagated_belief"]
            assert high_b >= low_b, f"High ability should not decrease {outcome}"

    def test_leak_probability_validation(self):
        with pytest.raises(ValueError):
            NoisyORPropagator(leak_probability=-0.1)
        with pytest.raises(ValueError):
            NoisyORPropagator(leak_probability=1.5)

    def test_confidence_intervals(self, propagated_dag, propagator):
        ci = propagator.compute_confidence_intervals(propagated_dag, n_samples=100)
        for node in propagated_dag.nodes:
            assert node in ci
            assert ci[node]["ci_lower"] <= ci[node]["mean"] <= ci[node]["ci_upper"]
            assert ci[node]["std"] >= 0

    def test_evidence_out_of_range_raises(self, base_dag, propagator):
        with pytest.raises(ValueError, match="must be in"):
            propagator.propagate_with_evidence(base_dag, {"student_ability": 1.5})


# =========================================================================
# 3. Do-Calculus
# =========================================================================

class TestDoCalculus:
    def test_do_intervention_removes_parents(self, propagated_dag, do_engine):
        node = "school_selectivity"
        original_parents = list(propagated_dag.predecessors(node))

        intervened = do_engine.do_intervention(propagated_dag, node, 0.9)

        assert list(intervened.predecessors(node)) == []
        assert intervened.nodes[node]["propagated_belief"] == 0.9
        assert intervened.nodes[node]["intervened"] is True

    def test_do_preserves_original(self, propagated_dag, do_engine):
        orig_belief = propagated_dag.nodes["school_selectivity"].get("propagated_belief")
        do_engine.do_intervention(propagated_dag, "school_selectivity", 0.9)
        assert propagated_dag.nodes["school_selectivity"].get("propagated_belief") == orig_belief

    def test_do_invalid_node(self, propagated_dag, do_engine):
        with pytest.raises(KeyError):
            do_engine.do_intervention(propagated_dag, "nonexistent", 0.5)

    def test_do_invalid_value(self, propagated_dag, do_engine):
        with pytest.raises(ValueError):
            do_engine.do_intervention(propagated_dag, "school_selectivity", 1.5)

    def test_average_treatment_effect(self, propagated_dag, do_engine):
        ate = do_engine.average_treatment_effect(
            propagated_dag,
            treatment_node="school_selectivity",
            outcome_node="career_outcome",
            n_bootstrap=50,
        )
        assert "ate" in ate
        assert "ci_lower" in ate
        assert "ci_upper" in ate
        assert ate["ci_lower"] <= ate["ate"] <= ate["ci_upper"]

    def test_what_if_multiple_interventions(self, propagated_dag, do_engine):
        interventions = {
            "student_ability": 0.9,
            "financial_aid": 0.8,
        }
        result = do_engine.what_if(propagated_dag, interventions)
        assert isinstance(result, dict)
        assert "career_outcome" in result
        assert all(0 <= v <= 1 for v in result.values())

    def test_compare_schools(self, propagated_dag, do_engine):
        student = {"gpa": 3.8, "sat": 1480}
        school_a = {"acceptance_rate": 0.05, "avg_aid": 40_000}
        school_b = {"acceptance_rate": 0.50, "avg_aid": 20_000}

        result = do_engine.compare_schools(
            propagated_dag, student, school_a, school_b
        )
        assert "school_a_outcome" in result
        assert "school_b_outcome" in result
        assert "favoured" in result
        assert result["favoured"] in ("school_a", "school_b", "equal")

    def test_sensitivity_analysis(self, propagated_dag, do_engine):
        results = do_engine.sensitivity_analysis(
            propagated_dag,
            node_id="student_ability",
            outcome_node="career_outcome",
            n_steps=5,
        )
        assert len(results) == 5
        # Outcomes should generally increase with student ability
        first_outcome = results[0]["outcome_value"]
        last_outcome = results[-1]["outcome_value"]
        assert last_outcome >= first_outcome


# =========================================================================
# 4. Mediation Analysis
# =========================================================================

class TestMediationAnalysis:
    def test_total_effect(self, propagated_dag, mediation):
        te = mediation.total_effect(
            propagated_dag, "school_selectivity", "career_outcome"
        )
        assert isinstance(te, float)

    def test_direct_effect(self, propagated_dag, mediation):
        de = mediation.direct_effect(
            propagated_dag,
            "school_selectivity",
            "career_outcome",
            mediators=["peer_network", "brand_signal"],
        )
        assert isinstance(de, float)

    def test_indirect_effect(self, propagated_dag, mediation):
        ie = mediation.indirect_effect(
            propagated_dag,
            "school_selectivity",
            "peer_network",
            "career_outcome",
        )
        assert isinstance(ie, float)

    def test_decompose_pathways(self, propagated_dag, mediation):
        pathways = mediation.decompose_pathways(
            propagated_dag, "school_selectivity", "career_outcome"
        )
        assert isinstance(pathways, list)
        assert len(pathways) > 0
        for p in pathways:
            assert "path" in p
            assert "effect" in p
            assert "percentage" in p
            assert "mechanism" in p

    def test_find_all_paths(self, propagated_dag, mediation):
        paths = mediation.find_all_paths(
            propagated_dag, "school_selectivity", "career_outcome"
        )
        assert len(paths) > 0
        for path in paths:
            assert path[0] == "school_selectivity"
            assert path[-1] == "career_outcome"

    def test_invalid_node_raises(self, propagated_dag, mediation):
        with pytest.raises(KeyError):
            mediation.total_effect(propagated_dag, "nonexistent", "career_outcome")

    def test_te_equals_de_plus_ie(self, propagated_dag, mediation):
        """Total effect ≈ direct effect + indirect effects (approximately)."""
        treatment = "school_selectivity"
        outcome = "career_outcome"
        mediator = "peer_network"

        te = mediation.total_effect(propagated_dag, treatment, outcome)
        de = mediation.direct_effect(propagated_dag, treatment, outcome, mediators=[mediator])
        ie = mediation.indirect_effect(propagated_dag, treatment, mediator, outcome)

        # TE ≈ DE + IE (with some tolerance due to Noisy-OR nonlinearity)
        assert abs(te - (de + ie)) < 0.15, (
            f"TE={te:.4f}, DE={de:.4f}, IE={ie:.4f}, diff={abs(te - de - ie):.4f}"
        )


# =========================================================================
# 5. Backdoor Adjustment
# =========================================================================

class TestBackdoorAdjustment:
    def test_identify_confounders(self, propagated_dag, backdoor):
        # student_ability is a confounder for itself -> career_outcome
        # and itself -> admission_probability, both direct paths exist
        confounders = backdoor.identify_confounders(
            propagated_dag, "school_selectivity", "career_outcome"
        )
        assert isinstance(confounders, list)

        # Also test a pair known to have confounders:
        # school_choice -> outcomes is confounded by family_ses
        confounders2 = backdoor.identify_confounders(
            propagated_dag, "school_choice", "career_outcome"
        )
        assert isinstance(confounders2, list)

    def test_find_backdoor_set(self, propagated_dag, backdoor):
        adjustment_set = backdoor.find_backdoor_set(
            propagated_dag, "school_selectivity", "career_outcome"
        )
        assert isinstance(adjustment_set, set)

    def test_valid_adjustment_set(self, propagated_dag, backdoor):
        adj_set = backdoor.find_backdoor_set(
            propagated_dag, "school_selectivity", "career_outcome"
        )
        if adj_set:
            assert backdoor.is_valid_adjustment_set(
                propagated_dag, "school_selectivity", "career_outcome", adj_set
            )

    def test_descendants_not_in_adjustment(self, propagated_dag, backdoor):
        """Adjustment set should not contain descendants of treatment."""
        adj_set = backdoor.find_backdoor_set(
            propagated_dag, "school_selectivity", "career_outcome"
        )
        descendants = nx.descendants(propagated_dag, "school_selectivity")
        assert not adj_set & descendants

    def test_adjusted_effect(self, propagated_dag, backdoor):
        adj_set = backdoor.find_backdoor_set(
            propagated_dag, "school_selectivity", "career_outcome"
        )
        data = {node: 0.5 for node in adj_set}
        result = backdoor.adjusted_effect(
            propagated_dag, "school_selectivity", "career_outcome",
            adj_set, data,
        )
        assert "adjusted_effect" in result
        assert "adjustment_set" in result


# =========================================================================
# 6. Go/No-Go Scorer
# =========================================================================

class TestGoNoGoScorer:
    def test_compute_score(self, propagated_dag, scorer):
        eval_data = {
            "academic": 0.8,
            "financial": 0.7,
            "career": 0.85,
            "life": 0.75,
        }
        result = scorer.compute_score(eval_data, propagated_dag)
        assert "overall_score" in result
        assert "tier" in result
        assert "recommendation" in result
        assert "sub_scores" in result
        assert "confidence_interval" in result
        assert 0 <= result["overall_score"] <= 1

    def test_score_to_tier(self):
        assert _score_to_tier(0.85) == "strongly_recommend"
        assert _score_to_tier(0.65) == "recommend"
        assert _score_to_tier(0.45) == "neutral"
        assert _score_to_tier(0.20) == "not_recommend"

    def test_high_scores_yield_strong_recommendation(self, propagated_dag, scorer):
        eval_data = {"academic": 0.95, "financial": 0.95, "career": 0.95, "life": 0.95}
        result = scorer.compute_score(eval_data, propagated_dag)
        assert result["tier"] in ("strongly_recommend", "recommend")

    def test_low_scores_yield_negative_recommendation(self, propagated_dag, scorer):
        eval_data = {"academic": 0.1, "financial": 0.1, "career": 0.1, "life": 0.1}
        result = scorer.compute_score(eval_data, propagated_dag)
        assert result["tier"] in ("not_recommend", "neutral")

    def test_compare_offers(self, propagated_dag, scorer):
        offers = [
            {"school_name": "School A", "academic": 0.9, "financial": 0.8,
             "career": 0.85, "life": 0.7},
            {"school_name": "School B", "academic": 0.6, "financial": 0.9,
             "career": 0.5, "life": 0.8},
        ]
        ranked = scorer.compare_offers(offers, propagated_dag)
        assert len(ranked) == 2
        assert ranked[0]["rank"] == 1
        assert ranked[1]["rank"] == 2
        # Higher scorer should be first
        assert ranked[0]["overall_score"] >= ranked[1]["overall_score"]

    def test_generate_key_factors(self, propagated_dag, scorer):
        eval_data = {"academic": 0.8, "financial": 0.7, "career": 0.85, "life": 0.75}
        scores = scorer.compute_score(eval_data, propagated_dag)
        factors = scorer.generate_key_factors(scores, propagated_dag)
        assert len(factors) <= 10
        for f in factors:
            assert "node_id" in f
            assert "impact" in f
            assert "direction" in f

    def test_run_automatic_what_ifs(self, propagated_dag, scorer):
        student = {"gpa": 3.5, "sat": 1400}
        school = {"avg_aid": 20_000, "location_tier": 3}
        base_score = 0.6

        scenarios = scorer.run_automatic_what_ifs(
            propagated_dag, student, school, base_score
        )
        assert len(scenarios) == 4
        for s in scenarios:
            assert "scenario" in s
            assert "new_score" in s
            assert "delta" in s

    def test_custom_weights(self, propagated_dag, scorer):
        eval_data = {"academic": 0.8, "financial": 0.7, "career": 0.85, "life": 0.75}
        weights = {"academic": 0.5, "financial": 0.1, "career": 0.3, "life": 0.1}

        result_custom = scorer.compute_score(eval_data, propagated_dag, weights=weights)
        result_default = scorer.compute_score(eval_data, propagated_dag)

        # Different weights should generally produce different scores
        assert result_custom["overall_score"] != result_default["overall_score"]


# =========================================================================
# 7. Graph Store (Serialization)
# =========================================================================

class TestGraphStore:
    def test_serialize_roundtrip(self, propagated_dag):
        serialized = serialize_graph(propagated_dag)
        assert "nodes" in serialized
        assert "edges" in serialized
        assert "metadata" in serialized
        assert serialized["metadata"]["is_dag"] is True

        restored = deserialize_graph(serialized)
        assert set(restored.nodes) == set(propagated_dag.nodes)
        assert set(restored.edges) == set(propagated_dag.edges)

    def test_cytoscape_export(self, propagated_dag):
        cy = graph_to_cytoscape(propagated_dag)
        assert "elements" in cy
        assert "nodes" in cy["elements"]
        assert "edges" in cy["elements"]

        for node in cy["elements"]["nodes"]:
            assert "data" in node
            assert "id" in node["data"]
            assert "color" in node["data"]

        for edge in cy["elements"]["edges"]:
            assert "data" in edge
            assert "source" in edge["data"]
            assert "target" in edge["data"]
            assert "width" in edge["data"]

    def test_graph_diff(self, propagated_dag, do_engine):
        intervened = do_engine.do_intervention(
            propagated_dag, "student_ability", 0.9
        )
        diff = graph_diff(propagated_dag, intervened)

        assert "belief_changes" in diff
        assert "edges_removed" in diff
        # student_ability was intervened on, so incoming edges were removed
        assert len(diff["edges_removed"]) > 0
        # Beliefs should have changed
        assert len(diff["belief_changes"]) > 0

    def test_deserialize_missing_keys_raises(self):
        with pytest.raises(ValueError, match="nodes"):
            deserialize_graph({"foo": "bar"})


# =========================================================================
# 8. Integration: Full Pipeline
# =========================================================================

class TestFullPipeline:
    def test_end_to_end_student_evaluation(self):
        """Simulate the full evaluation pipeline for a student."""
        # 1. Build personalized DAG
        builder = AdmissionDAGBuilder()
        student = {"gpa": 3.8, "sat": 1480, "family_income": 80_000}
        school = {
            "acceptance_rate": 0.12,
            "research_expenditure": 300_000_000,
            "avg_aid": 35_000,
            "location_tier": 4,
            "career_services_rating": 0.75,
        }
        dag = builder.build_admission_dag(student, school)

        # 2. Propagate beliefs
        propagator = NoisyORPropagator()
        dag = propagator.propagate(dag)

        # 3. Check causal effects
        do_engine = DoCalculusEngine(propagator)
        ate = do_engine.average_treatment_effect(
            dag, "school_selectivity", "career_outcome", n_bootstrap=20
        )
        assert "ate" in ate

        # 4. Mediation analysis
        analyzer = MediationAnalyzer(propagator)
        pathways = analyzer.decompose_pathways(
            dag, "school_selectivity", "career_outcome"
        )
        assert len(pathways) > 0

        # 5. Backdoor adjustment
        adjuster = BackdoorAdjuster(propagator)
        adj_set = adjuster.find_backdoor_set(dag, "school_selectivity", "career_outcome")

        # 6. Go/No-Go score
        scorer = GoNoGoScorer(propagator)
        scores = scorer.compute_score({}, dag)
        assert scores["tier"] in (
            "strongly_recommend", "recommend", "neutral", "not_recommend"
        )
        assert 0 <= scores["overall_score"] <= 1

        # 7. Key factors
        factors = scorer.generate_key_factors(scores, dag)
        assert len(factors) > 0

        # 8. Serialize for frontend
        serialized = serialize_graph(dag)
        assert serialized["metadata"]["num_nodes"] == 16

    def test_two_school_comparison(self):
        """Compare two schools for the same student."""
        builder = AdmissionDAGBuilder()
        propagator = NoisyORPropagator()
        scorer = GoNoGoScorer(propagator)

        student = {"gpa": 3.5, "sat": 1350, "family_income": 60_000}

        school_mit = {
            "acceptance_rate": 0.04,
            "research_expenditure": 500_000_000,
            "avg_aid": 45_000,
            "location_tier": 5,
        }
        school_asu = {
            "acceptance_rate": 0.88,
            "research_expenditure": 50_000_000,
            "avg_aid": 12_000,
            "location_tier": 3,
        }

        dag_mit = builder.build_admission_dag(student, school_mit)
        propagator.propagate(dag_mit)
        score_mit = scorer.compute_score({}, dag_mit)

        dag_asu = builder.build_admission_dag(student, school_asu)
        propagator.propagate(dag_asu)
        score_asu = scorer.compute_score({}, dag_asu)

        # Both should produce valid scores
        for s in (score_mit, score_asu):
            assert 0 <= s["overall_score"] <= 1
            assert s["tier"] in (
                "strongly_recommend", "recommend", "neutral", "not_recommend"
            )
