from __future__ import annotations

from scholarpath.evals.recommendation_gold_live import (
    _budget_compliance_pass,
    _build_gate,
    _geo_alignment_pass,
    _major_alignment_pass,
    _tier_sanity_pass,
)


def test_tier_sanity_rejects_ultra_selective_likely_without_high_confidence():
    payload = {
        "schools": [
            {
                "school_name": "X",
                "tier": "likely",
                "acceptance_rate": 0.05,
                "admission_probability": 0.64,
            }
        ]
    }
    assert _tier_sanity_pass(payload=payload) is False


def test_alignment_and_budget_compliance_helpers():
    payload = {
        "prefilter_meta": {
            "budget_cap_used": 10000,
        },
        "schools": [
            {
                "school_name": "A",
                "tier": "target",
                "acceptance_rate": 0.3,
                "admission_probability": 0.6,
                "major_match": 0.9,
                "geo_match": 0.85,
                "prefilter_tag": "eligible",
                "net_price": 9000,
            },
            {
                "school_name": "B",
                "tier": "safety",
                "acceptance_rate": 0.5,
                "admission_probability": 0.72,
                "major_match": 0.8,
                "geo_match": 0.92,
                "prefilter_tag": "stretch",
                "net_price": 14000,
            },
        ],
    }
    assert _major_alignment_pass(payload=payload) is True
    assert _geo_alignment_pass(payload=payload) is True
    assert _budget_compliance_pass(payload=payload) is True


def test_gate_requires_new_quality_thresholds():
    gate = _build_gate(
        metrics={
            "scoring_coverage_rate": 0.98,
            "recommendation_route_hit_rate": 1.0,
            "recommendation_payload_exists_rate": 1.0,
            "hard_check_pass_rate": 0.99,
            "overall_user_feel_mean": 3.1,
            "judge_case_score_avg": 62.0,
        }
    )
    assert gate["passed"] is True
