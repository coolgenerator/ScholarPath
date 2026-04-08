from __future__ import annotations

import uuid

from scholarpath.services.recommendation_service import (
    _apply_scenario_constraints,
    _apply_budget_prefilter,
    _assign_tier,
    _compute_geo_match_score,
    _compute_sat_fit_with_mode,
    _effective_acceptance_rate_pct,
    _maybe_trigger_deepsearch_fallback,
    _resolve_requested_top_n,
    _validate_scenario_constraints,
)
from scholarpath.services.recommendation_skills import RecommendationSkillProfile


def _row(name: str, score: float, net_price: int | None) -> dict:
    return {
        "school_name": name,
        "overall_score": score,
        "school_info": {"avg_net_price": net_price},
    }


def test_budget_prefilter_keeps_eligible_plus_top_stretch():
    selected, meta = _apply_budget_prefilter(
        school_results=[
            _row("A", 0.95, 9000),
            _row("B", 0.90, 12000),
            _row("C", 0.85, 15000),
            _row("D", 0.80, None),
        ],
        budget_cap=10000,
        top_n=15,
        stretch_slots=1,
        budget_hard_gate=True,
    )
    assert [item["school_name"] for item in selected] == ["A", "B"]
    assert selected[0]["prefilter_tag"] == "eligible"
    assert selected[1]["prefilter_tag"] == "stretch"
    assert meta["eligible_count"] == 1
    assert meta["stretch_count"] == 1
    assert meta["excluded_reasons_summary"]["over_budget"] == 1
    assert meta["excluded_reasons_summary"]["missing_net_price"] == 1


def test_tier_guard_blocks_safety_for_ultra_selective_school():
    tier = _assign_tier(
        calibrated_prob=0.72,
        acceptance_rate_pct=5.0,
        sat_fit=0.60,
        risk_mode="balanced",
    )
    assert tier in {"reach", "target"}


def _profile(skill_id: str = "recommendation.risk_first") -> RecommendationSkillProfile:
    return RecommendationSkillProfile(
        skill_id=skill_id,
        candidate_pool_size=100,
        top_n=15,
        weights={"academic": 0.3, "financial": 0.25, "career": 0.25, "life": 0.2},
        budget_hard_gate=True,
        stretch_slots=3,
        major_boost=0.1,
        geo_boost=0.1,
        risk_mode="balanced",
        min_results=8,
        tier_confidence_min_count=6,
        missing_field_trigger_threshold=0.3,
        risk_min_tier_counts={"reach": 2, "target": 4, "safety": 3},
        major_match_threshold=0.65,
        major_match_min_ratio=0.45,
        geo_match_threshold=0.75,
        geo_match_min_ratio=0.5,
        roi_career_min_mean=0.62,
    )


def _scored_school(name: str, tier: str, *, major_match: float = 0.7, geo_match: float = 0.8, net_price: int = 9000) -> dict:
    return {
        "school_name": name,
        "tier": tier,
        "overall_score": 0.8,
        "admission_probability": 0.6,
        "major_match": major_match,
        "geo_match": geo_match,
        "program_data_available": True,
        "region_data_available": True,
        "prefilter_tag": "eligible",
        "is_stretch": False,
        "school_info": {"avg_net_price": net_price},
        "sub_scores": {"career": 0.7},
    }


class _StubSchool:
    def __init__(self, sat_25: int | None, sat_75: int | None):
        self.sat_25 = sat_25
        self.sat_75 = sat_75


class _GeoSchool:
    def __init__(self, *, state: str | None, city: str | None, campus_setting: str | None):
        self.state = state
        self.city = city
        self.campus_setting = campus_setting


def test_sat_fit_mode_section_and_total():
    section_school = _StubSchool(750, 790)
    total_school = _StubSchool(1350, 1500)
    section_score, section_mode = _compute_sat_fit_with_mode(student_sat=1500, school=section_school)
    total_score, total_mode = _compute_sat_fit_with_mode(student_sat=1500, school=total_school)
    assert section_mode == "section"
    assert total_mode == "total"
    assert section_score >= 0.2
    assert total_score >= 0.2


def test_tier_hard_cap_blocks_ultra_selective_safety_without_exception():
    tier = _assign_tier(
        calibrated_prob=0.9,
        acceptance_rate_pct=5.0,
        sat_fit=0.90,
        risk_mode="balanced",
    )
    assert tier == "target"


def test_tier_hard_cap_allows_exceptional_case_to_safety():
    tier = _assign_tier(
        calibrated_prob=0.9,
        acceptance_rate_pct=8.0,
        sat_fit=0.96,
        risk_mode="balanced",
    )
    assert tier in {"safety", "target"}


def test_effective_acceptance_caps_obvious_outlier_by_rank():
    effective, capped = _effective_acceptance_rate_pct(
        raw_acceptance_rate_pct=77.35,
        school_rank=12,
    )
    assert capped is True
    assert effective <= 20.0


def test_effective_acceptance_uses_rank_prior_when_missing():
    effective, capped = _effective_acceptance_rate_pct(
        raw_acceptance_rate_pct=None,
        school_rank=8,
    )
    assert capped is False
    assert 2.0 <= effective <= 10.0


def test_geo_match_uses_exact_state_signal_not_substring_noise():
    school = _GeoSchool(state="Illinois", city="Chicago", campus_setting="urban")
    score = _compute_geo_match_score(
        preferences={"location": ["or"]},
        school=school,
    )
    assert score <= 0.2


def test_geo_match_handles_region_aliases():
    school = _GeoSchool(state="California", city="Los Angeles", campus_setting="urban")
    score = _compute_geo_match_score(
        preferences={"location": ["West Coast"]},
        school=school,
    )
    assert score >= 0.9


def test_resolve_requested_top_n_parses_top5_prompt():
    top_n = _resolve_requested_top_n(
        user_message="Please give me a concise top-5 shortlist.",
        default_top_n=15,
    )
    assert top_n == 5


def test_validate_constraints_flags_risk_tier_mix_insufficient():
    selected = [
        _scored_school("A", "reach"),
        _scored_school("B", "target"),
        _scored_school("C", "target"),
        _scored_school("D", "target"),
        _scored_school("E", "target"),
        _scored_school("F", "target"),
        _scored_school("G", "reach"),
        _scored_school("H", "reach"),
    ]
    meta = {
        "budget_cap_used": 12000,
        "eligible_count": 8,
        "stretch_count": 0,
        "excluded_count": 0,
        "excluded_reasons_summary": {},
    }
    summary = _validate_scenario_constraints(
        selected_results=selected,
        prefilter_meta=meta,
        budget_cap=12000,
        skill_profile=_profile("recommendation.risk_first"),
        top_n=15,
    )
    assert summary["constraint_status"] == "degraded"
    assert "risk_tier_mix_insufficient" in summary["constraint_fail_reasons"]


def test_apply_scenario_constraints_risk_enforces_mix_with_shortfall():
    selected = [
        _scored_school("A", "target"),
        _scored_school("B", "target"),
        _scored_school("C", "target"),
        _scored_school("D", "target"),
        _scored_school("E", "target"),
    ]
    final_rows, execution = _apply_scenario_constraints(
        selected_results=selected,
        skill_profile=_profile("recommendation.risk_first"),
        top_n=5,
    )
    assert len(final_rows) == 5
    assert execution["risk_quota_shortfall_count"] >= 1


def test_apply_scenario_constraints_geo_reports_backfill():
    selected = [
        _scored_school("A", "target", geo_match=0.3),
        _scored_school("B", "target", geo_match=0.35),
        _scored_school("C", "target", geo_match=0.2),
        _scored_school("D", "target", geo_match=0.4),
        _scored_school("E", "target", geo_match=0.1),
    ]
    final_rows, execution = _apply_scenario_constraints(
        selected_results=selected,
        skill_profile=_profile("recommendation.geo_first"),
        top_n=5,
    )
    assert len(final_rows) == 5
    assert execution["geo_backfill_count"] >= 1


def test_deepsearch_fallback_triggers_on_constraint_failure(monkeypatch):
    selected = [
        _scored_school("A", "target"),
        _scored_school("B", "target"),
        _scored_school("C", "target"),
        _scored_school("D", "target"),
        _scored_school("E", "target"),
        _scored_school("F", "target"),
        _scored_school("G", "target"),
        _scored_school("H", "target"),
    ]
    validation = {
        "constraint_status": "degraded",
        "constraints": {"tier_confidence": {"actual": 1}},
    }
    monkeypatch.setattr(
        "scholarpath.services.recommendation_service._enqueue_deepsearch_fallback",
        lambda **_: ("task-1", None),
    )
    status = _maybe_trigger_deepsearch_fallback(
        student_id=uuid.uuid4(),
        skill_profile=_profile("recommendation.major_first"),
        selected_results=selected,
        scenario_validation=validation,
    )
    assert status["deepsearch_fallback_triggered"] is True
    assert status["deepsearch_pending"] is True
    assert status["task_id"] == "task-1"


def test_deepsearch_fallback_marks_triggered_even_when_enqueue_unavailable(monkeypatch):
    selected = [_scored_school(f"S{i}", "target") for i in range(8)]
    validation = {
        "constraint_status": "degraded",
        "constraints": {"tier_confidence": {"actual": 1}},
    }
    monkeypatch.setattr(
        "scholarpath.services.recommendation_service._enqueue_deepsearch_fallback",
        lambda **_: (None, "celery_unavailable"),
    )
    status = _maybe_trigger_deepsearch_fallback(
        student_id=uuid.uuid4(),
        skill_profile=_profile("recommendation.geo_first"),
        selected_results=selected,
        scenario_validation=validation,
    )
    assert status["deepsearch_fallback_triggered"] is True
    assert status["deepsearch_pending"] is False
    assert status["enqueue_error"] == "celery_unavailable"
