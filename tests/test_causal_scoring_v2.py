from __future__ import annotations

from scholarpath.causal_engine.scoring import CausalFeatureView, compute_pywhy_raw_scores


def _build_view(
    *,
    gpa_norm: float = 0.7,
    sat_norm: float = 0.7,
    need_aid: float = 0.5,
    acceptance_rate: float = 0.4,
    selectivity: float | None = None,
    grad_rate: float = 0.75,
    endowment: float = 0.6,
    location: float = 0.6,
    affordability_ratio: float = 0.6,
) -> CausalFeatureView:
    school_features: dict[str, float] = {
        "school_acceptance_rate": acceptance_rate,
        "school_grad_rate": grad_rate,
        "school_endowment_norm": endowment,
        "school_location_tier": location,
    }
    if selectivity is not None:
        school_features["school_selectivity"] = selectivity
    payload = {
        "student_features": {
            "student_gpa_norm": gpa_norm,
            "student_sat_norm": sat_norm,
            "student_need_aid": need_aid,
        },
        "school_features": school_features,
        "interaction_features": {
            "affordability_ratio_norm": affordability_ratio,
        },
    }
    return CausalFeatureView.from_payload(payload)


def test_raw_scores_v2_are_bounded() -> None:
    scores = compute_pywhy_raw_scores(_build_view())
    for outcome, value in scores.items():
        assert 0.0 <= value <= 1.0, f"{outcome} out of [0, 1]: {value}"


def test_life_satisfaction_monotonic_with_support_and_selectivity() -> None:
    lower_support = compute_pywhy_raw_scores(
        _build_view(need_aid=0.9, affordability_ratio=0.5, selectivity=0.5),
    )["life_satisfaction"]
    higher_support = compute_pywhy_raw_scores(
        _build_view(need_aid=0.1, affordability_ratio=0.5, selectivity=0.5),
    )["life_satisfaction"]
    assert higher_support >= lower_support

    high_selectivity = compute_pywhy_raw_scores(
        _build_view(need_aid=0.4, affordability_ratio=0.7, selectivity=0.8),
    )["life_satisfaction"]
    low_selectivity = compute_pywhy_raw_scores(
        _build_view(need_aid=0.4, affordability_ratio=0.7, selectivity=0.2),
    )["life_satisfaction"]
    assert low_selectivity >= high_selectivity


def test_phd_probability_monotonic_with_academic_and_selectivity() -> None:
    lower_academic = compute_pywhy_raw_scores(
        _build_view(gpa_norm=0.45, sat_norm=0.45, selectivity=0.5),
    )["phd_probability"]
    higher_academic = compute_pywhy_raw_scores(
        _build_view(gpa_norm=0.85, sat_norm=0.85, selectivity=0.5),
    )["phd_probability"]
    assert higher_academic >= lower_academic

    low_selectivity = compute_pywhy_raw_scores(
        _build_view(gpa_norm=0.7, sat_norm=0.7, selectivity=0.2),
    )["phd_probability"]
    high_selectivity = compute_pywhy_raw_scores(
        _build_view(gpa_norm=0.7, sat_norm=0.7, selectivity=0.8),
    )["phd_probability"]
    assert high_selectivity >= low_selectivity
