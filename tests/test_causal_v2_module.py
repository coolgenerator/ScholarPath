"""Tests for standalone causal inference V2 module."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from scholarpath.causal_v2 import (
    CausalEngineV2,
    CausalScenarioInput,
    CausalSchoolProfile,
    CausalStudentProfile,
    school_to_causal_v2_profile,
    student_to_causal_v2_profile,
)


@pytest.fixture
def engine() -> CausalEngineV2:
    # Keep CI sample count small for test speed.
    return CausalEngineV2(ci_samples=40)


@pytest.fixture
def student_profile() -> CausalStudentProfile:
    return CausalStudentProfile(gpa=3.85, sat=1490, family_income=95_000)


@pytest.fixture
def school_profile() -> CausalSchoolProfile:
    return CausalSchoolProfile(
        acceptance_rate=0.14,
        research_expenditure=250_000_000,
        avg_aid=35_000,
        location_tier=4,
        career_services_rating=0.75,
    )


def _all_scores_in_unit_interval(payload: dict[str, float]) -> bool:
    return all(0.0 <= value <= 1.0 for value in payload.values())


def test_v2_evaluate_returns_typed_scores(
    engine: CausalEngineV2,
    student_profile: CausalStudentProfile,
    school_profile: CausalSchoolProfile,
) -> None:
    result = engine.evaluate(student_profile, school_profile, include_confidence=True)

    assert result.tier in {"reach", "target", "safety", "likely"}
    assert _all_scores_in_unit_interval(result.dimensions.as_dict())
    assert _all_scores_in_unit_interval(result.outcomes.as_dict())
    assert result.confidence_interval is not None
    assert "admission_probability" in result.confidence_interval


def test_v2_what_if_applies_interventions(
    engine: CausalEngineV2,
    student_profile: CausalStudentProfile,
    school_profile: CausalSchoolProfile,
) -> None:
    result = engine.what_if(
        student_profile,
        school_profile,
        {"financial_aid": 0.95},
    )

    assert set(result.original_scores) == set(result.modified_scores) == set(result.deltas)
    assert any(abs(delta) > 0 for delta in result.deltas.values())


def test_v2_compare_scenarios_requires_two(
    engine: CausalEngineV2,
    student_profile: CausalStudentProfile,
    school_profile: CausalSchoolProfile,
) -> None:
    with pytest.raises(ValueError, match="at least 2"):
        engine.compare_scenarios(
            student_profile,
            [
                CausalScenarioInput(
                    school_profile=school_profile,
                    interventions={"financial_aid": 0.9},
                    label="Only One",
                )
            ],
        )


def test_v2_compare_scenarios_returns_best_label(
    engine: CausalEngineV2,
    student_profile: CausalStudentProfile,
) -> None:
    scenario_a = CausalScenarioInput(
        school_profile=CausalSchoolProfile(
            acceptance_rate=0.10,
            research_expenditure=400_000_000,
            avg_aid=45_000,
        ),
        interventions={"research_opportunities": 0.9},
        label="Research Heavy",
    )
    scenario_b = CausalScenarioInput(
        school_profile=CausalSchoolProfile(
            acceptance_rate=0.22,
            research_expenditure=80_000_000,
            avg_aid=15_000,
        ),
        interventions={"financial_aid": 0.95},
        label="Aid Heavy",
    )

    result = engine.compare_scenarios(student_profile, [scenario_a, scenario_b])

    assert len(result.scenarios) == 2
    assert result.best_scenario_label in {"Research Heavy", "Aid Heavy"}
    assert "delta_score=" in result.summary


def test_v2_adapters_map_current_models() -> None:
    student = SimpleNamespace(
        gpa=3.9,
        sat_total=1520,
        act_composite=None,
        budget_usd=120_000,
    )
    school = SimpleNamespace(
        acceptance_rate=0.16,
        avg_net_price=28_000,
        tuition_oos=56_000,
        endowment_per_student=500_000,
    )

    student_profile = student_to_causal_v2_profile(student)
    school_profile = school_to_causal_v2_profile(school)

    assert student_profile.gpa == pytest.approx(3.9)
    assert student_profile.sat == 1520
    assert student_profile.family_income == pytest.approx(120_000)

    assert school_profile.acceptance_rate == pytest.approx(0.16)
    assert school_profile.research_expenditure == pytest.approx(500_000)
    assert school_profile.avg_aid == pytest.approx(28_000)

