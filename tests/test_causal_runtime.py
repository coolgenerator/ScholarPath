from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from scholarpath.causal_engine import CausalRuntime, FeatureBuilder
from scholarpath.causal_engine.types import CausalEstimateResult
from scholarpath.db.models import (
    CausalFeatureSnapshot,
    CausalOutcomeEvent,
    CausalShadowComparison,
    School,
    Student,
)


@pytest.mark.asyncio
async def test_feature_builder_contract_shapes(session):
    student = Student(
        name="Alice",
        gpa=3.8,
        gpa_scale="4.0",
        sat_total=1480,
        curriculum_type="AP",
        intended_majors=["Computer Science"],
        budget_usd=60000,
        target_year=2027,
    )
    school = School(
        name="Test University",
        city="Boston",
        state="MA",
        school_type="university",
        size_category="medium",
        acceptance_rate=0.12,
        avg_net_price=28000,
        graduation_rate_4yr=0.92,
        endowment_per_student=500000,
        student_faculty_ratio=8.0,
    )
    session.add(student)
    session.add(school)
    await session.flush()

    builder = FeatureBuilder()
    bundle = builder.build(student=student, school=school, offer=None)

    assert "student_gpa_norm" in bundle.student_features
    assert "school_selectivity" in bundle.school_features
    assert "academic_match" in bundle.interaction_features
    assert bundle.all_features["school_selectivity"] > 0.8


@pytest.mark.asyncio
async def test_causal_runtime_shadow_logs_and_persists(session, monkeypatch):
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_ENGINE_MODE", "shadow")
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_PROXY_LABELS_ENABLED", True)
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_SHADOW_LOGGING", True)
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_MODEL_VERSION", "latest_stable")

    student = Student(
        name="Bob",
        gpa=3.7,
        gpa_scale="4.0",
        sat_total=1420,
        curriculum_type="AP",
        intended_majors=["Economics"],
        budget_usd=45000,
        target_year=2027,
    )
    school = School(
        name="Shadow University",
        city="Chicago",
        state="IL",
        school_type="university",
        size_category="large",
        acceptance_rate=0.2,
        avg_net_price=35000,
        graduation_rate_4yr=0.86,
        endowment_per_student=300000,
        student_faculty_ratio=10.0,
    )
    session.add(student)
    session.add(school)
    await session.flush()

    runtime = CausalRuntime(session)
    result, _ = await runtime.estimate(
        student=student,
        school=school,
        offer=None,
        context="test_shadow",
        outcomes=["admission_probability", "career_outcome"],
    )
    await session.flush()

    assert result.causal_engine_version == "legacy_dag_v1"
    assert "admission_probability" in result.scores

    snapshots = (
        await session.execute(
            CausalFeatureSnapshot.__table__.select()  # type: ignore[attr-defined]
        )
    ).all()
    outcomes = (
        await session.execute(
            CausalOutcomeEvent.__table__.select()  # type: ignore[attr-defined]
        )
    ).all()
    shadows = (
        await session.execute(
            CausalShadowComparison.__table__.select()  # type: ignore[attr-defined]
        )
    ).all()

    assert len(snapshots) >= 1
    assert len(outcomes) >= 4
    assert len(shadows) >= 1


@pytest.mark.asyncio
async def test_causal_runtime_shadow_rollout_pywhy_primary(session, monkeypatch):
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_ENGINE_MODE", "shadow")
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_PYWHY_PRIMARY_PERCENT", 100)
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_PROXY_LABELS_ENABLED", True)
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_SHADOW_LOGGING", True)

    student = Student(
        name="Dana",
        gpa=3.9,
        gpa_scale="4.0",
        sat_total=1500,
        curriculum_type="AP",
        intended_majors=["Computer Science"],
        budget_usd=70000,
        target_year=2027,
    )
    school = School(
        name="Rollout University",
        city="Seattle",
        state="WA",
        school_type="university",
        size_category="large",
        acceptance_rate=0.16,
        avg_net_price=42000,
        graduation_rate_4yr=0.9,
        endowment_per_student=450000,
        student_faculty_ratio=9.0,
    )
    session.add(student)
    session.add(school)
    await session.flush()

    runtime = CausalRuntime(session)
    mock_legacy = CausalEstimateResult(
        scores={"admission_probability": 0.61},
        confidence_by_outcome={"admission_probability": 0.7},
        estimate_confidence=0.7,
        label_type="proxy",
        label_confidence=0.6,
        causal_engine_version="legacy_dag_v1",
        causal_model_version="legacy",
        metadata={},
    )
    mock_pywhy = CausalEstimateResult(
        scores={"admission_probability": 0.72},
        confidence_by_outcome={"admission_probability": 0.8},
        estimate_confidence=0.8,
        label_type="proxy",
        label_confidence=0.7,
        causal_engine_version="pywhy_v1",
        causal_model_version="pywhy-test",
        metadata={},
    )
    monkeypatch.setattr(runtime._legacy, "estimate", AsyncMock(return_value=mock_legacy))
    monkeypatch.setattr(runtime._pywhy, "estimate", AsyncMock(return_value=mock_pywhy))

    result, _ = await runtime.estimate(
        student=student,
        school=school,
        offer=None,
        context="test_rollout",
        outcomes=["admission_probability"],
    )
    await session.flush()

    assert result.causal_engine_version == "pywhy_v1"
    assert result.metadata["rollout"]["pywhy_primary_percent"] == 100
    assert result.metadata["rollout"]["selected_primary"] == "pywhy"
    shadows = (
        await session.execute(
            CausalShadowComparison.__table__.select()  # type: ignore[attr-defined]
        )
    ).all()
    assert len(shadows) == 1
    assert shadows[0]._mapping["engine_mode"] == "shadow_pywhy"


@pytest.mark.asyncio
async def test_causal_runtime_legacy_mode_no_shadow(session, monkeypatch):
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_ENGINE_MODE", "legacy")
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_PROXY_LABELS_ENABLED", True)
    monkeypatch.setattr("scholarpath.config.settings.CAUSAL_SHADOW_LOGGING", True)

    student = Student(
        name="Carol",
        gpa=3.5,
        gpa_scale="4.0",
        sat_total=1380,
        curriculum_type="AP",
        intended_majors=["Math"],
        budget_usd=50000,
        target_year=2027,
    )
    school = School(
        name="Legacy University",
        city="NYC",
        state="NY",
        school_type="university",
        size_category="large",
        acceptance_rate=0.3,
        avg_net_price=30000,
        graduation_rate_4yr=0.8,
        endowment_per_student=150000,
        student_faculty_ratio=12.0,
    )
    session.add(student)
    session.add(school)
    await session.flush()

    runtime = CausalRuntime(session)
    result, _ = await runtime.estimate(
        student=student,
        school=school,
        offer=None,
        context="test_legacy",
        outcomes=["admission_probability"],
    )
    await session.flush()

    assert result.causal_engine_version == "legacy_dag_v1"
    shadows = (
        await session.execute(
            CausalShadowComparison.__table__.select()  # type: ignore[attr-defined]
        )
    ).all()
    assert len(shadows) == 0
