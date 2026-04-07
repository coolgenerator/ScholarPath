from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select

from scholarpath.db.models import (
    AdmissionEvent,
    CausalDatasetVersion,
    CausalFeatureSnapshot,
    CausalOutcomeEvent,
    Offer,
    School,
    SchoolEvaluation,
    Student,
)
from scholarpath.services.causal_data_service import register_admission_event
from scholarpath.services import causal_real_asset_service
from scholarpath.services.causal_real_asset_service import backfill_real_admission_assets


async def _create_student(session) -> Student:
    student = Student(
        name=f"Real Student {uuid.uuid4().hex[:6]}",
        gpa=3.82,
        gpa_scale="4.0",
        sat_total=1510,
        curriculum_type="AP",
        intended_majors=["Computer Science"],
        budget_usd=65000,
        need_financial_aid=False,
        target_year=2027,
        extracurriculars={"club": "robotics"},
        awards={"award": "state finalist"},
        preferences={"location": ["urban"]},
        ed_preference="ed",
        profile_completed=True,
    )
    session.add(student)
    await session.flush()
    return student


async def _create_school(session, name: str) -> School:
    school = School(
        name=name,
        city="Boston",
        state="MA",
        school_type="university",
        size_category="medium",
        us_news_rank=15,
        acceptance_rate=0.11,
        sat_25=1380,
        sat_75=1540,
        act_25=31,
        act_75=35,
        tuition_oos=62000,
        avg_net_price=28000,
        intl_student_pct=0.12,
        student_faculty_ratio=8.0,
        graduation_rate_4yr=0.91,
        campus_setting="urban",
    )
    session.add(school)
    await session.flush()
    return school


@pytest.mark.asyncio
async def test_waitlist_event_creates_true_outcome(session):
    student = await _create_student(session)
    school = await _create_school(session, "Real Asset A")

    event = await register_admission_event(
        session,
        student_id=str(student.id),
        school_id=str(school.id),
        cycle_year=2027,
        stage="waitlist",
        source_name="manual",
        metadata={"source_key": "manual:waitlist:1"},
    )
    await session.commit()

    event_rows = int((await session.scalar(select(func.count()).select_from(AdmissionEvent))) or 0)
    outcome_rows = int((await session.scalar(select(func.count()).select_from(CausalOutcomeEvent))) or 0)
    outcome = (
        await session.scalar(
            select(CausalOutcomeEvent).where(
                CausalOutcomeEvent.outcome_name == "admission_probability",
            )
        )
    )

    assert event.stage == "waitlist"
    assert event_rows == 1
    assert outcome_rows == 1
    assert outcome is not None
    assert outcome.label_type == "true"
    assert outcome.outcome_value == pytest.approx(0.35)


@pytest.mark.asyncio
async def test_backfill_real_admission_assets_is_idempotent_and_admission_only(session):
    student = await _create_student(session)
    school_a = await _create_school(session, "Real Asset B")
    school_b = await _create_school(session, "Real Asset C")

    session.add(
        SchoolEvaluation(
            student_id=student.id,
            school_id=school_a.id,
            tier="target",
            academic_fit=0.8,
            financial_fit=0.7,
            career_fit=0.75,
            life_fit=0.68,
            overall_score=0.73,
            admission_probability=0.62,
            reasoning="eval-a",
            fit_details={"source": "test"},
        )
    )
    session.add(
        SchoolEvaluation(
            student_id=student.id,
            school_id=school_b.id,
            tier="safety",
            academic_fit=0.76,
            financial_fit=0.74,
            career_fit=0.71,
            life_fit=0.7,
            overall_score=0.72,
            admission_probability=0.68,
            reasoning="eval-b",
            fit_details={"source": "test"},
        )
    )
    session.add(
        Offer(
            student_id=student.id,
            school_id=school_a.id,
            status="admitted",
            merit_scholarship=10000,
            need_based_grant=15000,
            loan_offered=0,
            work_study=0,
            total_aid=25000,
            total_cost=65000,
            net_cost=40000,
            honors_program=False,
            notes="offer-a",
        )
    )
    session.add(
        Offer(
            student_id=student.id,
            school_id=school_b.id,
            status="waitlisted",
            merit_scholarship=0,
            need_based_grant=0,
            loan_offered=0,
            work_study=0,
            total_aid=0,
            total_cost=62000,
            net_cost=62000,
            honors_program=False,
            notes="offer-b",
        )
    )
    await session.commit()

    first = await backfill_real_admission_assets(
        session,
        run_id="real-backfill-test-1",
        student_ids=[str(student.id)],
        school_ids=[str(school_a.id), str(school_b.id)],
        include_school_evaluations=True,
        include_offers=True,
        include_admission_events=False,
        build_dataset=True,
        dataset_version="real-only-ds-v1",
        active_outcomes=["admission_probability"],
        min_true_per_outcome=1,
    )
    await session.commit()

    snapshot_rows = int((await session.scalar(select(func.count()).select_from(CausalFeatureSnapshot))) or 0)
    outcome_rows = int((await session.scalar(select(func.count()).select_from(CausalOutcomeEvent))) or 0)
    event_rows = int((await session.scalar(select(func.count()).select_from(AdmissionEvent))) or 0)
    dataset_rows = int((await session.scalar(select(func.count()).select_from(CausalDatasetVersion))) or 0)

    assert first["status"] == "ok"
    assert first["dataset_result"]["active_outcomes"] == ["admission_probability"]
    assert first["dataset_result"]["truth_ratio_by_outcome"]["admission_probability"] == 1.0
    assert snapshot_rows >= 4
    assert event_rows == 2
    assert outcome_rows == 2
    assert dataset_rows == 1

    second = await backfill_real_admission_assets(
        session,
        run_id="real-backfill-test-2",
        student_ids=[str(student.id)],
        school_ids=[str(school_a.id), str(school_b.id)],
        include_school_evaluations=True,
        include_offers=True,
        include_admission_events=False,
        build_dataset=True,
        dataset_version="real-only-ds-v2",
        active_outcomes=["admission_probability"],
        min_true_per_outcome=1,
    )
    await session.commit()

    snapshot_rows_2 = int((await session.scalar(select(func.count()).select_from(CausalFeatureSnapshot))) or 0)
    outcome_rows_2 = int((await session.scalar(select(func.count()).select_from(CausalOutcomeEvent))) or 0)
    event_rows_2 = int((await session.scalar(select(func.count()).select_from(AdmissionEvent))) or 0)
    dataset_rows_2 = int((await session.scalar(select(func.count()).select_from(CausalDatasetVersion))) or 0)

    assert second["snapshots_deduped"] >= 4 or second["events_deduped"] >= 1
    assert snapshot_rows_2 == snapshot_rows
    assert outcome_rows_2 == outcome_rows
    assert event_rows_2 == event_rows
    assert dataset_rows_2 == 2


@pytest.mark.asyncio
async def test_backfill_real_admission_assets_adds_official_school_profile_snapshots(
    session,
    monkeypatch,
):
    student = await _create_student(session)
    school = await _create_school(session, "Real Asset Official")
    session.add(
        SchoolEvaluation(
            student_id=student.id,
            school_id=school.id,
            tier="target",
            academic_fit=0.8,
            financial_fit=0.7,
            career_fit=0.75,
            life_fit=0.68,
            overall_score=0.73,
            admission_probability=0.62,
            reasoning="eval-official",
            fit_details={"source": "test"},
        )
    )
    await session.commit()

    async def _fake_ingest(_session, *, school_names, cycle_year, run_id, **_kwargs):
        _ = school_names, cycle_year, run_id
        return {
            "status": "ok",
            "raw_facts": 1,
            "schools_updated": [str(school.id)],
        }

    monkeypatch.setattr(causal_real_asset_service, "ingest_official_facts", _fake_ingest)

    result = await backfill_real_admission_assets(
        session,
        run_id="real-backfill-official-test",
        student_ids=[str(student.id)],
        school_ids=[str(school.id)],
        include_school_evaluations=True,
        include_offers=False,
        include_admission_events=False,
        ingest_official_facts_enabled=True,
        build_dataset=False,
        active_outcomes=["admission_probability"],
        min_true_per_outcome=1,
    )
    await session.commit()

    snapshot_rows = int((await session.scalar(select(func.count()).select_from(CausalFeatureSnapshot))) or 0)
    official_snapshot_rows = int(
        (
            await session.scalar(
                select(func.count())
                .select_from(CausalFeatureSnapshot)
                .where(CausalFeatureSnapshot.context == "official_school_profile")
            )
        )
        or 0
    )

    assert result["official_snapshots_created"] == 1
    assert snapshot_rows >= 2
    assert official_snapshot_rows == 1
