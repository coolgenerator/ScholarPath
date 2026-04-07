from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from scholarpath.config import settings
from scholarpath.db.models import (
    CanonicalFact,
    CausalFeatureSnapshot,
    CausalOutcomeEvent,
    School,
    SchoolExternalId,
    SchoolMetricsYear,
    Student,
)
from scholarpath.search.canonical_merge import fingerprint_value
from scholarpath.services.admission_data_phase4_service import (
    PHASE4_OUTCOME_SOURCE,
    _ingest_ipeds_completions_truth,
    _load_school_year_metric_index,
    materialize_non_admission_true_labels,
    run_phase4_training_prep,
)


async def _create_student(session, name: str) -> Student:
    row = Student(
        name=name,
        gpa=3.85,
        gpa_scale="4.0",
        sat_total=1490,
        curriculum_type="AP",
        intended_majors=["Computer Science"],
        budget_usd=80000,
        need_financial_aid=False,
        target_year=2027,
        profile_completed=True,
    )
    session.add(row)
    await session.flush()
    return row


async def _create_school(session, name: str) -> School:
    row = School(
        name=name,
        city="Boston",
        state="MA",
        school_type="university",
        size_category="medium",
    )
    session.add(row)
    await session.flush()
    return row


async def _seed_school_year_truth(
    session,
    *,
    school: School,
    metric_year: int,
    grad_rate: float,
    retention_rate: float,
    earnings: float,
    doctoral_share: float,
) -> None:
    session.add(
        SchoolMetricsYear(
            school_id=school.id,
            source_name="phase4-test",
            metric_year=metric_year,
            grad_rate=grad_rate,
            avg_net_price=23000,
            admit_rate=0.3,
        )
    )

    for outcome_name, numeric in (
        ("retention_rate", retention_rate),
        ("median_earnings_10yr", earnings),
        ("doctoral_completions_share", doctoral_share),
    ):
        session.add(
            CanonicalFact(
                student_id=None,
                school_id=school.id,
                cycle_year=metric_year,
                outcome_name=outcome_name,
                canonical_value_text=str(numeric),
                canonical_value_numeric=numeric,
                canonical_value_bucket=fingerprint_value(value_text=str(numeric), value_numeric=numeric),
                source_family="phase4-test",
                confidence=0.99,
                observed_at=datetime.now(timezone.utc),
                metadata_={"run_id": "phase4-test"},
            )
        )


@pytest.mark.asyncio
async def test_materialize_non_admission_true_labels_idempotent(session):
    student = await _create_student(session, "Phase4 Student A")
    school = await _create_school(session, "Phase4 School A")
    now = datetime.now(timezone.utc)

    session.add(
        CausalFeatureSnapshot(
            student_id=student.id,
            school_id=school.id,
            offer_id=None,
            context="phase4_test",
            feature_payload={"k": "v"},
            metadata_={"run_id": "phase4-test"},
            observed_at=now,
        )
    )

    await _seed_school_year_truth(
        session,
        school=school,
        metric_year=now.year,
        grad_rate=0.9,
        retention_rate=0.84,
        earnings=75000,
        doctoral_share=0.12,
    )
    await session.flush()

    metric_index = await _load_school_year_metric_index(session, school_ids=[school.id])

    first = await materialize_non_admission_true_labels(
        session,
        run_id="phase4-test-run-1",
        lookback_days=540,
        school_ids=[school.id],
        school_year_metric_index=metric_index,
    )
    await session.flush()

    assert first["created"] == 4
    assert first["eligible_snapshots"] == 1

    rows = (
        (
            await session.execute(
                select(CausalOutcomeEvent).where(CausalOutcomeEvent.school_id == school.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 4
    assert {row.outcome_name for row in rows} == {
        "academic_outcome",
        "career_outcome",
        "life_satisfaction",
        "phd_probability",
    }
    assert all(str(row.label_type).lower() == "true" for row in rows)
    assert all(str(row.source) == PHASE4_OUTCOME_SOURCE for row in rows)

    second = await materialize_non_admission_true_labels(
        session,
        run_id="phase4-test-run-2",
        lookback_days=540,
        school_ids=[school.id],
        school_year_metric_index=metric_index,
    )
    assert second["created"] == 0
    assert second["deduped"] == 4


@pytest.mark.asyncio
async def test_career_outcome_uses_year_percentile_rank(session):
    student_a = await _create_student(session, "Phase4 Student B1")
    student_b = await _create_student(session, "Phase4 Student B2")
    school_a = await _create_school(session, "Phase4 School B1")
    school_b = await _create_school(session, "Phase4 School B2")
    now = datetime.now(timezone.utc)

    session.add_all(
        [
            CausalFeatureSnapshot(
                student_id=student_a.id,
                school_id=school_a.id,
                offer_id=None,
                context="phase4_test",
                feature_payload={"k": "v"},
                metadata_={"run_id": "phase4-test"},
                observed_at=now,
            ),
            CausalFeatureSnapshot(
                student_id=student_b.id,
                school_id=school_b.id,
                offer_id=None,
                context="phase4_test",
                feature_payload={"k": "v"},
                metadata_={"run_id": "phase4-test"},
                observed_at=now,
            ),
        ]
    )

    await _seed_school_year_truth(
        session,
        school=school_a,
        metric_year=now.year,
        grad_rate=0.9,
        retention_rate=0.84,
        earnings=90000,
        doctoral_share=0.22,
    )
    await _seed_school_year_truth(
        session,
        school=school_b,
        metric_year=now.year,
        grad_rate=0.88,
        retention_rate=0.83,
        earnings=45000,
        doctoral_share=0.09,
    )
    await session.flush()

    metric_index = await _load_school_year_metric_index(session, school_ids=[school_a.id, school_b.id])
    await materialize_non_admission_true_labels(
        session,
        run_id="phase4-test-career",
        lookback_days=540,
        school_ids=[school_a.id, school_b.id],
        school_year_metric_index=metric_index,
    )
    await session.flush()

    rows = (
        (
            await session.execute(
                select(CausalOutcomeEvent).where(CausalOutcomeEvent.outcome_name == "career_outcome")
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 2
    by_school = {str(row.school_id): float(row.outcome_value) for row in rows}
    assert 0.0 <= by_school[str(school_a.id)] <= 1.0
    assert 0.0 <= by_school[str(school_b.id)] <= 1.0
    assert by_school[str(school_a.id)] > by_school[str(school_b.id)]


@pytest.mark.asyncio
async def test_ingest_ipeds_completions_truth_upserts_doctoral_share(session, tmp_path, monkeypatch):
    school = await _create_school(session, "Phase4 School C")
    session.add(
        SchoolExternalId(
            school_id=school.id,
            provider="ipeds",
            external_id="166683",
            is_primary=True,
            match_method="test",
            confidence=1.0,
            metadata_={"run_id": "phase4-test"},
        )
    )
    await session.flush()

    csv_path = tmp_path / "ipeds_completions.csv"
    csv_path.write_text(
        "unitid,year,award_level,count\n"
        "166683,2025,doctorate,120\n"
        "166683,2025,masters,480\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(settings, "IPEDS_COMPLETIONS_DATASET_PATH", str(csv_path))
    monkeypatch.setattr(settings, "IPEDS_COMPLETIONS_DATASET_URL", "")

    result = await _ingest_ipeds_completions_truth(
        session,
        run_id="phase4-completions-test",
        school_ids=[school.id],
        cycle_year=2025,
        output_dir=tmp_path,
    )
    await session.flush()

    assert result["status"] == "ok"
    assert result["facts_upserted"] >= 1

    row = await session.scalar(
        select(CanonicalFact).where(
            CanonicalFact.school_id == school.id,
            CanonicalFact.outcome_name == "doctoral_completions_share",
            CanonicalFact.cycle_year == 2025,
        )
    )
    assert row is not None
    assert float(row.canonical_value_numeric or 0.0) == pytest.approx(0.2)


@pytest.mark.asyncio
async def test_run_phase4_training_prep_returns_stage_readiness_payload(session, tmp_path):
    student = await _create_student(session, "Phase4 Student D")
    school = await _create_school(session, "Phase4 School D")
    now = datetime.now(timezone.utc)

    session.add(
        CausalFeatureSnapshot(
            student_id=student.id,
            school_id=school.id,
            offer_id=None,
            context="phase4_test",
            feature_payload={"k": "v"},
            metadata_={"run_id": "phase4-test"},
            observed_at=now,
        )
    )
    await _seed_school_year_truth(
        session,
        school=school,
        metric_year=now.year,
        grad_rate=92.0,
        retention_rate=88.0,
        earnings=81000,
        doctoral_share=14.0,
    )
    await session.flush()

    payload = await run_phase4_training_prep(
        session,
        run_id="phase4-prep-payload-test",
        output_dir=str(tmp_path),
        lookback_days=540,
        target_eligible_snapshots=1,
        school_names=[school.name],
        cycle_year=now.year,
        ingest_official_facts_enabled=False,
        ingest_ipeds_completions_enabled=False,
        max_auto_schools=20,
    )

    assert payload["materialization"]["created"] >= 4
    assert payload["materialization"]["eligible_snapshots"] >= 1
    assert payload["strict_true_only"]["passed"] is True
    assert payload["quality_gate"]["passed"] is True
    assert payload["stage1_readiness"]["passed"] is False
    assert any("academic_outcome_rows<3000" in reason for reason in payload["stage1_readiness"]["reasons"])
