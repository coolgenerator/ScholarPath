from __future__ import annotations

from datetime import datetime, timezone
import uuid

import pytest
from sqlalchemy import func, select

from scholarpath.db.models import (
    AdmissionEvent,
    CanonicalFact,
    CausalOutcomeEvent,
    DocumentChunk,
    PolicyFact,
    PolicyFactAudit,
    RawDocument,
    School,
)
from scholarpath.scripts import admission_data_phase2_pipeline
from scholarpath.search.sources.base import SearchResult
from scholarpath.services import causal_data_service


class _FakeLLM:
    async def complete_json(self, _messages, **_kwargs):
        return {"decision": "keep", "confidence": 0.93, "reason": "ok"}


class _RejectingLLM:
    async def complete_json(self, _messages, **_kwargs):
        return {"decision": "quarantine", "confidence": 0.82, "reason": "judge_reject"}


class _SingleFactSource:
    name = "fake_official"

    async def search(self, school_name: str, fields: list[str] | None = None):
        _ = school_name, fields
        return [
            SearchResult(
                source_name="fake_official",
                source_type="official",
                source_url="https://example.test/facts",
                variable_name="acceptance_rate",
                value_text="8%",
                value_numeric=0.08,
                confidence=0.91,
                raw_data={"fetch_mode": "search_api", "extractor_version": "phase2-test"},
            )
        ]


async def _create_school(session) -> School:
    school = School(
        name=f"Phase2 School {uuid.uuid4().hex[:6]}",
        city="Boston",
        state="MA",
        school_type="university",
        size_category="medium",
    )
    session.add(school)
    await session.flush()
    return school


@pytest.mark.asyncio
async def test_adjust_school_concurrency_controller_behavior():
    assert causal_data_service._adjust_school_concurrency(
        current=4,
        rpm_actual=120.0,
        rate_limit_errors=0,
        rpm_band_low=170.0,
        rpm_band_high=185.0,
        school_concurrency_max=10,
    ) == 5

    assert causal_data_service._adjust_school_concurrency(
        current=7,
        rpm_actual=191.0,
        rate_limit_errors=0,
        rpm_band_low=170.0,
        rpm_band_high=185.0,
        school_concurrency_max=10,
    ) == 5

    assert causal_data_service._adjust_school_concurrency(
        current=6,
        rpm_actual=174.0,
        rate_limit_errors=1,
        rpm_band_low=170.0,
        rpm_band_high=185.0,
        school_concurrency_max=10,
    ) == 4

    assert causal_data_service._adjust_school_concurrency(
        current=6,
        rpm_actual=176.0,
        rate_limit_errors=0,
        rpm_band_low=170.0,
        rpm_band_high=185.0,
        school_concurrency_max=10,
    ) == 6


@pytest.mark.asyncio
async def test_ingest_official_facts_phase2_tables_idempotent_and_required_fields(session, monkeypatch):
    school = await _create_school(session)
    await session.commit()

    monkeypatch.setattr(causal_data_service, "_build_official_sources", lambda: [_SingleFactSource()])

    first = await causal_data_service.ingest_official_facts(
        session,
        school_names=[school.name],
        cycle_year=2026,
        run_id="phase2-idempotent-1",
        llm=_FakeLLM(),
    )
    second = await causal_data_service.ingest_official_facts(
        session,
        school_names=[school.name],
        cycle_year=2026,
        run_id="phase2-idempotent-2",
        llm=_FakeLLM(),
    )
    await session.commit()

    raw_documents = int((await session.scalar(select(func.count()).select_from(RawDocument))) or 0)
    chunks = int((await session.scalar(select(func.count()).select_from(DocumentChunk))) or 0)
    policy_facts = int((await session.scalar(select(func.count()).select_from(PolicyFact))) or 0)
    policy_audits = int((await session.scalar(select(func.count()).select_from(PolicyFactAudit))) or 0)
    admission_events = int((await session.scalar(select(func.count()).select_from(AdmissionEvent))) or 0)
    outcomes = int((await session.scalar(select(func.count()).select_from(CausalOutcomeEvent))) or 0)

    assert first["raw_documents_created"] == 1
    assert first["document_chunks_created"] == 1
    assert first["policy_facts_created"] == 1
    assert second["raw_documents_created"] == 0
    assert second["document_chunks_created"] == 0
    assert second["policy_facts_created"] == 0

    assert raw_documents == 1
    assert chunks == 1
    assert policy_facts == 1
    assert policy_audits >= 6  # extracted/validated/accepted per run

    assert admission_events == 0
    assert outcomes == 0

    row = (await session.execute(select(PolicyFact))).scalars().one()
    assert row.source_url
    assert row.evidence_quote
    assert row.extractor_version
    assert row.reviewed_flag is True
    assert row.confidence >= 0.0


@pytest.mark.asyncio
async def test_ingest_official_facts_phase2_reject_path_records_audit(session, monkeypatch):
    school = await _create_school(session)
    await session.commit()

    monkeypatch.setattr(causal_data_service, "_build_official_sources", lambda: [_SingleFactSource()])

    result = await causal_data_service.ingest_official_facts(
        session,
        school_names=[school.name],
        cycle_year=2026,
        run_id="phase2-reject",
        llm=_RejectingLLM(),
    )
    await session.commit()

    policy_facts = int((await session.scalar(select(func.count()).select_from(PolicyFact))) or 0)
    rejected_audits = int(
        (
            await session.scalar(
                select(func.count()).select_from(PolicyFactAudit).where(PolicyFactAudit.action == "rejected")
            )
        )
        or 0
    )

    assert result["quarantined_count"] >= 1
    assert policy_facts == 0
    assert rejected_audits >= 1


def test_phase2_parse_explicit_schools_supports_repeatable_flag_and_pipe_delimiter():
    args = admission_data_phase2_pipeline._build_parser().parse_args(
        [
            "--scope",
            "explicit",
            "--school",
            "University of California, Berkeley",
            "--school",
            "University of Michigan, Ann Arbor",
            "--schools",
            "University of California, Davis|University of Minnesota, Twin Cities",
        ]
    )
    schools = admission_data_phase2_pipeline._parse_explicit_schools(args)
    assert schools == [
        "University of California, Berkeley",
        "University of Michigan, Ann Arbor",
        "University of California, Davis",
        "University of Minnesota, Twin Cities",
    ]


def test_phase2_gate_skips_rpm_band_for_low_call_volume():
    gate = admission_data_phase2_pipeline._evaluate_phase2_gate(
        school_names=["A"],
        school_table_counts={"A": {"policy_facts": 1}},
        merged_run={
            "rpm_windows": [{"rpm_actual": 30.0}, {"rpm_actual": 45.0}],
            "rate_limit_error_count": 0,
            "kept_count": 10,
            "conflicts_count": 0,
            "llm_calls_extract": 0,
            "llm_calls_judge": 25,
        },
        truth_before={"admission_events": 1, "causal_outcome_events": 1},
        truth_after={"admission_events": 1, "causal_outcome_events": 1},
        rpm_band_low=170.0,
        rpm_band_high=185.0,
    )
    assert gate["passed"] is True
    assert gate["observed"]["rpm_band_evaluable"] is False
    assert "rpm_in_band_rate_lt_0.8" not in gate["reasons"]


def test_phase2_gate_enforces_rpm_band_when_call_volume_is_enough():
    gate = admission_data_phase2_pipeline._evaluate_phase2_gate(
        school_names=["A"],
        school_table_counts={"A": {"policy_facts": 1}},
        merged_run={
            "rpm_windows": [{"rpm_actual": 100.0}, {"rpm_actual": 120.0}, {"rpm_actual": 130.0}],
            "rate_limit_error_count": 0,
            "kept_count": 10,
            "conflicts_count": 0,
            "llm_calls_extract": 0,
            "llm_calls_judge": 220,
        },
        truth_before={"admission_events": 1, "causal_outcome_events": 1},
        truth_after={"admission_events": 1, "causal_outcome_events": 1},
        rpm_band_low=170.0,
        rpm_band_high=185.0,
    )
    assert gate["passed"] is False
    assert "rpm_in_band_rate_lt_0.8" in gate["reasons"]


@pytest.mark.asyncio
async def test_check_conflict_ignores_cross_source_variation(session):
    school = await _create_school(session)
    session.add(
        CanonicalFact(
            school_id=school.id,
            cycle_year=2026,
            outcome_name="enrollment",
            canonical_value_text="41,000+ students",
            canonical_value_numeric=41000.0,
            canonical_value_bucket="41000",
            source_family="cds_parser",
            confidence=0.8,
            observed_at=datetime.now(timezone.utc),
            metadata_={"source_url": "https://admissions.example.edu"},
        )
    )
    await session.commit()

    conflict = await causal_data_service._check_conflict(
        session=session,
        school_id=school.id,
        cycle_year=2026,
        variable_name="enrollment",
        value_numeric=30760.0,
        value_text="30760",
        source_family="college_scorecard",
        source_url="https://collegescorecard.ed.gov/school/?123456",
    )
    assert conflict is None


@pytest.mark.asyncio
async def test_check_conflict_replaces_mismatched_scorecard_school_id(session):
    school = await _create_school(session)
    session.add(
        CanonicalFact(
            school_id=school.id,
            cycle_year=2026,
            outcome_name="acceptance_rate",
            canonical_value_text="74.19%",
            canonical_value_numeric=74.19,
            canonical_value_bucket="74.19",
            source_family="college_scorecard",
            confidence=0.9,
            observed_at=datetime.now(timezone.utc),
            metadata_={"source_url": "https://collegescorecard.ed.gov/school/?399869"},
        )
    )
    await session.commit()

    conflict = await causal_data_service._check_conflict(
        session=session,
        school_id=school.id,
        cycle_year=2026,
        variable_name="acceptance_rate",
        value_numeric=2.57,
        value_text="2.57%",
        source_family="college_scorecard",
        source_url="https://collegescorecard.ed.gov/school/?110404",
    )
    assert conflict is not None
    assert "replace_fact_id" in conflict
