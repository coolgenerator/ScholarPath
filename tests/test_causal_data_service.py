from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select

from scholarpath.db.models import CanonicalFact, EvidenceArtifact, FactLineage, School
from scholarpath.search.sources.base import SearchResult
from scholarpath.services import causal_data_service


class _FakeLLM:
    async def complete_json(self, _messages, **_kwargs):
        return {"decision": "keep", "confidence": 0.9, "reason": "ok"}


class _FakeSource:
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
                confidence=0.92,
            )
        ]


class _RichFakeSource:
    name = "rich_fake_official"

    async def search(self, school_name: str, fields: list[str] | None = None):
        _ = school_name, fields
        return [
            SearchResult(
                source_name="rich_fake_official",
                source_type="official",
                source_url="https://example.test/facts",
                variable_name="acceptance_rate",
                value_text="8%",
                value_numeric=0.08,
                confidence=0.92,
            ),
            SearchResult(
                source_name="rich_fake_official",
                source_type="official",
                source_url="https://example.test/facts",
                variable_name="avg_net_price",
                value_text="$19,066",
                value_numeric=19066,
                confidence=0.92,
            ),
            SearchResult(
                source_name="rich_fake_official",
                source_type="official",
                source_url="https://example.test/facts",
                variable_name="student_faculty_ratio",
                value_text="7",
                value_numeric=7,
                confidence=0.92,
            ),
            SearchResult(
                source_name="rich_fake_official",
                source_type="official",
                source_url="https://example.test/facts",
                variable_name="graduation_rate_4yr",
                value_text="97.45%",
                value_numeric=0.9745,
                confidence=0.92,
            ),
            SearchResult(
                source_name="rich_fake_official",
                source_type="official",
                source_url="https://example.test/facts",
                variable_name="endowment_per_student",
                value_text="7000",
                value_numeric=7000,
                confidence=0.92,
            ),
        ]


class _EmptyFakeSource:
    name = "empty_fake_official"

    async def search(self, school_name: str, fields: list[str] | None = None):
        _ = school_name, fields
        return []


class _LowConfidenceIpedsSource:
    name = "ipeds_college_navigator"

    async def search_for_school(
        self,
        *,
        school_name: str,
        school_state: str,
        website_url: str | None,
        fields: list[str] | None = None,
        external_ids: dict[str, str] | None = None,
    ):
        _ = school_name, school_state, website_url, fields, external_ids
        return [
            SearchResult(
                source_name="ipeds_college_navigator",
                source_type="official",
                source_url="https://nces.ed.gov",
                variable_name="acceptance_rate",
                value_text="8%",
                value_numeric=8.0,
                confidence=0.8,
                raw_data={
                    "fetch_mode": "ipeds_bulk",
                    "match_method": "name_state",
                    "match_confidence": 0.6,
                    "external_id": "12345",
                },
            )
        ]


async def _create_school(session) -> School:
    school = School(
        name=f"Causal Real {uuid.uuid4().hex[:6]}",
        city="Boston",
        state="MA",
        school_type="university",
        size_category="medium",
        us_news_rank=15,
        acceptance_rate=0.11,
        sat_25=1380,
        sat_75=1530,
        tuition_oos=62000,
        avg_net_price=31000,
    )
    session.add(school)
    await session.flush()
    return school


@pytest.mark.asyncio
async def test_register_evidence_artifact_dedup_by_source_hash(session):
    school = await _create_school(session)
    row1 = await causal_data_service.register_evidence_artifact(
        session,
        student_id=None,
        school_id=str(school.id),
        cycle_year=2026,
        source_name="manual",
        source_type="user_upload",
        source_url="https://example.test/evidence",
        content_text="same fact payload",
        metadata={"k": "v"},
    )
    row2 = await causal_data_service.register_evidence_artifact(
        session,
        student_id=None,
        school_id=str(school.id),
        cycle_year=2026,
        source_name="manual",
        source_type="user_upload",
        source_url="https://example.test/evidence",
        content_text="same fact payload",
        metadata={"k": "v2"},
    )
    await session.commit()

    assert row1.id == row2.id
    evidence_rows = int(
        (await session.scalar(select(func.count()).select_from(EvidenceArtifact))) or 0
    )
    assert evidence_rows == 1


@pytest.mark.asyncio
async def test_ingest_official_facts_is_idempotent_for_canonical_and_lineage(
    session,
    monkeypatch,
):
    school = await _create_school(session)
    await session.commit()

    monkeypatch.setattr(
        causal_data_service,
        "_build_official_sources",
        lambda: [_FakeSource()],
    )
    llm = _FakeLLM()

    first = await causal_data_service.ingest_official_facts(
        session,
        school_names=[school.name],
        cycle_year=2026,
        run_id="idempotent-run-1",
        llm=llm,
    )
    second = await causal_data_service.ingest_official_facts(
        session,
        school_names=[school.name],
        cycle_year=2026,
        run_id="idempotent-run-2",
        llm=llm,
    )
    await session.commit()

    canonical_rows = int(
        (await session.scalar(select(func.count()).select_from(CanonicalFact))) or 0
    )
    lineage_rows = int(
        (await session.scalar(select(func.count()).select_from(FactLineage))) or 0
    )
    evidence_rows = int(
        (await session.scalar(select(func.count()).select_from(EvidenceArtifact))) or 0
    )

    assert canonical_rows == 1
    assert lineage_rows == 1
    assert evidence_rows == 1
    assert first["kept_count"] == 1
    assert second["deduped_count"] >= 1


@pytest.mark.asyncio
async def test_ingest_official_facts_hydrates_school_fields_and_metadata(
    session,
    monkeypatch,
):
    school = await _create_school(session)
    school.acceptance_rate = None
    school.avg_net_price = None
    school.student_faculty_ratio = None
    school.graduation_rate_4yr = None
    school.endowment_per_student = None
    school.website_url = None
    school.metadata_ = None
    await session.flush()

    monkeypatch.setattr(
        causal_data_service,
        "_build_official_sources",
        lambda: [_RichFakeSource()],
    )

    result = await causal_data_service.ingest_official_facts(
        session,
        school_names=[school.name],
        cycle_year=2026,
        run_id="hydrate-run",
        llm=_FakeLLM(),
    )
    await session.commit()
    await session.refresh(school)

    assert result["schools_updated_count"] == 1
    assert result["schools_updated"] == [str(school.id)]
    assert school.acceptance_rate == pytest.approx(0.08)
    assert school.avg_net_price == 19066
    assert school.student_faculty_ratio == pytest.approx(7.0)
    assert school.graduation_rate_4yr == pytest.approx(0.9745)
    assert school.endowment_per_student == 7000
    assert isinstance(school.metadata_, dict)
    official = school.metadata_.get("official_facts") or {}
    assert official.get("field_count") >= 5
    assert "acceptance_rate" in (official.get("fields") or {})


@pytest.mark.asyncio
async def test_ingest_official_facts_uses_direct_html_fallback_when_search_sources_empty(
    session,
    monkeypatch,
):
    school = await _create_school(session)
    school.website_url = "https://example.edu"
    school.metadata_ = None
    await session.flush()

    monkeypatch.setattr(causal_data_service, "_build_official_sources", lambda: [_EmptyFakeSource()])

    async def _fake_profile_direct(_session, *, school, fields, run_id):
        _ = fields, run_id
        return [
            SearchResult(
                source_name="school_official_profile",
                source_type="official",
                source_url="https://example.edu/admissions",
                variable_name="acceptance_rate",
                value_text="8%",
                value_numeric=0.08,
                confidence=0.81,
                raw_data={
                    "school_name": school.name,
                    "fetch_mode": "direct_html",
                    "source_kind": "official_direct_fetch",
                },
            )
        ]

    async def _fake_cds_direct(*args, **kwargs):
        _ = args, kwargs
        return []

    monkeypatch.setattr(causal_data_service, "fetch_school_official_profile_direct", _fake_profile_direct)
    monkeypatch.setattr(causal_data_service, "fetch_common_dataset_direct", _fake_cds_direct)

    result = await causal_data_service.ingest_official_facts(
        session,
        school_names=[school.name],
        cycle_year=2026,
        run_id="direct-html-fallback",
        llm=_FakeLLM(),
    )
    await session.commit()
    await session.refresh(school)

    assert result["status"] == "ok"
    assert result["kept_count"] == 1
    official = (school.metadata_ or {}).get("official_facts") or {}
    fields = official.get("fields") or {}
    assert fields["acceptance_rate"]["fetch_mode"] == "direct_html"
    assert fields["acceptance_rate"]["source_name"] == "school_official_profile"


@pytest.mark.asyncio
async def test_ingest_official_facts_uses_direct_pdf_fallback_when_cds_url_present(
    session,
    monkeypatch,
):
    school = await _create_school(session)
    school.website_url = "https://example.edu"
    school.cds_url = "https://example.edu/common-data-set.pdf"
    school.metadata_ = None
    await session.flush()

    monkeypatch.setattr(causal_data_service, "_build_official_sources", lambda: [_EmptyFakeSource()])

    async def _fake_profile_direct(*args, **kwargs):
        _ = args, kwargs
        return []

    async def _fake_cds_direct(_session, *, school, fields, run_id):
        _ = fields, run_id
        return [
            SearchResult(
                source_name="cds_parser",
                source_type="official",
                source_url="https://example.edu/common-data-set.pdf",
                variable_name="graduation_rate_4yr",
                value_text="97%",
                value_numeric=0.97,
                confidence=0.86,
                raw_data={
                    "school_name": school.name,
                    "fetch_mode": "direct_pdf",
                    "source_kind": "official_direct_fetch",
                },
            )
        ]

    monkeypatch.setattr(causal_data_service, "fetch_school_official_profile_direct", _fake_profile_direct)
    monkeypatch.setattr(causal_data_service, "fetch_common_dataset_direct", _fake_cds_direct)

    result = await causal_data_service.ingest_official_facts(
        session,
        school_names=[school.name],
        cycle_year=2026,
        run_id="direct-pdf-fallback",
        llm=_FakeLLM(),
    )
    await session.commit()
    await session.refresh(school)

    assert result["status"] == "ok"
    assert result["kept_count"] == 1
    official = (school.metadata_ or {}).get("official_facts") or {}
    fields = official.get("fields") or {}
    assert fields["graduation_rate_4yr"]["fetch_mode"] == "direct_pdf"
    assert fields["graduation_rate_4yr"]["source_name"] == "cds_parser"


@pytest.mark.asyncio
async def test_ingest_official_facts_quarantines_low_confidence_ipeds_match(
    session,
    monkeypatch,
):
    school = await _create_school(session)
    school.metadata_ = None
    await session.flush()

    monkeypatch.setattr(
        causal_data_service,
        "_build_official_sources",
        lambda: [_LowConfidenceIpedsSource()],
    )

    result = await causal_data_service.ingest_official_facts(
        session,
        school_names=[school.name],
        cycle_year=2026,
        run_id="ipeds-low-confidence",
        llm=_FakeLLM(),
    )
    await session.commit()

    canonical_rows = int(
        (await session.scalar(select(func.count()).select_from(CanonicalFact))) or 0
    )
    assert result["quarantined_count"] >= 1
    assert result["external_id_match_rate"] == 0.0
    assert canonical_rows == 0
