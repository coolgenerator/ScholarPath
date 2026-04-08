from __future__ import annotations

import json
import uuid
import zipfile

import pytest
from sqlalchemy import func, select

from scholarpath.db.models import (
    CanonicalFact,
    CausalOutcomeEvent,
    CausalTrendSignal,
    Program,
    School,
    SchoolExternalId,
)
from scholarpath.search.sources.ipeds_college_navigator import IPEDSCollegeNavigatorSource
from scholarpath.services import causal_data_service


def _write_ipeds_csv(path) -> None:
    path.write_text(
        "\n".join(
            [
                "unitid,institution_name,state,city,website_url,year,applicants_total,admitted_total,enrolled_total,acceptance_rate,yield_rate",
                "1001,Test University,MA,Boston,https://testu.edu,2025,50000,4000,1600,0.08,0.40",
                "1002,Other College,CA,Los Angeles,https://other.edu,2025,30000,6000,1200,0.20,0.20",
            ]
        ),
        encoding="utf-8",
    )


def _write_ipeds_program_csv(path) -> None:
    path.write_text(
        "\n".join(
            [
                "unitid,institution_name,state,city,website_url,year,applicants_total,cip_code,cip_title,award_level,completions_total",
                "1001,Test University,MA,Boston,https://testu.edu,2024,50000,11.0701,Computer Science,bachelor,120",
                "1001,Test University,MA,Boston,https://testu.edu,2025,52000,11.0701,Computer Science,bachelor,130",
                "1001,Test University,MA,Boston,https://testu.edu,2025,52000,42.0101,Psychology,bachelor,90",
                "1002,Other College,CA,Los Angeles,https://other.edu,2025,30000,14.0901,Computer Engineering,bachelor,60",
            ]
        ),
        encoding="utf-8",
    )


def _write_zip_csv(path, member_name: str, content: str) -> None:
    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member_name, content)


@pytest.mark.asyncio
async def test_ipeds_source_prefers_external_id_match(tmp_path):
    csv_path = tmp_path / "ipeds.csv"
    _write_ipeds_csv(csv_path)
    source = IPEDSCollegeNavigatorSource(dataset_path=str(csv_path))

    rows = await source.search_for_school(
        school_name="Test University",
        school_state="MA",
        website_url="https://testu.edu",
        fields=["acceptance_rate"],
        external_ids={"ipeds": "1001"},
    )

    assert len(rows) == 1
    assert rows[0].source_name == "ipeds_college_navigator"
    assert rows[0].raw_data["match_method"] == "external_id"
    assert rows[0].raw_data["external_id"] == "1001"


@pytest.mark.asyncio
async def test_ipeds_source_lists_program_completions(tmp_path):
    csv_path = tmp_path / "ipeds_programs.csv"
    _write_ipeds_program_csv(csv_path)
    source = IPEDSCollegeNavigatorSource(dataset_path=str(csv_path))

    rows = await source.list_program_completions(
        years=3,
        min_completions=80,
        award_levels={"bachelor"},
    )

    assert len(rows) == 2
    assert rows[0]["cip_code"] == "11.0701"
    assert rows[0]["completions"] == 130
    assert rows[1]["cip_code"] == "42.0101"
    assert rows[1]["award_level"] == "bachelor"


@pytest.mark.asyncio
async def test_ipeds_source_lists_program_completions_from_dual_zip(tmp_path):
    completions_zip = tmp_path / "c2024_a.zip"
    institution_zip = tmp_path / "hd2024.zip"
    _write_zip_csv(
        completions_zip,
        "c2024_a.csv",
        "\n".join(
            [
                "UNITID,CIPCODE,AWLEVEL,CTOTALT",
                "1001,11.0701,5,130",
                "1001,42.0101,5,90",
                "1002,14.0901,5,40",
            ]
        ),
    )
    _write_zip_csv(
        institution_zip,
        "hd2024.csv",
        "\n".join(
            [
                "UNITID,INSTNM,CITY,STABBR,WEBADDR",
                "1001,Test University,Boston,MA,https://testu.edu",
                "1002,Other College,Los Angeles,CA,https://other.edu",
            ]
        ),
    )
    source = IPEDSCollegeNavigatorSource(
        completions_dataset_path=str(completions_zip),
        institution_dataset_path=str(institution_zip),
    )

    rows = await source.list_program_completions(
        years=3,
        min_completions=80,
        award_levels={"bachelor"},
    )

    assert len(rows) == 2
    assert rows[0]["external_id"] == "1001"
    assert rows[0]["school_name"] == "Test University"
    assert rows[0]["state"] == "MA"
    assert rows[0]["city"] == "Boston"
    assert rows[0]["cip_code"] == "11.0701"
    assert rows[0]["award_level"] == "bachelor"
    assert rows[0]["year"] == 2024


@pytest.mark.asyncio
async def test_ingest_ipeds_school_pool_upserts_school_and_mapping(session, monkeypatch, tmp_path):
    csv_path = tmp_path / "ipeds_pool.csv"
    _write_ipeds_csv(csv_path)
    monkeypatch.setattr(causal_data_service.settings, "IPEDS_DATASET_PATH", str(csv_path))
    monkeypatch.setattr(causal_data_service.settings, "IPEDS_DATASET_URL", "")

    first = await causal_data_service.ingest_ipeds_school_pool(
        session,
        run_id="ipeds-seed-1",
        top_schools=1,
        years=5,
        selection_metric="applicants_total",
    )
    await session.commit()
    second = await causal_data_service.ingest_ipeds_school_pool(
        session,
        run_id="ipeds-seed-2",
        top_schools=1,
        years=5,
        selection_metric="applicants_total",
    )
    await session.commit()

    school_count = int((await session.scalar(select(func.count()).select_from(School))) or 0)
    mapping_count = int((await session.scalar(select(func.count()).select_from(SchoolExternalId))) or 0)

    assert first["schools_upserted"] == 1
    assert second["schools_upserted"] == 0
    assert school_count == 1
    assert mapping_count == 1


@pytest.mark.asyncio
async def test_common_app_trend_ingestion_is_trend_only(session, monkeypatch, tmp_path):
    trend_path = tmp_path / "common_app.json"
    trend_path.write_text(
        json.dumps(
            [
                {
                    "metric": "applications_yoy_growth",
                    "period": "2025",
                    "segment": "us_first_year",
                    "value": "6.3",
                    "source_url": "https://example.test/common-app-report",
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(causal_data_service.settings, "COMMON_APP_TREND_PATH", str(trend_path))
    monkeypatch.setattr(causal_data_service.settings, "COMMON_APP_TREND_URL", "")

    result = await causal_data_service.ingest_common_app_trends(
        session,
        run_id=f"trend-{uuid.uuid4().hex[:8]}",
        years=5,
    )
    await session.commit()

    trend_rows = int((await session.scalar(select(func.count()).select_from(CausalTrendSignal))) or 0)
    canonical_rows = int((await session.scalar(select(func.count()).select_from(CanonicalFact))) or 0)
    outcome_rows = int((await session.scalar(select(func.count()).select_from(CausalOutcomeEvent))) or 0)

    assert result["signals_written"] == 1
    assert trend_rows == 1
    assert canonical_rows == 0
    assert outcome_rows == 0


@pytest.mark.asyncio
async def test_ingest_ipeds_program_facts_upserts_program_rows(session, monkeypatch, tmp_path):
    csv_path = tmp_path / "ipeds_programs.csv"
    _write_ipeds_program_csv(csv_path)
    monkeypatch.setattr(causal_data_service.settings, "IPEDS_DATASET_PATH", str(csv_path))
    monkeypatch.setattr(causal_data_service.settings, "IPEDS_DATASET_URL", "")
    monkeypatch.setattr(causal_data_service.settings, "IPEDS_COMPLETIONS_DATASET_PATH", "")
    monkeypatch.setattr(causal_data_service.settings, "IPEDS_COMPLETIONS_DATASET_URL", "")
    monkeypatch.setattr(causal_data_service.settings, "IPEDS_INSTITUTION_DATASET_PATH", "")
    monkeypatch.setattr(causal_data_service.settings, "IPEDS_INSTITUTION_DATASET_URL", "")

    seed = await causal_data_service.ingest_ipeds_school_pool(
        session,
        run_id="ipeds-seed-program-1",
        top_schools=2,
        years=5,
        selection_metric="applicants_total",
    )
    await session.commit()
    assert seed["schools_upserted"] == 2

    first = await causal_data_service.ingest_ipeds_program_facts(
        session,
        run_id="ipeds-program-1",
        years=3,
        min_completions=80,
        award_levels=["bachelor"],
    )
    await session.commit()

    second = await causal_data_service.ingest_ipeds_program_facts(
        session,
        run_id="ipeds-program-2",
        years=3,
        min_completions=80,
        award_levels=["bachelor"],
    )
    await session.commit()

    program_rows = (await session.execute(select(Program))).scalars().all()
    assert len(program_rows) == 2
    assert first["programs_inserted"] == 2
    assert first["programs_updated"] == 0
    assert second["programs_inserted"] == 0
    assert second["programs_updated"] == 0

    descriptions = [json.loads(str(row.description or "{}")) for row in program_rows]
    by_cip = {str(item.get("cip_code")): item for item in descriptions}
    assert by_cip["11.0701"]["year"] == 2025
    assert by_cip["11.0701"]["completions"] == 130
    assert by_cip["42.0101"]["completions"] == 90
