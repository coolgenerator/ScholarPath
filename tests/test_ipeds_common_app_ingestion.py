from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import func, select

from scholarpath.db.models import (
    CanonicalFact,
    CausalOutcomeEvent,
    CausalTrendSignal,
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
