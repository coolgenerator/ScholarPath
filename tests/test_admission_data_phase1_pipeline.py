from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select

from scholarpath.db.models import (
    FactQuarantine,
    Institution,
    RawSourceSnapshot,
    RawStructuredRecord,
    School,
    SchoolExternalId,
    SchoolMetricsYear,
    SourceEntityMap,
)
from scholarpath.services import admission_data_phase1_service
from scholarpath.services.admission_data_phase1_service import (
    _evaluate_phase1_gate,
    _load_payload_from_path_or_url,
    _rows_for_source,
    _validate_metrics,
    run_admission_phase1_pipeline,
)


def _write_scorecard_csv(path) -> None:
    path.write_text(
        "\n".join(
            [
                "UNITID,INSTNM,STABBR,CITY,YEAR,ADM_RATE,APPLCN,ADMSSN,ENRLT,NPT4_PUB,C150_4,SAT_25,SAT_75",
                "1001,Test University,MA,Boston,2025,0.12,20000,2500,1200,21000,0.90,1300,1500",
                "1002,Other College,CA,Los Angeles,2025,0.24,30000,7000,1600,28000,0.82,1200,1450",
            ]
        ),
        encoding="utf-8",
    )


def _write_ipeds_csv(path) -> None:
    path.write_text(
        "\n".join(
            [
                "unitid,institution_name,state,city,year,applicants_total,admitted_total,enrolled_total,acceptance_rate,yield_rate,avg_net_price,graduation_rate_4yr,sat_25,sat_75,act_25,act_75",
                "1001,Test University,MA,Boston,2025,20100,2600,1210,0.129,0.465,20800,0.905,1290,1490,30,34",
                "1002,Other College,CA,Los Angeles,2025,30100,7100,1610,0.236,0.226,27900,0.818,1190,1440,27,32",
            ]
        ),
        encoding="utf-8",
    )


@pytest.mark.asyncio
async def test_rows_for_source_extracts_year_and_metrics():
    rows = _rows_for_source(
        [
            {
                "UNITID": "2001",
                "INSTNM": "Sample U",
                "STABBR": "NY",
                "YEAR": "2024",
                "ADM_RATE": "0.19",
                "APPLCN": "10000",
                "ADMSSN": "2500",
                "ENRLT": "900",
                "NPT4_PUB": "23000",
                "C150_4": "0.81",
            }
        ],
        alias_map=admission_data_phase1_service._SCORECARD_ALIAS,
        source_name="college_scorecard_bulk",
        fallback_year=2026,
    )
    assert len(rows) == 1
    assert rows[0]["data_year"] == 2024
    assert rows[0]["metrics"]["applications"] == 10000
    assert rows[0]["metrics"]["admits"] == 2500
    assert rows[0]["metrics"]["enrolled"] == 900
    assert rows[0]["metrics"]["admit_rate"] == 0.19


def test_validate_metrics_flags_impossible_counts():
    issues = _validate_metrics(
        {
            "applications": 1000,
            "admits": 1200,
            "enrolled": 1300,
            "admit_rate": 0.8,
            "yield_rate": 1.2,
            "grad_rate": 0.9,
        }
    )
    assert "applications_lt_admits" in issues
    assert "admits_lt_enrolled" in issues
    assert "yield_rate_out_of_range" in issues


@pytest.mark.asyncio
async def test_load_payload_prefers_local_path(monkeypatch, tmp_path):
    local = tmp_path / "scorecard.csv"
    local.write_bytes(b"local-scorecard")

    async def _boom(_url: str):
        raise AssertionError("network should not be called when local path is present")

    monkeypatch.setattr(admission_data_phase1_service, "_download_bytes", _boom)

    payload, source_url, file_path, source_version = await _load_payload_from_path_or_url(
        local_path=str(local),
        url="https://example.com/scorecard.zip",
        fallback_urls=("https://example.com/fallback.zip",),
        download_dir=tmp_path / "downloaded",
        filename_hint="scorecard_bulk",
    )

    assert payload == b"local-scorecard"
    assert source_url is None
    assert file_path == str(local)
    assert source_version == local.name


def test_evaluate_phase1_gate_reasons():
    summary = {
        "coverage": {
            "mapped_school_rate": 0.9,
            "admit_rate_school_coverage": 0.8,
            "avg_net_price_school_coverage": 0.9,
        },
        "sources": {
            "college_scorecard_bulk": {
                "rows_read": 0,
            }
        },
        "truth_counts": {
            "before": {"admission_events": 7, "causal_outcome_events": 5},
            "after": {"admission_events": 8, "causal_outcome_events": 5},
        },
    }
    gate = _evaluate_phase1_gate(
        summary,
        min_admit_rate_coverage=0.95,
        min_net_price_coverage=0.95,
    )
    assert gate["passed"] is False
    assert "mapped_school_rate_not_1.0" in gate["reasons"]
    assert "admit_rate_coverage_lt_0.95" in gate["reasons"]
    assert "avg_net_price_coverage_lt_0.95" in gate["reasons"]
    assert "scorecard_bulk_rows_read_eq_0" in gate["reasons"]
    assert "admission_events_changed" in gate["reasons"]


@pytest.mark.asyncio
async def test_phase1_pipeline_idempotent_for_bronze_to_silver(session, monkeypatch, tmp_path):
    scorecard_path = tmp_path / "scorecard.csv"
    ipeds_path = tmp_path / "ipeds.csv"
    _write_scorecard_csv(scorecard_path)
    _write_ipeds_csv(ipeds_path)

    school_a = School(
        name="Test University",
        city="Boston",
        state="MA",
        school_type="university",
        size_category="large",
    )
    school_b = School(
        name="Other College",
        city="Los Angeles",
        state="CA",
        school_type="university",
        size_category="large",
    )
    session.add(school_a)
    session.add(school_b)
    await session.flush()
    session.add(
        SchoolExternalId(
            school_id=school_a.id,
            provider="ipeds",
            external_id="1001",
            is_primary=True,
            match_method="seed",
            confidence=0.99,
            metadata_={"source": "test"},
        )
    )
    await session.commit()

    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_BULK_PATH", str(scorecard_path))
    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_BULK_URL", "")
    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_API_KEY", "")
    monkeypatch.setattr(admission_data_phase1_service.settings, "IPEDS_DATASET_PATH", str(ipeds_path))
    monkeypatch.setattr(admission_data_phase1_service.settings, "IPEDS_DATASET_URL", "")

    first = await run_admission_phase1_pipeline(
        session,
        run_id=f"phase1-{uuid.uuid4().hex[:6]}",
        scope="existing_65",
        dry_run=False,
        metric_year=2025,
        output_dir=str(tmp_path / "out"),
    )
    await session.commit()
    second = await run_admission_phase1_pipeline(
        session,
        run_id=f"phase1-{uuid.uuid4().hex[:6]}",
        scope="existing_65",
        dry_run=False,
        metric_year=2025,
        output_dir=str(tmp_path / "out"),
    )
    await session.commit()

    snapshots = int((await session.scalar(select(func.count()).select_from(RawSourceSnapshot))) or 0)
    raw_records = int((await session.scalar(select(func.count()).select_from(RawStructuredRecord))) or 0)
    maps = int((await session.scalar(select(func.count()).select_from(SourceEntityMap))) or 0)
    institutions = int((await session.scalar(select(func.count()).select_from(Institution))) or 0)
    metrics = int((await session.scalar(select(func.count()).select_from(SchoolMetricsYear))) or 0)

    assert first["status"] == "ok"
    assert second["status"] == "ok"
    assert first["gate"]["passed"] is True
    assert second["gate"]["passed"] is True
    assert snapshots == 2  # one Scorecard snapshot + one IPEDS snapshot
    assert raw_records > 0
    assert maps == 4  # 2 schools * 2 sources
    assert institutions >= 2
    assert metrics == 4  # 2 schools * 2 sources


class _FailingJudge:
    async def complete_json(self, *_args, **_kwargs):
        raise RuntimeError("judge endpoint unavailable")


@pytest.mark.asyncio
async def test_phase1_pipeline_degrades_when_judge_unavailable(session, monkeypatch, tmp_path):
    scorecard_path = tmp_path / "scorecard_invalid.csv"
    scorecard_path.write_text(
        "\n".join(
            [
                "UNITID,INSTNM,STABBR,CITY,YEAR,APPLCN,ADMSSN,ENRLT,ADM_RATE",
                "2001,Invalid School,TX,Austin,2025,1000,1500,900,0.8",
            ]
        ),
        encoding="utf-8",
    )
    ipeds_path = tmp_path / "ipeds_empty.csv"
    ipeds_path.write_text(
        "unitid,institution_name,state,city,year,applicants_total,admitted_total,enrolled_total,acceptance_rate,yield_rate\n",
        encoding="utf-8",
    )

    school = School(
        name="Invalid School",
        city="Austin",
        state="TX",
        school_type="university",
        size_category="medium",
    )
    session.add(school)
    await session.commit()

    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_BULK_PATH", str(scorecard_path))
    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_BULK_URL", "")
    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_API_KEY", "")
    monkeypatch.setattr(admission_data_phase1_service.settings, "IPEDS_DATASET_PATH", str(ipeds_path))
    monkeypatch.setattr(admission_data_phase1_service.settings, "IPEDS_DATASET_URL", "")

    result = await run_admission_phase1_pipeline(
        session,
        run_id=f"phase1-{uuid.uuid4().hex[:6]}",
        scope="existing_65",
        dry_run=False,
        metric_year=2025,
        output_dir=str(tmp_path / "out"),
        llm=_FailingJudge(),
    )
    await session.commit()

    quarantine = int((await session.scalar(select(func.count()).select_from(FactQuarantine))) or 0)
    metrics = int((await session.scalar(select(func.count()).select_from(SchoolMetricsYear))) or 0)

    assert result["counts"]["llm_judge_calls"] >= 1
    assert result["counts"]["llm_judge_reject"] >= 1
    assert result["counts"]["quarantine_created"] >= 1
    assert quarantine >= 1
    assert metrics == 0


@pytest.mark.asyncio
async def test_phase1_pipeline_gate_fails_when_scorecard_bulk_missing(session, monkeypatch, tmp_path):
    ipeds_path = tmp_path / "ipeds.csv"
    _write_ipeds_csv(ipeds_path)

    school = School(
        name="Test University",
        city="Boston",
        state="MA",
        school_type="university",
        size_category="large",
    )
    session.add(school)
    await session.flush()
    session.add(
        SchoolExternalId(
            school_id=school.id,
            provider="ipeds",
            external_id="1001",
            is_primary=True,
            match_method="seed",
            confidence=0.99,
            metadata_={"source": "test"},
        )
    )
    await session.commit()

    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_BULK_PATH", "")
    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_BULK_URL", "")
    monkeypatch.setattr(admission_data_phase1_service, "_SCORECARD_BULK_URL_CANDIDATES", ())
    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_API_KEY", "")
    monkeypatch.setattr(admission_data_phase1_service.settings, "IPEDS_DATASET_PATH", str(ipeds_path))
    monkeypatch.setattr(admission_data_phase1_service.settings, "IPEDS_DATASET_URL", "")

    result = await run_admission_phase1_pipeline(
        session,
        run_id=f"phase1-{uuid.uuid4().hex[:6]}",
        scope="existing_65",
        dry_run=False,
        metric_year=2025,
        output_dir=str(tmp_path / "out"),
    )
    await session.commit()

    assert result["status"] == "gate_failed"
    assert result["gate"]["passed"] is False
    assert "scorecard_bulk_rows_read_eq_0" in result["gate"]["reasons"]


@pytest.mark.asyncio
async def test_phase1_pipeline_gate_fails_when_truth_tables_change(session, monkeypatch, tmp_path):
    scorecard_path = tmp_path / "scorecard.csv"
    ipeds_path = tmp_path / "ipeds.csv"
    _write_scorecard_csv(scorecard_path)
    _write_ipeds_csv(ipeds_path)

    school = School(
        name="Test University",
        city="Boston",
        state="MA",
        school_type="university",
        size_category="large",
    )
    session.add(school)
    await session.flush()
    session.add(
        SchoolExternalId(
            school_id=school.id,
            provider="ipeds",
            external_id="1001",
            is_primary=True,
            match_method="seed",
            confidence=0.99,
            metadata_={"source": "test"},
        )
    )
    await session.commit()

    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_BULK_PATH", str(scorecard_path))
    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_BULK_URL", "")
    monkeypatch.setattr(admission_data_phase1_service.settings, "SCORECARD_API_KEY", "")
    monkeypatch.setattr(admission_data_phase1_service.settings, "IPEDS_DATASET_PATH", str(ipeds_path))
    monkeypatch.setattr(admission_data_phase1_service.settings, "IPEDS_DATASET_URL", "")

    calls = {"value": 0}

    async def _fake_truth_counts(_session):
        calls["value"] += 1
        if calls["value"] == 1:
            return {"admission_events": 7, "causal_outcome_events": 5}
        return {"admission_events": 8, "causal_outcome_events": 5}

    monkeypatch.setattr(admission_data_phase1_service, "_count_truth_tables", _fake_truth_counts)

    result = await run_admission_phase1_pipeline(
        session,
        run_id=f"phase1-{uuid.uuid4().hex[:6]}",
        scope="existing_65",
        dry_run=False,
        metric_year=2025,
        output_dir=str(tmp_path / "out"),
    )
    await session.commit()

    assert result["status"] == "gate_failed"
    assert result["gate"]["passed"] is False
    assert "admission_events_changed" in result["gate"]["reasons"]
