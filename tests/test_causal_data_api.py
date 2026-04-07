from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select

from scholarpath.db.models import AdmissionEvent, CausalDatasetVersion, EvidenceArtifact, School


async def _create_student(client) -> dict:
    resp = await client.post(
        "/api/students/",
        json={
            "name": "Causal Student",
            "gpa": 3.8,
            "gpa_scale": "4.0",
            "sat_total": 1500,
            "curriculum_type": "AP",
            "intended_majors": ["Computer Science"],
            "budget_usd": 70000,
            "target_year": 2027,
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _create_school(session) -> School:
    school = School(
        name=f"Causal U {uuid.uuid4().hex[:6]}",
        city="Boston",
        state="MA",
        school_type="university",
        size_category="medium",
        us_news_rank=20,
        acceptance_rate=0.2,
        sat_25=1300,
        sat_75=1500,
        tuition_oos=55000,
        avg_net_price=28000,
    )
    session.add(school)
    await session.flush()
    return school


@pytest.mark.asyncio
async def test_admission_evidence_and_event_routes(client, session):
    student = await _create_student(client)
    school = await _create_school(session)
    await session.commit()

    evidence_resp = await client.post(
        f"/api/students/{student['id']}/admission-evidence",
        json={
            "school_id": str(school.id),
            "cycle_year": 2026,
            "source_name": "user_upload",
            "source_type": "user_upload",
            "source_url": "https://example.test/evidence",
            "content_text": "Admitted to Causal U in 2026",
        },
    )
    assert evidence_resp.status_code == 201, evidence_resp.text
    evidence_id = evidence_resp.json()["id"]

    event_resp = await client.post(
        f"/api/students/{student['id']}/admission-events",
        json={
            "school_id": str(school.id),
            "cycle_year": 2026,
            "stage": "admit",
            "evidence_ref": evidence_id,
            "source_name": "manual",
        },
    )
    assert event_resp.status_code == 201, event_resp.text
    assert event_resp.json()["stage"] == "admit"


@pytest.mark.asyncio
async def test_get_causal_dataset_version_alias_route(client, session):
    row = CausalDatasetVersion(
        version="ds-test-v1",
        status="ready",
        config_json={"lookback_days": 540},
        stats_json={"rows_total": 10},
        truth_ratio_by_outcome={"admission_probability": 1.0},
        mini_gate_passed=True,
    )
    session.add(row)
    await session.commit()

    resp = await client.get("/api/causal/datasets/ds-test-v1")
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["version"] == "ds-test-v1"
    assert payload["mini_gate_passed"] is True


@pytest.mark.asyncio
async def test_causal_data_write_routes_removed(client, session):
    student = await _create_student(client)
    school = await _create_school(session)
    await session.commit()

    evidence_resp = await client.post(
        f"/api/causal-data/students/{student['id']}/admission-evidence",
        json={
            "school_id": str(school.id),
            "cycle_year": 2026,
            "source_name": "compat_write",
            "source_type": "user_upload",
            "source_url": "https://example.test/evidence",
            "content_text": "legacy write should be rejected",
        },
    )
    assert evidence_resp.status_code == 404, evidence_resp.text

    event_resp = await client.post(
        f"/api/causal-data/students/{student['id']}/admission-events",
        json={
            "school_id": str(school.id),
            "cycle_year": 2026,
            "stage": "admit",
            "source_name": "compat_write",
        },
    )
    assert event_resp.status_code == 404, event_resp.text

    evidence_rows = int(
        (await session.scalar(select(func.count()).select_from(EvidenceArtifact))) or 0
    )
    event_rows = int(
        (await session.scalar(select(func.count()).select_from(AdmissionEvent))) or 0
    )
    assert evidence_rows == 0
    assert event_rows == 0
