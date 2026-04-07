from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from scholarpath.db.models import Offer, School, SchoolEvaluation, Student


@pytest.mark.asyncio
async def test_seed_route_uses_normalized_path(client):
    resp = await client.post("/api/seed/schools")
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert "count" in payload


@pytest.mark.asyncio
async def test_legacy_seed_path_removed(client):
    resp = await client.post("/api/api/seed/schools")
    assert resp.status_code == 404, resp.text


@pytest.mark.asyncio
async def test_demo_seed_routes_tolerate_existing_duplicates(client, engine):
    await client.post("/api/seed/schools")
    await client.post("/api/seed/demo-student")
    await client.post("/api/seed/demo-evaluations")
    await client.post("/api/seed/demo-offers")

    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        student = (
            await session.execute(
                select(Student).where(Student.email == "demo@scholarpath.dev").limit(1)
            )
        ).scalar_one()
        school = (await session.execute(select(School).limit(1))).scalar_one()

        session.add(
            SchoolEvaluation(
                student_id=student.id,
                school_id=school.id,
                tier="target",
                academic_fit=0.8,
                financial_fit=0.7,
                career_fit=0.75,
                life_fit=0.7,
                overall_score=0.74,
                admission_probability=0.31,
                ed_ea_recommendation="ea",
                reasoning="duplicate evaluation for idempotency regression test",
            )
        )
        session.add(
            Offer(
                student_id=student.id,
                school_id=school.id,
                status="admitted",
                merit_scholarship=1000,
                need_based_grant=500,
                loan_offered=0,
                work_study=0,
                total_aid=1500,
                total_cost=25000,
                net_cost=23500,
                honors_program=False,
                notes="duplicate offer for idempotency regression test",
            )
        )
        await session.commit()

    eval_resp = await client.post("/api/seed/demo-evaluations")
    offer_resp = await client.post("/api/seed/demo-offers")

    assert eval_resp.status_code == 200, eval_resp.text
    assert offer_resp.status_code == 200, offer_resp.text
