"""Integration tests for all backend API endpoints.

Uses an in-memory SQLite database and mocked embedding service.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio


# =========================================================================
# Helper to create entities
# =========================================================================

async def _create_student(client, **overrides) -> dict:
    payload = {
        "name": "Test Student",
        "gpa": 3.7,
        "gpa_scale": "4.0",
        "sat_total": 1450,
        "curriculum_type": "AP",
        "intended_majors": ["Computer Science"],
        "budget_usd": 50000,
        "target_year": 2027,
        **overrides,
    }
    resp = await client.post("/api/students/", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _create_school(client, session, **overrides) -> dict:
    """Insert a school directly via the DB (no public create endpoint)."""
    from scholarpath.db.models.school import School

    data = {
        "name": f"Test University {uuid.uuid4().hex[:6]}",
        "city": "Boston",
        "state": "MA",
        "school_type": "university",
        "size_category": "medium",
        "us_news_rank": 20,
        "acceptance_rate": 0.15,
        "sat_25": 1400,
        "sat_75": 1550,
        "tuition_oos": 55000,
        "avg_net_price": 25000,
        **overrides,
    }
    school = School(**data)
    session.add(school)
    await session.flush()
    await session.refresh(school)
    return {
        "id": str(school.id),
        "name": school.name,
        "tuition_oos": school.tuition_oos,
    }


# =========================================================================
# 1. Student CRUD
# =========================================================================

class TestStudentAPI:
    async def test_create_student(self, client):
        data = await _create_student(client)
        assert "id" in data
        assert data["name"] == "Test Student"
        assert data["gpa"] == 3.7

    async def test_get_student(self, client):
        created = await _create_student(client)
        resp = await client.get(f"/api/students/{created['id']}")
        assert resp.status_code == 200
        assert resp.json()["id"] == created["id"]

    async def test_get_nonexistent_student(self, client):
        fake_id = str(uuid.uuid4())
        resp = await client.get(f"/api/students/{fake_id}")
        assert resp.status_code == 404

    async def test_patch_student_portfolio(self, client):
        created = await _create_student(client)
        resp = await client.patch(
            f"/api/students/{created['id']}/portfolio",
            json={"academics": {"gpa": 3.9, "sat_total": 1520}},
        )
        assert resp.status_code == 200
        assert resp.json()["academics"]["gpa"] == 3.9
        assert resp.json()["academics"]["sat_total"] == 1520

    async def test_put_student_removed(self, client):
        created = await _create_student(client)
        resp = await client.put(
            f"/api/students/{created['id']}",
            json={"gpa": 3.9},
        )
        assert resp.status_code == 405

    async def test_delete_student(self, client):
        created = await _create_student(client)
        resp = await client.delete(f"/api/students/{created['id']}")
        assert resp.status_code == 204

        # Verify deleted
        resp2 = await client.get(f"/api/students/{created['id']}")
        assert resp2.status_code == 404

    async def test_create_student_validation(self, client):
        # Missing required fields
        resp = await client.post("/api/students/", json={"name": "Bad"})
        assert resp.status_code == 422

    async def test_create_student_gpa_validation(self, client):
        resp = await client.post("/api/students/", json={
            "name": "Bad",
            "gpa": -1.0,
            "gpa_scale": "4.0",
            "curriculum_type": "AP",
            "intended_majors": ["CS"],
            "target_year": 2027,
        })
        assert resp.status_code == 422


# =========================================================================
# 2. School API
# =========================================================================

class TestSchoolAPI:
    async def test_list_schools_empty(self, client):
        resp = await client.get("/api/schools/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["items"] == []

    async def test_list_schools_with_data(self, client, session):
        await _create_school(client, session, name="MIT", us_news_rank=2)
        await _create_school(client, session, name="Stanford", us_news_rank=3)
        await session.commit()

        resp = await client.get("/api/schools/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2

    async def test_get_school(self, client, session):
        school = await _create_school(client, session)
        await session.commit()

        resp = await client.get(f"/api/schools/{school['id']}")
        assert resp.status_code == 200
        assert resp.json()["name"] == school["name"]

    async def test_get_nonexistent_school(self, client):
        resp = await client.get(f"/api/schools/{uuid.uuid4()}")
        assert resp.status_code == 404

    async def test_search_by_state(self, client, session):
        await _create_school(client, session, name="MIT", state="MA")
        await _create_school(client, session, name="Stanford", state="CA")
        await session.commit()

        resp = await client.get("/api/schools/", params={"state": "MA"})
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["state"] == "MA"

    async def test_search_by_rank(self, client, session):
        await _create_school(client, session, name="Top School", us_news_rank=5)
        await _create_school(client, session, name="Lower School", us_news_rank=50)
        await session.commit()

        resp = await client.get("/api/schools/", params={"max_rank": 10})
        data = resp.json()
        assert data["total"] == 1

    async def test_pagination(self, client, session):
        for i in range(5):
            await _create_school(client, session, name=f"School {i}", us_news_rank=i + 1)
        await session.commit()

        resp = await client.get("/api/schools/", params={"per_page": 2, "page": 1})
        data = resp.json()
        assert len(data["items"]) == 2
        assert data["total"] == 5


# =========================================================================
# 3. Evaluation API
# =========================================================================

class TestEvaluationAPI:
    async def test_evaluate_school_fit(self, client, session):
        student = await _create_student(client)
        school = await _create_school(client, session)
        await session.commit()

        resp = await client.post(
            f"/api/evaluations/students/{student['id']}/evaluate/{school['id']}"
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["student_id"] == student["id"]
        assert data["school_id"] == school["id"]
        assert "tier" in data

    async def test_list_evaluations(self, client, session):
        student = await _create_student(client)
        school = await _create_school(client, session)
        await session.commit()

        await client.post(
            f"/api/evaluations/students/{student['id']}/evaluate/{school['id']}"
        )

        resp = await client.get(
            f"/api/evaluations/students/{student['id']}/evaluations"
        )
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    async def test_get_tiered_list(self, client, session):
        student = await _create_student(client)
        school = await _create_school(client, session)
        await session.commit()

        await client.post(
            f"/api/evaluations/students/{student['id']}/evaluate/{school['id']}"
        )

        resp = await client.get(
            f"/api/evaluations/students/{student['id']}/tiers"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "reach" in data
        assert "target" in data
        assert "safety" in data
        assert "likely" in data

    async def test_evaluate_nonexistent_student(self, client, session):
        school = await _create_school(client, session)
        await session.commit()

        resp = await client.post(
            f"/api/evaluations/students/{uuid.uuid4()}/evaluate/{school['id']}"
        )
        assert resp.status_code == 404

    async def test_evaluate_nonexistent_school(self, client):
        student = await _create_student(client)

        resp = await client.post(
            f"/api/evaluations/students/{student['id']}/evaluate/{uuid.uuid4()}"
        )
        assert resp.status_code == 404


# =========================================================================
# 4. Offer API
# =========================================================================

class TestOfferAPI:
    async def test_create_offer(self, client, session):
        student = await _create_student(client)
        school = await _create_school(client, session)
        await session.commit()

        resp = await client.post(
            f"/api/offers/students/{student['id']}/offers",
            json={
                "school_id": school["id"],
                "status": "admitted",
                "merit_scholarship": 15000,
                "need_based_grant": 10000,
                "loan_offered": 5000,
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["total_aid"] == 30000
        assert data["status"] == "admitted"

    async def test_list_offers(self, client, session):
        student = await _create_student(client)
        school = await _create_school(client, session)
        await session.commit()

        await client.post(
            f"/api/offers/students/{student['id']}/offers",
            json={"school_id": school["id"], "status": "admitted"},
        )

        resp = await client.get(f"/api/offers/students/{student['id']}/offers")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    async def test_update_offer(self, client, session):
        student = await _create_student(client)
        school = await _create_school(client, session)
        await session.commit()

        create_resp = await client.post(
            f"/api/offers/students/{student['id']}/offers",
            json={"school_id": school["id"], "status": "waitlisted"},
        )
        offer_id = create_resp.json()["id"]

        resp = await client.put(
            f"/api/offers/offers/{offer_id}",
            json={"status": "admitted", "merit_scholarship": 20000},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "admitted"
        assert resp.json()["merit_scholarship"] == 20000

    async def test_compare_offers_no_admits(self, client):
        student = await _create_student(client)

        resp = await client.get(
            f"/api/offers/students/{student['id']}/offers/compare"
        )
        assert resp.status_code == 404

    async def test_compare_offers(self, client, session):
        student = await _create_student(client)
        school1 = await _create_school(client, session, name="Univ A", tuition_oos=50000)
        school2 = await _create_school(client, session, name="Univ B", tuition_oos=40000)
        await session.commit()

        await client.post(
            f"/api/offers/students/{student['id']}/offers",
            json={
                "school_id": school1["id"],
                "status": "admitted",
                "merit_scholarship": 15000,
            },
        )
        await client.post(
            f"/api/offers/students/{student['id']}/offers",
            json={
                "school_id": school2["id"],
                "status": "admitted",
                "merit_scholarship": 25000,
            },
        )

        resp = await client.get(
            f"/api/offers/students/{student['id']}/offers/compare"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["offers"]) == 2
        assert len(data["comparison_scores"]) == 2

    async def test_create_offer_nonexistent_student(self, client, session):
        school = await _create_school(client, session)
        await session.commit()

        resp = await client.post(
            f"/api/offers/students/{uuid.uuid4()}/offers",
            json={"school_id": school["id"], "status": "admitted"},
        )
        assert resp.status_code == 404


# =========================================================================
# 5. Simulation API
# =========================================================================

class TestSimulationAPI:
    async def test_what_if(self, client, session):
        student = await _create_student(client)
        school = await _create_school(client, session)
        await session.commit()

        resp = await client.post(
            f"/api/simulations/students/{student['id']}/schools/{school['id']}/what-if",
            json={"interventions": {"scholarship": 15000}},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "original_scores" in data
        assert "modified_scores" in data
        assert "deltas" in data

    async def test_what_if_nonexistent_student(self, client, session):
        school = await _create_school(client, session)
        await session.commit()

        resp = await client.post(
            f"/api/simulations/students/{uuid.uuid4()}/schools/{school['id']}/what-if",
            json={"interventions": {"scholarship": 15000}},
        )
        assert resp.status_code == 404

    async def test_what_if_nonexistent_school(self, client):
        student = await _create_student(client)

        resp = await client.post(
            f"/api/simulations/students/{student['id']}/schools/{uuid.uuid4()}/what-if",
            json={"interventions": {"scholarship": 15000}},
        )
        assert resp.status_code == 404


# =========================================================================
# 6. Report API
# =========================================================================

class TestReportAPI:
    async def test_generate_go_no_go(self, client, session):
        student = await _create_student(client)
        school = await _create_school(client, session)
        await session.commit()

        # Create an offer first
        offer_resp = await client.post(
            f"/api/offers/students/{student['id']}/offers",
            json={"school_id": school["id"], "status": "admitted"},
        )
        offer_id = offer_resp.json()["id"]

        resp = await client.post(
            f"/api/reports/students/{student['id']}/offers/{offer_id}/go-no-go"
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "overall_score" in data
        assert "recommendation" in data

    async def test_get_report(self, client, session):
        student = await _create_student(client)
        school = await _create_school(client, session)
        await session.commit()

        offer_resp = await client.post(
            f"/api/offers/students/{student['id']}/offers",
            json={"school_id": school["id"], "status": "admitted"},
        )
        offer_id = offer_resp.json()["id"]

        create_resp = await client.post(
            f"/api/reports/students/{student['id']}/offers/{offer_id}/go-no-go"
        )
        report_id = create_resp.json()["id"]

        resp = await client.get(f"/api/reports/reports/{report_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == report_id

    async def test_get_nonexistent_report(self, client):
        resp = await client.get(f"/api/reports/reports/{uuid.uuid4()}")
        assert resp.status_code == 404

    async def test_go_no_go_nonexistent_student(self, client):
        resp = await client.post(
            f"/api/reports/students/{uuid.uuid4()}/offers/{uuid.uuid4()}/go-no-go"
        )
        assert resp.status_code == 404

    async def test_go_no_go_mismatched_offer(self, client, session):
        student1 = await _create_student(client, name="Student 1")
        student2 = await _create_student(client, name="Student 2")
        school = await _create_school(client, session)
        await session.commit()

        # Create offer for student2
        offer_resp = await client.post(
            f"/api/offers/students/{student2['id']}/offers",
            json={"school_id": school["id"], "status": "admitted"},
        )
        offer_id = offer_resp.json()["id"]

        # Try to generate report for student1 with student2's offer
        resp = await client.post(
            f"/api/reports/students/{student1['id']}/offers/{offer_id}/go-no-go"
        )
        assert resp.status_code == 404


# =========================================================================
# 7. Usage API
# =========================================================================

class TestUsageAPI:
    async def test_usage_summary_days_filter(self, client, session):
        from scholarpath.db.models.token_usage import TokenUsage

        now = datetime.now(timezone.utc)
        session.add_all(
            [
                TokenUsage(
                    created_at=now - timedelta(hours=2),
                    model="gpt-5.4-mini",
                    provider="zai",
                    caller="search.web_extract",
                    method="complete_json",
                    prompt_tokens=100,
                    completion_tokens=40,
                    total_tokens=140,
                    error=None,
                    latency_ms=1200,
                ),
                TokenUsage(
                    created_at=now - timedelta(days=10),
                    model="gpt-5.4-mini",
                    provider="zai",
                    caller="search.internal_web_search",
                    method="complete_json",
                    prompt_tokens=200,
                    completion_tokens=80,
                    total_tokens=280,
                    error="timeout",
                    latency_ms=2100,
                ),
            ]
        )
        await session.commit()

        all_resp = await client.get("/api/usage/summary")
        assert all_resp.status_code == 200, all_resp.text
        all_payload = all_resp.json()
        assert all_payload["total_calls"] == 2
        assert all_payload["total_tokens"] == 420
        assert all_payload["error_count"] == 1

        day_resp = await client.get("/api/usage/summary", params={"days": 1})
        assert day_resp.status_code == 200, day_resp.text
        day_payload = day_resp.json()
        assert day_payload["total_calls"] == 1
        assert day_payload["total_tokens"] == 140
        assert day_payload["error_count"] == 0
        assert day_payload["by_caller"]["search.web_extract"]["calls"] == 1
        assert "search.internal_web_search" not in day_payload["by_caller"]

        month_resp = await client.get("/api/usage/summary", params={"days": 30})
        assert month_resp.status_code == 200, month_resp.text
        month_payload = month_resp.json()
        assert month_payload["total_calls"] == 2
        assert month_payload["total_tokens"] == 420
        assert month_payload["error_count"] == 1

    async def test_usage_summary_rejects_invalid_days(self, client):
        resp = await client.get("/api/usage/summary", params={"days": 0})
        assert resp.status_code == 422

    async def test_usage_llm_endpoint_health(self, client, monkeypatch):
        class _FakeLLM:
            async def endpoint_health(self, *, window_seconds: int = 60) -> dict:
                return {
                    "window_seconds": window_seconds,
                    "active_mode": "beecode",
                    "active_policy": "default",
                    "observer_enabled": True,
                    "observer_error": None,
                    "endpoints": [
                        {
                            "index": 0,
                            "endpoint_id": "bee-1",
                            "key_id": "abc123",
                            "requests_total": 42,
                            "errors_total": 2,
                            "rate_limit_total": 1,
                            "timeout_total": 0,
                            "same_task_retry_triggered": 1,
                            "same_task_retry_success": 1,
                            "same_task_retry_failed": 0,
                            "preferred_route_hits": 8,
                            "policy_applied_counts_by_method": {"complete_json": 12},
                            "required_output_missing": 0,
                            "requests_window": 10.0,
                            "errors_window": 1.0,
                            "rate_limit_window": 1.0,
                            "timeout_window": 0.0,
                            "latency_ms_avg": 820.5,
                            "cooldown_active": False,
                        },
                    ],
                }

        monkeypatch.setattr(
            "scholarpath.llm.client.get_llm_client",
            lambda: _FakeLLM(),
        )

        resp = await client.get("/api/usage/llm-endpoints", params={"window_seconds": 120})
        assert resp.status_code == 200, resp.text
        payload = resp.json()
        assert payload["window_seconds"] == 120
        assert payload["active_mode"] == "beecode"
        assert payload["active_policy"] == "default"
        assert payload["observer_enabled"] is True
        assert payload["endpoints"][0]["endpoint_id"] == "bee-1"
        assert payload["endpoints"][0]["preferred_route_hits"] == 8
        assert payload["endpoints"][0]["requests_total"] == 42


# =========================================================================
# 8. Health Check / App Startup
# =========================================================================

class TestAppStartup:
    async def test_openapi_schema_available(self, client):
        resp = await client.get("/openapi.json")
        assert resp.status_code == 200
        schema = resp.json()
        assert schema["info"]["title"] == "ScholarPath"

    async def test_docs_redirect(self, client):
        resp = await client.get("/docs")
        assert resp.status_code == 200
