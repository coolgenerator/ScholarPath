"""Consistency tests for dashboard/advisor profile data handling."""

from __future__ import annotations

import pytest

from scholarpath.chat.handlers.guided_intake import (
    _normalize_financial_aid,
    _step_is_satisfied,
)
from scholarpath.db.models.student import Student
from scholarpath.services.student_service import check_profile_completeness


@pytest.mark.asyncio
async def test_profile_completeness_accepts_sat_or_act() -> None:
    base = {
        "name": "Student",
        "gpa": 3.8,
        "gpa_scale": "4.0",
        "curriculum_type": "AP",
        "intended_majors": ["Computer Science"],
        "budget_usd": 60000,
        "target_year": 2028,
        "need_financial_aid": False,
    }

    sat_student = Student(**base, sat_total=1460, act_composite=None)
    act_student = Student(**base, sat_total=None, act_composite=33)
    no_test_student = Student(**base, sat_total=None, act_composite=None)

    sat_result = await check_profile_completeness(sat_student)
    act_result = await check_profile_completeness(act_student)
    no_test_result = await check_profile_completeness(no_test_student)

    assert sat_result["completed"] is True
    assert act_result["completed"] is True
    assert no_test_result["completed"] is False
    assert "SAT or ACT total score" in no_test_result["missing_fields"]


def test_guided_intake_step_satisfaction_for_academics() -> None:
    assert _step_is_satisfied("academics", {"gpa": 3.7, "sat_total": 1450}) is True
    assert _step_is_satisfied("academics", {"gpa": 3.7, "act_composite": 32}) is True
    assert _step_is_satisfied("academics", {"sat_total": 1450}) is False
    assert _step_is_satisfied("academics", {"gpa": 3.7}) is False


@pytest.mark.parametrize(
    ("raw_need", "raw_type", "expected_need", "expected_type"),
    [
        (None, "need_based", True, "need_based"),
        (None, "merit", True, "merit"),
        (None, "both", True, "both"),
        (None, "no", False, "no"),
        (True, None, True, "need_based"),
        (False, None, False, "no"),
    ],
)
def test_guided_intake_financial_aid_normalization(
    raw_need: bool | None,
    raw_type: str | None,
    expected_need: bool,
    expected_type: str,
) -> None:
    need, aid_type = _normalize_financial_aid(raw_need, raw_type)
    assert need == expected_need
    assert aid_type == expected_type


@pytest.mark.asyncio
async def test_students_api_profile_completed_with_act_only(client) -> None:
    payload = {
        "name": "ACT Student",
        "gpa": 3.75,
        "gpa_scale": "4.0",
        "sat_total": None,
        "act_composite": 31,
        "curriculum_type": "AP",
        "intended_majors": ["Math"],
        "budget_usd": 55000,
        "target_year": 2028,
    }
    resp = await client.post("/api/students/", json=payload)
    assert resp.status_code == 201
    body = resp.json()
    assert body["profile_completed"] is True


@pytest.mark.asyncio
async def test_students_api_profile_completed_false_without_sat_and_act(client) -> None:
    payload = {
        "name": "No Test Student",
        "gpa": 3.75,
        "gpa_scale": "4.0",
        "sat_total": None,
        "act_composite": None,
        "curriculum_type": "AP",
        "intended_majors": ["Math"],
        "budget_usd": 55000,
        "target_year": 2028,
    }
    resp = await client.post("/api/students/", json=payload)
    assert resp.status_code == 201
    body = resp.json()
    assert body["profile_completed"] is False


@pytest.mark.asyncio
async def test_students_api_update_recomputes_profile_completed(client) -> None:
    payload = {
        "name": "Update Student",
        "gpa": 3.75,
        "gpa_scale": "4.0",
        "sat_total": None,
        "act_composite": None,
        "curriculum_type": "AP",
        "intended_majors": ["Math"],
        "budget_usd": 55000,
        "target_year": 2028,
    }
    created = await client.post("/api/students/", json=payload)
    assert created.status_code == 201
    student_id = created.json()["id"]
    assert created.json()["profile_completed"] is False

    updated = await client.patch(
        f"/api/students/{student_id}/portfolio",
        json={"academics": {"act_composite": 32}},
    )
    assert updated.status_code == 200
    assert updated.json()["completion"]["profile_completed"] is True


@pytest.mark.asyncio
async def test_portfolio_read_maps_legacy_preference_aliases(client) -> None:
    payload = {
        "name": "Legacy Pref Student",
        "gpa": 3.8,
        "gpa_scale": "4.0",
        "sat_total": 1450,
        "curriculum_type": "AP",
        "intended_majors": ["CS"],
        "budget_usd": 60000,
        "target_year": 2028,
        "preferences": {
            "location_preference": ["urban"],
            "school_size_preference": ["small"],
            "campus_culture": ["international_friendly"],
        },
    }
    created = await client.post("/api/students/", json=payload)
    assert created.status_code == 201
    student_id = created.json()["id"]

    resp = await client.get(f"/api/students/{student_id}/portfolio")
    assert resp.status_code == 200
    prefs = resp.json()["preferences"]
    assert prefs["location"] == ["urban"]
    assert prefs["size"] == ["small"]
    assert prefs["culture"] == ["international_friendly"]


@pytest.mark.asyncio
async def test_portfolio_patch_writes_canonical_preferences(client) -> None:
    payload = {
        "name": "Canonical Pref Student",
        "gpa": 3.8,
        "gpa_scale": "4.0",
        "sat_total": 1450,
        "curriculum_type": "AP",
        "intended_majors": ["CS"],
        "budget_usd": 60000,
        "target_year": 2028,
    }
    created = await client.post("/api/students/", json=payload)
    assert created.status_code == 201
    student_id = created.json()["id"]

    patched = await client.patch(
        f"/api/students/{student_id}/portfolio",
        json={"preferences": {"location": ["urban"], "size": ["small"]}},
    )
    assert patched.status_code == 200
    assert patched.json()["preferences"]["location"] == ["urban"]
    assert patched.json()["preferences"]["size"] == ["small"]

    raw = await client.get(f"/api/students/{student_id}")
    assert raw.status_code == 200
    raw_prefs = raw.json().get("preferences") or {}
    assert raw_prefs.get("location") == ["urban"]
    assert raw_prefs.get("size") == ["small"]
    assert "location_preference" not in raw_prefs
    assert "school_size_preference" not in raw_prefs


@pytest.mark.asyncio
async def test_portfolio_read_maps_preferred_region_alias(client) -> None:
    payload = {
        "name": "Preferred Region Alias Student",
        "gpa": 3.8,
        "gpa_scale": "4.0",
        "sat_total": 1450,
        "curriculum_type": "AP",
        "intended_majors": ["CS"],
        "budget_usd": 60000,
        "target_year": 2028,
        "preferences": {
            "preferred_region": "West Coast",
        },
    }
    created = await client.post("/api/students/", json=payload)
    assert created.status_code == 201
    student_id = created.json()["id"]

    resp = await client.get(f"/api/students/{student_id}/portfolio")
    assert resp.status_code == 200
    prefs = resp.json()["preferences"]
    assert prefs["location"] == ["West Coast"]


@pytest.mark.asyncio
async def test_portfolio_patch_nullable_clear_and_non_nullable_reject(client) -> None:
    payload = {
        "name": "Clear Field Student",
        "gpa": 3.8,
        "gpa_scale": "4.0",
        "sat_total": 1450,
        "curriculum_type": "AP",
        "intended_majors": ["CS"],
        "budget_usd": 60000,
        "target_year": 2028,
        "ed_preference": "ed",
    }
    created = await client.post("/api/students/", json=payload)
    assert created.status_code == 201
    student_id = created.json()["id"]

    clear_nullable = await client.patch(
        f"/api/students/{student_id}/portfolio",
        json={"strategy": {"ed_preference": None}},
    )
    assert clear_nullable.status_code == 200
    assert clear_nullable.json()["strategy"]["ed_preference"] is None

    reject_non_nullable = await client.patch(
        f"/api/students/{student_id}/portfolio",
        json={"finance": {"budget_usd": None}},
    )
    assert reject_non_nullable.status_code == 422


@pytest.mark.asyncio
async def test_portfolio_patch_rejects_unknown_fields(client) -> None:
    payload = {
        "name": "Unknown Field Student",
        "gpa": 3.8,
        "gpa_scale": "4.0",
        "sat_total": 1450,
        "curriculum_type": "AP",
        "intended_majors": ["CS"],
        "budget_usd": 60000,
        "target_year": 2028,
    }
    created = await client.post("/api/students/", json=payload)
    assert created.status_code == 201
    student_id = created.json()["id"]

    bad = await client.patch(
        f"/api/students/{student_id}/portfolio",
        json={"preferences": {"unexpected_key": "x"}},
    )
    assert bad.status_code == 422
