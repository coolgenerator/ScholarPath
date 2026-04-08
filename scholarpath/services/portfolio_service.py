"""Unified student portfolio contract and patch semantics."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.db.models import Student
from scholarpath.services.student_service import (
    check_profile_completeness,
    get_student,
    update_student,
)

PREFERENCE_CANONICAL_KEYS = {
    "interests",
    "risk_preference",
    "cost_priority",
    "location",
    "size",
    "culture",
    "career_goal",
    "research_vs_teaching",
    "target_schools",
    "financial_aid_type",
    "ui_preference_tags",
}

_PREFERENCE_ALIAS_KEYS: dict[str, str] = {
    "location_preference": "location",
    "preferred_region": "location",
    "school_size_preference": "size",
    "campus_culture": "culture",
}

_PREFERENCE_LIST_KEYS = {
    "interests",
    "location",
    "size",
    "culture",
    "target_schools",
    "ui_preference_tags",
}

_NON_NULLABLE_GROUP_FIELDS = {
    ("identity", "name"),
    ("identity", "target_year"),
    ("academics", "gpa"),
    ("academics", "gpa_scale"),
    ("academics", "curriculum_type"),
    ("finance", "budget_usd"),
    ("finance", "need_financial_aid"),
}

_FINANCIAL_AID_TYPES = {"need_based", "merit", "both", "no"}
_ED_PREFERENCE_CANONICAL = {"ed", "ea", "rea", "scea", "rd", "rolling"}


def _to_string_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        return [v] if v else None
    if isinstance(value, list):
        out = [str(item).strip() for item in value if str(item).strip()]
        return out or None
    return None


def _normalize_preference_value(key: str, value: Any) -> Any:
    if value is None:
        return None

    if key in _PREFERENCE_LIST_KEYS:
        return _to_string_list(value)

    if key == "financial_aid_type":
        if not isinstance(value, str):
            return None
        normalized = value.strip().lower()
        return normalized if normalized in _FINANCIAL_AID_TYPES else None

    if key in {"risk_preference", "cost_priority", "career_goal", "research_vs_teaching"}:
        if not isinstance(value, str):
            return None
        stripped = value.strip()
        return stripped or None

    return value


def _normalize_ed_preference(value: Any) -> str | None:
    """Normalize ED preference to a compact canonical value that fits schema."""
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    text = value.strip().lower()
    if not text:
        return None

    if text in _ED_PREFERENCE_CANONICAL:
        return text

    if "single choice early action" in text:
        return "scea"
    if "restrictive early action" in text:
        return "rea"
    if "early decision" in text:
        return "ed"
    if "early action" in text:
        return "ea"
    if "regular decision" in text:
        return "rd"
    if "rolling" in text:
        return "rolling"

    return None


def canonicalize_preferences(preferences: dict[str, Any] | None) -> dict[str, Any]:
    """Return canonical preference keys with normalized values.

    Legacy keys are mapped to canonical equivalents on read:
    - ``location_preference`` -> ``location``
    - ``school_size_preference`` -> ``size``
    - ``campus_culture`` -> ``culture``
    """
    raw = dict(preferences or {})
    canonical: dict[str, Any] = {}

    for key in PREFERENCE_CANONICAL_KEYS:
        value = raw.get(key)
        if value is None:
            for legacy_key, mapped_key in _PREFERENCE_ALIAS_KEYS.items():
                if mapped_key == key and legacy_key in raw:
                    value = raw.get(legacy_key)
                    break
        normalized = _normalize_preference_value(key, value)
        if normalized is not None:
            canonical[key] = normalized

    return canonical


def normalize_financial_aid(
    need_financial_aid: bool | None,
    financial_aid_type: str | None,
) -> tuple[bool | None, str | None]:
    """Normalize bool + aid type so the two fields stay semantically consistent."""
    normalized_type: str | None = None
    if isinstance(financial_aid_type, str):
        low = financial_aid_type.strip().lower()
        if low in _FINANCIAL_AID_TYPES:
            normalized_type = low

    normalized_need: bool | None = need_financial_aid
    if normalized_type in {"need_based", "merit", "both"}:
        normalized_need = True
    elif normalized_type == "no":
        normalized_need = False
    elif normalized_need is not None:
        normalized_type = "need_based" if normalized_need else "no"

    return normalized_need, normalized_type


def get_student_canonical_preferences(student: Student) -> dict[str, Any]:
    """Read canonicalized preferences for downstream consumers."""
    canonical = canonicalize_preferences(student.preferences)
    normalized_need, normalized_type = normalize_financial_aid(
        student.need_financial_aid,
        canonical.get("financial_aid_type"),
    )
    if normalized_type is not None:
        canonical["financial_aid_type"] = normalized_type
    else:
        canonical.pop("financial_aid_type", None)
    if normalized_need is not None and student.need_financial_aid != normalized_need:
        # This function is read-only; callers writing back should sync through apply_portfolio_patch.
        pass
    return canonical


def get_student_sat_equivalent(student: Student) -> int:
    """Return SAT or ACT-equivalent score for DAG inputs."""
    if student.sat_total is not None:
        return int(student.sat_total)
    if student.act_composite is not None:
        scaled = int(round((float(student.act_composite) / 36.0) * 1600.0))
        return max(400, min(1600, scaled))
    return 1100


def _build_portfolio_dict(
    student: Student,
    completion: dict[str, Any],
) -> dict[str, Any]:
    canonical_preferences = get_student_canonical_preferences(student)
    return {
        "student_id": student.id,
        "identity": {
            "name": student.name,
            "target_year": student.target_year,
        },
        "academics": {
            "gpa": student.gpa,
            "gpa_scale": student.gpa_scale,
            "sat_total": student.sat_total,
            "act_composite": student.act_composite,
            "toefl_total": student.toefl_total,
            "curriculum_type": student.curriculum_type,
            "ap_courses": student.ap_courses,
            "intended_majors": student.intended_majors,
        },
        "activities": {
            "extracurriculars": student.extracurriculars,
            "awards": student.awards,
        },
        "finance": {
            "budget_usd": student.budget_usd,
            "need_financial_aid": student.need_financial_aid,
        },
        "strategy": {
            "ed_preference": student.ed_preference,
        },
        "preferences": canonical_preferences,
        "completion": {
            "profile_completed": completion["completed"],
            "completion_pct": completion["completion_pct"],
            "missing_fields": completion["missing_fields"],
        },
    }


async def get_portfolio(
    session: AsyncSession,
    student_id: uuid.UUID,
) -> dict[str, Any]:
    student = await get_student(session, student_id)
    completion = await check_profile_completeness(student)
    return _build_portfolio_dict(student, completion)


def _assert_clearable(group: str, field: str, value: Any) -> None:
    if value is None and (group, field) in _NON_NULLABLE_GROUP_FIELDS:
        raise ValueError(f"{group}.{field} cannot be null")


def _merge_preferences(
    current: dict[str, Any],
    patch: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(current)
    for key, value in patch.items():
        if key not in PREFERENCE_CANONICAL_KEYS:
            continue
        normalized = _normalize_preference_value(key, value)
        if normalized is None:
            merged.pop(key, None)
        else:
            merged[key] = normalized
    return merged


async def apply_portfolio_patch(
    session: AsyncSession,
    student_id: uuid.UUID,
    payload: Any,
) -> dict[str, Any]:
    """Apply grouped portfolio patch, then return canonical portfolio response."""
    if hasattr(payload, "model_dump"):
        data = payload.model_dump(exclude_unset=True)
    else:
        data = dict(payload or {})
    student = await get_student(session, student_id)

    update_data: dict[str, Any] = {}

    for group_name, fields in (
        ("identity", data.get("identity") or {}),
        ("academics", data.get("academics") or {}),
        ("activities", data.get("activities") or {}),
        ("finance", data.get("finance") or {}),
        ("strategy", data.get("strategy") or {}),
    ):
        for key, value in fields.items():
            _assert_clearable(group_name, key, value)
            if group_name == "strategy" and key == "ed_preference":
                value = _normalize_ed_preference(value)
            update_data[key] = value

    prefs_patch = data.get("preferences")
    merged_preferences = canonicalize_preferences(student.preferences)
    if prefs_patch is not None:
        merged_preferences = _merge_preferences(merged_preferences, prefs_patch)

    if prefs_patch is not None:
        update_data["preferences"] = merged_preferences or None

    need_input = (
        update_data["need_financial_aid"]
        if "need_financial_aid" in update_data
        else student.need_financial_aid
    )
    aid_type_input = merged_preferences.get("financial_aid_type")
    normalized_need, normalized_type = normalize_financial_aid(
        need_input,
        aid_type_input,
    )

    if normalized_need is not None and (
        "need_financial_aid" in update_data or prefs_patch is not None
    ):
        update_data["need_financial_aid"] = normalized_need

    if prefs_patch is not None or "need_financial_aid" in update_data:
        if normalized_type is not None:
            merged_preferences["financial_aid_type"] = normalized_type
        else:
            merged_preferences.pop("financial_aid_type", None)
        update_data["preferences"] = merged_preferences or None

    if update_data:
        student = await update_student(session, student_id, update_data)

    completion = await check_profile_completeness(student)
    return _build_portfolio_dict(student, completion)
