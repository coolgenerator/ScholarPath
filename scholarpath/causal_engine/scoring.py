"""Shared raw scoring utilities for PyWhy runtime and training calibration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_float(mapping: dict[str, Any], *keys: str, default: float | None = None) -> float | None:
    for key in keys:
        if key in mapping:
            raw = _as_float(mapping.get(key))
            if raw is not None:
                return raw
    return default


@dataclass(slots=True)
class CausalFeatureView:
    student_features: dict[str, Any]
    school_features: dict[str, Any]
    interaction_features: dict[str, Any]

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "CausalFeatureView":
        payload = payload if isinstance(payload, dict) else {}
        return cls(
            student_features=dict(payload.get("student_features") or {}),
            school_features=dict(payload.get("school_features") or {}),
            interaction_features=dict(payload.get("interaction_features") or {}),
        )


def compute_pywhy_raw_scores(view: CausalFeatureView) -> dict[str, float]:
    student = view.student_features
    school = view.school_features
    interaction = view.interaction_features

    gpa_norm = _clip01(
        _pick_float(student, "student_gpa_norm", "gpa_norm", default=0.0) or 0.0
    )
    sat_norm = _clip01(
        _pick_float(student, "student_sat_norm", "sat_norm", default=0.0) or 0.0
    )
    academic_match = _pick_float(interaction, "academic_match")
    if academic_match is None:
        academic_match = (gpa_norm + sat_norm) / 2.0
    academic_match = _clip01(academic_match)

    affordability_ratio = _pick_float(interaction, "affordability_ratio_norm")
    affordability_gap = _pick_float(interaction, "affordability_gap_norm")
    if affordability_ratio is None:
        if affordability_gap is not None:
            affordability_ratio = 1.0 - _clip01(affordability_gap)
        else:
            affordability_ratio = 0.5
    affordability_ratio = _clip01(affordability_ratio)
    affordability_gap = _clip01(affordability_gap if affordability_gap is not None else (1.0 - affordability_ratio))

    need = _clip01(
        _pick_float(
            student,
            "student_need_aid",
            "need_financial_aid",
            "need_aid",
            default=0.0,
        )
        or 0.0
    )

    acceptance = _pick_float(school, "school_acceptance_rate", "acceptance_rate")
    selectivity = _pick_float(school, "school_selectivity")
    if selectivity is None:
        selectivity = 1.0 - _clip01(acceptance if acceptance is not None else 0.5)
    selectivity = _clip01(selectivity)

    grad_rate = _clip01(
        _pick_float(school, "school_grad_rate", "graduation_rate_4yr", default=0.5) or 0.5
    )
    endowment = _clip01(
        _pick_float(school, "school_endowment_norm", "endowment_norm", default=0.4) or 0.4
    )
    location = _clip01(
        _pick_float(school, "school_location_tier", "location_tier", default=0.6) or 0.6
    )

    support = _clip01(1.0 - need * (1.0 - affordability_ratio))

    admission = _clip01(
        0.55 * academic_match + 0.25 * (1.0 - selectivity) + 0.20 * support
    )
    academic = _clip01(0.60 * academic_match + 0.25 * grad_rate + 0.15 * support)
    career = _clip01(0.40 * academic + 0.35 * endowment + 0.25 * location)
    life = _clip01(0.45 * support + 0.30 * location + 0.25 * (1.0 - affordability_gap))
    phd = _clip01(0.5 * academic + 0.3 * career + 0.2 * endowment)

    return {
        "admission_probability": admission,
        "academic_outcome": academic,
        "career_outcome": career,
        "life_satisfaction": life,
        "phd_probability": phd,
    }

