"""Unified feature builder for causal runtime and training."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scholarpath.services.portfolio_service import get_student_sat_equivalent


def _clip01(value: float | None) -> float:
    if value is None:
        return 0.0
    return max(0.0, min(1.0, float(value)))


def _norm_sat(value: int | None) -> float:
    if value is None:
        return 0.0
    return _clip01(value / 1600.0)


def _norm_gpa(value: float | None, scale: str | None) -> float:
    if value is None:
        return 0.0
    raw_scale = (scale or "4.0").strip()
    try:
        denom = float(raw_scale)
    except ValueError:
        denom = 4.0
    if denom <= 0:
        denom = 4.0
    return _clip01(value / denom)


def _norm_currency(value: float | int | None, cap: float = 120000.0) -> float:
    if value is None:
        return 0.0
    return _clip01(float(value) / cap)


@dataclass(slots=True)
class FeaturePayload:
    student_features: dict[str, float]
    school_features: dict[str, float]
    interaction_features: dict[str, float]
    metadata: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "student_features": self.student_features,
            "school_features": self.school_features,
            "interaction_features": self.interaction_features,
            "metadata": self.metadata,
        }


def build_feature_payload(
    *,
    student: Any,
    school: Any | None,
    context: str,
    metadata: dict[str, Any] | None = None,
) -> FeaturePayload:
    """Build canonical student/school/interaction features.

    The schema is intentionally aligned with existing `causal_feature_snapshots`
    rows so historical and new training assets are interoperable.
    """
    sat_equiv = get_student_sat_equivalent(student)
    student_budget = getattr(student, "budget_usd", None)
    need_aid = 1.0 if bool(getattr(student, "need_financial_aid", False)) else 0.0
    profile_done = 1.0 if bool(getattr(student, "profile_completed", False)) else 0.0

    student_features = {
        "student_gpa_norm": _norm_gpa(getattr(student, "gpa", None), getattr(student, "gpa_scale", None)),
        "student_sat_norm": _norm_sat(sat_equiv),
        "student_act_norm": _clip01((getattr(student, "act_composite", None) or 0) / 36.0),
        "student_budget_norm": _norm_currency(student_budget),
        "student_need_aid": need_aid,
        "student_profile_completed": profile_done,
    }

    school_features = {
        "school_acceptance_rate": _clip01(getattr(school, "acceptance_rate", None)),
        "school_selectivity": 1.0 - _clip01(getattr(school, "acceptance_rate", None)),
        "school_grad_rate": _clip01(getattr(school, "graduation_rate_4yr", None)),
        "school_net_price_norm": _norm_currency(getattr(school, "avg_net_price", None), cap=90000.0),
        "school_endowment_norm": _norm_currency(getattr(school, "endowment_per_student", None), cap=1_500_000.0),
        "school_student_faculty_norm": _clip01(
            1.0 - max(float(getattr(school, "student_faculty_ratio", None) or 0.0) - 1.0, 0.0) / 24.0
        ),
        "school_location_tier": _clip01(_location_to_tier(getattr(school, "campus_setting", None)) / 5.0),
        "school_intl_pct_norm": _clip01(float(getattr(school, "intl_student_pct", None) or 0.0)),
    }

    net_price = getattr(school, "avg_net_price", None)
    affordability_gap = 0.0
    affordability_ratio = 0.0
    if student_budget and net_price:
        gap = float(net_price) - float(student_budget)
        affordability_gap = _clip01(max(gap, 0.0) / 90000.0)
        affordability_ratio = _clip01(float(student_budget) / max(float(net_price), 1.0))

    interaction_features = {
        "affordability_gap_norm": affordability_gap,
        "affordability_ratio_norm": affordability_ratio,
        "academic_match": _clip01((student_features["student_gpa_norm"] + student_features["student_sat_norm"]) / 2.0),
        "has_offer_signal": 0.0,
    }

    merged_meta: dict[str, Any] = {"context": context}
    if metadata:
        merged_meta.update(metadata)

    return FeaturePayload(
        student_features=student_features,
        school_features=school_features,
        interaction_features=interaction_features,
        metadata=merged_meta,
    )


def _location_to_tier(value: str | None) -> int:
    text = (value or "").strip().lower()
    if text == "urban":
        return 4
    if text == "suburban":
        return 3
    if text == "rural":
        return 2
    return 3
