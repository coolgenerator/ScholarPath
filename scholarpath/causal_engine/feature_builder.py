"""Feature contract builder shared by training and online inference."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from scholarpath.db.models import Offer, School, Student


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class FeatureBundle:
    student_features: dict[str, float]
    school_features: dict[str, float]
    interaction_features: dict[str, float]

    @property
    def all_features(self) -> dict[str, float]:
        merged = dict(self.student_features)
        merged.update(self.school_features)
        merged.update(self.interaction_features)
        return merged


class FeatureBuilder:
    """Single source of truth for causal feature engineering."""

    def build(
        self,
        *,
        student: Student,
        school: School | None,
        offer: Offer | None = None,
    ) -> FeatureBundle:
        student_features = self._student_features(student)
        school_features = self._school_features(school)
        interaction_features = self._interaction_features(student, school, offer)
        return FeatureBundle(
            student_features=student_features,
            school_features=school_features,
            interaction_features=interaction_features,
        )

    def _student_features(self, student: Student) -> dict[str, float]:
        sat = _safe_float(student.sat_total, 1100.0)
        gpa = _safe_float(student.gpa, 3.0)
        budget = _safe_float(student.budget_usd, 0.0)
        act = _safe_float(student.act_composite, 0.0)

        return {
            "student_gpa_norm": max(0.0, min(gpa / 4.0, 1.0)),
            "student_sat_norm": max(0.0, min((sat - 400.0) / 1200.0, 1.0)),
            "student_act_norm": max(0.0, min(act / 36.0, 1.0)),
            "student_budget_norm": max(0.0, min(budget / 100_000.0, 1.0)),
            "student_need_aid": 1.0 if getattr(student, "need_financial_aid", False) else 0.0,
            "student_profile_completed": 1.0 if getattr(student, "profile_completed", False) else 0.0,
        }

    def _school_features(self, school: School | None) -> dict[str, float]:
        if school is None:
            return {
                "school_acceptance_rate": 0.5,
                "school_selectivity": 0.5,
                "school_grad_rate": 0.5,
                "school_net_price_norm": 0.5,
                "school_endowment_norm": 0.5,
                "school_student_faculty_norm": 0.5,
                "school_location_tier": 0.6,
                "school_intl_pct_norm": 0.2,
            }

        acceptance = _safe_float(school.acceptance_rate, 0.5)
        grad = _safe_float(school.graduation_rate_4yr, 0.5)
        net_price = _safe_float(school.avg_net_price, 50_000.0)
        endowment = _safe_float(school.endowment_per_student, 100_000.0)
        s_f_ratio = _safe_float(school.student_faculty_ratio, 15.0)
        intl_pct = _safe_float(school.intl_student_pct, 0.1)
        setting = (school.campus_setting or "").lower()
        location_tier = 4.0 if setting == "urban" else 3.0 if setting == "suburban" else 2.0

        return {
            "school_acceptance_rate": max(0.01, min(acceptance, 0.99)),
            "school_selectivity": max(0.01, min(1.0 - acceptance, 0.99)),
            "school_grad_rate": max(0.0, min(grad, 1.0)),
            "school_net_price_norm": max(0.0, min(net_price / 90_000.0, 1.0)),
            "school_endowment_norm": max(0.0, min(endowment / 1_000_000.0, 1.0)),
            "school_student_faculty_norm": max(0.0, min(1.0 - s_f_ratio / 40.0, 1.0)),
            "school_location_tier": max(0.0, min(location_tier / 5.0, 1.0)),
            "school_intl_pct_norm": max(0.0, min(intl_pct, 1.0)),
        }

    def _interaction_features(
        self,
        student: Student,
        school: School | None,
        offer: Offer | None,
    ) -> dict[str, float]:
        budget = _safe_float(student.budget_usd, 0.0)
        net_price = _safe_float(getattr(school, "avg_net_price", None), 50_000.0)
        offer_net_cost = _safe_float(getattr(offer, "net_cost", None), net_price)
        affordability_gap = offer_net_cost - budget
        affordability_ratio = offer_net_cost / max(budget, 1.0)
        academic_match = _safe_float(student.gpa, 3.0) / 4.0
        if school and school.sat_25 and school.sat_75 and student.sat_total:
            midpoint = (school.sat_25 + school.sat_75) / 2.0
            width = max(1.0, float(school.sat_75 - school.sat_25))
            academic_match = max(0.0, min(0.5 + (student.sat_total - midpoint) / width * 0.4, 1.0))

        return {
            "affordability_gap_norm": max(-1.0, min(affordability_gap / 100_000.0, 1.0)),
            "affordability_ratio_norm": max(0.0, min(affordability_ratio / 3.0, 1.0)),
            "academic_match": max(0.0, min(academic_match, 1.0)),
            "has_offer_signal": 1.0 if offer is not None else 0.0,
        }

    def build_payload(
        self,
        *,
        student: Student,
        school: School | None,
        offer: Offer | None,
        context: str,
    ) -> dict[str, Any]:
        bundle = self.build(student=student, school=school, offer=offer)
        return {
            "context": context,
            "student_features": bundle.student_features,
            "school_features": bundle.school_features,
            "interaction_features": bundle.interaction_features,
            "generated_at": datetime.now(UTC).isoformat(),
        }
