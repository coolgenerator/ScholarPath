"""Adapters from current DB models to Causal V2 contracts.

This file is intentionally integration-ready but not used by mainline
services yet.
"""

from __future__ import annotations

from scholarpath.causal_v2.contracts import CausalSchoolProfile, CausalStudentProfile


def student_to_causal_v2_profile(student) -> CausalStudentProfile:
    """Map a Student ORM object to the V2 student profile."""
    from scholarpath.services.portfolio_service import get_student_sat_equivalent

    sat_equivalent = get_student_sat_equivalent(student)
    return CausalStudentProfile(
        gpa=float(student.gpa) if student.gpa is not None else None,
        sat=int(sat_equivalent) if sat_equivalent is not None else None,
        family_income=float(student.budget_usd) if student.budget_usd is not None else None,
    )


def school_to_causal_v2_profile(
    school,
    *,
    avg_aid_override: float | None = None,
) -> CausalSchoolProfile:
    """Map a School ORM object to the V2 school profile."""
    avg_aid = avg_aid_override
    if avg_aid is None and school.avg_net_price is not None and school.tuition_oos is not None:
        avg_aid = max(0.0, float(school.tuition_oos) - float(school.avg_net_price))

    return CausalSchoolProfile(
        acceptance_rate=float(school.acceptance_rate) if school.acceptance_rate is not None else None,
        research_expenditure=float(school.endowment_per_student) if school.endowment_per_student is not None else None,
        avg_aid=avg_aid,
        location_tier=None,
        career_services_rating=None,
    )
