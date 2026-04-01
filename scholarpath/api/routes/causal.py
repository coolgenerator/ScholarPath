"""Causal DAG visualization endpoints."""

from __future__ import annotations

import uuid
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from scholarpath.api.deps import SessionDep
from scholarpath.db.models.student import Student
from scholarpath.db.models.school import School

router = APIRouter(prefix="/causal", tags=["causal"])


@router.get("/students/{student_id}/schools/{school_id}/dag")
async def get_causal_dag(
    student_id: uuid.UUID,
    school_id: uuid.UUID,
    session: SessionDep,
) -> dict[str, Any]:
    """Return a personalized causal DAG in Cytoscape.js format.

    Builds the DAG using the student profile and school data,
    runs belief propagation, and returns the full graph for
    frontend visualization.
    """
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")

    school = await session.get(School, school_id)
    if school is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="School not found")

    from scholarpath.causal import AdmissionDAGBuilder, NoisyORPropagator, graph_to_cytoscape

    # Build student profile dict
    student_profile: dict[str, Any] = {
        "gpa": student.gpa,
        "sat": student.sat_total,
        "family_income": student.budget_usd * 2 if student.budget_usd else None,  # rough proxy
    }

    # Build school data dict
    school_data: dict[str, Any] = {
        "acceptance_rate": school.acceptance_rate,
        "avg_aid": school.avg_net_price,
        "location_tier": 4 if school.campus_setting in ("urban",) else 3 if school.campus_setting == "suburban" else 2,
        "career_services_rating": min(1.0, (school.graduation_rate_4yr or 0.5) * 1.1),
    }
    if school.endowment_per_student:
        school_data["research_expenditure"] = school.endowment_per_student * 10

    builder = AdmissionDAGBuilder()
    dag = builder.build_admission_dag(student_profile, school_data)

    propagator = NoisyORPropagator()
    dag = propagator.propagate(dag)

    return graph_to_cytoscape(dag)
