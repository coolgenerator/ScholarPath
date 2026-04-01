"""Simulation routes -- what-if analysis and scenario comparison."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status

from scholarpath.api.deps import SessionDep
from scholarpath.api.models.simulation import (
    ScenarioCompareRequest,
    ScenarioCompareResponse,
    WhatIfRequest,
    WhatIfResponse,
)
from scholarpath.db.models.evaluation import SchoolEvaluation
from scholarpath.db.models.school import School
from scholarpath.db.models.student import Student

router = APIRouter(prefix="/simulations", tags=["simulations"])


async def _require_student(session, student_id: uuid.UUID) -> Student:
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )
    return student


async def _run_what_if(
    student: Student,
    school: School,
    interventions: dict[str, float | str],
    session,
) -> WhatIfResponse:
    """Execute a single what-if simulation.

    Delegates to the causal simulation engine when available;
    returns a stub response otherwise.
    """
    try:
        from scholarpath.causal import simulate_intervention  # type: ignore[import-untyped]

        return await simulate_intervention(student, school, interventions, session)
    except ImportError:
        # Stub: return zeroed response until causal engine is wired up
        original = {
            "academic_fit": 0.0,
            "financial_fit": 0.0,
            "career_fit": 0.0,
            "life_fit": 0.0,
            "overall_score": 0.0,
        }
        return WhatIfResponse(
            original_scores=original,
            modified_scores=original,
            deltas={k: 0.0 for k in original},
            explanation="Simulation engine not yet implemented.",
        )


@router.post(
    "/students/{student_id}/schools/{school_id}/what-if",
    response_model=WhatIfResponse,
)
async def run_what_if(
    student_id: uuid.UUID,
    school_id: uuid.UUID,
    payload: WhatIfRequest,
    session: SessionDep,
) -> WhatIfResponse:
    """Run a what-if simulation for a specific student-school pair."""
    student = await _require_student(session, student_id)

    school = await session.get(School, school_id)
    if school is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"School {school_id} not found",
        )

    return await _run_what_if(student, school, payload.interventions, session)


@router.post(
    "/students/{student_id}/compare-scenarios",
    response_model=ScenarioCompareResponse,
)
async def compare_scenarios(
    student_id: uuid.UUID,
    payload: ScenarioCompareRequest,
    session: SessionDep,
) -> ScenarioCompareResponse:
    """Compare multiple what-if scenarios side by side.

    Each scenario is run independently; the response includes a narrative
    summary comparing the outcomes.
    """
    student = await _require_student(session, student_id)

    # For scenario comparison we need a reference school.  Use the first
    # evaluation's school if the client doesn't specify one.
    from sqlalchemy import select

    stmt = (
        select(SchoolEvaluation)
        .where(SchoolEvaluation.student_id == student_id)
        .limit(1)
    )
    result = await session.execute(stmt)
    eval_row = result.scalars().first()

    if eval_row is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No evaluations exist for this student. Evaluate a school first.",
        )

    school = await session.get(School, eval_row.school_id)
    if school is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Reference school not found",
        )

    results: list[WhatIfResponse] = []
    for scenario in payload.scenarios:
        resp = await _run_what_if(student, school, scenario.interventions, session)
        results.append(resp)

    summary = (
        f"Compared {len(results)} scenarios. "
        "See individual results for score deltas."
    )

    return ScenarioCompareResponse(results=results, summary=summary)
