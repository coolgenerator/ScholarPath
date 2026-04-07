"""Simulation routes -- what-if analysis and scenario comparison."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status

from scholarpath.api.deps import AppLLMDep, SessionDep
from scholarpath.api.models.simulation import (
    ScenarioCompareRequest,
    ScenarioCompareResponse,
    WhatIfRequest,
    WhatIfResponse,
)
from scholarpath.db.models.school import School
from scholarpath.db.models.student import Student
from scholarpath.services.simulation_service import (
    compare_scenarios as compare_scenarios_service,
    run_what_if as run_what_if_service,
)

router = APIRouter(prefix="/simulations", tags=["simulations"])


async def _require_student(session, student_id: uuid.UUID) -> Student:
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )
    return student


@router.post(
    "/students/{student_id}/schools/{school_id}/what-if",
    response_model=WhatIfResponse,
)
async def run_what_if(
    student_id: uuid.UUID,
    school_id: uuid.UUID,
    payload: WhatIfRequest,
    llm: AppLLMDep,
    session: SessionDep,
) -> WhatIfResponse:
    """Run a what-if simulation for a specific student-school pair."""
    await _require_student(session, student_id)

    school = await session.get(School, school_id)
    if school is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"School {school_id} not found",
        )

    result = await run_what_if_service(
        session,
        llm,
        student_id,
        school_id,
        payload.interventions,
    )
    return WhatIfResponse.model_validate(result)


@router.post(
    "/students/{student_id}/compare-scenarios",
    response_model=ScenarioCompareResponse,
)
async def compare_scenarios(
    student_id: uuid.UUID,
    payload: ScenarioCompareRequest,
    llm: AppLLMDep,
    session: SessionDep,
) -> ScenarioCompareResponse:
    """Compare multiple what-if scenarios side by side.

    Each scenario is run independently; the response includes a narrative
    summary comparing the outcomes.
    """
    await _require_student(session, student_id)

    scenario_payload = [
        {
            "school_id": str(item.school_id),
            "interventions": item.interventions,
            "label": item.label,
        }
        for item in payload.scenarios
    ]
    result = await compare_scenarios_service(
        session,
        llm,
        student_id,
        scenario_payload,
    )
    scenario_results = result.get("scenarios", [])
    return ScenarioCompareResponse(
        results=[WhatIfResponse.model_validate(item) for item in scenario_results],
        summary=str(result.get("summary") or ""),
    )
