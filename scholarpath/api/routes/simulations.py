"""Simulation routes -- what-if analysis and scenario comparison."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from scholarpath.api.deps import LLMDep, SessionDep
from scholarpath.api.models.simulation import (
    ScenarioCompareRequest,
    ScenarioCompareResponse,
    WhatIfRequest,
    WhatIfResponse,
)
from scholarpath.db.models.school import School
from scholarpath.db.models.student import Student
from scholarpath.exceptions import ScholarPathError
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
    session: SessionDep,
    llm: LLMDep,
) -> WhatIfResponse:
    """Run a what-if simulation for a specific student-school pair."""
    await _require_student(session, student_id)

    school = await session.get(School, school_id)
    if school is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"School {school_id} not found",
        )

    try:
        result = await run_what_if_service(
            session=session,
            llm=llm,
            student_id=student_id,
            school_id=school_id,
            interventions=payload.interventions,
        )
    except ScholarPathError as exc:
        detail = str(exc)
        status_code = (
            status.HTTP_404_NOT_FOUND
            if "not found" in detail.lower()
            else status.HTTP_400_BAD_REQUEST
        )
        raise HTTPException(status_code=status_code, detail=detail) from exc

    return WhatIfResponse.model_validate(result)


@router.post(
    "/students/{student_id}/compare-scenarios",
    response_model=ScenarioCompareResponse,
)
async def compare_scenarios(
    student_id: uuid.UUID,
    payload: ScenarioCompareRequest,
    session: SessionDep,
    llm: LLMDep,
) -> ScenarioCompareResponse:
    """Compare multiple what-if scenarios side by side.

    Each scenario is run independently; the response includes a narrative
    summary comparing the outcomes.
    """
    await _require_student(session, student_id)

    scenario_school_ids = {scenario.school_id for scenario in payload.scenarios}
    rows = (
        (
            await session.execute(
                select(School.id).where(School.id.in_(scenario_school_ids)),
            )
        )
        .scalars()
        .all()
    )
    existing_school_ids = set(rows)
    missing = scenario_school_ids - existing_school_ids
    if missing:
        missing_str = ", ".join(sorted(str(v) for v in missing))
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"School(s) not found: {missing_str}",
        )

    scenarios_payload = [
        {
            "school_id": str(scenario.school_id),
            "interventions": scenario.interventions,
            "label": scenario.label or f"Scenario {idx + 1}",
        }
        for idx, scenario in enumerate(payload.scenarios)
    ]
    try:
        result = await compare_scenarios_service(
            session=session,
            llm=llm,
            student_id=student_id,
            scenarios=scenarios_payload,
        )
    except ScholarPathError as exc:
        detail = str(exc)
        status_code = (
            status.HTTP_404_NOT_FOUND
            if "not found" in detail.lower()
            else status.HTTP_400_BAD_REQUEST
        )
        raise HTTPException(status_code=status_code, detail=detail) from exc

    return ScenarioCompareResponse(
        results=result.get("scenarios", []),
        summary=str(result.get("summary") or ""),
    )
