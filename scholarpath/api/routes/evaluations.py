"""Evaluation routes -- school fit scoring and tiered lists."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from scholarpath.api.deps import AppLLMDep, SessionDep
from scholarpath.api.models.evaluation import (
    EvaluationResponse,
    TieredSchoolList,
)
from scholarpath.db.models.evaluation import SchoolEvaluation
from scholarpath.db.models.school import School
from scholarpath.db.models.student import Student
from scholarpath.services.evaluation_service import (
    evaluate_school_fit as evaluate_school_fit_service,
    get_tiered_list as get_tiered_list_service,
)

router = APIRouter(prefix="/evaluations", tags=["evaluations"])


async def _require_student(session, student_id: uuid.UUID) -> Student:
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )
    return student


@router.post(
    "/students/{student_id}/evaluate/{school_id}",
    response_model=EvaluationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def evaluate_school_fit(
    student_id: uuid.UUID,
    school_id: uuid.UUID,
    llm: AppLLMDep,
    session: SessionDep,
) -> SchoolEvaluation:
    """Evaluate how well a school fits a student's profile.

    Runs the evaluation pipeline and persists the result.
    """
    student = await _require_student(session, student_id)

    school = await session.get(School, school_id)
    if school is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"School {school_id} not found",
        )

    evaluation = await evaluate_school_fit_service(
        session,
        llm,
        student_id,
        school_id,
    )
    await session.refresh(evaluation)
    return evaluation


@router.get(
    "/students/{student_id}/evaluations",
    response_model=list[EvaluationResponse],
)
async def list_evaluations(
    student_id: uuid.UUID,
    session: SessionDep,
) -> list[SchoolEvaluation]:
    """List all evaluations for a student."""
    await _require_student(session, student_id)

    stmt = (
        select(SchoolEvaluation)
        .where(SchoolEvaluation.student_id == student_id)
        .order_by(SchoolEvaluation.overall_score.desc())
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


@router.get(
    "/students/{student_id}/tiers",
    response_model=TieredSchoolList,
)
async def get_tiered_list(
    student_id: uuid.UUID,
    session: SessionDep,
) -> TieredSchoolList:
    """Get evaluations organised by admission tier."""
    await _require_student(session, student_id)
    tiered = await get_tiered_list_service(session, student_id)
    return TieredSchoolList(
        reach=tiered.get("reach", []),
        target=tiered.get("target", []),
        safety=tiered.get("safety", []),
        likely=tiered.get("likely", []),
    )
