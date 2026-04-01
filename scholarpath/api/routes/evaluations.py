"""Evaluation routes -- school fit scoring and tiered lists."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from scholarpath.api.deps import SessionDep
from scholarpath.api.models.evaluation import (
    EvaluationResponse,
    TieredSchoolList,
)
from scholarpath.db.models.evaluation import SchoolEvaluation
from scholarpath.db.models.school import School
from scholarpath.db.models.student import Student

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

    # Delegate to the evaluation pipeline (stub -- real implementation
    # will call the causal + LLM pipeline).
    try:
        from scholarpath.pipeline import evaluate  # type: ignore[import-untyped]

        evaluation = await evaluate(student, school, session)
    except ImportError:
        # Fallback: create a placeholder evaluation so the API contract works.
        evaluation = SchoolEvaluation(
            student_id=student_id,
            school_id=school_id,
            tier="target",
            academic_fit=0.0,
            financial_fit=0.0,
            career_fit=0.0,
            life_fit=0.0,
            overall_score=0.0,
            reasoning="Evaluation pipeline not yet implemented.",
        )
        session.add(evaluation)
        await session.flush()
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

    stmt = select(SchoolEvaluation).where(
        SchoolEvaluation.student_id == student_id,
    )
    result = await session.execute(stmt)
    evaluations = result.scalars().all()

    tiered: dict[str, list] = {
        "reach": [],
        "target": [],
        "safety": [],
        "likely": [],
    }
    for ev in evaluations:
        tier_key = ev.tier.lower() if ev.tier else "target"
        if tier_key in tiered:
            tiered[tier_key].append(ev)

    return TieredSchoolList(**tiered)
