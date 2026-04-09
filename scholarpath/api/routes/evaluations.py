"""Evaluation routes -- school fit scoring and tiered lists."""

from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select

from scholarpath.api.deps import AppLLMDep, SessionDep
from scholarpath.api.models.comparison import (
    CompareReportRequest,
    CompareReportResponse,
)
from scholarpath.api.models.evaluation import (
    EvaluationResponse,
    TieredSchoolList,
)
from scholarpath.db.models.evaluation import SchoolEvaluation
from scholarpath.db.models.school import School
from scholarpath.db.models.student import Student
from scholarpath.services.comparison_service import (
    generate_comparison_report,
    generate_comparison_report_stream,
)
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


@router.post(
    "/students/{student_id}/compare-report",
    response_model=CompareReportResponse,
    status_code=status.HTTP_200_OK,
)
async def compare_report(
    student_id: uuid.UUID,
    body: CompareReportRequest,
    llm: AppLLMDep,
    session: SessionDep,
) -> CompareReportResponse:
    """Generate a multi-school comparison report with orientation scores and causal graphs."""
    await _require_student(session, student_id)
    return await generate_comparison_report(
        session=session,
        llm=llm,
        student_id=student_id,
        school_ids=body.school_ids,
        orientations=body.orientations,
    )


@router.post(
    "/students/{student_id}/compare-report/stream",
    status_code=status.HTTP_200_OK,
)
async def compare_report_stream(
    student_id: uuid.UUID,
    body: CompareReportRequest,
    llm: AppLLMDep,
    session: SessionDep,
) -> StreamingResponse:
    """Streaming comparison report — yields NDJSON lines progressively.

    Each line is a JSON object with ``event`` and ``data`` fields:
    - ``{"event": "orientation", "data": {...}}`` for each completed orientation
    - ``{"event": "recommendation", "data": {...}}`` at the end
    - ``{"event": "error", "data": {"message": "..."}}`` on failure
    """
    await _require_student(session, student_id)

    async def _stream():
        async for chunk in generate_comparison_report_stream(
            session=session,
            llm=llm,
            student_id=student_id,
            school_ids=body.school_ids,
            orientations=body.orientations,
        ):
            yield json.dumps(chunk, ensure_ascii=False, default=str) + "\n"

    return StreamingResponse(
        _stream(),
        media_type="application/x-ndjson",
    )
