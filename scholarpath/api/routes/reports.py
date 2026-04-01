"""Report routes -- Go/No-Go recommendation reports."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from scholarpath.api.deps import SessionDep
from scholarpath.api.models.report import GoNoGoResponse
from scholarpath.db.models.offer import Offer
from scholarpath.db.models.report import GoNoGoReport
from scholarpath.db.models.student import Student

router = APIRouter(prefix="/reports", tags=["reports"])


@router.post(
    "/students/{student_id}/offers/{offer_id}/go-no-go",
    response_model=GoNoGoResponse,
    status_code=status.HTTP_201_CREATED,
)
async def generate_go_no_go(
    student_id: uuid.UUID,
    offer_id: uuid.UUID,
    session: SessionDep,
) -> GoNoGoReport:
    """Generate a Go/No-Go recommendation report for a specific offer."""
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )

    offer = await session.get(Offer, offer_id)
    if offer is None or offer.student_id != student_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Offer {offer_id} not found for student {student_id}",
        )

    # Delegate to the report generation pipeline
    try:
        from scholarpath.pipeline import generate_report  # type: ignore[import-untyped]

        report = await generate_report(student, offer, session)
    except ImportError:
        # Stub report until pipeline is implemented
        report = GoNoGoReport(
            student_id=student_id,
            offer_id=offer_id,
            overall_score=0.0,
            confidence_lower=0.0,
            confidence_upper=0.0,
            academic_score=0.0,
            financial_score=0.0,
            career_score=0.0,
            life_score=0.0,
            recommendation="neutral",
            top_factors=[],
            risks=[],
            narrative="Report generation pipeline not yet implemented.",
        )
        session.add(report)
        await session.flush()
        await session.refresh(report)

    return report


@router.get("/reports/{report_id}", response_model=GoNoGoResponse)
async def get_report(report_id: uuid.UUID, session: SessionDep) -> GoNoGoReport:
    """Retrieve an existing Go/No-Go report."""
    report = await session.get(GoNoGoReport, report_id)
    if report is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report {report_id} not found",
        )
    return report
