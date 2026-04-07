"""Report routes -- Go/No-Go recommendation reports."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status

from scholarpath.api.deps import AppLLMDep, SessionDep
from scholarpath.api.models.report import GoNoGoResponse
from scholarpath.db.models.offer import Offer
from scholarpath.db.models.report import GoNoGoReport
from scholarpath.db.models.student import Student
from scholarpath.services.report_service import generate_go_no_go as generate_go_no_go_service

router = APIRouter(prefix="/reports", tags=["reports"])


@router.post(
    "/students/{student_id}/offers/{offer_id}/go-no-go",
    response_model=GoNoGoResponse,
    status_code=status.HTTP_201_CREATED,
)
async def generate_go_no_go(
    student_id: uuid.UUID,
    offer_id: uuid.UUID,
    llm: AppLLMDep,
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

    report = await generate_go_no_go_service(
        session,
        llm,
        student_id,
        offer_id,
    )
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
