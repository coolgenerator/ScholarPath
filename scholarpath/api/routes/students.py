"""Student CRUD routes."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status

from scholarpath.api.deps import SessionDep
from scholarpath.api.models.student import (
    StudentCreate,
    StudentPortfolioPatch,
    StudentPortfolioResponse,
    StudentResponse,
)
from scholarpath.api.models.causal_data import (
    AdmissionEvidenceCreate,
    AdmissionEvidenceResponse,
    AdmissionEventCreate,
    AdmissionEventResponse,
)
from scholarpath.db.models.student import Student
from scholarpath.exceptions import ScholarPathError
from scholarpath.services.causal_data_service import (
    register_admission_event,
    register_evidence_artifact,
)
from scholarpath.services.portfolio_service import (
    apply_portfolio_patch,
    get_portfolio,
)
from scholarpath.services.student_service import (
    create_student as create_student_service,
    delete_student as delete_student_service,
    get_student as get_student_service,
)

router = APIRouter(prefix="/students", tags=["students"])


@router.post(
    "/",
    response_model=StudentResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_student(payload: StudentCreate, session: SessionDep) -> Student:
    """Create a new student profile."""
    data = payload.model_dump(exclude_unset=True)
    data.setdefault("budget_usd", 0)
    data.setdefault("need_financial_aid", False)
    student = await create_student_service(session, data)
    await session.refresh(student)
    return student


@router.get("/{student_id}", response_model=StudentResponse)
async def get_student(student_id: uuid.UUID, session: SessionDep) -> Student:
    """Retrieve a student profile by ID."""
    try:
        student = await get_student_service(session, student_id)
    except ScholarPathError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )
    return student


@router.get(
    "/{student_id}/portfolio",
    response_model=StudentPortfolioResponse,
)
async def get_student_portfolio(
    student_id: uuid.UUID,
    session: SessionDep,
) -> dict:
    """Read canonical grouped portfolio for a student."""
    try:
        return await get_portfolio(session, student_id)
    except ScholarPathError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )


@router.patch(
    "/{student_id}/portfolio",
    response_model=StudentPortfolioResponse,
)
async def patch_student_portfolio(
    student_id: uuid.UUID,
    payload: StudentPortfolioPatch,
    session: SessionDep,
) -> dict:
    """Patch portfolio groups with strict, typed contract semantics."""
    try:
        return await apply_portfolio_patch(session, student_id, payload)
    except ScholarPathError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        )


@router.delete("/{student_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_student(student_id: uuid.UUID, session: SessionDep) -> None:
    """Delete a student profile and all associated data."""
    try:
        await delete_student_service(session, student_id)
    except ScholarPathError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )


@router.post(
    "/{student_id}/admission-evidence",
    response_model=AdmissionEvidenceResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_student_admission_evidence(
    student_id: uuid.UUID,
    payload: AdmissionEvidenceCreate,
    session: SessionDep,
) -> dict:
    try:
        row = await register_evidence_artifact(
            session,
            student_id=str(student_id),
            school_id=str(payload.school_id) if payload.school_id else None,
            cycle_year=payload.cycle_year,
            source_name=payload.source_name,
            source_type=payload.source_type,
            source_url=payload.source_url,
            content_text=payload.content_text,
            metadata=payload.metadata,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        )
    await session.commit()
    return {
        "id": row.id,
        "student_id": row.student_id,
        "school_id": row.school_id,
        "cycle_year": row.cycle_year,
        "source_name": row.source_name,
        "source_type": row.source_type,
        "source_url": row.source_url,
        "source_hash": row.source_hash,
        "captured_at": row.captured_at,
        "redaction_status": row.redaction_status,
        "metadata": row.metadata_,
    }


@router.post(
    "/{student_id}/admission-events",
    response_model=AdmissionEventResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_student_admission_event(
    student_id: uuid.UUID,
    payload: AdmissionEventCreate,
    session: SessionDep,
) -> dict:
    try:
        row = await register_admission_event(
            session,
            student_id=str(student_id),
            school_id=str(payload.school_id),
            cycle_year=payload.cycle_year,
            major_bucket=payload.major_bucket,
            stage=payload.stage,
            happened_at=payload.happened_at,
            evidence_ref=str(payload.evidence_ref) if payload.evidence_ref else None,
            source_name=payload.source_name,
            metadata=payload.metadata,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        )
    await session.commit()
    return {
        "id": row.id,
        "student_id": row.student_id,
        "school_id": row.school_id,
        "cycle_year": row.cycle_year,
        "major_bucket": row.major_bucket,
        "stage": row.stage,
        "happened_at": row.happened_at,
        "evidence_ref": row.evidence_ref,
        "source_name": row.source_name,
        "metadata": row.metadata_,
    }
