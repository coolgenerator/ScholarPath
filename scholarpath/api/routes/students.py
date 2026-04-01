"""Student CRUD routes."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from scholarpath.api.deps import SessionDep
from scholarpath.api.models.student import (
    StudentCreate,
    StudentResponse,
    StudentUpdate,
)
from scholarpath.db.models.student import Student
from scholarpath.llm.embeddings import get_embedding_service

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
    student = Student(**data)
    session.add(student)
    await session.flush()

    # Auto-embed profile if enough data
    if student.intended_majors and student.gpa:
        try:
            emb = get_embedding_service()
            student.profile_embedding = await emb.embed_student_profile(
                payload.model_dump(exclude_unset=True)
            )
            await session.flush()
        except Exception:
            pass  # Embedding is best-effort

    await session.refresh(student)
    return student


@router.get("/{student_id}", response_model=StudentResponse)
async def get_student(student_id: uuid.UUID, session: SessionDep) -> Student:
    """Retrieve a student profile by ID."""
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )
    return student


@router.put("/{student_id}", response_model=StudentResponse)
async def update_student(
    student_id: uuid.UUID,
    payload: StudentUpdate,
    session: SessionDep,
) -> Student:
    """Update an existing student profile."""
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )

    update_data = payload.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(student, field, value)

    # Re-embed if profile-relevant fields changed
    embedding_fields = {"gpa", "sat_total", "intended_majors", "extracurriculars",
                        "awards", "preferences", "budget_usd"}
    if embedding_fields & update_data.keys():
        try:
            emb = get_embedding_service()
            profile_data = {
                "intended_majors": student.intended_majors,
                "gpa": student.gpa,
                "gpa_scale": student.gpa_scale,
                "sat_total": student.sat_total,
                "extracurriculars": student.extracurriculars,
                "awards": student.awards,
                "preferences": student.preferences,
                "budget_usd": student.budget_usd,
            }
            student.profile_embedding = await emb.embed_student_profile(profile_data)
        except Exception:
            pass

    await session.flush()
    await session.refresh(student)
    return student


@router.delete("/{student_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_student(student_id: uuid.UUID, session: SessionDep) -> None:
    """Delete a student profile and all associated data."""
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )
    await session.delete(student)
    await session.flush()
