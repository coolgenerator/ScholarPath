"""Student profile CRUD and completeness checks."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.db.models import Student
from scholarpath.exceptions import ScholarPathError
from scholarpath.llm.embeddings import get_embedding_service

# Fields required for a complete profile, mapped to human-readable labels.
_REQUIRED_FIELDS: dict[str, str] = {
    "name": "Full name",
    "gpa": "GPA",
    "gpa_scale": "GPA scale",
    "curriculum_type": "Curriculum type",
    "intended_majors": "Intended major(s)",
    "budget_usd": "Annual budget (USD)",
    "target_year": "Target enrollment year",
}

# Additional fields that improve recommendations but are not strictly required.
_RECOMMENDED_FIELDS: dict[str, str] = {
    "extracurriculars": "Extracurricular activities",
    "awards": "Awards / honors",
    "preferences": "School preferences",
    "toefl_total": "TOEFL score",
}


async def create_student(
    session: AsyncSession,
    data: dict[str, Any],
) -> Student:
    """Create a new student record.

    Parameters
    ----------
    session:
        Active async database session.
    data:
        Dictionary of student attributes. Keys should match
        :class:`~scholarpath.db.models.Student` column names.

    Returns
    -------
    Student
        The persisted student object with its generated ``id``.
    """
    student = Student(**data)
    session.add(student)
    await session.flush()

    # Evaluate profile completeness on creation as well.
    completeness = await check_profile_completeness(student)
    student.profile_completed = completeness["completed"]

    # Generate profile embedding if enough data is present
    await _maybe_embed_profile(student)
    await session.flush()

    return student


async def get_student(
    session: AsyncSession,
    student_id: uuid.UUID,
) -> Student:
    """Fetch a student by primary key.

    Raises
    ------
    ScholarPathError
        If no student with the given ID exists.
    """
    student = await session.get(Student, student_id)
    if student is None:
        raise ScholarPathError(f"Student {student_id} not found")
    return student


async def update_student(
    session: AsyncSession,
    student_id: uuid.UUID,
    data: dict[str, Any],
) -> Student:
    """Partially update a student record.

    Parameters
    ----------
    session:
        Active async database session.
    student_id:
        UUID of the student to update.
    data:
        Dictionary of fields to update. Unknown keys are silently ignored.

    Returns
    -------
    Student
        The updated student object.
    """
    student = await get_student(session, student_id)
    for key, value in data.items():
        if hasattr(student, key):
            setattr(student, key, value)
    # Re-evaluate profile completeness after every update.
    completeness = await check_profile_completeness(student)
    student.profile_completed = completeness["completed"]

    # Re-generate profile embedding when profile data changes
    embedding_fields = {"gpa", "sat_total", "intended_majors", "extracurriculars",
                        "awards", "preferences", "budget_usd"}
    if embedding_fields & data.keys():
        await _maybe_embed_profile(student)

    await session.flush()
    return student


async def delete_student(
    session: AsyncSession,
    student_id: uuid.UUID,
) -> None:
    """Soft-delete (or hard-delete) a student record.

    Currently performs a hard delete. Cascade rules on the model
    ensure evaluations and offers are removed as well.
    """
    student = await get_student(session, student_id)
    await session.delete(student)
    await session.flush()


async def check_profile_completeness(student: Student) -> dict[str, Any]:
    """Evaluate how complete a student profile is.

    Returns
    -------
    dict
        ``completed`` -- ``True`` when all required fields are present.
        ``missing_fields`` -- list of human-readable labels for absent fields.
        ``completion_pct`` -- float in [0, 1] representing overall progress
        (includes both required and recommended fields).
    """
    # SAT/ACT is one required slot (either one satisfies the requirement).
    required_slots = len(_REQUIRED_FIELDS) + 1
    total_fields = required_slots + len(_RECOMMENDED_FIELDS)
    filled = 0
    missing: list[str] = []

    for field, label in _REQUIRED_FIELDS.items():
        value = getattr(student, field, None)
        if _is_empty(value):
            missing.append(label)
        else:
            filled += 1

    sat_filled = not _is_empty(getattr(student, "sat_total", None))
    act_filled = not _is_empty(getattr(student, "act_composite", None))
    if sat_filled or act_filled:
        filled += 1
    else:
        missing.append("SAT or ACT total score")

    for field in _RECOMMENDED_FIELDS:
        value = getattr(student, field, None)
        if not _is_empty(value):
            filled += 1

    completion_pct = filled / total_fields if total_fields else 1.0

    return {
        "completed": len(missing) == 0,
        "missing_fields": missing,
        "completion_pct": round(completion_pct, 3),
    }


def _is_empty(value: Any) -> bool:
    """Return ``True`` for values considered "not provided"."""
    if value is None:
        return True
    if isinstance(value, (str, list, dict)) and len(value) == 0:
        return True
    return False


async def _maybe_embed_profile(student: Student) -> None:
    """Generate a profile embedding if the student has enough data.

    Requires at least ``intended_majors`` and ``gpa`` to produce a
    meaningful embedding.  Failures are logged but do not propagate.
    """
    if not student.intended_majors or not student.gpa:
        return

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
        import logging
        logging.getLogger(__name__).warning(
            "Failed to embed profile for student %s", student.id, exc_info=True,
        )
