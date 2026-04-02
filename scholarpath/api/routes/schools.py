"""School listing, search, and school-list generation routes."""

from __future__ import annotations

import logging
import uuid
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from scholarpath.api.deps import SessionDep
from scholarpath.api.models.evaluation import TieredSchoolList
from scholarpath.api.models.school import (
    SchoolListResponse,
    SchoolResponse,
    SchoolSearchParams,
)
from scholarpath.db.models.school import School
from scholarpath.db.models.student import Student

router = APIRouter(prefix="/schools", tags=["schools"])
logger = logging.getLogger(__name__)


def _apply_filters(stmt, params: SchoolSearchParams):
    """Apply optional search filters to a school query."""
    if params.query:
        stmt = stmt.where(School.name.ilike(f"%{params.query}%"))
    if params.state:
        stmt = stmt.where(School.state == params.state)
    if params.min_rank is not None:
        stmt = stmt.where(School.us_news_rank >= params.min_rank)
    if params.max_rank is not None:
        stmt = stmt.where(School.us_news_rank <= params.max_rank)
    if params.max_tuition is not None:
        stmt = stmt.where(School.tuition_oos <= params.max_tuition)
    if params.school_type:
        stmt = stmt.where(School.school_type == params.school_type)
    return stmt


@router.get("/", response_model=SchoolListResponse)
async def list_schools(
    session: SessionDep,
    params: SchoolSearchParams = Depends(),
) -> dict:
    """List or search schools with optional filters."""
    base = select(School)
    base = _apply_filters(base, params)

    # Total count
    count_stmt = select(func.count()).select_from(base.subquery())
    total = (await session.execute(count_stmt)).scalar_one()

    # Paginated results
    offset = (params.page - 1) * params.per_page
    data_stmt = (
        base.options(selectinload(School.programs))
        .order_by(School.us_news_rank.asc().nulls_last(), School.name)
        .offset(offset)
        .limit(params.per_page)
    )
    result = await session.execute(data_stmt)
    schools = result.scalars().unique().all()

    return {
        "items": schools,
        "total": total,
        "page": params.page,
        "per_page": params.per_page,
    }


@router.get("/{school_id}", response_model=SchoolResponse)
async def get_school(school_id: uuid.UUID, session: SessionDep) -> School:
    """Get full school details including programs and data points."""
    stmt = (
        select(School)
        .options(selectinload(School.programs), selectinload(School.data_points))
        .where(School.id == school_id)
    )
    result = await session.execute(stmt)
    school = result.scalars().first()
    if school is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"School {school_id} not found",
        )
    return school


class SchoolListHints(BaseModel):
    """Optional hints to guide school list generation."""
    interests: List[str] = []
    preferences: List[str] = []

    model_config = {"extra": "allow"}


@router.post(
    "/students/{student_id}/school-list",
    status_code=status.HTTP_200_OK,
)
async def generate_school_list_endpoint(
    student_id: uuid.UUID,
    session: SessionDep,
    hints: Optional[SchoolListHints] = None,
) -> dict:
    """Generate a school list for a student.

    Accepts optional ``hints`` with interests and preferences to
    guide the AI recommendation.  Tries Celery first; falls back to
    synchronous execution.
    """
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )

    hints_dict: Optional[dict] = None
    if hints and (hints.interests or hints.preferences):
        hints_dict = {"interests": hints.interests, "preferences": hints.preferences}

    # Try Celery first
    try:
        from scholarpath.tasks import generate_school_list_task  # type: ignore[import-untyped]

        result = generate_school_list_task.delay(str(student_id), hints_dict)
        return {"task_id": result.id, "status": "PENDING"}
    except ImportError as exc:
        logger.info(
            "School-list Celery task unavailable, fallback to sync generation: student=%s",
            student_id,
            exc_info=exc,
        )

    # Synchronous fallback
    from scholarpath.services.school_service import generate_school_list as gen_list
    from scholarpath.llm.client import get_llm_client

    llm = get_llm_client()
    schools = await gen_list(session, llm, student_id, hints=hints_dict)
    return {"status": "completed", "count": len(schools), "schools": schools}


@router.get(
    "/students/{student_id}/school-list",
    response_model=TieredSchoolList,
)
async def get_school_list(
    student_id: uuid.UUID,
    session: SessionDep,
) -> TieredSchoolList:
    """Get the generated tiered school list for a student."""
    from scholarpath.db.models.evaluation import SchoolEvaluation

    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )

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


class SchoolLookupRequest(BaseModel):
    """Request to look up or create a school by name."""
    name: str


@router.post("/lookup", response_model=SchoolResponse)
async def lookup_school(
    request: SchoolLookupRequest,
    session: SessionDep,
) -> School:
    """Search for a school by name. If not in DB, create it using LLM.

    This is the 'agent search' endpoint: the user types a school name,
    and the system either returns the existing record or creates a new
    one with AI-generated data.
    """
    # 1. Try exact match
    result = await session.execute(
        select(School).where(
            func.lower(School.name) == request.name.strip().lower()
        )
    )
    school = result.scalar_one_or_none()
    if school is not None:
        return school

    # 2. Try fuzzy match (ILIKE)
    result = await session.execute(
        select(School).where(
            School.name.ilike(f"%{request.name.strip()}%")
        ).limit(1)
    )
    school = result.scalar_one_or_none()
    if school is not None:
        return school

    # 3. Not found — use LLM to generate school data
    from scholarpath.llm.client import get_llm_client

    llm = get_llm_client()
    prompt_messages = [
        {
            "role": "system",
            "content": (
                "You are a college data assistant. Given a school name, "
                "return a JSON object with accurate data about the school. "
                "Use your knowledge to provide the best available data. "
                "All numeric fields should be numbers, not strings."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Provide data for: {request.name}\n\n"
                "Return JSON with these fields:\n"
                "name, name_cn (Chinese name), city, state, "
                "school_type (university/lac/technical), "
                "size_category (small/medium/large), "
                "us_news_rank (int or null), acceptance_rate (0-1 float), "
                "sat_25, sat_75 (int), tuition_oos (int), avg_net_price (int), "
                "intl_student_pct (0-1 float), student_faculty_ratio (float), "
                "graduation_rate_4yr (0-1 float), "
                "campus_setting (urban/suburban/rural), "
                "website_url"
            ),
        },
    ]

    try:
        data = await llm.complete_json(prompt_messages, temperature=0.2)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Could not find or generate data for '{request.name}'",
        )

    # Validate required fields
    if not data.get("name") or not data.get("city") or not data.get("state"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Insufficient data generated for '{request.name}'",
        )

    # Ensure the school_type is valid
    valid_types = {"university", "lac", "technical"}
    if data.get("school_type") not in valid_types:
        data["school_type"] = "university"

    valid_sizes = {"small", "medium", "large"}
    if data.get("size_category") not in valid_sizes:
        data["size_category"] = "medium"

    # Create school
    school = School(
        name=data["name"],
        name_cn=data.get("name_cn"),
        city=data["city"],
        state=data["state"],
        school_type=data.get("school_type", "university"),
        size_category=data.get("size_category", "medium"),
        us_news_rank=data.get("us_news_rank"),
        acceptance_rate=data.get("acceptance_rate"),
        sat_25=data.get("sat_25"),
        sat_75=data.get("sat_75"),
        tuition_oos=data.get("tuition_oos"),
        avg_net_price=data.get("avg_net_price"),
        intl_student_pct=data.get("intl_student_pct"),
        student_faculty_ratio=data.get("student_faculty_ratio"),
        graduation_rate_4yr=data.get("graduation_rate_4yr"),
        campus_setting=data.get("campus_setting"),
        website_url=data.get("website_url"),
    )
    session.add(school)
    await session.flush()

    # Generate embedding
    try:
        from scholarpath.llm.embeddings import get_embedding_service

        emb = get_embedding_service()
        parts = [school.name]
        if school.name_cn:
            parts.append(f"({school.name_cn})")
        parts.append(f"Location: {school.city}, {school.state}")
        parts.append(f"Type: {school.school_type}")
        if school.us_news_rank:
            parts.append(f"US News Rank: #{school.us_news_rank}")
        vectors = await emb.embed_batch([". ".join(parts)], task_type="RETRIEVAL_DOCUMENT")
        if vectors:
            school.embedding = vectors[0]
    except Exception as exc:
        logger.warning(
            "Best-effort school lookup embedding failed: school=%s stage=lookup_school",
            school.name,
            exc_info=exc,
        )

    return school
