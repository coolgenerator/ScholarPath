"""School listing, search, and school-list generation routes."""

from __future__ import annotations

import uuid
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from scholarpath.api.deps import AppLLMDep, SessionDep
from scholarpath.api.models.evaluation import TieredSchoolList
from scholarpath.api.models.school import (
    SchoolListResponse,
    SchoolResponse,
    SchoolSearchParams,
)
from scholarpath.db.models.community_review import SchoolCommunityReport
from scholarpath.db.models.school import School
from scholarpath.db.models.student import Student

router = APIRouter(prefix="/schools", tags=["schools"])


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
    count_stmt = select(func.count()).select_from(
        select(School.id).where(*[c for c in (base.whereclause,) if c is not None]).subquery()
    ) if base.whereclause is not None else select(func.count(School.id))
    total = (await session.execute(count_stmt)).scalar_one()

    # Paginated results — build fresh to avoid stale joins
    offset = (params.page - 1) * params.per_page
    data_stmt = _apply_filters(select(School), params)
    data_stmt = (
        data_stmt
        .options(selectinload(School.programs))
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
    budget_cap_usd: int | None = None

    model_config = {"extra": "allow"}


@router.post(
    "/students/{student_id}/school-list",
    status_code=status.HTTP_200_OK,
)
async def generate_school_list_endpoint(
    student_id: uuid.UUID,
    llm: AppLLMDep,
    session: SessionDep,
    hints: Optional[SchoolListHints] = None,
) -> dict:
    """Generate a school list for a student."""
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )

    preference_hints: list[str] = []
    budget_cap_override: int | None = None
    if hints:
        if hints.preferences:
            preference_hints = [str(item).strip() for item in hints.preferences if str(item).strip()]
        if hints.budget_cap_usd and hints.budget_cap_usd > 0:
            budget_cap_override = int(hints.budget_cap_usd)

    from scholarpath.services.recommendation_service import generate_recommendations

    payload = await generate_recommendations(
        session,
        llm,
        student_id,
        budget_cap_override=budget_cap_override,
        preference_hints=preference_hints,
        persist_evaluations=True,
    )
    schools = payload.get("schools", [])
    return {
        "status": "completed",
        "count": len(schools),
        "schools": schools,
        "prefilter_meta": payload.get("prefilter_meta"),
        "scenario_pack": payload.get("scenario_pack"),
    }


@router.post(
    "/students/{student_id}/scenario-pack",
    status_code=status.HTTP_200_OK,
)
async def generate_school_scenario_pack_endpoint(
    student_id: uuid.UUID,
    llm: AppLLMDep,
    session: SessionDep,
    hints: Optional[SchoolListHints] = None,
) -> dict:
    """Generate a deterministic baseline+scenario pack without mutating evaluations."""
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )

    preference_hints: list[str] = []
    budget_cap_override: int | None = None
    if hints:
        if hints.preferences:
            preference_hints = [str(item).strip() for item in hints.preferences if str(item).strip()]
        if hints.budget_cap_usd and hints.budget_cap_usd > 0:
            budget_cap_override = int(hints.budget_cap_usd)

    from scholarpath.services.recommendation_service import generate_recommendations

    payload = await generate_recommendations(
        session,
        llm,
        student_id,
        budget_cap_override=budget_cap_override,
        preference_hints=preference_hints,
        persist_evaluations=False,
    )

    return {
        "status": "completed",
        "scenario_pack": payload.get("scenario_pack"),
        "prefilter_meta": payload.get("prefilter_meta"),
        "count": len(payload.get("schools", [])),
    }


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
    llm: AppLLMDep,
    session: SessionDep,
) -> School:
    """Search for a school by name. If not in DB, create it using LLM.

    This is the 'agent search' endpoint: the user types a school name,
    and the system either returns the existing record or creates a new
    one with AI-generated data.
    """
    query = request.name.strip()

    # Common abbreviation map
    ABBREVIATIONS: dict[str, str] = {
        "MIT": "Massachusetts Institute of Technology",
        "Caltech": "California Institute of Technology",
        "CMU": "Carnegie Mellon University",
        "UCLA": "University of California, Los Angeles",
        "UCSD": "University of California, San Diego",
        "UCSB": "University of California, Santa Barbara",
        "UPenn": "University of Pennsylvania",
        "UVA": "University of Virginia",
        "UNC": "University of North Carolina at Chapel Hill",
        "UIUC": "University of Illinois Urbana-Champaign",
        "NYU": "New York University",
        "USC": "University of Southern California",
        "GaTech": "Georgia Institute of Technology",
        "Georgia Tech": "Georgia Institute of Technology",
        "WashU": "Washington University in St. Louis",
        "RPI": "Rensselaer Polytechnic Institute",
        "WPI": "Worcester Polytechnic Institute",
        "SMU": "Southern Methodist University",
        "BU": "Boston University",
        "BC": "Boston College",
        "UMich": "University of Michigan, Ann Arbor",
        "UT Austin": "University of Texas at Austin",
        "Penn State": "Penn State University Park",
        "UCB": "University of California, Berkeley",
        "Cal": "University of California, Berkeley",
        "Berkeley": "University of California, Berkeley",
    }

    _base = select(School).options(selectinload(School.programs))

    # 1. Check abbreviation map
    expanded = ABBREVIATIONS.get(query) or ABBREVIATIONS.get(query.upper())
    if expanded:
        result = await session.execute(
            _base.where(func.lower(School.name) == expanded.lower())
        )
        school = result.scalar_one_or_none()
        if school is not None:
            return school

    # 2. Try exact match
    result = await session.execute(
        _base.where(func.lower(School.name) == query.lower())
    )
    school = result.scalar_one_or_none()
    if school is not None:
        return school

    # 3. Try Chinese name match
    result = await session.execute(
        _base.where(func.lower(School.name_cn) == query.lower()).limit(1)
    )
    school = result.scalar_one_or_none()
    if school is not None:
        return school

    # 4. Try fuzzy match (ILIKE on name and name_cn)
    result = await session.execute(
        _base.where(
            School.name.ilike(f"%{query}%") | School.name_cn.ilike(f"%{query}%")
        ).limit(1)
    )
    school = result.scalar_one_or_none()
    if school is not None:
        return school

    # 3. Not found — use LLM to generate school data
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
    except Exception:
        pass  # Best effort

    return school


@router.get("/{school_id}/community-reviews")
async def get_community_reviews(
    school_id: uuid.UUID,
    session: SessionDep,
) -> dict:
    """Return raw community review posts for a school (no LLM processing)."""
    from scholarpath.db.models.community_review import CommunityReview

    result = await session.execute(
        select(CommunityReview)
        .where(CommunityReview.school_id == school_id)
        .order_by(CommunityReview.post_score.desc())
        .limit(30)
    )
    reviews = list(result.scalars().all())
    return {
        "school_id": str(school_id),
        "count": len(reviews),
        "reviews": [
            {
                "source": r.subreddit,
                "title": r.post_title,
                "body": (r.post_body or "")[:300],
                "score": r.post_score,
                "url": r.post_url,
                "comments": (r.top_comments or [])[:3],
            }
            for r in reviews
        ],
    }


@router.get("/{school_id}/community-report")
async def get_community_report(
    school_id: uuid.UUID,
    session: SessionDep,
    llm: AppLLMDep,
) -> dict:
    """Return the community sentiment report for a school.

    If no report exists or it's stale, triggers real-time collection from
    Reddit + Xiaohongshu and LLM summarization on demand.
    """
    from scholarpath.services.community_review_service import get_or_generate_report

    school = await session.get(School, school_id)
    if school is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "School not found")

    report = await get_or_generate_report(session, llm, school)

    if report is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No community reviews found for {school.name}",
        )
    return {
        "school_id": str(report.school_id),
        "review_count": report.review_count,
        "dimensions": report.dimensions,
        "overall_score": report.overall_score,
        "overall_summary": report.overall_summary,
        "generated_at": report.generated_at.isoformat() if report.generated_at else None,
        "model_version": report.model_version,
    }


@router.get("/{school_id}/claims-graph")
async def get_claims_graph(
    school_id: uuid.UUID,
    session: SessionDep,
    llm: AppLLMDep,
) -> dict:
    """Return the claims/argument graph for a school.

    Extracts claims from community reviews, builds an argument graph
    with support/contradiction relationships, and uses belief propagation
    for opposing viewpoint analysis.  Results are cached for 7 days.
    """
    from scholarpath.services.claims_graph_service import get_or_generate_claims_graph

    school = await session.get(School, school_id)
    if school is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "School not found")

    payload = await get_or_generate_claims_graph(session, llm, school)

    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No community reviews found for {school.name}; cannot generate claims graph",
        )
    return payload
