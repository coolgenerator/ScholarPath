"""School search, detail retrieval, and LLM-powered list generation."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from scholarpath.db.models import (
    Conflict,
    DataPoint,
    Program,
    School,
    SchoolEvaluation,
    Tier,
)
from scholarpath.exceptions import ScholarPathError
from scholarpath.llm.client import LLMClient
from scholarpath.llm.embeddings import get_embedding_service
from scholarpath.llm.prompts import (
    SCHOOL_EVALUATION_PROMPT,
    format_school_evaluation,
)
from scholarpath.services.student_service import get_student

logger = logging.getLogger(__name__)


async def search_schools(
    session: AsyncSession,
    filters: dict[str, Any],
) -> list[School]:
    """Search schools with optional filters and pagination.

    Supported filter keys
    ---------------------
    - ``q`` (str): free-text name search (case-insensitive)
    - ``school_type`` (str): ``university``, ``lac``, ``technical``
    - ``state`` (str): US state abbreviation
    - ``min_rank`` / ``max_rank`` (int): US News ranking bounds
    - ``max_acceptance_rate`` (float): upper bound on acceptance rate
    - ``max_net_price`` (int): upper bound on average net price
    - ``offset`` (int): pagination offset, default 0
    - ``limit`` (int): page size, default 20 (max 100)
    """
    stmt = select(School)

    # Free-text name filter
    if q := filters.get("q"):
        pattern = f"%{q}%"
        stmt = stmt.where(
            or_(
                School.name.ilike(pattern),
                School.name_cn.ilike(pattern),
            )
        )

    if school_type := filters.get("school_type"):
        stmt = stmt.where(School.school_type == school_type)

    if state := filters.get("state"):
        stmt = stmt.where(School.state == state)

    if (min_rank := filters.get("min_rank")) is not None:
        stmt = stmt.where(School.us_news_rank >= min_rank)

    if (max_rank := filters.get("max_rank")) is not None:
        stmt = stmt.where(School.us_news_rank <= max_rank)

    if (max_ar := filters.get("max_acceptance_rate")) is not None:
        stmt = stmt.where(School.acceptance_rate <= max_ar)

    if (max_np := filters.get("max_net_price")) is not None:
        stmt = stmt.where(School.avg_net_price <= max_np)

    # Pagination
    offset = max(int(filters.get("offset", 0)), 0)
    limit = min(max(int(filters.get("limit", 20)), 1), 100)
    stmt = stmt.order_by(School.us_news_rank.asc().nullslast()).offset(offset).limit(limit)

    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_school_detail(
    session: AsyncSession,
    school_id: uuid.UUID,
) -> dict[str, Any]:
    """Return a school with its programs, data points, and conflicts.

    Returns
    -------
    dict
        ``school``: the :class:`School` instance.
        ``programs``: list of :class:`Program` objects.
        ``data_points``: list of :class:`DataPoint` objects.
        ``conflicts``: list of :class:`Conflict` objects.
    """
    stmt = (
        select(School)
        .options(selectinload(School.programs), selectinload(School.data_points))
        .where(School.id == school_id)
    )
    result = await session.execute(stmt)
    school = result.scalars().first()
    if school is None:
        raise ScholarPathError(f"School {school_id} not found")

    # Fetch conflicts for this school
    conflicts_result = await session.execute(
        select(Conflict).where(Conflict.school_id == school_id)
    )
    conflicts = list(conflicts_result.scalars().all())

    return {
        "school": school,
        "programs": list(school.programs),
        "data_points": list(school.data_points),
        "conflicts": conflicts,
    }


async def generate_school_list(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
    *,
    hints: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Use the student profile and LLM to generate a matched school list with tiers.

    Parameters
    ----------
    hints : dict, optional
        Optional guidance from the frontend, e.g.
        ``{"interests": ["cs", "engineering"], "preferences": ["more_reach", "low_cost"]}``.
        These are injected into the LLM prompt to steer recommendations.

    Flow
    ----
    1. Load student profile.
    2. Build a profile summary for the LLM prompt.
    3. Ask the LLM to evaluate and score candidate schools.
    4. Tier each school based on estimated admission probability:
       - reach:  < 20%
       - target: 20-60%
       - safety: 60-85%
       - likely: > 85%

    Returns
    -------
    list[dict]
        Each dict contains ``school_name``, ``reasoning``, ``scores``,
        ``admission_probability``, and ``tier``.
    """
    student = await get_student(session, student_id)

    # If student has a profile embedding, use vector similarity to pre-filter
    # the most relevant schools before sending to LLM for detailed evaluation.
    vector_candidates: list[str] = []
    if student.profile_embedding is not None:
        try:
            vec_literal = f"[{','.join(str(v) for v in student.profile_embedding)}]"
            vector_stmt = (
                select(School.name)
                .where(School.embedding.isnot(None))
                .order_by(School.embedding.cosine_distance(vec_literal))
                .limit(20)
            )
            vr = await session.execute(vector_stmt)
            vector_candidates = [row[0] for row in vr.all()]
            logger.info(
                "Vector pre-filtered %d candidate schools for student %s",
                len(vector_candidates), student_id,
            )
        except Exception:
            logger.warning("Vector pre-filtering failed, using LLM only", exc_info=True)

    # Build profile dict for the prompt
    profile = {
        "name": student.name,
        "gpa": student.gpa,
        "gpa_scale": student.gpa_scale,
        "sat_total": student.sat_total,
        "act_composite": student.act_composite,
        "toefl_total": student.toefl_total,
        "curriculum_type": student.curriculum_type,
        "ap_courses": student.ap_courses,
        "extracurriculars": student.extracurriculars,
        "awards": student.awards,
        "intended_majors": student.intended_majors,
        "budget_usd": student.budget_usd,
        "need_financial_aid": student.need_financial_aid,
        "preferences": student.preferences,
        "target_year": student.target_year,
    }

    school_context: dict[str, Any] = {"task": "generate_school_list", "count": 15}
    if vector_candidates:
        school_context["vector_recommended_schools"] = vector_candidates
    if hints:
        if hints.get("interests"):
            school_context["student_interests"] = hints["interests"]
            profile["additional_interests"] = hints["interests"]
        if hints.get("preferences"):
            school_context["recommendation_preferences"] = hints["preferences"]

    user_prompt = format_school_evaluation(
        student_profile=profile,
        school_data=school_context,
    )
    messages = [
        {"role": "system", "content": SCHOOL_EVALUATION_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    result = await llm.complete_json(
        messages,
        schema={
            "schools": [
                {
                    "school_name": "string",
                    "reasoning": "string",
                    "academic_fit": "float 0-1",
                    "financial_fit": "float 0-1",
                    "career_fit": "float 0-1",
                    "life_fit": "float 0-1",
                    "overall_score": "float 0-1",
                    "admission_probability": "float 0-1",
                }
            ]
        },
        temperature=0.4,
    )

    schools_raw: list[dict] = result.get("schools", [])

    # Assign tiers based on admission probability
    for entry in schools_raw:
        prob = entry.get("admission_probability", 0.5)
        entry["tier"] = _assign_tier(prob)

    logger.info(
        "Generated school list for student %s: %d schools",
        student_id,
        len(schools_raw),
    )
    return schools_raw


def _assign_tier(admission_probability: float) -> str:
    """Map an admission probability to a tier label."""
    if admission_probability > 0.85:
        return Tier.LIKELY.value
    if admission_probability > 0.60:
        return Tier.SAFETY.value
    if admission_probability >= 0.20:
        return Tier.TARGET.value
    return Tier.REACH.value
