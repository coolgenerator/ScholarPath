"""Vector search routes -- semantic similarity via pgvector + Gemini embeddings."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select, text

from scholarpath.api.deps import EmbeddingDep, SessionDep
from scholarpath.api.models.school import SchoolResponse
from scholarpath.db.models.school import School
from scholarpath.db.models.student import Student

router = APIRouter(prefix="/vectors", tags=["vectors"])


class SimilarSchoolsRequest(BaseModel):
    """Find schools semantically similar to a query or student profile."""

    query: str | None = Field(None, description="Free-text search query")
    student_id: uuid.UUID | None = Field(None, description="Match against student profile embedding")
    limit: int = Field(10, ge=1, le=50)


class SchoolBrief(BaseModel):
    """Lightweight school schema without lazy-loaded relationships."""
    model_config = {"from_attributes": True}

    id: uuid.UUID
    name: str
    name_cn: str | None = None
    city: str
    state: str
    school_type: str
    us_news_rank: int | None = None
    acceptance_rate: float | None = None
    tuition_oos: int | None = None
    avg_net_price: int | None = None

class SimilarSchoolResult(BaseModel):
    school: SchoolBrief
    similarity: float


class EmbedTextRequest(BaseModel):
    """Generate an embedding for arbitrary text (dev/debug)."""

    text: str
    task_type: str = "SEMANTIC_SIMILARITY"


class EmbedTextResponse(BaseModel):
    dimension: int
    embedding: list[float]


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.post("/search/schools", response_model=list[SimilarSchoolResult])
async def search_schools_by_vector(
    payload: SimilarSchoolsRequest,
    session: SessionDep,
    embeddings: EmbeddingDep,
) -> list[dict]:
    """Find schools most similar to a query or student profile using pgvector.

    Either ``query`` (free text) or ``student_id`` (use saved profile
    embedding) must be provided.
    """
    if payload.query:
        query_vec = await embeddings.embed_query(payload.query)
    elif payload.student_id:
        student = await session.get(Student, payload.student_id)
        if student is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Student not found")
        if student.profile_embedding is None:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "Student profile has not been embedded yet. "
                "Call POST /vectors/students/{id}/embed first.",
            )
        query_vec = list(student.profile_embedding)
    else:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Provide either 'query' or 'student_id'.",
        )

    # pgvector cosine distance: <=> operator (lower = more similar)
    from sqlalchemy import literal_column, func, cast
    from pgvector.sqlalchemy import Vector

    vec_str = "[" + ",".join(str(v) for v in query_vec) + "]"
    vec_param = cast(literal_column(f"'{vec_str}'"), Vector(len(query_vec)))

    distance = School.embedding.cosine_distance(vec_param)
    stmt = (
        select(
            School,
            (1 - distance).label("similarity"),
        )
        .where(School.embedding.isnot(None))
        .order_by(distance)
        .limit(payload.limit)
    )

    result = await session.execute(stmt)
    rows = result.all()

    return [
        {"school": school, "similarity": round(float(sim), 4)}
        for school, sim in rows
    ]


@router.post(
    "/students/{student_id}/embed",
    status_code=status.HTTP_200_OK,
)
async def embed_student_profile(
    student_id: uuid.UUID,
    session: SessionDep,
    embeddings: EmbeddingDep,
) -> dict:
    """Generate and store a profile embedding for a student."""
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Student not found")

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

    vector = await embeddings.embed_student_profile(profile_data)
    student.profile_embedding = vector
    await session.flush()

    return {"student_id": str(student_id), "dimension": len(vector), "status": "embedded"}


@router.post(
    "/schools/{school_id}/embed",
    status_code=status.HTTP_200_OK,
)
async def embed_school_profile(
    school_id: uuid.UUID,
    session: SessionDep,
    embeddings: EmbeddingDep,
) -> dict:
    """Generate and store a profile embedding for a school."""
    school = await session.get(School, school_id)
    if school is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "School not found")

    school_data = {
        "name": school.name,
        "name_cn": school.name_cn,
        "city": school.city,
        "state": school.state,
        "school_type": school.school_type,
        "us_news_rank": school.us_news_rank,
        "acceptance_rate": school.acceptance_rate,
        "campus_setting": school.campus_setting,
    }

    vector = await embeddings.embed_school_profile(school_data)
    school.embedding = vector
    await session.flush()

    return {"school_id": str(school_id), "dimension": len(vector), "status": "embedded"}


@router.post(
    "/schools/embed-all",
    status_code=status.HTTP_200_OK,
)
async def embed_all_schools(
    session: SessionDep,
    embeddings: EmbeddingDep,
) -> dict:
    """Batch-embed all schools that don't have embeddings yet."""
    stmt = select(School).where(School.embedding.is_(None))
    result = await session.execute(stmt)
    schools = list(result.scalars().all())

    if not schools:
        return {"embedded": 0, "message": "All schools already have embeddings"}

    # Build texts for batch embedding
    texts = []
    for school in schools:
        parts = [school.name]
        if school.name_cn:
            parts.append(f"({school.name_cn})")
        if school.city and school.state:
            parts.append(f"Location: {school.city}, {school.state}")
        if school.school_type:
            parts.append(f"Type: {school.school_type}")
        if school.us_news_rank:
            parts.append(f"US News Rank: #{school.us_news_rank}")
        if school.acceptance_rate:
            parts.append(f"Acceptance rate: {school.acceptance_rate:.1%}")
        texts.append(". ".join(parts))

    vectors = await embeddings.embed_batch(texts, task_type="RETRIEVAL_DOCUMENT")

    for school, vector in zip(schools, vectors):
        school.embedding = vector

    await session.flush()

    return {"embedded": len(schools), "message": f"Embedded {len(schools)} schools"}


@router.post("/embed-text", response_model=EmbedTextResponse)
async def embed_text(
    payload: EmbedTextRequest,
    embeddings: EmbeddingDep,
) -> dict:
    """Generate an embedding for arbitrary text (development/debugging)."""
    vector = await embeddings.embed_text(payload.text, task_type=payload.task_type)
    return {"dimension": len(vector), "embedding": vector}
