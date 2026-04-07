"""One-shot API endpoint to enrich all schools with real data via LLM."""

from __future__ import annotations

import logging

from fastapi import APIRouter
from sqlalchemy import select

from scholarpath.api.deps import AppLLMDep, SessionDep
from scholarpath.db.models import School

router = APIRouter(prefix="/enrich", tags=["enrich"])
logger = logging.getLogger(__name__)

PROMPT = """\
You are a college data expert. For the given US school, return ACCURATE real data as JSON.
Use your training knowledge (US News, IPEDS, College Scorecard, CDS).
{
  "us_news_rank": <int>,
  "acceptance_rate": <float 0-1>,
  "sat_25": <int>, "sat_75": <int>,
  "act_25": <int or null>, "act_75": <int or null>,
  "tuition_oos": <int, out-of-state tuition+fees USD>,
  "avg_net_price": <int, average net price USD>,
  "intl_student_pct": <float 0-1>,
  "student_faculty_ratio": <float>,
  "graduation_rate_4yr": <float 0-1>,
  "endowment_per_student": <int, endowment/enrollment USD>,
  "campus_setting": <"urban"|"suburban"|"rural"|"college_town">,
  "size_category": <"small"|"medium"|"large">,
  "median_earnings_10yr": <int>,
  "retention_rate": <float 0-1>
}"""


@router.post("/schools")
async def enrich_all_schools(llm: AppLLMDep, session: SessionDep) -> dict:
    """Enrich all schools with LLM-generated real stats."""
    stmt = select(School).order_by(School.us_news_rank.asc().nullslast())
    result = await session.execute(stmt)
    schools = list(result.scalars().all())

    enriched = []
    errors = []

    for school in schools:
        try:
            data = await llm.complete_json(
                [
                    {"role": "system", "content": PROMPT},
                    {"role": "user", "content": f"{school.name}, {school.city}, {school.state}"},
                ],
                temperature=0.1,
                max_tokens=400,
                caller="enrich.schools",
            )

            updated_fields = []
            field_map = [
                "us_news_rank", "acceptance_rate", "sat_25", "sat_75",
                "act_25", "act_75", "tuition_oos", "avg_net_price",
                "intl_student_pct", "student_faculty_ratio",
                "graduation_rate_4yr", "endowment_per_student",
                "campus_setting", "size_category",
            ]
            for k in field_map:
                v = data.get(k)
                if v is not None and hasattr(school, k):
                    setattr(school, k, v)
                    updated_fields.append(k)

            # Extra fields in metadata
            meta = dict(school.metadata_ or {})
            for k in ["median_earnings_10yr", "retention_rate"]:
                if data.get(k) is not None:
                    meta[k] = data[k]
            school.metadata_ = meta

            await session.flush()
            enriched.append({"name": school.name, "fields": len(updated_fields)})

        except Exception as e:
            errors.append({"name": school.name, "error": str(e)[:100]})

    return {
        "enriched": len(enriched),
        "errors": len(errors),
        "details": enriched[:5],
        "error_details": errors[:5],
    }
