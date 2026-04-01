"""Batch-enrich school data using LLM with real-world knowledge.

Usage:
    python -m scholarpath.scripts.enrich_schools

Uses gpt-5.4-mini to generate accurate, up-to-date school statistics
and writes them to the database.
"""

from __future__ import annotations

import asyncio
import json
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.config import settings
from scholarpath.db.models import School
from scholarpath.db.session import async_session_factory
from scholarpath.llm.client import get_llm_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ENRICHMENT_PROMPT = """\
You are a college admissions data expert. For the given US university/college,
provide ACCURATE, REAL data based on the most recent available information
(2024-2025 academic year preferred).

Return ONLY valid JSON with these fields. Use null if truly unknown.
DO NOT fabricate data — use your training knowledge which includes
US News rankings, College Scorecard, IPEDS, and Common Data Sets.

{
  "us_news_rank": <int, latest US News National University or LAC rank>,
  "acceptance_rate": <float 0-1, e.g. 0.04 for 4%>,
  "sat_25": <int, 25th percentile SAT>,
  "sat_75": <int, 75th percentile SAT>,
  "act_25": <int, 25th percentile ACT, null if unknown>,
  "act_75": <int, 75th percentile ACT, null if unknown>,
  "tuition_oos": <int, out-of-state tuition + fees in USD>,
  "avg_net_price": <int, average net price after aid in USD>,
  "intl_student_pct": <float 0-1, international student percentage>,
  "student_faculty_ratio": <float, e.g. 6.0 for 6:1>,
  "graduation_rate_4yr": <float 0-1, 4-year graduation rate>,
  "endowment_per_student": <int, endowment divided by enrollment, in USD>,
  "campus_setting": <"urban" | "suburban" | "rural" | "college_town">,
  "size_category": <"small" | "medium" | "large">,
  "school_type": <"university" | "lac" | "technical">,
  "notable_cs_ranking": <int or null, CS-specific USNews ranking if applicable>,
  "median_earnings_10yr": <int, median earnings 10 years after enrollment>,
  "retention_rate": <float 0-1, first-year retention rate>,
  "research_expenditure_millions": <int, annual research spending in millions USD, null if unknown>
}
"""


async def enrich_school(llm, school: School, session: AsyncSession) -> bool:
    """Enrich a single school with LLM-generated real data."""
    messages = [
        {"role": "system", "content": ENRICHMENT_PROMPT},
        {"role": "user", "content": f"School: {school.name}\nCity: {school.city}, {school.state}\nType: {school.school_type}"},
    ]

    try:
        data = await llm.complete_json(
            messages, temperature=0.1, max_tokens=512,
            caller="scripts.enrich_schools",
        )
    except Exception:
        logger.error("LLM call failed for %s", school.name, exc_info=True)
        return False

    # Update fields
    updated = []
    field_map = {
        "us_news_rank": "us_news_rank",
        "acceptance_rate": "acceptance_rate",
        "sat_25": "sat_25",
        "sat_75": "sat_75",
        "act_25": "act_25",
        "act_75": "act_75",
        "tuition_oos": "tuition_oos",
        "avg_net_price": "avg_net_price",
        "intl_student_pct": "intl_student_pct",
        "student_faculty_ratio": "student_faculty_ratio",
        "graduation_rate_4yr": "graduation_rate_4yr",
        "endowment_per_student": "endowment_per_student",
        "campus_setting": "campus_setting",
        "size_category": "size_category",
        "school_type": "school_type",
    }

    for json_key, db_field in field_map.items():
        value = data.get(json_key)
        if value is not None and hasattr(school, db_field):
            setattr(school, db_field, value)
            updated.append(db_field)

    # Store extra data in metadata
    extra_fields = ["notable_cs_ranking", "median_earnings_10yr", "retention_rate", "research_expenditure_millions"]
    metadata = dict(school.metadata_ or {})
    for key in extra_fields:
        value = data.get(key)
        if value is not None:
            metadata[key] = value
    school.metadata_ = metadata

    logger.info("  Updated %s: %d fields (%s)", school.name, len(updated), ", ".join(updated[:5]))
    return True


async def main():
    llm = get_llm_client()

    async with async_session_factory() as session:
        result = await session.execute(
            select(School).order_by(School.us_news_rank.asc().nullslast())
        )
        schools = list(result.scalars().all())
        logger.info("Found %d schools to enrich", len(schools))

        success = 0
        for i, school in enumerate(schools):
            logger.info("[%d/%d] Enriching %s...", i + 1, len(schools), school.name)
            ok = await enrich_school(llm, school, session)
            if ok:
                success += 1
            # Commit after each school so data isn't lost if script crashes
            await session.commit()

        logger.info("Done. Enriched %d/%d schools.", success, len(schools))


if __name__ == "__main__":
    asyncio.run(main())
