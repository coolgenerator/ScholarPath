"""Update seeded schools with real data from College Scorecard API.

Usage:
    python -m scholarpath.scripts.update_schools_from_scorecard
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.db.models import School
from scholarpath.db.session import async_session_factory
from scholarpath.services.scorecard_service import get_school_by_name

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Fields to update from Scorecard data
_UPDATE_FIELDS = [
    "acceptance_rate",
    "sat_25",
    "sat_75",
    "act_25",
    "act_75",
    "tuition_in_state",
    "tuition_oos",
    "tuition_intl",
    "avg_net_price",
    "graduation_rate_4yr",
    "website_url",
    "size_category",
]


async def run() -> None:
    async with async_session_factory() as session:
        result = await session.execute(select(School).order_by(School.name))
        schools = list(result.scalars().all())
        logger.info("Found %d schools in database", len(schools))

        updated = 0
        for school in schools:
            try:
                data = await get_school_by_name(school.name)
            except Exception:
                logger.exception("ERROR fetching: %s", school.name)
                continue
            if not data:
                logger.warning("NOT FOUND: %s", school.name)
                continue

            changes = []
            for field in _UPDATE_FIELDS:
                new_val = data.get(field)
                if new_val is not None:
                    old_val = getattr(school, field, None)
                    if old_val != new_val:
                        setattr(school, field, new_val)
                        changes.append(f"{field}: {old_val} → {new_val}")

            if changes:
                updated += 1
                logger.info("UPDATED: %s — %s", school.name, "; ".join(changes[:5]))
            else:
                logger.info("UNCHANGED: %s", school.name)

        await session.commit()
        logger.info("Done. Updated %d / %d schools.", updated, len(schools))


if __name__ == "__main__":
    asyncio.run(run())
