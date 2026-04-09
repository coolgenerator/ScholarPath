"""CLI script for collecting Reddit community reviews and generating reports.

Usage
-----
    python scripts/collect_community_reviews.py collect              # fetch Reddit posts
    python scripts/collect_community_reviews.py summarize            # generate LLM reports
    python scripts/collect_community_reviews.py all                  # both steps
    python scripts/collect_community_reviews.py collect --school MIT
    python scripts/collect_community_reviews.py all --concurrency 2
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Ensure the project root is on sys.path so scholarpath is importable.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.db.models.school import School
from scholarpath.db.session import async_session_factory
from scholarpath.llm.client import get_llm_client
from scholarpath.services.community_review_service import (
    SCHOOL_SUBREDDIT_MAP,
    collect_reviews_for_school,
    generate_community_report,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
)
logger = logging.getLogger("collect_community_reviews")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _resolve_schools(
    session: AsyncSession,
    school_filter: str | None,
) -> list[School]:
    """Return the list of schools to process.

    If *school_filter* is provided, do a fuzzy-match against ``School.name``.
    Otherwise return all schools that appear in ``SCHOOL_SUBREDDIT_MAP``.
    """
    if school_filter:
        stmt = select(School).where(
            School.name.ilike(f"%{school_filter}%")
        )
        result = await session.execute(stmt)
        schools = list(result.scalars().all())
        if not schools:
            logger.error("No school found matching '%s'", school_filter)
        return schools

    # Default: only schools with a known subreddit mapping
    mapped_names = list(SCHOOL_SUBREDDIT_MAP.keys())
    stmt = select(School).where(School.name.in_(mapped_names))
    result = await session.execute(stmt)
    return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Collect
# ---------------------------------------------------------------------------

async def _collect(school_filter: str | None, concurrency: int) -> None:
    """Fetch Reddit posts for matching schools."""
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as http_client:
        async with async_session_factory() as session:
            schools = await _resolve_schools(session, school_filter)
            logger.info("Collecting reviews for %d school(s)", len(schools))

            async def _process(school: School) -> None:
                async with semaphore:
                    try:
                        n = await collect_reviews_for_school(
                            session, school, http_client=http_client,
                        )
                        logger.info("  %s — %d new reviews", school.name, n)
                    except Exception:
                        logger.exception("  %s — FAILED", school.name)

            tasks = [_process(s) for s in schools]
            await asyncio.gather(*tasks)

            await session.commit()

    logger.info("Collection complete.")


# ---------------------------------------------------------------------------
# Summarize
# ---------------------------------------------------------------------------

async def _summarize(school_filter: str | None, concurrency: int) -> None:
    """Generate LLM community reports for matching schools."""
    llm = get_llm_client()
    semaphore = asyncio.Semaphore(concurrency)

    async with async_session_factory() as session:
        schools = await _resolve_schools(session, school_filter)
        logger.info("Generating reports for %d school(s)", len(schools))

        async def _process(school: School) -> None:
            async with semaphore:
                try:
                    report = await generate_community_report(session, llm, school)
                    logger.info(
                        "  %s — score=%.1f (%d reviews)",
                        school.name, report.overall_score or 0, report.review_count,
                    )
                except ValueError as exc:
                    logger.warning("  %s — skipped: %s", school.name, exc)
                except Exception:
                    logger.exception("  %s — FAILED", school.name)

        tasks = [_process(s) for s in schools]
        await asyncio.gather(*tasks)

        await session.commit()

    logger.info("Summarization complete.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect Reddit community reviews and generate reports.",
    )
    parser.add_argument(
        "action",
        choices=["collect", "summarize", "all"],
        help="Which step(s) to run.",
    )
    parser.add_argument(
        "--school",
        type=str,
        default=None,
        help="Filter to a single school by (partial) name.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=2,
        help="Max parallel tasks (default: 2).",
    )

    args = parser.parse_args()

    if args.action in ("collect", "all"):
        asyncio.run(_collect(args.school, args.concurrency))

    if args.action in ("summarize", "all"):
        asyncio.run(_summarize(args.school, args.concurrency))


if __name__ == "__main__":
    main()
