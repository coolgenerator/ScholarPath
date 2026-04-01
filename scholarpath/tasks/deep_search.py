"""Celery task: run DeepSearch orchestrator for a set of schools."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from scholarpath.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="scholarpath.tasks.deep_search.run_deep_search",
    bind=True,
    max_retries=2,
    default_retry_delay=30,
)
def run_deep_search(
    self: Any,
    student_id: str,
    school_names: list[str],
) -> dict[str, Any]:
    """Run the DeepSearch orchestrator for a student and list of school names.

    This task is synchronous at the Celery boundary but internally runs
    the async orchestrator via ``asyncio.run``.

    Parameters
    ----------
    student_id:
        UUID string of the student requesting the search.
    school_names:
        List of school names to research.

    Returns
    -------
    dict
        Summary of the search results including data points discovered
        and any errors encountered.
    """
    logger.info(
        "Starting deep search for student %s: %s",
        student_id,
        school_names,
    )

    try:
        result = asyncio.run(
            _run_deep_search_async(uuid.UUID(student_id), school_names)
        )
        return result
    except Exception as exc:
        logger.exception("Deep search failed for student %s", student_id)
        raise self.retry(exc=exc)


async def _run_deep_search_async(
    student_id: uuid.UUID,
    school_names: list[str],
) -> dict[str, Any]:
    """Async implementation of the deep search task."""
    from scholarpath.db.session import async_session_factory
    from scholarpath.llm.client import get_llm_client
    from scholarpath.search import DeepSearchOrchestrator

    llm = get_llm_client()

    async with async_session_factory() as session:
        orchestrator = DeepSearchOrchestrator(llm=llm, session=session)

        results_summary: dict[str, Any] = {
            "student_id": str(student_id),
            "schools_searched": [],
            "data_points_created": 0,
            "errors": [],
        }

        for school_name in school_names:
            try:
                result = await orchestrator.search(
                    query=school_name,
                    student_id=student_id,
                )
                results_summary["schools_searched"].append(school_name)
                results_summary["data_points_created"] += len(
                    result.data_points if hasattr(result, "data_points") else []
                )
            except Exception as exc:
                logger.warning(
                    "Deep search failed for school '%s': %s",
                    school_name,
                    exc,
                )
                results_summary["errors"].append(
                    {"school": school_name, "error": str(exc)}
                )

        await session.commit()

    return results_summary
