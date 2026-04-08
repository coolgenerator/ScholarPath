"""Celery task: detect conflicts among data points for a school."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from scholarpath.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="scholarpath.tasks.conflict_pipeline.run_conflict_detection",
    bind=True,
    max_retries=2,
    default_retry_delay=15,
)
def run_conflict_detection(
    self: Any,
    school_id: str,
) -> dict[str, Any]:
    """Load DataPoints for a school, run the conflict detector, and persist Conflicts.

    This task is synchronous at the Celery boundary but internally runs
    async database and LLM operations via ``asyncio.run``.

    Parameters
    ----------
    school_id:
        UUID string of the school to analyse.

    Returns
    -------
    dict
        Summary including counts of data points analysed and conflicts found.
    """
    logger.info("Starting conflict detection for school %s", school_id)

    try:
        result = asyncio.run(_run_conflict_detection_async(uuid.UUID(school_id)))
        return result
    except Exception as exc:
        logger.exception("Conflict detection failed for school %s", school_id)
        raise self.retry(exc=exc)


async def _run_conflict_detection_async(
    school_id: uuid.UUID,
) -> dict[str, Any]:
    """Async implementation of the conflict detection pipeline."""
    from itertools import combinations

    from sqlalchemy import select

    from scholarpath.db.models import Conflict, DataPoint, ResolutionStatus, Severity
    from scholarpath.db.session import async_session_factory
    from scholarpath.llm.client import get_llm_client
    from scholarpath.llm.prompts import CONFLICT_DETECTION_PROMPT, format_conflict_detection

    llm = get_llm_client()

    async with async_session_factory() as session:
        # Load all data points for this school
        result = await session.execute(
            select(DataPoint).where(DataPoint.school_id == school_id)
        )
        data_points = list(result.scalars().all())

        if len(data_points) < 2:
            return {
                "school_id": str(school_id),
                "data_points_analysed": len(data_points),
                "conflicts_found": 0,
            }

        # Group by variable_name
        by_variable: dict[str, list[DataPoint]] = {}
        for dp in data_points:
            by_variable.setdefault(dp.variable_name, []).append(dp)

        conflicts_created = 0

        for variable_name, dps in by_variable.items():
            if len(dps) < 2:
                continue

            for dp_a, dp_b in combinations(dps, 2):
                # Quick heuristic: skip if values are identical
                if dp_a.value_text.strip() == dp_b.value_text.strip():
                    continue

                # Use LLM to assess whether this is a meaningful conflict
                user_prompt = format_conflict_detection(
                    variable=variable_name,
                    source_a_value=dp_a.value_text,
                    source_b_value=dp_b.value_text,
                    source_a_name=dp_a.source_name,
                    source_b_name=dp_b.source_name,
                )
                messages = [
                    {"role": "system", "content": CONFLICT_DETECTION_PROMPT},
                    {"role": "user", "content": user_prompt},
                ]

                try:
                    assessment = await llm.complete_json(
                        messages,
                        temperature=0.2,
                        max_tokens=256,
                        caller="tasks.conflict_pipeline.assess",
                    )
                except Exception:
                    logger.warning(
                        "LLM conflict assessment failed for %s", variable_name,
                        exc_info=True,
                    )
                    continue

                is_conflict = assessment.get("is_conflict", False)
                if not is_conflict:
                    continue

                severity_raw = assessment.get("severity", "low")
                severity = (
                    Severity.HIGH.value
                    if severity_raw == "high"
                    else Severity.MEDIUM.value
                    if severity_raw == "medium"
                    else Severity.LOW.value
                )

                conflict = Conflict(
                    school_id=school_id,
                    variable_name=variable_name,
                    datapoint_a_id=dp_a.id,
                    datapoint_b_id=dp_b.id,
                    severity=severity,
                    value_a=dp_a.value_text,
                    value_b=dp_b.value_text,
                    resolution_status=ResolutionStatus.UNRESOLVED.value,
                    causal_analysis=assessment.get("analysis"),
                )
                session.add(conflict)
                conflicts_created += 1

        await session.commit()

    summary = {
        "school_id": str(school_id),
        "data_points_analysed": len(data_points),
        "conflicts_found": conflicts_created,
    }
    logger.info("Conflict detection complete: %s", summary)
    return summary
