"""Celery task: run DeepSearch orchestrator for a set of schools."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from scholarpath.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)
_WORKER_EVENT_LOOP: asyncio.AbstractEventLoop | None = None


class StudentNotFoundError(ValueError):
    """Raised when DeepSearch target student does not exist."""


def _reset_worker_event_loop() -> None:
    """Reset cached worker loop (called on worker process init/fork)."""
    global _WORKER_EVENT_LOOP
    loop = _WORKER_EVENT_LOOP
    _WORKER_EVENT_LOOP = None
    if loop is None:
        return
    if loop.is_running():
        return
    try:
        loop.close()
    except Exception:
        return


def _get_worker_event_loop() -> asyncio.AbstractEventLoop:
    global _WORKER_EVENT_LOOP
    if _WORKER_EVENT_LOOP is None or _WORKER_EVENT_LOOP.is_closed():
        _WORKER_EVENT_LOOP = asyncio.new_event_loop()
    return _WORKER_EVENT_LOOP


def _run_on_worker_loop(coro: Any) -> Any:
    """Run coroutine on a process-local loop to avoid cross-loop pool reuse."""
    loop = _get_worker_event_loop()
    return loop.run_until_complete(coro)


def _require_scorecard_api_key(raw_key: str | None) -> str:
    key = (raw_key or "").strip()
    if not key:
        raise ValueError(
            "SCORECARD_API_KEY is required for DeepSearch. Set SCORECARD_API_KEY in environment.",
        )
    return key


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
    required_fields: list[str] | None = None,
    freshness_days: int = 90,
    max_internal_websearch_calls_per_school: int = 1,
    budget_mode: str = "balanced",
    eval_run_id: str | None = None,
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

    from scholarpath.config import settings

    scorecard_api_key = _require_scorecard_api_key(settings.SCORECARD_API_KEY)

    try:
        result = _run_on_worker_loop(
            _run_deep_search_async(
                uuid.UUID(student_id),
                school_names,
                required_fields=required_fields,
                freshness_days=freshness_days,
                max_internal_websearch_calls_per_school=max_internal_websearch_calls_per_school,
                budget_mode=budget_mode,
                eval_run_id=eval_run_id,
                scorecard_api_key=scorecard_api_key,
            )
        )
        return result
    except StudentNotFoundError as exc:
        logger.warning("Deep search skipped (non-retryable): %s", exc)
        return {
            "student_id": str(student_id),
            "schools_searched": list(school_names),
            "schools_returned": 0,
            "conflicts_found": 0,
            "coverage_score": 0.0,
            "required_fields": required_fields or [],
            "freshness_days": freshness_days,
            "max_internal_websearch_calls_per_school": max_internal_websearch_calls_per_school,
            "budget_mode": budget_mode,
            "eval_run_id": eval_run_id,
            "errors": [{"error": "student_not_found", "detail": str(exc)}],
            "non_retryable_error": "student_not_found",
        }
    except Exception as exc:
        logger.exception("Deep search failed for student %s", student_id)
        raise self.retry(exc=exc)


async def _run_deep_search_async(
    student_id: uuid.UUID,
    school_names: list[str],
    required_fields: list[str] | None = None,
    freshness_days: int = 90,
    max_internal_websearch_calls_per_school: int = 1,
    budget_mode: str = "balanced",
    eval_run_id: str | None = None,
    scorecard_api_key: str | None = None,
) -> dict[str, Any]:
    """Async implementation of the deep search task."""
    from scholarpath.config import settings
    from scholarpath.db.session import async_session_factory, engine
    from scholarpath.db.models import Student
    from scholarpath.llm.client import get_llm_client
    from scholarpath.search import DeepSearchOrchestrator

    llm = get_llm_client()
    resolved_scorecard_key = _require_scorecard_api_key(
        scorecard_api_key or settings.SCORECARD_API_KEY,
    )

    # Celery prefork + asyncio.run creates one event loop per task call.
    # Disposing pooled connections here prevents cross-loop reuse by asyncpg.
    await engine.dispose()

    async with async_session_factory() as session:
        student = await session.get(Student, student_id)
        if student is None:
            raise StudentNotFoundError(f"Student {student_id} not found")

        student_profile = {
            "gpa": student.gpa,
            "sat_total": student.sat_total,
            "intended_major": (student.intended_majors or [None])[0],
            "budget_usd": student.budget_usd,
            "preferences": student.preferences,
        }

        orchestrator = DeepSearchOrchestrator(
            llm=llm,
            scorecard_api_key=resolved_scorecard_key,
            search_api_url=settings.WEB_SEARCH_API_URL,
            search_api_key=settings.WEB_SEARCH_API_KEY,
            school_profile_search_api_url=settings.SCHOOL_PROFILE_SEARCH_API_URL,
            school_profile_search_api_key=settings.SCHOOL_PROFILE_SEARCH_API_KEY,
            school_concurrency=settings.DEEPSEARCH_SCHOOL_CONCURRENCY,
            source_http_concurrency=settings.DEEPSEARCH_SOURCE_HTTP_CONCURRENCY,
            self_extract_concurrency=settings.DEEPSEARCH_SELF_EXTRACT_CONCURRENCY,
            internal_websearch_concurrency=settings.DEEPSEARCH_INTERNAL_WEBSEARCH_CONCURRENCY,
        )

        results_summary: dict[str, Any] = {
            "student_id": str(student_id),
            "schools_searched": [],
            "schools_returned": 0,
            "conflicts_found": 0,
            "coverage_score": 0.0,
            "errors": [],
            "required_fields": required_fields or [],
            "freshness_days": freshness_days,
            "max_internal_websearch_calls_per_school": max_internal_websearch_calls_per_school,
            "budget_mode": budget_mode,
            "eval_run_id": eval_run_id,
        }

        try:
            suffix_token = llm.set_caller_suffix(eval_run_id)
            try:
                result = await orchestrator.search(
                    student_profile=student_profile,
                    target_schools=school_names,
                    required_fields=required_fields,
                    freshness_days=freshness_days,
                    max_internal_websearch_calls_per_school=max_internal_websearch_calls_per_school,
                    budget_mode=budget_mode,
                    eval_run_id=eval_run_id,
                )
            finally:
                llm.reset_caller_suffix(suffix_token)
            results_summary["schools_searched"] = school_names
            results_summary["schools_returned"] = len(result.schools)
            results_summary["conflicts_found"] = len(result.conflicts)
            results_summary["coverage_score"] = result.coverage_score
            results_summary["schools"] = result.schools
            results_summary["search_metadata"] = result.search_metadata
            for key in (
                "db_hit_ratio",
                "self_source_calls",
                "internal_websearch_calls",
                "tokens_by_stage",
                "fallback_trigger_rate",
                "source_value_scores",
                "source_runtime_metrics",
                "source_priority_next_run",
                "raw_fact_count_before_merge",
                "unique_fact_count_after_merge",
                "dedupe_drop_count",
                "multi_source_agreement_count",
                "multi_source_conflict_count",
                "critical_coverage_by_school",
            ):
                if key in result.search_metadata:
                    results_summary[key] = result.search_metadata[key]
        except Exception as exc:
            logger.warning(
                "Deep search failed for schools '%s': %s",
                school_names,
                exc,
            )
            results_summary["errors"].append(
                {"schools": school_names, "error": str(exc)}
            )

        await session.commit()

    return results_summary
