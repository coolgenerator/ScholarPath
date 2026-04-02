"""Celery tasks for advisor memory ingestion and cleanup."""

from __future__ import annotations

import logging
from typing import Any

from scholarpath.tasks.async_runtime import run_async
from scholarpath.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="scholarpath.tasks.advisor_memory.advisor_memory_ingest",
    bind=True,
    max_retries=1,
    default_retry_delay=10,
)
def advisor_memory_ingest(self: Any, event: dict[str, Any]) -> dict[str, Any]:
    """Ingest one advisor turn event into layered memory stores."""
    try:
        return run_async(_advisor_memory_ingest_async(event=event))
    except Exception as exc:
        logger.exception("Advisor memory ingest failed")
        raise self.retry(exc=exc)


@celery_app.task(
    name="scholarpath.tasks.advisor_memory.advisor_memory_ingest_message",
    bind=True,
    max_retries=1,
    default_retry_delay=10,
)
def advisor_memory_ingest_message(self: Any, message_id: str) -> dict[str, Any]:
    """Ingest one advisor message row by message id (preferred path)."""
    try:
        return run_async(_advisor_memory_ingest_async(event=None, message_id=message_id))
    except Exception as exc:
        logger.exception("Advisor memory ingest by message_id failed")
        raise self.retry(exc=exc)


@celery_app.task(
    name="scholarpath.tasks.advisor_memory.advisor_memory_cleanup",
    bind=True,
    max_retries=1,
    default_retry_delay=30,
)
def advisor_memory_cleanup(self: Any, batch_size: int = 2000) -> dict[str, Any]:
    """Daily cleanup for expired memory rows and orphan vectors."""
    try:
        return run_async(_advisor_memory_cleanup_async(batch_size=batch_size))
    except Exception as exc:
        logger.exception("Advisor memory cleanup failed")
        raise self.retry(exc=exc)


async def _advisor_memory_ingest_async(
    *,
    event: dict[str, Any] | None,
    message_id: str | None = None,
) -> dict[str, Any]:
    from scholarpath.advisor.memory_context import ingest_memory_event
    from scholarpath.db.session import async_session_factory

    async with async_session_factory() as session:
        result = await ingest_memory_event(
            session=session,
            event=event,
            message_id=message_id,
        )
        await session.commit()
        return result


async def _advisor_memory_cleanup_async(batch_size: int = 2000) -> dict[str, Any]:
    from scholarpath.advisor.memory_context import cleanup_memory_records
    from scholarpath.db.session import async_session_factory

    async with async_session_factory() as session:
        result = await cleanup_memory_records(session=session, batch_size=batch_size)
        await session.commit()
        return result
