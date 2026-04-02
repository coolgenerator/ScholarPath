"""Token usage tracker -- persists every LLM call to the database."""

from __future__ import annotations

import logging
import time
from typing import Any

from scholarpath.db.models.token_usage import TokenUsage
from scholarpath.observability import log_fallback

logger = logging.getLogger(__name__)

# In-memory fallback when DB is not available (e.g. during startup)
_in_memory_log: list[dict[str, Any]] = []


async def record_usage(
    *,
    model: str,
    provider: str,
    caller: str,
    method: str,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
    student_id: str | None = None,
    session_id: str | None = None,
    request_id: str | None = None,
    error: str | None = None,
    latency_ms: int | None = None,
    estimated_cost_usd: float | None = None,
) -> None:
    """Record a single LLM API call to the database.

    Best-effort: failures are logged but never propagated.
    """
    entry = {
        "model": model,
        "provider": provider,
        "caller": caller,
        "method": method,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "student_id": student_id,
        "session_id": session_id,
        "request_id": request_id,
        "error": error,
        "latency_ms": latency_ms,
        "estimated_cost_usd": estimated_cost_usd,
    }

    try:
        from scholarpath.db.session import async_session_factory

        async with async_session_factory() as session:
            usage = TokenUsage(**entry)
            session.add(usage)
            await session.commit()

        logger.debug(
            "Tracked usage  model=%s caller=%s tokens=%d (p=%d c=%d)",
            model, caller, total_tokens, prompt_tokens, completion_tokens,
        )
    except Exception as exc:
        # Fall back to in-memory log
        _in_memory_log.append(entry)
        log_fallback(
            logger=logger,
            component="llm.usage_tracker",
            stage="record_usage.persist",
            reason="db_unavailable",
            fallback_used=True,
            exc=exc,
            extra={
                "model": model,
                "caller": caller,
                "provider": provider,
                "total_tokens": total_tokens,
            },
        )


def get_in_memory_log() -> list[dict[str, Any]]:
    """Return usage entries that couldn't be persisted to DB."""
    return list(_in_memory_log)
