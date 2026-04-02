"""Structured fallback logging helpers.

Best-effort paths should keep business behavior unchanged while emitting
consistent observability fields for debugging and eval analysis.
"""

from __future__ import annotations

import logging
from typing import Any


def _exc_info_tuple(exc: Exception | None) -> tuple[type[BaseException], BaseException, Any] | None:
    if exc is None:
        return None
    return (type(exc), exc, exc.__traceback__)


def log_fallback(
    *,
    logger: logging.Logger,
    component: str,
    stage: str,
    reason: str,
    fallback_used: bool = True,
    exc: Exception | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    """Emit a structured fallback event.

    Parameters
    ----------
    logger:
        Logger instance to write to.
    component:
        Stable component id, e.g. ``advisor.adapters``.
    stage:
        Stable stage id within the component.
    reason:
        Stable fallback reason code.
    fallback_used:
        Whether a fallback path was taken.
    exc:
        Optional exception associated with this fallback.
    extra:
        Optional key-value metadata, merged into structured payload.
    """
    payload: dict[str, Any] = {
        "component": component,
        "stage": stage,
        "reason": reason,
        "fallback_used": bool(fallback_used),
    }
    if extra:
        payload.update(extra)

    logger.warning(
        "Fallback applied component=%s stage=%s reason=%s fallback_used=%s",
        component,
        stage,
        reason,
        bool(fallback_used),
        exc_info=_exc_info_tuple(exc),
        extra={"fallback_event": payload},
    )
