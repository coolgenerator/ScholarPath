"""Shared async runtime for Celery tasks.

Celery tasks are synchronous callables, but many task bodies await async DB/LLM code.
Using ``asyncio.run`` per task invocation can create a fresh event loop each time, which
causes cross-loop failures when pooled async resources are reused.

This module keeps a single background event loop per worker process and executes
coroutines on that loop via ``run_coroutine_threadsafe``.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")

_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_loop_lock = threading.Lock()


def _ensure_loop() -> asyncio.AbstractEventLoop:
    """Return a live background event loop for this process."""
    global _loop
    global _loop_thread

    with _loop_lock:
        if (
            _loop is not None
            and _loop_thread is not None
            and _loop_thread.is_alive()
            and not _loop.is_closed()
        ):
            return _loop

        loop = asyncio.new_event_loop()

        def _runner() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(
            target=_runner,
            name="scholarpath-celery-async-runtime",
            daemon=True,
        )
        thread.start()

        _loop = loop
        _loop_thread = thread
        return loop


def run_async(coro: Awaitable[T]) -> T:
    """Run a coroutine on the shared task loop and return its result."""
    loop = _ensure_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()

