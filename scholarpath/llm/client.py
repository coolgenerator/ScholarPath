"""LLM client using the OpenAI-compatible API with token usage tracking and rate limiting."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from collections.abc import AsyncGenerator
from typing import Any

import openai
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from scholarpath.config import settings

logger = logging.getLogger(__name__)


class _RateLimiter:
    """Simple sliding-window rate limiter (in-process).

    Tracks request timestamps in a deque and blocks (via asyncio.sleep)
    when the window is full.
    """

    def __init__(self, max_rpm: int) -> None:
        self._max_rpm = max_rpm
        self._window: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a request slot is available within the 60s window."""
        async with self._lock:
            now = time.monotonic()
            # Purge entries older than 60 seconds
            while self._window and self._window[0] <= now - 60:
                self._window.popleft()

            if len(self._window) >= self._max_rpm:
                # Sleep until the oldest entry expires
                sleep_for = 60 - (now - self._window[0]) + 0.05
                logger.warning(
                    "Rate limit reached (%d RPM). Sleeping %.1fs",
                    self._max_rpm, sleep_for,
                )
                await asyncio.sleep(sleep_for)
                # Purge again after sleeping
                now = time.monotonic()
                while self._window and self._window[0] <= now - 60:
                    self._window.popleft()

            self._window.append(time.monotonic())

# Retry policy shared by all API calls.
_RETRY = retry(
    retry=retry_if_exception_type(
        (openai.APIConnectionError, openai.APITimeoutError, openai.RateLimitError,
         openai.InternalServerError),
    ),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)


class LLMClient:
    """Z.AI LLM client using OpenAI-compatible API with token usage tracking."""

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str,
        max_rpm: int | None = None,
    ) -> None:
        self._client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url)
        self._model = model
        self._rate_limiter = _RateLimiter(max_rpm or settings.LLM_RATE_LIMIT_RPM)

    async def _track(
        self,
        *,
        method: str,
        caller: str,
        usage: Any | None,
        request_id: str | None = None,
        error: str | None = None,
        latency_ms: int | None = None,
    ) -> None:
        """Record token usage asynchronously (best-effort)."""
        from scholarpath.llm.usage_tracker import record_usage

        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0

        if usage is not None:
            prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
            completion_tokens = getattr(usage, "completion_tokens", 0) or 0
            total_tokens = getattr(usage, "total_tokens", 0) or 0

        await record_usage(
            model=self._model,
            provider="zai",
            caller=caller,
            method=method,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            request_id=request_id,
            error=error,
            latency_ms=latency_ms,
        )

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @_RETRY
    async def complete(
        self,
        messages: list[dict[str, str]],
        *,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        caller: str = "unknown",
    ) -> str:
        """Return a plain-text completion."""
        await self._rate_limiter.acquire()
        logger.debug(
            "LLM complete  model=%s  msgs=%d  temp=%.2f  caller=%s",
            self._model, len(messages), temperature, caller,
        )
        t0 = time.monotonic()
        error_msg = None
        usage = None
        request_id = None

        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            usage = response.usage
            request_id = getattr(response, "id", None)
            text = response.choices[0].message.content or ""
            logger.debug("LLM complete  tokens=%s", usage)
            return text
        except Exception as exc:
            error_msg = str(exc)
            raise
        finally:
            latency_ms = int((time.monotonic() - t0) * 1000)
            await self._track(
                method="complete",
                caller=caller,
                usage=usage,
                request_id=request_id,
                error=error_msg,
                latency_ms=latency_ms,
            )

    @_RETRY
    async def complete_json(
        self,
        messages: list[dict[str, str]],
        *,
        schema: dict[str, Any] | None = None,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        caller: str = "unknown",
    ) -> dict[str, Any]:
        """Return a parsed JSON dict."""
        await self._rate_limiter.acquire()
        if schema is not None:
            schema_instruction = (
                "\n\nYou MUST respond with valid JSON matching this schema:\n"
                f"```json\n{json.dumps(schema, ensure_ascii=False, indent=2)}\n```"
            )
            messages = _inject_schema_hint(messages, schema_instruction)

        logger.debug(
            "LLM complete_json  model=%s  msgs=%d  temp=%.2f  caller=%s",
            self._model, len(messages), temperature, caller,
        )
        t0 = time.monotonic()
        error_msg = None
        usage = None
        request_id = None

        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
            usage = response.usage
            request_id = getattr(response, "id", None)
            raw = response.choices[0].message.content or "{}"
            logger.debug("LLM complete_json  tokens=%s", usage)
            return json.loads(raw)
        except Exception as exc:
            error_msg = str(exc)
            raise
        finally:
            latency_ms = int((time.monotonic() - t0) * 1000)
            await self._track(
                method="complete_json",
                caller=caller,
                usage=usage,
                request_id=request_id,
                error=error_msg,
                latency_ms=latency_ms,
            )

    @_RETRY
    async def stream(
        self,
        messages: list[dict[str, str]],
        *,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        caller: str = "unknown",
    ) -> AsyncGenerator[str, None]:
        """Yield text chunks via SSE streaming."""
        await self._rate_limiter.acquire()
        logger.debug(
            "LLM stream  model=%s  msgs=%d  temp=%.2f  caller=%s",
            self._model, len(messages), temperature, caller,
        )
        t0 = time.monotonic()
        total_chunks = 0
        error_msg = None

        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                stream=True,
            )
            async for chunk in response:
                delta = chunk.choices[0].delta
                if delta.content:
                    total_chunks += 1
                    yield delta.content
        except Exception as exc:
            error_msg = str(exc)
            raise
        finally:
            latency_ms = int((time.monotonic() - t0) * 1000)
            # For streaming, we estimate tokens since usage isn't returned
            await self._track(
                method="stream",
                caller=caller,
                usage=None,
                error=error_msg,
                latency_ms=latency_ms,
            )


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _inject_schema_hint(
    messages: list[dict[str, str]],
    hint: str,
) -> list[dict[str, str]]:
    """Return a *copy* of messages with *hint* appended to the system prompt."""
    messages = [dict(m) for m in messages]  # shallow copy each dict
    for msg in messages:
        if msg.get("role") == "system":
            msg["content"] += hint
            return messages
    # No system message found -- prepend one.
    messages.insert(0, {"role": "system", "content": hint.strip()})
    return messages


# ------------------------------------------------------------------
# Factory
# ------------------------------------------------------------------

_singleton: LLMClient | None = None


def get_llm_client() -> LLMClient:
    """Return a module-level singleton :class:`LLMClient` configured from settings."""
    global _singleton  # noqa: PLW0603
    if _singleton is None:
        _singleton = LLMClient(
            api_key=settings.ZAI_API_KEY,
            base_url=settings.ZAI_BASE_URL,
            model=settings.ZAI_MODEL,
        )
    return _singleton
