"""LLM client using the OpenAI-compatible API with token usage tracking and rate limiting."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from contextvars import ContextVar, Token
from dataclasses import dataclass
from collections import deque
from collections.abc import AsyncGenerator
from typing import Any, Protocol

import openai
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from scholarpath.config import settings

logger = logging.getLogger(__name__)

try:
    from redis import asyncio as redis_asyncio
except Exception:  # pragma: no cover - optional import fallback
    redis_asyncio = None


class _AcquireLimiter(Protocol):
    async def acquire(self) -> None:
        """Acquire a permit before issuing an API call."""


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


_REDIS_RATE_LIMITER_LUA = """
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call("ZREMRANGEBYSCORE", key, 0, now_ms - window_ms)
local count = redis.call("ZCARD", key)
if count < limit then
  redis.call("ZADD", key, now_ms, member)
  redis.call("PEXPIRE", key, window_ms + 1000)
  return 0
end

local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
if oldest[2] == nil then
  return 10
end

local wait_ms = tonumber(oldest[2]) + window_ms - now_ms + 5
if wait_ms < 5 then
  wait_ms = 5
end
return wait_ms
"""


class _RedisRateLimiter:
    """Cross-process sliding-window limiter stored in Redis."""

    def __init__(self, *, redis_url: str, key: str, max_rpm: int) -> None:
        if redis_asyncio is None:
            raise RuntimeError("redis.asyncio is unavailable")
        self._redis = redis_asyncio.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        self._key = key
        self._max_rpm = max(1, int(max_rpm))
        self._window_ms = 60_000

    async def acquire(self) -> None:
        while True:
            now_ms = int(time.time() * 1000)
            member = f"{now_ms}:{time.monotonic_ns()}"
            wait_ms_raw = await self._redis.eval(
                _REDIS_RATE_LIMITER_LUA,
                1,
                self._key,
                now_ms,
                self._window_ms,
                self._max_rpm,
                member,
            )
            try:
                wait_ms = int(wait_ms_raw or 0)
            except (TypeError, ValueError):
                wait_ms = 0

            if wait_ms <= 0:
                return
            await asyncio.sleep(wait_ms / 1000.0)


class _SmartRateLimiter:
    """Uses Redis limiter when available; falls back to local in-memory limiter."""

    def __init__(
        self,
        *,
        max_rpm: int,
        redis_url: str | None,
        endpoint_label: str,
        api_key_fingerprint: str,
    ) -> None:
        self._local = _RateLimiter(max_rpm)
        self._endpoint_label = endpoint_label
        self._degraded_until = 0.0
        self._redis_limiter: _RedisRateLimiter | None = None

        redis_url_clean = (redis_url or "").strip()
        if redis_url_clean and redis_asyncio is not None:
            redis_key = f"llm:rpm:{api_key_fingerprint}"
            self._redis_limiter = _RedisRateLimiter(
                redis_url=redis_url_clean,
                key=redis_key,
                max_rpm=max_rpm,
            )

    async def acquire(self) -> None:
        now = time.monotonic()
        if self._redis_limiter is not None and now >= self._degraded_until:
            try:
                await self._redis_limiter.acquire()
                return
            except Exception as exc:
                self._degraded_until = time.monotonic() + 30.0
                logger.warning(
                    "Redis limiter unavailable for %s, fallback to local limiter for 30s: %s",
                    self._endpoint_label,
                    exc,
                )

        await self._local.acquire()


class _RedisEndpointObserver:
    """Aggregates per-key request/error activity in Redis for real-time observability."""

    def __init__(self, redis_url: str) -> None:
        if redis_asyncio is None:
            raise RuntimeError("redis.asyncio is unavailable")
        self._redis = redis_asyncio.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        self._retention_ms = 24 * 60 * 60 * 1000

    @staticmethod
    def _k(prefix: str, key_id: str) -> str:
        return f"llm:obs:{key_id}:{prefix}"

    async def record(
        self,
        *,
        key_id: str,
        ok: bool,
        error_kind: str | None,
        latency_ms: int | None,
    ) -> None:
        now_ms = int(time.time() * 1000)
        member = f"{now_ms}:{time.monotonic_ns()}"
        req_key = self._k("req", key_id)
        err_key = self._k("err", key_id)
        rl_key = self._k("rate_limit", key_id)
        timeout_key = self._k("timeout", key_id)
        stats_key = self._k("stats", key_id)

        pipe = self._redis.pipeline(transaction=False)
        # Requests window + totals.
        pipe.zadd(req_key, {member: now_ms})
        pipe.zremrangebyscore(req_key, 0, now_ms - self._retention_ms)
        pipe.pexpire(req_key, self._retention_ms + 60_000)
        pipe.hincrby(stats_key, "requests_total", 1)

        if latency_ms is not None:
            pipe.hincrbyfloat(stats_key, "latency_ms_total", max(0, latency_ms))

        if not ok:
            pipe.zadd(err_key, {member: now_ms})
            pipe.zremrangebyscore(err_key, 0, now_ms - self._retention_ms)
            pipe.pexpire(err_key, self._retention_ms + 60_000)
            pipe.hincrby(stats_key, "errors_total", 1)

            if error_kind == "rate_limit":
                pipe.zadd(rl_key, {member: now_ms})
                pipe.zremrangebyscore(rl_key, 0, now_ms - self._retention_ms)
                pipe.pexpire(rl_key, self._retention_ms + 60_000)
                pipe.hincrby(stats_key, "rate_limit_total", 1)
            elif error_kind == "timeout":
                pipe.zadd(timeout_key, {member: now_ms})
                pipe.zremrangebyscore(timeout_key, 0, now_ms - self._retention_ms)
                pipe.pexpire(timeout_key, self._retention_ms + 60_000)
                pipe.hincrby(stats_key, "timeout_total", 1)

        pipe.pexpire(stats_key, self._retention_ms + 60_000)
        await pipe.execute()

    async def snapshot(
        self,
        *,
        key_ids: list[str],
        window_seconds: int = 60,
    ) -> dict[str, dict[str, float]]:
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - max(1, int(window_seconds)) * 1000

        pipe = self._redis.pipeline(transaction=False)
        for key_id in key_ids:
            pipe.zcount(self._k("req", key_id), start_ms, now_ms)
            pipe.zcount(self._k("err", key_id), start_ms, now_ms)
            pipe.zcount(self._k("rate_limit", key_id), start_ms, now_ms)
            pipe.zcount(self._k("timeout", key_id), start_ms, now_ms)
            pipe.hgetall(self._k("stats", key_id))
        rows = await pipe.execute()

        out: dict[str, dict[str, float]] = {}
        idx = 0
        for key_id in key_ids:
            req_window = int(rows[idx] or 0)
            idx += 1
            err_window = int(rows[idx] or 0)
            idx += 1
            rl_window = int(rows[idx] or 0)
            idx += 1
            timeout_window = int(rows[idx] or 0)
            idx += 1
            totals_raw = rows[idx] or {}
            idx += 1

            def _to_float(field: str) -> float:
                raw = totals_raw.get(field, 0)
                try:
                    return float(raw)
                except (TypeError, ValueError):
                    return 0.0

            out[key_id] = {
                "requests_window": float(req_window),
                "errors_window": float(err_window),
                "rate_limit_window": float(rl_window),
                "timeout_window": float(timeout_window),
                "requests_total": _to_float("requests_total"),
                "errors_total": _to_float("errors_total"),
                "rate_limit_total": _to_float("rate_limit_total"),
                "timeout_total": _to_float("timeout_total"),
                "latency_ms_total": _to_float("latency_ms_total"),
            }

        return out

# Retry policy shared by all API calls.
_RETRY = retry(
    retry=retry_if_exception_type(
        (openai.APIConnectionError, openai.APITimeoutError, openai.RateLimitError,
         openai.InternalServerError),
    ),
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=0.2, min=0.2, max=1.0),
    reraise=True,
)

_RETRIABLE_EXCEPTIONS = (
    openai.APIConnectionError,
    openai.APITimeoutError,
    openai.RateLimitError,
    openai.InternalServerError,
)

_SAME_TASK_RETRY_DELAY_SECONDS = 5.0
_TIMEOUT_RETRY_DELAY_SECONDS = 0.4
_PROVIDER_LIMIT_TEXT_HINTS = (
    "too many pending requests",
    "request reached limit",
    "request has reached limit",
    "request limit reached",
    "request达到上限",
    "请求达到上限",
    "达到上限",
)

_CALLER_SUFFIX: ContextVar[str | None] = ContextVar(
    "llm_caller_suffix",
    default=None,
)


@dataclass
class _Endpoint:
    index: int
    key_id: str
    client: openai.AsyncOpenAI
    rate_limiter: _AcquireLimiter
    sent_requests: int = 0
    error_requests: int = 0
    rate_limit_errors: int = 0
    timeout_errors: int = 0
    cooldown_until: float = 0.0
    same_task_retry_triggered: int = 0
    same_task_retry_success: int = 0
    same_task_retry_failed: int = 0


class LLMClient:
    """Z.AI LLM client using OpenAI-compatible API with token usage tracking."""

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str,
        api_keys: list[str] | None = None,
        max_rpm: int | None = None,
    ) -> None:
        self._model = model
        self._base_url = base_url
        max_rpm_per_endpoint = max_rpm or settings.LLM_RATE_LIMIT_RPM
        self._request_timeout_seconds = max(
            1.0,
            float(getattr(settings, "LLM_REQUEST_TIMEOUT_SECONDS", 4.5) or 4.5),
        )
        base_url_lower = (base_url or "").strip().lower()
        # Some providers return empty message.content for non-stream chat completions.
        # For those providers, force JSON extraction through streamed deltas.
        self._xcode_stream_json_mode = (
            "api.xcode.best" in base_url_lower
            or "beecode.cc" in base_url_lower
        )
        # beecode gateway is more stable with response_format only (no extra response_type/content_type hints).
        self._chat_json_transport_hints_enabled = "beecode.cc" not in base_url_lower
        # beecode blocks some default SDK headers; pin a lean header set for compatibility.
        beecode_default_headers: dict[str, str] | None = None
        if "beecode.cc" in base_url_lower:
            beecode_default_headers = {
                "User-Agent": "scholarpath-llm-client/1.0",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }

        keys = [k for k in (api_keys or [api_key]) if k]
        if not keys:
            keys = [api_key]

        redis_url = (settings.REDIS_URL or "").strip()
        self._observer: _RedisEndpointObserver | None = None
        self._observer_error: str | None = None
        if redis_url and redis_asyncio is not None:
            try:
                self._observer = _RedisEndpointObserver(redis_url)
            except Exception as exc:  # pragma: no cover - runtime/env specific
                self._observer_error = str(exc)
                logger.warning(
                    "LLM endpoint observer disabled because Redis observer init failed: %s",
                    exc,
                )
        self._endpoints: list[_Endpoint] = []
        for i, key in enumerate(keys):
            key_id = _api_key_fingerprint(key)
            client_kwargs: dict[str, Any] = {
                "api_key": key,
                "base_url": base_url,
            }
            if beecode_default_headers is not None:
                client_kwargs["default_headers"] = beecode_default_headers
            self._endpoints.append(
                _Endpoint(
                    index=i,
                    key_id=key_id,
                    client=openai.AsyncOpenAI(**client_kwargs),
                    rate_limiter=_SmartRateLimiter(
                        max_rpm=max_rpm_per_endpoint,
                        redis_url=redis_url,
                        endpoint_label=f"endpoint[{i}]",
                        api_key_fingerprint=key_id,
                    ),
                ),
            )
        self._rr_cursor = 0
        self._rr_lock = asyncio.Lock()

    async def _ordered_endpoints(self) -> list[_Endpoint]:
        if not self._endpoints:
            raise RuntimeError("No LLM endpoints configured")
        async with self._rr_lock:
            start = self._rr_cursor
            self._rr_cursor = (self._rr_cursor + 1) % len(self._endpoints)
        ordered = [
            self._endpoints[(start + i) % len(self._endpoints)]
            for i in range(len(self._endpoints))
        ]
        now = time.monotonic()
        ready = [endpoint for endpoint in ordered if endpoint.cooldown_until <= now]
        cooling = [endpoint for endpoint in ordered if endpoint.cooldown_until > now]
        return ready + cooling

    @staticmethod
    def _classify_error_kind(exc: Exception) -> str:
        if isinstance(exc, openai.RateLimitError):
            return "rate_limit"
        if isinstance(exc, openai.APITimeoutError):
            return "timeout"
        return "other"

    @staticmethod
    def _cooldown_seconds(error_kind: str) -> float:
        if error_kind == "rate_limit":
            return 2.0
        if error_kind == "timeout":
            return 1.0
        return 0.4

    @staticmethod
    def _is_provider_limit_retry_error(exc: Exception) -> bool:
        if not isinstance(exc, openai.RateLimitError):
            return False
        message = str(exc or "").strip().lower()
        if not message:
            return False
        return any(token in message for token in _PROVIDER_LIMIT_TEXT_HINTS)

    async def _record_endpoint_outcome(
        self,
        *,
        endpoint: _Endpoint,
        ok: bool,
        error_kind: str | None,
        latency_ms: int | None,
    ) -> None:
        if ok:
            endpoint.sent_requests += 1
        else:
            endpoint.error_requests += 1
            if error_kind == "rate_limit":
                endpoint.rate_limit_errors += 1
            elif error_kind == "timeout":
                endpoint.timeout_errors += 1
            cooldown = self._cooldown_seconds(error_kind or "other")
            endpoint.cooldown_until = max(
                endpoint.cooldown_until,
                time.monotonic() + cooldown,
            )

        if self._observer is None:
            return
        try:
            await self._observer.record(
                key_id=endpoint.key_id,
                ok=ok,
                error_kind=error_kind,
                latency_ms=latency_ms,
            )
        except Exception as exc:  # pragma: no cover - runtime/env specific
            logger.debug(
                "Failed to record endpoint observation for endpoint[%d]: %s",
                endpoint.index,
                exc,
            )

    async def _chat_completion_with_failover(
        self,
        *,
        messages: list[dict[str, str]],
        temperature: float,
        max_tokens: int,
        response_format: dict[str, Any] | None = None,
        json_transport_hints: bool = False,
        stream: bool = False,
    ) -> tuple[_Endpoint, Any]:
        last_exc: Exception | None = None

        for endpoint in await self._ordered_endpoints():
            attempt_t0 = time.monotonic()
            try:
                if endpoint.cooldown_until > time.monotonic():
                    await asyncio.sleep(
                        min(max(endpoint.cooldown_until - time.monotonic(), 0.0), 0.2),
                    )
                await endpoint.rate_limiter.acquire()
                payload: dict[str, Any] = {
                    "model": self._model,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                    "stream": stream,
                    "timeout": self._request_timeout_seconds,
                }
                if response_format is not None:
                    payload["response_format"] = response_format
                if json_transport_hints:
                    # Hint OpenAI-compatible gateways to keep request/response in strict JSON mode.
                    payload["extra_headers"] = {
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    }
                    payload["extra_body"] = {
                        "response_type": "json",
                        "content_type": "application/json",
                    }

                response = await endpoint.client.chat.completions.create(**payload)
                await self._record_endpoint_outcome(
                    endpoint=endpoint,
                    ok=True,
                    error_kind=None,
                    latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                )
                return endpoint, response
            except _RETRIABLE_EXCEPTIONS as exc:
                if self._is_provider_limit_retry_error(exc):
                    endpoint.same_task_retry_triggered += 1
                    await self._record_endpoint_outcome(
                        endpoint=endpoint,
                        ok=False,
                        error_kind="rate_limit",
                        latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                    )
                    logger.warning(
                        "same_task_retry_triggered endpoint[%d] key=%s op=chat wait=%.1fs reason=%s",
                        endpoint.index,
                        endpoint.key_id,
                        _SAME_TASK_RETRY_DELAY_SECONDS,
                        exc,
                    )
                    await asyncio.sleep(_SAME_TASK_RETRY_DELAY_SECONDS)
                    retry_t0 = time.monotonic()
                    try:
                        await endpoint.rate_limiter.acquire()
                        response = await endpoint.client.chat.completions.create(**payload)
                        endpoint.same_task_retry_success += 1
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=True,
                            error_kind=None,
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.info(
                            "same_task_retry_success endpoint[%d] key=%s op=chat",
                            endpoint.index,
                            endpoint.key_id,
                        )
                        return endpoint, response
                    except _RETRIABLE_EXCEPTIONS as retry_exc:
                        endpoint.same_task_retry_failed += 1
                        last_exc = retry_exc
                        retry_kind = self._classify_error_kind(retry_exc)
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=False,
                            error_kind=retry_kind,
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.warning(
                            "same_task_retry_failed endpoint[%d] key=%s op=chat kind=%s, trying next endpoint: %s",
                            endpoint.index,
                            endpoint.key_id,
                            retry_kind,
                            retry_exc,
                        )
                        continue
                    except Exception:
                        endpoint.same_task_retry_failed += 1
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=False,
                            error_kind="other",
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.warning(
                            "same_task_retry_failed endpoint[%d] key=%s op=chat kind=other",
                            endpoint.index,
                            endpoint.key_id,
                        )
                        raise

                if isinstance(exc, openai.APITimeoutError):
                    endpoint.same_task_retry_triggered += 1
                    await self._record_endpoint_outcome(
                        endpoint=endpoint,
                        ok=False,
                        error_kind="timeout",
                        latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                    )
                    logger.warning(
                        "same_task_retry_triggered endpoint[%d] key=%s op=chat wait=%.1fs reason=timeout",
                        endpoint.index,
                        endpoint.key_id,
                        _TIMEOUT_RETRY_DELAY_SECONDS,
                    )
                    await asyncio.sleep(_TIMEOUT_RETRY_DELAY_SECONDS)
                    retry_t0 = time.monotonic()
                    try:
                        await endpoint.rate_limiter.acquire()
                        response = await endpoint.client.chat.completions.create(**payload)
                        endpoint.same_task_retry_success += 1
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=True,
                            error_kind=None,
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.info(
                            "same_task_retry_success endpoint[%d] key=%s op=chat reason=timeout",
                            endpoint.index,
                            endpoint.key_id,
                        )
                        return endpoint, response
                    except _RETRIABLE_EXCEPTIONS as retry_exc:
                        endpoint.same_task_retry_failed += 1
                        last_exc = retry_exc
                        retry_kind = self._classify_error_kind(retry_exc)
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=False,
                            error_kind=retry_kind,
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.warning(
                            "same_task_retry_failed endpoint[%d] key=%s op=chat reason=timeout kind=%s, trying next endpoint: %s",
                            endpoint.index,
                            endpoint.key_id,
                            retry_kind,
                            retry_exc,
                        )
                        continue
                    except Exception:
                        endpoint.same_task_retry_failed += 1
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=False,
                            error_kind="other",
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.warning(
                            "same_task_retry_failed endpoint[%d] key=%s op=chat reason=timeout kind=other",
                            endpoint.index,
                            endpoint.key_id,
                        )
                        raise

                last_exc = exc
                error_kind = self._classify_error_kind(exc)
                await self._record_endpoint_outcome(
                    endpoint=endpoint,
                    ok=False,
                    error_kind=error_kind,
                    latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                )
                logger.warning(
                    "LLM request failed on endpoint[%d] key=%s kind=%s, trying next endpoint: %s",
                    endpoint.index,
                    endpoint.key_id,
                    error_kind,
                    exc,
                )
                continue
            except Exception as exc:
                await self._record_endpoint_outcome(
                    endpoint=endpoint,
                    ok=False,
                    error_kind="other",
                    latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                )
                raise

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("No available LLM endpoint")

    async def _responses_with_failover(
        self,
        *,
        input_text: str,
        tools: list[dict[str, Any]] | None = None,
        temperature: float = 0.1,
        max_output_tokens: int = 1024,
    ) -> tuple[_Endpoint, Any]:
        last_exc: Exception | None = None

        for endpoint in await self._ordered_endpoints():
            attempt_t0 = time.monotonic()
            try:
                if endpoint.cooldown_until > time.monotonic():
                    await asyncio.sleep(
                        min(max(endpoint.cooldown_until - time.monotonic(), 0.0), 0.2),
                    )
                await endpoint.rate_limiter.acquire()
                payload: dict[str, Any] = {
                    "model": self._model,
                    "input": input_text,
                    "max_output_tokens": max_output_tokens,
                    "temperature": temperature,
                    "timeout": self._request_timeout_seconds,
                }
                if tools is not None:
                    payload["tools"] = tools

                response = await endpoint.client.responses.create(**payload)
                await self._record_endpoint_outcome(
                    endpoint=endpoint,
                    ok=True,
                    error_kind=None,
                    latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                )
                return endpoint, response
            except _RETRIABLE_EXCEPTIONS as exc:
                if self._is_provider_limit_retry_error(exc):
                    endpoint.same_task_retry_triggered += 1
                    await self._record_endpoint_outcome(
                        endpoint=endpoint,
                        ok=False,
                        error_kind="rate_limit",
                        latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                    )
                    logger.warning(
                        "same_task_retry_triggered endpoint[%d] key=%s op=responses wait=%.1fs reason=%s",
                        endpoint.index,
                        endpoint.key_id,
                        _SAME_TASK_RETRY_DELAY_SECONDS,
                        exc,
                    )
                    await asyncio.sleep(_SAME_TASK_RETRY_DELAY_SECONDS)
                    retry_t0 = time.monotonic()
                    try:
                        await endpoint.rate_limiter.acquire()
                        response = await endpoint.client.responses.create(**payload)
                        endpoint.same_task_retry_success += 1
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=True,
                            error_kind=None,
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.info(
                            "same_task_retry_success endpoint[%d] key=%s op=responses",
                            endpoint.index,
                            endpoint.key_id,
                        )
                        return endpoint, response
                    except _RETRIABLE_EXCEPTIONS as retry_exc:
                        endpoint.same_task_retry_failed += 1
                        last_exc = retry_exc
                        retry_kind = self._classify_error_kind(retry_exc)
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=False,
                            error_kind=retry_kind,
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.warning(
                            "same_task_retry_failed endpoint[%d] key=%s op=responses kind=%s, trying next endpoint: %s",
                            endpoint.index,
                            endpoint.key_id,
                            retry_kind,
                            retry_exc,
                        )
                        continue
                    except Exception:
                        endpoint.same_task_retry_failed += 1
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=False,
                            error_kind="other",
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.warning(
                            "same_task_retry_failed endpoint[%d] key=%s op=responses kind=other",
                            endpoint.index,
                            endpoint.key_id,
                        )
                        raise

                if isinstance(exc, openai.APITimeoutError):
                    endpoint.same_task_retry_triggered += 1
                    await self._record_endpoint_outcome(
                        endpoint=endpoint,
                        ok=False,
                        error_kind="timeout",
                        latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                    )
                    logger.warning(
                        "same_task_retry_triggered endpoint[%d] key=%s op=responses wait=%.1fs reason=timeout",
                        endpoint.index,
                        endpoint.key_id,
                        _TIMEOUT_RETRY_DELAY_SECONDS,
                    )
                    await asyncio.sleep(_TIMEOUT_RETRY_DELAY_SECONDS)
                    retry_t0 = time.monotonic()
                    try:
                        await endpoint.rate_limiter.acquire()
                        response = await endpoint.client.responses.create(**payload)
                        endpoint.same_task_retry_success += 1
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=True,
                            error_kind=None,
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.info(
                            "same_task_retry_success endpoint[%d] key=%s op=responses reason=timeout",
                            endpoint.index,
                            endpoint.key_id,
                        )
                        return endpoint, response
                    except _RETRIABLE_EXCEPTIONS as retry_exc:
                        endpoint.same_task_retry_failed += 1
                        last_exc = retry_exc
                        retry_kind = self._classify_error_kind(retry_exc)
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=False,
                            error_kind=retry_kind,
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.warning(
                            "same_task_retry_failed endpoint[%d] key=%s op=responses reason=timeout kind=%s, trying next endpoint: %s",
                            endpoint.index,
                            endpoint.key_id,
                            retry_kind,
                            retry_exc,
                        )
                        continue
                    except Exception:
                        endpoint.same_task_retry_failed += 1
                        await self._record_endpoint_outcome(
                            endpoint=endpoint,
                            ok=False,
                            error_kind="other",
                            latency_ms=int((time.monotonic() - retry_t0) * 1000),
                        )
                        logger.warning(
                            "same_task_retry_failed endpoint[%d] key=%s op=responses reason=timeout kind=other",
                            endpoint.index,
                            endpoint.key_id,
                        )
                        raise

                last_exc = exc
                error_kind = self._classify_error_kind(exc)
                await self._record_endpoint_outcome(
                    endpoint=endpoint,
                    ok=False,
                    error_kind=error_kind,
                    latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                )
                logger.warning(
                    "LLM responses call failed on endpoint[%d] key=%s kind=%s, trying next endpoint: %s",
                    endpoint.index,
                    endpoint.key_id,
                    error_kind,
                    exc,
                )
                continue
            except Exception as exc:
                await self._record_endpoint_outcome(
                    endpoint=endpoint,
                    ok=False,
                    error_kind="other",
                    latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                )
                raise

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("No available LLM endpoint")

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
            prompt_tokens = (
                getattr(usage, "prompt_tokens", None)
                or getattr(usage, "input_tokens", 0)
                or 0
            )
            completion_tokens = (
                getattr(usage, "completion_tokens", None)
                or getattr(usage, "output_tokens", 0)
                or 0
            )
            total_tokens = getattr(usage, "total_tokens", 0) or (
                prompt_tokens + completion_tokens
            )

        caller_with_suffix = self._with_caller_suffix(caller)

        await record_usage(
            model=self._model,
            provider="zai",
            caller=caller_with_suffix,
            method=method,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            request_id=request_id,
            error=error,
            latency_ms=latency_ms,
        )

    @staticmethod
    def _with_caller_suffix(caller: str) -> str:
        suffix = _CALLER_SUFFIX.get()
        if not suffix:
            return caller
        marker = f"#{suffix}"
        if caller.endswith(marker):
            return caller
        return f"{caller}{marker}"

    @staticmethod
    def set_caller_suffix(suffix: str | None) -> Token[str | None]:
        """Set a temporary suffix to tag tracked callers in this async context."""
        normalized = (suffix or "").strip() or None
        return _CALLER_SUFFIX.set(normalized)

    @staticmethod
    def reset_caller_suffix(token: Token[str | None]) -> None:
        _CALLER_SUFFIX.reset(token)

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
        logger.debug(
            "LLM complete  model=%s  msgs=%d  temp=%.2f  caller=%s",
            self._model, len(messages), temperature, caller,
        )
        t0 = time.monotonic()
        error_msg = None
        usage = None
        request_id = None

        try:
            endpoint, response = await self._chat_completion_with_failover(
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            usage = response.usage
            request_id = getattr(response, "id", None)
            text = response.choices[0].message.content or ""
            logger.debug(
                "LLM complete  endpoint=%d  tokens=%s",
                endpoint.index,
                usage,
            )
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
            response_format = _build_json_schema_response_format(
                schema=schema,
                name="scholarpath_complete_json",
            )
            chat_kwargs: dict[str, Any] = {
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "response_format": response_format,
            }
            if self._chat_json_transport_hints_enabled:
                chat_kwargs["json_transport_hints"] = True

            if self._xcode_stream_json_mode:
                endpoint, stream_response = await self._chat_completion_with_failover(
                    **chat_kwargs,
                    stream=True,
                )
                raw = await _collect_stream_text(stream_response)
            else:
                endpoint, response = await self._chat_completion_with_failover(**chat_kwargs)
                usage = response.usage
                request_id = getattr(response, "id", None)
                raw = response.choices[0].message.content or "{}"
            logger.debug(
                "LLM complete_json  endpoint=%d  tokens=%s",
                endpoint.index,
                usage,
            )
            parsed = _parse_json_object(raw)
            if not parsed and (raw or "").strip():
                logger.warning(
                    "LLM complete_json received non-JSON payload, returning empty dict. caller=%s",
                    caller,
                )
            return parsed
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
        logger.debug(
            "LLM stream  model=%s  msgs=%d  temp=%.2f  caller=%s",
            self._model, len(messages), temperature, caller,
        )
        t0 = time.monotonic()
        total_chunks = 0
        error_msg = None
        last_exc: Exception | None = None

        try:
            for endpoint in await self._ordered_endpoints():
                attempt_t0 = time.monotonic()
                try:
                    if endpoint.cooldown_until > time.monotonic():
                        await asyncio.sleep(
                            min(max(endpoint.cooldown_until - time.monotonic(), 0.0), 0.2),
                        )
                    await endpoint.rate_limiter.acquire()
                    response = await endpoint.client.chat.completions.create(
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
                    await self._record_endpoint_outcome(
                        endpoint=endpoint,
                        ok=True,
                        error_kind=None,
                        latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                    )
                    return
                except _RETRIABLE_EXCEPTIONS as exc:
                    last_exc = exc
                    error_kind = self._classify_error_kind(exc)
                    await self._record_endpoint_outcome(
                        endpoint=endpoint,
                        ok=False,
                        error_kind=error_kind,
                        latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                    )
                    logger.warning(
                        "LLM stream failed on endpoint[%d] key=%s kind=%s, trying next endpoint: %s",
                        endpoint.index,
                        endpoint.key_id,
                        error_kind,
                        exc,
                    )
                    continue
                except Exception as exc:
                    await self._record_endpoint_outcome(
                        endpoint=endpoint,
                        ok=False,
                        error_kind="other",
                        latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                    )
                    raise
            if last_exc is not None:
                raise last_exc
            raise RuntimeError("No available LLM endpoint")
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

    @_RETRY
    async def complete_json_with_web_search(
        self,
        *,
        prompt: str,
        temperature: float = 0.1,
        max_output_tokens: int = 512,
        caller: str = "unknown",
    ) -> dict[str, Any]:
        """Return parsed JSON using the model's built-in web_search tool."""
        logger.debug(
            "LLM complete_json_with_web_search model=%s caller=%s",
            self._model,
            caller,
        )
        t0 = time.monotonic()
        error_msg = None
        usage = None
        request_id = None

        try:
            endpoint, response = await self._responses_with_failover(
                input_text=prompt,
                tools=[{"type": "web_search"}],
                temperature=temperature,
                max_output_tokens=max_output_tokens,
            )
            usage = getattr(response, "usage", None)
            request_id = getattr(response, "id", None)
            raw_text = _response_output_text(response)
            logger.debug(
                "LLM complete_json_with_web_search endpoint=%d usage=%s",
                endpoint.index,
                usage,
            )
            return _parse_json_object(raw_text)
        except Exception as exc:
            error_msg = str(exc)
            raise
        finally:
            latency_ms = int((time.monotonic() - t0) * 1000)
            await self._track(
                method="complete_json_with_web_search",
                caller=caller,
                usage=usage,
                request_id=request_id,
                error=error_msg,
                latency_ms=latency_ms,
            )

    def endpoint_stats(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        return [
            {
                "index": endpoint.index,
                "key_id": endpoint.key_id,
                "sent_requests": endpoint.sent_requests,
                "error_requests": endpoint.error_requests,
                "rate_limit_errors": endpoint.rate_limit_errors,
                "timeout_errors": endpoint.timeout_errors,
                "same_task_retry_triggered": endpoint.same_task_retry_triggered,
                "same_task_retry_success": endpoint.same_task_retry_success,
                "same_task_retry_failed": endpoint.same_task_retry_failed,
                "cooldown_active": endpoint.cooldown_until > now,
            }
            for endpoint in self._endpoints
        ]

    async def endpoint_health(self, *, window_seconds: int = 60) -> dict[str, Any]:
        window = max(1, int(window_seconds))
        now = time.monotonic()
        base_rows: list[dict[str, Any]] = []
        for endpoint in self._endpoints:
            requests_total = int(endpoint.sent_requests + endpoint.error_requests)
            latency_total = 0.0
            base_rows.append(
                {
                    "index": endpoint.index,
                    "key_id": endpoint.key_id,
                    "requests_total": requests_total,
                    "errors_total": int(endpoint.error_requests),
                    "rate_limit_total": int(endpoint.rate_limit_errors),
                    "timeout_total": int(endpoint.timeout_errors),
                    "same_task_retry_triggered": int(endpoint.same_task_retry_triggered),
                    "same_task_retry_success": int(endpoint.same_task_retry_success),
                    "same_task_retry_failed": int(endpoint.same_task_retry_failed),
                    "requests_window": 0.0,
                    "errors_window": 0.0,
                    "rate_limit_window": 0.0,
                    "timeout_window": 0.0,
                    "latency_ms_avg": (
                        round(latency_total / requests_total, 2)
                        if requests_total > 0
                        else 0.0
                    ),
                    "cooldown_active": endpoint.cooldown_until > now,
                },
            )

        if self._observer is None:
            return {
                "window_seconds": window,
                "observer_enabled": False,
                "observer_error": self._observer_error,
                "endpoints": base_rows,
            }

        try:
            snapshots = await self._observer.snapshot(
                key_ids=[endpoint.key_id for endpoint in self._endpoints],
                window_seconds=window,
            )
            for row in base_rows:
                raw = snapshots.get(str(row["key_id"]), {})
                requests_total = int(raw.get("requests_total", row["requests_total"]) or row["requests_total"])
                errors_total = int(raw.get("errors_total", row["errors_total"]) or row["errors_total"])
                rate_limit_total = int(raw.get("rate_limit_total", row["rate_limit_total"]) or row["rate_limit_total"])
                timeout_total = int(raw.get("timeout_total", row["timeout_total"]) or row["timeout_total"])
                latency_total = float(raw.get("latency_ms_total", 0.0) or 0.0)
                row.update(
                    {
                        "requests_total": requests_total,
                        "errors_total": errors_total,
                        "rate_limit_total": rate_limit_total,
                        "timeout_total": timeout_total,
                        "requests_window": float(raw.get("requests_window", 0.0) or 0.0),
                        "errors_window": float(raw.get("errors_window", 0.0) or 0.0),
                        "rate_limit_window": float(raw.get("rate_limit_window", 0.0) or 0.0),
                        "timeout_window": float(raw.get("timeout_window", 0.0) or 0.0),
                        "latency_ms_avg": (
                            round(latency_total / requests_total, 2)
                            if requests_total > 0
                            else 0.0
                        ),
                    },
                )
            return {
                "window_seconds": window,
                "observer_enabled": True,
                "observer_error": None,
                "endpoints": base_rows,
            }
        except Exception as exc:  # pragma: no cover - runtime/env specific
            return {
                "window_seconds": window,
                "observer_enabled": False,
                "observer_error": str(exc),
                "endpoints": base_rows,
            }


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


def _default_json_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "_": {
                "type": "string",
                "description": "Optional placeholder. Real output can use any keys.",
            },
        },
        "additionalProperties": True,
    }


def _build_json_schema_response_format(
    *,
    schema: dict[str, Any] | None,
    name: str,
) -> dict[str, Any]:
    effective_schema = _normalize_json_schema(schema)
    return {
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "strict": False,
            "schema": effective_schema,
        },
    }


def _normalize_json_schema(schema: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(schema, dict) or not schema:
        return _default_json_schema()

    normalized: dict[str, Any] = dict(schema)
    if normalized.get("type") != "object":
        logger.warning(
            "Invalid JSON schema received (type=%r), fallback to default object schema",
            normalized.get("type"),
        )
        return _default_json_schema()

    properties = normalized.get("properties")
    if not isinstance(properties, dict) or not properties:
        normalized["properties"] = _default_json_schema().get("properties", {})
    normalized.setdefault("additionalProperties", True)
    return normalized


async def _collect_stream_text(stream_response: Any) -> str:
    chunks: list[str] = []
    async for chunk in stream_response:
        choices = getattr(chunk, "choices", None) or []
        if not choices:
            continue
        delta = getattr(choices[0], "delta", None)
        if delta is None:
            continue
        content = getattr(delta, "content", None)
        if isinstance(content, str):
            chunks.append(content)
            continue
        if isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    text = part.get("text")
                    if isinstance(text, str):
                        chunks.append(text)
    return "".join(chunks).strip()


def _response_output_text(response: Any) -> str:
    text = getattr(response, "output_text", None)
    if text:
        return str(text)

    output = getattr(response, "output", None)
    if not output:
        return "{}"

    chunks: list[str] = []
    for item in output:
        item_type = getattr(item, "type", None)
        if item_type != "message":
            continue
        content = getattr(item, "content", None) or []
        for part in content:
            part_type = getattr(part, "type", None)
            if part_type in {"output_text", "text"}:
                value = getattr(part, "text", None)
                if value:
                    chunks.append(str(value))
    return "\n".join(chunks) if chunks else "{}"


def _parse_json_object(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            return {"data": data}
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
    if match:
        try:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {"data": parsed}
        except json.JSONDecodeError:
            return {}
    return {}


def _api_key_fingerprint(api_key: str) -> str:
    raw = (api_key or "").strip()
    if not raw:
        return "empty"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


# ------------------------------------------------------------------
# Factory
# ------------------------------------------------------------------

_singleton: LLMClient | None = None


def get_llm_client() -> LLMClient:
    """Return a module-level singleton :class:`LLMClient` configured from settings."""
    global _singleton  # noqa: PLW0603
    if _singleton is None:
        active_mode = settings.llm_active_mode
        if active_mode is not None:
            api_keys = list(active_mode.api_keys)
            _singleton = LLMClient(
                api_key=api_keys[0],
                api_keys=api_keys,
                base_url=active_mode.base_url,
                model=active_mode.model,
                max_rpm=settings.LLM_RATE_LIMIT_RPM,
            )
            return _singleton

        api_keys = settings.zai_api_keys
        if not api_keys:
            api_keys = [settings.ZAI_API_KEY]
        _singleton = LLMClient(
            api_key=api_keys[0] if api_keys else "",
            api_keys=api_keys,
            base_url=settings.ZAI_BASE_URL,
            model=settings.ZAI_MODEL,
        )
    return _singleton
