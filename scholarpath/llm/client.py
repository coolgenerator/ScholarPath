"""LLM client using the OpenAI-compatible API with token usage tracking and rate limiting."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
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

from scholarpath.config import LLMGatewayPolicyConfig, ResolvedLLMEndpointConfig, settings

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
    endpoint_id: str
    model: str
    key_id: str
    client: openai.AsyncOpenAI
    rate_limiter: _AcquireLimiter
    stream_json_default: bool
    json_transport_hints_default: bool
    sent_requests: int = 0
    error_requests: int = 0
    rate_limit_errors: int = 0
    timeout_errors: int = 0
    cooldown_until: float = 0.0
    same_task_retry_triggered: int = 0
    same_task_retry_success: int = 0
    same_task_retry_failed: int = 0
    preferred_route_hits: int = 0
    required_output_missing: int = 0
    parse_fail: int = 0
    non_json: int = 0
    schema_mismatch: int = 0
    policy_applied_counts_by_method: dict[str, int] = field(default_factory=dict)


class LLMClient:
    """Policy-driven multi-endpoint LLM gateway client."""

    def __init__(
        self,
        *,
        mode_name: str,
        policy_name: str,
        policy: LLMGatewayPolicyConfig,
        endpoints: list[ResolvedLLMEndpointConfig],
    ) -> None:
        self._mode_name = mode_name
        self._policy_name = policy_name
        self._policy = policy
        self._default_model = endpoints[0].model if endpoints else ""
        self._request_timeout_seconds = max(
            1.0,
            float(getattr(settings, "LLM_REQUEST_TIMEOUT_SECONDS", 4.5) or 4.5),
        )
        if not endpoints:
            raise RuntimeError("No LLM endpoints configured for active mode")

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
        for i, endpoint_cfg in enumerate(endpoints):
            key_id = _api_key_fingerprint(endpoint_cfg.api_key)
            base_url_lower = endpoint_cfg.base_url.strip().lower()
            stream_json_default = (
                "api.xcode.best" in base_url_lower
                or "beecode.cc" in base_url_lower
            )
            json_transport_hints_default = "beecode.cc" not in base_url_lower
            default_headers: dict[str, str] | None = None
            if "beecode.cc" in base_url_lower:
                default_headers = {
                    "User-Agent": "scholarpath-llm-client/1.0",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                }

            client_kwargs: dict[str, Any] = {
                "api_key": endpoint_cfg.api_key,
                "base_url": endpoint_cfg.base_url,
            }
            if default_headers is not None:
                client_kwargs["default_headers"] = default_headers
            self._endpoints.append(
                _Endpoint(
                    index=i,
                    endpoint_id=endpoint_cfg.endpoint_id,
                    model=endpoint_cfg.model,
                    key_id=key_id,
                    client=openai.AsyncOpenAI(**client_kwargs),
                    rate_limiter=_SmartRateLimiter(
                        max_rpm=max(int(endpoint_cfg.rpm), 1),
                        redis_url=redis_url,
                        endpoint_label=f"endpoint[{i}]/{endpoint_cfg.endpoint_id}",
                        api_key_fingerprint=key_id,
                    ),
                    stream_json_default=stream_json_default,
                    json_transport_hints_default=json_transport_hints_default,
                ),
            )
        self._rr_cursor = 0
        self._rr_lock = asyncio.Lock()

    def _is_strict_json_caller(self, caller: str) -> bool:
        return caller in self._policy.strict_json_callers

    async def _ordered_endpoints(self, *, caller: str | None = None) -> list[_Endpoint]:
        if not self._endpoints:
            raise RuntimeError("No LLM endpoints configured")
        async with self._rr_lock:
            start = self._rr_cursor
            self._rr_cursor = (self._rr_cursor + 1) % len(self._endpoints)
        ordered = [
            self._endpoints[(start + i) % len(self._endpoints)]
            for i in range(len(self._endpoints))
        ]
        preferred_endpoint_id = self._policy.route.get((caller or "").strip())
        if preferred_endpoint_id:
            preferred = next(
                (endpoint for endpoint in ordered if endpoint.endpoint_id == preferred_endpoint_id),
                None,
            )
            if preferred is not None:
                preferred.preferred_route_hits += 1
                ordered = [preferred] + [
                    endpoint
                    for endpoint in ordered
                    if endpoint.endpoint_id != preferred_endpoint_id
                ]

        now = time.monotonic()
        ready = [endpoint for endpoint in ordered if endpoint.cooldown_until <= now]
        cooling = [endpoint for endpoint in ordered if endpoint.cooldown_until > now]
        return ready + cooling

    def _resolve_method_policy(
        self,
        *,
        method: str,
        caller: str,
        endpoint: _Endpoint,
    ) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        defaults = self._policy.call_defaults.get(method, {})
        endpoint_overrides = (
            self._policy.endpoint_overrides.get(endpoint.endpoint_id, {}).get(method, {})
        )
        caller_overrides = (
            self._policy.caller_overrides.get(caller, {}).get(method, {})
        )
        _deep_merge_dict(merged, defaults)
        _deep_merge_dict(merged, endpoint_overrides)
        _deep_merge_dict(merged, caller_overrides)
        endpoint.policy_applied_counts_by_method[method] = (
            endpoint.policy_applied_counts_by_method.get(method, 0) + 1
        )
        return merged

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
        method: str,
        caller: str,
        payload_builder: Any,
    ) -> tuple[_Endpoint, Any, dict[str, Any], dict[str, Any]]:
        last_exc: Exception | None = None

        for endpoint in await self._ordered_endpoints(caller=caller):
            attempt_t0 = time.monotonic()
            method_policy = self._resolve_method_policy(
                method=method,
                caller=caller,
                endpoint=endpoint,
            )
            payload: dict[str, Any] = payload_builder(endpoint, method_policy)
            try:
                if endpoint.cooldown_until > time.monotonic():
                    await asyncio.sleep(
                        min(max(endpoint.cooldown_until - time.monotonic(), 0.0), 0.2),
                    )
                await endpoint.rate_limiter.acquire()
                response = await endpoint.client.chat.completions.create(**payload)
                await self._record_endpoint_outcome(
                    endpoint=endpoint,
                    ok=True,
                    error_kind=None,
                    latency_ms=int((time.monotonic() - attempt_t0) * 1000),
                )
                return endpoint, response, method_policy, payload
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
                        return endpoint, response, method_policy, payload
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
                        return endpoint, response, method_policy, payload
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

    async def _track(
        self,
        *,
        method: str,
        caller: str,
        model: str | None,
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
            model=model or self._default_model,
            provider=self._mode_name,
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
        caller = _require_caller(caller=caller, method="complete")
        logger.debug(
            "LLM complete  model=%s  msgs=%d  temp=%.2f  caller=%s",
            self._default_model, len(messages), temperature, caller,
        )
        t0 = time.monotonic()
        error_msg = None
        usage = None
        request_id = None
        model_used: str | None = None

        try:
            def _payload_builder(endpoint: _Endpoint, _method_policy: dict[str, Any]) -> dict[str, Any]:
                return {
                    "model": endpoint.model,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                    "stream": False,
                    "timeout": self._request_timeout_seconds,
                }

            endpoint, response, _method_policy, _payload = await self._chat_completion_with_failover(
                method="complete",
                caller=caller,
                payload_builder=_payload_builder,
            )
            usage = response.usage
            request_id = getattr(response, "id", None)
            model_used = endpoint.model
            text = response.choices[0].message.content or ""
            logger.debug(
                "LLM complete endpoint=%d endpoint_id=%s tokens=%s",
                endpoint.index,
                endpoint.endpoint_id,
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
                model=model_used,
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
        caller = _require_caller(caller=caller, method="complete_json")
        logger.debug(
            "LLM complete_json  model=%s  msgs=%d  temp=%.2f  caller=%s",
            self._default_model, len(messages), temperature, caller,
        )
        t0 = time.monotonic()
        error_msg = None
        usage = None
        request_id = None
        model_used: str | None = None
        strict_json_caller = self._is_strict_json_caller(caller)

        try:
            def _payload_builder(endpoint: _Endpoint, method_policy: dict[str, Any]) -> dict[str, Any]:
                response_type = _method_policy_response_format_type(method_policy)
                if strict_json_caller:
                    response_type = "json_schema"
                response_format: dict[str, Any] | None = None
                if response_type == "json_schema":
                    schema_name = _method_policy_json_schema_name(
                        method_policy,
                        default=f"{caller.replace('.', '_')}_json",
                    )
                    schema_strict = _method_policy_json_schema_strict(
                        method_policy,
                        default=strict_json_caller,
                    )
                    if strict_json_caller:
                        schema_strict = True
                    response_format = _build_json_schema_response_format(
                        schema=schema,
                        name=schema_name,
                        strict=schema_strict,
                    )
                elif response_type == "json_object":
                    response_format = {"type": "json_object"}

                stream_json_enabled = _method_policy_bool(
                    method_policy,
                    "stream_json_enabled",
                    default=endpoint.stream_json_default,
                )
                schema_hint_enabled = _method_policy_bool(
                    method_policy,
                    "schema_hint_enabled",
                    default=True,
                )
                json_transport_hints_enabled = _method_policy_bool(
                    method_policy,
                    "json_transport_hints_enabled",
                    default=endpoint.json_transport_hints_default,
                )

                payload_messages = messages
                if schema is not None and schema_hint_enabled:
                    schema_instruction = (
                        "\n\nYou MUST respond with valid JSON matching this schema:\n"
                        f"```json\n{json.dumps(schema, ensure_ascii=False, indent=2)}\n```"
                    )
                    payload_messages = _inject_schema_hint(messages, schema_instruction)

                payload: dict[str, Any] = {
                    "model": endpoint.model,
                    "messages": payload_messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                    "stream": stream_json_enabled,
                    "timeout": self._request_timeout_seconds,
                }
                if response_format is not None:
                    payload["response_format"] = response_format
                if json_transport_hints_enabled:
                    payload["extra_headers"] = {
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    }
                    payload["extra_body"] = {
                        "response_type": "json",
                        "content_type": "application/json",
                    }
                return payload

            endpoint, response, method_policy, payload = await self._chat_completion_with_failover(
                method="complete_json",
                caller=caller,
                payload_builder=_payload_builder,
            )
            model_used = endpoint.model

            if bool(payload.get("stream")):
                raw = await _collect_stream_text(response)
            else:
                usage = getattr(response, "usage", None)
                request_id = getattr(response, "id", None)
                raw = response.choices[0].message.content or "{}"
            logger.debug(
                "LLM complete_json endpoint=%d endpoint_id=%s tokens=%s",
                endpoint.index,
                endpoint.endpoint_id,
                usage,
            )
            parse_mode = "strict" if strict_json_caller else _method_policy_parse_mode(method_policy)
            parsed = _parse_json_with_mode(raw, parse_mode=parse_mode)
            if not parsed and (raw or "").strip():
                endpoint.non_json += 1
                endpoint.parse_fail += 1
                logger.warning(
                    "LLM complete_json non-JSON payload caller=%s parse_mode=%s",
                    caller,
                    parse_mode,
                )
                if strict_json_caller:
                    raise ValueError(
                        f"Strict JSON caller '{caller}' produced non-JSON output",
                    )
            if strict_json_caller:
                mismatch_reason = _json_schema_mismatch_reason(parsed, schema)
                if mismatch_reason:
                    endpoint.schema_mismatch += 1
                    endpoint.parse_fail += 1
                    raise ValueError(
                        f"Strict JSON caller '{caller}' schema mismatch: {mismatch_reason}",
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
                model=model_used,
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
        caller = _require_caller(caller=caller, method="stream")
        logger.debug(
            "LLM stream  model=%s  msgs=%d  temp=%.2f  caller=%s",
            self._default_model, len(messages), temperature, caller,
        )
        t0 = time.monotonic()
        total_chunks = 0
        error_msg = None
        last_exc: Exception | None = None
        model_used: str | None = None

        try:
            for endpoint in await self._ordered_endpoints(caller=caller):
                attempt_t0 = time.monotonic()
                self._resolve_method_policy(
                    method="stream",
                    caller=caller,
                    endpoint=endpoint,
                )
                try:
                    if endpoint.cooldown_until > time.monotonic():
                        await asyncio.sleep(
                            min(max(endpoint.cooldown_until - time.monotonic(), 0.0), 0.2),
                        )
                    await endpoint.rate_limiter.acquire()
                    model_used = endpoint.model
                    response = await endpoint.client.chat.completions.create(
                        model=endpoint.model,
                        messages=messages,
                        temperature=temperature,
                        max_tokens=max_tokens,
                        stream=True,
                        timeout=self._request_timeout_seconds,
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
                model=model_used,
                usage=None,
                error=error_msg,
                latency_ms=latency_ms,
            )

    def endpoint_stats(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        return [
            {
                "index": endpoint.index,
                "endpoint_id": endpoint.endpoint_id,
                "key_id": endpoint.key_id,
                "sent_requests": endpoint.sent_requests,
                "error_requests": endpoint.error_requests,
                "rate_limit_errors": endpoint.rate_limit_errors,
                "timeout_errors": endpoint.timeout_errors,
                "same_task_retry_triggered": endpoint.same_task_retry_triggered,
                "same_task_retry_success": endpoint.same_task_retry_success,
                "same_task_retry_failed": endpoint.same_task_retry_failed,
                "preferred_route_hits": endpoint.preferred_route_hits,
                "policy_applied_counts_by_method": dict(endpoint.policy_applied_counts_by_method),
                "required_output_missing": endpoint.required_output_missing,
                "parse_fail": endpoint.parse_fail,
                "non_json": endpoint.non_json,
                "schema_mismatch": endpoint.schema_mismatch,
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
                    "endpoint_id": endpoint.endpoint_id,
                    "key_id": endpoint.key_id,
                    "requests_total": requests_total,
                    "errors_total": int(endpoint.error_requests),
                    "rate_limit_total": int(endpoint.rate_limit_errors),
                    "timeout_total": int(endpoint.timeout_errors),
                    "same_task_retry_triggered": int(endpoint.same_task_retry_triggered),
                    "same_task_retry_success": int(endpoint.same_task_retry_success),
                    "same_task_retry_failed": int(endpoint.same_task_retry_failed),
                    "preferred_route_hits": int(endpoint.preferred_route_hits),
                    "policy_applied_counts_by_method": dict(endpoint.policy_applied_counts_by_method),
                    "required_output_missing": int(endpoint.required_output_missing),
                    "parse_fail": int(endpoint.parse_fail),
                    "non_json": int(endpoint.non_json),
                    "schema_mismatch": int(endpoint.schema_mismatch),
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
                "active_mode": self._mode_name,
                "active_policy": self._policy_name,
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
                "active_mode": self._mode_name,
                "active_policy": self._policy_name,
                "observer_enabled": True,
                "observer_error": None,
                "endpoints": base_rows,
            }
        except Exception as exc:  # pragma: no cover - runtime/env specific
            return {
                "window_seconds": window,
                "active_mode": self._mode_name,
                "active_policy": self._policy_name,
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


def _require_caller(*, caller: str, method: str) -> str:
    normalized = (caller or "").strip()
    if not normalized or normalized == "unknown":
        raise ValueError(
            f"LLM caller must be explicitly named for {method}. "
            "Using empty/unknown caller is disallowed.",
        )
    return normalized


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
    strict: bool = False,
) -> dict[str, Any]:
    effective_schema = _normalize_json_schema(schema)
    return {
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "strict": bool(strict),
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


def _parse_json_strict(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        return {"data": data}
    return {}


def _parse_fenced_json(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}

    fenced = re.findall(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    for snippet in fenced:
        parsed = _parse_json_strict(snippet)
        if parsed:
            return parsed
    return _parse_json_strict(raw)


def _parse_json_with_mode(text: str, *, parse_mode: str) -> dict[str, Any]:
    mode = (parse_mode or "extract_object").strip().lower()
    if mode == "strict":
        return _parse_json_strict(text)
    if mode == "fenced_json":
        return _parse_fenced_json(text)
    return _parse_json_object(text)


def _json_schema_mismatch_reason(
    payload: dict[str, Any],
    schema: dict[str, Any] | None,
) -> str | None:
    if not isinstance(schema, dict) or not schema:
        return None
    return _validate_schema_node(payload, schema, path="$")


def _validate_schema_node(value: Any, schema: dict[str, Any], *, path: str) -> str | None:
    expected_type = schema.get("type")
    if isinstance(expected_type, str):
        if expected_type == "object":
            if not isinstance(value, dict):
                return f"{path}: expected object"
            required = schema.get("required")
            if isinstance(required, list):
                for key in required:
                    if isinstance(key, str) and key not in value:
                        return f"{path}: missing required key '{key}'"
            properties = schema.get("properties")
            additional = schema.get("additionalProperties", True)
            if isinstance(properties, dict):
                for key, sub_schema in properties.items():
                    if key in value and isinstance(sub_schema, dict):
                        child_reason = _validate_schema_node(
                            value[key],
                            sub_schema,
                            path=f"{path}.{key}",
                        )
                        if child_reason:
                            return child_reason
                if additional is False:
                    for key in value.keys():
                        if key not in properties:
                            return f"{path}: unexpected key '{key}'"
        elif expected_type == "array":
            if not isinstance(value, list):
                return f"{path}: expected array"
            item_schema = schema.get("items")
            if isinstance(item_schema, dict):
                for idx, item in enumerate(value):
                    child_reason = _validate_schema_node(
                        item,
                        item_schema,
                        path=f"{path}[{idx}]",
                    )
                    if child_reason:
                        return child_reason
        elif expected_type == "string":
            if not isinstance(value, str):
                return f"{path}: expected string"
        elif expected_type == "number":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return f"{path}: expected number"
        elif expected_type == "integer":
            if not isinstance(value, int) or isinstance(value, bool):
                return f"{path}: expected integer"
        elif expected_type == "boolean":
            if not isinstance(value, bool):
                return f"{path}: expected boolean"
        elif expected_type == "null":
            if value is not None:
                return f"{path}: expected null"
    return None


def _method_policy_response_format_type(method_policy: dict[str, Any]) -> str:
    response_format = method_policy.get("response_format")
    if isinstance(response_format, dict):
        value = str(response_format.get("type", "")).strip().lower()
        if value in {"json_schema", "json_object", "none"}:
            return value
    return "json_schema"


def _method_policy_json_schema_name(method_policy: dict[str, Any], *, default: str) -> str:
    response_format = method_policy.get("response_format")
    if not isinstance(response_format, dict):
        return default
    payload = response_format.get("json_schema")
    if not isinstance(payload, dict):
        return default
    name = str(payload.get("name", "")).strip()
    return name or default


def _method_policy_json_schema_strict(method_policy: dict[str, Any], *, default: bool) -> bool:
    response_format = method_policy.get("response_format")
    if not isinstance(response_format, dict):
        return default
    payload = response_format.get("json_schema")
    if not isinstance(payload, dict):
        return default
    value = payload.get("strict")
    if isinstance(value, bool):
        return value
    return default


def _method_policy_parse_mode(method_policy: dict[str, Any]) -> str:
    raw = str(method_policy.get("parse_mode", "extract_object")).strip().lower()
    if raw in {"strict", "extract_object", "fenced_json"}:
        return raw
    return "extract_object"


def _method_policy_bool(method_policy: dict[str, Any], key: str, *, default: bool) -> bool:
    value = method_policy.get(key)
    if isinstance(value, bool):
        return value
    return default


def _deep_merge_dict(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    for key, value in incoming.items():
        if (
            key in base
            and isinstance(base[key], dict)
            and isinstance(value, dict)
        ):
            _deep_merge_dict(base[key], value)
        else:
            base[key] = value
    return base


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
        _singleton = LLMClient(
            mode_name=settings.llm_active_mode.name,
            policy_name=settings.llm_active_policy.name,
            policy=settings.llm_active_policy,
            endpoints=list(settings.resolve_active_mode_endpoints()),
        )
    return _singleton
