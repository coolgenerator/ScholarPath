"""Redis-backed turn trace recorder for chat execution visualization."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from scholarpath.api.models.chat import TurnEvent, TurnResult

TRACE_TTL_SECONDS = 7 * 24 * 60 * 60
MAX_SESSION_TRACES = 50
MAX_TRACE_STEPS = 300


class TurnTraceRecorder:
    """Persist and query turn-level execution traces in Redis."""

    def __init__(
        self,
        redis,
        *,
        ttl_seconds: int = TRACE_TTL_SECONDS,
        max_session_traces: int = MAX_SESSION_TRACES,
        max_trace_steps: int = MAX_TRACE_STEPS,
    ) -> None:
        self._redis = redis
        self._ttl_seconds = ttl_seconds
        self._max_session_traces = max_session_traces
        self._max_trace_steps = max_trace_steps

    @staticmethod
    def _meta_key(trace_id: str) -> str:
        return f"scholarpath:chat:trace:{trace_id}:meta"

    @staticmethod
    def _steps_key(trace_id: str) -> str:
        return f"scholarpath:chat:trace:{trace_id}:steps"

    @staticmethod
    def _session_index_key(session_id: str) -> str:
        return f"scholarpath:chat:session_traces:{session_id}"

    @staticmethod
    def _step_seen_key(trace_id: str) -> str:
        return f"scholarpath:chat:trace:{trace_id}:step_seen"

    async def append_event(
        self,
        *,
        trace_id: str,
        session_id: str,
        student_id: str | None,
        event: TurnEvent,
    ) -> None:
        if self._redis is None:
            return
        now = datetime.now(UTC).isoformat()
        meta = await self._ensure_meta(
            trace_id=trace_id,
            session_id=session_id,
            student_id=student_id,
            started_at=event.timestamp.isoformat(),
        )
        payload = event.data if isinstance(event.data, dict) else {}
        step_id = str(payload.get("step_id") or f"evt-{event.event}-{int(event.timestamp.timestamp() * 1000)}")
        event_seq = self._safe_int(payload.get("event_seq"), default=0)
        if event_seq <= 0:
            event_seq = self._safe_int(meta.get("last_event_seq"), default=0) + 1
        duplicate = await self._is_duplicate_step_event(
            trace_id=trace_id,
            step_id=step_id,
            event_seq=event_seq,
        )
        if duplicate:
            return
        step = {
            "trace_id": trace_id,
            "event": event.event,
            "timestamp": event.timestamp.isoformat(),
            "step_id": step_id,
            "parent_step_id": payload.get("parent_step_id"),
            "step_kind": payload.get("step_kind"),
            "step_status": self._normalize_step_status(payload.get("step_status")),
            "phase": payload.get("phase"),
            "wave_index": payload.get("wave_index"),
            "capability_id": payload.get("capability_id"),
            "duration_ms": payload.get("duration_ms"),
            "checkpoint_summary": payload.get("checkpoint_summary"),
            "compact_reason_code": payload.get("compact_reason_code"),
            "event_seq": event_seq,
            "display": payload.get("display"),
            "metrics": payload.get("metrics"),
            "data": payload,
        }
        await self._redis.rpush(self._steps_key(trace_id), json.dumps(step, ensure_ascii=False))
        await self._redis.ltrim(self._steps_key(trace_id), -self._max_trace_steps, -1)
        await self._redis.expire(self._steps_key(trace_id), self._ttl_seconds)

        meta["step_count"] = min(int(meta.get("step_count", 0)) + 1, self._max_trace_steps)
        meta["last_event_seq"] = max(self._safe_int(meta.get("last_event_seq"), default=0), event_seq)
        meta["updated_at"] = now
        await self._set_meta(trace_id, meta)

    async def finalize_result(
        self,
        *,
        trace_id: str,
        session_id: str,
        student_id: str | None,
        result: TurnResult,
    ) -> None:
        usage = result.usage if isinstance(result.usage, dict) else {}
        await self.finalize_trace(
            trace_id=trace_id,
            session_id=session_id,
            student_id=student_id,
            status=result.status,
            usage=usage,
        )

    async def finalize_trace(
        self,
        *,
        trace_id: str,
        session_id: str,
        student_id: str | None,
        status: str,
        usage: dict[str, Any] | None = None,
    ) -> None:
        if self._redis is None:
            return
        meta = await self._ensure_meta(
            trace_id=trace_id,
            session_id=session_id,
            student_id=student_id,
            started_at=datetime.now(UTC).isoformat(),
        )
        ended_at = datetime.now(UTC)
        usage_payload = dict(usage or {})
        if "duration_ms" not in usage_payload:
            started_raw = str(meta.get("started_at") or "")
            try:
                started_at = datetime.fromisoformat(started_raw)
                usage_payload["duration_ms"] = max(0, int((ended_at - started_at).total_seconds() * 1000))
            except Exception:
                usage_payload["duration_ms"] = 0
        meta["status"] = "ok" if status == "ok" else "error"
        meta["ended_at"] = ended_at.isoformat()
        meta["usage"] = usage_payload
        meta["updated_at"] = datetime.now(UTC).isoformat()
        await self._set_meta(trace_id, meta)

    async def record_lock_rejection(
        self,
        *,
        trace_id: str,
        session_id: str,
        student_id: str | None,
        usage: dict[str, Any],
    ) -> None:
        if self._redis is None:
            return
        now = datetime.now(UTC).isoformat()
        step = {
            "trace_id": trace_id,
            "event": "rollback",
            "timestamp": now,
            "step_id": f"step-lock-reject-{trace_id}",
            "parent_step_id": f"turn-{trace_id}",
            "step_kind": "rollback",
            "step_status": "failed",
            "phase": "lock",
            "wave_index": None,
            "capability_id": None,
            "duration_ms": 0,
            "checkpoint_summary": None,
            "compact_reason_code": "LOCK_REJECTED",
            "event_seq": 1,
            "display": {"title": "Lock Rejected", "badge": "error", "severity": "error"},
            "metrics": {"lock_wait_ms": usage.get("lock_wait_ms", 0)},
            "data": {
                "trace_id": trace_id,
                "compact_reason_code": "LOCK_REJECTED",
                "display": {"title": "Lock Rejected", "badge": "error", "severity": "error"},
            },
        }
        meta = {
            "trace_id": trace_id,
            "session_id": session_id,
            "student_id": student_id,
            "status": "error",
            "started_at": now,
            "ended_at": now,
            "usage": usage,
            "step_count": 1,
            "last_event_seq": 1,
            "updated_at": now,
        }
        await self._set_meta(trace_id, meta)
        await self._append_session_index(session_id=session_id, trace_id=trace_id)
        await self._redis.delete(self._steps_key(trace_id))
        await self._redis.delete(self._step_seen_key(trace_id))
        await self._redis.rpush(self._steps_key(trace_id), json.dumps(step, ensure_ascii=False))
        await self._redis.expire(self._steps_key(trace_id), self._ttl_seconds)
        await self._redis.expire(self._meta_key(trace_id), self._ttl_seconds)

    async def get_trace(self, trace_id: str, *, view: str = "compact") -> dict[str, Any] | None:
        if self._redis is None:
            return None
        meta = await self._get_meta(trace_id)
        if meta is None:
            return None
        raw_steps = await self._redis.lrange(self._steps_key(trace_id), 0, -1)
        steps = [self._safe_json(raw) for raw in raw_steps]
        parsed_steps = self._dedupe_and_repair_steps(
            trace_id=trace_id,
            meta=meta,
            steps=[item for item in steps if isinstance(item, dict)],
        )
        response = dict(meta)
        response["steps"] = [self._render_step_for_view(step=item, view=view) for item in parsed_steps]
        response["step_count"] = len(parsed_steps)
        response["usage"] = self._render_usage_for_view(
            usage=response.get("usage") if isinstance(response.get("usage"), dict) else {},
            view=view,
        )
        return response

    async def list_session_traces(
        self,
        *,
        session_id: str,
        limit: int = MAX_SESSION_TRACES,
        view: str = "compact",
    ) -> dict[str, Any]:
        if self._redis is None:
            return {"items": [], "total": 0}
        bounded_limit = max(1, min(limit, self._max_session_traces))
        raw_ids = await self._redis.lrange(self._session_index_key(session_id), -bounded_limit, -1)
        trace_ids = [str(item) for item in reversed(raw_ids)]
        items: list[dict[str, Any]] = []
        for trace_id in trace_ids:
            meta = await self._get_meta(trace_id)
            if not isinstance(meta, dict):
                continue
            items.append(
                {
                    "trace_id": meta.get("trace_id", trace_id),
                    "session_id": meta.get("session_id", session_id),
                    "student_id": meta.get("student_id"),
                    "status": meta.get("status", "running"),
                    "started_at": meta.get("started_at"),
                    "ended_at": meta.get("ended_at"),
                    "usage": self._render_usage_for_view(
                        usage=meta.get("usage") if isinstance(meta.get("usage"), dict) else {},
                        view=view,
                    ),
                    "step_count": int(meta.get("step_count", 0)),
                }
            )
        return {"items": items, "total": len(items)}

    async def _ensure_meta(
        self,
        *,
        trace_id: str,
        session_id: str,
        student_id: str | None,
        started_at: str,
    ) -> dict[str, Any]:
        existing = await self._get_meta(trace_id)
        if isinstance(existing, dict):
            return existing
        now = datetime.now(UTC).isoformat()
        meta = {
            "trace_id": trace_id,
            "session_id": session_id,
            "student_id": student_id,
            "status": "running",
            "started_at": started_at,
            "ended_at": None,
            "usage": {},
            "step_count": 0,
            "last_event_seq": 0,
            "updated_at": now,
        }
        await self._set_meta(trace_id, meta)
        await self._append_session_index(session_id=session_id, trace_id=trace_id)
        return meta

    async def _append_session_index(self, *, session_id: str, trace_id: str) -> None:
        index_key = self._session_index_key(session_id)
        await self._redis.rpush(index_key, trace_id)
        await self._redis.ltrim(index_key, -self._max_session_traces, -1)
        await self._redis.expire(index_key, self._ttl_seconds)

    async def _get_meta(self, trace_id: str) -> dict[str, Any] | None:
        raw = await self._redis.get(self._meta_key(trace_id))
        parsed = self._safe_json(raw)
        return parsed if isinstance(parsed, dict) else None

    async def _set_meta(self, trace_id: str, meta: dict[str, Any]) -> None:
        await self._redis.set(self._meta_key(trace_id), json.dumps(meta, ensure_ascii=False))
        await self._redis.expire(self._meta_key(trace_id), self._ttl_seconds)

    async def _is_duplicate_step_event(
        self,
        *,
        trace_id: str,
        step_id: str,
        event_seq: int,
    ) -> bool:
        marker = f"{step_id}:{event_seq}"
        seen_key = self._step_seen_key(trace_id)
        sadd_fn = getattr(self._redis, "sadd", None)
        if callable(sadd_fn):
            added = await sadd_fn(seen_key, marker)
            await self._redis.expire(seen_key, self._ttl_seconds)
            return int(added) == 0

        # Fallback path for test doubles without set ops.
        raw_steps = await self._redis.lrange(self._steps_key(trace_id), 0, -1)
        for raw in raw_steps:
            parsed = self._safe_json(raw)
            if not isinstance(parsed, dict):
                continue
            existing_step_id = str(parsed.get("step_id", ""))
            existing_seq = self._safe_int(parsed.get("event_seq"), default=0)
            if existing_step_id == step_id and existing_seq == event_seq:
                return True
        return False

    @staticmethod
    def _normalize_step_status(value: Any) -> str | None:
        status = str(value).strip().lower() if value is not None else ""
        if not status:
            return None
        valid = {
            "queued",
            "running",
            "completed",
            "failed",
            "blocked",
            "cancelled",
            "timeout",
            "retrying",
        }
        if status in valid:
            return status
        if status in {"noop", "changed", "ok", "success"}:
            return "completed"
        if status in {"error"}:
            return "failed"
        return "failed"

    def _dedupe_and_repair_steps(
        self,
        *,
        trace_id: str,
        meta: dict[str, Any],
        steps: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        latest_by_step: dict[str, tuple[int, int, dict[str, Any]]] = {}
        for idx, raw_step in enumerate(steps):
            step = dict(raw_step)
            step_id = str(step.get("step_id") or "")
            if not step_id:
                continue
            event_seq = self._safe_int(step.get("event_seq"), default=idx + 1)
            step["event_seq"] = event_seq
            step["step_status"] = self._normalize_step_status(step.get("step_status"))
            current = latest_by_step.get(step_id)
            if current is None or event_seq > current[0] or (event_seq == current[0] and idx > current[1]):
                latest_by_step[step_id] = (event_seq, idx, step)

        ordered_steps = [
            item[2] for item in sorted(latest_by_step.values(), key=lambda row: (row[0], row[1]))
        ]
        root_step_id = f"turn-{trace_id}"
        has_turn = any(str(step.get("step_kind")) == "turn" for step in ordered_steps)
        if not has_turn:
            ordered_steps.insert(
                0,
                {
                    "trace_id": trace_id,
                    "event": "turn_started",
                    "timestamp": str(meta.get("started_at") or datetime.now(UTC).isoformat()),
                    "step_id": root_step_id,
                    "parent_step_id": None,
                    "step_kind": "turn",
                    "step_status": "running" if meta.get("status") == "running" else "completed",
                    "phase": "lifecycle",
                    "wave_index": None,
                    "capability_id": None,
                    "duration_ms": self._safe_int(
                        (meta.get("usage") or {}).get("duration_ms") if isinstance(meta.get("usage"), dict) else 0,
                        default=0,
                    ),
                    "checkpoint_summary": None,
                    "compact_reason_code": None,
                    "event_seq": 0,
                    "display": {"title": "Turn", "badge": "turn", "severity": "info"},
                    "metrics": None,
                    "data": None,
                },
            )
        else:
            for step in ordered_steps:
                if str(step.get("step_kind")) == "turn":
                    root_step_id = str(step.get("step_id") or root_step_id)
                    break

        existing_ids = {str(step.get("step_id")) for step in ordered_steps}
        for step in ordered_steps:
            if str(step.get("step_kind")) == "turn":
                step["parent_step_id"] = None
                continue
            parent = step.get("parent_step_id")
            if not parent or str(parent) not in existing_ids:
                step["parent_step_id"] = root_step_id
        return ordered_steps[-self._max_trace_steps :]

    @staticmethod
    def _render_step_for_view(*, step: dict[str, Any], view: str) -> dict[str, Any]:
        if view == "full":
            return step
        compact = dict(step)
        compact.pop("data", None)
        compact.pop("metrics", None)
        return compact

    @staticmethod
    def _render_usage_for_view(*, usage: dict[str, Any], view: str) -> dict[str, Any]:
        if view == "full":
            return dict(usage)
        allow_keys = {
            "tool_steps_used",
            "tool_step_budget",
            "wave_count",
            "rejected_by_lock",
            "guardrail_triggered",
            "duration_ms",
            "input_compacted",
            "context_compacted",
            "output_compacted",
            "compression_passes",
            "context_chars",
            "lock_scope",
            "lock_wait_ms",
        }
        compact: dict[str, Any] = {}
        for key, value in usage.items():
            if key in allow_keys:
                compact[key] = value
        return compact

    @staticmethod
    def _safe_int(value: Any, *, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _safe_json(raw: Any) -> Any:
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        if not isinstance(raw, str):
            return raw
        try:
            return json.loads(raw)
        except Exception:
            return None
