"""Redis-backed conversation memory for the chat agent."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Literal, Mapping

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# Redis key prefixes
_HISTORY_PREFIX = "scholarpath:chat:history:"
_CONTEXT_PREFIX = "scholarpath:chat:context:"
_STUDENT_CONTEXT_PREFIX = "scholarpath:chat:student_context:"

# Context layers
ContextLayer = Literal["short_term", "working", "long_term"]
_LAYER_ORDER: tuple[ContextLayer, ...] = ("long_term", "working", "short_term")
_LAYER_TTL_SECONDS: dict[ContextLayer, int | None] = {
    # Ephemeral follow-up context.
    "short_term": 6 * 60 * 60,
    # Session-task context that may span multiple user turns.
    "working": 14 * 24 * 60 * 60,
    # Reserved for durable chat-derived traits.
    "long_term": None,
}

# Known key -> layer routing for backwards-compatible save_context calls.
_SHORT_TERM_KEYS = {
    "current_school_id",
    "current_school_name",
    "last_what_if",
    "last_comparison",
    "last_strategy",
    "pending_profile_patch",
}
_WORKING_KEYS = {
    "last_extracted",
    "completed_steps",
    "intake_step",
    "intake_complete",
    "recommendations",
}

# Maximum number of messages retained per session.
_MAX_HISTORY = 50


class ChatMemory:
    """Manages per-session conversation history and layered context in Redis.

    Conversation history is stored as a Redis list (RPUSH / LRANGE).
    Context memory is layered into:
    - ``short_term``: immediate follow-up references.
    - ``working``: active task state across multiple turns.
    - ``long_term``: durable preferences/traits extracted from chat.

    ``save_context`` / ``get_context`` remain backwards-compatible and return a
    merged context view so existing handlers continue to work unchanged.
    """

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    def begin_turn_journal(self) -> "TurnMemoryJournal":
        """Create a turn-scoped staging journal for atomic memory commit."""
        return TurnMemoryJournal(self)

    # ------------------------------------------------------------------
    # Message history
    # ------------------------------------------------------------------

    async def save_message(
        self,
        session_id: str,
        role: str,
        content: str,
        *,
        extras: dict[str, Any] | None = None,
    ) -> None:
        """Append a message to the conversation history."""
        key = f"{_HISTORY_PREFIX}{session_id}"
        entry: dict[str, Any] = {"role": role, "content": content}
        if extras:
            entry.update(extras)
        payload = json.dumps(entry, ensure_ascii=False)
        await self._redis.rpush(key, payload)
        # Trim to keep only the most recent messages.
        await self._redis.ltrim(key, -_MAX_HISTORY, -1)

    async def save_assistant_turn(
        self,
        session_id: str,
        *,
        content: str,
        status: str,
        trace_id: str,
        blocks: list[dict[str, Any]],
        actions: list[str] | None = None,
        execution_digest: dict[str, Any] | None = None,
    ) -> None:
        """Persist a structured assistant turn for rich history replay."""
        await self.save_message(
            session_id,
            "assistant",
            content,
            extras={
                "status": status,
                "trace_id": trace_id,
                "blocks": blocks,
                "actions": actions or [],
                "execution_digest": execution_digest,
            },
        )

    async def get_history(
        self,
        session_id: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Retrieve recent conversation messages."""
        key = f"{_HISTORY_PREFIX}{session_id}"
        raw_items = await self._redis.lrange(key, -limit, -1)
        messages: list[dict[str, Any]] = []
        for item in raw_items:
            try:
                messages.append(json.loads(item))
            except json.JSONDecodeError:
                logger.warning("Corrupt history entry in session %s", session_id)
        return messages

    # ------------------------------------------------------------------
    # Layered context
    # ------------------------------------------------------------------

    async def save_context(
        self,
        session_id: str,
        key: str,
        value: Any,
        *,
        layer: ContextLayer | None = None,
    ) -> None:
        """Store a context value under a specific (or inferred) memory layer."""
        target_layer = layer or self._infer_layer_for_key(key)
        target_key = self._layer_context_key(session_id, target_layer)
        other_layer_keys = [
            self._layer_context_key(session_id, item)
            for item in _LAYER_ORDER
            if item != target_layer
        ]
        legacy_key = self._legacy_context_key(session_id)

        if value is None:
            await self._hdel_many([target_key, *other_layer_keys, legacy_key], key)
            return

        await self._redis.hset(target_key, key, json.dumps(value, ensure_ascii=False))
        await self._hdel_many([*other_layer_keys, legacy_key], key)
        await self._touch_layer_ttl(target_key, target_layer)

    async def save_student_context(
        self,
        student_id: uuid.UUID | str,
        key: str,
        value: Any,
        *,
        layer: ContextLayer = "long_term",
    ) -> None:
        """Store cross-session student memory in a dedicated layered namespace."""
        student_key = self._normalize_student_id(student_id)
        target_key = self._student_layer_context_key(student_key, layer)
        other_layer_keys = [
            self._student_layer_context_key(student_key, item)
            for item in _LAYER_ORDER
            if item != layer
        ]

        if value is None:
            await self._hdel_many([target_key, *other_layer_keys], key)
            return

        await self._redis.hset(target_key, key, json.dumps(value, ensure_ascii=False))
        await self._hdel_many(other_layer_keys, key)
        await self._touch_layer_ttl(target_key, layer)

    async def save_contexts(
        self,
        session_id: str,
        values: Mapping[str, Any],
        *,
        layer: ContextLayer | None = None,
    ) -> None:
        """Store multiple context values in sequence."""
        for key, value in values.items():
            await self.save_context(session_id, key, value, layer=layer)

    async def save_student_contexts(
        self,
        student_id: uuid.UUID | str,
        values: Mapping[str, Any],
        *,
        layer: ContextLayer = "long_term",
    ) -> None:
        """Store multiple cross-session student memory entries."""
        for key, value in values.items():
            await self.save_student_context(student_id, key, value, layer=layer)

    async def get_context(
        self,
        session_id: str,
    ) -> dict[str, Any]:
        """Return merged context (legacy + long_term + working + short_term)."""
        layered = await self.get_context_layers(session_id)
        return layered["merged"]

    async def get_context_layer(
        self,
        session_id: str,
        layer: ContextLayer,
    ) -> dict[str, Any]:
        """Return one specific context layer."""
        redis_key = self._layer_context_key(session_id, layer)
        raw = await self._redis.hgetall(redis_key)
        return self._decode_hash(raw)

    async def get_context_layers(
        self,
        session_id: str,
    ) -> dict[str, dict[str, Any]]:
        """Return all context layers plus a merged view."""
        legacy_key = self._legacy_context_key(session_id)
        keys = [legacy_key, *[self._layer_context_key(session_id, layer) for layer in _LAYER_ORDER]]
        raw_legacy, raw_long, raw_working, raw_short = await asyncio.gather(
            self._redis.hgetall(keys[0]),
            self._redis.hgetall(keys[1]),
            self._redis.hgetall(keys[2]),
            self._redis.hgetall(keys[3]),
        )
        layers = {
            "legacy": self._decode_hash(raw_legacy),
            "long_term": self._decode_hash(raw_long),
            "working": self._decode_hash(raw_working),
            "short_term": self._decode_hash(raw_short),
        }
        merged: dict[str, Any] = dict(layers["legacy"])
        for layer_name in _LAYER_ORDER:
            merged.update(layers[layer_name])
        return {
            "legacy": layers["legacy"],
            "long_term": layers["long_term"],
            "working": layers["working"],
            "short_term": layers["short_term"],
            "merged": merged,
        }

    async def get_student_context(
        self,
        student_id: uuid.UUID | str,
    ) -> dict[str, Any]:
        """Return merged cross-session student memory."""
        layered = await self.get_student_context_layers(student_id)
        return layered["merged"]

    async def get_student_context_layer(
        self,
        student_id: uuid.UUID | str,
        layer: ContextLayer,
    ) -> dict[str, Any]:
        """Return one student memory layer."""
        student_key = self._normalize_student_id(student_id)
        raw = await self._redis.hgetall(self._student_layer_context_key(student_key, layer))
        return self._decode_hash(raw)

    async def get_student_context_layers(
        self,
        student_id: uuid.UUID | str,
    ) -> dict[str, dict[str, Any]]:
        """Return all cross-session student layers plus a merged view."""
        student_key = self._normalize_student_id(student_id)
        keys = [self._student_layer_context_key(student_key, layer) for layer in _LAYER_ORDER]
        raw_long, raw_working, raw_short = await asyncio.gather(
            self._redis.hgetall(keys[0]),
            self._redis.hgetall(keys[1]),
            self._redis.hgetall(keys[2]),
        )
        layers = {
            "long_term": self._decode_hash(raw_long),
            "working": self._decode_hash(raw_working),
            "short_term": self._decode_hash(raw_short),
        }
        merged: dict[str, Any] = {}
        for layer_name in _LAYER_ORDER:
            merged.update(layers[layer_name])
        return {
            "long_term": layers["long_term"],
            "working": layers["working"],
            "short_term": layers["short_term"],
            "merged": merged,
        }

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def clear(self, session_id: str) -> None:
        """Delete all history and context for a session."""
        await self._redis.delete(
            f"{_HISTORY_PREFIX}{session_id}",
            self._legacy_context_key(session_id),
            *[self._layer_context_key(session_id, layer) for layer in _LAYER_ORDER],
        )

    async def clear_student_context(self, student_id: uuid.UUID | str) -> None:
        """Delete all cross-session context for a student."""
        student_key = self._normalize_student_id(student_id)
        await self._redis.delete(
            *[self._student_layer_context_key(student_key, layer) for layer in _LAYER_ORDER],
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _legacy_context_key(session_id: str) -> str:
        return f"{_CONTEXT_PREFIX}{session_id}"

    @staticmethod
    def _layer_context_key(session_id: str, layer: ContextLayer) -> str:
        return f"{_CONTEXT_PREFIX}{session_id}:{layer}"

    @staticmethod
    def _student_layer_context_key(student_id: str, layer: ContextLayer) -> str:
        return f"{_STUDENT_CONTEXT_PREFIX}{student_id}:{layer}"

    @staticmethod
    def _normalize_student_id(student_id: uuid.UUID | str) -> str:
        return str(student_id).strip()

    @staticmethod
    def _infer_layer_for_key(key: str) -> ContextLayer:
        if key in _SHORT_TERM_KEYS:
            return "short_term"
        if key in _WORKING_KEYS:
            return "working"
        # Unknown keys default to working for backwards compatibility.
        return "working"

    @staticmethod
    def _decode_hash(raw: Mapping[Any, Any]) -> dict[str, Any]:
        context: dict[str, Any] = {}
        for raw_key, raw_value in raw.items():
            key = raw_key.decode("utf-8", errors="ignore") if isinstance(raw_key, bytes) else str(raw_key)
            value = (
                raw_value.decode("utf-8", errors="ignore")
                if isinstance(raw_value, bytes)
                else raw_value
            )
            try:
                context[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                context[key] = value
        return context

    async def _hdel_many(self, redis_keys: list[str], field: str) -> None:
        hdel = getattr(self._redis, "hdel", None)
        if not callable(hdel):
            return
        await asyncio.gather(*(hdel(redis_key, field) for redis_key in redis_keys))

    async def _touch_layer_ttl(self, redis_key: str, layer: ContextLayer) -> None:
        ttl_seconds = _LAYER_TTL_SECONDS.get(layer)
        if ttl_seconds is None:
            return
        expire = getattr(self._redis, "expire", None)
        if callable(expire):
            await expire(redis_key, ttl_seconds)


_DELETED = object()


class TurnMemoryJournal:
    """Turn-scoped memory journal with staged writes and atomic commit/discard."""

    def __init__(self, base: ChatMemory) -> None:
        self._base = base
        self._staged_ops: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self._staged_history: dict[str, list[dict[str, Any]]] = {}
        self._staged_session_layers: dict[str, dict[str, dict[str, Any]]] = {}
        self._staged_student_layers: dict[str, dict[str, dict[str, Any]]] = {}
        self._cleared_sessions: set[str] = set()
        self._cleared_students: set[str] = set()
        self._finalized = False

    # ------------------------------------------------------------------
    # Commit/discard lifecycle
    # ------------------------------------------------------------------

    async def commit(self) -> None:
        if self._finalized:
            return
        for method_name, args, kwargs in self._staged_ops:
            method = getattr(self._base, method_name)
            await method(*args, **kwargs)
        self._finalized = True
        self._clear_staged_state()

    async def discard(self) -> None:
        if self._finalized:
            return
        self._finalized = True
        self._clear_staged_state()

    def _clear_staged_state(self) -> None:
        self._staged_ops.clear()
        self._staged_history.clear()
        self._staged_session_layers.clear()
        self._staged_student_layers.clear()
        self._cleared_sessions.clear()
        self._cleared_students.clear()

    # ------------------------------------------------------------------
    # Staged history writes
    # ------------------------------------------------------------------

    async def save_message(
        self,
        session_id: str,
        role: str,
        content: str,
        *,
        extras: dict[str, Any] | None = None,
    ) -> None:
        self._record("save_message", session_id, role, content, extras=extras)
        entry: dict[str, Any] = {"role": role, "content": content}
        if extras:
            entry.update(extras)
        self._staged_history.setdefault(session_id, []).append(entry)

    async def save_assistant_turn(
        self,
        session_id: str,
        *,
        content: str,
        status: str,
        trace_id: str,
        blocks: list[dict[str, Any]],
        actions: list[str] | None = None,
        execution_digest: dict[str, Any] | None = None,
    ) -> None:
        self._record(
            "save_assistant_turn",
            session_id,
            content=content,
            status=status,
            trace_id=trace_id,
            blocks=blocks,
            actions=actions,
            execution_digest=execution_digest,
        )
        self._staged_history.setdefault(session_id, []).append(
            {
                "role": "assistant",
                "content": content,
                "status": status,
                "trace_id": trace_id,
                "blocks": blocks,
                "actions": actions or [],
                "execution_digest": execution_digest,
            }
        )

    async def get_history(self, session_id: str, limit: int = 20) -> list[dict[str, Any]]:
        base_items = [] if session_id in self._cleared_sessions else await self._base.get_history(session_id, limit=50)
        staged = self._staged_history.get(session_id, [])
        merged = [*base_items, *staged]
        if limit <= 0:
            return []
        return merged[-limit:]

    # ------------------------------------------------------------------
    # Staged session context writes
    # ------------------------------------------------------------------

    async def save_context(
        self,
        session_id: str,
        key: str,
        value: Any,
        *,
        layer: ContextLayer | None = None,
    ) -> None:
        self._record("save_context", session_id, key, value, layer=layer)
        target_layer = layer or self._base._infer_layer_for_key(key)
        staged_layers = self._staged_session_layers.setdefault(
            session_id,
            {
                "legacy": {},
                "short_term": {},
                "working": {},
                "long_term": {},
            },
        )
        if value is None:
            staged_layers["legacy"][key] = _DELETED
            staged_layers["short_term"][key] = _DELETED
            staged_layers["working"][key] = _DELETED
            staged_layers["long_term"][key] = _DELETED
            return

        staged_layers["legacy"][key] = _DELETED
        for layer_name in ("short_term", "working", "long_term"):
            staged_layers[layer_name][key] = value if layer_name == target_layer else _DELETED

    async def save_contexts(
        self,
        session_id: str,
        values: Mapping[str, Any],
        *,
        layer: ContextLayer | None = None,
    ) -> None:
        for key, value in values.items():
            await self.save_context(session_id, key, value, layer=layer)

    async def get_context(self, session_id: str) -> dict[str, Any]:
        layered = await self.get_context_layers(session_id)
        return layered["merged"]

    async def get_context_layer(self, session_id: str, layer: ContextLayer) -> dict[str, Any]:
        layered = await self.get_context_layers(session_id)
        return dict(layered[layer])

    async def get_context_layers(self, session_id: str) -> dict[str, dict[str, Any]]:
        if session_id in self._cleared_sessions:
            base_layers = {
                "legacy": {},
                "short_term": {},
                "working": {},
                "long_term": {},
                "merged": {},
            }
        else:
            base_layers = await self._base.get_context_layers(session_id)
        combined = {
            "legacy": dict(base_layers.get("legacy", {})),
            "short_term": dict(base_layers.get("short_term", {})),
            "working": dict(base_layers.get("working", {})),
            "long_term": dict(base_layers.get("long_term", {})),
        }
        staged = self._staged_session_layers.get(session_id, {})
        for layer_name in ("legacy", "short_term", "working", "long_term"):
            for key, value in staged.get(layer_name, {}).items():
                if value is _DELETED:
                    combined[layer_name].pop(key, None)
                else:
                    combined[layer_name][key] = value
        merged = dict(combined["legacy"])
        for layer_name in _LAYER_ORDER:
            merged.update(combined[layer_name])
        combined["merged"] = merged
        return combined

    # ------------------------------------------------------------------
    # Staged student context writes
    # ------------------------------------------------------------------

    async def save_student_context(
        self,
        student_id: uuid.UUID | str,
        key: str,
        value: Any,
        *,
        layer: ContextLayer = "long_term",
    ) -> None:
        self._record("save_student_context", student_id, key, value, layer=layer)
        student_key = self._base._normalize_student_id(student_id)
        staged_layers = self._staged_student_layers.setdefault(
            student_key,
            {
                "short_term": {},
                "working": {},
                "long_term": {},
            },
        )
        if value is None:
            staged_layers["short_term"][key] = _DELETED
            staged_layers["working"][key] = _DELETED
            staged_layers["long_term"][key] = _DELETED
            return
        for layer_name in ("short_term", "working", "long_term"):
            staged_layers[layer_name][key] = value if layer_name == layer else _DELETED

    async def save_student_contexts(
        self,
        student_id: uuid.UUID | str,
        values: Mapping[str, Any],
        *,
        layer: ContextLayer = "long_term",
    ) -> None:
        for key, value in values.items():
            await self.save_student_context(student_id, key, value, layer=layer)

    async def get_student_context(self, student_id: uuid.UUID | str) -> dict[str, Any]:
        layered = await self.get_student_context_layers(student_id)
        return layered["merged"]

    async def get_student_context_layer(
        self,
        student_id: uuid.UUID | str,
        layer: ContextLayer,
    ) -> dict[str, Any]:
        layered = await self.get_student_context_layers(student_id)
        return dict(layered[layer])

    async def get_student_context_layers(self, student_id: uuid.UUID | str) -> dict[str, dict[str, Any]]:
        student_key = self._base._normalize_student_id(student_id)
        if student_key in self._cleared_students:
            base_layers = {
                "short_term": {},
                "working": {},
                "long_term": {},
                "merged": {},
            }
        else:
            base_layers = await self._base.get_student_context_layers(student_key)
        combined = {
            "short_term": dict(base_layers.get("short_term", {})),
            "working": dict(base_layers.get("working", {})),
            "long_term": dict(base_layers.get("long_term", {})),
        }
        staged = self._staged_student_layers.get(student_key, {})
        for layer_name in ("short_term", "working", "long_term"):
            for key, value in staged.get(layer_name, {}).items():
                if value is _DELETED:
                    combined[layer_name].pop(key, None)
                else:
                    combined[layer_name][key] = value
        merged: dict[str, Any] = {}
        for layer_name in _LAYER_ORDER:
            merged.update(combined[layer_name])
        combined["merged"] = merged
        return combined

    # ------------------------------------------------------------------
    # Cleanup staging
    # ------------------------------------------------------------------

    async def clear(self, session_id: str) -> None:
        self._record("clear", session_id)
        self._cleared_sessions.add(session_id)
        self._staged_history.pop(session_id, None)
        self._staged_session_layers.pop(session_id, None)

    async def clear_student_context(self, student_id: uuid.UUID | str) -> None:
        self._record("clear_student_context", student_id)
        student_key = self._base._normalize_student_id(student_id)
        self._cleared_students.add(student_key)
        self._staged_student_layers.pop(student_key, None)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _record(self, method_name: str, *args: Any, **kwargs: Any) -> None:
        self._staged_ops.append((method_name, args, kwargs))

    def __getattr__(self, item: str) -> Any:
        return getattr(self._base, item)
