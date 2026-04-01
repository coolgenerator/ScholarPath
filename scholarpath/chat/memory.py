"""Redis-backed conversation memory for the chat agent."""

from __future__ import annotations

import json
import logging
from typing import Any

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# Redis key prefixes
_HISTORY_PREFIX = "scholarpath:chat:history:"
_CONTEXT_PREFIX = "scholarpath:chat:context:"

# Maximum number of messages retained per session.
_MAX_HISTORY = 50


class ChatMemory:
    """Manages per-session conversation history and extracted context in Redis.

    Conversation history is stored as a Redis list (RPUSH / LRANGE).
    Extracted context (e.g. current school under discussion, partially
    filled profile fields) is stored as a Redis hash.
    """

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    # ------------------------------------------------------------------
    # Message history
    # ------------------------------------------------------------------

    async def save_message(
        self,
        session_id: str,
        role: str,
        content: str,
    ) -> None:
        """Append a message to the conversation history.

        Parameters
        ----------
        session_id:
            Unique conversation session identifier.
        role:
            ``"user"`` or ``"assistant"``.
        content:
            Message text.
        """
        key = f"{_HISTORY_PREFIX}{session_id}"
        payload = json.dumps({"role": role, "content": content}, ensure_ascii=False)
        await self._redis.rpush(key, payload)
        # Trim to keep only the most recent messages.
        await self._redis.ltrim(key, -_MAX_HISTORY, -1)

    async def get_history(
        self,
        session_id: str,
        limit: int = 20,
    ) -> list[dict[str, str]]:
        """Retrieve recent conversation messages.

        Parameters
        ----------
        session_id:
            Unique conversation session identifier.
        limit:
            Maximum number of messages to return (most recent).

        Returns
        -------
        list[dict]
            Each dict has ``role`` and ``content`` keys.
        """
        key = f"{_HISTORY_PREFIX}{session_id}"
        raw_items = await self._redis.lrange(key, -limit, -1)
        messages: list[dict[str, str]] = []
        for item in raw_items:
            try:
                messages.append(json.loads(item))
            except json.JSONDecodeError:
                logger.warning("Corrupt history entry in session %s", session_id)
        return messages

    # ------------------------------------------------------------------
    # Extracted context
    # ------------------------------------------------------------------

    async def save_context(
        self,
        session_id: str,
        key: str,
        value: Any,
    ) -> None:
        """Store an extracted context value (e.g. current school name).

        The value is JSON-serialised before storage so it can represent
        strings, numbers, lists, or dicts.
        """
        redis_key = f"{_CONTEXT_PREFIX}{session_id}"
        await self._redis.hset(redis_key, key, json.dumps(value, ensure_ascii=False))

    async def get_context(
        self,
        session_id: str,
    ) -> dict[str, Any]:
        """Return all extracted context for a session.

        Values are JSON-deserialised back to their Python types.
        """
        redis_key = f"{_CONTEXT_PREFIX}{session_id}"
        raw = await self._redis.hgetall(redis_key)
        context: dict[str, Any] = {}
        for k, v in raw.items():
            try:
                context[k] = json.loads(v)
            except json.JSONDecodeError:
                context[k] = v
        return context

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def clear(self, session_id: str) -> None:
        """Delete all history and context for a session."""
        await self._redis.delete(
            f"{_HISTORY_PREFIX}{session_id}",
            f"{_CONTEXT_PREFIX}{session_id}",
        )
