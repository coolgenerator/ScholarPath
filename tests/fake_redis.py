"""Minimal async Redis stub for unit tests."""

from __future__ import annotations


class FakeRedis:
    """Small subset of redis.asyncio API used by ChatMemory."""

    def __init__(self) -> None:
        self._lists: dict[str, list[str]] = {}
        self._hashes: dict[str, dict[str, str]] = {}

    async def rpush(self, key: str, value: str) -> None:
        self._lists.setdefault(key, []).append(value)

    async def ltrim(self, key: str, start: int, end: int) -> None:
        values = self._lists.get(key, [])
        self._lists[key] = _slice(values, start, end)

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        values = self._lists.get(key, [])
        return _slice(values, start, end)

    async def hset(self, key: str, field: str, value: str) -> None:
        self._hashes.setdefault(key, {})[field] = value

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._hashes.get(key, {}))

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self._lists.pop(key, None)
            self._hashes.pop(key, None)


def _slice(values: list[str], start: int, end: int) -> list[str]:
    """Redis-style inclusive slicing with negative indexes."""
    if not values:
        return []

    n = len(values)
    if start < 0:
        start += n
    if end < 0:
        end += n

    start = max(0, start)
    end = min(n - 1, end)
    if start > end or start >= n:
        return []
    return values[start : end + 1]
