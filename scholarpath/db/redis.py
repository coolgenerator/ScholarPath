from __future__ import annotations

from collections.abc import AsyncIterator

import redis.asyncio as aioredis

from scholarpath.config import settings

redis_pool: aioredis.Redis = aioredis.from_url(
    settings.REDIS_URL,
    decode_responses=True,
)


async def get_redis() -> AsyncIterator[aioredis.Redis]:
    """FastAPI dependency that yields a Redis connection."""
    yield redis_pool
