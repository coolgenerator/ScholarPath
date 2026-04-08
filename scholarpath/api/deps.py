"""Shared FastAPI dependencies."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from scholarpath.db.session import get_session
from scholarpath.db.redis import get_redis
from scholarpath.llm.embeddings import EmbeddingService, get_embedding_service

# Re-export so route modules can import from a single place
__all__ = ["SessionDep", "RedisDep", "EmbeddingDep"]


async def get_embeddings() -> AsyncIterator[EmbeddingService]:
    """Yield a Gemini embedding service instance."""
    yield get_embedding_service()


# Annotated shortcuts for clean dependency injection in route signatures
SessionDep = Annotated[AsyncSession, Depends(get_session)]
RedisDep = Annotated[aioredis.Redis, Depends(get_redis)]
EmbeddingDep = Annotated[EmbeddingService, Depends(get_embeddings)]
