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
from scholarpath.llm import LLMClient, get_llm_client

# Re-export so route modules can import from a single place
__all__ = ["SessionDep", "RedisDep", "LLMDep", "EmbeddingDep"]


def get_llm() -> LLMClient:
    """Return the shared ScholarPath LLM client."""
    return get_llm_client()


async def get_embeddings() -> AsyncIterator[EmbeddingService]:
    """Yield a Gemini embedding service instance."""
    yield get_embedding_service()


# Annotated shortcuts for clean dependency injection in route signatures
SessionDep = Annotated[AsyncSession, Depends(get_session)]
RedisDep = Annotated[aioredis.Redis, Depends(get_redis)]
LLMDep = Annotated[LLMClient, Depends(get_llm)]
EmbeddingDep = Annotated[EmbeddingService, Depends(get_embeddings)]
