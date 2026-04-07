"""Shared FastAPI dependencies."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends
from openai import AsyncOpenAI
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from scholarpath.config import settings
from scholarpath.db.session import get_session
from scholarpath.db.redis import get_redis
from scholarpath.llm.embeddings import EmbeddingService, get_embedding_service
from scholarpath.llm.client import LLMClient, get_llm_client

# Re-export so route modules can import from a single place
__all__ = ["SessionDep", "RedisDep", "LLMDep", "EmbeddingDep", "AppLLMDep"]


async def get_llm() -> AsyncIterator[AsyncOpenAI]:
    """Yield an OpenAI-compatible async client pointed at the ZAI endpoint."""
    active_mode = settings.llm_active_mode
    if active_mode is not None:
        api_key = active_mode.api_keys[0]
        base_url = active_mode.base_url
    else:
        api_key = settings.ZAI_API_KEY
        base_url = settings.ZAI_BASE_URL

    client = AsyncOpenAI(
        api_key=api_key,
        base_url=base_url,
    )
    try:
        yield client
    finally:
        await client.close()


async def get_embeddings() -> AsyncIterator[EmbeddingService]:
    """Yield a Gemini embedding service instance."""
    yield get_embedding_service()


async def get_app_llm() -> AsyncIterator[LLMClient]:
    """Yield the app-level LLM client wrapper."""
    yield get_llm_client()


# Annotated shortcuts for clean dependency injection in route signatures
SessionDep = Annotated[AsyncSession, Depends(get_session)]
RedisDep = Annotated[aioredis.Redis, Depends(get_redis)]
LLMDep = Annotated[AsyncOpenAI, Depends(get_llm)]
EmbeddingDep = Annotated[EmbeddingService, Depends(get_embeddings)]
AppLLMDep = Annotated[LLMClient, Depends(get_app_llm)]
