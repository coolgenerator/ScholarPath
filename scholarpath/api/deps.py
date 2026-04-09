"""Shared FastAPI dependencies."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status
from openai import AsyncOpenAI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from scholarpath.config import settings
from scholarpath.db.session import get_session
from scholarpath.db.redis import get_redis
from scholarpath.db.models.user import User
from scholarpath.llm.embeddings import EmbeddingService, get_embedding_service
from scholarpath.llm.client import LLMClient, get_llm_client
from scholarpath.services.auth_service import decode_access_token

# Re-export so route modules can import from a single place
__all__ = [
    "SessionDep",
    "RedisDep",
    "LLMDep",
    "EmbeddingDep",
    "AppLLMDep",
    "CurrentUserDep",
    "OptionalUserDep",
]


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


# ---------------------------------------------------------------------------
# Auth dependencies
# ---------------------------------------------------------------------------


async def get_current_user(
    session: Annotated[AsyncSession, Depends(get_session)],
    authorization: Annotated[str | None, Header()] = None,
) -> User:
    """Extract and validate a Bearer token, returning the active ``User``."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = authorization.removeprefix("Bearer ").strip()
    try:
        user_id: uuid.UUID = decode_access_token(token, settings.AUTH_SECRET_KEY)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user is None or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive.",
        )
    return user


async def get_optional_user(
    session: Annotated[AsyncSession, Depends(get_session)],
    authorization: Annotated[str | None, Header()] = None,
) -> User | None:
    """Like ``get_current_user`` but returns ``None`` instead of 401."""
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.removeprefix("Bearer ").strip()
    try:
        user_id = decode_access_token(token, settings.AUTH_SECRET_KEY)
    except Exception:
        return None

    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user is None or not user.is_active:
        return None
    return user


CurrentUserDep = Annotated[User, Depends(get_current_user)]
OptionalUserDep = Annotated[User | None, Depends(get_optional_user)]
