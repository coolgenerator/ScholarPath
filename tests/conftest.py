"""Shared test fixtures: in-memory SQLite backend + httpx AsyncClient."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# ---------------------------------------------------------------------------
# Override pgvector Vector column type BEFORE importing models.
# SQLite does not support pgvector, so we replace it with a no-op type.
# ---------------------------------------------------------------------------
import sqlalchemy
from sqlalchemy import types as sa_types


class _FakeVector(sa_types.TypeDecorator):
    """Stores vectors as plain text in SQLite (for testing only)."""

    impl = sa_types.Text
    cache_ok = True

    def __init__(self, dim=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return value


# Monkey-patch pgvector before any model import
import pgvector.sqlalchemy

pgvector.sqlalchemy.Vector = _FakeVector  # type: ignore[attr-defined]

# Now safe to import models
from scholarpath.db.models.base import Base  # noqa: E402


# ---------------------------------------------------------------------------
# Async SQLite engine + session fixtures
# ---------------------------------------------------------------------------

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture
async def engine():
    eng = create_async_engine(TEST_DATABASE_URL, echo=False)

    # SQLite needs PRAGMA for FK support
    @event.listens_for(eng.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield eng

    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine) -> AsyncIterator[AsyncSession]:
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as sess:
        yield sess
        await sess.rollback()


# ---------------------------------------------------------------------------
# FastAPI test client
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def client(engine):
    """httpx AsyncClient wired to the FastAPI app with test DB session."""
    from httpx import ASGITransport, AsyncClient

    from scholarpath.db.session import get_session
    from scholarpath.main import app

    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _override_session():
        async with factory() as sess:
            try:
                yield sess
                await sess.commit()
            except Exception:
                await sess.rollback()
                raise

    # Override embedding service to avoid real API calls
    from scholarpath.llm.embeddings import EmbeddingService
    from scholarpath.llm.client import LLMClient

    mock_embedding_svc = AsyncMock(spec=EmbeddingService)
    mock_embedding_svc.embed_student_profile.return_value = [0.0] * 10
    mock_embedding_svc.embed_query.return_value = [0.0] * 10
    mock_embedding_svc.embed_text.return_value = [0.0] * 10

    mock_llm = MagicMock(spec=LLMClient)
    mock_llm.complete = AsyncMock(return_value="test llm response")
    mock_llm.complete_json = AsyncMock(return_value={})

    from scholarpath.api.deps import get_embeddings, get_llm

    async def _override_embeddings():
        yield mock_embedding_svc

    def _override_llm():
        return mock_llm

    app.dependency_overrides[get_session] = _override_session
    app.dependency_overrides[get_embeddings] = _override_embeddings
    app.dependency_overrides[get_llm] = _override_llm

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest_asyncio.fixture(autouse=True)
async def _dispose_app_db_engine_after_each_test():
    """Aggressively drain pooled asyncpg connections to avoid loop teardown warnings."""
    yield
    try:
        from scholarpath.db.session import engine as app_engine
    except Exception:
        return
    try:
        await app_engine.dispose()
    except Exception:
        return


@pytest.fixture(scope="session", autouse=True)
def _dispose_app_db_engine_on_exit():
    """Ensure global asyncpg engine is disposed to avoid unclosed connection warnings."""
    yield
    try:
        from scholarpath.db.session import engine as app_engine
    except Exception:
        return

    try:
        import asyncio

        asyncio.run(app_engine.dispose())
    except Exception:
        return
