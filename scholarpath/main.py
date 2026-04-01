from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from scholarpath.config import settings
from scholarpath.db.session import engine
from scholarpath.db.redis import redis_pool


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Startup
    yield
    # Shutdown
    await engine.dispose()
    if redis_pool is not None:
        await redis_pool.aclose()  # type: ignore[union-attr]


app = FastAPI(
    title="ScholarPath",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Router registration (lazy imports to avoid circular deps) ---


def _register_routers() -> None:
    from scholarpath.api import router as api_router  # noqa: F811

    app.include_router(api_router, prefix="/api")


_register_routers()
