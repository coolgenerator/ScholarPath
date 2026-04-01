"""Standalone script to initialise the ScholarPath database.

Usage:
    python -m scholarpath.init_db

Creates all tables defined by the SQLAlchemy models and installs the pgvector
extension if it is not already present.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from scholarpath.config import settings

# Importing models ensures they are registered on Base.metadata.
from scholarpath.db.models import Base  # noqa: F401

logger = logging.getLogger(__name__)


async def init_db() -> None:
    engine = create_async_engine(settings.DATABASE_URL, echo=True)

    async with engine.begin() as conn:
        # Install the pgvector extension (requires superuser on first run).
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        logger.info("pgvector extension ensured.")

        # Create all tables that don't exist yet.
        await conn.run_sync(Base.metadata.create_all)
        logger.info("All tables created.")

    await engine.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(init_db())


if __name__ == "__main__":
    main()
