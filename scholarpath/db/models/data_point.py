"""DataPoint model -- heterogeneous data from multiple sources."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from scholarpath.config import settings
from .base import Base, UUIDPrimaryKey
from .school import School


class SourceType(str, enum.Enum):
    OFFICIAL = "official"
    PROXY = "proxy"
    UGC = "ugc"


class DataPoint(UUIDPrimaryKey, Base):
    __tablename__ = "data_points"

    school_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("schools.id"), nullable=True
    )
    program_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("programs.id"), nullable=True
    )

    source_type: Mapped[str] = mapped_column(String(20))  # SourceType value
    source_name: Mapped[str] = mapped_column(String(100))  # e.g. 'cds', 'niche', 'xiaohongshu'
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    variable_name: Mapped[str] = mapped_column(String(150))
    value_text: Mapped[str] = mapped_column(Text)
    value_numeric: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    confidence: Mapped[float] = mapped_column(Float)  # 0-1
    sample_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    temporal_range: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)

    crawled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    derivation_method: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Embedding (Gemini gemini-embedding-001 -> 3072 dims)
    embedding: Mapped[Optional[list]] = mapped_column(
        Vector(settings.EMBEDDING_DIMENSION), nullable=True
    )

    # Relationships
    school: Mapped[Optional[School]] = relationship(back_populates="data_points")
