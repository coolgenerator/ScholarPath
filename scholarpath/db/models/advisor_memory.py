"""Advisor memory and RAG storage models."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from scholarpath.config import settings
from .base import Base, TimestampMixin, UUIDPrimaryKey


class AdvisorMessage(UUIDPrimaryKey, TimestampMixin, Base):
    """Raw advisor turn messages for replay and async memory ingestion."""

    __tablename__ = "advisor_messages"
    __table_args__ = (
        UniqueConstraint("session_id", "turn_id", "role", name="uq_advisor_messages_turn_role"),
    )

    turn_id: Mapped[str] = mapped_column(String(120), index=True)
    session_id: Mapped[str] = mapped_column(String(120), index=True)
    student_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("students.id"),
        nullable=True,
        index=True,
    )

    role: Mapped[str] = mapped_column(String(20))
    domain: Mapped[str] = mapped_column(String(30), index=True)
    capability: Mapped[str] = mapped_column(String(80), index=True)
    content: Mapped[str] = mapped_column(Text)

    artifacts_json: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    done_json: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    pending_json: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    next_actions_json: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)

    ingestion_status: Mapped[str] = mapped_column(String(20), default="pending", index=True)


class AdvisorMessageChunk(UUIDPrimaryKey, TimestampMixin, Base):
    """Chunked advisor messages for semantic retrieval."""

    __tablename__ = "advisor_message_chunks"
    __table_args__ = (
        UniqueConstraint("message_id", "chunk_index", name="uq_advisor_chunks_message_index"),
    )

    message_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("advisor_messages.id", ondelete="CASCADE"),
        index=True,
    )
    turn_id: Mapped[str] = mapped_column(String(120), index=True)
    session_id: Mapped[str] = mapped_column(String(120), index=True)
    student_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("students.id"),
        nullable=True,
        index=True,
    )
    domain: Mapped[Optional[str]] = mapped_column(String(30), nullable=True, index=True)

    chunk_index: Mapped[int] = mapped_column(Integer)
    content: Mapped[str] = mapped_column(Text)
    token_count: Mapped[int] = mapped_column(Integer, default=0)
    score_meta: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    embedding: Mapped[Optional[list]] = mapped_column(
        Vector(settings.EMBEDDING_DIMENSION),
        nullable=True,
    )


class AdvisorMemoryItem(UUIDPrimaryKey, TimestampMixin, Base):
    """Structured long-lived advisor memory items."""

    __tablename__ = "advisor_memory_items"

    session_id: Mapped[Optional[str]] = mapped_column(String(120), nullable=True, index=True)
    student_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("students.id"),
        nullable=True,
        index=True,
    )
    domain: Mapped[Optional[str]] = mapped_column(String(30), nullable=True, index=True)

    scope: Mapped[str] = mapped_column(String(20), index=True)
    item_type: Mapped[str] = mapped_column(String(40), index=True)
    item_key: Mapped[str] = mapped_column(String(180), index=True)
    item_value: Mapped[dict] = mapped_column(JSON)

    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    status: Mapped[str] = mapped_column(String(30), default="active", index=True)
    source_turn_id: Mapped[str] = mapped_column(String(120), index=True)

    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
