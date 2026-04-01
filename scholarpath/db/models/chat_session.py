"""Chat session model -- tracks conversation sessions for history."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, UUIDPrimaryKey


class ChatSession(UUIDPrimaryKey, Base):
    """Persists chat session metadata for the History panel."""

    __tablename__ = "chat_sessions"

    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"))
    session_id: Mapped[str] = mapped_column(String(100), unique=True, index=True)

    title: Mapped[str] = mapped_column(String(300), default="New Session")
    preview: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    school_count: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_active_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
