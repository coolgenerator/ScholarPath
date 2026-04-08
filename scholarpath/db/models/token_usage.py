"""Token usage tracking model."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, UUIDPrimaryKey


class TokenUsage(UUIDPrimaryKey, Base):
    """Tracks token consumption for every LLM API call."""

    __tablename__ = "token_usage"

    # When
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Which model / provider
    model: Mapped[str] = mapped_column(String(60))
    provider: Mapped[str] = mapped_column(String(30), default="zai")  # zai / gemini

    # What triggered the call
    caller: Mapped[str] = mapped_column(String(120))  # e.g. "chat.intent_classification", "search.web_extract"
    method: Mapped[str] = mapped_column(String(30))  # complete / complete_json / stream / embed

    # Token counts
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0)

    # Optional context
    student_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    session_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Cost estimate (if available)
    estimated_cost_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Request metadata
    request_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
