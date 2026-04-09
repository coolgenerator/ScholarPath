"""Community review models -- Reddit posts and aggregated school reports."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, UUIDPrimaryKey


class CommunityReview(UUIDPrimaryKey, TimestampMixin, Base):
    """A single Reddit post (with top comments) about a school."""

    __tablename__ = "community_reviews"
    __table_args__ = (
        UniqueConstraint("post_id", name="uq_community_review_post_id"),
    )

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"))
    source: Mapped[str] = mapped_column(String(30), default="reddit")
    subreddit: Mapped[str] = mapped_column(String(120))
    post_id: Mapped[str] = mapped_column(String(20))
    post_title: Mapped[str] = mapped_column(Text)
    post_body: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    post_score: Mapped[int] = mapped_column(Integer, default=0)
    post_url: Mapped[str] = mapped_column(String(500))
    post_created_utc: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    top_comments: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    comment_count: Mapped[int] = mapped_column(Integer, default=0)


class SchoolCommunityReport(UUIDPrimaryKey, TimestampMixin, Base):
    """LLM-generated community sentiment report for a school."""

    __tablename__ = "school_community_reports"
    __table_args__ = (
        UniqueConstraint("school_id", name="uq_community_report_school"),
    )

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"))
    review_count: Mapped[int] = mapped_column(Integer, default=0)
    dimensions: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    overall_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    overall_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
    )
    model_version: Mapped[str] = mapped_column(String(60))
