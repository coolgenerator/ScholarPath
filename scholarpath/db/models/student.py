"""Student profile model."""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import Boolean, Float, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from scholarpath.config import settings
from .base import Base, TimestampMixin, UUIDPrimaryKey

if TYPE_CHECKING:
    from .evaluation import SchoolEvaluation
    from .offer import Offer


class CurriculumType(str, enum.Enum):
    AP = "AP"
    IB = "IB"
    A_LEVEL = "A-Level"
    OTHER = "other"


class Student(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "students"

    name: Mapped[str] = mapped_column(String(200))
    email: Mapped[Optional[str]] = mapped_column(String(254), nullable=True)
    gpa: Mapped[float] = mapped_column(Float)
    gpa_scale: Mapped[str] = mapped_column(String(20))  # e.g. "4.0", "5.0", "100"

    # Standardised test scores (all nullable)
    sat_total: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sat_rw: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sat_math: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    act_composite: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    toefl_total: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Curriculum & activities
    curriculum_type: Mapped[str] = mapped_column(String(20))  # CurriculumType value
    ap_courses: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    extracurriculars: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    awards: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    intended_majors: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)

    # Financial
    budget_usd: Mapped[int] = mapped_column(Integer)
    need_financial_aid: Mapped[bool] = mapped_column(Boolean, default=False)

    # Preferences
    preferences: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    ed_preference: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Planning
    target_year: Mapped[int] = mapped_column(Integer)
    profile_completed: Mapped[bool] = mapped_column(Boolean, default=False)

    # Embedding (Gemini gemini-embedding-001 -> 3072 dims)
    profile_embedding: Mapped[Optional[list]] = mapped_column(
        Vector(settings.EMBEDDING_DIMENSION), nullable=True
    )

    # Relationships
    evaluations: Mapped[list[SchoolEvaluation]] = relationship(
        back_populates="student", cascade="all, delete-orphan"
    )
    offers: Mapped[list[Offer]] = relationship(
        back_populates="student", cascade="all, delete-orphan"
    )
