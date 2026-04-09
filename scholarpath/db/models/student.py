"""Student profile model."""

from __future__ import annotations

import enum
import uuid
from typing import TYPE_CHECKING, Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import Boolean, Float, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from scholarpath.config import settings
from .base import Base, TimestampMixin, UUIDPrimaryKey

if TYPE_CHECKING:
    from .evaluation import SchoolEvaluation
    from .offer import Offer
    from .user import User


class DegreeLevel(str, enum.Enum):
    UNDERGRADUATE = "undergraduate"
    MASTERS = "masters"
    PHD = "phd"


class CurriculumType(str, enum.Enum):
    AP = "AP"
    IB = "IB"
    A_LEVEL = "A-Level"
    OTHER = "other"


class Student(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "students"

    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("users.id"), unique=True, index=True, nullable=True,
    )

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

    # Citizenship & residency — used to auto-derive tuition tier per school
    citizenship: Mapped[Optional[str]] = mapped_column(
        String(60), nullable=True,
    )  # e.g. "CN", "US", "IN", "KR" — ISO 3166-1 alpha-2
    residency_state: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True,
    )  # US state abbreviation if US citizen/PR, e.g. "CA"

    # Financial
    budget_usd: Mapped[int] = mapped_column(Integer)
    need_financial_aid: Mapped[bool] = mapped_column(Boolean, default=False)

    # Preferences
    preferences: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    ed_preference: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Planning
    degree_level: Mapped[str] = mapped_column(
        String(20), default="undergraduate",
    )  # DegreeLevel value: undergraduate / masters / phd
    target_year: Mapped[int] = mapped_column(Integer)
    profile_completed: Mapped[bool] = mapped_column(Boolean, default=False)

    # Embedding (Gemini gemini-embedding-001 -> 3072 dims)
    profile_embedding: Mapped[Optional[list]] = mapped_column(
        Vector(settings.EMBEDDING_DIMENSION), nullable=True
    )

    # Relationships
    user: Mapped[Optional[User]] = relationship(lazy="joined")
    evaluations: Mapped[list[SchoolEvaluation]] = relationship(
        back_populates="student", cascade="all, delete-orphan"
    )
    offers: Mapped[list[Offer]] = relationship(
        back_populates="student", cascade="all, delete-orphan"
    )
