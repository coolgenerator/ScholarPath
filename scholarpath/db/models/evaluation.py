"""SchoolEvaluation model -- per-student assessment of a school."""

from __future__ import annotations

import enum
import uuid
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Float, ForeignKey, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin, UUIDPrimaryKey

if TYPE_CHECKING:
    from .school import School
    from .student import Student


class Tier(str, enum.Enum):
    REACH = "reach"
    TARGET = "target"
    SAFETY = "safety"
    LIKELY = "likely"


class SchoolEvaluation(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "school_evaluations"

    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"))
    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"))

    tier: Mapped[str] = mapped_column(String(10))  # Tier value

    # Fit scores (0-1)
    academic_fit: Mapped[float] = mapped_column(Float)
    financial_fit: Mapped[float] = mapped_column(Float)
    career_fit: Mapped[float] = mapped_column(Float)
    life_fit: Mapped[float] = mapped_column(Float)
    overall_score: Mapped[float] = mapped_column(Float)

    admission_probability: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ed_ea_recommendation: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True
    )  # ed/ea/rea/rd

    reasoning: Mapped[str] = mapped_column(Text)
    fit_details: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    causal_graph_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("causal_graphs.id"), nullable=True
    )

    # Relationships
    student: Mapped[Student] = relationship(back_populates="evaluations")
    school: Mapped[School] = relationship()
