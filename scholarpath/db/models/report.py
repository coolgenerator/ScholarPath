"""GoNoGoReport model -- final recommendation report for a specific offer."""

from __future__ import annotations

import enum
import uuid
from typing import Optional

from sqlalchemy import Float, ForeignKey, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin, UUIDPrimaryKey
from .offer import Offer
from .student import Student


class Recommendation(str, enum.Enum):
    STRONGLY_RECOMMEND = "strongly_recommend"
    RECOMMEND = "recommend"
    NEUTRAL = "neutral"
    NOT_RECOMMEND = "not_recommend"


class GoNoGoReport(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "go_no_go_reports"

    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"))
    offer_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("offers.id"))

    # Scores
    overall_score: Mapped[float] = mapped_column(Float)
    confidence_lower: Mapped[float] = mapped_column(Float)
    confidence_upper: Mapped[float] = mapped_column(Float)
    academic_score: Mapped[float] = mapped_column(Float)
    financial_score: Mapped[float] = mapped_column(Float)
    career_score: Mapped[float] = mapped_column(Float)
    life_score: Mapped[float] = mapped_column(Float)

    recommendation: Mapped[str] = mapped_column(String(30))  # Recommendation value

    top_factors: Mapped[list] = mapped_column(JSON)
    risks: Mapped[list] = mapped_column(JSON)
    what_if_results: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    narrative: Mapped[str] = mapped_column(Text)

    causal_graph_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("causal_graphs.id"), nullable=True
    )

    # Relationships
    student: Mapped[Student] = relationship()
    offer: Mapped[Offer] = relationship()
