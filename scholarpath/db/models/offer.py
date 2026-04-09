"""Offer model -- admission decisions and financial aid packages."""

from __future__ import annotations

import enum
import uuid
from datetime import date
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, Date, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin, UUIDPrimaryKey

if TYPE_CHECKING:
    from .school import School
    from .student import Student


class OfferStatus(str, enum.Enum):
    ADMITTED = "admitted"
    WAITLISTED = "waitlisted"
    DENIED = "denied"
    DEFERRED = "deferred"
    COMMITTED = "committed"
    DECLINED = "declined"


class Offer(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "offers"

    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"))
    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"))

    status: Mapped[str] = mapped_column(String(20))  # OfferStatus value
    program: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    # ── Cost of Attendance (user-reported per offer) ──
    tuition: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    room_and_board: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    books_supplies: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    personal_expenses: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    transportation: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # ── Financial aid breakdown ──
    merit_scholarship: Mapped[int] = mapped_column(Integer, default=0)
    need_based_grant: Mapped[int] = mapped_column(Integer, default=0)
    loan_offered: Mapped[int] = mapped_column(Integer, default=0)
    work_study: Mapped[int] = mapped_column(Integer, default=0)
    total_aid: Mapped[int] = mapped_column(Integer, default=0)

    # ── Computed totals ──
    total_cost: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    net_cost: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    honors_program: Mapped[bool] = mapped_column(Boolean, default=False)
    conditions: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    decision_deadline: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    student: Mapped[Student] = relationship(back_populates="offers")
    school: Mapped[School] = relationship()
