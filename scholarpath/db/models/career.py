"""CareerOutcomeProxy model -- estimated career outcomes per school/program."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, UUIDPrimaryKey
from .school import School


class OutcomeType(str, enum.Enum):
    EMPLOYMENT = "employment"
    PHD = "phd"
    BIG_TECH = "big_tech"
    STARTUP = "startup"
    GRAD_SCHOOL = "grad_school"
    FINANCE_BIZ = "finance_biz"
    PUBLIC_SERVICE = "public_service"


class CareerOutcomeProxy(UUIDPrimaryKey, Base):
    __tablename__ = "career_outcome_proxies"

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"))
    program_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("programs.id"), nullable=True
    )

    outcome_type: Mapped[str] = mapped_column(String(20))  # OutcomeType value
    proxy_signals: Mapped[list] = mapped_column(JSON)
    aggregated_estimate: Mapped[float] = mapped_column(Float)
    confidence_lower: Mapped[float] = mapped_column(Float)
    confidence_upper: Mapped[float] = mapped_column(Float)
    source_count: Mapped[int] = mapped_column(Integer)
    last_updated: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Relationships
    school: Mapped[School] = relationship()
