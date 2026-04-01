"""Conflict model -- tracks disagreements between data points."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, JSON, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, UUIDPrimaryKey
from .data_point import DataPoint
from .school import School


class Severity(str, enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ResolutionStatus(str, enum.Enum):
    UNRESOLVED = "unresolved"
    RESOLVED_A = "resolved_a"
    RESOLVED_B = "resolved_b"
    RESOLVED_MERGED = "resolved_merged"
    DISMISSED = "dismissed"


class Conflict(UUIDPrimaryKey, Base):
    __tablename__ = "conflicts"

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"))
    variable_name: Mapped[str] = mapped_column(String(150))

    datapoint_a_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("data_points.id"))
    datapoint_b_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("data_points.id"))

    severity: Mapped[str] = mapped_column(String(10))  # Severity value
    value_a: Mapped[str] = mapped_column(Text)
    value_b: Mapped[str] = mapped_column(Text)

    resolution_status: Mapped[str] = mapped_column(
        String(20), default=ResolutionStatus.UNRESOLVED.value
    )
    resolved_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    causal_analysis: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    resolved_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    school: Mapped[School] = relationship()
    datapoint_a: Mapped[DataPoint] = relationship(foreign_keys=[datapoint_a_id])
    datapoint_b: Mapped[DataPoint] = relationship(foreign_keys=[datapoint_b_id])
