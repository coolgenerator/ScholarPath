"""SchoolClaims model -- extracted claims and argument graphs from community reviews."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, UUIDPrimaryKey


class SchoolClaims(UUIDPrimaryKey, TimestampMixin, Base):
    """Extracted claims, argument graph, and controversy analysis for a school."""

    __tablename__ = "school_claims"
    __table_args__ = (
        UniqueConstraint("school_id", name="uq_school_claims_school"),
    )

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"))
    claims_json: Mapped[dict] = mapped_column(JSON)  # list of extracted claims
    graph_json: Mapped[dict] = mapped_column(JSON)  # cytoscape format
    controversies_json: Mapped[dict] = mapped_column(JSON)  # opposing claim pairs
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    model_version: Mapped[str] = mapped_column(String(60))
