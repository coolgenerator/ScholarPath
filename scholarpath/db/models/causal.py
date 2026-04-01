"""CausalGraph model -- serialised causal DAGs."""

from __future__ import annotations

import enum
import uuid
from typing import Optional

from sqlalchemy import ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, UUIDPrimaryKey


class CausalContext(str, enum.Enum):
    SELECTION = "selection"
    OFFER = "offer"
    WHAT_IF = "what_if"


class CausalGraph(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_graphs"

    student_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("students.id"), nullable=True
    )

    context: Mapped[str] = mapped_column(String(20))  # CausalContext value
    nodes_json: Mapped[dict] = mapped_column(JSON)
    edges_json: Mapped[dict] = mapped_column(JSON)
    scores_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)
