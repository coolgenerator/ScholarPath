"""Runtime assets for the PyWhy causal engine rollout."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    JSON,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin, UUIDPrimaryKey
from .offer import Offer
from .school import School
from .student import Student


class CausalFeatureSnapshot(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_feature_snapshots"

    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"))
    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"))
    offer_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("offers.id"),
        nullable=True,
    )
    context: Mapped[str] = mapped_column(String(40))
    feature_payload: Mapped[dict] = mapped_column(JSON)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    student: Mapped[Student] = relationship()
    school: Mapped[School] = relationship()
    offer: Mapped[Optional[Offer]] = relationship()


class CausalOutcomeEvent(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_outcome_events"

    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"))
    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"))
    offer_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("offers.id"),
        nullable=True,
    )
    outcome_name: Mapped[str] = mapped_column(String(64))
    outcome_value: Mapped[float] = mapped_column(Float)
    label_type: Mapped[str] = mapped_column(String(20))  # true | proxy
    label_confidence: Mapped[float] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String(80))
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)

    student: Mapped[Student] = relationship()
    school: Mapped[School] = relationship()
    offer: Mapped[Optional[Offer]] = relationship()


class CausalModelRegistry(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_model_registry"

    model_name: Mapped[str] = mapped_column(String(80), default="pywhy_full_graph")
    model_version: Mapped[str] = mapped_column(String(64), unique=True)
    status: Mapped[str] = mapped_column(String(20), default="draft")
    engine_type: Mapped[str] = mapped_column(String(20), default="pywhy")
    discovery_method: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    estimator_method: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    artifact_uri: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    graph_json: Mapped[dict] = mapped_column(JSON)
    metrics_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    refuter_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    training_window_start: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    training_window_end: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)


class CausalShadowComparison(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_shadow_comparisons"

    request_id: Mapped[str] = mapped_column(String(80))
    context: Mapped[str] = mapped_column(String(40))
    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"))
    school_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("schools.id"),
        nullable=True,
    )
    offer_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("offers.id"),
        nullable=True,
    )
    engine_mode: Mapped[str] = mapped_column(String(20))
    causal_model_version: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    legacy_scores: Mapped[dict] = mapped_column(JSON)
    pywhy_scores: Mapped[dict] = mapped_column(JSON)
    diff_scores: Mapped[dict] = mapped_column(JSON)
    fallback_used: Mapped[bool] = mapped_column(Boolean, default=False)
    fallback_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    student: Mapped[Student] = relationship()
    school: Mapped[Optional[School]] = relationship()
    offer: Mapped[Optional[Offer]] = relationship()
