"""Causal training assets, lineage, and dataset registry models."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin, UUIDPrimaryKey


class CausalFeatureSnapshot(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_feature_snapshots"

    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"), nullable=False)
    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"), nullable=False)
    offer_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("offers.id"), nullable=True)
    context: Mapped[str] = mapped_column(String(100), nullable=False)
    feature_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CausalOutcomeEvent(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_outcome_events"

    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"), nullable=False)
    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"), nullable=False)
    offer_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("offers.id"), nullable=True)
    outcome_name: Mapped[str] = mapped_column(String(120), nullable=False)
    outcome_value: Mapped[float] = mapped_column(Float, nullable=False)
    label_type: Mapped[str] = mapped_column(String(20), nullable=False)
    label_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str] = mapped_column(String(80), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class CausalModelRegistry(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_model_registry"

    model_name: Mapped[str] = mapped_column(String(80), nullable=False)
    model_version: Mapped[str] = mapped_column(String(120), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="trained")
    engine_type: Mapped[str] = mapped_column(String(30), nullable=False, default="pywhy")
    discovery_method: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    estimator_method: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    artifact_uri: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    graph_json: Mapped[dict] = mapped_column(JSON, nullable=False)
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
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class CausalShadowComparison(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_shadow_comparisons"

    request_id: Mapped[str] = mapped_column(String(120), nullable=False)
    context: Mapped[str] = mapped_column(String(80), nullable=False)
    student_id: Mapped[str] = mapped_column(String(36), nullable=False)
    school_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    offer_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    engine_mode: Mapped[str] = mapped_column(String(30), nullable=False)
    causal_model_version: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    legacy_scores: Mapped[dict] = mapped_column(JSON, nullable=False)
    pywhy_scores: Mapped[dict] = mapped_column(JSON, nullable=False)
    diff_scores: Mapped[dict] = mapped_column(JSON, nullable=False)
    fallback_used: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fallback_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)


class EvidenceArtifact(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "evidence_artifacts"

    student_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("students.id"), nullable=True)
    school_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("schools.id"), nullable=True)
    cycle_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    source_type: Mapped[str] = mapped_column(String(30), nullable=False, default="user_upload")
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_hash: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    redaction_status: Mapped[str] = mapped_column(String(30), nullable=False, default="pending")
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class SchoolExternalId(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "school_external_ids"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "external_id",
            name="uq_school_external_ids_provider_external_id",
        ),
        UniqueConstraint(
            "school_id",
            "provider",
            "is_primary",
            name="uq_school_external_ids_school_provider_primary",
        ),
    )

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"), nullable=False)
    provider: Mapped[str] = mapped_column(String(60), nullable=False)
    external_id: Mapped[str] = mapped_column(String(120), nullable=False)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    match_method: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class AdmissionEvent(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "admission_events"

    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"), nullable=False)
    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"), nullable=False)
    cycle_year: Mapped[int] = mapped_column(Integer, nullable=False)
    major_bucket: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    stage: Mapped[str] = mapped_column(String(30), nullable=False)
    happened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    evidence_ref: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("evidence_artifacts.id"),
        nullable=True,
    )
    source_name: Mapped[str] = mapped_column(String(80), nullable=False, default="manual")
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)

    evidence: Mapped[Optional[EvidenceArtifact]] = relationship()


class FactQuarantine(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "fact_quarantine"

    student_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("students.id"), nullable=True)
    school_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("schools.id"), nullable=True)
    cycle_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    outcome_name: Mapped[str] = mapped_column(String(120), nullable=False)
    raw_value: Mapped[str] = mapped_column(Text, nullable=False)
    stage: Mapped[str] = mapped_column(String(50), nullable=False)
    reason: Mapped[str] = mapped_column(String(200), nullable=False)
    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class CanonicalFact(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "canonical_facts"
    __table_args__ = (
        UniqueConstraint(
            "student_id",
            "school_id",
            "cycle_year",
            "outcome_name",
            "canonical_value_bucket",
            "source_family",
            name="uq_canonical_facts_key",
        ),
    )

    student_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("students.id"), nullable=True)
    school_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("schools.id"), nullable=True)
    cycle_year: Mapped[int] = mapped_column(Integer, nullable=False)
    outcome_name: Mapped[str] = mapped_column(String(120), nullable=False)
    canonical_value_text: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_value_numeric: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    canonical_value_bucket: Mapped[str] = mapped_column(String(120), nullable=False)
    source_family: Mapped[str] = mapped_column(String(60), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)

    lineage_rows: Mapped[list["FactLineage"]] = relationship(
        back_populates="canonical_fact",
        cascade="all, delete-orphan",
    )


class FactLineage(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "fact_lineage"

    canonical_fact_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("canonical_facts.id"),
        nullable=False,
    )
    evidence_artifact_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("evidence_artifacts.id"),
        nullable=True,
    )
    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_value_text: Mapped[str] = mapped_column(Text, nullable=False)
    raw_value_numeric: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    decision: Mapped[str] = mapped_column(String(30), nullable=False, default="kept")
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)

    canonical_fact: Mapped[CanonicalFact] = relationship(back_populates="lineage_rows")
    evidence_artifact: Mapped[Optional[EvidenceArtifact]] = relationship()


class CausalDatasetVersion(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_dataset_versions"

    version: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="building")
    config_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    stats_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    truth_ratio_by_outcome: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    training_window_start: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    training_window_end: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    mini_gate_passed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class CausalTrendSignal(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "causal_trend_signals"
    __table_args__ = (
        UniqueConstraint(
            "source_name",
            "metric",
            "period",
            "segment",
            "school_id",
            name="uq_causal_trend_signals_key",
        ),
    )

    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    metric: Mapped[str] = mapped_column(String(120), nullable=False)
    period: Mapped[str] = mapped_column(String(40), nullable=False)
    segment: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    school_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("schools.id"), nullable=True)
    value_numeric: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    value_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)
