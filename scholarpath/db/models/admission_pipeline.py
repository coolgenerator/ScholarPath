"""Bronze/Silver tables for phase-1 admissions data pipeline."""

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
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, UUIDPrimaryKey


class RawSourceSnapshot(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "raw_source_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "source_name",
            "source_version",
            "content_hash",
            name="uq_raw_source_snapshots_key",
        ),
    )

    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    source_version: Mapped[str] = mapped_column(String(120), nullable=False)
    pulled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    file_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class RawStructuredRecord(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "raw_structured_records"
    __table_args__ = (
        UniqueConstraint(
            "snapshot_id",
            "record_key",
            name="uq_raw_structured_records_snapshot_key",
        ),
    )

    snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("raw_source_snapshots.id"),
        nullable=False,
    )
    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    record_key: Mapped[str] = mapped_column(String(200), nullable=False)
    data_year: Mapped[int] = mapped_column(Integer, nullable=False)
    school_name: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    external_id: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    parse_status: Mapped[str] = mapped_column(String(30), nullable=False, default="raw")
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class Institution(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "institutions"
    __table_args__ = (
        UniqueConstraint(
            "source_name",
            "unitid",
            name="uq_institutions_source_unitid",
        ),
    )

    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    unitid: Mapped[str] = mapped_column(String(40), nullable=False)
    school_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("schools.id"), nullable=True)
    institution_name: Mapped[str] = mapped_column(String(300), nullable=False)
    state: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    website_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    opeid6: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class SourceEntityMap(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "source_entity_maps"
    __table_args__ = (
        UniqueConstraint(
            "source_name",
            "external_id",
            name="uq_source_entity_maps_external",
        ),
        UniqueConstraint(
            "school_id",
            "source_name",
            "is_primary",
            name="uq_source_entity_maps_primary",
        ),
    )

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"), nullable=False)
    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    external_id: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    source_school_name: Mapped[str] = mapped_column(String(300), nullable=False)
    source_state: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    source_city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    match_method: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    match_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class SchoolMetricsYear(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "school_metrics_year"
    __table_args__ = (
        UniqueConstraint(
            "school_id",
            "metric_year",
            "source_name",
            name="uq_school_metrics_year_key",
        ),
    )

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"), nullable=False)
    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    metric_year: Mapped[int] = mapped_column(Integer, nullable=False)
    applications: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    admits: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    enrolled: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    admit_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    yield_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    sat_25: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sat_50: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sat_75: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    act_25: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    act_50: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    act_75: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    avg_net_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    grad_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class RawDocument(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "raw_documents"
    __table_args__ = (
        UniqueConstraint(
            "school_id",
            "source_url",
            "content_hash",
            name="uq_raw_documents_school_source_hash",
        ),
    )

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"), nullable=False)
    source_name: Mapped[str] = mapped_column(String(80), nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    fetch_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    content_type: Mapped[str] = mapped_column(String(20), nullable=False, default="text")
    content_text: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    cycle_year: Mapped[int] = mapped_column(Integer, nullable=False)
    run_id: Mapped[str] = mapped_column(String(120), nullable=False)
    pulled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class DocumentChunk(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "document_chunks"
    __table_args__ = (
        UniqueConstraint(
            "raw_document_id",
            "chunk_index",
            name="uq_document_chunks_doc_chunk",
        ),
    )

    raw_document_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("raw_documents.id"), nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    chunk_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    token_estimate: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class PolicyFact(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "policy_facts"
    __table_args__ = (
        UniqueConstraint(
            "school_id",
            "cycle_year",
            "fact_key",
            "source_url",
            "fact_hash",
            name="uq_policy_facts_key",
        ),
    )

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"), nullable=False)
    raw_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("raw_documents.id"),
        nullable=True,
    )
    document_chunk_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("document_chunks.id"),
        nullable=True,
    )
    cycle_year: Mapped[int] = mapped_column(Integer, nullable=False)
    fact_key: Mapped[str] = mapped_column(String(120), nullable=False)
    value_text: Mapped[str] = mapped_column(Text, nullable=False)
    value_numeric: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_quote: Mapped[str] = mapped_column(Text, nullable=False)
    extractor_version: Mapped[str] = mapped_column(String(80), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    reviewed_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="accepted")
    fact_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)


class PolicyFactAudit(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "policy_fact_audits"

    policy_fact_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("policy_facts.id"),
        nullable=True,
    )
    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"), nullable=False)
    cycle_year: Mapped[int] = mapped_column(Integer, nullable=False)
    run_id: Mapped[str] = mapped_column(String(120), nullable=False)
    actor: Mapped[str] = mapped_column(String(80), nullable=False)
    action: Mapped[str] = mapped_column(String(40), nullable=False)
    payload: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
