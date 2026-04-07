"""Add phase-2 official fact ingestion tables.

Revision ID: c3d2a4e9b7f1
Revises: f2a1c6b9d4e8
Create Date: 2026-04-05 01:40:00
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c3d2a4e9b7f1"
down_revision: str | None = "f2a1c6b9d4e8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "raw_documents",
        sa.Column("school_id", sa.Uuid(), nullable=False),
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("fetch_mode", sa.String(length=40), nullable=False),
        sa.Column("content_type", sa.String(length=20), nullable=False),
        sa.Column("content_text", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column("cycle_year", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.String(length=120), nullable=False),
        sa.Column("pulled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "school_id",
            "source_url",
            "content_hash",
            name="uq_raw_documents_school_source_hash",
        ),
    )
    op.create_index(
        "ix_raw_documents_school_cycle",
        "raw_documents",
        ["school_id", "cycle_year"],
        unique=False,
    )

    op.create_table(
        "document_chunks",
        sa.Column("raw_document_id", sa.Uuid(), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("chunk_text", sa.Text(), nullable=False),
        sa.Column("chunk_hash", sa.String(length=128), nullable=False),
        sa.Column("token_estimate", sa.Integer(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["raw_document_id"], ["raw_documents.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "raw_document_id",
            "chunk_index",
            name="uq_document_chunks_doc_chunk",
        ),
    )

    op.create_table(
        "policy_facts",
        sa.Column("school_id", sa.Uuid(), nullable=False),
        sa.Column("raw_document_id", sa.Uuid(), nullable=True),
        sa.Column("document_chunk_id", sa.Uuid(), nullable=True),
        sa.Column("cycle_year", sa.Integer(), nullable=False),
        sa.Column("fact_key", sa.String(length=120), nullable=False),
        sa.Column("value_text", sa.Text(), nullable=False),
        sa.Column("value_numeric", sa.Float(), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("evidence_quote", sa.Text(), nullable=False),
        sa.Column("extractor_version", sa.String(length=80), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("reviewed_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("status", sa.String(length=30), nullable=False, server_default=sa.text("'accepted'")),
        sa.Column("fact_hash", sa.String(length=128), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["document_chunk_id"], ["document_chunks.id"]),
        sa.ForeignKeyConstraint(["raw_document_id"], ["raw_documents.id"]),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "school_id",
            "cycle_year",
            "fact_key",
            "source_url",
            "fact_hash",
            name="uq_policy_facts_key",
        ),
    )
    op.create_index(
        "ix_policy_facts_school_cycle",
        "policy_facts",
        ["school_id", "cycle_year"],
        unique=False,
    )

    op.create_table(
        "policy_fact_audits",
        sa.Column("policy_fact_id", sa.Uuid(), nullable=True),
        sa.Column("school_id", sa.Uuid(), nullable=False),
        sa.Column("cycle_year", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.String(length=120), nullable=False),
        sa.Column("actor", sa.String(length=80), nullable=False),
        sa.Column("action", sa.String(length=40), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["policy_fact_id"], ["policy_facts.id"]),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_policy_fact_audits_school_cycle",
        "policy_fact_audits",
        ["school_id", "cycle_year"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_policy_fact_audits_school_cycle", table_name="policy_fact_audits")
    op.drop_table("policy_fact_audits")
    op.drop_index("ix_policy_facts_school_cycle", table_name="policy_facts")
    op.drop_table("policy_facts")
    op.drop_table("document_chunks")
    op.drop_index("ix_raw_documents_school_cycle", table_name="raw_documents")
    op.drop_table("raw_documents")
