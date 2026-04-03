"""add causal data pipeline tables

Revision ID: 9c3a7d2b1e42
Revises: 0f346a1591c2
Create Date: 2026-04-02 07:10:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "9c3a7d2b1e42"
down_revision = "0f346a1591c2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "evidence_artifacts",
        sa.Column("student_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("school_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("cycle_year", sa.Integer(), nullable=True),
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("source_type", sa.String(length=30), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("source_hash", sa.String(length=128), nullable=True),
        sa.Column("captured_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("redaction_status", sa.String(length=30), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.ForeignKeyConstraint(["student_id"], ["students.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "admission_events",
        sa.Column("student_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("school_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("cycle_year", sa.Integer(), nullable=False),
        sa.Column("major_bucket", sa.String(length=100), nullable=True),
        sa.Column("stage", sa.String(length=30), nullable=False),
        sa.Column("happened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence_ref", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["evidence_ref"], ["evidence_artifacts.id"]),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.ForeignKeyConstraint(["student_id"], ["students.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "fact_quarantine",
        sa.Column("student_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("school_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("cycle_year", sa.Integer(), nullable=True),
        sa.Column("outcome_name", sa.String(length=120), nullable=False),
        sa.Column("raw_value", sa.Text(), nullable=False),
        sa.Column("stage", sa.String(length=50), nullable=False),
        sa.Column("reason", sa.String(length=200), nullable=False),
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("resolved", sa.Boolean(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.ForeignKeyConstraint(["student_id"], ["students.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "canonical_facts",
        sa.Column("student_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("school_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("cycle_year", sa.Integer(), nullable=False),
        sa.Column("outcome_name", sa.String(length=120), nullable=False),
        sa.Column("canonical_value_text", sa.Text(), nullable=False),
        sa.Column("canonical_value_numeric", sa.Float(), nullable=True),
        sa.Column("canonical_value_bucket", sa.String(length=120), nullable=False),
        sa.Column("source_family", sa.String(length=60), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.ForeignKeyConstraint(["student_id"], ["students.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "student_id",
            "school_id",
            "cycle_year",
            "outcome_name",
            "canonical_value_bucket",
            "source_family",
            name="uq_canonical_facts_key",
        ),
    )

    op.create_table(
        "fact_lineage",
        sa.Column("canonical_fact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("evidence_artifact_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("raw_value_text", sa.Text(), nullable=False),
        sa.Column("raw_value_numeric", sa.Float(), nullable=True),
        sa.Column("decision", sa.String(length=30), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["canonical_fact_id"], ["canonical_facts.id"]),
        sa.ForeignKeyConstraint(["evidence_artifact_id"], ["evidence_artifacts.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "causal_dataset_versions",
        sa.Column("version", sa.String(length=120), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("config_json", sa.JSON(), nullable=False),
        sa.Column("stats_json", sa.JSON(), nullable=True),
        sa.Column("truth_ratio_by_outcome", sa.JSON(), nullable=True),
        sa.Column("training_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("training_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("mini_gate_passed", sa.Boolean(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("version"),
    )


def downgrade() -> None:
    op.drop_table("causal_dataset_versions")
    op.drop_table("fact_lineage")
    op.drop_table("canonical_facts")
    op.drop_table("fact_quarantine")
    op.drop_table("admission_events")
    op.drop_table("evidence_artifacts")
