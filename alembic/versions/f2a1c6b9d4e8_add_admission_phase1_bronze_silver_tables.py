"""Add admission phase-1 bronze/silver tables.

Revision ID: f2a1c6b9d4e8
Revises: e1c4d93af5c2
Create Date: 2026-04-05 00:10:00
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f2a1c6b9d4e8"
down_revision: str | None = "e1c4d93af5c2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "raw_source_snapshots",
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("source_version", sa.String(length=120), nullable=False),
        sa.Column("pulled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("file_path", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_name",
            "source_version",
            "content_hash",
            name="uq_raw_source_snapshots_key",
        ),
    )

    op.create_table(
        "raw_structured_records",
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("record_key", sa.String(length=200), nullable=False),
        sa.Column("data_year", sa.Integer(), nullable=False),
        sa.Column("school_name", sa.String(length=300), nullable=True),
        sa.Column("state", sa.String(length=60), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("external_id", sa.String(length=120), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("parse_status", sa.String(length=30), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["snapshot_id"], ["raw_source_snapshots.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "snapshot_id",
            "record_key",
            name="uq_raw_structured_records_snapshot_key",
        ),
    )
    op.create_index(
        "ix_raw_structured_records_source_year",
        "raw_structured_records",
        ["source_name", "data_year"],
        unique=False,
    )

    op.create_table(
        "institutions",
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("unitid", sa.String(length=40), nullable=False),
        sa.Column("school_id", sa.Uuid(), nullable=True),
        sa.Column("institution_name", sa.String(length=300), nullable=False),
        sa.Column("state", sa.String(length=60), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("website_url", sa.Text(), nullable=True),
        sa.Column("opeid6", sa.String(length=20), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_name",
            "unitid",
            name="uq_institutions_source_unitid",
        ),
    )

    op.create_table(
        "source_entity_maps",
        sa.Column("school_id", sa.Uuid(), nullable=False),
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("external_id", sa.String(length=120), nullable=True),
        sa.Column("source_school_name", sa.String(length=300), nullable=False),
        sa.Column("source_state", sa.String(length=60), nullable=True),
        sa.Column("source_city", sa.String(length=120), nullable=True),
        sa.Column("match_method", sa.String(length=40), nullable=True),
        sa.Column("match_confidence", sa.Float(), nullable=True),
        sa.Column("is_primary", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_name",
            "external_id",
            name="uq_source_entity_maps_external",
        ),
        sa.UniqueConstraint(
            "school_id",
            "source_name",
            "is_primary",
            name="uq_source_entity_maps_primary",
        ),
    )

    op.create_table(
        "school_metrics_year",
        sa.Column("school_id", sa.Uuid(), nullable=False),
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("metric_year", sa.Integer(), nullable=False),
        sa.Column("applications", sa.Integer(), nullable=True),
        sa.Column("admits", sa.Integer(), nullable=True),
        sa.Column("enrolled", sa.Integer(), nullable=True),
        sa.Column("admit_rate", sa.Float(), nullable=True),
        sa.Column("yield_rate", sa.Float(), nullable=True),
        sa.Column("sat_25", sa.Integer(), nullable=True),
        sa.Column("sat_50", sa.Integer(), nullable=True),
        sa.Column("sat_75", sa.Integer(), nullable=True),
        sa.Column("act_25", sa.Integer(), nullable=True),
        sa.Column("act_50", sa.Integer(), nullable=True),
        sa.Column("act_75", sa.Integer(), nullable=True),
        sa.Column("avg_net_price", sa.Float(), nullable=True),
        sa.Column("grad_rate", sa.Float(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "school_id",
            "metric_year",
            "source_name",
            name="uq_school_metrics_year_key",
        ),
    )
    op.create_index(
        "ix_school_metrics_year_source_year",
        "school_metrics_year",
        ["source_name", "metric_year"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_school_metrics_year_source_year", table_name="school_metrics_year")
    op.drop_table("school_metrics_year")
    op.drop_table("source_entity_maps")
    op.drop_table("institutions")
    op.drop_index("ix_raw_structured_records_source_year", table_name="raw_structured_records")
    op.drop_table("raw_structured_records")
    op.drop_table("raw_source_snapshots")
