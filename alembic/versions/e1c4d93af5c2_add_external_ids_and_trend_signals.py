"""Add school external ids and causal trend signals tables.

Revision ID: e1c4d93af5c2
Revises: b7f0b5e2d6c1
Create Date: 2026-04-03 03:20:00
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e1c4d93af5c2"
down_revision: str | None = "b7f0b5e2d6c1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "school_external_ids",
        sa.Column("school_id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(length=60), nullable=False),
        sa.Column("external_id", sa.String(length=120), nullable=False),
        sa.Column("is_primary", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("match_method", sa.String(length=40), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "provider",
            "external_id",
            name="uq_school_external_ids_provider_external_id",
        ),
        sa.UniqueConstraint(
            "school_id",
            "provider",
            "is_primary",
            name="uq_school_external_ids_school_provider_primary",
        ),
    )

    op.create_table(
        "causal_trend_signals",
        sa.Column("source_name", sa.String(length=80), nullable=False),
        sa.Column("metric", sa.String(length=120), nullable=False),
        sa.Column("period", sa.String(length=40), nullable=False),
        sa.Column("segment", sa.String(length=120), nullable=True),
        sa.Column("school_id", sa.Uuid(), nullable=True),
        sa.Column("value_numeric", sa.Float(), nullable=True),
        sa.Column("value_text", sa.Text(), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_name",
            "metric",
            "period",
            "segment",
            "school_id",
            name="uq_causal_trend_signals_key",
        ),
    )


def downgrade() -> None:
    op.drop_table("causal_trend_signals")
    op.drop_table("school_external_ids")
