"""add causal runtime assets

Revision ID: 6ad5f6e44f11
Revises: 0f346a1591c2
Create Date: 2026-04-01 05:20:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "6ad5f6e44f11"
down_revision: Union[str, None] = "0f346a1591c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "causal_feature_snapshots",
        sa.Column("student_id", sa.Uuid(), nullable=False),
        sa.Column("school_id", sa.Uuid(), nullable=False),
        sa.Column("offer_id", sa.Uuid(), nullable=True),
        sa.Column("context", sa.String(length=40), nullable=False),
        sa.Column("feature_payload", sa.JSON(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["offer_id"], ["offers.id"]),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.ForeignKeyConstraint(["student_id"], ["students.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_causal_feature_snapshots_context", "causal_feature_snapshots", ["context"])
    op.create_index("ix_causal_feature_snapshots_student_school", "causal_feature_snapshots", ["student_id", "school_id"])

    op.create_table(
        "causal_outcome_events",
        sa.Column("student_id", sa.Uuid(), nullable=False),
        sa.Column("school_id", sa.Uuid(), nullable=False),
        sa.Column("offer_id", sa.Uuid(), nullable=True),
        sa.Column("outcome_name", sa.String(length=64), nullable=False),
        sa.Column("outcome_value", sa.Float(), nullable=False),
        sa.Column("label_type", sa.String(length=20), nullable=False),
        sa.Column("label_confidence", sa.Float(), nullable=False),
        sa.Column("source", sa.String(length=80), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["offer_id"], ["offers.id"]),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.ForeignKeyConstraint(["student_id"], ["students.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_causal_outcome_events_name", "causal_outcome_events", ["outcome_name"])
    op.create_index("ix_causal_outcome_events_student_school", "causal_outcome_events", ["student_id", "school_id"])

    op.create_table(
        "causal_model_registry",
        sa.Column("model_name", sa.String(length=80), nullable=False),
        sa.Column("model_version", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("engine_type", sa.String(length=20), nullable=False),
        sa.Column("discovery_method", sa.String(length=80), nullable=True),
        sa.Column("estimator_method", sa.String(length=80), nullable=True),
        sa.Column("artifact_uri", sa.Text(), nullable=True),
        sa.Column("graph_json", sa.JSON(), nullable=False),
        sa.Column("metrics_json", sa.JSON(), nullable=True),
        sa.Column("refuter_json", sa.JSON(), nullable=True),
        sa.Column("training_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("training_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("model_version"),
    )
    op.create_index("ix_causal_model_registry_status", "causal_model_registry", ["status", "is_active"])

    op.create_table(
        "causal_shadow_comparisons",
        sa.Column("request_id", sa.String(length=80), nullable=False),
        sa.Column("context", sa.String(length=40), nullable=False),
        sa.Column("student_id", sa.Uuid(), nullable=False),
        sa.Column("school_id", sa.Uuid(), nullable=True),
        sa.Column("offer_id", sa.Uuid(), nullable=True),
        sa.Column("engine_mode", sa.String(length=20), nullable=False),
        sa.Column("causal_model_version", sa.String(length=64), nullable=True),
        sa.Column("legacy_scores", sa.JSON(), nullable=False),
        sa.Column("pywhy_scores", sa.JSON(), nullable=False),
        sa.Column("diff_scores", sa.JSON(), nullable=False),
        sa.Column("fallback_used", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("fallback_reason", sa.Text(), nullable=True),
        sa.Column("error_json", sa.JSON(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["offer_id"], ["offers.id"]),
        sa.ForeignKeyConstraint(["school_id"], ["schools.id"]),
        sa.ForeignKeyConstraint(["student_id"], ["students.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_causal_shadow_comparisons_request", "causal_shadow_comparisons", ["request_id"])
    op.create_index("ix_causal_shadow_comparisons_context", "causal_shadow_comparisons", ["context"])


def downgrade() -> None:
    op.drop_index("ix_causal_shadow_comparisons_context", table_name="causal_shadow_comparisons")
    op.drop_index("ix_causal_shadow_comparisons_request", table_name="causal_shadow_comparisons")
    op.drop_table("causal_shadow_comparisons")

    op.drop_index("ix_causal_model_registry_status", table_name="causal_model_registry")
    op.drop_table("causal_model_registry")

    op.drop_index("ix_causal_outcome_events_student_school", table_name="causal_outcome_events")
    op.drop_index("ix_causal_outcome_events_name", table_name="causal_outcome_events")
    op.drop_table("causal_outcome_events")

    op.drop_index("ix_causal_feature_snapshots_student_school", table_name="causal_feature_snapshots")
    op.drop_index("ix_causal_feature_snapshots_context", table_name="causal_feature_snapshots")
    op.drop_table("causal_feature_snapshots")
