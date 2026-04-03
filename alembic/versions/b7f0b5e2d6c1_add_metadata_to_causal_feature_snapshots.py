"""add metadata to causal_feature_snapshots

Revision ID: b7f0b5e2d6c1
Revises: 9c3a7d2b1e42
Create Date: 2026-04-03 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "b7f0b5e2d6c1"
down_revision = "9c3a7d2b1e42"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("causal_feature_snapshots", sa.Column("metadata", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("causal_feature_snapshots", "metadata")
