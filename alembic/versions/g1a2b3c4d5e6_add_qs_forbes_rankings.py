"""Add qs_world_rank and forbes_rank columns to schools table.

Revision ID: g1a2b3c4d5e6
Revises: f6a7b8c9d0e1
Create Date: 2026-04-09 12:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "g1a2b3c4d5e6"
down_revision: str | None = "f6a7b8c9d0e1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("schools", sa.Column("qs_world_rank", sa.Integer(), nullable=True))
    op.add_column("schools", sa.Column("forbes_rank", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("schools", "forbes_rank")
    op.drop_column("schools", "qs_world_rank")
