"""add residency status and tuition tiers

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-04-08 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('students', sa.Column('residency_status', sa.String(20), nullable=True))
    op.add_column('students', sa.Column('residency_state', sa.String(10), nullable=True))
    op.add_column('schools', sa.Column('tuition_in_state', sa.Integer(), nullable=True))
    op.add_column('schools', sa.Column('tuition_intl', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('schools', 'tuition_intl')
    op.drop_column('schools', 'tuition_in_state')
    op.drop_column('students', 'residency_state')
    op.drop_column('students', 'residency_status')
