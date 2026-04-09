"""rename residency_status to citizenship

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-04-08 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('students', 'residency_status', new_column_name='citizenship', type_=sa.String(60))


def downgrade() -> None:
    op.alter_column('students', 'citizenship', new_column_name='residency_status', type_=sa.String(20))
