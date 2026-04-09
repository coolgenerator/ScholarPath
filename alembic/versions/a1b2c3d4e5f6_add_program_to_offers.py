"""add program column to offers

Revision ID: a1b2c3d4e5f6
Revises: f92bd95b5f23
Create Date: 2026-04-07 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = 'f92bd95b5f23'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('offers', sa.Column('program', sa.String(200), nullable=True))


def downgrade() -> None:
    op.drop_column('offers', 'program')
