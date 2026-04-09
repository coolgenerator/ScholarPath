"""add metro_area_profiles table and school FK

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-04-08 18:01:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'e5f6a7b8c9d0'
down_revision: Union[str, None] = 'd4e5f6a7b8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'metro_area_profiles',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('city', sa.String(120), nullable=False),
        sa.Column('state', sa.String(60), nullable=False),
        sa.Column('tech_employer_count', sa.Integer(), nullable=True),
        sa.Column('vc_investment_usd', sa.Integer(), nullable=True),
        sa.Column('cost_of_living_index', sa.Float(), nullable=True),
        sa.Column('safety_index', sa.Float(), nullable=True),
        sa.Column('median_household_income', sa.Integer(), nullable=True),
        sa.Column('asian_population_pct', sa.Float(), nullable=True),
        sa.Column('climate_zone', sa.String(40), nullable=True),
        sa.Column('finance_hub_distance_km', sa.Float(), nullable=True),
        sa.Column('federal_lab_count', sa.Integer(), nullable=True),
        sa.Column('nsf_funding_total', sa.Integer(), nullable=True),
        sa.Column('data_year', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('city', 'state', 'data_year', name='uq_metro_city_state_year'),
    )

    op.add_column(
        'schools',
        sa.Column('metro_area_id', sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        'fk_schools_metro_area_id',
        'schools', 'metro_area_profiles',
        ['metro_area_id'], ['id'],
    )


def downgrade() -> None:
    op.drop_constraint('fk_schools_metro_area_id', 'schools', type_='foreignkey')
    op.drop_column('schools', 'metro_area_id')
    op.drop_table('metro_area_profiles')
