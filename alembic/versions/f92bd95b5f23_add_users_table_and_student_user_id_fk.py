"""add users table and student user_id fk

Revision ID: f92bd95b5f23
Revises: c3d2a4e9b7f1
Create Date: 2026-04-07 14:16:11.848937

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f92bd95b5f23'
down_revision: Union[str, None] = 'c3d2a4e9b7f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("email", sa.String(254), nullable=False),
        sa.Column("phone", sa.String(20), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
        sa.UniqueConstraint("phone"),
    )
    op.create_index("ix_users_email", "users", ["email"])

    op.add_column("students", sa.Column("user_id", sa.Uuid(), nullable=True))
    op.create_foreign_key("fk_students_user_id", "students", "users", ["user_id"], ["id"])
    op.create_index("ix_students_user_id", "students", ["user_id"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_students_user_id", table_name="students")
    op.drop_constraint("fk_students_user_id", "students", type_="foreignkey")
    op.drop_column("students", "user_id")
    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")
