"""add advisor memory layers

Revision ID: 9b8d1e8af11f
Revises: 6ad5f6e44f11
Create Date: 2026-04-01 09:58:00.000000
"""

from typing import Sequence, Union

from alembic import op
from pgvector.sqlalchemy import Vector
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9b8d1e8af11f"
down_revision: Union[str, None] = "6ad5f6e44f11"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "advisor_messages",
        sa.Column("turn_id", sa.String(length=120), nullable=False),
        sa.Column("session_id", sa.String(length=120), nullable=False),
        sa.Column("student_id", sa.Uuid(), nullable=True),
        sa.Column("role", sa.String(length=20), nullable=False),
        sa.Column("domain", sa.String(length=30), nullable=False),
        sa.Column("capability", sa.String(length=80), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("artifacts_json", sa.JSON(), nullable=True),
        sa.Column("done_json", sa.JSON(), nullable=True),
        sa.Column("pending_json", sa.JSON(), nullable=True),
        sa.Column("next_actions_json", sa.JSON(), nullable=True),
        sa.Column("ingestion_status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["student_id"], ["students.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id", "turn_id", "role", name="uq_advisor_messages_turn_role"),
    )
    op.create_index("ix_advisor_messages_turn_id", "advisor_messages", ["turn_id"])
    op.create_index("ix_advisor_messages_session_id", "advisor_messages", ["session_id"])
    op.create_index("ix_advisor_messages_student_id", "advisor_messages", ["student_id"])
    op.create_index("ix_advisor_messages_domain", "advisor_messages", ["domain"])
    op.create_index("ix_advisor_messages_capability", "advisor_messages", ["capability"])
    op.create_index("ix_advisor_messages_ingestion_status", "advisor_messages", ["ingestion_status"])

    op.create_table(
        "advisor_message_chunks",
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("turn_id", sa.String(length=120), nullable=False),
        sa.Column("session_id", sa.String(length=120), nullable=False),
        sa.Column("student_id", sa.Uuid(), nullable=True),
        sa.Column("domain", sa.String(length=30), nullable=True),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("token_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("score_meta", sa.JSON(), nullable=True),
        sa.Column("embedding", Vector(3072), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["message_id"], ["advisor_messages.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["student_id"], ["students.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("message_id", "chunk_index", name="uq_advisor_chunks_message_index"),
    )
    op.create_index("ix_advisor_chunks_message_id", "advisor_message_chunks", ["message_id"])
    op.create_index("ix_advisor_chunks_turn_id", "advisor_message_chunks", ["turn_id"])
    op.create_index("ix_advisor_chunks_session_id", "advisor_message_chunks", ["session_id"])
    op.create_index("ix_advisor_chunks_student_id", "advisor_message_chunks", ["student_id"])
    op.create_index("ix_advisor_chunks_domain", "advisor_message_chunks", ["domain"])

    op.create_table(
        "advisor_memory_items",
        sa.Column("session_id", sa.String(length=120), nullable=True),
        sa.Column("student_id", sa.Uuid(), nullable=True),
        sa.Column("domain", sa.String(length=30), nullable=True),
        sa.Column("scope", sa.String(length=20), nullable=False),
        sa.Column("item_type", sa.String(length=40), nullable=False),
        sa.Column("item_key", sa.String(length=180), nullable=False),
        sa.Column("item_value", sa.JSON(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="active"),
        sa.Column("source_turn_id", sa.String(length=120), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["student_id"], ["students.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_advisor_memory_items_session_id", "advisor_memory_items", ["session_id"])
    op.create_index("ix_advisor_memory_items_student_id", "advisor_memory_items", ["student_id"])
    op.create_index("ix_advisor_memory_items_domain", "advisor_memory_items", ["domain"])
    op.create_index("ix_advisor_memory_items_scope", "advisor_memory_items", ["scope"])
    op.create_index("ix_advisor_memory_items_item_type", "advisor_memory_items", ["item_type"])
    op.create_index("ix_advisor_memory_items_item_key", "advisor_memory_items", ["item_key"])
    op.create_index("ix_advisor_memory_items_status", "advisor_memory_items", ["status"])
    op.create_index("ix_advisor_memory_items_source_turn_id", "advisor_memory_items", ["source_turn_id"])
    op.create_index("ix_advisor_memory_items_expires_at", "advisor_memory_items", ["expires_at"])


def downgrade() -> None:
    op.drop_index("ix_advisor_memory_items_expires_at", table_name="advisor_memory_items")
    op.drop_index("ix_advisor_memory_items_source_turn_id", table_name="advisor_memory_items")
    op.drop_index("ix_advisor_memory_items_status", table_name="advisor_memory_items")
    op.drop_index("ix_advisor_memory_items_item_key", table_name="advisor_memory_items")
    op.drop_index("ix_advisor_memory_items_item_type", table_name="advisor_memory_items")
    op.drop_index("ix_advisor_memory_items_scope", table_name="advisor_memory_items")
    op.drop_index("ix_advisor_memory_items_domain", table_name="advisor_memory_items")
    op.drop_index("ix_advisor_memory_items_student_id", table_name="advisor_memory_items")
    op.drop_index("ix_advisor_memory_items_session_id", table_name="advisor_memory_items")
    op.drop_table("advisor_memory_items")

    op.drop_index("ix_advisor_chunks_domain", table_name="advisor_message_chunks")
    op.drop_index("ix_advisor_chunks_student_id", table_name="advisor_message_chunks")
    op.drop_index("ix_advisor_chunks_session_id", table_name="advisor_message_chunks")
    op.drop_index("ix_advisor_chunks_turn_id", table_name="advisor_message_chunks")
    op.drop_index("ix_advisor_chunks_message_id", table_name="advisor_message_chunks")
    op.drop_table("advisor_message_chunks")

    op.drop_index("ix_advisor_messages_ingestion_status", table_name="advisor_messages")
    op.drop_index("ix_advisor_messages_capability", table_name="advisor_messages")
    op.drop_index("ix_advisor_messages_domain", table_name="advisor_messages")
    op.drop_index("ix_advisor_messages_student_id", table_name="advisor_messages")
    op.drop_index("ix_advisor_messages_session_id", table_name="advisor_messages")
    op.drop_index("ix_advisor_messages_turn_id", table_name="advisor_messages")
    op.drop_table("advisor_messages")
