"""Add analytics event store for read model replay.

Revision ID: 20260214_000003
Revises: 20260214_000002
Create Date: 2026-02-14 00:00:03
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260214_000003"
down_revision: Union[str, Sequence[str], None] = "20260214_000002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ar_event_store",
        sa.Column("workspace_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("workspace_id", "event_id", name="pk_ar_event_store"),
    )
    op.create_index(
        "ix_ar_event_store_workspace_occurred_at",
        "ar_event_store",
        ["workspace_id", "occurred_at"],
        unique=False,
    )
    op.create_index(
        "ix_ar_event_store_workspace_event_type_occurred_at",
        "ar_event_store",
        ["workspace_id", "event_type", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_ar_event_store_workspace_event_type_occurred_at", table_name="ar_event_store")
    op.drop_index("ix_ar_event_store_workspace_occurred_at", table_name="ar_event_store")
    op.drop_table("ar_event_store")
