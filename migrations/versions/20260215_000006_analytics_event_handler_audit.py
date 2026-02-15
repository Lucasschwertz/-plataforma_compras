"""Add analytics projection handler audit table.

Revision ID: 20260215_000006
Revises: 20260215_000005
Create Date: 2026-02-15 00:00:06
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260215_000006"
down_revision: Union[str, Sequence[str], None] = "20260215_000005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    inspector = sa.inspect(bind)
    return bool(inspector.has_table(table_name))


def _index_exists(bind, table_name: str, index_name: str) -> bool:
    inspector = sa.inspect(bind)
    if not inspector.has_table(table_name):
        return False
    for index in inspector.get_indexes(table_name):
        if str(index.get("name") or "") == index_name:
            return True
    return False


def _create_index_if_missing(bind, index_name: str, table_name: str, columns: list[str]) -> None:
    if not _index_exists(bind, table_name, index_name):
        op.create_index(index_name, table_name, columns, unique=False)


def upgrade() -> None:
    bind = op.get_bind()

    if not _table_exists(bind, "ar_event_handler_audit"):
        op.create_table(
            "ar_event_handler_audit",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("workspace_id", sa.Text(), nullable=False),
            sa.Column("event_id", sa.Text(), nullable=False),
            sa.Column("schema_name", sa.Text(), nullable=False),
            sa.Column("schema_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
            sa.Column("handler_name", sa.Text(), nullable=False),
            sa.Column("status", sa.Text(), nullable=False),
            sa.Column("duration_ms", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("error_code", sa.Text(), nullable=True),
            sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            sa.PrimaryKeyConstraint("id", name="pk_ar_event_handler_audit"),
        )

    _create_index_if_missing(
        bind,
        "ix_ar_event_handler_audit_workspace_processed_at",
        "ar_event_handler_audit",
        ["workspace_id", "processed_at"],
    )
    _create_index_if_missing(
        bind,
        "ix_ar_event_handler_audit_workspace_schema_processed_at",
        "ar_event_handler_audit",
        ["workspace_id", "schema_name", "processed_at"],
    )


def downgrade() -> None:
    bind = op.get_bind()
    if _index_exists(bind, "ar_event_handler_audit", "ix_ar_event_handler_audit_workspace_schema_processed_at"):
        op.drop_index("ix_ar_event_handler_audit_workspace_schema_processed_at", table_name="ar_event_handler_audit")
    if _index_exists(bind, "ar_event_handler_audit", "ix_ar_event_handler_audit_workspace_processed_at"):
        op.drop_index("ix_ar_event_handler_audit_workspace_processed_at", table_name="ar_event_handler_audit")
    op.execute("DROP TABLE IF EXISTS ar_event_handler_audit")
