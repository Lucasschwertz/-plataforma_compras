"""Add analytics shadow compare diff log table.

Revision ID: 20260215_000005
Revises: 20260214_000004
Create Date: 2026-02-15 00:00:05
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260215_000005"
down_revision: Union[str, Sequence[str], None] = "20260214_000004"
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

    if not _table_exists(bind, "analytics_shadow_diff_log"):
        op.create_table(
            "analytics_shadow_diff_log",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            sa.Column("workspace_id", sa.Text(), nullable=False),
            sa.Column("section", sa.Text(), nullable=False),
            sa.Column("primary_source", sa.Text(), nullable=False),
            sa.Column("primary_hash", sa.Text(), nullable=False),
            sa.Column("shadow_hash", sa.Text(), nullable=False),
            sa.Column("diff_summary", sa.Text(), nullable=False),
            sa.Column("diff_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("request_id", sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint("id", name="pk_analytics_shadow_diff_log"),
        )

    _create_index_if_missing(
        bind,
        "ix_analytics_shadow_diff_log_occurred_at",
        "analytics_shadow_diff_log",
        ["occurred_at"],
    )
    _create_index_if_missing(
        bind,
        "ix_analytics_shadow_diff_log_workspace_section",
        "analytics_shadow_diff_log",
        ["workspace_id", "section"],
    )


def downgrade() -> None:
    bind = op.get_bind()
    if _index_exists(bind, "analytics_shadow_diff_log", "ix_analytics_shadow_diff_log_workspace_section"):
        op.drop_index("ix_analytics_shadow_diff_log_workspace_section", table_name="analytics_shadow_diff_log")
    if _index_exists(bind, "analytics_shadow_diff_log", "ix_analytics_shadow_diff_log_occurred_at"):
        op.drop_index("ix_analytics_shadow_diff_log_occurred_at", table_name="analytics_shadow_diff_log")
    op.execute("DROP TABLE IF EXISTS analytics_shadow_diff_log")
