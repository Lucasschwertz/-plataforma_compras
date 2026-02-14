"""Add analytics read model tables.

Revision ID: 20260214_000002
Revises: 20260210_000001
Create Date: 2026-02-14 00:00:02
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260214_000002"
down_revision: Union[str, Sequence[str], None] = "20260210_000001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ar_projection_state",
        sa.Column("workspace_id", sa.Text(), nullable=False),
        sa.Column("projector", sa.Text(), nullable=False),
        sa.Column("last_event_id", sa.Text(), nullable=True),
        sa.Column("last_processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'running'")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("workspace_id", "projector", name="pk_ar_projection_state"),
    )
    op.create_index(
        "ix_ar_projection_state_status",
        "ar_projection_state",
        ["status"],
        unique=False,
    )

    op.create_table(
        "ar_event_dedupe",
        sa.Column("workspace_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("projector", sa.Text(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("workspace_id", "projector", "event_id", name="pk_ar_event_dedupe"),
    )
    op.create_index(
        "ix_ar_event_dedupe_processed_at",
        "ar_event_dedupe",
        ["workspace_id", "projector", "processed_at"],
        unique=False,
    )

    op.create_table(
        "ar_kpi_daily",
        sa.Column("workspace_id", sa.Text(), nullable=False),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("metric", sa.Text(), nullable=False),
        sa.Column("value_num", sa.Numeric(18, 6), nullable=True),
        sa.Column("value_int", sa.BigInteger(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("workspace_id", "day", "metric", name="pk_ar_kpi_daily"),
    )
    op.create_index(
        "ix_ar_kpi_daily_workspace_day",
        "ar_kpi_daily",
        ["workspace_id", "day"],
        unique=False,
    )
    op.create_index(
        "ix_ar_kpi_daily_workspace_metric_day",
        "ar_kpi_daily",
        ["workspace_id", "metric", "day"],
        unique=False,
    )

    op.create_table(
        "ar_supplier_daily",
        sa.Column("workspace_id", sa.Text(), nullable=False),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("supplier_key", sa.Text(), nullable=False),
        sa.Column("invites", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("responses", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("avg_response_hours", sa.Numeric(18, 6), nullable=True),
        sa.Column("savings_abs", sa.Numeric(18, 6), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("workspace_id", "day", "supplier_key", name="pk_ar_supplier_daily"),
    )
    op.create_index(
        "ix_ar_supplier_daily_workspace_day",
        "ar_supplier_daily",
        ["workspace_id", "day"],
        unique=False,
    )

    op.create_table(
        "ar_process_stage_daily",
        sa.Column("workspace_id", sa.Text(), nullable=False),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("avg_hours", sa.Numeric(18, 6), nullable=True),
        sa.Column("count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("workspace_id", "day", "stage", name="pk_ar_process_stage_daily"),
    )
    op.create_index(
        "ix_ar_process_stage_daily_workspace_day",
        "ar_process_stage_daily",
        ["workspace_id", "day"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_ar_process_stage_daily_workspace_day", table_name="ar_process_stage_daily")
    op.drop_table("ar_process_stage_daily")

    op.drop_index("ix_ar_supplier_daily_workspace_day", table_name="ar_supplier_daily")
    op.drop_table("ar_supplier_daily")

    op.drop_index("ix_ar_kpi_daily_workspace_metric_day", table_name="ar_kpi_daily")
    op.drop_index("ix_ar_kpi_daily_workspace_day", table_name="ar_kpi_daily")
    op.drop_table("ar_kpi_daily")

    op.drop_index("ix_ar_event_dedupe_processed_at", table_name="ar_event_dedupe")
    op.drop_table("ar_event_dedupe")

    op.drop_index("ix_ar_projection_state_status", table_name="ar_projection_state")
    op.drop_table("ar_projection_state")
