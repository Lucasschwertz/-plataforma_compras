"""Add operational backbone tables for commands, audit, outbox and read-model versions.

Revision ID: 20260214_000004
Revises: 20260214_000003
Create Date: 2026-02-14 00:00:04
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "20260214_000004"
down_revision: Union[str, Sequence[str], None] = "20260214_000003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _is_postgres(bind) -> bool:
    return str(getattr(getattr(bind, "dialect", None), "name", "") or "").lower().startswith("postgres")


def _now_default(bind):
    # Use NOW() in PostgreSQL and CURRENT_TIMESTAMP in SQLite test runs.
    return sa.text("NOW()") if _is_postgres(bind) else sa.text("CURRENT_TIMESTAMP")


def _false_default(bind):
    return sa.text("FALSE") if _is_postgres(bind) else sa.text("0")


def _jsonb_type():
    return sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


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
    now_default = _now_default(bind)
    jsonb = _jsonb_type()
    uuid_type = sa.Uuid(as_uuid=False)

    if not _table_exists(bind, "commands"):
        op.create_table(
            "commands",
            sa.Column("id", uuid_type, nullable=False),
            sa.Column("tenant_id", uuid_type, nullable=False),
            sa.Column("command_type", sa.Text(), nullable=False),
            sa.Column("entity_type", sa.Text(), nullable=False),
            sa.Column("entity_id", sa.Text(), nullable=False),
            sa.Column("payload", jsonb, nullable=False),
            sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
            sa.Column("requested_by", sa.Text(), nullable=False),
            sa.Column("requested_at", sa.DateTime(timezone=False), nullable=False, server_default=now_default),
            sa.Column("executed_at", sa.DateTime(timezone=False), nullable=True),
            sa.Column("failed_at", sa.DateTime(timezone=False), nullable=True),
            sa.Column("error_message", sa.Text(), nullable=True),
            sa.Column("idempotency_key", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=False), nullable=False, server_default=now_default),
            sa.Column("updated_at", sa.DateTime(timezone=False), nullable=False, server_default=now_default),
            sa.PrimaryKeyConstraint("id", name="pk_commands"),
        )
    _create_index_if_missing(bind, "idx_commands_tenant", "commands", ["tenant_id"])
    _create_index_if_missing(bind, "idx_commands_status", "commands", ["status"])
    _create_index_if_missing(bind, "idx_commands_entity", "commands", ["entity_type", "entity_id"])
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_commands_tenant_idempotency_key
        ON commands (tenant_id, idempotency_key)
        WHERE idempotency_key IS NOT NULL
        """
    )

    if not _table_exists(bind, "audit_log"):
        op.create_table(
            "audit_log",
            sa.Column("id", uuid_type, nullable=False),
            sa.Column("tenant_id", uuid_type, nullable=False),
            sa.Column("entity_type", sa.Text(), nullable=False),
            sa.Column("entity_id", sa.Text(), nullable=False),
            sa.Column("action", sa.Text(), nullable=False),
            sa.Column("actor", sa.Text(), nullable=False),
            sa.Column("before_state", jsonb, nullable=True),
            sa.Column("after_state", jsonb, nullable=True),
            sa.Column("metadata", jsonb, nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=False), nullable=False, server_default=now_default),
            sa.PrimaryKeyConstraint("id", name="pk_audit_log"),
        )
    _create_index_if_missing(bind, "idx_audit_tenant", "audit_log", ["tenant_id"])
    _create_index_if_missing(bind, "idx_audit_entity", "audit_log", ["entity_type", "entity_id"])

    if not _table_exists(bind, "outbox_events"):
        op.create_table(
            "outbox_events",
            sa.Column("id", uuid_type, nullable=False),
            sa.Column("tenant_id", uuid_type, nullable=False),
            sa.Column("event_type", sa.Text(), nullable=False),
            sa.Column("aggregate_type", sa.Text(), nullable=False),
            sa.Column("aggregate_id", sa.Text(), nullable=False),
            sa.Column("payload", jsonb, nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=False), nullable=False, server_default=now_default),
            sa.Column("processed", sa.Boolean(), nullable=False, server_default=_false_default(bind)),
            sa.Column("processed_at", sa.DateTime(timezone=False), nullable=True),
            sa.PrimaryKeyConstraint("id", name="pk_outbox_events"),
        )
    _create_index_if_missing(bind, "idx_outbox_processed", "outbox_events", ["processed", "created_at"])
    _create_index_if_missing(bind, "idx_outbox_tenant", "outbox_events", ["tenant_id"])

    if not _table_exists(bind, "read_model_versions"):
        op.create_table(
            "read_model_versions",
            sa.Column("tenant_id", uuid_type, nullable=False),
            sa.Column("model_name", sa.Text(), nullable=False),
            sa.Column("version", sa.Integer(), nullable=False),
            sa.Column("last_event_id", uuid_type, nullable=True),
            sa.Column("rebuilt_at", sa.DateTime(timezone=False), nullable=True),
            sa.PrimaryKeyConstraint("tenant_id", "model_name", name="pk_read_model_versions"),
        )


def downgrade() -> None:
    bind = op.get_bind()

    op.execute("DROP INDEX IF EXISTS uq_commands_tenant_idempotency_key")

    if _index_exists(bind, "outbox_events", "idx_outbox_tenant"):
        op.drop_index("idx_outbox_tenant", table_name="outbox_events")
    if _index_exists(bind, "outbox_events", "idx_outbox_processed"):
        op.drop_index("idx_outbox_processed", table_name="outbox_events")

    if _index_exists(bind, "audit_log", "idx_audit_entity"):
        op.drop_index("idx_audit_entity", table_name="audit_log")
    if _index_exists(bind, "audit_log", "idx_audit_tenant"):
        op.drop_index("idx_audit_tenant", table_name="audit_log")

    if _index_exists(bind, "commands", "idx_commands_entity"):
        op.drop_index("idx_commands_entity", table_name="commands")
    if _index_exists(bind, "commands", "idx_commands_status"):
        op.drop_index("idx_commands_status", table_name="commands")
    if _index_exists(bind, "commands", "idx_commands_tenant"):
        op.drop_index("idx_commands_tenant", table_name="commands")

    op.execute("DROP TABLE IF EXISTS read_model_versions")
    op.execute("DROP TABLE IF EXISTS outbox_events")
    op.execute("DROP TABLE IF EXISTS audit_log")
    op.execute("DROP TABLE IF EXISTS commands")

