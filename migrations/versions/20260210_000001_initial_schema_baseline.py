"""Initial schema baseline from app.db

Revision ID: 20260210_000001
Revises:
Create Date: 2026-02-10 00:00:01
"""

from __future__ import annotations

from typing import Iterable, Sequence, Union

from alembic import op
from sqlalchemy.engine import Connection

from app.db import _convert_qmark_to_pg, _init_db_postgres, _init_db_sqlite, _split_sql_statements


# revision identifiers, used by Alembic.
revision: str = "20260210_000001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


class _ResultAdapter:
    def __init__(self, result):
        self._result = result

    @staticmethod
    def _map_row(row):
        if row is None:
            return None
        if isinstance(row, dict):
            return row
        mapping = getattr(row, "_mapping", None)
        if mapping is not None:
            return dict(mapping)
        return row

    def fetchone(self):
        return self._map_row(self._result.fetchone())

    def fetchall(self):
        rows = self._result.fetchall()
        return [self._map_row(row) for row in rows]


class _AlembicDbAdapter:
    def __init__(self, connection: Connection, backend: str):
        self._connection = connection
        self.backend = backend

    def execute(self, sql: str, params: Iterable | None = None):
        statement = sql
        if params is None:
            result = self._connection.exec_driver_sql(statement)
            return _ResultAdapter(result)

        values = tuple(params)
        if self.backend == "postgres":
            statement = _convert_qmark_to_pg(statement)
        result = self._connection.exec_driver_sql(statement, values)
        return _ResultAdapter(result)

    def executescript(self, sql: str):
        if self.backend == "sqlite":
            raw_conn = getattr(self._connection, "connection", None)
            driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
            if driver_conn is not None and hasattr(driver_conn, "executescript"):
                driver_conn.executescript(sql)
                return
        for statement in _split_sql_statements(sql):
            if statement.strip():
                self.execute(statement)

    def commit(self):
        # Alembic controla transacoes no contexto da migration.
        return None

    def close(self):
        return None


def _resolve_backend(connection: Connection) -> str:
    dialect = (connection.dialect.name or "").lower()
    if dialect.startswith("postgres"):
        return "postgres"
    return "sqlite"


def upgrade() -> None:
    connection = op.get_bind()
    backend = _resolve_backend(connection)
    adapter = _AlembicDbAdapter(connection, backend)

    if backend == "postgres":
        _init_db_postgres(adapter)
        return

    _init_db_sqlite(adapter)


def downgrade() -> None:
    connection = op.get_bind()
    backend = _resolve_backend(connection)

    tables = [
        "status_events",
        "sync_runs",
        "erp_receipt_items",
        "erp_purchase_order_items",
        "erp_quote_suppliers",
        "erp_quote_processes",
        "erp_supplier_quotes",
        "integration_watermarks",
        "receipts",
        "purchase_orders",
        "awards",
        "quote_items",
        "quotes",
        "rfq_supplier_invites",
        "rfq_item_suppliers",
        "rfq_items",
        "rfqs",
        "purchase_request_items",
        "purchase_requests",
        "suppliers",
        "auth_users",
        "tenants",
    ]

    if backend == "postgres":
        op.execute("DROP FUNCTION IF EXISTS set_updated_at() CASCADE")

    for table in tables:
        op.execute(f"DROP TABLE IF EXISTS {table}")
