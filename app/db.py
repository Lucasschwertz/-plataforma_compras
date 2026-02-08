import os
import sqlite3
from pathlib import Path
from typing import Iterable, List

try:
    import psycopg2
    import psycopg2.extras
except ImportError:  # pragma: no cover - optional dependency for postgres
    psycopg2 = None

from flask import current_app, g


DEFAULT_TENANT_ID = "tenant-demo"


class Database:
    def __init__(self, backend: str, connection):
        self.backend = backend
        self._conn = connection

    def execute(self, sql: str, params: Iterable | None = None):
        if self.backend == "postgres":
            cursor = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if params:
                sql = _convert_qmark_to_pg(sql)
                cursor.execute(sql, list(params))
            else:
                cursor.execute(sql)
            return cursor
        return self._conn.execute(sql, params or ())

    def executescript(self, sql: str):
        if self.backend != "postgres":
            return self._conn.executescript(sql)
        for statement in _split_sql_statements(sql):
            if statement.strip():
                self.execute(statement)

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def _split_sql_statements(sql: str) -> List[str]:
    statements = []
    current = []
    in_single = False
    in_double = False
    in_dollar = False
    dollar_tag = ""
    i = 0
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i : i + 2]
        if not in_single and not in_double and nxt == "$$":
            if in_dollar:
                if sql[i : i + len(dollar_tag)] == dollar_tag:
                    in_dollar = False
                    current.append(dollar_tag)
                    i += len(dollar_tag)
                    continue
            else:
                in_dollar = True
                dollar_tag = "$$"
                current.append("$$")
                i += 2
                continue
        if not in_dollar:
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif ch == ";" and not in_single and not in_double:
                statements.append("".join(current))
                current = []
                i += 1
                continue
        current.append(ch)
        i += 1
    if current:
        statements.append("".join(current))
    return statements


def _convert_qmark_to_pg(sql: str) -> str:
    return sql.replace("?", "%s")


def _connect_database(db_path: str) -> Database:
    if db_path.lower().startswith("postgres"):
        if psycopg2 is None:
            raise RuntimeError("psycopg2 nao instalado.")
        conn = psycopg2.connect(db_path)
        conn.autocommit = True
        return Database("postgres", conn)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return Database("sqlite", conn)


def get_db():
    if "db" not in g:
        db_path = current_app.config["DB_PATH"]
        g.db = _connect_database(db_path)
    return g.db


def get_read_db():
    if "db_read" not in g:
        db_path = current_app.config.get("DATABASE_READ_URL") or current_app.config["DB_PATH"]
        g.db_read = _connect_database(db_path)
    return g.db_read


def close_db(_error=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()
    db_read = g.pop("db_read", None)
    if db_read is not None:
        db_read.close()


def init_db():
    db = get_db()
    if db.backend == "postgres":
        _init_db_postgres(db)
        return

    _init_db_sqlite(db)


def _init_db_sqlite(db: Database):
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            subdomain TEXT UNIQUE,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT,
            external_id TEXT,
            tax_id TEXT,
            risk_flags TEXT NOT NULL DEFAULT '{"no_supplier_response": false, "late_delivery": false, "sla_breach": false}',
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS purchase_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT,
            status TEXT NOT NULL DEFAULT 'pending_rfq' CHECK (
                status IN ('pending_rfq','in_rfq','awarded','ordered','partially_received','received','cancelled')
            ),
            priority TEXT NOT NULL DEFAULT 'medium' CHECK (
                priority IN ('low','medium','high','urgent')
            ),
            requested_by TEXT,
            department TEXT,
            needed_at TEXT,
            erp_num_cot TEXT,
            erp_num_pct TEXT,
            erp_sent_at TEXT,
            external_id TEXT,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS purchase_request_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_request_id INTEGER NOT NULL,
            line_no INTEGER,
            description TEXT NOT NULL,
            quantity REAL NOT NULL DEFAULT 1,
            uom TEXT NOT NULL DEFAULT 'UN',
            category TEXT,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS rfqs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            status TEXT NOT NULL DEFAULT 'draft' CHECK (
                status IN ('draft','open','collecting_quotes','closed','awarded','cancelled')
            ),
            cancel_reason TEXT,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS rfq_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rfq_id INTEGER NOT NULL,
            purchase_request_item_id INTEGER,
            description TEXT NOT NULL,
            quantity REAL NOT NULL DEFAULT 1,
            uom TEXT NOT NULL DEFAULT 'UN',
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS rfq_item_suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rfq_item_id INTEGER NOT NULL,
            supplier_id INTEGER NOT NULL,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (rfq_item_id, supplier_id, tenant_id)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS rfq_supplier_invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rfq_id INTEGER NOT NULL,
            supplier_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            email TEXT,
            status TEXT NOT NULL DEFAULT 'pending' CHECK (
                status IN ('pending','opened','submitted','expired','cancelled')
            ),
            expires_at TEXT,
            opened_at TEXT,
            submitted_at TEXT,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rfq_id INTEGER NOT NULL,
            supplier_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'submitted',
            currency TEXT NOT NULL DEFAULT 'BRL',
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (rfq_id, supplier_id, tenant_id)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS quote_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_id INTEGER NOT NULL,
            rfq_item_id INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            lead_time_days INTEGER,
            notes TEXT,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (quote_id, rfq_item_id, tenant_id)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS awards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rfq_id INTEGER NOT NULL,
            supplier_name TEXT,
            status TEXT NOT NULL DEFAULT 'awarded' CHECK (
                status IN ('awarded','converted_to_po','cancelled')
            ),
            reason TEXT NOT NULL,
            purchase_order_id INTEGER,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS purchase_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT,
            award_id INTEGER,
            supplier_name TEXT,
            status TEXT NOT NULL DEFAULT 'draft' CHECK (
                status IN ('draft','approved','sent_to_erp','erp_accepted','partially_received','received','cancelled','erp_error')
            ),
            currency TEXT NOT NULL DEFAULT 'BRL',
            total_amount REAL,
            erp_last_error TEXT,
            external_id TEXT,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS receipts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT,
            purchase_order_id INTEGER,
            purchase_order_external_id TEXT,
            status TEXT NOT NULL,
            received_at TEXT,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tenant_id, external_id)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS integration_watermarks (
            tenant_id TEXT NOT NULL,
            system TEXT NOT NULL DEFAULT 'senior',
            entity TEXT NOT NULL CHECK (
                entity IN (
                    'purchase_request',
                    'rfq',
                    'award',
                    'purchase_order',
                    'receipt',
                    'supplier',
                    'category',
                    'quote',
                    'quote_process',
                    'quote_supplier'
                )
            ),
            last_success_source_updated_at TEXT,
            last_success_source_id TEXT,
            last_success_cursor TEXT,
            last_success_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, system, entity)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_supplier_quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            erp_num_cot TEXT,
            erp_num_pct TEXT,
            supplier_external_id TEXT,
            quote_date TEXT,
            quote_time TEXT,
            quote_datetime TEXT,
            source_table TEXT NOT NULL DEFAULT 'E410COT',
            external_id TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (
                tenant_id,
                erp_num_cot,
                erp_num_pct,
                supplier_external_id,
                quote_datetime,
                source_table
            )
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_quote_processes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            erp_num_pct TEXT NOT NULL,
            opened_at TEXT,
            source_table TEXT NOT NULL DEFAULT 'E410PCT',
            external_id TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tenant_id, erp_num_pct, source_table)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_quote_suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            erp_num_pct TEXT NOT NULL,
            supplier_external_id TEXT NOT NULL,
            source_table TEXT NOT NULL DEFAULT 'E410FPC',
            external_id TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tenant_id, erp_num_pct, supplier_external_id, source_table)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_purchase_order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            erp_num_ocp TEXT NOT NULL,
            line_no INTEGER,
            product_code TEXT,
            description TEXT,
            quantity REAL,
            unit_price REAL,
            total_price REAL,
            source_table TEXT NOT NULL DEFAULT 'E420IPO',
            external_id TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tenant_id, erp_num_ocp, line_no, source_table)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_receipt_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            erp_num_nfc TEXT NOT NULL,
            erp_num_ocp TEXT,
            line_no INTEGER,
            product_code TEXT,
            quantity_received REAL,
            source_table TEXT NOT NULL DEFAULT 'E440IPC',
            external_id TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tenant_id, erp_num_nfc, line_no, source_table)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system TEXT NOT NULL DEFAULT 'senior',
            scope TEXT NOT NULL CHECK (
                scope IN (
                    'purchase_request',
                    'rfq',
                    'award',
                    'purchase_order',
                    'receipt',
                    'supplier',
                    'category',
                    'quote',
                    'quote_process',
                    'quote_supplier'
                )
            ),
            status TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 1,
            parent_sync_run_id INTEGER,
            payload_ref TEXT,
            payload_hash TEXT,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TEXT,
            duration_ms INTEGER,
            records_in INTEGER NOT NULL DEFAULT 0,
            records_upserted INTEGER NOT NULL DEFAULT 0,
            records_failed INTEGER NOT NULL DEFAULT 0,
            error_summary TEXT,
            error_details TEXT,
            tenant_id TEXT NOT NULL
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS status_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity TEXT NOT NULL CHECK (
                entity IN ('purchase_request','rfq','award','purchase_order','receipt')
            ),
            entity_id INTEGER NOT NULL,
            from_status TEXT,
            to_status TEXT NOT NULL,
            reason TEXT,
            actor_user_id INTEGER,
            occurred_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            tenant_id TEXT NOT NULL
        )
        """
    )

    # Ensure newer columns exist in existing SQLite databases.
    _ensure_column(db, "awards", "purchase_order_id", "INTEGER")
    _ensure_column(db, "purchase_orders", "award_id", "INTEGER")
    _ensure_column(db, "purchase_orders", "supplier_name", "TEXT")
    _ensure_column(db, "purchase_orders", "currency", "TEXT NOT NULL DEFAULT 'BRL'")
    _ensure_column(db, "purchase_orders", "total_amount", "REAL")
    _ensure_column(db, "receipts", "external_id", "TEXT")
    _ensure_column(db, "receipts", "purchase_order_id", "INTEGER")
    _ensure_column(db, "receipts", "purchase_order_external_id", "TEXT")
    _ensure_column(db, "receipts", "status", "TEXT")
    _ensure_column(db, "receipts", "received_at", "TEXT")
    _ensure_column(db, "purchase_requests", "erp_num_cot", "TEXT")
    _ensure_column(db, "purchase_requests", "erp_num_pct", "TEXT")
    _ensure_column(db, "purchase_requests", "erp_sent_at", "TEXT")
    _ensure_column(db, "suppliers", "email", "TEXT")
    _ensure_column(db, "integration_watermarks", "last_success_source_updated_at", "TEXT")
    _ensure_column(db, "integration_watermarks", "last_success_source_id", "TEXT")
    _ensure_column(db, "integration_watermarks", "last_success_cursor", "TEXT")
    _ensure_sync_scope_support(db)
    _ensure_sqlite_unique_indexes(db)
    _ensure_tenant_backfill(db)
    _ensure_erp_mirror_tables(db)

    # Padroniza titulos antigos para PT-BR (RFQ -> Cotacao).
    db.execute(
        """
        UPDATE rfqs
        SET title = REPLACE(title, 'RFQ - ', 'Cotacao - ')
        WHERE title LIKE 'RFQ - %'
        """
    )

    _backfill_demo_items_and_quotes(db)

    db.executescript(
        """
        CREATE TRIGGER IF NOT EXISTS trg_suppliers_updated_at
        AFTER UPDATE ON suppliers
        FOR EACH ROW
        BEGIN
            UPDATE suppliers SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_purchase_requests_updated_at
        AFTER UPDATE ON purchase_requests
        FOR EACH ROW
        BEGIN
            UPDATE purchase_requests SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_purchase_request_items_updated_at
        AFTER UPDATE ON purchase_request_items
        FOR EACH ROW
        BEGIN
            UPDATE purchase_request_items SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_rfqs_updated_at
        AFTER UPDATE ON rfqs
        FOR EACH ROW
        BEGIN
            UPDATE rfqs SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_rfq_items_updated_at
        AFTER UPDATE ON rfq_items
        FOR EACH ROW
        BEGIN
            UPDATE rfq_items SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_quotes_updated_at
        AFTER UPDATE ON quotes
        FOR EACH ROW
        BEGIN
            UPDATE quotes SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_quote_items_updated_at
        AFTER UPDATE ON quote_items
        FOR EACH ROW
        BEGIN
            UPDATE quote_items SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_rfq_supplier_invites_updated_at
        AFTER UPDATE ON rfq_supplier_invites
        FOR EACH ROW
        BEGIN
            UPDATE rfq_supplier_invites SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_awards_updated_at
        AFTER UPDATE ON awards
        FOR EACH ROW
        BEGIN
            UPDATE awards SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_purchase_orders_updated_at
        AFTER UPDATE ON purchase_orders
        FOR EACH ROW
        BEGIN
            UPDATE purchase_orders SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_receipts_updated_at
        AFTER UPDATE ON receipts
        FOR EACH ROW
        BEGIN
            UPDATE receipts SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_erp_supplier_quotes_updated_at
        AFTER UPDATE ON erp_supplier_quotes
        FOR EACH ROW
        BEGIN
            UPDATE erp_supplier_quotes SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_erp_quote_processes_updated_at
        AFTER UPDATE ON erp_quote_processes
        FOR EACH ROW
        BEGIN
            UPDATE erp_quote_processes SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_erp_quote_suppliers_updated_at
        AFTER UPDATE ON erp_quote_suppliers
        FOR EACH ROW
        BEGIN
            UPDATE erp_quote_suppliers SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_erp_purchase_order_items_updated_at
        AFTER UPDATE ON erp_purchase_order_items
        FOR EACH ROW
        BEGIN
            UPDATE erp_purchase_order_items SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_erp_receipt_items_updated_at
        AFTER UPDATE ON erp_receipt_items
        FOR EACH ROW
        BEGIN
            UPDATE erp_receipt_items SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_auth_users_updated_at
        AFTER UPDATE ON auth_users
        FOR EACH ROW
        BEGIN
            UPDATE auth_users SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_integration_watermarks_updated_at
        AFTER UPDATE ON integration_watermarks
        FOR EACH ROW
        BEGIN
            UPDATE integration_watermarks
            SET updated_at = CURRENT_TIMESTAMP
            WHERE tenant_id = NEW.tenant_id AND system = NEW.system AND entity = NEW.entity;
        END;
        """
    )

    db.commit()


def _init_db_postgres(db: Database) -> None:
    db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            subdomain TEXT UNIQUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS suppliers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            external_id TEXT,
            tax_id TEXT,
            risk_flags TEXT NOT NULL DEFAULT '{"no_supplier_response": false, "late_delivery": false, "sla_breach": false}',
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS purchase_requests (
            id SERIAL PRIMARY KEY,
            number TEXT,
            status TEXT NOT NULL DEFAULT 'pending_rfq' CHECK (
                status IN ('pending_rfq','in_rfq','awarded','ordered','partially_received','received','cancelled')
            ),
            priority TEXT NOT NULL DEFAULT 'medium' CHECK (
                priority IN ('low','medium','high','urgent')
            ),
            requested_by TEXT,
            department TEXT,
            needed_at TEXT,
            erp_num_cot TEXT,
            erp_num_pct TEXT,
            erp_sent_at TEXT,
            external_id TEXT,
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS purchase_request_items (
            id SERIAL PRIMARY KEY,
            purchase_request_id INTEGER NOT NULL,
            line_no INTEGER,
            description TEXT NOT NULL,
            quantity REAL NOT NULL DEFAULT 1,
            uom TEXT NOT NULL DEFAULT 'UN',
            category TEXT,
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS rfqs (
            id SERIAL PRIMARY KEY,
            title TEXT,
            status TEXT NOT NULL DEFAULT 'draft' CHECK (
                status IN ('draft','open','collecting_quotes','closed','awarded','cancelled')
            ),
            cancel_reason TEXT,
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS rfq_items (
            id SERIAL PRIMARY KEY,
            rfq_id INTEGER NOT NULL,
            purchase_request_item_id INTEGER,
            description TEXT NOT NULL,
            quantity REAL NOT NULL DEFAULT 1,
            uom TEXT NOT NULL DEFAULT 'UN',
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS rfq_item_suppliers (
            id SERIAL PRIMARY KEY,
            rfq_item_id INTEGER NOT NULL,
            supplier_id INTEGER NOT NULL,
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (rfq_item_id, supplier_id, tenant_id)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS rfq_supplier_invites (
            id SERIAL PRIMARY KEY,
            rfq_id INTEGER NOT NULL,
            supplier_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            email TEXT,
            status TEXT NOT NULL DEFAULT 'pending' CHECK (
                status IN ('pending','opened','submitted','expired','cancelled')
            ),
            expires_at TEXT,
            opened_at TEXT,
            submitted_at TEXT,
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS quotes (
            id SERIAL PRIMARY KEY,
            rfq_id INTEGER NOT NULL,
            supplier_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'submitted',
            currency TEXT NOT NULL DEFAULT 'BRL',
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (rfq_id, supplier_id, tenant_id)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS quote_items (
            id SERIAL PRIMARY KEY,
            quote_id INTEGER NOT NULL,
            rfq_item_id INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            lead_time_days INTEGER,
            notes TEXT,
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (quote_id, rfq_item_id, tenant_id)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS awards (
            id SERIAL PRIMARY KEY,
            rfq_id INTEGER NOT NULL,
            supplier_name TEXT,
            status TEXT NOT NULL DEFAULT 'awarded' CHECK (
                status IN ('awarded','converted_to_po','cancelled')
            ),
            reason TEXT NOT NULL,
            purchase_order_id INTEGER,
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS purchase_orders (
            id SERIAL PRIMARY KEY,
            number TEXT,
            award_id INTEGER,
            supplier_name TEXT,
            status TEXT NOT NULL DEFAULT 'draft' CHECK (
                status IN ('draft','approved','sent_to_erp','erp_accepted','partially_received','received','cancelled','erp_error')
            ),
            currency TEXT NOT NULL DEFAULT 'BRL',
            total_amount REAL,
            erp_last_error TEXT,
            external_id TEXT,
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS receipts (
            id SERIAL PRIMARY KEY,
            external_id TEXT,
            purchase_order_id INTEGER,
            purchase_order_external_id TEXT,
            status TEXT NOT NULL,
            received_at TEXT,
            tenant_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tenant_id, external_id)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS integration_watermarks (
            tenant_id TEXT NOT NULL,
            system TEXT NOT NULL DEFAULT 'senior',
            entity TEXT NOT NULL CHECK (
                entity IN (
                    'purchase_request',
                    'rfq',
                    'award',
                    'purchase_order',
                    'receipt',
                    'supplier',
                    'category',
                    'quote',
                    'quote_process',
                    'quote_supplier'
                )
            ),
            last_success_source_updated_at TEXT,
            last_success_source_id TEXT,
            last_success_cursor TEXT,
            last_success_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, system, entity)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_supplier_quotes (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            erp_num_cot TEXT,
            erp_num_pct TEXT,
            supplier_external_id TEXT,
            quote_date TEXT,
            quote_time TEXT,
            quote_datetime TEXT,
            source_table TEXT NOT NULL DEFAULT 'E410COT',
            external_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (
                tenant_id,
                erp_num_cot,
                erp_num_pct,
                supplier_external_id,
                quote_datetime,
                source_table
            )
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_quote_processes (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            erp_num_pct TEXT NOT NULL,
            opened_at TEXT,
            source_table TEXT NOT NULL DEFAULT 'E410PCT',
            external_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tenant_id, erp_num_pct, source_table)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_quote_suppliers (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            erp_num_pct TEXT NOT NULL,
            supplier_external_id TEXT NOT NULL,
            source_table TEXT NOT NULL DEFAULT 'E410FPC',
            external_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tenant_id, erp_num_pct, supplier_external_id, source_table)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_purchase_order_items (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            erp_num_ocp TEXT NOT NULL,
            line_no INTEGER,
            product_code TEXT,
            description TEXT,
            quantity REAL,
            unit_price REAL,
            total_price REAL,
            source_table TEXT NOT NULL DEFAULT 'E420IPO',
            external_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tenant_id, erp_num_ocp, line_no, source_table)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_receipt_items (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            erp_num_nfc TEXT NOT NULL,
            erp_num_ocp TEXT,
            line_no INTEGER,
            product_code TEXT,
            quantity_received REAL,
            source_table TEXT NOT NULL DEFAULT 'E440IPC',
            external_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tenant_id, erp_num_nfc, line_no, source_table)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
            id SERIAL PRIMARY KEY,
            system TEXT NOT NULL DEFAULT 'senior',
            scope TEXT NOT NULL CHECK (
                scope IN (
                    'purchase_request',
                    'rfq',
                    'award',
                    'purchase_order',
                    'receipt',
                    'supplier',
                    'category',
                    'quote',
                    'quote_process',
                    'quote_supplier'
                )
            ),
            status TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 1,
            parent_sync_run_id INTEGER,
            payload_ref TEXT,
            payload_hash TEXT,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            duration_ms INTEGER,
            records_in INTEGER NOT NULL DEFAULT 0,
            records_upserted INTEGER NOT NULL DEFAULT 0,
            records_failed INTEGER NOT NULL DEFAULT 0,
            error_summary TEXT,
            error_details TEXT,
            tenant_id TEXT NOT NULL
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS status_events (
            id SERIAL PRIMARY KEY,
            entity TEXT NOT NULL CHECK (
                entity IN ('purchase_request','rfq','award','purchase_order','receipt')
            ),
            entity_id INTEGER NOT NULL,
            from_status TEXT,
            to_status TEXT NOT NULL,
            reason TEXT,
            actor_user_id INTEGER,
            occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            tenant_id TEXT NOT NULL
        )
        """
    )

    _ensure_column(db, "awards", "purchase_order_id", "INTEGER")
    _ensure_column(db, "purchase_orders", "award_id", "INTEGER")
    _ensure_column(db, "purchase_orders", "supplier_name", "TEXT")
    _ensure_column(db, "purchase_orders", "currency", "TEXT NOT NULL DEFAULT 'BRL'")
    _ensure_column(db, "purchase_orders", "total_amount", "REAL")
    _ensure_column(db, "receipts", "external_id", "TEXT")
    _ensure_column(db, "receipts", "purchase_order_id", "INTEGER")
    _ensure_column(db, "receipts", "purchase_order_external_id", "TEXT")
    _ensure_column(db, "receipts", "status", "TEXT")
    _ensure_column(db, "receipts", "received_at", "TEXT")
    _ensure_column(db, "purchase_requests", "erp_num_cot", "TEXT")
    _ensure_column(db, "purchase_requests", "erp_num_pct", "TEXT")
    _ensure_column(db, "purchase_requests", "erp_sent_at", "TEXT")
    _ensure_column(db, "suppliers", "email", "TEXT")
    _ensure_column(db, "integration_watermarks", "last_success_source_updated_at", "TEXT")
    _ensure_column(db, "integration_watermarks", "last_success_source_id", "TEXT")
    _ensure_column(db, "integration_watermarks", "last_success_cursor", "TEXT")

    _ensure_tenant_backfill(db)
    _ensure_erp_mirror_tables(db)
    _backfill_demo_items_and_quotes(db)
    _create_postgres_updated_at_triggers(db)

    db.commit()


def _create_postgres_updated_at_triggers(db: Database) -> None:
    db.execute(
        """
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    tables = [
        "suppliers",
        "purchase_requests",
        "purchase_request_items",
        "rfqs",
        "rfq_items",
        "quotes",
        "quote_items",
        "rfq_supplier_invites",
        "awards",
        "purchase_orders",
        "receipts",
        "auth_users",
        "integration_watermarks",
        "erp_supplier_quotes",
        "erp_quote_processes",
        "erp_quote_suppliers",
        "erp_purchase_order_items",
        "erp_receipt_items",
    ]

    for table in tables:
        db.execute(
            f"""
            DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table};
            CREATE TRIGGER trg_{table}_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
            """
        )



def _ensure_erp_mirror_tables(db: Database) -> None:
    """Create ERP mirror tables from schema file when enabled."""
    try:
        auto_create = bool(current_app.config.get("ERP_MIRROR_AUTO_CREATE", True))
        schema_path_raw = current_app.config.get("ERP_MIRROR_SCHEMA")
    except RuntimeError:
        auto_create = os.environ.get("ERP_MIRROR_AUTO_CREATE", "1").strip().lower() in {"1", "true", "yes", "on"}
        schema_path_raw = os.environ.get("ERP_MIRROR_SCHEMA")

    if not auto_create:
        return

    schema_path = None
    if schema_path_raw:
        schema_path = Path(str(schema_path_raw)).expanduser()
    if not schema_path:
        schema_path = Path(__file__).resolve().parents[1] / "tabelas.csv"
    if not schema_path.exists():
        return

    try:
        from database.erp_mirror import ensure_mirror_tables, load_schema_tables

        schema_tables = load_schema_tables(schema_path)
        ensure_mirror_tables(db, schema_tables)
    except Exception as exc:
        try:
            current_app.logger.warning("Falha ao criar espelho ERP (%s): %s", schema_path, exc)
        except RuntimeError:
            pass
def _ensure_column(db: Database, table: str, column: str, definition: str) -> None:
    try:
        if db.backend == "postgres":
            db.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")
        else:
            db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except Exception:
        return


def _ensure_sqlite_unique_indexes(db: Database) -> None:
    if db.backend != "sqlite":
        return
    db.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_quote_items_quote_rfq_tenant
        ON quote_items (quote_id, rfq_item_id, tenant_id)
        """
    )
    db.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_rfq_item_suppliers_unique
        ON rfq_item_suppliers (rfq_item_id, supplier_id, tenant_id)
        """
    )


def _ensure_sync_scope_support(db) -> None:
    _ensure_check_value(
        db,
        table="sync_runs",
        column="scope",
        allowed_values=(
            "purchase_request",
            "rfq",
            "award",
            "purchase_order",
            "receipt",
            "supplier",
            "category",
            "quote",
            "quote_process",
            "quote_supplier",
        ),
    )
    _ensure_check_value(
        db,
        table="integration_watermarks",
        column="entity",
        allowed_values=(
            "purchase_request",
            "rfq",
            "award",
            "purchase_order",
            "receipt",
            "supplier",
            "category",
            "quote",
            "quote_process",
            "quote_supplier",
        ),
    )


def _ensure_check_value(
    db: Database,
    table: str,
    column: str,
    allowed_values: Iterable[str],
) -> None:
    if db.backend == "postgres":
        return
    if not _table_exists(db, table):
        return

    row = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone()
    if not row or not row["sql"]:
        return

    current_sql = row["sql"]
    missing = [value for value in allowed_values if f"'{value}'" not in current_sql]
    if not missing:
        return

    if table == "sync_runs":
        _recreate_sync_runs(db, allowed_values)
    elif table == "integration_watermarks":
        _recreate_integration_watermarks(db, allowed_values)


def _recreate_sync_runs(db, allowed_values: Iterable[str]) -> None:
    values = ",".join(f"'{value}'" for value in allowed_values)
    # Prevent collisions when a previous migration left a temp table behind.
    db.execute("DROP TABLE IF EXISTS sync_runs_old")
    db.execute("ALTER TABLE sync_runs RENAME TO sync_runs_old")
    db.execute(
        f"""
        CREATE TABLE sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system TEXT NOT NULL DEFAULT 'senior',
            scope TEXT NOT NULL CHECK (scope IN ({values})),
            status TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 1,
            parent_sync_run_id INTEGER,
            payload_ref TEXT,
            payload_hash TEXT,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TEXT,
            duration_ms INTEGER,
            records_in INTEGER NOT NULL DEFAULT 0,
            records_upserted INTEGER NOT NULL DEFAULT 0,
            records_failed INTEGER NOT NULL DEFAULT 0,
            error_summary TEXT,
            error_details TEXT,
            tenant_id TEXT NOT NULL
        )
        """
    )
    db.execute(
        """
        INSERT INTO sync_runs (
            id,
            system,
            scope,
            status,
            attempt,
            parent_sync_run_id,
            payload_ref,
            payload_hash,
            started_at,
            finished_at,
            duration_ms,
            records_in,
            records_upserted,
            records_failed,
            error_summary,
            error_details,
            tenant_id
        )
        SELECT
            id,
            system,
            scope,
            status,
            attempt,
            parent_sync_run_id,
            payload_ref,
            payload_hash,
            started_at,
            finished_at,
            duration_ms,
            records_in,
            records_upserted,
            records_failed,
            error_summary,
            error_details,
            tenant_id
        FROM sync_runs_old
        """
    )
    db.execute("DROP TABLE sync_runs_old")


def _recreate_integration_watermarks(db, allowed_values: Iterable[str]) -> None:
    values = ",".join(f"'{value}'" for value in allowed_values)
    db.execute("ALTER TABLE integration_watermarks RENAME TO integration_watermarks_old")
    db.execute(
        f"""
        CREATE TABLE integration_watermarks (
            tenant_id TEXT NOT NULL,
            system TEXT NOT NULL DEFAULT 'senior',
            entity TEXT NOT NULL CHECK (entity IN ({values})),
            last_success_source_updated_at TEXT,
            last_success_source_id TEXT,
            last_success_cursor TEXT,
            last_success_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, system, entity)
        )
        """
    )
    db.execute(
        """
        INSERT INTO integration_watermarks (
            tenant_id,
            system,
            entity,
            last_success_source_updated_at,
            last_success_source_id,
            last_success_cursor,
            last_success_at,
            updated_at
        )
        SELECT
            tenant_id,
            system,
            entity,
            last_success_source_updated_at,
            last_success_source_id,
            last_success_cursor,
            last_success_at,
            updated_at
        FROM integration_watermarks_old
        """
    )
    db.execute("DROP TABLE integration_watermarks_old")


def _table_exists(db: Database, table: str) -> bool:
    if db.backend == "postgres":
        row = db.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ?
            """,
            (table,),
        ).fetchone()
        return row is not None

    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _column_exists(db: Database, table: str, column: str) -> bool:
    if not _table_exists(db, table):
        return False
    if db.backend == "postgres":
        row = db.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ? AND column_name = ?
            """,
            (table, column),
        ).fetchone()
        return row is not None
    rows = db.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def _ensure_tenant_backfill(db) -> None:
    tenant_tables = [
        "suppliers",
        "purchase_requests",
        "purchase_request_items",
        "rfqs",
        "rfq_items",
        "rfq_item_suppliers",
        "rfq_supplier_invites",
        "quotes",
        "quote_items",
        "awards",
        "purchase_orders",
        "receipts",
        "erp_supplier_quotes",
        "erp_quote_processes",
        "erp_quote_suppliers",
        "erp_purchase_order_items",
        "erp_receipt_items",
        "integration_watermarks",
        "sync_runs",
        "status_events",
    ]

    for table in tenant_tables:
        if not _table_exists(db, table):
            continue
        _ensure_column(db, table, "tenant_id", "TEXT")

        if _column_exists(db, table, "company_id"):
            db.execute(
                f"""
                UPDATE {table}
                SET tenant_id = COALESCE(tenant_id, 'tenant-' || company_id)
                """
            )

        db.execute(
            f"""
            UPDATE {table}
            SET tenant_id = COALESCE(tenant_id, '{DEFAULT_TENANT_ID}')
            """
        )

    if _table_exists(db, "tenants"):
        db.execute(
            """
            INSERT INTO tenants (id, name, subdomain)
            VALUES (?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            (DEFAULT_TENANT_ID, "Tenant Demo", DEFAULT_TENANT_ID),
        )

    if _table_exists(db, "empresas") and _table_exists(db, "tenants"):
        rows = db.execute("SELECT id, nome, subdomain FROM empresas").fetchall()
        for row in rows:
            tenant_id = f"tenant-{row['id']}"
            db.execute(
                """
                INSERT INTO tenants (id, name, subdomain)
                VALUES (?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                (tenant_id, row["nome"], row["subdomain"]),
            )


def _backfill_demo_items_and_quotes(db) -> None:
    """Cria itens, convites e propostas demo quando ainda nao existem."""
    suppliers = _ensure_demo_suppliers(db)
    if not suppliers:
        return

    rfqs_missing_items = db.execute(
        """
        SELECT r.id, r.title, r.tenant_id
        FROM rfqs r
        WHERE r.tenant_id = ?
          AND NOT EXISTS (
              SELECT 1 FROM rfq_items ri WHERE ri.rfq_id = r.id
          )
        ORDER BY r.id
        """,
        (DEFAULT_TENANT_ID,),
    ).fetchall()

    for rfq in rfqs_missing_items:
        rfq_id = int(rfq["id"])
        tenant_id = rfq["tenant_id"]
        title = (rfq["title"] or f"Cotacao {rfq_id}").replace("RFQ - ", "Cotacao - ")

        item_ids = []
        item_specs = [
            (f"{title} - Item 1", 10, "UN"),
            (f"{title} - Item 2", 5, "UN"),
            (f"{title} - Item 3", 2, "CX"),
        ]
        for line_no, (description, quantity, uom) in enumerate(item_specs, start=1):
            cursor = db.execute(
                """
                INSERT INTO rfq_items (rfq_id, description, quantity, uom, tenant_id)
                VALUES (?, ?, ?, ?, ?)
                RETURNING id
                """,
                (rfq_id, description, quantity, uom, tenant_id),
            )
            item_row = cursor.fetchone()
            item_ids.append(int(item_row["id"] if isinstance(item_row, dict) else item_row[0]))

        # Convida fornecedores diferentes por grupo de itens.
        supplier_groups: List[Iterable[int]] = [
            [suppliers[0]["id"], suppliers[1]["id"]],
            [suppliers[1]["id"], suppliers[2]["id"]],
            [suppliers[0]["id"], suppliers[2]["id"]],
        ]

        for item_index, rfq_item_id in enumerate(item_ids):
            invited_supplier_ids = supplier_groups[item_index % len(supplier_groups)]
            for supplier_id in invited_supplier_ids:
                db.execute(
                    """
                    INSERT INTO rfq_item_suppliers (rfq_item_id, supplier_id, tenant_id)
                    VALUES (?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    (rfq_item_id, supplier_id, tenant_id),
                )

                quote_id = _get_or_create_quote(db, rfq_id, supplier_id, tenant_id)
                base_price = 90 + (item_index * 15)
                supplier_offset = (supplier_id % 3) * 4
                unit_price = float(base_price + supplier_offset)
                lead_time_days = int(5 + (supplier_id % 4) + item_index)

                db.execute(
                    """
                    INSERT INTO quote_items (quote_id, rfq_item_id, unit_price, lead_time_days, tenant_id)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (quote_id, rfq_item_id, tenant_id) DO UPDATE SET
                        unit_price = excluded.unit_price,
                        lead_time_days = excluded.lead_time_days,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (quote_id, rfq_item_id, unit_price, lead_time_days, tenant_id),
                )


def _ensure_demo_suppliers(db):
    existing = db.execute(
        """
        SELECT id, name, tenant_id
        FROM suppliers
        WHERE tenant_id = ?
        ORDER BY id
        LIMIT 3
        """,
        (DEFAULT_TENANT_ID,),
    ).fetchall()
    if existing:
        return existing

    demo_names = ["Fornecedor Atlas", "Fornecedor Nexo", "Fornecedor Prisma"]
    for name in demo_names:
        db.execute(
            "INSERT INTO suppliers (name, tenant_id) VALUES (?, ?)",
            (name, DEFAULT_TENANT_ID),
        )
    return db.execute(
        """
        SELECT id, name, tenant_id
        FROM suppliers
        WHERE tenant_id = ?
        ORDER BY id
        LIMIT 3
        """,
        (DEFAULT_TENANT_ID,),
    ).fetchall()


def _get_or_create_quote(db, rfq_id: int, supplier_id: int, tenant_id: str) -> int:
    row = db.execute(
        """
        SELECT id
        FROM quotes
        WHERE rfq_id = ? AND supplier_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (rfq_id, supplier_id, tenant_id),
    ).fetchone()
    if row:
        return int(row["id"])

    cursor = db.execute(
        """
        INSERT INTO quotes (rfq_id, supplier_id, status, currency, tenant_id)
        VALUES (?, ?, 'submitted', 'BRL', ?)
        RETURNING id
        """,
        (rfq_id, supplier_id, tenant_id),
    )
    row = cursor.fetchone()
    return int(row["id"] if isinstance(row, dict) else row[0])





