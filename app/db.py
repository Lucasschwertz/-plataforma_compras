import sqlite3
from typing import Iterable, List

from flask import current_app, g


def get_db():
    if "db" not in g:
        db_path = current_app.config["DB_PATH"]
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db


def close_db(_error=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS empresas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            subdomain TEXT UNIQUE,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            external_id TEXT,
            tax_id TEXT,
            risk_flags TEXT NOT NULL DEFAULT '{"no_supplier_response": false, "late_delivery": false, "sla_breach": false}',
            company_id INTEGER,
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
            external_id TEXT,
            company_id INTEGER,
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
            company_id INTEGER,
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
            company_id INTEGER,
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
            company_id INTEGER,
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
            company_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (rfq_item_id, supplier_id, company_id)
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
            company_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (rfq_id, supplier_id, company_id)
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
            company_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (quote_id, rfq_item_id, company_id)
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
            company_id INTEGER,
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
            company_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS integration_watermarks (
            company_id INTEGER,
            system TEXT NOT NULL DEFAULT 'senior',
            entity TEXT NOT NULL CHECK (
                entity IN ('purchase_request','rfq','award','purchase_order','receipt','supplier','category')
            ),
            last_success_source_updated_at TEXT,
            last_success_source_id TEXT,
            last_success_cursor TEXT,
            last_success_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (company_id, system, entity)
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system TEXT NOT NULL DEFAULT 'senior',
            scope TEXT NOT NULL CHECK (
                scope IN ('purchase_request','rfq','award','purchase_order','receipt','supplier','category')
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
            company_id INTEGER
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
            company_id INTEGER
        )
        """
    )

    # Ensure newer columns exist in existing SQLite databases.
    _ensure_column(db, "awards", "purchase_order_id", "INTEGER")
    _ensure_column(db, "purchase_orders", "award_id", "INTEGER")
    _ensure_column(db, "purchase_orders", "supplier_name", "TEXT")
    _ensure_column(db, "purchase_orders", "currency", "TEXT NOT NULL DEFAULT 'BRL'")
    _ensure_column(db, "purchase_orders", "total_amount", "REAL")
    _ensure_column(db, "integration_watermarks", "last_success_source_updated_at", "TEXT")
    _ensure_column(db, "integration_watermarks", "last_success_source_id", "TEXT")
    _ensure_column(db, "integration_watermarks", "last_success_cursor", "TEXT")

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

        CREATE TRIGGER IF NOT EXISTS trg_integration_watermarks_updated_at
        AFTER UPDATE ON integration_watermarks
        FOR EACH ROW
        BEGIN
            UPDATE integration_watermarks
            SET updated_at = CURRENT_TIMESTAMP
            WHERE company_id IS NEW.company_id AND system = NEW.system AND entity = NEW.entity;
        END;
        """
    )

    db.commit()


def _ensure_column(db, table: str, column: str, definition: str) -> None:
    try:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except sqlite3.OperationalError:
        return


def _backfill_demo_items_and_quotes(db) -> None:
    """Cria itens, convites e propostas demo quando ainda nao existem."""
    suppliers = _ensure_demo_suppliers(db)
    if not suppliers:
        return

    rfqs_missing_items = db.execute(
        """
        SELECT r.id, r.title, r.company_id
        FROM rfqs r
        WHERE NOT EXISTS (
            SELECT 1 FROM rfq_items ri WHERE ri.rfq_id = r.id
        )
        ORDER BY r.id
        """
    ).fetchall()

    for rfq in rfqs_missing_items:
        rfq_id = int(rfq["id"])
        company_id = rfq["company_id"]
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
                INSERT INTO rfq_items (rfq_id, description, quantity, uom, company_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (rfq_id, description, quantity, uom, company_id),
            )
            item_ids.append(int(cursor.lastrowid))

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
                    INSERT OR IGNORE INTO rfq_item_suppliers (rfq_item_id, supplier_id, company_id)
                    VALUES (?, ?, ?)
                    """,
                    (rfq_item_id, supplier_id, company_id),
                )

                quote_id = _get_or_create_quote(db, rfq_id, supplier_id, company_id)
                base_price = 90 + (item_index * 15)
                supplier_offset = (supplier_id % 3) * 4
                unit_price = float(base_price + supplier_offset)
                lead_time_days = int(5 + (supplier_id % 4) + item_index)

                db.execute(
                    """
                    INSERT OR REPLACE INTO quote_items (quote_id, rfq_item_id, unit_price, lead_time_days, company_id)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (quote_id, rfq_item_id, unit_price, lead_time_days, company_id),
                )


def _ensure_demo_suppliers(db):
    existing = db.execute("SELECT id, name, company_id FROM suppliers ORDER BY id LIMIT 3").fetchall()
    if existing:
        return existing

    demo_names = ["Fornecedor Atlas", "Fornecedor Nexo", "Fornecedor Prisma"]
    for name in demo_names:
        db.execute(
            "INSERT INTO suppliers (name, company_id) VALUES (?, NULL)",
            (name,),
        )
    return db.execute("SELECT id, name, company_id FROM suppliers ORDER BY id LIMIT 3").fetchall()


def _get_or_create_quote(db, rfq_id: int, supplier_id: int, company_id: int | None) -> int:
    row = db.execute(
        """
        SELECT id
        FROM quotes
        WHERE rfq_id = ? AND supplier_id = ? AND (company_id IS NULL OR company_id = ?)
        LIMIT 1
        """,
        (rfq_id, supplier_id, company_id),
    ).fetchone()
    if row:
        return int(row["id"])

    cursor = db.execute(
        """
        INSERT INTO quotes (rfq_id, supplier_id, status, currency, company_id)
        VALUES (?, ?, 'submitted', 'BRL', ?)
        """,
        (rfq_id, supplier_id, company_id),
    )
    return int(cursor.lastrowid)
