from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

from app.config import Config
from app.db import Database, _connect_database
from app.routes.procurement_routes import _combine_erp_datetime, _parse_datetime


def _load_schema_columns(schema_path: Path, table: str) -> List[str]:
    columns: List[tuple[int, str]] = []
    with schema_path.open("r", encoding="utf-8-sig", errors="ignore") as handle:
        reader = csv.reader(handle, delimiter=";")
        for row in reader:
            if len(row) < 13:
                continue
            if row[1] != table:
                continue
            try:
                order = int(row[12])
            except ValueError:
                continue
            columns.append((order, row[3]))
    return [name for order, name in sorted(columns, key=lambda item: item[0])]


def _safe_value(row: List[str], idx: int) -> str | None:
    if idx >= len(row):
        return None
    value = str(row[idx]).strip()
    if not value or value.upper() == "NULL":
        return None
    return value


def _iter_csv_rows(path: Path) -> Iterable[List[str]]:
    with path.open("r", encoding="utf-8-sig", errors="ignore") as handle:
        reader = csv.reader(handle, delimiter=";")
        for row in reader:
            if not row:
                continue
            yield row


def _parse_erp_datetime(value: str | None) -> str | None:
    if not value:
        return None
    parsed = _parse_datetime(value)
    if not parsed:
        return None
    return parsed.isoformat(sep=" ").replace("+00:00", "Z")


def _load_sample_rows(path: Path, limit: int = 200) -> List[List[str]]:
    rows = []
    for row in _iter_csv_rows(path):
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _infer_e410pct_indices(path: Path) -> Dict[str, int]:
    rows = _load_sample_rows(path, limit=200)
    if not rows:
        raise ValueError("Arquivo E410PCT vazio.")
    max_cols = max(len(row) for row in rows)

    # NumPct: coluna numerica com maior diversidade.
    numpct_idx = None
    best_distinct = -1
    for idx in range(max_cols):
        values = set()
        for row in rows:
            if idx >= len(row):
                continue
            raw = row[idx].strip()
            if not raw:
                continue
            try:
                value = int(float(raw))
            except ValueError:
                continue
            values.add(value)
        if len(values) > best_distinct:
            best_distinct = len(values)
            numpct_idx = idx

    # DatAbe: coluna de data com mais valores validos (nao sentinela).
    databe_idx = None
    best_dates = -1
    for idx in range(max_cols):
        count = 0
        for row in rows:
            if idx >= len(row):
                continue
            value = row[idx].strip()
            if not value:
                continue
            parsed = _parse_erp_datetime(value)
            if parsed:
                count += 1
        if count > best_dates:
            best_dates = count
            databe_idx = idx

    if numpct_idx is None:
        raise ValueError("Nao foi possivel inferir NumPct no E410PCT.")

    return {"NumPct": numpct_idx, "DatAbe": databe_idx if databe_idx is not None else -1}


def _upsert_purchase_request(
    db: Database,
    tenant_id: str,
    num_sol: str,
    dat_efc: str | None,
    num_cot: str | None,
    num_pct: str | None,
    department: str | None,
) -> None:
    existing = db.execute(
        """
        SELECT id, erp_sent_at
        FROM purchase_requests
        WHERE external_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (num_sol, tenant_id),
    ).fetchone()

    erp_sent_at = dat_efc or None
    if existing:
        current_sent_at = existing["erp_sent_at"]
        if current_sent_at:
            current_dt = _parse_datetime(current_sent_at)
            incoming_dt = _parse_datetime(erp_sent_at) if erp_sent_at else None
            if current_dt and incoming_dt and incoming_dt > current_dt:
                erp_sent_at = current_sent_at

        db.execute(
            """
            UPDATE purchase_requests
            SET erp_num_cot = COALESCE(?, erp_num_cot),
                erp_num_pct = COALESCE(?, erp_num_pct),
                erp_sent_at = COALESCE(?, erp_sent_at),
                department = COALESCE(?, department),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (num_cot, num_pct, erp_sent_at, department, existing["id"], tenant_id),
        )
        return

    db.execute(
        """
        INSERT INTO purchase_requests (
            number,
            status,
            priority,
            requested_by,
            department,
            needed_at,
            erp_num_cot,
            erp_num_pct,
            erp_sent_at,
            external_id,
            tenant_id,
            created_at,
            updated_at
        ) VALUES (?, 'pending_rfq', 'medium', NULL, ?, NULL, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (
            num_sol,
            department,
            num_cot,
            num_pct,
            erp_sent_at,
            num_sol,
            tenant_id,
        ),
    )


def _ensure_supplier(db: Database, tenant_id: str, external_id: str) -> None:
    exists = db.execute(
        """
        SELECT id
        FROM suppliers
        WHERE external_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (external_id, tenant_id),
    ).fetchone()
    if exists:
        return
    db.execute(
        """
        INSERT INTO suppliers (name, external_id, tenant_id, created_at, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (f"Fornecedor ERP {external_id}", external_id, tenant_id),
    )


def _upsert_erp_quote(
    db: Database,
    tenant_id: str,
    num_cot: str,
    cod_for: str | None,
    dat_cot: str | None,
    hor_cot: str | None,
) -> None:
    quote_datetime = _combine_erp_datetime(dat_cot, hor_cot)
    db.execute(
        """
        INSERT INTO erp_supplier_quotes (
            tenant_id,
            erp_num_cot,
            erp_num_pct,
            supplier_external_id,
            quote_date,
            quote_time,
            quote_datetime,
            source_table,
            created_at,
            updated_at
        ) VALUES (?, ?, NULL, ?, ?, ?, ?, 'E410COT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(
            tenant_id,
            erp_num_cot,
            erp_num_pct,
            supplier_external_id,
            quote_datetime,
            source_table
        ) DO UPDATE SET
            quote_date = excluded.quote_date,
            quote_time = excluded.quote_time,
            quote_datetime = excluded.quote_datetime,
            updated_at = excluded.updated_at
        """,
        (
            tenant_id,
            num_cot,
            cod_for,
            dat_cot,
            hor_cot,
            quote_datetime,
        ),
    )


def import_e405sol(db: Database, tenant_id: str, schema_path: Path, data_path: Path) -> None:
    columns = _load_schema_columns(schema_path, "E405SOL")
    index = {name: columns.index(name) for name in columns}

    required = ["NumSol", "DatEfc", "NumCot", "NumPct"]
    for name in required:
        if name not in index:
            raise ValueError(f"Campo ausente na tabela E405SOL: {name}")

    department_idx = index.get("CodDep")

    for row in _iter_csv_rows(data_path):
        num_sol = _safe_value(row, index["NumSol"])
        if not num_sol:
            continue
        dat_efc = _safe_value(row, index["DatEfc"])
        num_cot = _safe_value(row, index["NumCot"])
        num_pct = _safe_value(row, index["NumPct"])
        department = _safe_value(row, department_idx) if department_idx is not None else None

        erp_sent_at = _parse_erp_datetime(dat_efc)
        _upsert_purchase_request(db, tenant_id, num_sol, erp_sent_at, num_cot, num_pct, department)


def import_e410cot(db: Database, tenant_id: str, schema_path: Path, data_path: Path) -> None:
    columns = _load_schema_columns(schema_path, "E410COT")
    index = {name: columns.index(name) for name in columns}

    required = ["NumCot", "CodFor", "DatCot", "HorCot"]
    for name in required:
        if name not in index:
            raise ValueError(f"Campo ausente na tabela E410COT: {name}")

    for row in _iter_csv_rows(data_path):
        num_cot = _safe_value(row, index["NumCot"])
        if not num_cot:
            continue
        cod_for = _safe_value(row, index["CodFor"])
        dat_cot = _safe_value(row, index["DatCot"])
        hor_cot = _safe_value(row, index["HorCot"])

        if cod_for:
            _ensure_supplier(db, tenant_id, cod_for)
        _upsert_erp_quote(db, tenant_id, num_cot, cod_for, dat_cot, hor_cot)


def import_e410pct(db: Database, tenant_id: str, schema_path: Path, data_path: Path) -> None:
    columns = _load_schema_columns(schema_path, "E410PCT")
    if not columns:
        fallback_schema = Path("e410pct inteira.csv")
        if fallback_schema.exists():
            columns = _load_schema_columns(fallback_schema, "E410PCT")

    if not columns:
        inferred = _infer_e410pct_indices(data_path)
        index = {"NumPct": inferred["NumPct"]}
        opened_idx = inferred["DatAbe"] if inferred["DatAbe"] >= 0 else None
    else:
        index = {name: columns.index(name) for name in columns}
        required = ["NumPct"]
        for name in required:
            if name not in index:
                raise ValueError(f"Campo ausente na tabela E410PCT: {name}")
        opened_idx = index.get("DatGer")
        if opened_idx is None:
            opened_idx = index.get("DatEnv")
        if opened_idx is None:
            opened_idx = index.get("DatAbe")

    for row in _iter_csv_rows(data_path):
        num_pct = _safe_value(row, index["NumPct"])
        if not num_pct:
            continue
        opened_at = _safe_value(row, opened_idx) if opened_idx is not None else None
        opened_at = _parse_erp_datetime(opened_at)
        db.execute(
            """
            INSERT INTO erp_quote_processes (
                tenant_id,
                erp_num_pct,
                opened_at,
                source_table,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, 'E410PCT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(tenant_id, erp_num_pct, source_table) DO UPDATE SET
                opened_at = excluded.opened_at,
                updated_at = excluded.updated_at
            """,
            (tenant_id, num_pct, opened_at),
        )


def import_e410fpc(db: Database, tenant_id: str, schema_path: Path, data_path: Path) -> None:
    columns = _load_schema_columns(schema_path, "E410FPC")
    if not columns:
        raise ValueError("Tabela E410FPC nao encontrada no schema.")
    index = {name: columns.index(name) for name in columns}

    required = ["NumPct", "CodFor"]
    for name in required:
        if name not in index:
            raise ValueError(f"Campo ausente na tabela E410FPC: {name}")

    for row in _iter_csv_rows(data_path):
        num_pct = _safe_value(row, index["NumPct"])
        cod_for = _safe_value(row, index["CodFor"])
        if not num_pct or not cod_for:
            continue
        _ensure_supplier(db, tenant_id, cod_for)
        db.execute(
            """
            INSERT INTO erp_quote_suppliers (
                tenant_id,
                erp_num_pct,
                supplier_external_id,
                source_table,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, 'E410FPC', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(tenant_id, erp_num_pct, supplier_external_id, source_table) DO UPDATE SET
                updated_at = excluded.updated_at
            """,
            (tenant_id, num_pct, cod_for),
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa CSVs ERP (E405SOL, E410COT).")
    parser.add_argument("--schema", default="tabelas.csv", help="CSV de schema do Senior.")
    parser.add_argument("--e405sol", required=True, help="CSV de dados E405SOL.")
    parser.add_argument("--e410cot", required=True, help="CSV de dados E410COT.")
    parser.add_argument("--e410pct", help="CSV de dados E410PCT.")
    parser.add_argument("--e410fpc", help="CSV de dados E410FPC.")
    parser.add_argument("--db", default=Config.DB_PATH, help="SQLite DB path.")
    parser.add_argument("--tenant", default="tenant-demo", help="Tenant id.")
    args = parser.parse_args()

    schema_path = Path(args.schema)
    e405_path = Path(args.e405sol)
    e410_path = Path(args.e410cot)

    if not schema_path.exists():
        raise FileNotFoundError(schema_path)
    if not e405_path.exists():
        raise FileNotFoundError(e405_path)
    if not e410_path.exists():
        raise FileNotFoundError(e410_path)

    db = _connect_database(args.db)

    try:
        import_e405sol(db, args.tenant, schema_path, e405_path)
        import_e410cot(db, args.tenant, schema_path, e410_path)
        if args.e410pct:
            import_e410pct(db, args.tenant, schema_path, Path(args.e410pct))
        if args.e410fpc:
            import_e410fpc(db, args.tenant, schema_path, Path(args.e410fpc))
        db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    main()
