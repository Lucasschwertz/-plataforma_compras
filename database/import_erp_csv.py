from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

# Permite executar este script diretamente sem configurar PYTHONPATH manualmente.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.config import Config
from app.db import Database, _connect_database
from app.routes.procurement_routes import (
    _combine_erp_datetime,
    _parse_datetime,
    _upsert_purchase_order,
    _upsert_receipt,
)
from database.erp_mirror import ensure_mirror_tables, import_csv_into_mirror, load_schema_tables


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
) -> int:
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
        return int(existing["id"])

    cursor = db.execute(
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
    created = cursor.fetchone()
    return int(created["id"] if isinstance(created, dict) else created[0])


def _parse_float_value(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _build_item_description(
    cod_pro: str | None,
    cod_der: str | None,
    cod_ser: str | None,
    obs_sol: str | None,
    pro_ser: str | None,
) -> str:
    parts: List[str] = []
    if cod_pro:
        if cod_der:
            parts.append(f"Produto {cod_pro}/{cod_der}")
        else:
            parts.append(f"Produto {cod_pro}")
    elif cod_ser:
        parts.append(f"Servico {cod_ser}")
    elif pro_ser:
        parts.append(f"Item {pro_ser}")
    else:
        parts.append("Item ERP")

    if obs_sol:
        obs_clean = " ".join(obs_sol.split())
        if obs_clean:
            parts.append(obs_clean[:120])
    return " | ".join(parts)


def _upsert_purchase_request_item(
    db: Database,
    tenant_id: str,
    purchase_request_id: int,
    line_no: int,
    description: str,
    quantity: float,
    uom: str,
    category: str | None,
) -> None:
    existing = db.execute(
        """
        SELECT id
        FROM purchase_request_items
        WHERE purchase_request_id = ? AND line_no = ? AND tenant_id = ?
        LIMIT 1
        """,
        (purchase_request_id, line_no, tenant_id),
    ).fetchone()
    if existing:
        db.execute(
            """
            UPDATE purchase_request_items
            SET description = ?,
                quantity = ?,
                uom = ?,
                category = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (description, quantity, uom, category, int(existing["id"]), tenant_id),
        )
        return

    db.execute(
        """
        INSERT INTO purchase_request_items (
            purchase_request_id, line_no, description, quantity, uom, category, tenant_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (purchase_request_id, line_no, description, quantity, uom, category, tenant_id),
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

    required = ["NumSol"]
    for name in required:
        if name not in index:
            raise ValueError(f"Campo ausente na tabela E405SOL: {name}")

    department_idx = index.get("CodDep")
    dat_efc_idx = index.get("DatEfc")
    num_cot_idx = index.get("NumCot")
    num_pct_idx = index.get("NumPct")

    seq_sol_idx = index.get("SeqSol")
    qtd_apr_idx = index.get("QtdApr")
    qtd_sol_idx = index.get("QtdSol")
    uni_med_idx = index.get("UniMed")
    pro_ser_idx = index.get("ProSer")
    cod_pro_idx = index.get("CodPro")
    cod_der_idx = index.get("CodDer")
    cod_ser_idx = index.get("CodSer")
    obs_sol_idx = index.get("ObsSol")

    for row in _iter_csv_rows(data_path):
        num_sol = _safe_value(row, index["NumSol"])
        if not num_sol:
            continue
        dat_efc = _safe_value(row, dat_efc_idx) if dat_efc_idx is not None else None
        num_cot = _safe_value(row, num_cot_idx) if num_cot_idx is not None else None
        num_pct = _safe_value(row, num_pct_idx) if num_pct_idx is not None else None
        department = _safe_value(row, department_idx) if department_idx is not None else None

        erp_sent_at = _parse_erp_datetime(dat_efc)
        purchase_request_id = _upsert_purchase_request(
            db,
            tenant_id,
            num_sol,
            erp_sent_at,
            num_cot,
            num_pct,
            department,
        )

        line_no = _safe_value(row, seq_sol_idx) if seq_sol_idx is not None else None
        try:
            parsed_line = int(line_no) if line_no else None
        except ValueError:
            parsed_line = None
        if parsed_line is None or parsed_line <= 0:
            parsed_line = 1

        qtd_apr = _safe_value(row, qtd_apr_idx) if qtd_apr_idx is not None else None
        qtd_sol = _safe_value(row, qtd_sol_idx) if qtd_sol_idx is not None else None
        quantity = _parse_float_value(qtd_apr) or _parse_float_value(qtd_sol) or 1.0

        uom = _safe_value(row, uni_med_idx) if uni_med_idx is not None else None
        if not uom:
            uom = "UN"

        pro_ser = _safe_value(row, pro_ser_idx) if pro_ser_idx is not None else None
        cod_pro = _safe_value(row, cod_pro_idx) if cod_pro_idx is not None else None
        cod_der = _safe_value(row, cod_der_idx) if cod_der_idx is not None else None
        cod_ser = _safe_value(row, cod_ser_idx) if cod_ser_idx is not None else None
        obs_sol = _safe_value(row, obs_sol_idx) if obs_sol_idx is not None else None

        description = _build_item_description(cod_pro, cod_der, cod_ser, obs_sol, pro_ser)
        _upsert_purchase_request_item(
            db=db,
            tenant_id=tenant_id,
            purchase_request_id=purchase_request_id,
            line_no=parsed_line,
            description=description,
            quantity=quantity,
            uom=uom,
            category=pro_ser,
        )


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


def _is_header_row(row: List[str], columns: List[str]) -> bool:
    if not row or not columns:
        return False
    compare = min(len(row), len(columns), 24)
    if compare < 2:
        return False
    expected = {str(column).strip().lower() for column in columns}
    matches = 0
    for idx in range(compare):
        if str(row[idx]).strip().lower() in expected:
            matches += 1
    return matches >= max(2, int(compare * 0.7))


def _load_table_records(schema_path: Path, table: str, data_path: Path) -> List[dict]:
    columns = _load_schema_columns(schema_path, table)
    if not columns:
        return []

    records: List[dict] = []
    first_data_row = True
    for row in _iter_csv_rows(data_path):
        if first_data_row and _is_header_row(row, columns):
            first_data_row = False
            continue
        first_data_row = False

        payload: dict = {}
        for idx, name in enumerate(columns):
            value = _safe_value(row, idx)
            if value is not None:
                payload[name] = value
        if payload:
            records.append(payload)
    return records


def _normalize_po_status(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"approved", "aprovada", "2", "a"}:
        return "approved"
    if raw in {"sent_to_erp", "enviado_erp", "3"}:
        return "sent_to_erp"
    if raw in {"erp_accepted", "aceita", "4"}:
        return "erp_accepted"
    if raw in {"cancelled", "cancelada", "9", "c"}:
        return "cancelled"
    if raw in {"erp_error", "erro", "e"}:
        return "erp_error"
    return "draft"


def _normalize_receipt_status(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"partially", "partial", "parcial", "partially_received", "2"}:
        return "partially_received"
    if raw in {"received", "recebido", "recebida", "3"}:
        return "received"
    return "pending"


def import_e420ocp_e420ipo(
    db: Database,
    tenant_id: str,
    schema_path: Path,
    e420ocp_path: Path,
    e420ipo_path: Path | None,
) -> int:
    purchase_orders = _load_table_records(schema_path, "E420OCP", e420ocp_path)
    po_items = _load_table_records(schema_path, "E420IPO", e420ipo_path) if e420ipo_path else []

    items_by_ocp: Dict[str, List[dict]] = defaultdict(list)
    for item in po_items:
        num_ocp = str(item.get("NumOcp") or "").strip()
        if not num_ocp:
            continue
        item["source_table"] = "E420IPO"
        items_by_ocp[num_ocp].append(item)

    imported = 0
    for po in purchase_orders:
        num_ocp = str(po.get("NumOcp") or "").strip()
        if not num_ocp:
            continue
        po["status"] = _normalize_po_status(po.get("SitOcp"))
        po["source_table"] = "E420OCP"
        po["items"] = items_by_ocp.get(num_ocp, [])
        imported += _upsert_purchase_order(db, tenant_id, po)
    return imported


def import_e440nfc_e440ipc(
    db: Database,
    tenant_id: str,
    schema_path: Path,
    e440nfc_path: Path,
    e440ipc_path: Path | None,
) -> int:
    receipts = _load_table_records(schema_path, "E440NFC", e440nfc_path)
    receipt_items = _load_table_records(schema_path, "E440IPC", e440ipc_path) if e440ipc_path else []

    items_by_nfc: Dict[str, List[dict]] = defaultdict(list)
    for item in receipt_items:
        num_nfc = str(item.get("NumNfc") or "").strip()
        if not num_nfc:
            continue
        item["source_table"] = "E440IPC"
        items_by_nfc[num_nfc].append(item)

    imported = 0
    for receipt in receipts:
        num_nfc = str(receipt.get("NumNfc") or "").strip()
        if not num_nfc:
            continue
        receipt["status"] = _normalize_receipt_status(receipt.get("SitNfc"))
        receipt["source_table"] = "E440NFC"
        receipt["items"] = items_by_nfc.get(num_nfc, [])
        imported += _upsert_receipt(db, tenant_id, receipt)
    return imported


def _parse_raw_table_arg(raw_value: str) -> tuple[str, Path]:
    text = str(raw_value or "").strip()
    if not text or "=" not in text:
        raise ValueError("Formato invalido em --raw-table. Use TABELA=arquivo.csv")
    table_name, file_path = text.split("=", 1)
    table_name = table_name.strip().upper()
    if not table_name:
        raise ValueError("Tabela invalida em --raw-table.")
    path = Path(file_path.strip())
    if not path.exists():
        raise FileNotFoundError(path)
    return table_name, path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Importa CSVs ERP para espelho bruto e para o dominio da plataforma."
    )
    parser.add_argument("--schema", default="tabelas.csv", help="CSV de schema do Senior.")
    parser.add_argument("--e405sol", help="CSV de dados E405SOL.")
    parser.add_argument("--e410cot", help="CSV de dados E410COT.")
    parser.add_argument("--e410pct", help="CSV de dados E410PCT.")
    parser.add_argument("--e410fpc", help="CSV de dados E410FPC.")
    parser.add_argument("--e420ocp", help="CSV de dados E420OCP.")
    parser.add_argument("--e420ipo", help="CSV de dados E420IPO.")
    parser.add_argument("--e440nfc", help="CSV de dados E440NFC.")
    parser.add_argument("--e440ipc", help="CSV de dados E440IPC.")
    parser.add_argument(
        "--raw-table",
        action="append",
        default=[],
        help="Importacao raw no formato TABELA=arquivo.csv (pode repetir).",
    )
    parser.add_argument(
        "--mirror-schema-only",
        action="store_true",
        help="Cria todas as tabelas espelho do ERP e encerra sem importar dominio.",
    )
    parser.add_argument(
        "--truncate-raw",
        action="store_true",
        help="Apaga dados do tenant nas tabelas raw antes de importar.",
    )
    parser.add_argument("--db", default=Config.DB_PATH, help="SQLite DB path.")
    parser.add_argument("--tenant", default="tenant-demo", help="Tenant id.")
    args = parser.parse_args()

    schema_path = Path(args.schema)
    e405_path = Path(args.e405sol) if args.e405sol else None
    e410_path = Path(args.e410cot) if args.e410cot else None
    e410pct_path = Path(args.e410pct) if args.e410pct else None
    e410fpc_path = Path(args.e410fpc) if args.e410fpc else None
    e420ocp_path = Path(args.e420ocp) if args.e420ocp else None
    e420ipo_path = Path(args.e420ipo) if args.e420ipo else None
    e440nfc_path = Path(args.e440nfc) if args.e440nfc else None
    e440ipc_path = Path(args.e440ipc) if args.e440ipc else None

    if not schema_path.exists():
        raise FileNotFoundError(schema_path)
    for path in [
        e405_path,
        e410_path,
        e410pct_path,
        e410fpc_path,
        e420ocp_path,
        e420ipo_path,
        e440nfc_path,
        e440ipc_path,
    ]:
        if path and not path.exists():
            raise FileNotFoundError(path)

    raw_inputs: List[tuple[str, Path]] = []
    if e405_path:
        raw_inputs.append(("E405SOL", e405_path))
    if e410_path:
        raw_inputs.append(("E410COT", e410_path))
    if e410pct_path:
        raw_inputs.append(("E410PCT", e410pct_path))
    if e410fpc_path:
        raw_inputs.append(("E410FPC", e410fpc_path))
    if e420ocp_path:
        raw_inputs.append(("E420OCP", e420ocp_path))
    if e420ipo_path:
        raw_inputs.append(("E420IPO", e420ipo_path))
    if e440nfc_path:
        raw_inputs.append(("E440NFC", e440nfc_path))
    if e440ipc_path:
        raw_inputs.append(("E440IPC", e440ipc_path))
    for raw_arg in args.raw_table:
        raw_inputs.append(_parse_raw_table_arg(raw_arg))

    if not args.mirror_schema_only and not raw_inputs:
        parser.error("Informe pelo menos um CSV para importar, ou use --mirror-schema-only.")

    db = _connect_database(args.db)

    try:
        schema_tables = load_schema_tables(schema_path)
        created_tables = ensure_mirror_tables(db, schema_tables)

        raw_counts: Dict[str, int] = {}
        for table_name, file_path in raw_inputs:
            count = import_csv_into_mirror(
                db=db,
                schema_tables=schema_tables,
                source_table=table_name,
                csv_path=file_path,
                tenant_id=args.tenant,
                truncate_tenant=args.truncate_raw,
            )
            raw_counts[table_name] = raw_counts.get(table_name, 0) + count

        domain_counts = {
            "purchase_requests": 0,
            "supplier_quotes": 0,
            "quote_processes": 0,
            "quote_suppliers": 0,
            "purchase_orders": 0,
            "receipts": 0,
        }

        if e405_path:
            import_e405sol(db, args.tenant, schema_path, e405_path)
            domain_counts["purchase_requests"] = raw_counts.get("E405SOL", 0)
        if e410_path:
            import_e410cot(db, args.tenant, schema_path, e410_path)
            domain_counts["supplier_quotes"] = raw_counts.get("E410COT", 0)
        if e410pct_path:
            import_e410pct(db, args.tenant, schema_path, e410pct_path)
            domain_counts["quote_processes"] = raw_counts.get("E410PCT", 0)
        if e410fpc_path:
            import_e410fpc(db, args.tenant, schema_path, e410fpc_path)
            domain_counts["quote_suppliers"] = raw_counts.get("E410FPC", 0)
        if e420ocp_path:
            domain_counts["purchase_orders"] = import_e420ocp_e420ipo(
                db=db,
                tenant_id=args.tenant,
                schema_path=schema_path,
                e420ocp_path=e420ocp_path,
                e420ipo_path=e420ipo_path,
            )
        if e440nfc_path:
            domain_counts["receipts"] = import_e440nfc_e440ipc(
                db=db,
                tenant_id=args.tenant,
                schema_path=schema_path,
                e440nfc_path=e440nfc_path,
                e440ipc_path=e440ipc_path,
            )

        db.commit()
        pr_total = db.execute(
            "SELECT COUNT(*) AS total FROM purchase_requests WHERE tenant_id = ?",
            (args.tenant,),
        ).fetchone()["total"]
        pri_total = db.execute(
            "SELECT COUNT(*) AS total FROM purchase_request_items WHERE tenant_id = ?",
            (args.tenant,),
        ).fetchone()["total"]
        erp_total = db.execute(
            "SELECT COUNT(*) AS total FROM purchase_requests WHERE tenant_id = ? AND external_id IS NOT NULL",
            (args.tenant,),
        ).fetchone()["total"]
        po_total = db.execute(
            "SELECT COUNT(*) AS total FROM purchase_orders WHERE tenant_id = ?",
            (args.tenant,),
        ).fetchone()["total"]
        receipt_total = db.execute(
            "SELECT COUNT(*) AS total FROM receipts WHERE tenant_id = ?",
            (args.tenant,),
        ).fetchone()["total"]
        raw_summary = ", ".join(f"{name}={count}" for name, count in sorted(raw_counts.items())) or "sem_csv"
        print(
            "Import concluido | "
            f"tenant={args.tenant} | "
            f"tabelas_espelho={created_tables} | "
            f"raw({raw_summary}) | "
            f"solicitacoes={pr_total} | "
            f"itens={pri_total} | "
            f"solicitacoes_erp={erp_total} | "
            f"ocs={po_total} | "
            f"recebimentos={receipt_total} | "
            f"dominio={domain_counts}"
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
