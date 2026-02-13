from __future__ import annotations

import csv
import json
import os
import ssl
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from flask import current_app

from app.contexts.erp.infrastructure.mock import DEFAULT_RISK_FLAGS, fetch_erp_records as fetch_mock_records, push_purchase_order as push_mock_po


def _assert_worker_import_context() -> None:
    context = str(os.environ.get("ERP_CLIENT_CONTEXT") or "").strip().lower()
    if context == "worker":
        return
    raise RuntimeError(
        "app.erp_client is restricted to worker context. "
        "Use app.domain.erp_gateway + app.infrastructure.erp.senior_erp_gateway in workers."
    )


_assert_worker_import_context()


class ErpError(RuntimeError):
    pass


def fetch_erp_records(
    entity: str,
    since_updated_at: str | None,
    since_id: str | None,
    limit: int = 100,
) -> List[dict]:
    mode = _get_config("ERP_MODE", "mock").lower()
    if mode == "mock":
        return fetch_mock_records(entity, since_updated_at, since_id, limit=limit)
    if mode == "senior_csv":
        return _fetch_senior_csv_records(entity, since_updated_at, since_id, limit=limit)
    if mode != "senior":
        raise ErpError(f"ERP_MODE invalido: {mode}")
    return _fetch_senior_records(entity, since_updated_at, since_id, limit=limit)


def push_purchase_order(purchase_order: dict) -> dict:
    mode = _get_config("ERP_MODE", "mock").lower()
    if mode == "mock":
        return push_mock_po(purchase_order)
    if mode == "senior_csv":
        response = push_mock_po(purchase_order)
        response["message"] = response.get("message") or "Bridge CSV, retorno simulado para outbound."
        return response
    if mode != "senior":
        raise ErpError(f"ERP_MODE invalido: {mode}")
    return _push_senior_purchase_order(purchase_order)


def _fetch_senior_csv_records(
    entity: str,
    since_updated_at: str | None,
    since_id: str | None,
    limit: int,
) -> List[dict]:
    schema_path = _resolve_csv_path(_get_config("ERP_CSV_SCHEMA", "tabelas.csv"))
    if not schema_path:
        raise ErpError("ERP_CSV_SCHEMA nao configurado para ERP_MODE=senior_csv.")

    if entity == "purchase_request":
        rows = _load_purchase_request_records_from_csv(schema_path)
    elif entity == "quote":
        rows = _load_quote_records_from_csv(schema_path)
    elif entity == "quote_process":
        rows = _load_quote_process_records_from_csv(schema_path)
    elif entity == "quote_supplier":
        rows = _load_quote_supplier_records_from_csv(schema_path)
    elif entity == "supplier":
        rows = _load_supplier_records_from_csv(schema_path)
    elif entity == "purchase_order":
        rows = _load_purchase_order_records_from_csv(schema_path)
    elif entity == "receipt":
        rows = _load_receipt_records_from_csv(schema_path)
    else:
        raise ErpError(f"Entidade ERP nao suportada no modo CSV: {entity}")

    ordered = sorted(rows, key=lambda record: (_sort_key_updated_at(record), _sort_key_external_id(record)))
    filtered = [record for record in ordered if _is_after_watermark(record, since_updated_at, since_id)]
    if limit <= 0:
        return filtered
    return filtered[:limit]


def _resolve_csv_path(raw_value: object | None) -> Path | None:
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    path = Path(value).expanduser()
    candidates = [path]

    if not path.is_absolute():
        candidates.append(Path.cwd() / path)
        try:
            app_root = Path(current_app.root_path).parent
            candidates.append(app_root / path)
        except RuntimeError:
            pass

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _load_schema_columns(schema_path: Path, table: str) -> List[str]:
    columns: List[tuple[int, str]] = []
    with schema_path.open("r", encoding="utf-8-sig", errors="ignore") as handle:
        reader = csv.reader(handle, delimiter=";")
        for row in reader:
            if len(row) < 13:
                continue
            if str(row[1]).strip().upper() != table.upper():
                continue
            try:
                order = int(str(row[12]).strip())
            except ValueError:
                continue
            field_name = str(row[3]).strip()
            if not field_name:
                continue
            columns.append((order, field_name))
    return [name for order, name in sorted(columns, key=lambda item: item[0])]


def _safe_value(row: List[str], idx: int | None) -> str | None:
    if idx is None:
        return None
    if idx < 0 or idx >= len(row):
        return None
    value = str(row[idx]).strip()
    if not value or value.upper() == "NULL":
        return None
    return value


def _value_by_names(row: List[str], index: dict[str, int], *names: str) -> str | None:
    for name in names:
        value = _safe_value(row, index.get(name))
        if value is not None:
            return value
    return None


def _is_header_cell(value: str | None, expected_name: str) -> bool:
    if not value:
        return False
    return value.strip().upper() == expected_name.strip().upper()


def _iter_csv_rows(path: Path) -> List[List[str]]:
    with path.open("r", encoding="utf-8-sig", errors="ignore") as handle:
        first_line = handle.readline()
        delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","
        handle.seek(0)
        reader = csv.reader(handle, delimiter=delimiter)
        return [row for row in reader if row]


def _build_column_index(schema_path: Path, table: str) -> dict[str, int]:
    columns = _load_schema_columns(schema_path, table)
    return {name: idx for idx, name in enumerate(columns)}


def _parse_iso_datetime(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    if value.startswith("1900-12-31"):
        return None

    if value.isdigit() and len(value) == 8:
        year = value[0:4]
        month = value[4:6]
        day = value[6:8]
        value = f"{year}-{month}-{day}"

    normalized = value.replace("T", " ").replace("/", "-")
    try:
        if normalized.endswith("Z"):
            parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        else:
            parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = None

    if parsed is None:
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(normalized, fmt)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_erp_time(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    if value.isdigit() and len(value) in (3, 4):
        padded = value.zfill(4)
        return f"{padded[:2]}:{padded[2:]}"
    if value.isdigit() and len(value) == 6:
        return f"{value[:2]}:{value[2:4]}:{value[4:]}"
    return value


def _normalize_decimal(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    if "," in value and "." in value:
        if value.rfind(",") > value.rfind("."):
            value = value.replace(".", "").replace(",", ".")
        else:
            value = value.replace(",", "")
    elif "," in value:
        value = value.replace(".", "").replace(",", ".")
    return value


def _combine_datetime(date_value: str | None, time_value: str | None) -> str | None:
    date_part = _parse_iso_datetime(date_value)
    if not date_part:
        return None
    if not time_value:
        return date_part
    time_part = _normalize_erp_time(time_value)
    if not time_part:
        return date_part
    base = date_part.replace("T", " ").replace("Z", "")
    date_only = base.split(" ", 1)[0]
    combined = _parse_iso_datetime(f"{date_only} {time_part}")
    return combined or date_part


def _stable_epoch() -> str:
    return "1970-01-01T00:00:00Z"


def _sort_key_updated_at(record: dict) -> str:
    return str(record.get("updated_at") or _stable_epoch())


def _sort_key_external_id(record: dict) -> str:
    return str(record.get("external_id") or "")


def _is_after_watermark(record: dict, since_updated_at: str | None, since_id: str | None) -> bool:
    current_updated = _sort_key_updated_at(record)
    current_id = _sort_key_external_id(record)

    if since_updated_at:
        if current_updated > since_updated_at:
            return True
        if current_updated < since_updated_at:
            return False
        if since_id:
            return current_id > str(since_id)
        return False

    if since_id:
        return current_id > str(since_id)
    return True


def _load_purchase_request_records_from_csv(schema_path: Path) -> List[dict]:
    data_path = _resolve_csv_path(_get_config("ERP_CSV_E405SOL"))
    if not data_path:
        raise ErpError("ERP_CSV_E405SOL nao configurado para sync de purchase_request.")
    index = _build_column_index(schema_path, "E405SOL")
    if "NumSol" not in index:
        raise ErpError("Schema E405SOL sem coluna NumSol.")

    result: List[dict] = []
    for row_number, row in enumerate(_iter_csv_rows(data_path), start=1):
        num_sol = _safe_value(row, index.get("NumSol"))
        if not num_sol:
            continue
        if _is_header_cell(num_sol, "NumSol"):
            continue
        dat_efc = _safe_value(row, index.get("DatEfc"))
        num_cot = _safe_value(row, index.get("NumCot"))
        num_pct = _safe_value(row, index.get("NumPct"))
        cod_dep = _safe_value(row, index.get("CodDep"))
        seq_sol = _safe_value(row, index.get("SeqSol"))
        qtd_apr = _safe_value(row, index.get("QtdApr"))
        qtd_sol = _safe_value(row, index.get("QtdSol"))
        uni_med = _safe_value(row, index.get("UniMed"))
        pro_ser = _safe_value(row, index.get("ProSer"))
        cod_pro = _safe_value(row, index.get("CodPro"))
        cod_der = _safe_value(row, index.get("CodDer"))
        cod_ser = _safe_value(row, index.get("CodSer"))
        obs_sol = _safe_value(row, index.get("ObsSol"))
        updated_at = _parse_iso_datetime(dat_efc) or _stable_epoch()
        line_key = seq_sol or str(row_number)
        result.append(
            {
                # External row id no bridge CSV, para watermark incremental por item da solicitacao.
                "external_id": f"{num_sol}:{line_key}",
                "updated_at": updated_at,
                "NumSol": num_sol,
                "DatEfc": dat_efc,
                "NumCot": num_cot,
                "NumPct": num_pct,
                "CodDep": cod_dep,
                "SeqSol": seq_sol,
                "QtdApr": qtd_apr,
                "QtdSol": qtd_sol,
                "UniMed": uni_med,
                "ProSer": pro_ser,
                "CodPro": cod_pro,
                "CodDer": cod_der,
                "CodSer": cod_ser,
                "ObsSol": obs_sol,
            }
        )
    return result


def _load_quote_records_from_csv(schema_path: Path) -> List[dict]:
    data_path = _resolve_csv_path(_get_config("ERP_CSV_E410COT"))
    if not data_path:
        raise ErpError("ERP_CSV_E410COT nao configurado para sync de quote.")
    index = _build_column_index(schema_path, "E410COT")
    if "NumCot" not in index:
        raise ErpError("Schema E410COT sem coluna NumCot.")
    result: List[dict] = []
    for row in _iter_csv_rows(data_path):
        num_cot = _safe_value(row, index.get("NumCot"))
        if not num_cot:
            continue
        if _is_header_cell(num_cot, "NumCot"):
            continue
        cod_for = _safe_value(row, index.get("CodFor"))
        num_pct = _safe_value(row, index.get("NumPct"))
        dat_cot = _safe_value(row, index.get("DatCot"))
        hor_cot = _safe_value(row, index.get("HorCot"))
        quote_datetime = _combine_datetime(dat_cot, hor_cot)
        updated_at = quote_datetime or _stable_epoch()
        external_id = f"{num_cot}:{cod_for or 'na'}:{quote_datetime or 'na'}"
        result.append(
            {
                "external_id": external_id,
                "updated_at": updated_at,
                "NumCot": num_cot,
                "NumPct": num_pct,
                "CodFor": cod_for,
                "DatCot": dat_cot,
                "HorCot": hor_cot,
            }
        )
    return result


def _load_quote_process_records_from_csv(schema_path: Path) -> List[dict]:
    data_path = _resolve_csv_path(_get_config("ERP_CSV_E410PCT"))
    if not data_path:
        raise ErpError("ERP_CSV_E410PCT nao configurado para sync de quote_process.")
    index = _build_column_index(schema_path, "E410PCT")
    if "NumPct" not in index:
        raise ErpError("Schema E410PCT sem coluna NumPct.")
    date_column = index.get("DatAbe")
    if date_column is None:
        date_column = index.get("DatGer")
    if date_column is None:
        date_column = index.get("DatEnv")

    result: List[dict] = []
    for row in _iter_csv_rows(data_path):
        num_pct = _safe_value(row, index.get("NumPct"))
        if not num_pct:
            continue
        if _is_header_cell(num_pct, "NumPct"):
            continue
        opened_raw = _safe_value(row, date_column)
        opened_at = _parse_iso_datetime(opened_raw)
        result.append(
            {
                "external_id": num_pct,
                "updated_at": opened_at or _stable_epoch(),
                "NumPct": num_pct,
                "DatAbe": opened_raw,
            }
        )
    return result


def _load_quote_supplier_records_from_csv(schema_path: Path) -> List[dict]:
    data_path = _resolve_csv_path(_get_config("ERP_CSV_E410FPC"))
    if not data_path:
        raise ErpError("ERP_CSV_E410FPC nao configurado para sync de quote_supplier.")
    index = _build_column_index(schema_path, "E410FPC")
    if "NumPct" not in index or "CodFor" not in index:
        raise ErpError("Schema E410FPC sem colunas NumPct ou CodFor.")
    result: List[dict] = []
    for row in _iter_csv_rows(data_path):
        num_pct = _safe_value(row, index.get("NumPct"))
        cod_for = _safe_value(row, index.get("CodFor"))
        if not num_pct or not cod_for:
            continue
        if _is_header_cell(num_pct, "NumPct") or _is_header_cell(cod_for, "CodFor"):
            continue
        external_id = f"{num_pct}:{cod_for}"
        result.append(
            {
                "external_id": external_id,
                "updated_at": _stable_epoch(),
                "NumPct": num_pct,
                "CodFor": cod_for,
            }
        )
    return result


def _map_purchase_order_status(raw_status: str | None) -> str:
    if raw_status is None:
        return "erp_accepted"
    value = str(raw_status).strip().lower()
    if not value:
        return "erp_accepted"

    direct = {
        "0": "draft",
        "1": "approved",
        "2": "sent_to_erp",
        "3": "erp_accepted",
        "4": "partially_received",
        "5": "received",
        "9": "cancelled",
    }
    if value in direct:
        return direct[value]

    if "cancel" in value or "canc" in value:
        return "cancelled"
    if "erro" in value or "error" in value:
        return "erp_error"
    if "receb" in value or "received" in value:
        return "received"
    if "parcial" in value or "partial" in value:
        return "partially_received"
    if "envi" in value or "sent" in value:
        return "sent_to_erp"
    if "aprov" in value or "liber" in value or "approved" in value:
        return "approved"
    return "erp_accepted"


def _map_receipt_status(raw_status: str | None) -> str:
    if raw_status is None:
        return "received"
    value = str(raw_status).strip().lower()
    if not value:
        return "received"

    direct = {
        "0": "pending",
        "1": "partially_received",
        "2": "received",
        "p": "pending",
        "par": "partially_received",
        "r": "received",
    }
    if value in direct:
        return direct[value]

    if "pend" in value:
        return "pending"
    if "parcial" in value or "partial" in value:
        return "partially_received"
    if "receb" in value or "received" in value or "entreg" in value:
        return "received"
    return "received"


def _load_purchase_order_records_from_csv(schema_path: Path) -> List[dict]:
    data_path = _resolve_csv_path(_get_config("ERP_CSV_E420OCP"))
    if not data_path:
        return []
    index = _build_column_index(schema_path, "E420OCP")
    if "NumOcp" not in index:
        raise ErpError("Schema E420OCP sem coluna NumOcp.")
    items_by_order = _load_purchase_order_item_records_from_csv(schema_path)

    result: List[dict] = []
    for row in _iter_csv_rows(data_path):
        num_ocp = _safe_value(row, index.get("NumOcp"))
        if not num_ocp:
            continue
        if _is_header_cell(num_ocp, "NumOcp"):
            continue

        cod_for = _value_by_names(row, index, "CodFor")
        cod_moe = _value_by_names(row, index, "CodMoe")
        status_raw = _value_by_names(row, index, "SitOcp", "SitApr", "SitPed")
        total_raw = _value_by_names(row, index, "VlrOcp", "VlrLiq", "VlrBru")
        date_raw = _value_by_names(row, index, "DatEmi", "DatGer", "DatAbe")
        time_raw = _value_by_names(row, index, "HorEmi", "HorGer")
        updated_at = _combine_datetime(date_raw, time_raw) or _stable_epoch()

        result.append(
            {
                "external_id": num_ocp,
                "updated_at": updated_at,
                "NumOcp": num_ocp,
                "status": _map_purchase_order_status(status_raw),
                "supplier_name": f"Fornecedor ERP {cod_for}" if cod_for else None,
                "CodMoe": cod_moe or "BRL",
                "VlrOcp": _normalize_decimal(total_raw),
                "CodFor": cod_for,
                "items": items_by_order.get(num_ocp, []),
            }
        )
    return result


def _load_receipt_records_from_csv(schema_path: Path) -> List[dict]:
    data_path = _resolve_csv_path(_get_config("ERP_CSV_E440NFC"))
    if not data_path:
        return []
    index = _build_column_index(schema_path, "E440NFC")
    if "NumNfc" not in index:
        raise ErpError("Schema E440NFC sem coluna NumNfc.")
    items_by_receipt = _load_receipt_item_records_from_csv(schema_path)

    result: List[dict] = []
    for row in _iter_csv_rows(data_path):
        num_nfc = _safe_value(row, index.get("NumNfc"))
        if not num_nfc:
            continue
        if _is_header_cell(num_nfc, "NumNfc"):
            continue

        num_ocp = _value_by_names(row, index, "NumOcp")
        status_raw = _value_by_names(row, index, "SitNfc", "SitNfv", "SitNf")
        rec_date = _value_by_names(row, index, "DatRec", "DatEnt", "DatEmi")
        rec_time = _value_by_names(row, index, "HorRec", "HorEnt", "HorEmi")
        received_at = _combine_datetime(rec_date, rec_time)
        updated_at = received_at or _stable_epoch()
        external_id = f"{num_nfc}:{num_ocp or 'na'}"

        result.append(
            {
                "external_id": external_id,
                "updated_at": updated_at,
                "NumNfc": num_nfc,
                "NumOcp": num_ocp,
                "DatRec": rec_date,
                "status": _map_receipt_status(status_raw),
                "received_at": received_at,
                "items": items_by_receipt.get(num_nfc, []),
            }
        )
    return result


def _load_purchase_order_item_records_from_csv(schema_path: Path) -> dict[str, List[dict]]:
    data_path = _resolve_csv_path(_get_config("ERP_CSV_E420IPO"))
    if not data_path:
        return {}
    index = _build_column_index(schema_path, "E420IPO")
    if "NumOcp" not in index:
        return {}

    grouped: dict[str, List[dict]] = {}
    for row_index, row in enumerate(_iter_csv_rows(data_path), start=1):
        num_ocp = _safe_value(row, index.get("NumOcp"))
        if not num_ocp:
            continue
        if _is_header_cell(num_ocp, "NumOcp"):
            continue

        line_no = _value_by_names(row, index, "SeqIpo", "NumSeq", "SeqPed")
        cod_pro = _value_by_names(row, index, "CodPro")
        des_pro = _value_by_names(row, index, "DesPro")
        qty = _normalize_decimal(_value_by_names(row, index, "QtdPed", "QtdAte", "QtdApr"))
        unit_price = _normalize_decimal(_value_by_names(row, index, "PreUni", "VlrUni"))
        total_price = _normalize_decimal(_value_by_names(row, index, "VlrTot"))
        if total_price is None and qty and unit_price:
            try:
                total_price = str(float(qty) * float(unit_price))
            except ValueError:
                total_price = None

        parsed_line = 0
        if line_no and line_no.isdigit():
            parsed_line = int(line_no)
        elif line_no:
            try:
                parsed_line = int(float(line_no))
            except ValueError:
                parsed_line = row_index
        else:
            parsed_line = row_index

        item = {
            "line_no": parsed_line,
            "product_code": cod_pro,
            "description": des_pro,
            "quantity": qty,
            "unit_price": unit_price,
            "total_price": total_price,
            "source_table": "E420IPO",
            "external_id": f"{num_ocp}:{parsed_line}:{cod_pro or 'na'}",
        }
        grouped.setdefault(num_ocp, []).append(item)
    return grouped


def _load_receipt_item_records_from_csv(schema_path: Path) -> dict[str, List[dict]]:
    grouped: dict[str, List[dict]] = {}

    path_ipc = _resolve_csv_path(_get_config("ERP_CSV_E440IPC"))
    if path_ipc:
        index = _build_column_index(schema_path, "E440IPC")
        if "NumNfc" in index:
            for row_index, row in enumerate(_iter_csv_rows(path_ipc), start=1):
                num_nfc = _safe_value(row, index.get("NumNfc"))
                if not num_nfc:
                    continue
                if _is_header_cell(num_nfc, "NumNfc"):
                    continue
                line_no = _value_by_names(row, index, "SeqIpc", "NumSeq", "SeqNfc")
                cod_pro = _value_by_names(row, index, "CodPro")
                qty = _normalize_decimal(_value_by_names(row, index, "QtdRec", "QtdEnt", "QtdIpc"))
                num_ocp = _value_by_names(row, index, "NumOcp")

                parsed_line = row_index
                if line_no:
                    try:
                        parsed_line = int(float(line_no))
                    except ValueError:
                        parsed_line = row_index

                grouped.setdefault(num_nfc, []).append(
                    {
                        "line_no": parsed_line,
                        "product_code": cod_pro,
                        "quantity_received": qty,
                        "source_table": "E440IPC",
                        "external_id": f"{num_nfc}:{parsed_line}:{cod_pro or 'na'}",
                        "NumOcp": num_ocp,
                    }
                )

    path_isc = _resolve_csv_path(_get_config("ERP_CSV_E440ISC"))
    if path_isc:
        index = _build_column_index(schema_path, "E440ISC")
        if "NumNfc" in index:
            for row_index, row in enumerate(_iter_csv_rows(path_isc), start=1):
                num_nfc = _safe_value(row, index.get("NumNfc"))
                if not num_nfc:
                    continue
                if _is_header_cell(num_nfc, "NumNfc"):
                    continue
                line_no = _value_by_names(row, index, "SeqIsc", "NumSeq", "SeqNfc")
                cod_pro = _value_by_names(row, index, "CodSer", "CodPro")
                qty = _normalize_decimal(_value_by_names(row, index, "QtdRec", "QtdEnt", "QtdIsc"))
                num_ocp = _value_by_names(row, index, "NumOcp")

                parsed_line = row_index
                if line_no:
                    try:
                        parsed_line = int(float(line_no))
                    except ValueError:
                        parsed_line = row_index

                grouped.setdefault(num_nfc, []).append(
                    {
                        "line_no": parsed_line,
                        "product_code": cod_pro,
                        "quantity_received": qty,
                        "source_table": "E440ISC",
                        "external_id": f"{num_nfc}:{parsed_line}:{cod_pro or 'na'}",
                        "NumOcp": num_ocp,
                    }
                )

    return grouped


def _load_supplier_records_from_csv(schema_path: Path) -> List[dict]:
    supplier_ids: set[str] = set()
    from_quotes = _resolve_csv_path(_get_config("ERP_CSV_E410COT"))
    if from_quotes:
        quote_index = _build_column_index(schema_path, "E410COT")
        for row in _iter_csv_rows(from_quotes):
            cod_for = _safe_value(row, quote_index.get("CodFor"))
            if cod_for:
                if _is_header_cell(cod_for, "CodFor"):
                    continue
                supplier_ids.add(cod_for)

    from_quote_supplier = _resolve_csv_path(_get_config("ERP_CSV_E410FPC"))
    if from_quote_supplier:
        quote_supplier_index = _build_column_index(schema_path, "E410FPC")
        for row in _iter_csv_rows(from_quote_supplier):
            cod_for = _safe_value(row, quote_supplier_index.get("CodFor"))
            if cod_for:
                if _is_header_cell(cod_for, "CodFor"):
                    continue
                supplier_ids.add(cod_for)

    from_purchase_orders = _resolve_csv_path(_get_config("ERP_CSV_E420OCP"))
    if from_purchase_orders:
        purchase_order_index = _build_column_index(schema_path, "E420OCP")
        for row in _iter_csv_rows(from_purchase_orders):
            cod_for = _safe_value(row, purchase_order_index.get("CodFor"))
            if cod_for:
                if _is_header_cell(cod_for, "CodFor"):
                    continue
                supplier_ids.add(cod_for)

    return [
        {
            "external_id": supplier_id,
            "updated_at": _stable_epoch(),
            "name": f"Fornecedor ERP {supplier_id}",
            "risk_flags": DEFAULT_RISK_FLAGS,
        }
        for supplier_id in sorted(supplier_ids)
    ]


def _fetch_senior_records(
    entity: str,
    since_updated_at: str | None,
    since_id: str | None,
    limit: int,
) -> List[dict]:
    endpoint = _entity_endpoint(entity)
    query = {
        "updated_since": since_updated_at,
        "last_id": since_id,
        "limit": str(limit),
    }
    url = f"{endpoint}?{urllib.parse.urlencode({k: v for k, v in query.items() if v})}"
    payload = _request_json("GET", url, allow_retry=True)
    return _normalize_records(payload)


def _push_senior_purchase_order(purchase_order: dict) -> dict:
    endpoint = _entity_endpoint("purchase_order")
    payload = {
        "number": purchase_order.get("number"),
        "supplier_name": purchase_order.get("supplier_name"),
        "currency": purchase_order.get("currency"),
        "total_amount": purchase_order.get("total_amount"),
        "local_id": purchase_order.get("id"),
        "source": "plataforma_compras",
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = _request_json(
        "POST",
        endpoint,
        payload=payload,
        allow_retry=_bool_config("ERP_RETRY_ON_POST", False),
    )
    if not isinstance(response, dict):
        raise ErpError("Resposta inesperada do ERP (JSON nao-objeto).")

    external_id = response.get("external_id") or response.get("id") or response.get("codigo")
    status = response.get("status") or response.get("erp_status") or "erp_accepted"
    message = response.get("message")

    if not external_id:
        raise ErpError("ERP nao retornou external_id para a OC.")

    return {"external_id": external_id, "status": status, "message": message}


def _entity_endpoint(entity: str) -> str:
    base_url = _get_config("ERP_BASE_URL")
    if not base_url:
        raise ErpError("ERP_BASE_URL nao configurado.")
    base_url = base_url.rstrip("/")

    mapping = _parse_mapping(_get_config("ERP_ENTITY_ENDPOINTS"))
    path = mapping.get(entity, entity)
    return f"{base_url}/{path.lstrip('/')}"


def _request_json(
    method: str,
    url: str,
    payload: dict | None = None,
    allow_retry: bool = False,
) -> object:
    timeout = _int_config("ERP_TIMEOUT_SECONDS", 20)
    attempts = _int_config("ERP_RETRY_ATTEMPTS", 2) if allow_retry else 1
    backoff_ms = _int_config("ERP_RETRY_BACKOFF_MS", 300)
    headers = {"Accept": "application/json"}

    token = _get_config("ERP_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    api_key = _get_config("ERP_API_KEY")
    if api_key:
        headers["X-API-Key"] = api_key

    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")

    request = urllib.request.Request(url, data=data, headers=headers, method=method.upper())

    context = None
    if not _bool_config("ERP_VERIFY_SSL", True):
        context = ssl._create_unverified_context()

    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
                body = response.read().decode("utf-8")
                if not body:
                    return {}
                return json.loads(body)
        except urllib.error.HTTPError as exc:  # noqa: PERF203
            error_body = exc.read().decode("utf-8") if exc.fp else ""
            should_retry = allow_retry and attempt < attempts - 1 and exc.code >= 500
            if should_retry:
                time.sleep(backoff_ms / 1000)
                continue
            raise ErpError(f"ERP HTTP {exc.code}: {error_body[:200]}") from exc
        except urllib.error.URLError as exc:
            should_retry = allow_retry and attempt < attempts - 1
            if should_retry:
                time.sleep(backoff_ms / 1000)
                continue
            raise ErpError(f"Erro de conexao ERP: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise ErpError("ERP retornou JSON invalido.") from exc

    raise ErpError("Falha ao chamar ERP.")


def _normalize_records(payload: object) -> List[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        items = payload.get("items") or payload.get("data") or payload.get("records") or []
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
    return []


def _parse_mapping(value: object) -> dict[str, str]:
    if not value:
        return {}
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    if isinstance(value, str):
        mapping: dict[str, str] = {}
        pairs = [pair.strip() for pair in value.split(",") if pair.strip()]
        for pair in pairs:
            if "=" not in pair:
                continue
            key, mapped = pair.split("=", 1)
            mapping[key.strip()] = mapped.strip()
        return mapping
    return {}


def _get_config(key: str, default: object | None = None) -> object | None:
    try:
        if key in current_app.config:
            value = current_app.config.get(key)
            if value is not None:
                return value
        return os.environ.get(key, default)
    except RuntimeError:
        return os.environ.get(key, default)


def _int_config(key: str, default: int) -> int:
    value = _get_config(key, default)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _bool_config(key: str, default: bool) -> bool:
    value = _get_config(key, default)
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

