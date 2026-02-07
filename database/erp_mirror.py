from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

from app.db import Database


@dataclass
class MirrorTable:
    source_name: str
    mirror_name: str
    ordered_columns: List[str]
    column_map: Dict[str, str]
    pk_columns: List[str]


def _safe_identifier(value: str, fallback: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9_]", "_", (value or "").strip().lower())
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        text = fallback
    if text[0].isdigit():
        text = f"c_{text}"
    return text


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _clean_csv_value(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() == "NULL":
        return None
    return text


def _is_header_row(row: List[str], expected_columns: List[str]) -> bool:
    if not row or not expected_columns:
        return False
    compare = min(len(row), len(expected_columns), 24)
    if compare < 2:
        return False

    expected_set = {_safe_identifier(name, fallback="col") for name in expected_columns}
    matches = 0
    for idx in range(compare):
        value = _safe_identifier(str(row[idx]), fallback=f"c{idx}")
        if value in expected_set:
            matches += 1
    return matches >= max(2, int(compare * 0.7))


def load_schema_tables(schema_path: Path) -> Dict[str, MirrorTable]:
    if not schema_path.exists():
        raise FileNotFoundError(schema_path)

    staging: Dict[str, Dict[str, object]] = {}
    with schema_path.open("r", encoding="utf-8-sig", errors="ignore") as handle:
        reader = csv.reader(handle, delimiter=";")
        for row in reader:
            if len(row) < 4:
                continue
            source_table = (row[1] or "").strip().upper()
            source_column = (row[3] or "").strip()
            if not source_table or not source_column:
                continue

            entry = staging.setdefault(
                source_table,
                {
                    "columns": {},
                    "pk_columns": [],
                },
            )

            order_value = 9999
            if len(row) > 12:
                try:
                    order_value = int(row[12])
                except ValueError:
                    order_value = 9999
            entry["columns"][source_column] = min(
                order_value,
                int(entry["columns"].get(source_column, order_value)),
            )

            pk_raw = (row[2] or "").strip()
            if pk_raw:
                for pk in [part.strip() for part in pk_raw.split(";") if part.strip()]:
                    if pk not in entry["pk_columns"]:
                        entry["pk_columns"].append(pk)

    tables: Dict[str, MirrorTable] = {}
    for source_table, payload in staging.items():
        ordered = sorted(payload["columns"].items(), key=lambda item: (item[1], item[0]))
        ordered_columns = [name for name, _ in ordered]

        used_identifiers: set[str] = set()
        column_map: Dict[str, str] = {}
        for source_column in ordered_columns:
            base = _safe_identifier(source_column, fallback="col")
            normalized = base
            suffix = 2
            while normalized in used_identifiers:
                normalized = f"{base}_{suffix}"
                suffix += 1
            used_identifiers.add(normalized)
            column_map[source_column] = normalized

        pk_columns = [pk for pk in payload["pk_columns"] if pk in ordered_columns]
        mirror_name = _safe_identifier(source_table, fallback="erp_table")

        tables[source_table] = MirrorTable(
            source_name=source_table,
            mirror_name=mirror_name,
            ordered_columns=ordered_columns,
            column_map=column_map,
            pk_columns=pk_columns,
        )

    return tables


def ensure_mirror_tables(
    db: Database,
    schema_tables: Dict[str, MirrorTable],
    only_tables: Iterable[str] | None = None,
) -> int:
    selected = set(table.strip().upper() for table in only_tables or [] if table and table.strip())
    created = 0

    for source_table in sorted(schema_tables):
        if selected and source_table not in selected:
            continue
        spec = schema_tables[source_table]
        table_ident = _quote_ident(spec.mirror_name)

        column_defs = ['"tenant_id" TEXT NOT NULL']
        for source_column in spec.ordered_columns:
            mirror_column = spec.column_map[source_column]
            column_defs.append(f"{_quote_ident(mirror_column)} TEXT")
        column_defs.append('"_ingested_at" TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP')

        primary_keys = ["tenant_id"] + [spec.column_map[col] for col in spec.pk_columns if col in spec.column_map]
        if len(primary_keys) > 1:
            pk_sql = ", ".join(_quote_ident(col) for col in primary_keys)
            column_defs.append(f"PRIMARY KEY ({pk_sql})")

        ddl = f"CREATE TABLE IF NOT EXISTS {table_ident} ({', '.join(column_defs)})"
        db.execute(ddl)
        created += 1
    return created


def import_csv_into_mirror(
    db: Database,
    schema_tables: Dict[str, MirrorTable],
    source_table: str,
    csv_path: Path,
    tenant_id: str,
    truncate_tenant: bool = False,
) -> int:
    table_key = (source_table or "").strip().upper()
    if not table_key:
        raise ValueError("Tabela ERP invalida.")
    if table_key not in schema_tables:
        raise ValueError(f"Tabela {table_key} nao encontrada no schema.")
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    spec = schema_tables[table_key]
    table_ident = _quote_ident(spec.mirror_name)

    if truncate_tenant:
        db.execute(f"DELETE FROM {table_ident} WHERE tenant_id = ?", (tenant_id,))

    insert_columns = ["tenant_id"] + [spec.column_map[col] for col in spec.ordered_columns]
    insert_cols_sql = ", ".join(_quote_ident(col) for col in insert_columns)
    placeholders_sql = ", ".join("?" for _ in insert_columns)

    pk_columns = ["tenant_id"] + [spec.column_map[col] for col in spec.pk_columns if col in spec.column_map]
    non_pk_columns = [col for col in insert_columns if col not in pk_columns]

    upsert_sql = ""
    if len(pk_columns) > 1:
        conflict_cols = ", ".join(_quote_ident(col) for col in pk_columns)
        if non_pk_columns:
            updates = ", ".join(
                f"{_quote_ident(col)} = excluded.{_quote_ident(col)}" for col in non_pk_columns
            )
            upsert_sql = f" ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates}, \"_ingested_at\" = CURRENT_TIMESTAMP"
        else:
            upsert_sql = f" ON CONFLICT ({conflict_cols}) DO UPDATE SET \"_ingested_at\" = CURRENT_TIMESTAMP"

    sql = f"INSERT INTO {table_ident} ({insert_cols_sql}) VALUES ({placeholders_sql}){upsert_sql}"

    imported = 0
    first_data_row = True
    with csv_path.open("r", encoding="utf-8-sig", errors="ignore") as handle:
        reader = csv.reader(handle, delimiter=";")
        for row in reader:
            if not row:
                continue
            if first_data_row and _is_header_row(row, spec.ordered_columns):
                first_data_row = False
                continue
            first_data_row = False

            values: List[str | None] = [tenant_id]
            for idx in range(len(spec.ordered_columns)):
                raw = row[idx] if idx < len(row) else None
                values.append(_clean_csv_value(raw))
            db.execute(sql, tuple(values))
            imported += 1

    return imported
