from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Dict, List

from app.infrastructure.repositories.base import BaseRepository


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return Decimal("0")


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        resolved = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            resolved = datetime.fromisoformat(normalized)
        except ValueError:
            day = _parse_date(raw)
            if not day:
                return None
            resolved = datetime.combine(day, time.min)

    if resolved.tzinfo is None:
        resolved = resolved.replace(tzinfo=timezone.utc)
    return resolved.astimezone(timezone.utc)


def _coerce_range_datetime(value: Any, *, is_end: bool) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        day_time = time.max if is_end else time.min
        resolved = datetime.combine(value, day_time).replace(tzinfo=timezone.utc)
        return resolved.astimezone(timezone.utc)
    raw = str(value).strip()
    if raw and len(raw) <= 10:
        day = _parse_date(raw)
        if day is not None:
            day_time = time.max if is_end else time.min
            resolved = datetime.combine(day, day_time).replace(tzinfo=timezone.utc)
            return resolved.astimezone(timezone.utc)
    return _parse_datetime(value)


def _to_iso_timestamp(value: datetime | None) -> str:
    resolved = value or datetime.now(timezone.utc)
    if resolved.tzinfo is None:
        resolved = resolved.replace(tzinfo=timezone.utc)
    return resolved.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    keys = getattr(row, "keys", None)
    if callable(keys):
        return {key: row[key] for key in row.keys()}
    return {}


class AnalyticsReadModelRepository(BaseRepository):
    def _normalize_workspace_id(self, workspace_id: str | None) -> str:
        resolved = str(workspace_id or self.workspace_id or "").strip()
        if not resolved:
            raise ValueError("workspace_id is required for analytics read model queries")
        return resolved

    @staticmethod
    def _date_bounds(filters: Dict[str, Any] | None) -> tuple[date | None, date | None]:
        source = dict(filters or {})
        start = _parse_date(source.get("start_date"))
        end = _parse_date(source.get("end_date"))
        if start and end and start > end:
            start, end = end, start
        return start, end

    @staticmethod
    def _period_clause(start: date | None, end: date | None) -> tuple[str, tuple[Any, ...]]:
        clause = ""
        params: list[Any] = []
        if start:
            clause += " AND day >= ?"
            params.append(start.isoformat())
        if end:
            clause += " AND day <= ?"
            params.append(end.isoformat())
        return clause, tuple(params)

    def get_kpis(
        self,
        db,
        workspace_id: str | None,
        filters: Dict[str, Any] | None,
        section: str | None,
    ) -> Dict[str, Dict[str, Any]]:
        del section  # read-model query itself is section-agnostic.
        normalized_workspace = self._normalize_workspace_id(workspace_id)
        start, end = self._date_bounds(filters)
        period_clause, period_params = self._period_clause(start, end)

        rows = db.execute(
            f"""
            SELECT
                metric,
                SUM(COALESCE(value_int, 0)) AS total_int,
                SUM(COALESCE(value_num, 0)) AS total_num,
                AVG(COALESCE(value_num, 0)) AS avg_num,
                COUNT(*) AS total_rows
            FROM ar_kpi_daily
            WHERE workspace_id = ?
              {period_clause}
            GROUP BY metric
            """,
            (normalized_workspace, *period_params),
        ).fetchall()

        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            metric = str(row["metric"] or "").strip()
            if not metric:
                continue
            result[metric] = {
                "value_int": int(row["total_int"] or 0),
                "value_num": float(_to_decimal(row["total_num"])),
                "avg_num": float(_to_decimal(row["avg_num"])),
                "rows": int(row["total_rows"] or 0),
            }
        return result

    def get_supplier_metrics(
        self,
        db,
        workspace_id: str | None,
        filters: Dict[str, Any] | None,
    ) -> List[Dict[str, Any]]:
        normalized_workspace = self._normalize_workspace_id(workspace_id)
        start, end = self._date_bounds(filters)
        period_clause, period_params = self._period_clause(start, end)

        rows = db.execute(
            f"""
            SELECT
                supplier_key,
                SUM(COALESCE(invites, 0)) AS invites,
                SUM(COALESCE(responses, 0)) AS responses,
                SUM(COALESCE(avg_response_hours, 0) * COALESCE(responses, 0)) AS weighted_response_hours,
                SUM(COALESCE(savings_abs, 0)) AS savings_abs
            FROM ar_supplier_daily
            WHERE workspace_id = ?
              {period_clause}
            GROUP BY supplier_key
            ORDER BY savings_abs DESC, supplier_key ASC
            """,
            (normalized_workspace, *period_params),
        ).fetchall()

        result: List[Dict[str, Any]] = []
        for row in rows:
            invites = int(row["invites"] or 0)
            responses = int(row["responses"] or 0)
            weighted = _to_decimal(row["weighted_response_hours"])
            avg_response = float(weighted / responses) if responses > 0 else 0.0
            result.append(
                {
                    "supplier_key": str(row["supplier_key"] or "").strip(),
                    "invites": invites,
                    "responses": responses,
                    "avg_response_hours": avg_response,
                    "savings_abs": float(_to_decimal(row["savings_abs"])),
                }
            )
        return result

    def get_stage_metrics(
        self,
        db,
        workspace_id: str | None,
        filters: Dict[str, Any] | None,
    ) -> List[Dict[str, Any]]:
        normalized_workspace = self._normalize_workspace_id(workspace_id)
        start, end = self._date_bounds(filters)
        period_clause, period_params = self._period_clause(start, end)

        rows = db.execute(
            f"""
            SELECT
                stage,
                SUM(COALESCE(count, 0)) AS total_count,
                SUM(COALESCE(avg_hours, 0) * COALESCE(count, 0)) AS weighted_hours
            FROM ar_process_stage_daily
            WHERE workspace_id = ?
              {period_clause}
            GROUP BY stage
            ORDER BY stage ASC
            """,
            (normalized_workspace, *period_params),
        ).fetchall()

        result: List[Dict[str, Any]] = []
        for row in rows:
            total_count = int(row["total_count"] or 0)
            weighted = _to_decimal(row["weighted_hours"])
            avg_hours = float(weighted / total_count) if total_count > 0 else 0.0
            result.append(
                {
                    "stage": str(row["stage"] or "").strip(),
                    "count": total_count,
                    "avg_hours": avg_hours,
                }
            )
        return result

    def get_meta(
        self,
        db,
        workspace_id: str | None,
        filters: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        normalized_workspace = self._normalize_workspace_id(workspace_id)
        start, end = self._date_bounds(filters)
        period_clause, period_params = self._period_clause(start, end)

        kpi_rows = db.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM ar_kpi_daily
            WHERE workspace_id = ?
              {period_clause}
            """,
            (normalized_workspace, *period_params),
        ).fetchone()
        stage_rows = db.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM ar_process_stage_daily
            WHERE workspace_id = ?
              {period_clause}
            """,
            (normalized_workspace, *period_params),
        ).fetchone()
        supplier_rows = db.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM ar_supplier_daily
            WHERE workspace_id = ?
              {period_clause}
            """,
            (normalized_workspace, *period_params),
        ).fetchone()

        records_count = int((kpi_rows or {}).get("total") if isinstance(kpi_rows, dict) else (kpi_rows["total"] if kpi_rows else 0))
        comparison_records_count = int((stage_rows or {}).get("total") if isinstance(stage_rows, dict) else (stage_rows["total"] if stage_rows else 0))
        supplier_count = int((supplier_rows or {}).get("total") if isinstance(supplier_rows, dict) else (supplier_rows["total"] if supplier_rows else 0))

        return {
            "records_count": records_count,
            "comparison_records_count": comparison_records_count,
            "supplier_rows_count": supplier_count,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

    @staticmethod
    def _datetime_bounds(start_value: Any, end_value: Any) -> tuple[datetime | None, datetime | None]:
        start_dt = _coerce_range_datetime(start_value, is_end=False)
        end_dt = _coerce_range_datetime(end_value, is_end=True)
        if start_dt and end_dt and start_dt > end_dt:
            start_dt, end_dt = end_dt, start_dt
        return start_dt, end_dt

    def append_event_store(
        self,
        db,
        *,
        workspace_id: str | None,
        event_id: str,
        event_type: str,
        occurred_at: datetime | str | None,
        payload: Dict[str, Any] | str,
    ) -> bool:
        normalized_workspace = self._normalize_workspace_id(workspace_id)
        normalized_event_id = str(event_id or "").strip()
        normalized_event_type = str(event_type or "").strip()
        if not normalized_event_id:
            raise ValueError("event_id is required")
        if not normalized_event_type:
            raise ValueError("event_type is required")

        occurred_dt = _parse_datetime(occurred_at)
        payload_json = payload if isinstance(payload, str) else json.dumps(payload or {}, ensure_ascii=True, separators=(",", ":"))
        cursor = db.execute(
            """
            INSERT INTO ar_event_store (
                workspace_id,
                event_id,
                event_type,
                occurred_at,
                payload_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(workspace_id, event_id) DO NOTHING
            """,
            (
                normalized_workspace,
                normalized_event_id,
                normalized_event_type,
                _to_iso_timestamp(occurred_dt),
                str(payload_json or "{}"),
            ),
        )
        return int(getattr(cursor, "rowcount", 0) or 0) > 0

    def list_event_store_events(
        self,
        db,
        *,
        workspace_id: str | None,
        start_date: Any = None,
        end_date: Any = None,
    ) -> List[Dict[str, Any]]:
        normalized_workspace = self._normalize_workspace_id(workspace_id)
        start_dt, end_dt = self._datetime_bounds(start_date, end_date)

        where_clause = ""
        params: list[Any] = [normalized_workspace]
        if start_dt:
            where_clause += " AND occurred_at >= ?"
            params.append(_to_iso_timestamp(start_dt))
        if end_dt:
            where_clause += " AND occurred_at <= ?"
            params.append(_to_iso_timestamp(end_dt))

        rows = db.execute(
            f"""
            SELECT workspace_id, event_id, event_type, occurred_at, payload_json, created_at
            FROM ar_event_store
            WHERE workspace_id = ?
              {where_clause}
            ORDER BY occurred_at ASC, created_at ASC, event_id ASC
            """,
            tuple(params),
        ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def clear_projection_workspace(self, db, *, workspace_id: str | None) -> None:
        normalized_workspace = self._normalize_workspace_id(workspace_id)
        for table_name in (
            "ar_kpi_daily",
            "ar_supplier_daily",
            "ar_process_stage_daily",
            "ar_event_dedupe",
            "ar_projection_state",
        ):
            db.execute(
                f"DELETE FROM {table_name} WHERE workspace_id = ?",
                (normalized_workspace,),
            )
