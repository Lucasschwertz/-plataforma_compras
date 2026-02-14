from __future__ import annotations

from datetime import date, datetime, timezone
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
