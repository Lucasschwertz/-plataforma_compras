from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, Sequence

from app.core import (
    DomainEvent,
    ErpOrderAccepted,
    ErpOrderRejected,
    PurchaseOrderCreated,
    PurchaseRequestCreated,
    RfqAwarded,
    RfqCreated,
)
from app.observability import (
    observe_analytics_projection_failed,
    observe_analytics_projection_lag,
    observe_analytics_projection_processed,
)
from app.contexts.analytics.projections.base import (
    Projector,
    ensure_idempotent,
    matches_event_type,
    update_state,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    keys = getattr(row, "keys", None)
    if callable(keys):
        return {key: row[key] for key in row.keys()}
    return {}


def _coerce_datetime(value) -> datetime:
    if isinstance(value, datetime):
        resolved = value
    else:
        resolved = _utc_now()
    if resolved.tzinfo is None:
        resolved = resolved.replace(tzinfo=timezone.utc)
    return resolved.astimezone(timezone.utc)


def _event_day(event: DomainEvent) -> str:
    return _coerce_datetime(getattr(event, "occurred_at", None)).date().isoformat()


def _to_decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _upsert_kpi_daily(
    db,
    *,
    workspace_id: str,
    day: str,
    metric: str,
    delta_int: int = 0,
    delta_num: Decimal | None = None,
    floor_zero_int: bool = False,
) -> None:
    row = db.execute(
        """
        SELECT value_int, value_num
        FROM ar_kpi_daily
        WHERE workspace_id = ? AND day = ? AND metric = ?
        """,
        (workspace_id, day, metric),
    ).fetchone()
    current = _row_to_dict(row)

    current_int = int(current.get("value_int") or 0)
    next_int = current_int + int(delta_int or 0)
    if floor_zero_int and next_int < 0:
        next_int = 0

    current_num = _to_decimal(current.get("value_num"))
    delta_num_value = _to_decimal(delta_num)
    next_num = current_num + delta_num_value

    if row:
        db.execute(
            """
            UPDATE ar_kpi_daily
            SET value_int = ?, value_num = ?, updated_at = CURRENT_TIMESTAMP
            WHERE workspace_id = ? AND day = ? AND metric = ?
            """,
            (next_int, str(next_num), workspace_id, day, metric),
        )
        return

    db.execute(
        """
        INSERT INTO ar_kpi_daily (workspace_id, day, metric, value_num, value_int, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (workspace_id, day, metric, str(next_num), next_int),
    )


def _upsert_stage_daily(
    db,
    *,
    workspace_id: str,
    day: str,
    stage: str,
    avg_hours_sample: Decimal | None = None,
    increment_count: int = 1,
) -> None:
    row = db.execute(
        """
        SELECT avg_hours, count
        FROM ar_process_stage_daily
        WHERE workspace_id = ? AND day = ? AND stage = ?
        """,
        (workspace_id, day, stage),
    ).fetchone()
    current = _row_to_dict(row)
    current_count = int(current.get("count") or 0)
    next_count = current_count + max(0, int(increment_count or 0))

    current_avg = current.get("avg_hours")
    if current_avg is None:
        next_avg = _to_decimal(avg_hours_sample) if avg_hours_sample is not None else Decimal("0")
    elif avg_hours_sample is None:
        next_avg = _to_decimal(current_avg)
    elif current_count <= 0:
        next_avg = _to_decimal(avg_hours_sample)
    else:
        next_avg = ((_to_decimal(current_avg) * current_count) + _to_decimal(avg_hours_sample)) / next_count

    if row:
        db.execute(
            """
            UPDATE ar_process_stage_daily
            SET avg_hours = ?, count = ?, updated_at = CURRENT_TIMESTAMP
            WHERE workspace_id = ? AND day = ? AND stage = ?
            """,
            (str(next_avg), next_count, workspace_id, day, stage),
        )
        return

    db.execute(
        """
        INSERT INTO ar_process_stage_daily (workspace_id, day, stage, avg_hours, count, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (workspace_id, day, stage, str(next_avg), next_count),
    )


def _upsert_supplier_daily(
    db,
    *,
    workspace_id: str,
    day: str,
    supplier_key: str,
    invites_delta: int = 0,
    responses_delta: int = 0,
    avg_response_hours: Decimal | None = None,
    savings_delta: Decimal | None = None,
) -> None:
    normalized_supplier = str(supplier_key or "").strip()
    if not normalized_supplier:
        return

    row = db.execute(
        """
        SELECT invites, responses, avg_response_hours, savings_abs
        FROM ar_supplier_daily
        WHERE workspace_id = ? AND day = ? AND supplier_key = ?
        """,
        (workspace_id, day, normalized_supplier),
    ).fetchone()
    current = _row_to_dict(row)

    invites = int(current.get("invites") or 0) + max(0, int(invites_delta or 0))
    responses = int(current.get("responses") or 0) + max(0, int(responses_delta or 0))
    current_savings = _to_decimal(current.get("savings_abs"))
    next_savings = current_savings + _to_decimal(savings_delta)

    current_avg = current.get("avg_response_hours")
    if avg_response_hours is None:
        next_avg_response = _to_decimal(current_avg)
    elif current_avg is None or responses <= 0:
        next_avg_response = _to_decimal(avg_response_hours)
    else:
        previous_responses = max(0, responses - max(0, int(responses_delta or 0)))
        if previous_responses <= 0:
            next_avg_response = _to_decimal(avg_response_hours)
        else:
            next_avg_response = (
                (_to_decimal(current_avg) * previous_responses) + _to_decimal(avg_response_hours)
            ) / max(1, responses)

    if row:
        db.execute(
            """
            UPDATE ar_supplier_daily
            SET invites = ?,
                responses = ?,
                avg_response_hours = ?,
                savings_abs = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE workspace_id = ? AND day = ? AND supplier_key = ?
            """,
            (
                invites,
                responses,
                str(next_avg_response),
                str(next_savings),
                workspace_id,
                day,
                normalized_supplier,
            ),
        )
        return

    db.execute(
        """
        INSERT INTO ar_supplier_daily (
            workspace_id, day, supplier_key, invites, responses, avg_response_hours, savings_abs, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (workspace_id, day, normalized_supplier, invites, responses, str(next_avg_response), str(next_savings)),
    )


class ProcurementLifecycleProjector(Projector):
    name = "procurement_lifecycle"
    handled_events = (
        PurchaseRequestCreated,
        RfqCreated,
        RfqAwarded,
        PurchaseOrderCreated,
    )

    def handle(self, event: DomainEvent, db, workspace_id: str) -> None:
        day = _event_day(event)
        if isinstance(event, PurchaseRequestCreated):
            _upsert_kpi_daily(
                db,
                workspace_id=workspace_id,
                day=day,
                metric="backlog_open",
                delta_int=1,
            )
            _upsert_stage_daily(db, workspace_id=workspace_id, day=day, stage="SR", increment_count=1)
            return

        if isinstance(event, RfqCreated):
            _upsert_stage_daily(db, workspace_id=workspace_id, day=day, stage="RFQ", increment_count=1)
            return

        if isinstance(event, RfqAwarded):
            _upsert_stage_daily(db, workspace_id=workspace_id, day=day, stage="AWARD", increment_count=1)
            return

        if isinstance(event, PurchaseOrderCreated):
            _upsert_kpi_daily(
                db,
                workspace_id=workspace_id,
                day=day,
                metric="awaiting_erp",
                delta_int=1,
            )
            _upsert_stage_daily(db, workspace_id=workspace_id, day=day, stage="PO", increment_count=1)

            supplier_key = str(getattr(event, "supplier_key", "") or "").strip()
            if supplier_key:
                _upsert_supplier_daily(
                    db,
                    workspace_id=workspace_id,
                    day=day,
                    supplier_key=supplier_key,
                    invites_delta=1,
                )


class ErpStatusProjector(Projector):
    name = "erp_status"
    handled_events = (
        ErpOrderAccepted,
        ErpOrderRejected,
    )

    def handle(self, event: DomainEvent, db, workspace_id: str) -> None:
        day = _event_day(event)
        _upsert_stage_daily(db, workspace_id=workspace_id, day=day, stage="ERP", increment_count=1)
        _upsert_kpi_daily(
            db,
            workspace_id=workspace_id,
            day=day,
            metric="awaiting_erp",
            delta_int=-1,
            floor_zero_int=True,
        )
        if isinstance(event, ErpOrderRejected):
            _upsert_kpi_daily(
                db,
                workspace_id=workspace_id,
                day=day,
                metric="erp_rejections",
                delta_int=1,
            )


class AnalyticsProjectionDispatcher:
    def __init__(self, projectors: Sequence[Projector] | None = None) -> None:
        self._projectors: tuple[Projector, ...] = tuple(projectors or (ProcurementLifecycleProjector(), ErpStatusProjector()))

    @property
    def projectors(self) -> tuple[Projector, ...]:
        return self._projectors

    @property
    def handled_event_types(self) -> tuple[type[DomainEvent], ...]:
        seen: list[type[DomainEvent]] = []
        for projector in self._projectors:
            for event_type in projector.handled_events:
                if event_type not in seen:
                    seen.append(event_type)
        return tuple(seen)

    def process(self, event: DomainEvent, db, workspace_id: str) -> None:
        normalized_workspace = str(workspace_id or "").strip()
        event_type = type(event).__name__
        event_id = str(getattr(event, "event_id", "") or "").strip()
        event_occurred_at = _coerce_datetime(getattr(event, "occurred_at", None))

        for projector in self._projectors:
            if not matches_event_type(projector, event):
                continue
            try:
                if not ensure_idempotent(
                    db,
                    workspace_id=normalized_workspace,
                    projector=projector.name,
                    event_id=event_id,
                ):
                    update_state(
                        db,
                        workspace_id=normalized_workspace,
                        projector=projector.name,
                        status="ok",
                        last_event_id=event_id,
                        last_processed_at=event_occurred_at,
                    )
                    continue

                update_state(
                    db,
                    workspace_id=normalized_workspace,
                    projector=projector.name,
                    status="running",
                    last_event_id=event_id,
                    last_processed_at=event_occurred_at,
                )
                projector.handle(event, db, normalized_workspace)
                update_state(
                    db,
                    workspace_id=normalized_workspace,
                    projector=projector.name,
                    status="ok",
                    last_event_id=event_id,
                    last_processed_at=_utc_now(),
                )
                observe_analytics_projection_processed(projector.name, event_type)
                observe_analytics_projection_lag(projector.name, event_occurred_at)
            except Exception as exc:  # noqa: BLE001
                try:
                    update_state(
                        db,
                        workspace_id=normalized_workspace,
                        projector=projector.name,
                        status="error",
                        last_event_id=event_id,
                        last_processed_at=_utc_now(),
                        last_error=str(exc),
                    )
                except Exception:  # noqa: BLE001
                    pass
                observe_analytics_projection_failed(projector.name, event_type)


def default_projection_dispatcher() -> AnalyticsProjectionDispatcher:
    return AnalyticsProjectionDispatcher()
