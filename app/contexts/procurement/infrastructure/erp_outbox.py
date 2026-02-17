from __future__ import annotations

import json
import random
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Callable, Dict, List

from flask import current_app

from app.core import ErpOrderAccepted, ErpOrderRejected, EventBus, get_event_bus
from app.core.governance import get_worker_fairness
from app.contexts.erp.domain.contracts import (
    ErpPurchaseOrderLineV1,
    ErpPurchaseOrderV1,
    ErpPushResultV1,
    from_dict as erp_contract_from_dict,
    validate_contract,
)
from app.contexts.erp.domain.schemas import validate_schema
from app.contexts.erp.domain.gateway import ErpGatewayError
from app.contexts.erp.infrastructure.circuit_breaker import get_erp_circuit_breaker
from app.errors import classify_erp_failure
from app.observability import (
    observe_erp_contract_failure,
    observe_erp_contract_invalid,
    observe_erp_outbox_dead_letter,
    observe_erp_mapper_validation_failed,
    observe_erp_outbox_processing,
    observe_erp_outbox_retry,
    observe_erp_outbox_retry_backoff,
    observe_governance_worker_deferred,
    observe_governance_worker_overflow,
    observe_governance_worker_throttled,
    set_log_request_id,
)
from app.ui_strings import error_message


PO_OUTBOX_SCOPE = "purchase_order"
PO_OUTBOX_STATUS_QUEUED = "queued"
PO_OUTBOX_STATUS_RUNNING = "running"
PO_OUTBOX_STATUS_SUCCEEDED = "succeeded"
PO_OUTBOX_STATUS_FAILED = "failed"

_LOCAL_FAILURES_LOCK = Lock()
_LOCAL_FAILURES: deque[dict[str, object]] = deque(maxlen=10)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json_loads(value: str | None) -> Dict[str, object]:
    raw = str(value or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_dumps(value: Dict[str, object]) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


def _duration_expr(db) -> str:
    if getattr(db, "backend", "sqlite") == "postgres":
        return "CAST(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)) * 1000 AS INTEGER)"
    return "CAST((julianday(CURRENT_TIMESTAMP) - julianday(started_at)) * 86400000 AS INTEGER)"


def _dedup_key(tenant_id: str, purchase_order_id: int) -> str:
    return f"po_push:{tenant_id}:{int(purchase_order_id)}"


def _row_to_dict(row) -> Dict[str, object]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    keys = getattr(row, "keys", None)
    if callable(keys):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _normalize_po_status(value: str | None) -> str:
    if not value:
        return "erp_accepted"
    normalized = str(value).strip().lower()
    if normalized in {"draft", "approved", "sent_to_erp", "erp_accepted", "erp_error", "partially_received", "received", "cancelled"}:
        return normalized
    if normalized in {"queued", "processing", "sent"}:
        return "sent_to_erp"
    if normalized in {"accepted", "approved", "ok", "success"}:
        return "erp_accepted"
    if normalized in {"error", "failed", "rejected"}:
        return "erp_error"
    return "erp_accepted"


def _friendly_contract_invalid_message() -> str:
    return error_message("erp_contract_invalid", "Pedido invalido para envio ao ERP.")


def _remember_contract_failure(
    *,
    external_ref: str | None,
    status: str,
    code: str | None,
    timestamp: str | None = None,
) -> None:
    failure = {
        "external_ref": str(external_ref or "").strip() or None,
        "status": str(status or "invalid").strip().lower() or "invalid",
        "code": str(code or "").strip() or None,
        "timestamp": str(timestamp or _iso_utc(_utcnow())).strip() or _iso_utc(_utcnow()),
    }
    with _LOCAL_FAILURES_LOCK:
        _LOCAL_FAILURES.appendleft(failure)
    observe_erp_contract_failure(
        external_ref=failure.get("external_ref"),
        status=failure.get("status") or "invalid",
        code=failure.get("code"),
        timestamp=failure.get("timestamp"),
    )


def latest_contract_failures(limit: int = 10) -> list[dict[str, object]]:
    with _LOCAL_FAILURES_LOCK:
        return list(list(_LOCAL_FAILURES)[: max(1, int(limit))])


def _normalize_iso_string(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return _iso_utc(_utcnow())
    if "T" not in raw and " " in raw:
        raw = raw.replace(" ", "T", 1)
    if raw.endswith("+00:00"):
        raw = raw[:-6] + "Z"
    return raw


def _build_canonical_po_from_row(po: Dict[str, object], tenant_id: str) -> ErpPurchaseOrderV1:
    external_ref = str(po.get("id") or po.get("number") or "").strip()
    amount = po.get("total_amount")
    try:
        gross_total = float(amount or 0.0)
    except (TypeError, ValueError):
        gross_total = 0.0
    line = ErpPurchaseOrderLineV1(
        line_id=f"{external_ref or 'po'}:1",
        product_code=str(po.get("number") or external_ref or "item").strip() or "item",
        description=str(po.get("supplier_name") or "").strip() or None,
        qty=1.0,
        unit_price=max(0.0, gross_total),
        uom=None,
        cost_center=None,
        delivery_date=None,
    )
    return ErpPurchaseOrderV1(
        workspace_id=str(tenant_id or "").strip(),
        external_ref=external_ref,
        supplier_code=None,
        supplier_name=str(po.get("supplier_name") or "").strip() or None,
        currency=str(po.get("currency") or "BRL"),
        payment_terms=None,
        issued_at=_normalize_iso_string(str(po.get("updated_at") or po.get("created_at") or "")),
        lines=[line],
        totals={
            "gross_total": max(0.0, gross_total),
            "net_total": None,
        },
    )


def _canonical_po_from_meta(meta: Dict[str, object], po: Dict[str, object], tenant_id: str) -> tuple[ErpPurchaseOrderV1 | None, dict[str, object], list[str], str]:
    raw_payload = meta.get("canonical_po")
    payload = dict(raw_payload) if isinstance(raw_payload, dict) else {}
    if not payload:
        payload = _build_canonical_po_from_row(po, tenant_id).to_dict()
    payload["workspace_id"] = str(payload.get("workspace_id") or tenant_id)
    payload["schema_name"] = str(payload.get("schema_name") or "erp.purchase_order")
    payload["schema_version"] = int(payload.get("schema_version") or 1)
    schema_name = str(payload.get("schema_name") or "erp.purchase_order")
    schema_version = int(payload.get("schema_version") or 1)
    ok_schema, schema_errors = validate_schema(schema_name, schema_version, payload)
    contract_errors = validate_contract(payload)
    errors = list(schema_errors)
    for item in contract_errors:
        if item not in errors:
            errors.append(item)

    parsed = erp_contract_from_dict(payload)
    if not isinstance(parsed, ErpPurchaseOrderV1):
        errors.append("canonical payload is not erp.purchase_order v1")
        return (None, payload, errors, schema_name)

    if not ok_schema or errors:
        return (None, payload, errors, schema_name)
    return (parsed, payload, [], schema_name)


def _coerce_push_result(
    raw_result: object,
    *,
    workspace_id: str,
    external_ref: str,
) -> ErpPushResultV1:
    if isinstance(raw_result, ErpPushResultV1):
        return raw_result
    if isinstance(raw_result, dict):
        data = dict(raw_result)
        if str(data.get("schema_name") or "").strip().lower() == "erp.push_result":
            parsed = erp_contract_from_dict(data)
            if isinstance(parsed, ErpPushResultV1):
                return parsed

        status_raw = str(data.get("canonical_status") or data.get("status") or "").strip().lower()
        if status_raw in {"erp_accepted", "accepted", "ok", "success"}:
            status = "accepted"
        elif status_raw in {"erp_error", "rejected", "reject", "failed"}:
            status = "rejected"
        elif status_raw in {"temporary_failure", "retry", "queued", "sent_to_erp"}:
            status = "temporary_failure"
        else:
            status = "accepted"

        return ErpPushResultV1(
            workspace_id=workspace_id,
            external_ref=external_ref,
            erp_document_number=str(data.get("erp_document_number") or data.get("external_id") or "").strip() or None,
            status=status,
            rejection_code=str(data.get("rejection_code") or "").strip() or None,
            message=str(data.get("message") or "").strip() or None,
            occurred_at=_normalize_iso_string(str(data.get("occurred_at") or "")),
        )
    return ErpPushResultV1(
        workspace_id=workspace_id,
        external_ref=external_ref,
        erp_document_number=None,
        status="temporary_failure",
        rejection_code=None,
        message=error_message("erp_temporarily_unavailable"),
        occurred_at=_iso_utc(_utcnow()),
    )


def _upsert_integration_watermark(
    db,
    tenant_id: str,
    entity: str,
    source_updated_at: str | None,
    source_id: str | None,
) -> None:
    updated_at = source_updated_at or _iso_utc(_utcnow())
    db.execute(
        """
        INSERT INTO integration_watermarks (
            tenant_id,
            system,
            entity,
            last_success_source_updated_at,
            last_success_source_id,
            last_success_cursor,
            last_success_at
        ) VALUES (?, 'senior', ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(tenant_id, system, entity) DO UPDATE SET
            last_success_source_updated_at = excluded.last_success_source_updated_at,
            last_success_source_id = excluded.last_success_source_id,
            last_success_cursor = excluded.last_success_cursor,
            last_success_at = excluded.last_success_at
        """,
        (tenant_id, entity, updated_at, source_id, None),
    )


def _insert_status_event(
    db,
    tenant_id: str,
    purchase_order_id: int,
    from_status: str | None,
    to_status: str,
    reason: str,
) -> None:
    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
        VALUES ('purchase_order', ?, ?, ?, ?, ?)
        """,
        (purchase_order_id, from_status, to_status, reason, tenant_id),
    )


def _next_backoff_seconds(attempt: int) -> float:
    base = max(1, int(current_app.config.get("ERP_OUTBOX_BACKOFF_SECONDS", 30) or 30))
    max_seconds = max(base, int(current_app.config.get("ERP_OUTBOX_MAX_BACKOFF_SECONDS", 600) or 600))
    exponent = max(0, int(attempt) - 1)
    raw_backoff = float(min(max_seconds, base * (2**exponent)))
    jitter_ratio = float(current_app.config.get("ERP_OUTBOX_BACKOFF_JITTER_RATIO", 0.25) or 0.25)
    jitter_ratio = max(0.0, min(1.0, jitter_ratio))
    jitter_window = raw_backoff * jitter_ratio
    jitter = random.uniform(-jitter_window, jitter_window) if jitter_window > 0 else 0.0
    return max(1.0, min(float(max_seconds), raw_backoff + jitter))


def _max_attempts() -> int:
    return max(1, int(current_app.config.get("ERP_OUTBOX_MAX_ATTEMPTS", 4) or 4))


def _worker_backoff_on_limit_seconds() -> int:
    return max(1, int(current_app.config.get("GOV_WORKER_BACKOFF_ON_LIMIT_SECONDS", 30) or 30))


def _worker_deadletter_on_overflow() -> bool:
    return bool(current_app.config.get("GOV_WORKER_DEADLETTER_ON_OVERFLOW", False))


def _workspace_backlog_size(db, tenant_id: str) -> int:
    row = db.execute(
        """
        SELECT COUNT(*) AS total
        FROM sync_runs
        WHERE tenant_id = ?
          AND scope = ?
          AND status = ?
        """,
        (tenant_id, PO_OUTBOX_SCOPE, PO_OUTBOX_STATUS_QUEUED),
    ).fetchone()
    if not row:
        return 0
    try:
        if isinstance(row, dict):
            return max(0, int(row.get("total") or 0))
        return max(0, int(row["total"]))
    except Exception:
        return 0


def _configure_circuit_breaker() -> None:
    breaker = get_erp_circuit_breaker()
    breaker.configure(
        enabled=bool(current_app.config.get("ERP_CIRCUIT_ENABLED", True)),
        error_rate_threshold=float(current_app.config.get("ERP_CIRCUIT_ERROR_RATE_THRESHOLD", 0.6) or 0.6),
        min_samples=int(current_app.config.get("ERP_CIRCUIT_MIN_SAMPLES", 5) or 5),
        window_seconds=int(current_app.config.get("ERP_CIRCUIT_WINDOW_SECONDS", 120) or 120),
        open_seconds=int(current_app.config.get("ERP_CIRCUIT_OPEN_SECONDS", 30) or 30),
        half_open_max_calls=int(current_app.config.get("ERP_CIRCUIT_HALF_OPEN_MAX_CALLS", 1) or 1),
    )


def _load_purchase_order(db, tenant_id: str, purchase_order_id: int) -> Dict[str, object] | None:
    row = db.execute(
        """
        SELECT id, number, award_id, supplier_name, status, currency, total_amount, external_id, erp_last_error,
               created_at, updated_at, tenant_id
        FROM purchase_orders
        WHERE id = ? AND tenant_id = ?
        """,
        (purchase_order_id, tenant_id),
    ).fetchone()
    if not row:
        return None
    return _row_to_dict(row)


def _pending_run_for_po(db, tenant_id: str, purchase_order_id: int) -> Dict[str, object] | None:
    key = _dedup_key(tenant_id, purchase_order_id)
    row = db.execute(
        """
        SELECT id, status, attempt, payload_ref, payload_hash, started_at, tenant_id
        FROM sync_runs
        WHERE tenant_id = ?
          AND scope = ?
          AND payload_hash = ?
          AND status IN (?, ?)
        ORDER BY id DESC
        LIMIT 1
        """,
        (
            tenant_id,
            PO_OUTBOX_SCOPE,
            key,
            PO_OUTBOX_STATUS_QUEUED,
            PO_OUTBOX_STATUS_RUNNING,
        ),
    ).fetchone()
    if not row:
        return None
    result = _row_to_dict(row)
    result["meta"] = _json_loads(str(result.get("payload_ref") or ""))
    return result


def find_pending_purchase_order_push(db, tenant_id: str, purchase_order_id: int) -> Dict[str, object] | None:
    return _pending_run_for_po(db, tenant_id, purchase_order_id)


def queue_purchase_order_push(
    db,
    tenant_id: str,
    purchase_order: Dict[str, object],
    *,
    request_id: str | None = None,
) -> Dict[str, object]:
    purchase_order_id = int(purchase_order["id"])
    existing = _pending_run_for_po(db, tenant_id, purchase_order_id)
    if existing:
        return {
            "sync_run_id": int(existing["id"]),
            "status": str(existing.get("status") or PO_OUTBOX_STATUS_QUEUED),
            "already_queued": True,
            "payload_hash": str(existing.get("payload_hash") or ""),
        }

    dedup_hash = _dedup_key(tenant_id, purchase_order_id)
    canonical_po = _build_canonical_po_from_row(purchase_order, tenant_id).to_dict()
    meta = {
        "kind": "po_push",
        "purchase_order_id": purchase_order_id,
        "next_attempt_at": _iso_utc(_utcnow()),
        "request_id": str(request_id or "").strip() or None,
        "canonical_po": canonical_po,
    }

    cursor = db.execute(
        """
        INSERT INTO sync_runs (
            system, scope, status, attempt, parent_sync_run_id, payload_ref, payload_hash,
            started_at, records_in, records_upserted, records_failed, tenant_id
        )
        VALUES ('senior', ?, ?, 0, NULL, ?, ?, CURRENT_TIMESTAMP, 0, 0, 0, ?)
        RETURNING id
        """,
        (
            PO_OUTBOX_SCOPE,
            PO_OUTBOX_STATUS_QUEUED,
            _json_dumps(meta),
            dedup_hash,
            tenant_id,
        ),
    )
    row = cursor.fetchone()
    sync_run_id = int(row["id"] if isinstance(row, dict) else row[0])

    from_status = str(purchase_order.get("status") or "").strip().lower() or None
    queue_reason = "po_push_retry_queued" if from_status == "erp_error" else "po_push_queued"
    if from_status != "sent_to_erp":
        db.execute(
            """
            UPDATE purchase_orders
            SET status = 'sent_to_erp', updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (purchase_order_id, tenant_id),
        )
    _insert_status_event(
        db,
        tenant_id,
        purchase_order_id,
        from_status,
        "sent_to_erp",
        queue_reason,
    )
    current_app.logger.info(
        "erp_outbox_enqueued",
        extra={
            "request_id": str(request_id or "").strip() or "n/a",
            "tenant_id": tenant_id,
            "purchase_order_id": purchase_order_id,
            "sync_run_id": sync_run_id,
            "reason": queue_reason,
        },
    )

    return {
        "sync_run_id": sync_run_id,
        "status": PO_OUTBOX_STATUS_QUEUED,
        "already_queued": False,
        "payload_hash": dedup_hash,
    }


def _select_due_runs(db, tenant_id: str | None, limit: int) -> List[Dict[str, object]]:
    tenant_clause = ""
    params: List[object] = []
    if tenant_id:
        tenant_clause = "AND tenant_id = ?"
        params.append(tenant_id)
    rows = db.execute(
        f"""
        SELECT id, tenant_id, attempt, payload_ref, payload_hash
        FROM sync_runs
        WHERE scope = ?
          AND status = ?
          {tenant_clause}
        ORDER BY started_at ASC, id ASC
        LIMIT ?
        """,
        (
            PO_OUTBOX_SCOPE,
            PO_OUTBOX_STATUS_QUEUED,
            *params,
            max(1, int(limit) * 4),
        ),
    ).fetchall()

    now = _utcnow()
    due: List[Dict[str, object]] = []
    for raw_row in rows:
        row = _row_to_dict(raw_row)
        meta = _json_loads(str(row.get("payload_ref") or ""))
        next_attempt_at = _parse_iso_utc(str(meta.get("next_attempt_at") or ""))
        if next_attempt_at and next_attempt_at > now:
            continue
        row["meta"] = meta
        due.append(row)
        if len(due) >= limit:
            break
    return due


def _mark_run_running(db, tenant_id: str, run_id: int) -> bool:
    cursor = db.execute(
        """
        UPDATE sync_runs
        SET status = ?,
            attempt = COALESCE(attempt, 0) + 1,
            started_at = CURRENT_TIMESTAMP,
            finished_at = NULL,
            duration_ms = NULL
        WHERE id = ? AND tenant_id = ? AND status = ?
        """,
        (
            PO_OUTBOX_STATUS_RUNNING,
            run_id,
            tenant_id,
            PO_OUTBOX_STATUS_QUEUED,
        ),
    )
    return int(getattr(cursor, "rowcount", 0) or 0) > 0


def _update_run_terminal(
    db,
    *,
    tenant_id: str,
    run_id: int,
    status: str,
    records_in: int,
    records_upserted: int,
    records_failed: int,
    error_summary: str | None = None,
    error_details: str | None = None,
) -> None:
    duration_expr = _duration_expr(db)
    db.execute(
        f"""
        UPDATE sync_runs
        SET status = ?,
            finished_at = CURRENT_TIMESTAMP,
            duration_ms = {duration_expr},
            records_in = ?,
            records_upserted = ?,
            records_failed = ?,
            error_summary = ?,
            error_details = ?
        WHERE id = ? AND tenant_id = ?
        """,
        (
            status,
            records_in,
            records_upserted,
            records_failed,
            error_summary,
            error_details,
            run_id,
            tenant_id,
        ),
    )


def _requeue_run(
    db,
    *,
    tenant_id: str,
    run_id: int,
    purchase_order_id: int,
    request_id: str | None,
    next_attempt_at: datetime,
    error_summary: str,
    error_details: str,
    canonical_po: Dict[str, object] | None = None,
) -> None:
    meta = {
        "kind": "po_push",
        "purchase_order_id": purchase_order_id,
        "next_attempt_at": _iso_utc(next_attempt_at),
        "request_id": str(request_id or "").strip() or None,
        "canonical_po": dict(canonical_po or {}),
    }
    db.execute(
        """
        UPDATE sync_runs
        SET status = ?,
            finished_at = NULL,
            duration_ms = NULL,
            records_failed = COALESCE(records_failed, 0) + 1,
            error_summary = ?,
            error_details = ?,
            payload_ref = ?
        WHERE id = ? AND tenant_id = ?
        """,
        (
            PO_OUTBOX_STATUS_QUEUED,
            error_summary,
            error_details,
            _json_dumps(meta),
            run_id,
            tenant_id,
        ),
    )


def _delay_queued_run(
    db,
    *,
    tenant_id: str,
    run_id: int,
    purchase_order_id: int,
    request_id: str | None,
    next_attempt_at: datetime,
    error_summary: str,
    error_details: str,
    canonical_po: Dict[str, object] | None = None,
) -> None:
    meta = {
        "kind": "po_push",
        "purchase_order_id": purchase_order_id,
        "next_attempt_at": _iso_utc(next_attempt_at),
        "request_id": str(request_id or "").strip() or None,
        "canonical_po": dict(canonical_po or {}),
    }
    db.execute(
        """
        UPDATE sync_runs
        SET error_summary = ?,
            error_details = ?,
            payload_ref = ?
        WHERE id = ? AND tenant_id = ? AND status = ?
        """,
        (
            error_summary,
            error_details,
            _json_dumps(meta),
            run_id,
            tenant_id,
            PO_OUTBOX_STATUS_QUEUED,
        ),
    )


def _mark_dead_letter(
    db,
    *,
    tenant_id: str,
    run_id: int,
    purchase_order_id: int,
    request_id: str | None,
    records_in: int,
    records_upserted: int,
    records_failed: int,
    error_summary: str,
    error_details: str,
) -> None:
    _update_run_terminal(
        db,
        tenant_id=tenant_id,
        run_id=run_id,
        status=PO_OUTBOX_STATUS_FAILED,
        records_in=records_in,
        records_upserted=records_upserted,
        records_failed=records_failed,
        error_summary=error_summary,
        error_details=error_details,
    )
    dead_letter_meta = {
        "kind": "po_push",
        "purchase_order_id": purchase_order_id,
        "request_id": str(request_id or "").strip() or None,
        "dead_letter": True,
        "dead_letter_reason": error_summary,
        "dead_letter_at": _iso_utc(_utcnow()),
    }
    db.execute(
        """
        UPDATE sync_runs
        SET payload_ref = ?
        WHERE id = ? AND tenant_id = ?
        """,
        (
            _json_dumps(dead_letter_meta),
            run_id,
            tenant_id,
        ),
    )


def process_purchase_order_outbox(
    db,
    *,
    tenant_id: str | None = None,
    limit: int = 25,
    push_fn: Callable[[dict], dict] | None = None,
    event_bus: EventBus | None = None,
    worker_request_id: str | None = None,
) -> Dict[str, int]:
    if push_fn is None:
        raise RuntimeError("push_fn is required for outbox processing. Use worker ERP gateway.")
    bus = event_bus or get_event_bus()
    fairness = get_worker_fairness()
    _configure_circuit_breaker()
    circuit_breaker = get_erp_circuit_breaker()
    candidates = _select_due_runs(db, tenant_id, max(1, int(limit)))
    summary = {"processed": 0, "succeeded": 0, "failed": 0, "requeued": 0, "deferred": 0}

    for candidate in candidates:
        run_id = int(candidate["id"])
        run_tenant_id = str(candidate["tenant_id"])
        candidate_meta = dict(candidate.get("meta") or {})
        request_id = str(candidate_meta.get("request_id") or "").strip() or None
        effective_request_id = request_id or str(worker_request_id or "").strip() or "n/a"
        set_log_request_id(effective_request_id)
        started_ms = time.perf_counter()
        purchase_order_id = int(candidate_meta.get("purchase_order_id") or 0)
        if purchase_order_id <= 0:
            if _mark_run_running(db, run_tenant_id, run_id):
                _update_run_terminal(
                    db,
                    tenant_id=run_tenant_id,
                    run_id=run_id,
                    status=PO_OUTBOX_STATUS_FAILED,
                    records_in=0,
                    records_upserted=0,
                    records_failed=1,
                    error_summary="purchase_order_id_missing",
                    error_details="payload_ref sem purchase_order_id",
                )
                db.commit()
                summary["processed"] += 1
                summary["failed"] += 1
                observe_erp_outbox_processing((time.perf_counter() - started_ms) * 1000.0)
            continue

        backlog_size = _workspace_backlog_size(db, run_tenant_id)
        fairness_decision = fairness.can_process_job(run_tenant_id, backlog_size=backlog_size)
        if not bool(fairness_decision.get("allowed")):
            reason = str(fairness_decision.get("reason") or "concurrency").strip().lower() or "concurrency"
            retry_after = max(1, int(fairness_decision.get("retry_after") or _worker_backoff_on_limit_seconds()))
            overflow = reason == "backlog"
            observe_governance_worker_throttled("backlog" if overflow else "concurrency")
            if overflow:
                observe_governance_worker_overflow(1)
            if overflow and _worker_deadletter_on_overflow():
                _mark_dead_letter(
                    db,
                    tenant_id=run_tenant_id,
                    run_id=run_id,
                    purchase_order_id=purchase_order_id,
                    request_id=request_id,
                    records_in=0,
                    records_upserted=0,
                    records_failed=1,
                    error_summary="governance_overflow",
                    error_details=f"backlog_size={backlog_size}",
                )
                db.commit()
                summary["processed"] += 1
                summary["failed"] += 1
                continue

            _delay_queued_run(
                db,
                tenant_id=run_tenant_id,
                run_id=run_id,
                purchase_order_id=purchase_order_id,
                request_id=request_id,
                next_attempt_at=_utcnow() + timedelta(seconds=retry_after),
                error_summary="governance_throttled",
                error_details=f"reason={reason};backlog_size={backlog_size}",
                canonical_po=(candidate_meta.get("canonical_po") if isinstance(candidate_meta.get("canonical_po"), dict) else None),
            )
            fairness.note_deferred()
            db.commit()
            summary["processed"] += 1
            summary["requeued"] += 1
            summary["deferred"] += 1
            observe_governance_worker_deferred(1)
            continue

        try:
            queued_attempt = max(0, int(candidate.get("attempt") or 0))
        except (TypeError, ValueError):
            queued_attempt = 0
        may_call_erp, circuit_state = circuit_breaker.before_call()
        if not may_call_erp:
            backoff = _next_backoff_seconds(queued_attempt + 1)
            _delay_queued_run(
                db,
                tenant_id=run_tenant_id,
                run_id=run_id,
                purchase_order_id=purchase_order_id,
                request_id=request_id,
                next_attempt_at=_utcnow() + timedelta(seconds=backoff),
                error_summary="erp_circuit_open",
                error_details=f"circuit_state={circuit_state}",
                canonical_po=(candidate_meta.get("canonical_po") if isinstance(candidate_meta.get("canonical_po"), dict) else None),
            )
            db.commit()
            summary["processed"] += 1
            summary["requeued"] += 1
            observe_erp_outbox_retry(1)
            observe_erp_outbox_retry_backoff(backoff)
            current_app.logger.warning(
                "erp_outbox_circuit_blocked",
                extra={
                    "request_id": effective_request_id,
                    "tenant_id": run_tenant_id,
                    "purchase_order_id": purchase_order_id,
                    "sync_run_id": run_id,
                    "circuit_state": circuit_state,
                    "next_backoff_seconds": round(backoff, 3),
                },
            )
            observe_erp_outbox_processing((time.perf_counter() - started_ms) * 1000.0)
            continue

        if not _mark_run_running(db, run_tenant_id, run_id):
            continue

        run_row = db.execute(
            """
            SELECT attempt
            FROM sync_runs
            WHERE id = ? AND tenant_id = ?
            """,
            (run_id, run_tenant_id),
        ).fetchone()
        attempt = int((run_row or {}).get("attempt") if isinstance(run_row, dict) else (run_row["attempt"] if run_row else 1))
        if not attempt:
            attempt = 1

        po = _load_purchase_order(db, run_tenant_id, purchase_order_id)
        if not po:
            _update_run_terminal(
                db,
                tenant_id=run_tenant_id,
                run_id=run_id,
                status=PO_OUTBOX_STATUS_FAILED,
                records_in=0,
                records_upserted=0,
                records_failed=1,
                error_summary="purchase_order_not_found",
                error_details=f"purchase_order_id={purchase_order_id}",
            )
            db.commit()
            summary["processed"] += 1
            summary["failed"] += 1
            observe_erp_outbox_processing((time.perf_counter() - started_ms) * 1000.0)
            continue

        canonical_po, canonical_payload, contract_errors, schema_name = _canonical_po_from_meta(
            candidate_meta,
            po,
            run_tenant_id,
        )
        if contract_errors:
            friendly_message = _friendly_contract_invalid_message()
            error_details = "; ".join(contract_errors)[:1000]
            observe_erp_contract_invalid(schema_name)
            _remember_contract_failure(
                external_ref=str(po.get("id") or ""),
                status="invalid_contract",
                code="erp_contract_invalid",
            )
            db.execute(
                """
                UPDATE purchase_orders
                SET status = 'erp_error', erp_last_error = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND tenant_id = ?
                """,
                (friendly_message[:200], purchase_order_id, run_tenant_id),
            )
            _insert_status_event(
                db,
                run_tenant_id,
                purchase_order_id,
                str(po.get("status") or "").strip().lower() or None,
                "erp_error",
                "po_contract_invalid",
            )
            _mark_dead_letter(
                db,
                tenant_id=run_tenant_id,
                run_id=run_id,
                purchase_order_id=purchase_order_id,
                request_id=request_id,
                records_in=1,
                records_upserted=0,
                records_failed=1,
                error_summary="erp_contract_invalid",
                error_details=error_details,
            )
            db.commit()
            summary["processed"] += 1
            summary["failed"] += 1
            observe_erp_outbox_dead_letter(1)
            observe_erp_outbox_processing((time.perf_counter() - started_ms) * 1000.0)
            continue

        current_status = str(po.get("status") or "").strip().lower()
        if current_status == "erp_accepted":
            _update_run_terminal(
                db,
                tenant_id=run_tenant_id,
                run_id=run_id,
                status=PO_OUTBOX_STATUS_SUCCEEDED,
                records_in=0,
                records_upserted=0,
                records_failed=0,
            )
            db.commit()
            summary["processed"] += 1
            summary["succeeded"] += 1
            observe_erp_outbox_processing((time.perf_counter() - started_ms) * 1000.0)
            continue

        start_reason = "po_push_retry_started" if attempt > 1 or current_status == "erp_error" else "po_push_started"
        if current_status != "sent_to_erp":
            db.execute(
                """
                UPDATE purchase_orders
                SET status = 'sent_to_erp', updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND tenant_id = ?
                """,
                (purchase_order_id, run_tenant_id),
            )
        _insert_status_event(
            db,
            run_tenant_id,
            purchase_order_id,
            current_status or None,
            "sent_to_erp",
            start_reason,
        )

        try:
            raw_result = push_fn(dict(canonical_payload))
            push_result = _coerce_push_result(
                raw_result,
                workspace_id=run_tenant_id,
                external_ref=str(canonical_payload.get("external_ref") or purchase_order_id),
            )
            if push_result.status == "rejected":
                raise ErpGatewayError(
                    str(push_result.message or error_message("erp_order_rejected")),
                    code=push_result.rejection_code,
                    definitive=True,
                )
            if push_result.status == "temporary_failure":
                raise ErpGatewayError(
                    str(push_result.message or error_message("erp_temporarily_unavailable")),
                    code=push_result.rejection_code,
                    definitive=False,
                )

            circuit_breaker.record_success()
            external_id = push_result.erp_document_number
            resolved_status = _normalize_po_status("erp_accepted")
            reason = "po_push_succeeded"

            db.execute(
                """
                UPDATE purchase_orders
                SET status = ?, external_id = ?, erp_last_error = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND tenant_id = ?
                """,
                (resolved_status, external_id, purchase_order_id, run_tenant_id),
            )
            _insert_status_event(
                db,
                run_tenant_id,
                purchase_order_id,
                "sent_to_erp",
                resolved_status,
                reason,
            )

            _upsert_integration_watermark(
                db,
                run_tenant_id,
                entity="purchase_order",
                source_updated_at=None,
                source_id=str(external_id or ""),
            )
            bus.publish(
                ErpOrderAccepted(
                    tenant_id=run_tenant_id,
                    purchase_order_id=purchase_order_id,
                    sync_run_id=run_id,
                    external_id=str(external_id or "") or None,
                )
            )

            _update_run_terminal(
                db,
                tenant_id=run_tenant_id,
                run_id=run_id,
                status=PO_OUTBOX_STATUS_SUCCEEDED,
                records_in=1,
                records_upserted=1,
                records_failed=0,
            )
            db.commit()
            summary["processed"] += 1
            summary["succeeded"] += 1
            current_app.logger.info(
                "erp_outbox_processed",
                extra={
                    "request_id": effective_request_id,
                    "tenant_id": run_tenant_id,
                    "purchase_order_id": purchase_order_id,
                    "sync_run_id": run_id,
                    "result": "succeeded",
                },
            )
            observe_erp_outbox_processing((time.perf_counter() - started_ms) * 1000.0)
        except ErpGatewayError as exc:
            circuit_breaker.record_failure()
            error_details = str(exc)[:1000]
            error_code, message_key, _http_status = classify_erp_failure(error_details)
            if str(exc.code or "").strip() == "erp_payload_invalid_for_erp":
                message_key = "erp_payload_invalid_for_erp"
                error_code = "erp_order_rejected"
                observe_erp_mapper_validation_failed(1)
                _remember_contract_failure(
                    external_ref=str(canonical_payload.get("external_ref") or purchase_order_id),
                    status="mapper_validation_failed",
                    code="erp_payload_invalid_for_erp",
                )
            if str(exc.code or "").strip() == "erp_contract_invalid":
                message_key = "erp_contract_invalid"
                observe_erp_contract_invalid(str(canonical_payload.get("schema_name") or "erp.purchase_order"))
                _remember_contract_failure(
                    external_ref=str(canonical_payload.get("external_ref") or purchase_order_id),
                    status="invalid_contract",
                    code="erp_contract_invalid",
                )
            rejection = bool(exc.definitive) or error_code == "erp_order_rejected"
            failure_reason = "po_push_rejected" if rejection else "po_push_failed"

            db.execute(
                """
                UPDATE purchase_orders
                SET status = 'erp_error', erp_last_error = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND tenant_id = ?
                """,
                (error_details[:200], purchase_order_id, run_tenant_id),
            )
            _insert_status_event(
                db,
                run_tenant_id,
                purchase_order_id,
                "sent_to_erp",
                "erp_error",
                failure_reason,
            )
            if rejection:
                bus.publish(
                    ErpOrderRejected(
                        tenant_id=run_tenant_id,
                        purchase_order_id=purchase_order_id,
                        sync_run_id=run_id,
                        reason=message_key,
                    )
                )

            if rejection or attempt >= _max_attempts():
                _mark_dead_letter(
                    db,
                    tenant_id=run_tenant_id,
                    run_id=run_id,
                    purchase_order_id=purchase_order_id,
                    request_id=request_id,
                    records_in=1,
                    records_upserted=0,
                    records_failed=1,
                    error_summary=message_key,
                    error_details=error_details,
                )
                db.commit()
                summary["processed"] += 1
                summary["failed"] += 1
                observe_erp_outbox_dead_letter(1)
                current_app.logger.warning(
                    "erp_outbox_processed",
                    extra={
                        "request_id": effective_request_id,
                        "tenant_id": run_tenant_id,
                        "purchase_order_id": purchase_order_id,
                        "sync_run_id": run_id,
                        "result": "failed",
                        "error_code": message_key,
                    },
                )
                observe_erp_outbox_processing((time.perf_counter() - started_ms) * 1000.0)
                continue

            backoff = _next_backoff_seconds(attempt)
            _requeue_run(
                db,
                tenant_id=run_tenant_id,
                run_id=run_id,
                purchase_order_id=purchase_order_id,
                request_id=request_id,
                next_attempt_at=_utcnow() + timedelta(seconds=backoff),
                error_summary=message_key,
                error_details=error_details,
                canonical_po=canonical_payload,
            )
            db.commit()
            summary["processed"] += 1
            summary["requeued"] += 1
            observe_erp_outbox_retry(1)
            observe_erp_outbox_retry_backoff(backoff)
            current_app.logger.warning(
                "erp_outbox_processed",
                extra={
                    "request_id": effective_request_id,
                    "tenant_id": run_tenant_id,
                    "purchase_order_id": purchase_order_id,
                    "sync_run_id": run_id,
                    "result": "requeued",
                    "error_code": message_key,
                },
            )
            observe_erp_outbox_processing((time.perf_counter() - started_ms) * 1000.0)
        except Exception as exc:  # noqa: BLE001
            circuit_breaker.record_failure()
            error_details = str(exc)[:1000]
            if attempt >= _max_attempts():
                _mark_dead_letter(
                    db,
                    tenant_id=run_tenant_id,
                    run_id=run_id,
                    purchase_order_id=purchase_order_id,
                    request_id=request_id,
                    records_in=1,
                    records_upserted=0,
                    records_failed=1,
                    error_summary="erp_push_failed",
                    error_details=error_details,
                )
                db.commit()
                summary["processed"] += 1
                summary["failed"] += 1
                observe_erp_outbox_dead_letter(1)
                current_app.logger.error(
                    "erp_outbox_processed",
                    extra={
                        "request_id": effective_request_id,
                        "tenant_id": run_tenant_id,
                        "purchase_order_id": purchase_order_id,
                        "sync_run_id": run_id,
                        "result": "failed",
                        "error_code": "erp_push_failed",
                    },
                )
                observe_erp_outbox_processing((time.perf_counter() - started_ms) * 1000.0)
                continue

            backoff = _next_backoff_seconds(attempt)
            _requeue_run(
                db,
                tenant_id=run_tenant_id,
                run_id=run_id,
                purchase_order_id=purchase_order_id,
                request_id=request_id,
                next_attempt_at=_utcnow() + timedelta(seconds=backoff),
                error_summary="erp_push_failed",
                error_details=error_details,
                canonical_po=canonical_payload,
            )
            db.commit()
            summary["processed"] += 1
            summary["requeued"] += 1
            observe_erp_outbox_retry(1)
            observe_erp_outbox_retry_backoff(backoff)
            current_app.logger.warning(
                "erp_outbox_processed",
                extra={
                    "request_id": effective_request_id,
                    "tenant_id": run_tenant_id,
                    "purchase_order_id": purchase_order_id,
                    "sync_run_id": run_id,
                    "result": "requeued",
                    "error_code": "erp_push_failed",
                },
            )
            observe_erp_outbox_processing((time.perf_counter() - started_ms) * 1000.0)

    return summary


def erp_contract_health_snapshot() -> dict:
    from app.observability import erp_contract_metrics_snapshot

    metrics = erp_contract_metrics_snapshot()
    return {
        "invalid_contract_total": int(metrics.get("invalid_contract_total") or 0),
        "mapper_validation_failed_total": int(metrics.get("mapper_validation_failed_total") or 0),
        "last_10_failures": latest_contract_failures(limit=10),
    }


