from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Callable

from app.contexts.erp.domain.contracts import ErpPurchaseOrderLineV1, ErpPurchaseOrderV1, ErpPushResultV1, from_dict


def _activate_worker_erp_context() -> None:
    os.environ["ERP_CLIENT_CONTEXT"] = "worker"


def _read_simulator_seed() -> int:
    try:
        from flask import current_app

        configured = current_app.config.get("ERP_SIMULATOR_SEED")
        if configured is not None:
            return int(configured)
    except Exception:
        pass
    raw = str(os.environ.get("ERP_SIMULATOR_SEED") or "42").strip()
    try:
        return int(raw)
    except ValueError:
        return 42


def _coerce_canonical_purchase_order(payload: Any) -> ErpPurchaseOrderV1:
    if isinstance(payload, ErpPurchaseOrderV1):
        return payload
    raw = dict(payload or {}) if isinstance(payload, dict) else {}

    maybe = from_dict(raw) if raw else None
    if isinstance(maybe, ErpPurchaseOrderV1):
        return maybe

    workspace_id = str(raw.get("workspace_id") or raw.get("tenant_id") or "tenant-default")
    external_ref = str(raw.get("external_ref") or raw.get("id") or raw.get("number") or raw.get("external_id") or "")
    supplier_name = str(raw.get("supplier_name") or "").strip() or None
    gross_total = 0.0
    try:
        gross_total = float(raw.get("total_amount") or 0.0)
    except (TypeError, ValueError):
        gross_total = 0.0

    line = ErpPurchaseOrderLineV1(
        line_id=f"{external_ref or 'po'}:1",
        product_code=str(raw.get("number") or external_ref or "item").strip() or "item",
        description=supplier_name,
        qty=1.0,
        unit_price=max(0.0, gross_total),
        uom=None,
        cost_center=None,
        delivery_date=None,
    )

    return ErpPurchaseOrderV1(
        workspace_id=workspace_id,
        external_ref=external_ref,
        supplier_code=str(raw.get("supplier_code") or "").strip() or None,
        supplier_name=supplier_name,
        currency=str(raw.get("currency") or "BRL"),
        payment_terms=None,
        issued_at=str(raw.get("updated_at") or raw.get("created_at") or ""),
        lines=[line],
        totals={"gross_total": max(0.0, gross_total), "net_total": None},
    )


@lru_cache(maxsize=1)
def _gateway():
    _activate_worker_erp_context()
    mode = str(os.environ.get("ERP_MODE") or "mock").strip().lower()
    if mode == "simulator":
        from app.contexts.erp.infrastructure.simulator.deterministic_erp import DeterministicErpSimulatorGateway

        return DeterministicErpSimulatorGateway(seed=_read_simulator_seed())

    from app.contexts.erp.infrastructure.senior_gateway import SeniorErpGateway

    return SeniorErpGateway()


def _legacy_status(canonical_status: str) -> str:
    normalized = str(canonical_status or "").strip().lower()
    if normalized == "accepted":
        return "erp_accepted"
    if normalized == "rejected":
        return "erp_error"
    if normalized == "temporary_failure":
        return "temporary_failure"
    return "erp_error"


def build_worker_push_fn() -> Callable[[dict], dict]:
    gateway = _gateway()

    def _push(purchase_order_payload: dict) -> dict:
        canonical_po = _coerce_canonical_purchase_order(purchase_order_payload)
        result = gateway.push_purchase_order(canonical_po)
        if not isinstance(result, ErpPushResultV1):
            return dict(result or {})
        payload = result.to_dict()
        payload["external_id"] = result.erp_document_number
        payload["canonical_status"] = result.status
        payload["status"] = _legacy_status(result.status)
        return payload

    return _push


def build_worker_fetch_fn() -> Callable[[str, str | None, str | None, int], list[dict]]:
    gateway = _gateway()

    def _fetch(entity: str, since_updated_at: str | None, since_id: str | None, limit: int = 100) -> list[dict]:
        return gateway.fetch_records(entity, since_updated_at, since_id, limit=limit)

    return _fetch
