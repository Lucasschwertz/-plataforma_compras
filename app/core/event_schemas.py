from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any, Dict

from app.observability import observe_domain_event_schema_invalid


_LOGGER = logging.getLogger("app")


EVENT_SCHEMAS: dict[str, dict[str, Any]] = {
    "PurchaseRequestCreated": {
        "versions": (1, 2),
        "required_fields": ("tenant_id", "purchase_request_id", "status"),
        "optional_fields": (
            "items_created",
            "items_count",
            "workspace_id",
            "event_id",
            "occurred_at",
            "schema_name",
            "schema_version",
        ),
    },
    "RfqCreated": {
        "versions": (1,),
        "required_fields": ("tenant_id", "rfq_id"),
        "optional_fields": ("title", "workspace_id", "event_id", "occurred_at", "schema_name", "schema_version"),
    },
    "RfqAwarded": {
        "versions": (1,),
        "required_fields": ("tenant_id", "rfq_id", "award_id"),
        "optional_fields": ("workspace_id", "event_id", "occurred_at", "schema_name", "schema_version"),
    },
    "PurchaseOrderCreated": {
        "versions": (1, 2),
        "required_fields": ("tenant_id", "purchase_order_id", "status"),
        "optional_fields": ("source", "workspace_id", "event_id", "occurred_at", "schema_name", "schema_version"),
    },
    "ErpOrderAccepted": {
        "versions": (1,),
        "required_fields": ("tenant_id", "purchase_order_id", "sync_run_id"),
        "optional_fields": (
            "external_id",
            "workspace_id",
            "event_id",
            "occurred_at",
            "schema_name",
            "schema_version",
        ),
    },
    "ErpOrderRejected": {
        "versions": (1,),
        "required_fields": ("tenant_id", "purchase_order_id", "sync_run_id"),
        "optional_fields": ("reason", "workspace_id", "event_id", "occurred_at", "schema_name", "schema_version"),
    },
}


def latest_schema_version(schema_name: str | None) -> int:
    normalized_schema = str(schema_name or "").strip()
    if not normalized_schema:
        return 1
    schema = EVENT_SCHEMAS.get(normalized_schema) or {}
    versions = tuple(schema.get("versions") or ())
    if not versions:
        return 1
    return max(int(version) for version in versions)


def _event_payload(event: Any) -> Dict[str, Any]:
    try:
        raw = asdict(event)
    except TypeError:
        raw = dict(getattr(event, "__dict__", {}) or {})
    return dict(raw or {})


def validate_event(event: Any) -> bool:
    payload = _event_payload(event)
    schema_name = str(payload.get("schema_name") or getattr(event, "schema_name", "") or type(event).__name__).strip()
    if not schema_name:
        schema_name = "unknown"

    schema = EVENT_SCHEMAS.get(schema_name)
    if not schema:
        return True

    try:
        schema_version = int(payload.get("schema_version") or getattr(event, "schema_version", 1) or 1)
    except (TypeError, ValueError):
        schema_version = 1
    if schema_version < 1:
        schema_version = 1

    supported_versions = {int(version) for version in tuple(schema.get("versions") or ())}
    required_fields = [str(field).strip() for field in tuple(schema.get("required_fields") or ()) if str(field).strip()]
    missing_fields = [field for field in required_fields if field not in payload or payload.get(field) is None]
    valid_version = schema_version in supported_versions if supported_versions else True
    is_valid = not missing_fields and valid_version
    if is_valid:
        return True

    observe_domain_event_schema_invalid(schema_name)
    _LOGGER.error(
        "domain_event_schema_invalid",
        extra={
            "event_id": str(payload.get("event_id") or getattr(event, "event_id", "") or "").strip() or None,
            "schema_name": schema_name,
            "schema_version": schema_version,
            "supported_versions": sorted(supported_versions),
            "missing_fields": missing_fields,
        },
    )
    return False
