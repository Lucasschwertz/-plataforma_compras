from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.contexts.erp.domain.contracts import ErpPurchaseOrderV1, ErpPushResultV1
from app.contexts.erp.infrastructure.mappers.senior_errors import (
    classify_response_status,
    normalize_rejection_code,
)
from app.errors import ValidationError
from app.ui_strings import error_message, success_message


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def map_canonical_po_to_senior_payload(po: ErpPurchaseOrderV1) -> dict[str, Any]:
    if not po.lines:
        raise ValidationError(
            code="erp_payload_invalid_for_erp",
            message_key="erp_payload_invalid_for_erp",
            http_status=400,
            critical=False,
        )
    for line in po.lines:
        if float(line.qty) <= 0:
            raise ValidationError(
                code="erp_payload_invalid_for_erp",
                message_key="erp_payload_invalid_for_erp",
                http_status=400,
                critical=False,
            )
        if float(line.unit_price) < 0:
            raise ValidationError(
                code="erp_payload_invalid_for_erp",
                message_key="erp_payload_invalid_for_erp",
                http_status=400,
                critical=False,
            )

    gross_total = float((po.totals or {}).get("gross_total") or 0.0)
    local_id: str | int = po.external_ref
    try:
        local_id = int(str(po.external_ref))
    except (TypeError, ValueError):
        local_id = po.external_ref
    payload = {
        "number": po.external_ref,
        "supplier_name": po.supplier_name,
        "currency": po.currency or "BRL",
        "total_amount": gross_total,
        "local_id": local_id,
        "id": local_id,
        "source": "plataforma_compras",
    }
    return {key: value for key, value in payload.items() if value is not None}


def map_senior_response_to_push_result(
    resp_dict: dict[str, Any] | None,
    workspace_id: str,
    external_ref: str,
) -> ErpPushResultV1:
    payload = dict(resp_dict or {})
    raw_message = str(payload.get("message") or "").strip() or None
    raw_status = str(payload.get("status") or payload.get("erp_status") or "").strip()
    resolved_status = classify_response_status(raw_status, raw_message)
    erp_document_number = (
        str(payload.get("external_id") or payload.get("id") or payload.get("codigo") or "").strip() or None
    )
    rejection_code = None
    message = None

    if resolved_status == "accepted":
        message = success_message("erp_accepted")
    elif resolved_status == "rejected":
        rejection_code = normalize_rejection_code(
            str(payload.get("rejection_code") or payload.get("code") or raw_message or "")
        )
        message = error_message("erp_order_rejected")
    else:
        message = error_message("erp_temporarily_unavailable")

    return ErpPushResultV1(
        workspace_id=workspace_id,
        external_ref=external_ref,
        erp_document_number=erp_document_number,
        status=resolved_status,
        rejection_code=rejection_code,
        message=message,
        occurred_at=_utc_iso_now(),
    )
