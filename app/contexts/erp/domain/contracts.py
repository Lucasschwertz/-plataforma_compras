from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.contexts.erp.domain.schemas import latest_schema_version_for, validate_schema


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_str(value: object | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    return raw or None


def _safe_float(value: object | None, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


@dataclass
class ErpPurchaseOrderLineV1:
    line_id: str
    product_code: str
    description: str | None = None
    qty: float = 0.0
    unit_price: float = 0.0
    uom: str | None = None
    cost_center: str | None = None
    delivery_date: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "line_id": self.line_id,
            "product_code": self.product_code,
            "description": self.description,
            "qty": float(self.qty),
            "unit_price": float(self.unit_price),
            "uom": self.uom,
            "cost_center": self.cost_center,
            "delivery_date": self.delivery_date,
        }

    @staticmethod
    def from_dict(payload: dict[str, Any]) -> "ErpPurchaseOrderLineV1":
        data = dict(payload or {})
        return ErpPurchaseOrderLineV1(
            line_id=str(data.get("line_id") or ""),
            product_code=str(data.get("product_code") or ""),
            description=_safe_str(data.get("description")),
            qty=_safe_float(data.get("qty"), 0.0),
            unit_price=_safe_float(data.get("unit_price"), 0.0),
            uom=_safe_str(data.get("uom")),
            cost_center=_safe_str(data.get("cost_center")),
            delivery_date=_safe_str(data.get("delivery_date")),
        )


@dataclass
class ErpPurchaseOrderV1:
    workspace_id: str
    external_ref: str
    supplier_code: str | None = None
    supplier_name: str | None = None
    currency: str = "BRL"
    payment_terms: str | None = None
    issued_at: str = field(default_factory=_iso_now)
    lines: list[ErpPurchaseOrderLineV1] = field(default_factory=list)
    totals: dict[str, Any] = field(default_factory=lambda: {"gross_total": 0.0, "net_total": None})
    schema_name: str = "erp.purchase_order"
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.schema_name,
            "schema_version": int(self.schema_version),
            "workspace_id": self.workspace_id,
            "external_ref": self.external_ref,
            "supplier_code": self.supplier_code,
            "supplier_name": self.supplier_name,
            "currency": self.currency,
            "payment_terms": self.payment_terms,
            "issued_at": self.issued_at,
            "lines": [line.to_dict() for line in self.lines],
            "totals": {
                "gross_total": _safe_float((self.totals or {}).get("gross_total"), 0.0),
                "net_total": (
                    None
                    if (self.totals or {}).get("net_total") in (None, "")
                    else _safe_float((self.totals or {}).get("net_total"), 0.0)
                ),
            },
        }

    @staticmethod
    def from_dict(payload: dict[str, Any]) -> "ErpPurchaseOrderV1":
        data = dict(payload or {})
        lines_raw = data.get("lines")
        lines: list[ErpPurchaseOrderLineV1] = []
        if isinstance(lines_raw, list):
            for line in lines_raw:
                if isinstance(line, dict):
                    lines.append(ErpPurchaseOrderLineV1.from_dict(line))
        totals_raw = data.get("totals") if isinstance(data.get("totals"), dict) else {}
        return ErpPurchaseOrderV1(
            schema_name=str(data.get("schema_name") or "erp.purchase_order"),
            schema_version=int(data.get("schema_version") or 1),
            workspace_id=str(data.get("workspace_id") or ""),
            external_ref=str(data.get("external_ref") or ""),
            supplier_code=_safe_str(data.get("supplier_code")),
            supplier_name=_safe_str(data.get("supplier_name")),
            currency=str(data.get("currency") or "BRL"),
            payment_terms=_safe_str(data.get("payment_terms")),
            issued_at=str(data.get("issued_at") or _iso_now()),
            lines=lines,
            totals={
                "gross_total": _safe_float(totals_raw.get("gross_total"), 0.0),
                "net_total": (
                    None if totals_raw.get("net_total") in (None, "") else _safe_float(totals_raw.get("net_total"), 0.0)
                ),
            },
        )


@dataclass
class ErpPushResultV1:
    workspace_id: str
    external_ref: str
    erp_document_number: str | None
    status: str
    rejection_code: str | None = None
    message: str | None = None
    occurred_at: str = field(default_factory=_iso_now)
    schema_name: str = "erp.push_result"
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.schema_name,
            "schema_version": int(self.schema_version),
            "workspace_id": self.workspace_id,
            "external_ref": self.external_ref,
            "erp_document_number": self.erp_document_number,
            "status": self.status,
            "rejection_code": self.rejection_code,
            "message": self.message,
            "occurred_at": self.occurred_at,
        }

    @staticmethod
    def from_dict(payload: dict[str, Any]) -> "ErpPushResultV1":
        data = dict(payload or {})
        return ErpPushResultV1(
            schema_name=str(data.get("schema_name") or "erp.push_result"),
            schema_version=int(data.get("schema_version") or 1),
            workspace_id=str(data.get("workspace_id") or ""),
            external_ref=str(data.get("external_ref") or ""),
            erp_document_number=_safe_str(data.get("erp_document_number")),
            status=str(data.get("status") or "temporary_failure"),
            rejection_code=_safe_str(data.get("rejection_code")),
            message=_safe_str(data.get("message")),
            occurred_at=str(data.get("occurred_at") or _iso_now()),
        )


def to_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, (ErpPurchaseOrderV1, ErpPushResultV1, ErpPurchaseOrderLineV1)):
        return obj.to_dict()
    if isinstance(obj, dict):
        return dict(obj)
    return {}


def from_dict(payload: dict[str, Any]) -> ErpPurchaseOrderV1 | ErpPushResultV1 | None:
    data = dict(payload or {})
    schema_name = str(data.get("schema_name") or "").strip().lower()
    schema_version = int(data.get("schema_version") or 1)
    if schema_name == "erp.purchase_order" and schema_version == 1:
        return ErpPurchaseOrderV1.from_dict(data)
    if schema_name == "erp.push_result" and schema_version == 1:
        return ErpPushResultV1.from_dict(data)
    return None


def validate_contract(obj: Any) -> list[str]:
    payload = to_dict(obj)
    errors: list[str] = []
    schema_name = str(payload.get("schema_name") or "").strip()
    schema_version = int(payload.get("schema_version") or 1)
    ok, schema_errors = validate_schema(schema_name, schema_version, payload)
    if not ok:
        errors.extend(schema_errors)
    if schema_name == "erp.purchase_order" and schema_version == 1:
        lines = payload.get("lines")
        if not isinstance(lines, list) or not lines:
            errors.append("lines must be a non-empty list")
        else:
            for idx, raw_line in enumerate(lines):
                if not isinstance(raw_line, dict):
                    errors.append(f"lines[{idx}] must be object")
                    continue
                line = ErpPurchaseOrderLineV1.from_dict(raw_line)
                if not str(line.line_id).strip():
                    errors.append(f"lines[{idx}].line_id is required")
                if not str(line.product_code).strip():
                    errors.append(f"lines[{idx}].product_code is required")
                if float(line.qty) <= 0:
                    errors.append(f"lines[{idx}].qty must be > 0")
                if float(line.unit_price) < 0:
                    errors.append(f"lines[{idx}].unit_price must be >= 0")
        totals = payload.get("totals")
        if not isinstance(totals, dict):
            errors.append("totals must be object")
        else:
            if "gross_total" not in totals:
                errors.append("totals.gross_total is required")
    elif schema_name == "erp.push_result" and schema_version == 1:
        status = str(payload.get("status") or "").strip()
        if status not in {"accepted", "rejected", "temporary_failure"}:
            errors.append("status must be accepted|rejected|temporary_failure")
    return errors


def latest_contract_version(schema_name: str) -> int:
    return latest_schema_version_for(schema_name)

