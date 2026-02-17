from __future__ import annotations

from typing import Any


ERP_SCHEMAS: dict[str, dict[int, dict[str, list[str]]]] = {
    "erp.purchase_order": {
        1: {
            "required_fields": [
                "schema_name",
                "schema_version",
                "workspace_id",
                "external_ref",
                "issued_at",
                "lines",
                "totals",
            ],
            "optional_fields": [
                "supplier_code",
                "supplier_name",
                "currency",
                "payment_terms",
            ],
        }
    },
    "erp.push_result": {
        1: {
            "required_fields": [
                "schema_name",
                "schema_version",
                "workspace_id",
                "external_ref",
                "status",
                "occurred_at",
            ],
            "optional_fields": [
                "erp_document_number",
                "rejection_code",
                "message",
            ],
        }
    },
}


def latest_schema_version_for(schema_name: str | None) -> int:
    normalized = str(schema_name or "").strip().lower()
    versions = ERP_SCHEMAS.get(normalized) or {}
    if not versions:
        return 1
    return max(int(version) for version in versions.keys())


def validate_schema(schema_name: str, version: int, payload_dict: dict[str, Any] | None) -> tuple[bool, list[str]]:
    errors: list[str] = []
    normalized_name = str(schema_name or "").strip().lower()
    if not normalized_name:
        return (False, ["schema_name is required"])

    schema_versions = ERP_SCHEMAS.get(normalized_name)
    if not schema_versions:
        return (False, [f"unsupported schema_name: {normalized_name}"])

    schema_version = int(version or 1)
    schema = schema_versions.get(schema_version)
    if not schema:
        return (False, [f"unsupported schema_version={schema_version} for schema_name={normalized_name}"])

    payload = dict(payload_dict or {})
    for required in schema.get("required_fields", []):
        if required not in payload:
            errors.append(f"missing required field: {required}")
            continue
        value = payload.get(required)
        if value is None:
            errors.append(f"field cannot be null: {required}")
            continue
        if isinstance(value, str) and not value.strip():
            errors.append(f"field cannot be empty: {required}")

    return (len(errors) == 0, errors)

