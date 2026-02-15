from __future__ import annotations

from typing import Any, Callable, Dict, Tuple

from app.core.event_schemas import latest_schema_version


Upcaster = Callable[[Dict[str, Any]], Dict[str, Any]]


def _upcast_purchase_request_v1_to_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
    upgraded = dict(payload or {})
    if "items_created" not in upgraded and "items_count" in upgraded:
        upgraded["items_created"] = upgraded.get("items_count")
    upgraded.pop("items_count", None)
    return upgraded


UPCASTERS: Dict[Tuple[str, int], Upcaster] = {
    ("PurchaseRequestCreated", 1): _upcast_purchase_request_v1_to_v2,
}


def upcast(
    schema_name: str | None,
    version: int | str | None,
    payload: Dict[str, Any] | None,
    target_version: int | None = None,
) -> Dict[str, Any]:
    normalized_schema = str(schema_name or "").strip() or "unknown"
    try:
        current_version = int(version or 1)
    except (TypeError, ValueError):
        current_version = 1
    if current_version < 1:
        current_version = 1

    resolved_target = int(target_version or latest_schema_version(normalized_schema) or 1)
    if resolved_target < current_version:
        resolved_target = current_version

    upgraded = dict(payload or {})
    while current_version < resolved_target:
        upcaster = UPCASTERS.get((normalized_schema, current_version))
        if upcaster is not None:
            upgraded = dict(upcaster(dict(upgraded or {})) or {})
        current_version += 1

    upgraded["schema_name"] = normalized_schema
    upgraded["schema_version"] = current_version
    return upgraded
