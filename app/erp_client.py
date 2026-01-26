from __future__ import annotations

import json
import os
import ssl
import urllib.parse
import urllib.request
from typing import List

from flask import current_app

from app.erp_mock import DEFAULT_RISK_FLAGS, fetch_erp_records as fetch_mock_records, push_purchase_order as push_mock_po


class ErpError(RuntimeError):
    pass


def fetch_erp_records(
    entity: str,
    since_updated_at: str | None,
    since_id: str | None,
    limit: int = 100,
) -> List[dict]:
    mode = _get_config("ERP_MODE", "mock").lower()
    if mode == "mock":
        return fetch_mock_records(entity, since_updated_at, since_id, limit=limit)
    if mode != "senior":
        raise ErpError(f"ERP_MODE invalido: {mode}")
    return _fetch_senior_records(entity, since_updated_at, since_id, limit=limit)


def push_purchase_order(purchase_order: dict) -> dict:
    mode = _get_config("ERP_MODE", "mock").lower()
    if mode == "mock":
        return push_mock_po(purchase_order)
    if mode != "senior":
        raise ErpError(f"ERP_MODE invalido: {mode}")
    return _push_senior_purchase_order(purchase_order)


def _fetch_senior_records(
    entity: str,
    since_updated_at: str | None,
    since_id: str | None,
    limit: int,
) -> List[dict]:
    endpoint = _entity_endpoint(entity)
    query = {
        "updated_since": since_updated_at,
        "last_id": since_id,
        "limit": str(limit),
    }
    url = f"{endpoint}?{urllib.parse.urlencode({k: v for k, v in query.items() if v})}"
    payload = _request_json("GET", url)
    return _normalize_records(payload)


def _push_senior_purchase_order(purchase_order: dict) -> dict:
    endpoint = _entity_endpoint("purchase_order")
    payload = {
        "number": purchase_order.get("number"),
        "supplier_name": purchase_order.get("supplier_name"),
        "currency": purchase_order.get("currency"),
        "total_amount": purchase_order.get("total_amount"),
        "local_id": purchase_order.get("id"),
        "source": "plataforma_compras",
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = _request_json("POST", endpoint, payload=payload)
    if not isinstance(response, dict):
        raise ErpError("Resposta inesperada do ERP (JSON nao-objeto).")

    external_id = response.get("external_id") or response.get("id") or response.get("codigo")
    status = response.get("status") or response.get("erp_status") or "erp_accepted"
    message = response.get("message")

    if not external_id:
        raise ErpError("ERP nao retornou external_id para a OC.")

    return {"external_id": external_id, "status": status, "message": message}


def _entity_endpoint(entity: str) -> str:
    base_url = _get_config("ERP_BASE_URL")
    if not base_url:
        raise ErpError("ERP_BASE_URL nao configurado.")
    base_url = base_url.rstrip("/")

    mapping = _parse_mapping(_get_config("ERP_ENTITY_ENDPOINTS"))
    path = mapping.get(entity, entity)
    return f"{base_url}/{path.lstrip('/')}"


def _request_json(method: str, url: str, payload: dict | None = None) -> object:
    timeout = _int_config("ERP_TIMEOUT_SECONDS", 20)
    headers = {"Accept": "application/json"}

    token = _get_config("ERP_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    api_key = _get_config("ERP_API_KEY")
    if api_key:
        headers["X-API-Key"] = api_key

    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")

    request = urllib.request.Request(url, data=data, headers=headers, method=method.upper())

    context = None
    if not _bool_config("ERP_VERIFY_SSL", True):
        context = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            body = response.read().decode("utf-8")
            if not body:
                return {}
            return json.loads(body)
    except urllib.error.HTTPError as exc:  # noqa: PERF203
        error_body = exc.read().decode("utf-8") if exc.fp else ""
        raise ErpError(f"ERP HTTP {exc.code}: {error_body[:200]}") from exc
    except urllib.error.URLError as exc:
        raise ErpError(f"Erro de conexao ERP: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise ErpError("ERP retornou JSON invalido.") from exc


def _normalize_records(payload: object) -> List[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        items = payload.get("items") or payload.get("data") or payload.get("records") or []
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
    return []


def _parse_mapping(value: object) -> dict[str, str]:
    if not value:
        return {}
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    if isinstance(value, str):
        mapping: dict[str, str] = {}
        pairs = [pair.strip() for pair in value.split(",") if pair.strip()]
        for pair in pairs:
            if "=" not in pair:
                continue
            key, mapped = pair.split("=", 1)
            mapping[key.strip()] = mapped.strip()
        return mapping
    return {}


def _get_config(key: str, default: object | None = None) -> object | None:
    try:
        return current_app.config.get(key, default)
    except RuntimeError:
        return os.environ.get(key, default)


def _int_config(key: str, default: int) -> int:
    value = _get_config(key, default)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _bool_config(key: str, default: bool) -> bool:
    value = _get_config(key, default)
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
