from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import quote_plus

from app.procurement.critical_actions import get_critical_action
from app.procurement.flow_policy import (
    action_label as flow_action_label,
    allowed_actions as flow_allowed_actions,
    primary_action as flow_primary_action,
)
from app.ui_strings import get_ui_text


KPI_ACTION_RULES: Dict[str, Dict[str, Any]] = {
    "late_processes": {
        "actionable": True,
        "threshold_operator": "gt",
        "threshold_value": 0.0,
        "action_type": "open_list",
        "action_label_key": "analytics.action.view_actions",
        "action_context": {"hint": "late_processes"},
        "primary_actions": ["open_rfq", "award_rfq", "create_purchase_order", "push_to_erp"],
    },
    "backlog_open": {
        "actionable": True,
        "threshold_operator": "gt",
        "threshold_value": 0.0,
        "action_type": "open_list",
        "action_label_key": "analytics.action.view_actions",
        "action_context": {"hint": "backlog_open"},
        "primary_actions": ["open_rfq", "view_quotes", "create_purchase_order"],
    },
    "supplier_response_rate": {
        "actionable": True,
        "threshold_operator": "lt",
        "threshold_value": 70.0,
        "action_type": "open_list",
        "action_label_key": "analytics.action.view_actions",
        "action_context": {"hint": "supplier_low_response"},
        "primary_actions": ["invite_supplier", "manage_item_supplier", "view_quotes"],
    },
    "supplier_avg_response_time": {
        "actionable": True,
        "threshold_operator": "gt",
        "threshold_value": 48.0,
        "action_type": "open_list",
        "action_label_key": "analytics.action.view_actions",
        "action_context": {"hint": "supplier_slow_response"},
        "primary_actions": ["invite_supplier", "manage_item_supplier", "view_quotes"],
    },
    "erp_rejections": {
        "actionable": True,
        "threshold_operator": "gt",
        "threshold_value": 0.0,
        "action_type": "open_list",
        "action_label_key": "analytics.action.view_actions",
        "action_context": {"hint": "erp_rejections"},
        "primary_actions": ["view_order", "push_to_erp"],
    },
    "awaiting_erp": {
        "actionable": True,
        "threshold_operator": "gt",
        "threshold_value": 0.0,
        "action_type": "open_list",
        "action_label_key": "analytics.action.view_actions",
        "action_context": {"hint": "awaiting_erp"},
        "primary_actions": ["refresh_order", "view_order"],
    },
    "erp_retries": {
        "actionable": True,
        "threshold_operator": "gt",
        "threshold_value": 0.0,
        "action_type": "open_list",
        "action_label_key": "analytics.action.view_actions",
        "action_context": {"hint": "erp_retries"},
        "primary_actions": ["view_order", "push_to_erp", "refresh_order"],
    },
    "no_competition": {
        "actionable": True,
        "threshold_operator": "gt",
        "threshold_value": 0.0,
        "action_type": "open_list",
        "action_label_key": "analytics.action.view_actions",
        "action_context": {"hint": "no_competition"},
        "primary_actions": ["review_decision", "view_quotes", "invite_supplier"],
    },
    "emergency_without_competition": {
        "actionable": True,
        "threshold_operator": "gt",
        "threshold_value": 0.0,
        "action_type": "open_list",
        "action_label_key": "analytics.action.view_actions",
        "action_context": {"hint": "emergency_without_competition"},
        "primary_actions": ["review_decision", "invite_supplier", "open_rfq"],
    },
}


ACTION_UI_TEXT_KEYS: Dict[str, str] = {
    "open_rfq": "analytics.action.open_rfq",
    "invite_supplier": "analytics.action.invite_supplier",
    "manage_item_supplier": "analytics.action.manage_item_supplier",
    "award_rfq": "analytics.action.award_rfq",
    "create_purchase_order": "analytics.action.create_purchase_order",
    "push_to_erp": "analytics.action.push_to_erp",
    "view_quotes": "analytics.action.view_quotes",
    "view_order": "analytics.action.view_order",
    "review_decision": "analytics.action.review_decision",
    "refresh_order": "analytics.action.refresh_order",
    "track_receipt": "analytics.action.track_receipt",
    "view_history": "analytics.action.view_history",
}


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _triggered(rule: Dict[str, Any], value: float) -> bool:
    if not bool(rule.get("actionable", True)):
        return False
    operator = str(rule.get("threshold_operator") or "gt").strip().lower()
    threshold = _to_float(rule.get("threshold_value"))
    if operator == "lt":
        return value < threshold
    if operator == "eq":
        return value == threshold
    return value > threshold


def _rule_primary_actions(rule: Dict[str, Any]) -> List[str]:
    raw_actions = list(rule.get("primary_actions") or rule.get("recommended_actions") or [])
    normalized: List[str] = []
    seen = set()
    for value in raw_actions:
        key = str(value or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return normalized


def _action_label(action_key: str) -> str:
    ui_key = ACTION_UI_TEXT_KEYS.get(action_key)
    fallback = flow_action_label(action_key, action_key)
    if ui_key:
        return get_ui_text(ui_key, fallback)
    return fallback


def _normalized_filters(raw_filters: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = dict(raw_filters or {})
    return {key: value for key, value in payload.items() if str(value or "").strip()}


def enrich_kpi_actions(
    kpis: List[Dict[str, Any]] | None,
    section_key: str,
    raw_filters: Dict[str, Any] | None,
) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    filters = _normalized_filters(raw_filters)

    for item in list(kpis or []):
        current = dict(item)
        kpi_key = str(current.get("key") or "").strip()
        rule = KPI_ACTION_RULES.get(kpi_key)

        if not rule:
            current.update(
                {
                    "actionable": False,
                    "action_label": "",
                    "action_type": "",
                    "primary_actions": [],
                    "action_context": {},
                }
            )
            enriched.append(current)
            continue

        primary_action_keys = _rule_primary_actions(rule)
        primary_actions = [
            {"key": action_key, "label": _action_label(action_key)}
            for action_key in primary_action_keys
        ]
        context_template = dict(rule.get("action_context") or {})
        hint = str(context_template.get("hint") or rule.get("context_hint") or "")
        action_type = str(rule.get("action_type") or "open_list")
        actionable = _triggered(rule, _to_float(current.get("value")))

        action_label = ""
        if actionable:
            action_label_key = str(rule.get("action_label_key") or "analytics.action.view_actions")
            action_label = get_ui_text(
                action_label_key,
                get_ui_text("analytics.action.view_actions", "Ver acoes"),
            )

        current.update(
            {
                "actionable": actionable,
                "action_label": action_label,
                "action_type": action_type,
                "primary_actions": primary_action_keys,
                "action_context": {
                    "section": section_key,
                    "kpi_key": kpi_key,
                    "hint": hint,
                    "filters": filters,
                    "primary_actions": primary_actions,
                    # Backward compatibility for the first actionable-analytics rollout.
                    "recommended_actions": primary_actions,
                },
            }
        )
        enriched.append(current)

    return enriched


def kpi_primary_actions(kpi_key: str) -> List[Dict[str, str]]:
    key = str(kpi_key or "").strip()
    if not key:
        return []
    rule = KPI_ACTION_RULES.get(key)
    if not rule:
        return []
    return [{"key": action_key, "label": _action_label(action_key)} for action_key in _rule_primary_actions(rule)]


def _fallback_detail_url(record: Dict[str, Any]) -> str | None:
    if record.get("po_id"):
        return f"/procurement/purchase-orders/{int(record['po_id'])}"
    if record.get("rfq_id"):
        return f"/procurement/cotacoes/{int(record['rfq_id'])}"
    if record.get("pr_id"):
        return "/procurement/solicitacoes"
    return None


def _record_stage_status(record: Dict[str, Any]) -> tuple[str, str | None]:
    if record.get("po_id"):
        return ("ordem_compra", str(record.get("po_status") or "").strip() or None)
    if record.get("award_id"):
        return ("decisao", str(record.get("award_status") or "").strip() or None)
    if record.get("rfq_id"):
        return ("cotacao", str(record.get("rfq_status") or "").strip() or None)
    return ("solicitacao", str(record.get("pr_status") or "").strip() or None)


def _direct_action_payload(action_key: str, record: Dict[str, Any]) -> Dict[str, Any] | None:
    if action_key == "push_to_erp" and record.get("po_id"):
        po_id = int(record["po_id"])
        return {
            "action_type": "direct_action",
            "api_url": f"/api/procurement/purchase-orders/{po_id}/push-to-erp",
            "method": "POST",
            "entity": "purchase_order",
            "entity_id": po_id,
        }

    if action_key == "create_purchase_order" and record.get("award_id") and not record.get("po_id"):
        award_id = int(record["award_id"])
        return {
            "action_type": "direct_action",
            "api_url": f"/api/procurement/awards/{award_id}/purchase-orders",
            "method": "POST",
            "entity": "award",
            "entity_id": award_id,
        }

    return None


def build_record_primary_action(record: Dict[str, Any], fallback_url: str | None = None) -> Dict[str, Any] | None:
    stage, status = _record_stage_status(record)
    allowed_actions = flow_allowed_actions(stage, status)
    primary_action = flow_primary_action(stage, status)
    if primary_action not in allowed_actions:
        primary_action = allowed_actions[0] if allowed_actions else None

    if not primary_action:
        target_url = fallback_url or _fallback_detail_url(record)
        if not target_url:
            return None
        return {
            "action_key": "open_process",
            "label": get_ui_text("analytics.action.open_process", "Abrir processo"),
            "action_type": "open_item",
            "url": target_url,
            "method": "GET",
            "requires_confirmation": False,
            "confirm_action_key": "",
            "stage": stage,
            "status": status,
            "allowed_actions": allowed_actions,
        }

    direct = _direct_action_payload(primary_action, record)
    if direct:
        critical = get_critical_action(primary_action)
        return {
            "action_key": primary_action,
            "label": _action_label(primary_action),
            "action_type": direct["action_type"],
            "api_url": direct["api_url"],
            "method": direct["method"],
            "requires_confirmation": bool(critical),
            "confirm_action_key": primary_action if critical else "",
            "stage": stage,
            "status": status,
            "allowed_actions": allowed_actions,
            "entity": direct.get("entity"),
            "entity_id": direct.get("entity_id"),
        }

    target_url = fallback_url or _fallback_detail_url(record)
    if not target_url:
        return None
    return {
        "action_key": primary_action,
        "label": _action_label(primary_action),
        "action_type": "open_item",
        "url": target_url,
        "method": "GET",
        "requires_confirmation": False,
        "confirm_action_key": "",
        "stage": stage,
        "status": status,
        "allowed_actions": allowed_actions,
    }


def build_supplier_primary_action(supplier_name: str | None) -> Dict[str, Any] | None:
    supplier = str(supplier_name or "").strip()
    if not supplier:
        return None
    query = quote_plus(supplier)
    return {
        "action_key": "view_quotes",
        "label": get_ui_text("analytics.action.open_quotes", "Abrir cotacoes"),
        "action_type": "open_item",
        "url": f"/procurement/cotacoes?supplier={query}",
        "method": "GET",
        "requires_confirmation": False,
        "confirm_action_key": "",
        "stage": "cotacao",
        "status": None,
        "allowed_actions": [],
    }


