from __future__ import annotations

from typing import Dict, Tuple


CRITICAL_ACTIONS: Dict[str, Dict[str, str]] = {
    "cancel_request": {
        "action_key": "cancel_request",
        "confirm_message_key": "cancel_request",
        "impact_text_key": "impact.cancel_request",
    },
    "cancel_rfq": {
        "action_key": "cancel_rfq",
        "confirm_message_key": "cancel_quote",
        "impact_text_key": "impact.cancel_rfq",
    },
    "cancel_order": {
        "action_key": "cancel_order",
        "confirm_message_key": "cancel_order",
        "impact_text_key": "impact.cancel_order",
    },
    "cancel_invite": {
        "action_key": "cancel_invite",
        "confirm_message_key": "cancel_invite",
        "impact_text_key": "impact.cancel_invite",
    },
    "push_to_erp": {
        "action_key": "push_to_erp",
        "confirm_message_key": "push_order_erp",
        "impact_text_key": "impact.push_to_erp",
    },
    "award_rfq": {
        "action_key": "award_rfq",
        "confirm_message_key": "award_rfq",
        "impact_text_key": "impact.award_rfq",
    },
    "create_purchase_order": {
        "action_key": "create_purchase_order",
        "confirm_message_key": "create_purchase_order",
        "impact_text_key": "impact.create_purchase_order",
    },
    "delete_supplier_proposal": {
        "action_key": "delete_supplier_proposal",
        "confirm_message_key": "delete_supplier_proposal",
        "impact_text_key": "impact.delete_supplier_proposal",
    },
}


_TRUE_TEXT_VALUES = {"1", "true", "yes", "on"}


def get_critical_action(action_key: str | None) -> Dict[str, str] | None:
    if not action_key:
        return None
    return CRITICAL_ACTIONS.get(str(action_key).strip())


def is_critical_action(action_key: str | None) -> bool:
    return get_critical_action(action_key) is not None


def _is_explicit_true(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) == 1
    if isinstance(value, str):
        return value.strip().lower() in _TRUE_TEXT_VALUES
    return False


def resolve_confirmation(request_obj, payload: dict | None = None) -> Tuple[bool, str]:
    payload_dict = payload if isinstance(payload, dict) else {}

    confirm_token = (
        payload_dict.get("confirm_token")
        or request_obj.args.get("confirm_token")
        or request_obj.form.get("confirm_token")
        or request_obj.headers.get("X-Confirm-Token")
    )
    if isinstance(confirm_token, str) and confirm_token.strip():
        return True, "confirm_token"

    confirm_value = payload_dict.get("confirm")
    if confirm_value is None:
        confirm_value = request_obj.args.get("confirm")
    if confirm_value is None:
        confirm_value = request_obj.form.get("confirm")
    if confirm_value is None:
        confirm_value = request_obj.headers.get("X-Confirm")

    if _is_explicit_true(confirm_value):
        return True, "confirm_flag"

    return False, "missing_confirmation"
