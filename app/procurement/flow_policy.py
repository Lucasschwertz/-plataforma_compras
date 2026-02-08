from __future__ import annotations

from typing import Dict, List


PROCESS_STAGES: List[Dict[str, str]] = [
    {"key": "solicitacao", "label": "Solicitacao"},
    {"key": "cotacao", "label": "Cotacao"},
    {"key": "decisao", "label": "Decisao"},
    {"key": "ordem_compra", "label": "Ordem"},
    {"key": "erp", "label": "ERP"},
]


ACTION_LABELS: Dict[str, str] = {
    "open_rfq": "Abrir cotacao",
    "edit_request": "Editar solicitacao",
    "cancel_request": "Cancelar solicitacao",
    "invite_supplier": "Convidar fornecedor",
    "manage_item_supplier": "Convidar por item",
    "save_supplier_quote": "Registrar proposta",
    "award_rfq": "Registrar decisao",
    "create_purchase_order": "Gerar ordem de compra",
    "edit_order": "Editar ordem",
    "cancel_order": "Cancelar ordem",
    "push_to_erp": "Enviar ao ERP",
    "refresh_order": "Atualizar dados",
    "track_receipt": "Acompanhar recebimento",
    "view_quotes": "Acompanhar cotacao",
    "view_order": "Abrir ordem",
    "view_history": "Ver historico",
    "submit_quote": "Enviar proposta",
}


FLOW_POLICY: Dict[str, Dict[str, Dict[str, object]]] = {
    "solicitacao": {
        "pending_rfq": {
            "allowed_actions": [
                "edit_request",
                "update_request_status",
                "add_request_item",
                "edit_request_item",
                "delete_request_item",
                "cancel_request",
                "open_rfq",
                "view_inbox",
            ],
            "primary_action": "open_rfq",
        },
        "in_rfq": {
            "allowed_actions": [
                "edit_request",
                "update_request_status",
                "cancel_request",
                "view_quotes",
                "view_inbox",
            ],
            "primary_action": "view_quotes",
        },
        "awarded": {
            "allowed_actions": ["view_quotes", "view_decision", "view_inbox", "view_history"],
            "primary_action": "view_decision",
        },
        "ordered": {
            "allowed_actions": ["view_order", "view_history", "view_inbox"],
            "primary_action": "view_order",
        },
        "partially_received": {
            "allowed_actions": ["view_order", "track_receipt", "view_history"],
            "primary_action": "track_receipt",
        },
        "received": {
            "allowed_actions": ["view_history"],
            "primary_action": "view_history",
        },
        "cancelled": {
            "allowed_actions": ["view_history"],
            "primary_action": "view_history",
        },
    },
    "cotacao": {
        "draft": {
            "allowed_actions": ["edit_rfq", "update_rfq_status", "invite_supplier", "cancel_rfq", "view_quotes"],
            "primary_action": "invite_supplier",
        },
        "open": {
            "allowed_actions": [
                "edit_rfq",
                "update_rfq_status",
                "invite_supplier",
                "manage_item_supplier",
                "reopen_invite",
                "extend_invite",
                "cancel_invite",
                "save_supplier_quote",
                "award_rfq",
                "cancel_rfq",
                "view_quotes",
            ],
            "primary_action": "invite_supplier",
        },
        "collecting_quotes": {
            "allowed_actions": [
                "edit_rfq",
                "update_rfq_status",
                "invite_supplier",
                "manage_item_supplier",
                "reopen_invite",
                "extend_invite",
                "cancel_invite",
                "save_supplier_quote",
                "award_rfq",
                "cancel_rfq",
                "view_quotes",
            ],
            "primary_action": "award_rfq",
        },
        "closed": {
            "allowed_actions": ["update_rfq_status", "award_rfq", "cancel_rfq", "view_quotes"],
            "primary_action": "award_rfq",
        },
        "awarded": {
            "allowed_actions": ["view_award", "create_purchase_order", "view_quotes"],
            "primary_action": "create_purchase_order",
        },
        "cancelled": {
            "allowed_actions": ["view_history"],
            "primary_action": "view_history",
        },
    },
    "decisao": {
        "awarded": {
            "allowed_actions": ["review_decision", "create_purchase_order", "view_quotes"],
            "primary_action": "create_purchase_order",
        },
        "converted_to_po": {
            "allowed_actions": ["review_decision", "view_order"],
            "primary_action": "view_order",
        },
        "cancelled": {
            "allowed_actions": ["review_decision", "view_history"],
            "primary_action": "view_history",
        },
    },
    "ordem_compra": {
        "draft": {
            "allowed_actions": ["view_order", "edit_order", "cancel_order", "push_to_erp"],
            "primary_action": "push_to_erp",
        },
        "approved": {
            "allowed_actions": ["view_order", "edit_order", "cancel_order", "push_to_erp"],
            "primary_action": "push_to_erp",
        },
        "sent_to_erp": {
            "allowed_actions": ["view_order", "refresh_order"],
            "primary_action": "refresh_order",
        },
        "erp_error": {
            "allowed_actions": ["view_order", "edit_order", "cancel_order", "push_to_erp"],
            "primary_action": "push_to_erp",
        },
        "erp_accepted": {
            "allowed_actions": ["view_order", "track_receipt", "view_history"],
            "primary_action": "track_receipt",
        },
        "partially_received": {
            "allowed_actions": ["view_order", "track_receipt", "view_history"],
            "primary_action": "track_receipt",
        },
        "received": {
            "allowed_actions": ["view_order", "view_history"],
            "primary_action": "view_history",
        },
        "cancelled": {
            "allowed_actions": ["view_history"],
            "primary_action": "view_history",
        },
    },
    "fornecedor": {
        "pending": {
            "allowed_actions": ["open_invite_portal", "extend_invite", "cancel_invite", "reopen_invite"],
            "primary_action": "open_invite_portal",
        },
        "opened": {
            "allowed_actions": ["submit_quote", "extend_invite", "cancel_invite", "reopen_invite"],
            "primary_action": "submit_quote",
        },
        "submitted": {
            "allowed_actions": ["reopen_invite", "view_history"],
            "primary_action": "view_history",
        },
        "expired": {
            "allowed_actions": ["reopen_invite", "cancel_invite"],
            "primary_action": "reopen_invite",
        },
        "cancelled": {
            "allowed_actions": ["reopen_invite", "view_history"],
            "primary_action": "view_history",
        },
    },
}


def _fallback_policy() -> Dict[str, object]:
    return {"allowed_actions": [], "primary_action": None}


def status_policy(stage: str, status: str | None) -> Dict[str, object]:
    if not status:
        return _fallback_policy()
    return FLOW_POLICY.get(stage, {}).get(str(status), _fallback_policy())


def allowed_actions(stage: str, status: str | None) -> List[str]:
    actions = status_policy(stage, status).get("allowed_actions") or []
    if not isinstance(actions, list):
        return []
    return [str(action) for action in actions]


def primary_action(stage: str, status: str | None) -> str | None:
    action = status_policy(stage, status).get("primary_action")
    if not action:
        return None
    return str(action)


def action_allowed(stage: str, status: str | None, action: str) -> bool:
    if not action:
        return False
    return action in set(allowed_actions(stage, status))


def action_label(action: str, fallback: str | None = None) -> str:
    label = ACTION_LABELS.get(action)
    if label:
        return label
    if fallback is not None:
        return fallback
    return action


def flow_meta(stage: str, status: str | None) -> Dict[str, object]:
    return {
        "stage": stage,
        "status": status,
        "allowed_actions": allowed_actions(stage, status),
        "primary_action": primary_action(stage, status),
    }


def _stage_index(stage: str) -> int:
    for idx, item in enumerate(PROCESS_STAGES):
        if item["key"] == stage:
            return idx
    return 0


def build_process_steps(current_stage: str) -> List[Dict[str, object]]:
    current_idx = _stage_index(current_stage)
    steps: List[Dict[str, object]] = []
    for idx, stage in enumerate(PROCESS_STAGES):
        state = "future"
        if idx < current_idx:
            state = "completed"
        elif idx == current_idx:
            state = "current"
        steps.append(
            {
                "key": stage["key"],
                "label": stage["label"],
                "state": state,
            }
        )
    return steps


def stage_for_purchase_request_status(status: str | None) -> str:
    mapping = {
        "pending_rfq": "solicitacao",
        "in_rfq": "cotacao",
        "awarded": "decisao",
        "ordered": "ordem_compra",
        "partially_received": "erp",
        "received": "erp",
        "cancelled": "solicitacao",
    }
    return mapping.get(str(status or "").strip(), "solicitacao")


def stage_for_rfq_status(status: str | None) -> str:
    mapping = {
        "draft": "cotacao",
        "open": "cotacao",
        "collecting_quotes": "cotacao",
        "closed": "cotacao",
        "awarded": "decisao",
        "cancelled": "cotacao",
    }
    return mapping.get(str(status or "").strip(), "cotacao")


def stage_for_award_status(status: str | None) -> str:
    mapping = {
        "awarded": "decisao",
        "converted_to_po": "ordem_compra",
        "cancelled": "decisao",
    }
    return mapping.get(str(status or "").strip(), "decisao")


def stage_for_purchase_order_status(status: str | None) -> str:
    mapping = {
        "draft": "ordem_compra",
        "approved": "ordem_compra",
        "sent_to_erp": "ordem_compra",
        "erp_error": "ordem_compra",
        "cancelled": "ordem_compra",
        "erp_accepted": "erp",
        "partially_received": "erp",
        "received": "erp",
    }
    return mapping.get(str(status or "").strip(), "ordem_compra")


def frontend_bundle() -> Dict[str, object]:
    return {
        "stages": PROCESS_STAGES,
        "policy": FLOW_POLICY,
        "action_labels": ACTION_LABELS,
    }

