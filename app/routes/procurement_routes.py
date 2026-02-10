
from __future__ import annotations

import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
from urllib.parse import quote

from app.application.analytics_service import AnalyticsService
from app.application.erp_outbox_service import ErpOutboxService
from app.application.procurement_service import ProcurementService
from flask import Blueprint, current_app, g, jsonify, render_template, request, session

from app.db import get_db, get_read_db
from app.domain.contracts import (
    AnalyticsRequestInput,
    PurchaseOrderErpIntentInput,
    PurchaseOrderFromAwardInput,
    PurchaseRequestCreateInput,
    RfqAwardInput,
    RfqCreateInput,
)
from app.erp_client import DEFAULT_RISK_FLAGS, fetch_erp_records
from app.errors import IntegrationError, ValidationError
from app.policies import current_role as policy_current_role
from app.policies import require_roles as policy_require_roles
from app.procurement.analytics import (
    analytics_sections,
    build_analytics_payload,
    build_filter_options as build_analytics_filter_options,
    normalize_section_key,
    parse_analytics_filters,
    resolve_visibility as resolve_analytics_visibility,
)
from app.procurement.critical_actions import get_critical_action, resolve_confirmation
from app.procurement.erp_outbox import (
    find_pending_purchase_order_push,
    queue_purchase_order_push,
)
from app.procurement.flow_policy import (
    action_label as flow_action_label,
    action_allowed as flow_action_allowed,
    allowed_actions as flow_allowed_actions,
    build_process_steps,
    flow_meta,
    primary_action as flow_primary_action,
    stage_for_award_status,
    stage_for_purchase_order_status,
    stage_for_purchase_request_status,
    stage_for_rfq_status,
)
from app.tenant import DEFAULT_TENANT_ID, current_tenant_id, scoped_tenant_id
from app.ui_strings import (
    confirm_message,
    erp_status_payload,
    erp_timeline_event_label,
    error_message,
    get_ui_text,
    status_keys_for_group,
    success_message,
)


procurement_bp = Blueprint("procurement", __name__)


ALLOWED_TYPES = {"purchase_request", "rfq", "purchase_order"}
ALLOWED_PRIORITIES = {"low", "medium", "high", "urgent"}
ALLOWED_PR_STATUSES = set(status_keys_for_group("solicitacao"))
ALLOWED_PO_STATUSES = set(status_keys_for_group("ordem_compra"))
ALLOWED_RECEIPT_STATUSES = {"pending", "partially_received", "received"}
ALLOWED_RFQ_STATUSES = set(status_keys_for_group("cotacao"))
ALLOWED_INBOX_STATUSES = ALLOWED_PR_STATUSES | ALLOWED_PO_STATUSES | ALLOWED_RFQ_STATUSES

_ANALYTICS_SERVICE = AnalyticsService(ttl_seconds=60)
_PROCUREMENT_SERVICE = ProcurementService(outbox_service=ErpOutboxService())


def _clear_analytics_cache_for_tests() -> None:
    _ANALYTICS_SERVICE.clear_cache()


def _err(key: str, fallback: str | None = None) -> str:
    return error_message(key, fallback)


def _ok(key: str, fallback: str | None = None) -> str:
    return success_message(key, fallback)


def _confirm(key: str, fallback: str | None = None) -> str:
    return confirm_message(key, fallback)


def _process_context(stage: str) -> dict:
    return {
        "process_stage": stage,
        "process_steps": build_process_steps(stage),
    }


def _current_role() -> str:
    return policy_current_role()


def _require_roles(*allowed_roles: str) -> None:
    policy_require_roles(*allowed_roles)


def _forbidden_action(stage: str, status: str | None, action: str, http_status: int = 409):
    raise ValidationError(
        code="action_not_allowed_for_status",
        message_key="action_not_allowed_for_status",
        http_status=http_status,
        critical=False,
        payload={
            "stage": stage,
            "status": status,
            "action": action,
            "allowed_actions": flow_allowed_actions(stage, status),
            "primary_action": flow_primary_action(stage, status),
        },
    )


def _filter_actions(meta: Dict[str, object], blocked: set[str]) -> Dict[str, object]:
    allowed = [action for action in list(meta.get("allowed_actions") or []) if action not in blocked]
    primary = meta.get("primary_action")
    if primary not in allowed:
        primary = allowed[0] if allowed else None
    return {"allowed_actions": allowed, "primary_action": primary}


def _erp_timeline_event_type(reason: str | None, from_status: str | None, to_status: str | None) -> str | None:
    normalized_reason = str(reason or "").strip().lower()
    normalized_from = str(from_status or "").strip().lower()
    normalized_to = str(to_status or "").strip().lower()

    if normalized_reason == "po_push_retry_queued":
        return "reenvio"
    if normalized_reason == "po_push_queued":
        return "reenvio" if normalized_from == "erp_error" else "envio"
    if normalized_reason == "po_push_retry_started":
        return "reenvio"
    if normalized_reason == "po_push_started":
        return "reenvio" if normalized_from == "erp_error" else "envio"
    if normalized_reason in {"po_push_failed"}:
        return "erro"
    if normalized_reason in {"po_push_rejected", "po_push_succeeded"}:
        return "resposta"

    if normalized_to == "sent_to_erp":
        return "reenvio" if normalized_from == "erp_error" else "envio"
    if normalized_to in {"erp_accepted", "erp_error"}:
        return "resposta"
    return None


def _erp_timeline_event_message(event_type: str, reason: str | None, to_status: str | None) -> str:
    normalized_reason = str(reason or "").strip().lower()
    normalized_to = str(to_status or "").strip().lower()

    if normalized_reason == "po_push_rejected":
        return _err("erp_rejected", _err("erp_order_rejected"))
    if event_type == "erro":
        return _err("erp_unavailable", _err("erp_temporarily_unavailable"))
    if normalized_to == "erp_accepted":
        return _ok("erp_accepted")
    if normalized_to == "erp_error":
        return _err("erp_unavailable", _err("erp_temporarily_unavailable"))
    return _ok("order_sent_to_erp")


def _build_erp_timeline(events: List[dict]) -> List[dict]:
    timeline: List[dict] = []
    for event in events:
        if str(event.get("entity") or "") != "purchase_order":
            continue
        event_type = _erp_timeline_event_type(
            event.get("reason"),
            event.get("from_status"),
            event.get("to_status"),
        )
        if not event_type:
            continue

        reason_value = str(event.get("reason") or "").strip().lower()
        error_hint = "rejected" if reason_value == "po_push_rejected" else None
        status_view = erp_status_payload(
            event.get("to_status"),
            erp_last_error=error_hint,
            last_updated_at=event.get("occurred_at"),
        )
        timeline.append(
            {
                "id": event.get("id"),
                "event_type": event_type,
                "event_label": erp_timeline_event_label(event_type, event_type),
                "message": _erp_timeline_event_message(event_type, event.get("reason"), event.get("to_status")),
                "from_status": event.get("from_status"),
                "to_status": event.get("to_status"),
                "reason": event.get("reason"),
                "occurred_at": event.get("occurred_at"),
                "erp_status": status_view,
            }
        )
    return timeline


def _erp_next_action_key(order_status: str | None, allowed_actions: List[str], primary_action: str | None, erp_ui_key: str) -> str | None:
    normalized_status = str(order_status or "").strip().lower()
    if "push_to_erp" in set(allowed_actions):
        return "push_to_erp"
    if normalized_status == "sent_to_erp":
        return "refresh_order"
    return primary_action or (allowed_actions[0] if allowed_actions else None)


def _erp_action_label(action_key: str | None, erp_ui_key: str | None = None) -> str | None:
    if not action_key:
        return None
    if action_key == "push_to_erp" and erp_ui_key in {"rejeitado", "reenvio_necessario"}:
        return get_ui_text("erp.next_action.resend", "Reenviar ao ERP")
    if action_key == "refresh_order":
        return get_ui_text("erp.next_action.await_response", "Aguardar retorno do ERP.")
    return flow_action_label(action_key, action_key)


def _critical_confirmation_details(action_key: str) -> dict | None:
    meta = get_critical_action(action_key)
    if not meta:
        return None

    confirm_key = meta.get("confirm_message_key") or action_key
    impact_key = meta.get("impact_text_key") or f"impact.{action_key}"
    return {
        "action_key": action_key,
        "confirm_key": confirm_key,
        "confirm_message": _confirm(confirm_key, confirm_key),
        "impact_key": impact_key,
        "impact": get_ui_text(impact_key, impact_key),
    }


def _audit_confirmation(action_key: str, entity: str, entity_id: int, mode: str) -> None:
    request_id = (getattr(g, "request_id", None) or "").strip() or "n/a"
    user = (session.get("user_email") or session.get("display_name") or "anonymous").strip() or "anonymous"
    current_app.logger.info(
        "confirmation_event",
        extra={
            "request_id": request_id,
            "user": user,
            "action": action_key,
            "entity": entity,
            "entity_id": entity_id,
            "mode": mode,
        },
    )


def _require_critical_confirmation(
    action_key: str,
    *,
    entity: str,
    entity_id: int,
    payload: dict | None = None,
) -> None:
    meta = get_critical_action(action_key)
    if not meta:
        return

    confirmed, mode = resolve_confirmation(request, payload)
    if not confirmed:
        raise ValidationError(
            code="confirmation_required",
            message_key="confirmation_required",
            http_status=400,
            critical=False,
            payload={
                "action": action_key,
                "confirmation": _critical_confirmation_details(action_key),
            },
        )

    _audit_confirmation(action_key, entity, entity_id, mode)


# Compatibilidade: aceitar scopes antigos (plural) nos filtros de logs.
SCOPE_ALIASES = {
    "purchase_order": ("purchase_order", "purchase_orders"),
    "purchase_orders": ("purchase_order", "purchase_orders"),
    "purchase_request": ("purchase_request", "purchase_requests"),
    "purchase_requests": ("purchase_request", "purchase_requests"),
    "rfq": ("rfq", "rfqs"),
    "rfqs": ("rfq", "rfqs"),
    "supplier": ("supplier", "suppliers"),
    "suppliers": ("supplier", "suppliers"),
    "category": ("category", "categories"),
    "categories": ("category", "categories"),
    "receipt": ("receipt", "receipts"),
    "receipts": ("receipt", "receipts"),
    "quote": ("quote", "quotes"),
    "quotes": ("quote", "quotes"),
    "quote_process": ("quote_process", "quote_processes"),
    "quote_processes": ("quote_process", "quote_processes"),
    "quote_supplier": ("quote_supplier", "quote_suppliers"),
    "quote_suppliers": ("quote_supplier", "quote_suppliers"),
}

SYNC_SUPPORTED_SCOPES = {
    "supplier",
    "purchase_request",
    "purchase_order",
    "receipt",
    "quote",
    "quote_process",
    "quote_supplier",
}


@procurement_bp.route("/procurement/inbox", methods=["GET"])
def procurement_inbox_page():
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    type_hint = (request.args.get("type") or "").strip().lower()
    stage = "solicitacao"
    if type_hint == "rfq":
        stage = "cotacao"
    elif type_hint == "purchase_order":
        stage = "ordem_compra"
    return render_template("procurement_inbox.html", tenant_id=tenant_id, **_process_context(stage))


@procurement_bp.route("/procurement/solicitacoes", methods=["GET"])
def purchase_requests_page():
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template("procurement_solicitacoes.html", tenant_id=tenant_id, **_process_context("solicitacao"))


@procurement_bp.route("/procurement/cotacoes/abrir", methods=["GET"])
def cotacao_open_page():
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template("procurement_cotacao_abertura.html", tenant_id=tenant_id, **_process_context("cotacao"))


@procurement_bp.route("/procurement/cotacoes", methods=["GET"])
def cotacoes_page():
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template("procurement_cotacoes.html", tenant_id=tenant_id, **_process_context("cotacao"))


@procurement_bp.route("/procurement/cotacoes/<int:rfq_id>", methods=["GET"])
def cotacao_detail_page(rfq_id: int):
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template(
        "procurement_cotacao.html",
        tenant_id=tenant_id,
        rfq_id=rfq_id,
        **_process_context("cotacao"),
    )


@procurement_bp.route("/procurement/purchase-orders/<int:purchase_order_id>", methods=["GET"])
def purchase_order_detail_page(purchase_order_id: int):
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template(
        "procurement_purchase_order.html",
        tenant_id=tenant_id,
        purchase_order_id=purchase_order_id,
        **_process_context("ordem_compra"),
    )


@procurement_bp.route("/procurement/ordens-compra", methods=["GET"])
def purchase_orders_page():
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template("procurement_ordens_compra.html", tenant_id=tenant_id, **_process_context("ordem_compra"))


@procurement_bp.route("/procurement/integrations/logs", methods=["GET"])
def integration_logs_page():
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template("procurement_integration_logs.html", tenant_id=tenant_id, **_process_context("erp"))


@procurement_bp.route("/procurement/integrations/erp", methods=["GET"])
def erp_followup_page():
    _require_roles("admin", "manager")
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template("procurement_erp_followup.html", tenant_id=tenant_id, **_process_context("erp"))


@procurement_bp.route("/procurement/aprovacoes", methods=["GET"])
def approvals_page():
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template("procurement_approvals.html", tenant_id=tenant_id, **_process_context("decisao"))


@procurement_bp.route("/procurement/analises", methods=["GET"])
@procurement_bp.route("/procurement/analises/<string:section>", methods=["GET"])
def analytics_dashboard_page(section: str = "overview"):
    role = _current_role()
    section_key = normalize_section_key(section)
    if section_key == "executive":
        _require_roles("manager", "admin")
    else:
        _require_roles("buyer", "manager", "admin", "approver")
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    sections = _ANALYTICS_SERVICE.filter_sections_for_role(role, analytics_sections())
    return render_template(
        "procurement_analytics.html",
        tenant_id=tenant_id,
        analytics_section_key=section_key,
        analytics_sections=sections,
        active_nav=f"analytics_{section_key}",
    )


@procurement_bp.route("/fornecedor/convite/<string:token>", methods=["GET"])
def supplier_invite_page(token: str):
    return render_template("supplier_quote_portal.html", invite_token=token.strip())


@procurement_bp.route("/api/procurement/inbox", methods=["GET"])
def procurement_inbox():
    db = get_read_db()
    tenant_id = current_tenant_id()

    limit = _parse_int(request.args.get("limit"), default=50, min_value=1, max_value=200)
    offset = _parse_int(request.args.get("offset"), default=0, min_value=0, max_value=10_000)
    filters = _parse_inbox_filters(request.args)

    cards = _load_inbox_cards(db, tenant_id)
    items = _load_inbox_items(db, tenant_id, limit, offset, filters)

    has_more = len(items) == limit
    return jsonify(
        {
            "items": items,
            "kpis": cards,
            "paging": {
                "limit": limit,
                "offset": offset,
                "has_more": has_more,
            },
            "filters": filters,
        }
    )


@procurement_bp.route("/api/procurement/analytics/filters", methods=["GET"])
def analytics_filters_api():
    _require_roles("buyer", "manager", "admin", "approver")
    db = get_read_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    role = _current_role()
    payload = _ANALYTICS_SERVICE.build_filters_payload(
        db,
        AnalyticsRequestInput(
            section="filters",
            role=role,
            tenant_id=tenant_id,
            request_args=request.args.to_dict(flat=True),
            user_email=session.get("user_email"),
            display_name=session.get("display_name"),
            team_members=list(session.get("team_members") or []),
        ),
        parse_filters_fn=parse_analytics_filters,
        resolve_visibility_fn=resolve_analytics_visibility,
        build_filter_options_fn=build_analytics_filter_options,
    )
    return jsonify(payload)


@procurement_bp.route("/api/procurement/analytics", methods=["GET"])
@procurement_bp.route("/api/procurement/analytics/<string:section>", methods=["GET"])
def analytics_dashboard_api(section: str = "overview"):
    db = get_read_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    role = _current_role()
    section_key = normalize_section_key(request.args.get("section") or section)
    if section_key == "executive":
        _require_roles("manager", "admin")
    else:
        _require_roles("buyer", "manager", "admin", "approver")
    payload = _ANALYTICS_SERVICE.build_dashboard_payload(
        db,
        AnalyticsRequestInput(
            section=section_key,
            role=role,
            tenant_id=tenant_id,
            request_args=request.args.to_dict(flat=True),
            user_email=session.get("user_email"),
            display_name=session.get("display_name"),
            team_members=list(session.get("team_members") or []),
        ),
        parse_filters_fn=parse_analytics_filters,
        resolve_visibility_fn=resolve_analytics_visibility,
        build_payload_fn=build_analytics_payload,
    )
    return jsonify(payload)


@procurement_bp.route("/api/procurement/purchase-requests/open", methods=["GET"])
def purchase_requests_open():
    db = get_db()
    tenant_id = current_tenant_id()
    _hydrate_purchase_requests_from_erp_raw(db, tenant_id)
    limit = _parse_int(request.args.get("limit"), default=80, min_value=1, max_value=200)
    items = _load_open_purchase_requests(db, tenant_id, limit)
    return jsonify({"items": items})


@procurement_bp.route("/api/procurement/purchase-request-items/open", methods=["GET"])
def purchase_request_items_open():
    db = get_db()
    tenant_id = current_tenant_id()
    _hydrate_purchase_requests_from_erp_raw(db, tenant_id)
    limit = _parse_int(request.args.get("limit"), default=120, min_value=1, max_value=300)
    items = _load_open_purchase_request_items(db, tenant_id, limit)
    return jsonify({"items": items})


@procurement_bp.route("/api/procurement/solicitacoes", methods=["GET", "POST"])
def procurement_solicitacoes_api():
    if request.method == "POST":
        db = get_db()
        tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
        payload = request.get_json(silent=True) or {}

        status = str(payload.get("status") or "pending_rfq").strip()
        if status not in ALLOWED_PR_STATUSES:
            return jsonify({"error": "status_invalid", "message": _err("status_invalid")}), 400

        priority = str(payload.get("priority") or "medium").strip()
        if priority not in ALLOWED_PRIORITIES:
            return jsonify({"error": "priority_invalid", "message": _err("priority_invalid")}), 400

        number = (payload.get("number") or "").strip() or None
        requested_by = (payload.get("requested_by") or "").strip() or None
        department = (payload.get("department") or "").strip() or None
        needed_at = (payload.get("needed_at") or "").strip() or None
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        if not items:
            return jsonify({"error": "items_required", "message": _err("items_required")}), 400

        result = _PROCUREMENT_SERVICE.create_purchase_request(
            db,
            tenant_id=tenant_id,
            create_input=PurchaseRequestCreateInput(
                status=status,
                priority=priority,
                number=number,
                requested_by=requested_by,
                department=department,
                needed_at=needed_at,
                items=items,
            ),
            parse_optional_int_fn=_parse_optional_int,
            parse_optional_float_fn=_parse_optional_float,
            err_fn=_err,
        )
        db.commit()
        return jsonify(result.payload), result.status_code

    db = get_db()
    tenant_id = current_tenant_id()
    _hydrate_purchase_requests_from_erp_raw(db, tenant_id)
    limit = _parse_int(request.args.get("limit"), default=120, min_value=1, max_value=300)
    status_values = _parse_csv_values(request.args.get("status"))
    erp_only = request.args.get("erp_only") == "1"
    items = _load_purchase_requests_panel(
        db,
        tenant_id=tenant_id,
        limit=limit,
        status_values=status_values,
        erp_only=erp_only,
    )
    return jsonify({"items": items})


@procurement_bp.route("/api/procurement/solicitacoes/<int:purchase_request_id>", methods=["PATCH", "DELETE"])
def procurement_solicitacao_crud_api(purchase_request_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    row = db.execute(
        """
        SELECT id, status, external_id, erp_num_cot, erp_num_pct
        FROM purchase_requests
        WHERE id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (purchase_request_id, tenant_id),
    ).fetchone()
    if not row:
        return (
            jsonify(
                {
                    "error": "purchase_request_not_found", "message": _err("purchase_request_not_found"),
                    "purchase_request_id": purchase_request_id,
                }
            ),
            404,
        )

    previous_status = row["status"]
    is_erp_managed = bool(row["external_id"] or row["erp_num_cot"] or row["erp_num_pct"])
    if is_erp_managed:
        return (
            jsonify(
                {
                    "error": "erp_managed_request_readonly", "message": _err("erp_managed_request_readonly"),
                }
            ),
            409,
        )

    if request.method == "DELETE":
        if not flow_action_allowed("solicitacao", previous_status, "cancel_request"):
            return _forbidden_action("solicitacao", previous_status, "cancel_request")
        _require_critical_confirmation(
            "cancel_request",
            entity="purchase_request",
            entity_id=purchase_request_id,
        )
        if previous_status == "cancelled":
            return jsonify({"status": "cancelled", "purchase_request_id": purchase_request_id}), 200
        db.execute(
            """
            UPDATE purchase_requests
            SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (purchase_request_id, tenant_id),
        )
        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('purchase_request', ?, ?, 'cancelled', 'purchase_request_cancelled', ?)
            """,
            (purchase_request_id, previous_status, tenant_id),
        )
        db.commit()
        return jsonify({"status": "cancelled", "purchase_request_id": purchase_request_id}), 200

    if not flow_action_allowed("solicitacao", previous_status, "edit_request"):
        return _forbidden_action("solicitacao", previous_status, "edit_request")

    payload = request.get_json(silent=True) or {}
    updates: List[str] = []
    params: List[object] = []

    if "number" in payload:
        updates.append("number = ?")
        params.append((payload.get("number") or "").strip() or None)

    if "requested_by" in payload:
        updates.append("requested_by = ?")
        params.append((payload.get("requested_by") or "").strip() or None)

    if "department" in payload:
        updates.append("department = ?")
        params.append((payload.get("department") or "").strip() or None)

    if "needed_at" in payload:
        updates.append("needed_at = ?")
        params.append((payload.get("needed_at") or "").strip() or None)

    if "priority" in payload:
        priority = str(payload.get("priority") or "").strip()
        if priority and priority not in ALLOWED_PRIORITIES:
            return jsonify({"error": "priority_invalid", "message": _err("priority_invalid")}), 400
        updates.append("priority = ?")
        params.append(priority or "medium")

    next_status = previous_status
    if "status" in payload:
        if not flow_action_allowed("solicitacao", previous_status, "update_request_status"):
            return _forbidden_action("solicitacao", previous_status, "update_request_status")
        candidate = str(payload.get("status") or "").strip()
        if candidate not in ALLOWED_PR_STATUSES:
            return jsonify({"error": "status_invalid", "message": _err("status_invalid")}), 400
        if candidate == "cancelled" and candidate != previous_status:
            _require_critical_confirmation(
                "cancel_request",
                entity="purchase_request",
                entity_id=purchase_request_id,
                payload=payload,
            )
        next_status = candidate
        updates.append("status = ?")
        params.append(candidate)

    if not updates:
        return jsonify({"error": "no_changes", "message": _err("no_changes")}), 400

    params.extend([purchase_request_id, tenant_id])
    db.execute(
        f"""
        UPDATE purchase_requests
        SET {", ".join(updates)}, updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND tenant_id = ?
        """,
        tuple(params),
    )

    if next_status != previous_status:
        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('purchase_request', ?, ?, ?, 'purchase_request_updated', ?)
            """,
            (purchase_request_id, previous_status, next_status, tenant_id),
        )

    db.commit()
    return jsonify({"purchase_request_id": purchase_request_id, "status": next_status}), 200


@procurement_bp.route("/api/procurement/solicitacoes/<int:purchase_request_id>/itens", methods=["POST"])
def procurement_solicitacao_item_create_api(purchase_request_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    request_row = db.execute(
        """
        SELECT id, status, external_id, erp_num_cot, erp_num_pct
        FROM purchase_requests
        WHERE id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (purchase_request_id, tenant_id),
    ).fetchone()
    if not request_row:
        return jsonify({"error": "purchase_request_not_found", "message": _err("purchase_request_not_found")}), 404
    if request_row["external_id"] or request_row["erp_num_cot"] or request_row["erp_num_pct"]:
        return (
            jsonify(
                {
                    "error": "erp_managed_request_readonly", "message": _err("erp_managed_request_readonly"),
                }
            ),
            409,
        )
    if not flow_action_allowed("solicitacao", request_row["status"], "add_request_item"):
        return _forbidden_action("solicitacao", request_row["status"], "add_request_item")
    if request_row["status"] != "pending_rfq":
        return jsonify({"error": "request_locked", "message": _err("request_locked")}), 400

    payload = request.get_json(silent=True) or {}
    description = (payload.get("description") or "").strip()
    if not description:
        return jsonify({"error": "description_required", "message": _err("description_required")}), 400
    quantity = _parse_optional_float(payload.get("quantity"))
    if quantity is None or quantity <= 0:
        quantity = 1
    uom = (payload.get("uom") or "UN").strip() or "UN"
    category = (payload.get("category") or "").strip() or None
    line_no = _parse_optional_int(payload.get("line_no"))
    if line_no is None:
        next_line = db.execute(
            "SELECT COALESCE(MAX(line_no), 0) + 1 AS next_line FROM purchase_request_items WHERE purchase_request_id = ? AND tenant_id = ?",
            (purchase_request_id, tenant_id),
        ).fetchone()
        line_no = int(next_line["next_line"] or 1)

    cursor = db.execute(
        """
        INSERT INTO purchase_request_items (
            purchase_request_id, line_no, description, quantity, uom, category, tenant_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        RETURNING id
        """,
        (purchase_request_id, line_no, description, quantity, uom, category, tenant_id),
    )
    item_row = cursor.fetchone()
    db.commit()
    return jsonify({"id": int(item_row["id"] if isinstance(item_row, dict) else item_row[0])}), 201


@procurement_bp.route(
    "/api/procurement/solicitacoes/<int:purchase_request_id>/itens/<int:item_id>",
    methods=["PATCH", "DELETE"],
)
def procurement_solicitacao_item_crud_api(purchase_request_id: int, item_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    request_row = db.execute(
        """
        SELECT id, status, external_id, erp_num_cot, erp_num_pct
        FROM purchase_requests
        WHERE id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (purchase_request_id, tenant_id),
    ).fetchone()
    if not request_row:
        return jsonify({"error": "purchase_request_not_found", "message": _err("purchase_request_not_found")}), 404
    if request_row["external_id"] or request_row["erp_num_cot"] or request_row["erp_num_pct"]:
        return (
            jsonify(
                {
                    "error": "erp_managed_request_readonly", "message": _err("erp_managed_request_readonly"),
                }
            ),
            409,
        )
    requested_action = "delete_request_item" if request.method == "DELETE" else "edit_request_item"
    if not flow_action_allowed("solicitacao", request_row["status"], requested_action):
        return _forbidden_action("solicitacao", request_row["status"], requested_action)
    if request_row["status"] != "pending_rfq":
        return jsonify({"error": "request_locked", "message": _err("request_locked")}), 400

    item_row = db.execute(
        """
        SELECT id
        FROM purchase_request_items
        WHERE id = ? AND purchase_request_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (item_id, purchase_request_id, tenant_id),
    ).fetchone()
    if not item_row:
        return jsonify({"error": "item_not_found", "message": _err("item_not_found")}), 404

    if request.method == "DELETE":
        db.execute(
            "DELETE FROM purchase_request_items WHERE id = ? AND purchase_request_id = ? AND tenant_id = ?",
            (item_id, purchase_request_id, tenant_id),
        )
        db.commit()
        return jsonify({"deleted": True, "item_id": item_id}), 200

    payload = request.get_json(silent=True) or {}
    updates: List[str] = []
    params: List[object] = []
    if "description" in payload:
        description = (payload.get("description") or "").strip()
        if not description:
            return jsonify({"error": "description_required", "message": _err("description_required")}), 400
        updates.append("description = ?")
        params.append(description)
    if "quantity" in payload:
        quantity = _parse_optional_float(payload.get("quantity"))
        if quantity is None or quantity <= 0:
            return jsonify({"error": "quantity_invalid", "message": _err("quantity_invalid")}), 400
        updates.append("quantity = ?")
        params.append(quantity)
    if "uom" in payload:
        updates.append("uom = ?")
        params.append((payload.get("uom") or "UN").strip() or "UN")
    if "line_no" in payload:
        line_no = _parse_optional_int(payload.get("line_no"))
        if line_no is None or line_no <= 0:
            return jsonify({"error": "line_no_invalid", "message": _err("line_no_invalid")}), 400
        updates.append("line_no = ?")
        params.append(line_no)
    if "category" in payload:
        updates.append("category = ?")
        params.append((payload.get("category") or "").strip() or None)

    if not updates:
        return jsonify({"error": "no_changes", "message": _err("no_changes")}), 400

    params.extend([item_id, purchase_request_id, tenant_id])
    db.execute(
        f"""
        UPDATE purchase_request_items
        SET {", ".join(updates)}, updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND purchase_request_id = ? AND tenant_id = ?
        """,
        tuple(params),
    )
    db.commit()
    return jsonify({"item_id": item_id, "updated": True}), 200


@procurement_bp.route("/api/procurement/cotacoes/abertura-data", methods=["GET"])
def rfq_opening_data_api():
    db = get_read_db()
    tenant_id = current_tenant_id()
    limit = _parse_int(request.args.get("limit"), default=200, min_value=1, max_value=400)
    request_id = _parse_optional_int(request.args.get("purchase_request_id"))

    items = _load_open_purchase_request_items(db, tenant_id, limit)
    if request_id is not None:
        items = [item for item in items if int(item["purchase_request_id"]) == request_id]

    suppliers = _load_suppliers(db, tenant_id)
    grouped: Dict[int, dict] = {}
    for item in items:
        key = int(item["purchase_request_id"])
        group = grouped.get(key)
        if not group:
            group = {
                "purchase_request_id": key,
                "number": item.get("number"),
                "priority": item.get("priority"),
                "needed_at": item.get("needed_at"),
                "requested_by": item.get("requested_by"),
                "department": item.get("department"),
                "items": [],
            }
            grouped[key] = group
        group["items"].append(item)

    return jsonify(
        {
            "purchase_requests": list(grouped.values()),
            "items": items,
            "suppliers": suppliers,
        }
    )


@procurement_bp.route("/api/procurement/fornecedores", methods=["GET"])
def fornecedores_api():
    db = get_read_db()
    tenant_id = current_tenant_id()
    suppliers = _load_suppliers(db, tenant_id)
    return jsonify({"items": suppliers})


@procurement_bp.route("/api/procurement/cotacoes/<int:rfq_id>", methods=["GET"])
def cotacao_detail_api(rfq_id: int):
    db = get_read_db()
    tenant_id = current_tenant_id()

    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return (
            jsonify({"error": "rfq_not_found", "message": _err("rfq_not_found"), "rfq_id": rfq_id}),
            404,
        )

    award = _load_latest_award_for_rfq(db, tenant_id, rfq_id)
    purchase_order = None
    if award and award.get("purchase_order_id"):
        purchase_order = _load_purchase_order(db, tenant_id, int(award["purchase_order_id"]))

    itens = _load_rfq_items_with_quotes(db, tenant_id, rfq_id)
    convites = _load_rfq_supplier_invites(db, tenant_id, rfq_id)
    events = _load_status_events(db, tenant_id, entity="rfq", entity_id=rfq_id, limit=80)
    award_events: List[dict] = []
    if award:
        award_events = _load_status_events(db, tenant_id, entity="award", entity_id=int(award["id"]), limit=40)

    rfq_status = rfq["status"]
    rfq_flow = flow_meta("cotacao", rfq_status)
    award_flow = flow_meta("decisao", award["status"] if award else None)
    po_flow = flow_meta("ordem_compra", purchase_order["status"] if purchase_order else None)

    process_stage = stage_for_rfq_status(rfq_status)
    if award:
        process_stage = stage_for_award_status(award.get("status"))
    if purchase_order:
        process_stage = stage_for_purchase_order_status(purchase_order["status"])

    invite_items: List[dict] = []
    for invite in convites:
        invite_status = invite.get("status")
        invite_meta = flow_meta("fornecedor", invite_status)
        allowed_invite_actions: List[str] = []
        for action in ("reopen_invite", "extend_invite", "cancel_invite"):
            if flow_action_allowed("fornecedor", invite_status, action) and flow_action_allowed("cotacao", rfq_status, action):
                allowed_invite_actions.append(action)
        if invite.get("access_url") and flow_action_allowed("fornecedor", invite_status, "open_invite_portal"):
            allowed_invite_actions.append("open_invite_portal")
        primary_invite_action = invite_meta.get("primary_action")
        if primary_invite_action not in allowed_invite_actions:
            primary_invite_action = allowed_invite_actions[0] if allowed_invite_actions else None
        invite_items.append(
            {
                **invite,
                "allowed_actions": allowed_invite_actions,
                "primary_action": primary_invite_action,
            }
        )

    decisao_payload = None
    if award:
        decisao_payload = {
            **award,
            "allowed_actions": award_flow["allowed_actions"],
            "primary_action": award_flow["primary_action"],
            "process_stage": stage_for_award_status(award.get("status")),
        }

    ordem_compra_payload = None
    if purchase_order:
        ordem_compra_payload = {
            **_serialize_purchase_order(purchase_order),
            "allowed_actions": po_flow["allowed_actions"],
            "primary_action": po_flow["primary_action"],
            "process_stage": stage_for_purchase_order_status(purchase_order["status"]),
        }

    return jsonify(
        {
            "cotacao": {
                "id": rfq["id"],
                "titulo": rfq["title"],
                "status": rfq_status,
                "criada_em": rfq["created_at"],
                "atualizada_em": rfq["updated_at"],
                "allowed_actions": rfq_flow["allowed_actions"],
                "primary_action": rfq_flow["primary_action"],
                "process_stage": stage_for_rfq_status(rfq_status),
            },
            "itens": itens,
            "convites": invite_items,
            "decisao": decisao_payload,
            "ordem_compra": ordem_compra_payload,
            "eventos_cotacao": events,
            "eventos_decisao": award_events,
            "flow": {
                "process_stage": process_stage,
                "process_steps": build_process_steps(process_stage),
            },
        }
    )

@procurement_bp.route("/api/procurement/cotacoes/<int:rfq_id>/itens/<int:rfq_item_id>/fornecedores", methods=["POST"])
def cotacao_item_fornecedores_api(rfq_id: int, rfq_item_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}

    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": _err("rfq_not_found"), "rfq_id": rfq_id}), 404
    if not flow_action_allowed("cotacao", rfq["status"], "manage_item_supplier"):
        return _forbidden_action("cotacao", rfq["status"], "manage_item_supplier")

    rfq_item = _load_rfq_item(db, tenant_id, rfq_item_id)
    if not rfq_item or int(rfq_item["rfq_id"]) != rfq_id:
        return (
            jsonify(
                {
                    "error": "rfq_item_not_found", "message": _err("rfq_item_not_found"),
                    "rfq_item_id": rfq_item_id,
                }
            ),
            404,
        )

    supplier_ids = payload.get("supplier_ids") or []
    if not isinstance(supplier_ids, list) or not supplier_ids:
        return jsonify({"error": "supplier_ids_required", "message": _err("supplier_ids_required")}), 400

    valid_supplier_ids = {s["id"] for s in _load_suppliers(db, tenant_id)}

    for supplier_id in supplier_ids:
        try:
            parsed_supplier_id = int(supplier_id)
        except (TypeError, ValueError):
            continue
        if parsed_supplier_id not in valid_supplier_ids:
            continue
        db.execute(
            """
            INSERT INTO rfq_item_suppliers (rfq_item_id, supplier_id, tenant_id)
            VALUES (?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            (rfq_item_id, parsed_supplier_id, tenant_id),
        )

    db.commit()
    itens = _load_rfq_items_with_quotes(db, tenant_id, rfq_id)
    return jsonify({"itens": itens})


@procurement_bp.route("/api/procurement/cotacoes/<int:rfq_id>/propostas", methods=["POST", "PATCH"])
def cotacao_propostas_api(rfq_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}

    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": _err("rfq_not_found"), "rfq_id": rfq_id}), 404
    if not flow_action_allowed("cotacao", rfq["status"], "save_supplier_quote"):
        return _forbidden_action("cotacao", rfq["status"], "save_supplier_quote")

    supplier_id = payload.get("supplier_id")
    items = payload.get("items") or []

    if not supplier_id:
        return jsonify({"error": "supplier_id_required", "message": _err("supplier_id_required")}), 400
    if not isinstance(items, list) or not items:
        return jsonify({"error": "items_required", "message": _err("items_required")}), 400

    try:
        supplier_id = int(supplier_id)
    except (TypeError, ValueError):
        return jsonify({"error": "supplier_id_invalid", "message": _err("supplier_id_invalid")}), 400

    valid_supplier_ids = {s["id"] for s in _load_suppliers(db, tenant_id)}
    if supplier_id not in valid_supplier_ids:
        return jsonify({"error": "supplier_not_found", "message": _err("supplier_not_found")}), 404

    normalized_items_by_id: Dict[int, dict] = {}
    for item in items:
        rfq_item_id = item.get("rfq_item_id")
        unit_price = item.get("unit_price")
        lead_time_days = item.get("lead_time_days")

        if rfq_item_id in (None, "") or unit_price in (None, ""):
            continue

        try:
            parsed_rfq_item_id = int(rfq_item_id)
            parsed_unit_price = float(unit_price)
        except (TypeError, ValueError):
            continue

        if parsed_unit_price < 0:
            continue

        parsed_lead_time_days = None
        if lead_time_days not in (None, ""):
            try:
                parsed_lead_time_days = int(lead_time_days)
            except (TypeError, ValueError):
                continue
            if parsed_lead_time_days < 0:
                continue

        normalized_items_by_id[parsed_rfq_item_id] = {
            "rfq_item_id": parsed_rfq_item_id,
            "unit_price": parsed_unit_price,
            "lead_time_days": parsed_lead_time_days,
        }

    normalized_items = list(normalized_items_by_id.values())
    if not normalized_items:
        return (
            jsonify(
                {
                    "error": "valid_items_required", "message": _err("valid_items_required"),
                }
            ),
            400,
        )

    requested_item_ids = sorted(normalized_items_by_id.keys())
    placeholders = ",".join("?" for _ in requested_item_ids)

    valid_item_rows = db.execute(
        f"""
        SELECT id
        FROM rfq_items
        WHERE rfq_id = ? AND tenant_id = ? AND id IN ({placeholders})
        """,
        (rfq_id, tenant_id, *requested_item_ids),
    ).fetchall()
    valid_item_ids = {int(row["id"]) for row in valid_item_rows}
    invalid_item_ids = [item_id for item_id in requested_item_ids if item_id not in valid_item_ids]
    if invalid_item_ids:
        return (
            jsonify(
                {
                    "error": "rfq_items_not_found", "message": _err("rfq_items_not_found"),
                    "rfq_item_ids": invalid_item_ids,
                }
            ),
            400,
        )

    invited_rows = db.execute(
        f"""
        SELECT rfq_item_id
        FROM rfq_item_suppliers
        WHERE tenant_id = ? AND supplier_id = ? AND rfq_item_id IN ({placeholders})
        """,
        (tenant_id, supplier_id, *requested_item_ids),
    ).fetchall()
    invited_item_ids = {int(row["rfq_item_id"]) for row in invited_rows}
    uninvited_item_ids = [item_id for item_id in requested_item_ids if item_id not in invited_item_ids]
    if uninvited_item_ids:
        return (
            jsonify(
                {
                    "error": "supplier_not_invited_for_items", "message": _err("supplier_not_invited_for_items"),
                    "rfq_item_ids": uninvited_item_ids,
                    "supplier_id": supplier_id,
                }
            ),
            400,
        )

    quote_id = _get_or_create_quote(db, rfq_id, supplier_id, tenant_id)

    for item in normalized_items:
        _upsert_quote_item(
            db,
            quote_id=int(quote_id),
            rfq_item_id=int(item["rfq_item_id"]),
            unit_price=float(item["unit_price"]),
            lead_time_days=item["lead_time_days"],
            tenant_id=tenant_id,
        )

    db.commit()
    itens = _load_rfq_items_with_quotes(db, tenant_id, rfq_id)
    return jsonify({"itens": itens, "quote_id": quote_id, "saved_items": len(normalized_items)})


@procurement_bp.route(
    "/api/procurement/cotacoes/<int:rfq_id>/propostas/<int:supplier_id>",
    methods=["GET", "DELETE"],
)
def cotacao_supplier_proposta_api(rfq_id: int, supplier_id: int):
    db = get_db() if request.method == "DELETE" else get_read_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID

    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": _err("rfq_not_found"), "rfq_id": rfq_id}), 404

    supplier = db.execute(
        "SELECT id, name FROM suppliers WHERE id = ? AND tenant_id = ? LIMIT 1",
        (supplier_id, tenant_id),
    ).fetchone()
    if not supplier:
        return jsonify({"error": "supplier_not_found", "message": _err("supplier_not_found")}), 404

    quote = db.execute(
        """
        SELECT id
        FROM quotes
        WHERE rfq_id = ? AND supplier_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (rfq_id, supplier_id, tenant_id),
    ).fetchone()

    if request.method == "GET":
        quote_id = int(quote["id"]) if quote else None
        rows = db.execute(
            """
            SELECT
                ri.id AS rfq_item_id,
                ri.description,
                ri.quantity,
                ri.uom,
                qi.unit_price,
                qi.lead_time_days
            FROM rfq_items ri
            JOIN rfq_item_suppliers ris
              ON ris.rfq_item_id = ri.id
             AND ris.supplier_id = ?
             AND ris.tenant_id = ri.tenant_id
            LEFT JOIN quote_items qi
              ON qi.rfq_item_id = ri.id
             AND qi.quote_id = ?
             AND qi.tenant_id = ri.tenant_id
            WHERE ri.rfq_id = ? AND ri.tenant_id = ?
            ORDER BY ri.id
            """,
            (supplier_id, quote_id, rfq_id, tenant_id),
        ).fetchall()
        return jsonify(
            {
                "rfq_id": rfq_id,
                "supplier": {"id": supplier_id, "name": supplier["name"]},
                "quote_id": quote_id,
                "items": [
                    {
                        "rfq_item_id": int(row["rfq_item_id"]),
                        "description": row["description"],
                        "quantity": row["quantity"],
                        "uom": row["uom"],
                        "unit_price": row["unit_price"],
                        "lead_time_days": row["lead_time_days"],
                    }
                    for row in rows
                ],
            }
        )

    if not quote:
        return jsonify({"error": "quote_not_found", "message": _err("quote_not_found")}), 404

    _require_critical_confirmation(
        "delete_supplier_proposal",
        entity="quote",
        entity_id=int(quote["id"]),
    )

    quote_id = int(quote["id"])
    db.execute("DELETE FROM quote_items WHERE quote_id = ? AND tenant_id = ?", (quote_id, tenant_id))
    db.execute("DELETE FROM quotes WHERE id = ? AND tenant_id = ?", (quote_id, tenant_id))
    db.execute(
        """
        UPDATE rfq_supplier_invites
        SET status = CASE WHEN status = 'submitted' THEN 'opened' ELSE status END,
            submitted_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE rfq_id = ? AND supplier_id = ? AND tenant_id = ?
        """,
        (rfq_id, supplier_id, tenant_id),
    )
    db.commit()
    return jsonify({"deleted": True, "rfq_id": rfq_id, "supplier_id": supplier_id}), 200


@procurement_bp.route("/api/fornecedor/convite/<string:token>", methods=["GET"])
def supplier_invite_detail_api(token: str):
    db = get_db()
    invite = _load_supplier_invite_by_token(db, token)
    if not invite:
        return jsonify({"error": "invite_not_found", "message": _err("invite_not_found")}), 404

    if _invite_is_expired(invite):
        db.execute(
            "UPDATE rfq_supplier_invites SET status = 'expired', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (invite["id"],),
        )
        db.commit()
        return jsonify({"error": "invite_expired", "message": _err("invite_expired")}), 410

    if invite["status"] == "pending":
        db.execute(
            """
            UPDATE rfq_supplier_invites
            SET status = 'opened', opened_at = COALESCE(opened_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (invite["id"],),
        )
        db.commit()

    payload = _build_supplier_invite_payload(db, invite)
    return jsonify(payload)


@procurement_bp.route("/api/fornecedor/convite/<string:token>/propostas", methods=["POST"])
def supplier_invite_submit_api(token: str):
    db = get_db()
    invite = _load_supplier_invite_by_token(db, token)
    if not invite:
        return jsonify({"error": "invite_not_found", "message": _err("invite_not_found")}), 404
    if _invite_is_expired(invite):
        db.execute(
            "UPDATE rfq_supplier_invites SET status = 'expired', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (invite["id"],),
        )
        db.commit()
        return jsonify({"error": "invite_expired", "message": _err("invite_expired")}), 410

    payload = request.get_json(silent=True) or {}
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        return jsonify({"error": "items_required", "message": _err("items_required")}), 400

    rfq_id = int(invite["rfq_id"])
    supplier_id = int(invite["supplier_id"])
    tenant_id = str(invite["tenant_id"])
    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": _err("rfq_not_found")}), 404
    if not flow_action_allowed("fornecedor", invite["status"], "submit_quote"):
        return _forbidden_action("fornecedor", invite["status"], "submit_quote")
    if not flow_action_allowed("cotacao", rfq["status"], "save_supplier_quote"):
        return _forbidden_action("cotacao", rfq["status"], "save_supplier_quote")
    if rfq["status"] not in {"open", "collecting_quotes"}:
        return (
            jsonify(
                {
                    "error": "rfq_closed_for_quotes", "message": _err("rfq_closed_for_quotes"),
                }
            ),
            400,
        )

    rfq_item_rows = db.execute(
        """
        SELECT id
        FROM rfq_items
        WHERE rfq_id = ? AND tenant_id = ?
        ORDER BY id
        """,
        (rfq_id, tenant_id),
    ).fetchall()
    all_rfq_item_ids = {int(row["id"]) for row in rfq_item_rows}
    if not all_rfq_item_ids:
        return jsonify({"error": "rfq_items_not_found", "message": _err("rfq_items_not_found")}), 400

    invited_rows = db.execute(
        """
        SELECT rfq_item_id
        FROM rfq_item_suppliers
        WHERE rfq_item_id IN ({placeholders}) AND supplier_id = ? AND tenant_id = ?
        """.format(placeholders=",".join("?" for _ in all_rfq_item_ids)),
        (*sorted(all_rfq_item_ids), supplier_id, tenant_id),
    ).fetchall()
    invited_item_ids = {int(row["rfq_item_id"]) for row in invited_rows}
    if not invited_item_ids:
        return (
            jsonify(
                {
                    "error": "supplier_not_invited", "message": _err("supplier_not_invited"),
                }
            ),
            400,
        )

    normalized_by_id: Dict[int, dict] = {}
    for item in items:
        rfq_item_id = _parse_optional_int(item.get("rfq_item_id"))
        unit_price = _parse_optional_float(item.get("unit_price"))
        lead_time_days = _parse_optional_int(item.get("lead_time_days"))

        if rfq_item_id is None or unit_price is None:
            continue
        if rfq_item_id not in invited_item_ids:
            continue
        if unit_price < 0:
            continue
        if lead_time_days is not None and lead_time_days < 0:
            continue

        normalized_by_id[rfq_item_id] = {
            "rfq_item_id": rfq_item_id,
            "unit_price": unit_price,
            "lead_time_days": lead_time_days,
        }

    normalized_items = list(normalized_by_id.values())
    if not normalized_items:
        return jsonify({"error": "valid_items_required", "message": _err("valid_items_required")}), 400

    quote_id = _get_or_create_quote(db, rfq_id, supplier_id, tenant_id)
    for item in normalized_items:
        _upsert_quote_item(
            db=db,
            quote_id=int(quote_id),
            rfq_item_id=int(item["rfq_item_id"]),
            unit_price=float(item["unit_price"]),
            lead_time_days=item["lead_time_days"],
            tenant_id=tenant_id,
        )

    db.execute(
        """
        UPDATE rfq_supplier_invites
        SET status = 'submitted',
            submitted_at = CURRENT_TIMESTAMP,
            opened_at = COALESCE(opened_at, CURRENT_TIMESTAMP),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (invite["id"],),
    )
    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
        VALUES ('rfq', ?, ?, ?, 'supplier_quote_received', ?)
        """,
        (rfq_id, rfq["status"], "collecting_quotes", tenant_id),
    )
    db.execute(
        "UPDATE rfqs SET status = 'collecting_quotes', updated_at = CURRENT_TIMESTAMP WHERE id = ? AND tenant_id = ?",
        (rfq_id, tenant_id),
    )
    db.commit()

    return jsonify({"status": "submitted", "quote_id": quote_id, "saved_items": len(normalized_items)})


def _upsert_quote_item(
    db: Database,
    quote_id: int,
    rfq_item_id: int,
    unit_price: float,
    lead_time_days: int | None,
    tenant_id: str,
) -> None:
    if db.backend == "postgres":
        db.execute(
            """
            INSERT INTO quote_items (quote_id, rfq_item_id, unit_price, lead_time_days, tenant_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (quote_id, rfq_item_id, tenant_id) DO UPDATE SET
                unit_price = excluded.unit_price,
                lead_time_days = excluded.lead_time_days,
                updated_at = CURRENT_TIMESTAMP
            """,
            (quote_id, rfq_item_id, unit_price, lead_time_days, tenant_id),
        )
        return

    cursor = db.execute(
        """
        UPDATE quote_items
        SET unit_price = ?, lead_time_days = ?, updated_at = CURRENT_TIMESTAMP
        WHERE quote_id = ? AND rfq_item_id = ? AND tenant_id = ?
        """,
        (unit_price, lead_time_days, quote_id, rfq_item_id, tenant_id),
    )
    if cursor.rowcount and cursor.rowcount > 0:
        return
    db.execute(
        """
        INSERT INTO quote_items (quote_id, rfq_item_id, unit_price, lead_time_days, tenant_id)
        VALUES (?, ?, ?, ?, ?)
        """,
        (quote_id, rfq_item_id, unit_price, lead_time_days, tenant_id),
    )


@procurement_bp.route("/api/procurement/purchase-orders", methods=["GET", "POST"])
def purchase_orders_api():
    if request.method == "POST":
        db = get_db()
        tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
        payload = request.get_json(silent=True) or {}

        number = (payload.get("number") or "").strip() or None
        supplier_name = (payload.get("supplier_name") or payload.get("fornecedor") or "").strip() or None
        if not supplier_name:
            return jsonify({"error": "supplier_name_required", "message": _err("supplier_name_required")}), 400

        status = str(payload.get("status") or "draft").strip()
        if status not in ALLOWED_PO_STATUSES:
            return jsonify({"error": "status_invalid", "message": _err("status_invalid")}), 400

        currency = (payload.get("currency") or "BRL").strip() or "BRL"
        total_amount = _parse_optional_float(payload.get("total_amount"))
        if total_amount is None:
            total_amount = 0.0

        cursor = db.execute(
            """
            INSERT INTO purchase_orders (
                number, supplier_name, status, currency, total_amount, tenant_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id
            """,
            (number, supplier_name, status, currency, total_amount, tenant_id),
        )
        created_row = cursor.fetchone()
        purchase_order_id = int(created_row["id"] if isinstance(created_row, dict) else created_row[0])

        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('purchase_order', ?, NULL, ?, 'purchase_order_created', ?)
            """,
            (purchase_order_id, status, tenant_id),
        )
        db.commit()
        return jsonify({"id": purchase_order_id, "status": status}), 201

    db = get_read_db()
    tenant_id = current_tenant_id()
    limit = _parse_int(request.args.get("limit"), default=100, min_value=1, max_value=300)
    status_values = _parse_csv_values(request.args.get("status"))
    supplier_search = (request.args.get("supplier") or "").strip()
    erp_only = request.args.get("erp_only") == "1"
    items = _load_purchase_orders_panel(
        db,
        tenant_id=tenant_id,
        limit=limit,
        status_values=status_values,
        supplier_search=supplier_search,
        erp_only=erp_only,
    )
    return jsonify({"items": items})


@procurement_bp.route("/api/procurement/purchase-orders/<int:purchase_order_id>", methods=["GET", "PATCH", "DELETE"])
def purchase_order_detail_api(purchase_order_id: int):
    db = get_db() if request.method in {"PATCH", "DELETE"} else get_read_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID

    po = _load_purchase_order(db, tenant_id, purchase_order_id)
    if not po:
        return (
            jsonify(
                {
                    "error": "purchase_order_not_found", "message": _err("purchase_order_not_found"),
                    "purchase_order_id": purchase_order_id,
                }
            ),
            404,
        )

    if request.method == "DELETE":
        if not flow_action_allowed("ordem_compra", po["status"], "cancel_order"):
            return _forbidden_action("ordem_compra", po["status"], "cancel_order")
        if po["external_id"]:
            return (
                jsonify(
                    {
                        "error": "erp_managed_purchase_order_readonly", "message": _err("erp_managed_purchase_order_readonly"),
                    }
                ),
                409,
            )
        _require_critical_confirmation(
            "cancel_order",
            entity="purchase_order",
            entity_id=purchase_order_id,
        )
        previous_status = po["status"]
        if previous_status != "cancelled":
            db.execute(
                """
                UPDATE purchase_orders
                SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND tenant_id = ?
                """,
                (purchase_order_id, tenant_id),
            )
            db.execute(
                """
                INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
                VALUES ('purchase_order', ?, ?, 'cancelled', 'purchase_order_cancelled', ?)
                """,
                (purchase_order_id, previous_status, tenant_id),
            )
            db.commit()
        return jsonify({"purchase_order_id": purchase_order_id, "status": "cancelled"}), 200

    if request.method == "PATCH":
        if not flow_action_allowed("ordem_compra", po["status"], "edit_order"):
            return _forbidden_action("ordem_compra", po["status"], "edit_order")
        if po["external_id"]:
            return (
                jsonify(
                    {
                        "error": "erp_managed_purchase_order_readonly", "message": _err("erp_managed_purchase_order_readonly"),
                    }
                ),
                409,
            )
        payload = request.get_json(silent=True) or {}
        updates: List[str] = []
        params: List[object] = []

        if "number" in payload:
            updates.append("number = ?")
            params.append((payload.get("number") or "").strip() or None)

        if "supplier_name" in payload or "fornecedor" in payload:
            supplier_name = (payload.get("supplier_name") or payload.get("fornecedor") or "").strip() or None
            updates.append("supplier_name = ?")
            params.append(supplier_name)

        if "currency" in payload:
            currency = (payload.get("currency") or "BRL").strip() or "BRL"
            updates.append("currency = ?")
            params.append(currency)

        if "total_amount" in payload:
            total_amount = _parse_optional_float(payload.get("total_amount"))
            if total_amount is None:
                return jsonify({"error": "total_amount_invalid", "message": _err("total_amount_invalid")}), 400
            updates.append("total_amount = ?")
            params.append(total_amount)

        next_status = po["status"]
        if "status" in payload:
            status = str(payload.get("status") or "").strip()
            if status not in ALLOWED_PO_STATUSES:
                return jsonify({"error": "status_invalid", "message": _err("status_invalid")}), 400
            if status in {"sent_to_erp", "erp_accepted", "erp_error", "partially_received", "received"} and status != po["status"]:
                return _forbidden_action("ordem_compra", po["status"], "push_to_erp", http_status=400)
            required_action = "cancel_order" if status == "cancelled" else "edit_order"
            if not flow_action_allowed("ordem_compra", po["status"], required_action):
                return _forbidden_action("ordem_compra", po["status"], required_action)
            if status == "cancelled" and status != po["status"]:
                _require_critical_confirmation(
                    "cancel_order",
                    entity="purchase_order",
                    entity_id=purchase_order_id,
                    payload=payload,
                )
            next_status = status
            updates.append("status = ?")
            params.append(status)

        if not updates:
            return jsonify({"error": "no_changes", "message": _err("no_changes")}), 400

        params.extend([purchase_order_id, tenant_id])
        db.execute(
            f"""
            UPDATE purchase_orders
            SET {", ".join(updates)}, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            tuple(params),
        )

        if next_status != po["status"]:
            db.execute(
                """
                INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
                VALUES ('purchase_order', ?, ?, ?, 'purchase_order_updated', ?)
                """,
                (purchase_order_id, po["status"], next_status, tenant_id),
            )

        db.commit()
        return jsonify({"purchase_order_id": purchase_order_id, "status": next_status}), 200

    include_history_raw = (request.args.get("include_history") or "").strip().lower()
    include_history = include_history_raw not in {"0", "false", "no", "off"}

    events: List[dict] = []
    erp_timeline: List[dict] = []
    sync_runs: List[dict] = []
    if include_history:
        events = _load_status_events(db, tenant_id, entity="purchase_order", entity_id=purchase_order_id)
        erp_timeline = _build_erp_timeline(events)
        sync_runs = _load_sync_runs(db, tenant_id, scope="purchase_order", limit=20)
    po_flow = flow_meta("ordem_compra", po["status"])
    process_stage = stage_for_purchase_order_status(po["status"])
    last_erp_update = (erp_timeline[0]["occurred_at"] if erp_timeline else None) or po["updated_at"]
    erp_status = erp_status_payload(
        po["status"],
        erp_last_error=po["erp_last_error"],
        last_updated_at=last_erp_update,
    )
    next_action_key = _erp_next_action_key(
        po["status"],
        list(po_flow.get("allowed_actions") or []),
        po_flow.get("primary_action"),
        str(erp_status.get("key") or ""),
    )
    next_action_label = _erp_action_label(next_action_key, str(erp_status.get("key") or ""))

    return jsonify(
        {
            "purchase_order": {
                "id": po["id"],
                "number": po["number"],
                "award_id": po["award_id"],
                "supplier_name": po["supplier_name"],
                "status": po["status"],
                "currency": po["currency"],
                "total_amount": po["total_amount"],
                "external_id": po["external_id"],
                "erp_last_error": po["erp_last_error"],
                "created_at": po["created_at"],
                "updated_at": po["updated_at"],
                "allowed_actions": po_flow["allowed_actions"],
                "primary_action": po_flow["primary_action"],
                "process_stage": process_stage,
                "erp_status": erp_status,
                "erp_last_attempt_at": last_erp_update,
                "erp_next_action": next_action_key,
                "erp_next_action_label": next_action_label,
            },
            "erp_timeline": erp_timeline,
            "events": events,
            "sync_runs": sync_runs,
            "history_loaded": include_history,
            "flow": {
                "process_stage": process_stage,
                "process_steps": build_process_steps(process_stage),
            },
        }
    )


@procurement_bp.route("/api/procurement/integrations/logs", methods=["GET"])
def integration_logs_api():
    db = get_read_db()
    tenant_id = current_tenant_id()

    scope = (request.args.get("scope") or "").strip() or None
    limit = _parse_int(request.args.get("limit"), default=50, min_value=1, max_value=200)

    sync_runs = _load_sync_runs(db, tenant_id, scope=scope, limit=limit)
    recent_events = _load_recent_status_events(db, tenant_id, limit=80)

    return jsonify(
        {
            "sync_runs": sync_runs,
            "status_events": recent_events,
            "filters": {"scope": scope, "limit": limit},
        }
    )


@procurement_bp.route("/api/procurement/integrations/erp/orders", methods=["GET"])
def erp_followup_api():
    _require_roles("admin", "manager")
    db = get_read_db()
    tenant_id = current_tenant_id()

    limit = _parse_int(request.args.get("limit"), default=120, min_value=1, max_value=300)
    status_filter_values = {
        str(value).strip()
        for value in _parse_csv_values(request.args.get("erp_status"))
        if str(value).strip()
    }

    items = _load_purchase_orders_panel(
        db,
        tenant_id=tenant_id,
        limit=limit,
        status_values=[],
        supplier_search=(request.args.get("supplier") or "").strip(),
        erp_only=False,
    )

    rows: List[dict] = []
    for item in items:
        erp_status = item.get("erp_status") or erp_status_payload(
            item.get("status"),
            erp_last_error=item.get("erp_last_error"),
            last_updated_at=item.get("erp_last_attempt_at") or item.get("updated_at"),
        )
        erp_key = str(erp_status.get("key") or "")
        if status_filter_values and erp_key not in status_filter_values:
            continue

        allowed_actions = list(item.get("allowed_actions") or [])
        next_action_key = _erp_next_action_key(
            item.get("status"),
            allowed_actions,
            item.get("primary_action"),
            erp_key,
        )
        can_resend = "push_to_erp" in set(allowed_actions) and erp_key in {"rejeitado", "reenvio_necessario"}
        next_action_label = _erp_action_label(next_action_key, erp_key)
        if next_action_key == "refresh_order":
            next_action_label = get_ui_text("erp.next_action.await_response", "Aguardar retorno do ERP.")
        elif can_resend:
            next_action_label = get_ui_text("erp.next_action.resend", "Reenviar ao ERP")
        elif erp_key == "rejeitado":
            next_action_label = get_ui_text("erp.next_action.review_data", "Revisar dados da ordem e reenviar ao ERP.")

        rows.append(
            {
                "purchase_order_id": item.get("id"),
                "number": item.get("number"),
                "supplier_name": item.get("supplier_name"),
                "technical_status": item.get("status"),
                "erp_status": erp_status,
                "last_attempt_at": item.get("erp_last_attempt_at") or erp_status.get("last_updated_at") or item.get("updated_at"),
                "next_action": next_action_key,
                "next_action_label": next_action_label,
                "can_resend": can_resend,
                "allowed_actions": allowed_actions,
                "detail_url": f"/procurement/purchase-orders/{item.get('id')}",
            }
        )

    rows.sort(
        key=lambda row: (
            str(row.get("last_attempt_at") or ""),
            int(row.get("purchase_order_id") or 0),
        ),
        reverse=True,
    )

    return jsonify(
        {
            "items": rows,
            "filters": {
                "limit": limit,
                "erp_status": sorted(status_filter_values),
            },
        }
    )


@procurement_bp.route("/api/procurement/integrations/sync", methods=["POST"])
def integration_sync_api():
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}

    scope_value = (payload.get("scope") or request.args.get("scope") or "").strip()
    if not scope_value:
        return jsonify({"error": "scope_required", "message": _err("scope_required")}), 400

    canonical_scope = SCOPE_ALIASES.get(scope_value, (scope_value,))[0]
    if canonical_scope not in SYNC_SUPPORTED_SCOPES:
        return (
            jsonify(
                {
                    "error": "scope_not_supported", "message": _err("scope_not_supported"),
                    "scope": canonical_scope,
                }
            ),
            400,
        )

    limit_value = payload.get("limit")
    if limit_value is None:
        limit_value = request.args.get("limit")
    limit = _parse_int(
        str(limit_value) if limit_value is not None else None,
        default=100,
        min_value=1,
        max_value=500,
    )

    sync_run_id = _start_sync_run(db, tenant_id, scope=canonical_scope)

    try:
        result = _sync_from_erp(db, tenant_id, canonical_scope, limit=limit)
        _finish_sync_run(
            db,
            tenant_id,
            sync_run_id,
            status="succeeded",
            records_in=result["records_in"],
            records_upserted=result["records_upserted"],
        )
        db.commit()
    except Exception as exc:  # noqa: BLE001 - MVP: loga erro resumido
        _finish_sync_run(db, tenant_id, sync_run_id, status="failed", records_in=0, records_upserted=0)
        db.execute(
            """
            UPDATE sync_runs
            SET error_summary = ?
            WHERE id = ? AND tenant_id = ?
            """,
            (str(exc)[:200], sync_run_id, tenant_id),
        )
        db.commit()
        raise IntegrationError(
            code="sync_failed",
            message_key="sync_failed",
            http_status=500,
            critical=False,
            details=str(exc),
            payload={"scope": canonical_scope},
        )

    return jsonify(
        {
            "status": "succeeded",
            "scope": canonical_scope,
            "sync_run_id": sync_run_id,
            "result": result,
        }
    )


@procurement_bp.route("/api/procurement/seed", methods=["POST", "GET"])
def procurement_seed():
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID

    db.execute(
        "INSERT INTO tenants (id, name, subdomain) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
        (tenant_id, f"Tenant {tenant_id}", tenant_id),
    )

    existing = db.execute(
        "SELECT COUNT(*) AS total FROM purchase_requests WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchone()["total"]
    if existing:
        open_items = db.execute(
            """
            SELECT COUNT(*) AS total
            FROM purchase_request_items pri
            JOIN purchase_requests pr
              ON pr.id = pri.purchase_request_id AND pr.tenant_id = pri.tenant_id
            WHERE pr.status = 'pending_rfq' AND pr.tenant_id = ?
            """,
            (tenant_id,),
        ).fetchone()["total"]
        if not open_items:
            _seed_demo_items_for_pending_requests(db, tenant_id)
        _ensure_demo_suppliers(db, tenant_id)
        db.commit()
        cards = _load_inbox_cards(db, tenant_id)
        return jsonify(
            {
                "seeded": open_items == 0,
                "tenant_id": tenant_id,
                "kpis": cards,
                "hint": "Seed ja aplicado. Use GET /api/procurement/inbox",
            }
        )

    needed_at_pr1 = (datetime.now(timezone.utc) + timedelta(days=3)).date().isoformat()
    needed_at_pr2 = (datetime.now(timezone.utc) + timedelta(days=1)).date().isoformat()

    cursor = db.execute(
        """
        INSERT INTO purchase_requests (number, status, priority, requested_by, department, needed_at, tenant_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        RETURNING id
        """,
        ("SR-1001", "pending_rfq", "high", "Joao", "Manutencao", needed_at_pr1, tenant_id),
    )
    pr1_row = cursor.fetchone()
    pr1_id = pr1_row["id"] if isinstance(pr1_row, dict) else pr1_row[0]

    cursor = db.execute(
        """
        INSERT INTO purchase_requests (number, status, priority, requested_by, department, needed_at, tenant_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        RETURNING id
        """,
        ("SR-1002", "in_rfq", "urgent", "Maria", "Operacoes", needed_at_pr2, tenant_id),
    )
    pr2_row = cursor.fetchone()
    pr2_id = pr2_row["id"] if isinstance(pr2_row, dict) else pr2_row[0]

    if pr1_id:
        db.execute(
            """
            INSERT INTO purchase_request_items (purchase_request_id, line_no, description, quantity, uom, tenant_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (pr1_id, 1, "Rolamento 6202", 10, "UN", tenant_id),
        )
        db.execute(
            """
            INSERT INTO purchase_request_items (purchase_request_id, line_no, description, quantity, uom, tenant_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (pr1_id, 2, "Correia dentada", 4, "UN", tenant_id),
        )

    if pr2_id:
        db.execute(
            """
            INSERT INTO purchase_request_items (purchase_request_id, line_no, description, quantity, uom, tenant_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (pr2_id, 1, "Luva nitrilica", 200, "UN", tenant_id),
        )
        db.execute(
            """
            INSERT INTO purchase_request_items (purchase_request_id, line_no, description, quantity, uom, tenant_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (pr2_id, 2, "Mascara PFF2", 150, "UN", tenant_id),
        )

    db.execute(
        "INSERT INTO rfqs (title, status, tenant_id) VALUES (?, ?, ?)",
        ("Cotacao - Rolamentos", "collecting_quotes", tenant_id),
    )
    db.execute(
        "INSERT INTO rfqs (title, status, tenant_id) VALUES (?, ?, ?)",
        ("Cotacao - EPIs", "awarded", tenant_id),
    )

    db.execute(
        """
        INSERT INTO purchase_orders (number, status, tenant_id, supplier_name, total_amount)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("OC-2001", "approved", tenant_id, "Fornecedor A", 12450.90),
    )
    db.execute(
        """
        INSERT INTO purchase_orders (number, status, tenant_id, supplier_name, erp_last_error, total_amount)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("OC-2002", "erp_error", tenant_id, "Fornecedor B", "Fornecedor sem codigo no ERP", 9870.00),
    )

    _ensure_demo_suppliers(db, tenant_id)
    db.commit()

    cards = _load_inbox_cards(db, tenant_id)
    return jsonify(
        {
            "seeded": True,
            "tenant_id": tenant_id,
            "kpis": cards,
            "hint": "Agora use GET /api/procurement/inbox",
        }
    )


def _seed_demo_items_for_pending_requests(db, tenant_id: str, limit: int = 2) -> None:
    rows = db.execute(
        """
        SELECT id, number
        FROM purchase_requests
        WHERE tenant_id = ? AND status = 'pending_rfq'
        ORDER BY id DESC
        LIMIT ?
        """,
        (tenant_id, limit),
    ).fetchall()
    if not rows:
        return

    for row in rows:
        pr_id = row["id"]
        has_items = db.execute(
            "SELECT 1 FROM purchase_request_items WHERE tenant_id = ? AND purchase_request_id = ? LIMIT 1",
            (tenant_id, pr_id),
        ).fetchone()
        if has_items:
            continue

        db.execute(
            """
            INSERT INTO purchase_request_items (purchase_request_id, line_no, description, quantity, uom, tenant_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (pr_id, 1, f"Item demo 1 - {row['number']}", 10, "UN", tenant_id),
        )
        db.execute(
            """
            INSERT INTO purchase_request_items (purchase_request_id, line_no, description, quantity, uom, tenant_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (pr_id, 2, f"Item demo 2 - {row['number']}", 4, "UN", tenant_id),
        )


@procurement_bp.route("/api/procurement/rfqs", methods=["GET"])
def list_rfqs():
    db = get_read_db()
    tenant_id = current_tenant_id()
    clause, params = _tenant_clause(tenant_id, alias="r")

    filters: List[str] = []
    status_values = _parse_csv_values(request.args.get("status"))
    normalized_statuses = [status for status in status_values if status in ALLOWED_RFQ_STATUSES]
    if normalized_statuses:
        filters.append(f"r.status IN ({','.join('?' for _ in normalized_statuses)})")
        params.extend(normalized_statuses)

    where_sql = clause
    if filters:
        where_sql = f"{clause} AND {' AND '.join(filters)}"

    rows = db.execute(
        f"""
        SELECT
            r.id,
            r.title,
            r.status,
            r.updated_at,
            COUNT(DISTINCT ri.id) AS item_count,
            COUNT(DISTINCT ris.supplier_id) AS supplier_count
        FROM rfqs r
        LEFT JOIN rfq_items ri
          ON ri.rfq_id = r.id AND ri.tenant_id = r.tenant_id
        LEFT JOIN rfq_item_suppliers ris
          ON ris.rfq_item_id = ri.id AND ris.tenant_id = r.tenant_id
        WHERE {where_sql}
        GROUP BY r.id, r.title, r.status, r.updated_at
        ORDER BY r.updated_at DESC, r.id DESC
        LIMIT 150
        """,
        tuple(params),
    ).fetchall()

    items = []
    for row in rows:
        status = row["status"]
        stage = stage_for_rfq_status(status)
        meta = flow_meta("cotacao", status)
        items.append(
            {
                "id": row["id"],
                "title": row["title"],
                "status": status,
                "updated_at": row["updated_at"],
                "item_count": int(row["item_count"] or 0),
                "supplier_count": int(row["supplier_count"] or 0),
                "process_stage": stage,
                "allowed_actions": meta["allowed_actions"],
                "primary_action": meta["primary_action"],
            }
        )
    return jsonify({"items": items})


@procurement_bp.route("/api/procurement/rfqs", methods=["POST"])
def create_rfq():
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}
    result = _PROCUREMENT_SERVICE.create_rfq(
        db,
        tenant_id=tenant_id,
        create_input=RfqCreateInput(
            title=(payload.get("title") or "Nova Cotacao").strip(),
            purchase_request_item_ids=_normalize_int_list(payload.get("purchase_request_item_ids")),
        ),
        create_rfq_core_fn=_create_rfq_core,
    )
    db.commit()
    return jsonify(result.payload), result.status_code


@procurement_bp.route("/api/procurement/rfqs/<int:rfq_id>", methods=["PATCH", "DELETE"])
def rfq_crud_api(rfq_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": _err("rfq_not_found"), "rfq_id": rfq_id}), 404

    previous_status = rfq["status"]
    if request.method == "DELETE":
        if not flow_action_allowed("cotacao", previous_status, "cancel_rfq"):
            return _forbidden_action("cotacao", previous_status, "cancel_rfq")
        _require_critical_confirmation(
            "cancel_rfq",
            entity="rfq",
            entity_id=rfq_id,
        )
        if previous_status == "cancelled":
            return jsonify({"rfq_id": rfq_id, "status": "cancelled"}), 200
        db.execute(
            """
            UPDATE rfqs
            SET status = 'cancelled', cancel_reason = COALESCE(cancel_reason, 'rfq_cancelled'), updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (rfq_id, tenant_id),
        )
        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('rfq', ?, ?, 'cancelled', 'rfq_cancelled', ?)
            """,
            (rfq_id, previous_status, tenant_id),
        )
        db.commit()
        return jsonify({"rfq_id": rfq_id, "status": "cancelled"}), 200

    payload = request.get_json(silent=True) or {}
    updates: List[str] = []
    params: List[object] = []
    next_status = previous_status

    if "title" in payload:
        if not flow_action_allowed("cotacao", previous_status, "edit_rfq"):
            return _forbidden_action("cotacao", previous_status, "edit_rfq")
        updates.append("title = ?")
        params.append((payload.get("title") or "").strip() or None)

    if "status" in payload:
        status = str(payload.get("status") or "").strip()
        if status not in ALLOWED_RFQ_STATUSES:
            return jsonify({"error": "status_invalid", "message": _err("status_invalid")}), 400
        required_action = "cancel_rfq" if status == "cancelled" else "update_rfq_status"
        if status == "awarded":
            required_action = "award_rfq"
        if not flow_action_allowed("cotacao", previous_status, required_action):
            return _forbidden_action("cotacao", previous_status, required_action)
        if status == "awarded":
            return _forbidden_action("cotacao", previous_status, "award_rfq", http_status=400)
        if status == "cancelled" and status != previous_status:
            _require_critical_confirmation(
                "cancel_rfq",
                entity="rfq",
                entity_id=rfq_id,
                payload=payload,
            )
        next_status = status
        updates.append("status = ?")
        params.append(status)
        if status == "cancelled":
            updates.append("cancel_reason = ?")
            params.append((payload.get("cancel_reason") or "rfq_cancelled").strip() or "rfq_cancelled")

    if "cancel_reason" in payload and "status" not in payload:
        updates.append("cancel_reason = ?")
        params.append((payload.get("cancel_reason") or "").strip() or None)

    if not updates:
        return jsonify({"error": "no_changes", "message": _err("no_changes")}), 400

    params.extend([rfq_id, tenant_id])
    db.execute(
        f"""
        UPDATE rfqs
        SET {", ".join(updates)}, updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND tenant_id = ?
        """,
        tuple(params),
    )

    if next_status != previous_status:
        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('rfq', ?, ?, ?, 'rfq_updated', ?)
            """,
            (rfq_id, previous_status, next_status, tenant_id),
        )

    db.commit()
    return jsonify({"rfq_id": rfq_id, "status": next_status}), 200


@procurement_bp.route("/api/procurement/cotacoes/abertura", methods=["POST"])
def open_rfq_with_suppliers():
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}

    title = (payload.get("title") or "Cotacao aberta").strip()
    item_ids = _normalize_int_list(payload.get("purchase_request_item_ids"))
    supplier_ids = _normalize_int_list(payload.get("supplier_ids"))
    if not supplier_ids:
        return (
            jsonify(
                {
                    "error": "supplier_ids_required", "message": _err("supplier_ids_required"),
                }
            ),
            400,
        )
    valid_supplier_ids = {int(item["id"]) for item in _load_suppliers(db, tenant_id)}
    if not any(supplier_id in valid_supplier_ids for supplier_id in supplier_ids):
        return jsonify({"error": "suppliers_not_found", "message": _err("suppliers_not_found")}), 400

    created, error_payload, status_code = _create_rfq_core(db, tenant_id, title, item_ids)
    if error_payload:
        return jsonify(error_payload), status_code

    invite_result = _create_rfq_supplier_invites(
        db=db,
        tenant_id=tenant_id,
        rfq_id=int(created["id"]),
        rfq_items=created["rfq_items"],
        supplier_ids=supplier_ids,
        valid_days=_parse_int(payload.get("invite_valid_days"), default=7, min_value=1, max_value=30),
    )
    db.commit()
    return jsonify({"rfq": created, "invites": invite_result}), 201


@procurement_bp.route("/api/procurement/cotacoes/<int:rfq_id>/convites", methods=["POST"])
def create_rfq_invites(rfq_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}

    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return (
            jsonify({"error": "rfq_not_found", "message": _err("rfq_not_found"), "rfq_id": rfq_id}),
            404,
        )
    if not flow_action_allowed("cotacao", rfq["status"], "invite_supplier"):
        return _forbidden_action("cotacao", rfq["status"], "invite_supplier")

    supplier_ids = _normalize_int_list(payload.get("supplier_ids"))
    if not supplier_ids:
        return (
            jsonify({"error": "supplier_ids_required", "message": _err("supplier_ids_required")}),
            400,
        )

    requested_item_ids = _normalize_int_list(payload.get("rfq_item_ids"))
    all_items = _load_rfq_items(db, tenant_id, rfq_id)
    if requested_item_ids:
        items = [item for item in all_items if int(item["id"]) in set(requested_item_ids)]
    else:
        items = all_items
    if not items:
        return jsonify({"error": "rfq_items_required", "message": _err("rfq_items_required")}), 400

    invite_result = _create_rfq_supplier_invites(
        db=db,
        tenant_id=tenant_id,
        rfq_id=rfq_id,
        rfq_items=[
            {
                "rfq_item_id": int(item["id"]),
                "purchase_request_item_id": item.get("purchase_request_item_id"),
                "description": item.get("description"),
                "quantity": item.get("quantity"),
                "uom": item.get("uom"),
            }
            for item in items
        ],
        supplier_ids=supplier_ids,
        valid_days=_parse_int(payload.get("invite_valid_days"), default=7, min_value=1, max_value=30),
    )
    if invite_result["supplier_count"] == 0:
        return jsonify({"error": "suppliers_not_found", "message": _err("suppliers_not_found")}), 400
    db.commit()
    return jsonify({"rfq_id": rfq_id, "invites": invite_result}), 200


@procurement_bp.route("/api/procurement/cotacoes/<int:rfq_id>/convites", methods=["GET"])
def list_rfq_invites(rfq_id: int):
    db = get_read_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": _err("rfq_not_found"), "rfq_id": rfq_id}), 404
    invites = _load_rfq_supplier_invites(db, tenant_id, rfq_id)
    return jsonify({"rfq_id": rfq_id, "items": invites})


@procurement_bp.route(
    "/api/procurement/cotacoes/<int:rfq_id>/convites/<int:invite_id>",
    methods=["PATCH", "DELETE"],
)
def rfq_invite_crud_api(rfq_id: int, invite_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": _err("rfq_not_found"), "rfq_id": rfq_id}), 404

    invite = _load_rfq_supplier_invite_by_id(db, tenant_id, rfq_id, invite_id)
    if not invite:
        return jsonify({"error": "invite_not_found", "message": _err("invite_not_found")}), 404

    if request.method == "DELETE":
        if not flow_action_allowed("cotacao", rfq["status"], "cancel_invite"):
            return _forbidden_action("cotacao", rfq["status"], "cancel_invite")
        if not flow_action_allowed("fornecedor", invite["status"], "cancel_invite"):
            return _forbidden_action("fornecedor", invite["status"], "cancel_invite")
        _require_critical_confirmation(
            "cancel_invite",
            entity="rfq_supplier_invite",
            entity_id=invite_id,
        )
        _remove_supplier_from_rfq(db, tenant_id, rfq_id, int(invite["supplier_id"]))
        db.execute(
            """
            UPDATE rfq_supplier_invites
            SET status = 'cancelled',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (invite_id, tenant_id),
        )
        db.commit()
        return jsonify({"deleted": True, "invite_id": invite_id}), 200

    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action") or "").strip().lower()
    valid_days = _parse_int(payload.get("invite_valid_days"), default=7, min_value=1, max_value=30)
    expires_at = (datetime.now(timezone.utc) + timedelta(days=valid_days)).replace(microsecond=0).isoformat()
    action_map = {"reopen": "reopen_invite", "extend": "extend_invite", "cancel": "cancel_invite"}
    requested_action = action_map.get(action)
    if not requested_action:
        return (
            jsonify(
                {
                    "error": "action_invalid", "message": _err("action_invalid"),
                }
            ),
            400,
        )
    if not flow_action_allowed("cotacao", rfq["status"], requested_action):
        return _forbidden_action("cotacao", rfq["status"], requested_action)
    if not flow_action_allowed("fornecedor", invite["status"], requested_action):
        return _forbidden_action("fornecedor", invite["status"], requested_action)
    if action == "cancel":
        _require_critical_confirmation(
            "cancel_invite",
            entity="rfq_supplier_invite",
            entity_id=invite_id,
            payload=payload,
        )

    if action == "reopen":
        new_token = secrets.token_urlsafe(24)
        db.execute(
            """
            UPDATE rfq_supplier_invites
            SET token = ?,
                status = 'pending',
                expires_at = ?,
                opened_at = NULL,
                submitted_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (new_token, expires_at, invite_id, tenant_id),
        )
    elif action == "extend":
        db.execute(
            """
            UPDATE rfq_supplier_invites
            SET expires_at = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (expires_at, invite_id, tenant_id),
        )
    elif action == "cancel":
        db.execute(
            """
            UPDATE rfq_supplier_invites
            SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (invite_id, tenant_id),
        )

    db.commit()
    updated = _load_rfq_supplier_invite_by_id(db, tenant_id, rfq_id, invite_id)
    if not updated:
        return jsonify({"error": "invite_not_found", "message": _err("invite_not_found")}), 404
    return jsonify({"invite": _serialize_rfq_invite_row(updated)}), 200


@procurement_bp.route("/api/procurement/rfqs/<int:rfq_id>/comparison", methods=["GET"])
def rfq_comparison(rfq_id: int):
    db = get_read_db()
    tenant_id = current_tenant_id()

    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return (
            jsonify({"error": "rfq_not_found", "message": _err("rfq_not_found"), "rfq_id": rfq_id}),
            404,
        )

    comparison = _build_rfq_comparison(db, tenant_id, rfq_id)
    return jsonify(
        {
            "rfq_id": rfq["id"],
            "rfq": {
                "title": rfq["title"],
                "status": rfq["status"],
                "updated_at": rfq["updated_at"],
            },
            **comparison,
        }
    )


@procurement_bp.route("/api/procurement/rfqs/<int:rfq_id>/award", methods=["POST"])
def rfq_award(rfq_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}
    result = _PROCUREMENT_SERVICE.award_rfq(
        db,
        tenant_id=tenant_id,
        award_input=RfqAwardInput(
            rfq_id=rfq_id,
            reason=(payload.get("reason") or "").strip(),
            supplier_name=(payload.get("supplier_name") or "Fornecedor selecionado").strip(),
            payload=payload,
        ),
        load_rfq_fn=_load_rfq,
        flow_action_allowed_fn=flow_action_allowed,
        forbidden_action_fn=_forbidden_action,
        require_confirmation_fn=_require_critical_confirmation,
        err_fn=_err,
    )
    db.commit()
    return jsonify(result.payload), result.status_code

@procurement_bp.route("/api/procurement/awards/<int:award_id>/purchase-orders", methods=["POST"])
def create_purchase_order_from_award(award_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}
    result = _PROCUREMENT_SERVICE.create_purchase_order_from_award(
        db,
        tenant_id=tenant_id,
        create_input=PurchaseOrderFromAwardInput(award_id=award_id, payload=payload),
        load_award_fn=_load_award,
        flow_action_allowed_fn=flow_action_allowed,
        forbidden_action_fn=_forbidden_action,
        require_confirmation_fn=_require_critical_confirmation,
        err_fn=_err,
    )
    db.commit()
    return jsonify(result.payload), result.status_code


@procurement_bp.route("/api/procurement/purchase-orders/<int:purchase_order_id>/push-to-erp", methods=["POST"])
def push_purchase_order_to_erp(purchase_order_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}
    result = _PROCUREMENT_SERVICE.register_erp_intent(
        db,
        tenant_id=tenant_id,
        intent_input=PurchaseOrderErpIntentInput(
            purchase_order_id=purchase_order_id,
            request_id=(getattr(g, "request_id", None) or "").strip() or None,
            payload=payload,
        ),
        load_purchase_order_fn=_load_purchase_order,
        find_pending_push_fn=find_pending_purchase_order_push,
        flow_action_allowed_fn=flow_action_allowed,
        forbidden_action_fn=_forbidden_action,
        require_confirmation_fn=_require_critical_confirmation,
        queue_push_fn=queue_purchase_order_push,
        err_fn=_err,
        ok_fn=_ok,
    )
    db.commit()
    return jsonify(result.payload), result.status_code


def _load_open_purchase_requests(db, tenant_id: str | None, limit: int = 80) -> List[dict]:
    clause, params = _tenant_clause(tenant_id)
    rows = db.execute(
        f"""
        SELECT id, number, status, priority, requested_by, department, needed_at, updated_at
        FROM purchase_requests
        WHERE status = 'pending_rfq' AND {clause}
        ORDER BY needed_at IS NULL, needed_at, updated_at DESC, id DESC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()

    return [
        {
            "id": row["id"],
            "number": row["number"],
            "status": row["status"],
            "priority": row["priority"],
            "requested_by": row["requested_by"],
            "department": row["department"],
            "needed_at": row["needed_at"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def _load_open_purchase_request_items(db, tenant_id: str | None, limit: int = 120) -> List[dict]:
    effective_tenant_id = tenant_id or DEFAULT_TENANT_ID
    rows = db.execute(
        """
        SELECT
            pri.id,
            pri.purchase_request_id,
            pri.description,
            pri.quantity,
            pri.uom,
            pri.line_no,
            pr.number,
            pr.requested_by,
            pr.department,
            pr.needed_at,
            pr.priority
        FROM purchase_request_items pri
        JOIN purchase_requests pr
          ON pr.id = pri.purchase_request_id AND pr.tenant_id = pri.tenant_id
        WHERE pr.status = 'pending_rfq' AND pr.tenant_id = ?
        ORDER BY pr.needed_at IS NULL, pr.needed_at, pr.id DESC, pri.line_no, pri.id
        LIMIT ?
        """,
        (effective_tenant_id, limit),
    ).fetchall()

    return [
        {
            "id": row["id"],
            "purchase_request_id": row["purchase_request_id"],
            "number": row["number"],
            "requested_by": row["requested_by"],
            "department": row["department"],
            "needed_at": row["needed_at"],
            "priority": row["priority"],
            "description": row["description"],
            "quantity": row["quantity"],
            "uom": row["uom"],
            "line_no": row["line_no"],
        }
        for row in rows
    ]


def _load_purchase_requests_panel(
    db,
    tenant_id: str | None,
    limit: int = 120,
    status_values: List[str] | None = None,
    erp_only: bool = False,
) -> List[dict]:
    effective_tenant_id = tenant_id or DEFAULT_TENANT_ID
    params: List[object] = [effective_tenant_id]
    filters: List[str] = []

    allowed_statuses = status_values or []
    if allowed_statuses:
        normalized = [status for status in allowed_statuses if status in ALLOWED_PR_STATUSES]
        if normalized:
            filters.append(f"pr.status IN ({','.join('?' for _ in normalized)})")
            params.extend(normalized)

    if erp_only:
        filters.append("(pr.external_id IS NOT NULL OR pr.erp_num_cot IS NOT NULL OR pr.erp_num_pct IS NOT NULL)")

    where_sql = " AND ".join(filters) if filters else "1=1"
    params.append(limit)

    rows = db.execute(
        f"""
        SELECT
            pr.id,
            pr.number,
            pr.status,
            pr.priority,
            pr.requested_by,
            pr.department,
            pr.needed_at,
            pr.external_id,
            pr.erp_num_cot,
            pr.erp_num_pct,
            pr.erp_sent_at,
            pr.created_at,
            pr.updated_at,
            COUNT(pri.id) AS item_count
        FROM purchase_requests pr
        LEFT JOIN purchase_request_items pri
          ON pri.purchase_request_id = pr.id AND pri.tenant_id = pr.tenant_id
        WHERE pr.tenant_id = ? AND {where_sql}
        GROUP BY
            pr.id, pr.number, pr.status, pr.priority, pr.requested_by, pr.department, pr.needed_at,
            pr.external_id, pr.erp_num_cot, pr.erp_num_pct, pr.erp_sent_at, pr.created_at, pr.updated_at
        ORDER BY pr.needed_at IS NULL, pr.needed_at, pr.updated_at DESC, pr.id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()

    result = []
    for row in rows:
        status = row["status"]
        source = "erp" if row["external_id"] or row["erp_num_cot"] or row["erp_num_pct"] else "local"
        stage = stage_for_purchase_request_status(status)
        meta = flow_meta("solicitacao", status)
        if source == "erp":
            meta.update(
                _filter_actions(
                    meta,
                    {
                        "edit_request",
                        "update_request_status",
                        "add_request_item",
                        "edit_request_item",
                        "delete_request_item",
                        "cancel_request",
                    },
                )
            )
        result.append(
            {
                "id": row["id"],
                "number": row["number"],
                "status": status,
                "priority": row["priority"],
                "requested_by": row["requested_by"],
                "department": row["department"],
                "needed_at": row["needed_at"],
                "item_count": int(row["item_count"] or 0),
                "erp_num_cot": row["erp_num_cot"],
                "erp_num_pct": row["erp_num_pct"],
                "erp_sent_at": row["erp_sent_at"],
                "external_id": row["external_id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "source": source,
                "process_stage": stage,
                "allowed_actions": meta["allowed_actions"],
                "primary_action": meta["primary_action"],
            }
        )
    return result


def _load_purchase_orders_panel(
    db,
    tenant_id: str | None,
    limit: int = 120,
    status_values: List[str] | None = None,
    supplier_search: str | None = None,
    erp_only: bool = False,
) -> List[dict]:
    effective_tenant_id = tenant_id or DEFAULT_TENANT_ID
    params: List[object] = [effective_tenant_id]
    filters: List[str] = []

    normalized_statuses = [status for status in (status_values or []) if status in ALLOWED_PO_STATUSES]
    if normalized_statuses:
        filters.append(f"po.status IN ({','.join('?' for _ in normalized_statuses)})")
        params.extend(normalized_statuses)

    if supplier_search:
        filters.append("LOWER(COALESCE(po.supplier_name, '')) LIKE ?")
        params.append(f"%{supplier_search.lower()}%")

    if erp_only:
        filters.append("po.external_id IS NOT NULL")

    where_sql = " AND ".join(filters) if filters else "1=1"
    params.append(limit)

    rows = db.execute(
        f"""
        SELECT
            po.id,
            po.number,
            po.supplier_name,
            po.status,
            po.currency,
            po.total_amount,
            po.external_id,
            po.erp_last_error,
            (
                SELECT MAX(se.occurred_at)
                FROM status_events se
                WHERE se.entity = 'purchase_order'
                  AND se.entity_id = po.id
                  AND se.tenant_id = po.tenant_id
                  AND se.reason LIKE 'po_push_%'
            ) AS erp_last_attempt_at,
            (
                SELECT se.reason
                FROM status_events se
                WHERE se.entity = 'purchase_order'
                  AND se.entity_id = po.id
                  AND se.tenant_id = po.tenant_id
                  AND se.reason LIKE 'po_push_%'
                ORDER BY se.occurred_at DESC, se.id DESC
                LIMIT 1
            ) AS erp_last_event_reason,
            po.created_at,
            po.updated_at
        FROM purchase_orders po
        WHERE po.tenant_id = ? AND {where_sql}
        ORDER BY po.updated_at DESC, po.id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()

    result: List[dict] = []
    for row in rows:
        status = row["status"]
        source = "erp" if row["external_id"] else "local"
        stage = stage_for_purchase_order_status(status)
        meta = flow_meta("ordem_compra", status)
        if source == "erp":
            meta.update(_filter_actions(meta, {"edit_order", "cancel_order"}))
        erp_status = erp_status_payload(
            status,
            erp_last_error=row["erp_last_error"],
            last_updated_at=row["erp_last_attempt_at"] or row["updated_at"],
        )
        result.append(
            {
                "id": row["id"],
                "number": row["number"],
                "supplier_name": row["supplier_name"],
                "status": status,
                "currency": row["currency"],
                "total_amount": row["total_amount"],
                "external_id": row["external_id"],
                "erp_last_error": row["erp_last_error"],
                "erp_last_attempt_at": row["erp_last_attempt_at"],
                "erp_last_event_reason": row["erp_last_event_reason"],
                "erp_status": erp_status,
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "source": source,
                "process_stage": stage,
                "allowed_actions": meta["allowed_actions"],
                "primary_action": meta["primary_action"],
            }
        )
    return result


def _load_purchase_request_items_by_ids(db, tenant_id: str, item_ids: List[int]) -> List[dict]:
    if not item_ids:
        return []
    placeholders = ",".join(["?"] * len(item_ids))
    rows = db.execute(
        f"""
        SELECT
            pri.id,
            pri.purchase_request_id,
            pri.description,
            pri.quantity,
            pri.uom
        FROM purchase_request_items pri
        JOIN purchase_requests pr
          ON pr.id = pri.purchase_request_id AND pr.tenant_id = pri.tenant_id
        WHERE pr.status = 'pending_rfq'
          AND pri.tenant_id = ?
          AND pri.id IN ({placeholders})
        """,
        (tenant_id, *item_ids),
    ).fetchall()

    return [
        {
            "id": row["id"],
            "purchase_request_id": row["purchase_request_id"],
            "description": row["description"],
            "quantity": row["quantity"],
            "uom": row["uom"],
        }
        for row in rows
    ]


def _load_rfq_items(db, tenant_id: str, rfq_id: int) -> List[dict]:
    rows = db.execute(
        """
        SELECT id, rfq_id, purchase_request_item_id, description, quantity, uom
        FROM rfq_items
        WHERE rfq_id = ? AND tenant_id = ?
        ORDER BY id
        """,
        (rfq_id, tenant_id),
    ).fetchall()
    return [dict(row) for row in rows]


def _create_rfq_core(
    db,
    tenant_id: str,
    title: str,
    item_ids: List[int],
) -> Tuple[dict | None, dict | None, int]:
    if not item_ids:
        return (
            None,
            {
                "error": "purchase_request_item_ids_required", "message": _err("purchase_request_item_ids_required"),
            },
            400,
        )

    request_items = _load_purchase_request_items_by_ids(db, tenant_id, item_ids)
    if not request_items:
        return (
            None,
            {
                "error": "purchase_request_items_not_found", "message": _err("purchase_request_items_not_found"),
            },
            400,
        )

    status = "open"
    cursor = db.execute(
        "INSERT INTO rfqs (title, status, tenant_id) VALUES (?, ?, ?) RETURNING id",
        (title, status, tenant_id),
    )
    rfq_row = cursor.fetchone()
    rfq_id = rfq_row["id"] if isinstance(rfq_row, dict) else rfq_row[0]

    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
        VALUES ('rfq', ?, NULL, ?, 'rfq_created', ?)
        """,
        (rfq_id, status, tenant_id),
    )

    rfq_items_payload: List[dict] = []
    for item in request_items:
        cursor = db.execute(
            """
            INSERT INTO rfq_items (
                rfq_id,
                purchase_request_item_id,
                description,
                quantity,
                uom,
                tenant_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (
                rfq_id,
                item["id"],
                item["description"],
                item["quantity"],
                item["uom"],
                tenant_id,
            ),
        )
        rfq_item_row = cursor.fetchone()
        rfq_items_payload.append(
            {
                "rfq_item_id": rfq_item_row["id"] if isinstance(rfq_item_row, dict) else rfq_item_row[0],
                "purchase_request_item_id": item["id"],
                "description": item["description"],
                "quantity": item["quantity"],
                "uom": item["uom"],
            }
        )

    request_ids = sorted({item["purchase_request_id"] for item in request_items})
    _mark_purchase_requests_in_rfq(db, tenant_id, request_ids)
    created_row = db.execute(
        "SELECT created_at FROM rfqs WHERE id = ? AND tenant_id = ?",
        (rfq_id, tenant_id),
    ).fetchone()

    return (
        {
            "id": rfq_id,
            "status": status,
            "title": title,
            "created_at": created_row["created_at"] if created_row else None,
            "rfq_items": rfq_items_payload,
        },
        None,
        201,
    )


def _mark_purchase_requests_in_rfq(db, tenant_id: str, request_ids: List[int]) -> None:
    if not request_ids:
        return
    clause, params = _tenant_clause(tenant_id)
    placeholders = ",".join(["?"] * len(request_ids))
    rows = db.execute(
        f"""
        SELECT id, status
        FROM purchase_requests
        WHERE {clause} AND id IN ({placeholders})
        """,
        (*params, *request_ids),
    ).fetchall()

    for row in rows:
        previous_status = row["status"]
        if previous_status == "in_rfq":
            continue
        db.execute(
            "UPDATE purchase_requests SET status = 'in_rfq' WHERE id = ? AND tenant_id = ?",
            (row["id"], tenant_id),
        )
        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('purchase_request', ?, ?, 'in_rfq', 'rfq_created', ?)
            """,
            (row["id"], previous_status, tenant_id),
        )


def _load_suppliers(db, tenant_id: str | None) -> List[dict]:
    clause, params = _tenant_clause(tenant_id)
    rows = db.execute(
        f"""
        SELECT id, name, email, external_id, tax_id, tenant_id
        FROM suppliers
        WHERE {clause}
        ORDER BY name
        LIMIT 200
        """,
        tuple(params),
    ).fetchall()

    return [
        {
            "id": row["id"],
            "name": row["name"],
            "email": row["email"],
            "external_id": row["external_id"],
            "tax_id": row["tax_id"],
        }
        for row in rows
    ]


def _ensure_demo_suppliers(db, tenant_id: str) -> None:
    existing = db.execute(
        "SELECT COUNT(*) AS total FROM suppliers WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchone()["total"]
    if existing:
        return

    demo_suppliers = [
        ("Fornecedor Atlas", "atlas@fornecedor.exemplo"),
        ("Fornecedor Nexo", "nexo@fornecedor.exemplo"),
        ("Fornecedor Prisma", "prisma@fornecedor.exemplo"),
    ]
    for name, email in demo_suppliers:
        db.execute(
            "INSERT INTO suppliers (name, email, tenant_id) VALUES (?, ?, ?)",
            (name, email, tenant_id),
        )


def _create_rfq_supplier_invites(
    db,
    tenant_id: str,
    rfq_id: int,
    rfq_items: List[dict],
    supplier_ids: List[int],
    valid_days: int = 7,
) -> dict:
    suppliers = _load_suppliers(db, tenant_id)
    suppliers_map = {int(s["id"]): s for s in suppliers}
    selected_supplier_ids = [sid for sid in supplier_ids if sid in suppliers_map]
    rfq_item_ids = sorted({int(item["rfq_item_id"]) for item in rfq_items if item.get("rfq_item_id") is not None})

    expires_at = (datetime.now(timezone.utc) + timedelta(days=valid_days)).replace(microsecond=0).isoformat()
    invites_payload: List[dict] = []

    for supplier_id in selected_supplier_ids:
        supplier = suppliers_map[supplier_id]
        for rfq_item_id in rfq_item_ids:
            db.execute(
                """
                INSERT INTO rfq_item_suppliers (rfq_item_id, supplier_id, tenant_id)
                VALUES (?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                (rfq_item_id, supplier_id, tenant_id),
            )

        db.execute(
            """
            UPDATE rfq_supplier_invites
            SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
            WHERE rfq_id = ? AND supplier_id = ? AND tenant_id = ? AND status IN ('pending', 'opened')
            """,
            (rfq_id, supplier_id, tenant_id),
        )

        token = secrets.token_urlsafe(24)
        email = supplier.get("email")
        cursor = db.execute(
            """
            INSERT INTO rfq_supplier_invites (
                rfq_id, supplier_id, token, email, status, expires_at, tenant_id
            ) VALUES (?, ?, ?, ?, 'pending', ?, ?)
            RETURNING id
            """,
            (rfq_id, supplier_id, token, email, expires_at, tenant_id),
        )
        invite_row = cursor.fetchone()
        invite_id = int(invite_row["id"] if isinstance(invite_row, dict) else invite_row[0])

        access_url = _build_invite_public_url(token)
        invites_payload.append(
            {
                "id": invite_id,
                "supplier_id": supplier_id,
                "supplier_name": supplier.get("name"),
                "supplier_email": email,
                "token": token,
                "status": "pending",
                "expires_at": expires_at,
                "access_url": access_url,
                "mailto_url": _build_invite_mailto_url(
                    supplier_name=supplier.get("name"),
                    supplier_email=email,
                    rfq_id=rfq_id,
                    access_url=access_url,
                ),
            }
        )

    return {
        "rfq_id": rfq_id,
        "items_invited": len(rfq_item_ids),
        "supplier_count": len(invites_payload),
        "items": invites_payload,
    }


def _serialize_rfq_invite_row(row) -> dict:
    token = row["token"]
    access_url = _build_invite_public_url(token)
    return {
        "id": int(row["id"]),
        "rfq_id": int(row["rfq_id"]),
        "supplier_id": int(row["supplier_id"]),
        "supplier_name": row["supplier_name"],
        "supplier_email": row["email"],
        "token": token,
        "status": row["status"],
        "expires_at": row["expires_at"],
        "opened_at": row["opened_at"],
        "submitted_at": row["submitted_at"],
        "access_url": access_url,
        "mailto_url": _build_invite_mailto_url(
            supplier_name=row["supplier_name"],
            supplier_email=row["email"],
            rfq_id=int(row["rfq_id"]),
            access_url=access_url,
        ),
    }


def _load_rfq_supplier_invites(db, tenant_id: str, rfq_id: int) -> List[dict]:
    rows = db.execute(
        """
        SELECT
            i.id,
            i.rfq_id,
            i.supplier_id,
            i.token,
            i.email,
            i.status,
            i.expires_at,
            i.opened_at,
            i.submitted_at,
            s.name AS supplier_name
        FROM rfq_supplier_invites i
        LEFT JOIN suppliers s
          ON s.id = i.supplier_id AND s.tenant_id = i.tenant_id
        WHERE i.rfq_id = ? AND i.tenant_id = ?
        ORDER BY i.id DESC
        """,
        (rfq_id, tenant_id),
    ).fetchall()
    return [_serialize_rfq_invite_row(row) for row in rows]


def _load_rfq_supplier_invite_by_id(db, tenant_id: str, rfq_id: int, invite_id: int):
    return db.execute(
        """
        SELECT
            i.id,
            i.rfq_id,
            i.supplier_id,
            i.token,
            i.email,
            i.status,
            i.expires_at,
            i.opened_at,
            i.submitted_at,
            s.name AS supplier_name
        FROM rfq_supplier_invites i
        LEFT JOIN suppliers s
          ON s.id = i.supplier_id AND s.tenant_id = i.tenant_id
        WHERE i.id = ? AND i.rfq_id = ? AND i.tenant_id = ?
        LIMIT 1
        """,
        (invite_id, rfq_id, tenant_id),
    ).fetchone()


def _remove_supplier_from_rfq(db, tenant_id: str, rfq_id: int, supplier_id: int) -> None:
    quote_row = db.execute(
        """
        SELECT id
        FROM quotes
        WHERE rfq_id = ? AND supplier_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (rfq_id, supplier_id, tenant_id),
    ).fetchone()
    if quote_row:
        quote_id = int(quote_row["id"])
        db.execute("DELETE FROM quote_items WHERE quote_id = ? AND tenant_id = ?", (quote_id, tenant_id))
        db.execute("DELETE FROM quotes WHERE id = ? AND tenant_id = ?", (quote_id, tenant_id))

    rfq_item_rows = db.execute(
        "SELECT id FROM rfq_items WHERE rfq_id = ? AND tenant_id = ?",
        (rfq_id, tenant_id),
    ).fetchall()
    rfq_item_ids = [int(row["id"]) for row in rfq_item_rows]
    if rfq_item_ids:
        placeholders = ",".join("?" for _ in rfq_item_ids)
        db.execute(
            f"""
            DELETE FROM rfq_item_suppliers
            WHERE tenant_id = ? AND supplier_id = ? AND rfq_item_id IN ({placeholders})
            """,
            (tenant_id, supplier_id, *rfq_item_ids),
        )


def _build_invite_public_url(token: str) -> str:
    base_url = (current_app.config.get("PUBLIC_APP_URL") or request.url_root or "").strip().rstrip("/")
    return f"{base_url}/fornecedor/convite/{token}"


def _build_invite_mailto_url(supplier_name: str | None, supplier_email: str | None, rfq_id: int, access_url: str) -> str:
    recipient = supplier_email or ""
    subject = quote(f"Cotacao {rfq_id} - acesso para proposta")
    body_text = (
        f"Ola {supplier_name or 'fornecedor'},\n\n"
        "Voce foi convidado para enviar proposta na Plataforma Compras.\n"
        f"Acesse o link: {access_url}\n\n"
        "Atenciosamente,\nEquipe de Compras"
    )
    body = quote(body_text)
    return f"mailto:{recipient}?subject={subject}&body={body}"


def _load_supplier_invite_by_token(db, token: str):
    row = db.execute(
        """
        SELECT id, rfq_id, supplier_id, token, email, status, expires_at, opened_at, submitted_at, tenant_id
        FROM rfq_supplier_invites
        WHERE token = ?
        LIMIT 1
        """,
        (token.strip(),),
    ).fetchone()
    return row


def _invite_is_expired(invite) -> bool:
    expires_at = _parse_datetime(invite["expires_at"]) if invite is not None else None
    if not expires_at:
        return False
    now = datetime.now(timezone.utc)
    return now > expires_at


def _build_supplier_invite_payload(db, invite) -> dict:
    rfq_id = int(invite["rfq_id"])
    supplier_id = int(invite["supplier_id"])
    tenant_id = str(invite["tenant_id"])

    rfq = _load_rfq(db, tenant_id, rfq_id)
    supplier_row = db.execute(
        "SELECT id, name, email FROM suppliers WHERE id = ? AND tenant_id = ?",
        (supplier_id, tenant_id),
    ).fetchone()

    rows = db.execute(
        """
        SELECT
            ri.id AS rfq_item_id,
            ri.description,
            ri.quantity,
            ri.uom,
            qi.unit_price,
            qi.lead_time_days
        FROM rfq_items ri
        JOIN rfq_item_suppliers ris
          ON ris.rfq_item_id = ri.id
         AND ris.supplier_id = ?
         AND ris.tenant_id = ?
        LEFT JOIN quotes q
          ON q.rfq_id = ri.rfq_id
         AND q.supplier_id = ris.supplier_id
         AND q.tenant_id = ri.tenant_id
        LEFT JOIN quote_items qi
          ON qi.quote_id = q.id
         AND qi.rfq_item_id = ri.id
         AND qi.tenant_id = ri.tenant_id
        WHERE ri.rfq_id = ? AND ri.tenant_id = ?
        ORDER BY ri.id
        """,
        (supplier_id, tenant_id, rfq_id, tenant_id),
    ).fetchall()

    return {
        "invite": {
            "id": invite["id"],
            "status": invite["status"],
            "expires_at": invite["expires_at"],
            "opened_at": invite["opened_at"],
            "submitted_at": invite["submitted_at"],
        },
        "rfq": {
            "id": rfq_id,
            "title": rfq["title"] if rfq else f"Cotacao {rfq_id}",
            "status": rfq["status"] if rfq else "open",
        },
        "supplier": {
            "id": supplier_id,
            "name": supplier_row["name"] if supplier_row else "Fornecedor",
            "email": supplier_row["email"] if supplier_row else None,
        },
        "items": [
            {
                "rfq_item_id": int(row["rfq_item_id"]),
                "description": row["description"],
                "quantity": row["quantity"],
                "uom": row["uom"],
                "unit_price": row["unit_price"],
                "lead_time_days": row["lead_time_days"],
            }
            for row in rows
        ],
    }


def _load_rfq(db, tenant_id: str | None, rfq_id: int):
    clause, params = _tenant_clause(tenant_id)
    sql = f"SELECT id, title, status, created_at, updated_at, tenant_id FROM rfqs WHERE id = ? AND {clause}"
    row = db.execute(sql, (rfq_id, *params)).fetchone()
    return row


def _load_rfq_item(db, tenant_id: str | None, rfq_item_id: int):
    clause, params = _tenant_clause(tenant_id)
    row = db.execute(
        f"""
        SELECT id, rfq_id, description, quantity, uom, tenant_id, created_at, updated_at
        FROM rfq_items
        WHERE id = ? AND {clause}
        LIMIT 1
        """,
        (rfq_item_id, *params),
    ).fetchone()
    return row


def _load_rfq_items_with_quotes(db, tenant_id: str | None, rfq_id: int) -> List[dict]:
    effective_tenant_id = tenant_id or DEFAULT_TENANT_ID

    rows = db.execute(
        f"""
        SELECT
            ri.id AS rfq_item_id,
            ri.description,
            ri.quantity,
            ri.uom,
            s.id AS supplier_id,
            s.name AS supplier_name,
            q.id AS quote_id,
            qi.unit_price,
            qi.lead_time_days
        FROM rfq_items ri
        LEFT JOIN rfq_item_suppliers ris
            ON ris.rfq_item_id = ri.id AND ris.tenant_id = ?
        LEFT JOIN suppliers s
            ON s.id = ris.supplier_id AND s.tenant_id = ?
        LEFT JOIN quotes q
            ON q.rfq_id = ri.rfq_id AND q.supplier_id = s.id AND q.tenant_id = ?
        LEFT JOIN quote_items qi
            ON qi.quote_id = q.id AND qi.rfq_item_id = ri.id AND qi.tenant_id = ?
        WHERE ri.rfq_id = ? AND ri.tenant_id = ?
        ORDER BY ri.id, s.name
        """,
        (
            effective_tenant_id,
            effective_tenant_id,
            effective_tenant_id,
            effective_tenant_id,
            rfq_id,
            effective_tenant_id,
        ),
    ).fetchall()

    items: Dict[int, dict] = {}
    for row in rows:
        item_id = int(row["rfq_item_id"])
        item = items.get(item_id)
        if not item:
            item = {
                "rfq_item_id": item_id,
                "descricao": row["description"],
                "quantidade": row["quantity"],
                "uom": row["uom"],
                "fornecedores": [],
                "melhor_proposta": None,
            }
            items[item_id] = item

        supplier_id = row["supplier_id"]
        if supplier_id is None:
            continue

        proposta = {
            "supplier_id": supplier_id,
            "supplier_name": row["supplier_name"],
            "quote_id": row["quote_id"],
            "unit_price": row["unit_price"],
            "lead_time_days": row["lead_time_days"],
        }
        item["fornecedores"].append(proposta)

    for item in items.values():
        melhores = [f for f in item["fornecedores"] if f.get("unit_price") is not None]
        melhores.sort(key=lambda f: f["unit_price"])
        item["melhor_proposta"] = melhores[0] if melhores else None

    return list(items.values())


def _build_rfq_comparison(db, tenant_id: str | None, rfq_id: int) -> dict:
    effective_tenant_id = tenant_id or DEFAULT_TENANT_ID

    rfq_row = db.execute(
        """
        SELECT created_at
        FROM rfqs
        WHERE id = ? AND tenant_id = ?
        """,
        (rfq_id, effective_tenant_id),
    ).fetchone()
    rfq_created_at = rfq_row["created_at"] if rfq_row else None

    erp_context_rows = db.execute(
        """
        SELECT DISTINCT pr.erp_sent_at, pr.erp_num_cot, pr.erp_num_pct
        FROM rfq_items ri
        JOIN purchase_request_items pri
            ON pri.id = ri.purchase_request_item_id AND pri.tenant_id = ri.tenant_id
        JOIN purchase_requests pr
            ON pr.id = pri.purchase_request_id AND pr.tenant_id = pri.tenant_id
        WHERE ri.rfq_id = ? AND ri.tenant_id = ?
        """,
        (rfq_id, effective_tenant_id),
    ).fetchall()

    erp_sent_candidates = [row["erp_sent_at"] for row in erp_context_rows if row["erp_sent_at"]]
    erp_num_cot_values = sorted({row["erp_num_cot"] for row in erp_context_rows if row["erp_num_cot"]})
    erp_num_pct_values = sorted({row["erp_num_pct"] for row in erp_context_rows if row["erp_num_pct"]})

    sla_start_at = None
    if erp_sent_candidates:
        parsed: List[datetime] = []
        for value in erp_sent_candidates:
            dt = _parse_datetime(value)
            if dt:
                parsed.append(dt)
        if parsed:
            sla_start_at = _format_datetime(min(parsed))

    erp_last_quote_at = None
    if erp_num_cot_values or erp_num_pct_values:
        clauses = []
        params = [effective_tenant_id]
        if erp_num_cot_values:
            placeholders = ",".join("?" for _ in erp_num_cot_values)
            clauses.append(f"erp_num_cot IN ({placeholders})")
            params.extend(erp_num_cot_values)
        if erp_num_pct_values:
            placeholders = ",".join("?" for _ in erp_num_pct_values)
            clauses.append(f"erp_num_pct IN ({placeholders})")
            params.extend(erp_num_pct_values)
        where_clause = " OR ".join(clauses)
        last_quote_row = db.execute(
            f"""
            SELECT MAX(quote_datetime) AS last_quote_at
            FROM erp_supplier_quotes
            WHERE tenant_id = ? AND ({where_clause})
            """,
            params,
        ).fetchone()
        erp_last_quote_at = last_quote_row["last_quote_at"] if last_quote_row else None

    local_last_quote_row = db.execute(
        """
        SELECT MAX(qi.updated_at) AS last_quote_at
        FROM quote_items qi
        JOIN quotes q
          ON q.id = qi.quote_id AND q.tenant_id = qi.tenant_id
        WHERE q.rfq_id = ? AND qi.tenant_id = ?
        """,
        (rfq_id, effective_tenant_id),
    ).fetchone()
    local_last_quote_at = local_last_quote_row["last_quote_at"] if local_last_quote_row else None

    last_quote_at = erp_last_quote_at or local_last_quote_at
    sla_start_label = "dias desde abertura"
    sla_source_start = "local"
    sla_source_last_quote = "local"
    if sla_start_at:
        sla_start_label = "dias desde envio ao fornecedor"
        sla_source_start = "erp"
    if erp_last_quote_at:
        sla_source_last_quote = "erp"

    now = datetime.now(timezone.utc)
    rfq_age_days = _days_since(sla_start_at or rfq_created_at, now)
    last_quote_days = _days_since(last_quote_at, now)
    # SLA conforme docs/domain/sla.md
    sla_limit_days = _sla_threshold_days()
    sla_risk = False
    if sla_limit_days > 0:
        if last_quote_days is not None:
            sla_risk = last_quote_days >= sla_limit_days
        elif rfq_age_days is not None:
            sla_risk = rfq_age_days >= sla_limit_days

    items_rows = db.execute(
        """
        SELECT id, description, quantity, uom
        FROM rfq_items
        WHERE rfq_id = ? AND tenant_id = ?
        ORDER BY id
        """,
        (rfq_id, effective_tenant_id),
    ).fetchall()

    items: Dict[int, dict] = {
        int(row["id"]): {
            "rfq_item_id": int(row["id"]),
            "description": row["description"],
            "quantity": row["quantity"],
            "uom": row["uom"],
            "quotes": [],
            "suggested_supplier_id": None,
            "suggestion_reason": None,
        }
        for row in items_rows
    }

    quote_rows = db.execute(
        """
        SELECT
            qi.rfq_item_id,
            s.id AS supplier_id,
            s.name AS supplier_name,
            s.risk_flags,
            qi.unit_price,
            qi.lead_time_days
        FROM quote_items qi
        JOIN quotes q
          ON q.id = qi.quote_id AND q.tenant_id = qi.tenant_id
        JOIN suppliers s
          ON s.id = q.supplier_id AND s.tenant_id = q.tenant_id
        WHERE q.rfq_id = ? AND qi.tenant_id = ?
        ORDER BY qi.rfq_item_id, s.name
        """,
        (rfq_id, effective_tenant_id),
    ).fetchall()

    suppliers: Dict[int, dict] = {}
    totals: Dict[int, dict] = {}
    total_items = len(items_rows)

    for row in quote_rows:
        item_id = int(row["rfq_item_id"])
        item = items.get(item_id)
        if not item:
            continue

        unit_price = row["unit_price"]
        quantity = item["quantity"] or 0
        total_value = None
        if unit_price is not None:
            total_value = float(unit_price) * float(quantity or 0)

        quote = {
            "supplier_id": row["supplier_id"],
            "supplier_name": row["supplier_name"],
            "unit_price": unit_price,
            "lead_time_days": row["lead_time_days"],
            "total": total_value,
            "item_total": total_value,
            "best_price": False,
            "best_lead_time": False,
        }
        item["quotes"].append(quote)

        supplier_id = row["supplier_id"]
        if supplier_id not in suppliers:
            suppliers[supplier_id] = {
                "supplier_id": supplier_id,
                "name": row["supplier_name"],
                "risk_flags": _parse_risk_flags(row["risk_flags"]),
            }

        if supplier_id not in totals:
            totals[supplier_id] = {
                "supplier_id": supplier_id,
                "supplier_name": row["supplier_name"],
                "total_amount": 0.0,
                "lead_times": [],
                "items_quoted": set(),
            }

        if total_value is not None:
            totals[supplier_id]["total_amount"] += total_value
            totals[supplier_id]["items_quoted"].add(item_id)
        if row["lead_time_days"] is not None:
            totals[supplier_id]["lead_times"].append(int(row["lead_time_days"]))

    for item in items.values():
        quotes = item["quotes"]
        priced_quotes = [q for q in quotes if q.get("unit_price") is not None]
        lead_quotes = [q for q in quotes if q.get("lead_time_days") is not None]

        best_price = min((q["unit_price"] for q in priced_quotes), default=None)
        best_lead = min((q["lead_time_days"] for q in lead_quotes), default=None)

        for quote in quotes:
            if best_price is not None and quote.get("unit_price") == best_price:
                quote["best_price"] = True
            if best_lead is not None and quote.get("lead_time_days") == best_lead:
                quote["best_lead_time"] = True

        quotes_with_total = [q for q in quotes if q.get("total") is not None]
        if not quotes_with_total:
            continue
        quotes_with_total.sort(
            key=lambda q: (
                q["total"],
                q["lead_time_days"] if q["lead_time_days"] is not None else 9_999,
            )
        )
        best = quotes_with_total[0]
        item["suggested_supplier_id"] = best["supplier_id"]
        item["suggestion_reason"] = "menor total, desempate por prazo" if len(quotes_with_total) > 1 else "menor total"

    totals_list = []
    for data in totals.values():
        items_quoted = len(data["items_quoted"])
        lead_times = data["lead_times"]
        avg_lead_time = None
        max_lead_time = None
        if lead_times:
            avg_lead_time = sum(lead_times) / len(lead_times)
            max_lead_time = max(lead_times)
        supplier_flags = suppliers.get(data["supplier_id"], {}).get("risk_flags") or DEFAULT_RISK_FLAGS
        flags = {
            "late_delivery": bool(supplier_flags.get("late_delivery") or supplier_flags.get("sla_breach")),
            "no_supplier_response": bool(supplier_flags.get("no_supplier_response")),
        }
        totals_list.append(
            {
                "supplier_id": data["supplier_id"],
                "supplier_name": data["supplier_name"],
                "total_amount": data["total_amount"],
                "avg_lead_time_days": avg_lead_time,
                "max_lead_time_days": max_lead_time,
                "items_quoted": items_quoted,
                "items_total": total_items,
                "flags": flags,
            }
        )

    suggested_supplier_id = None
    suggestion_reason = None
    if totals_list:
        full_cover = [t for t in totals_list if t["items_quoted"] == total_items]
        if full_cover:
            candidates = full_cover
            suggestion_reason = "menor total (cobertura completa)"
        else:
            # Sem cobertura completa: nao sugerir melhor opcao geral.
            candidates = []
            suggestion_reason = "sem cobertura completa"

        if candidates:
            candidates.sort(
                key=lambda t: (
                    t["total_amount"],
                    t["avg_lead_time_days"] if t["avg_lead_time_days"] is not None else 9_999,
                )
            )
            suggested_supplier_id = candidates[0]["supplier_id"]

    return {
        "items": list(items.values()),
        "suppliers": list(suppliers.values()),
        "summary": {
            "total_items": total_items,
            "totals": totals_list,
            "suggested_supplier_id": suggested_supplier_id,
            "suggestion_reason": suggestion_reason,
            "sla": {
                "rfq_created_at": rfq_created_at,
                "sla_start_at": sla_start_at or rfq_created_at,
                "sla_start_label": sla_start_label,
                "sla_source_start": sla_source_start,
                "sla_source_last_quote": sla_source_last_quote if last_quote_at else "none",
                "rfq_age_days": rfq_age_days,
                "last_quote_at": last_quote_at,
                "last_quote_days_ago": last_quote_days,
                "sla_limit_days": sla_limit_days,
                "sla_risk": sla_risk,
            },
        },
    }


def _parse_risk_flags(raw_value: str | None) -> dict:
    if not raw_value:
        return DEFAULT_RISK_FLAGS
    try:
        parsed = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        return DEFAULT_RISK_FLAGS
    if isinstance(parsed, dict):
        return parsed
    return DEFAULT_RISK_FLAGS


def _sla_threshold_days() -> int:
    try:
        raw = current_app.config.get("RFQ_SLA_DAYS", 5)
        value = int(raw)
    except (TypeError, ValueError):
        value = 5
    return max(0, value)


def _format_datetime(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _combine_erp_datetime(date_value: str | None, time_value: str | None) -> str | None:
    date_value = str(date_value).strip() if date_value is not None else ""
    time_value = str(time_value).strip() if time_value is not None else ""
    if not date_value and not time_value:
        return None

    if time_value and time_value.isdigit() and len(time_value) in (3, 4):
        padded = time_value.zfill(4)
        time_value = f"{padded[:2]}:{padded[2:]}"

    if date_value and time_value:
        combined = _parse_datetime(f"{date_value} {time_value}")
    elif date_value:
        combined = _parse_datetime(date_value)
    else:
        combined = None

    if not combined:
        return None
    return _format_datetime(combined)


def _days_since(raw_value: str | None, now: datetime) -> int | None:
    dt = _parse_datetime(raw_value)
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now - dt.astimezone(timezone.utc)
    return max(0, int(delta.total_seconds() // 86_400))


def _parse_datetime(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    if value.startswith("1900-12-31"):
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.fromisoformat(value)
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _load_latest_award_for_rfq(db, tenant_id: str | None, rfq_id: int) -> dict | None:
    clause, params = _tenant_clause(tenant_id)
    row = db.execute(
        f"""
        SELECT id, rfq_id, supplier_name, status, reason, purchase_order_id, created_at, updated_at
        FROM awards
        WHERE rfq_id = ? AND {clause}
        ORDER BY id DESC
        LIMIT 1
        """,
        (rfq_id, *params),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "rfq_id": row["rfq_id"],
        "fornecedor": row["supplier_name"],
        "status": row["status"],
        "motivo": row["reason"],
        "purchase_order_id": row["purchase_order_id"],
        "criada_em": row["created_at"],
        "atualizada_em": row["updated_at"],
    }


def _load_award(db, tenant_id: str | None, award_id: int):
    clause, params = _tenant_clause(tenant_id)
    sql = f"""
        SELECT id, rfq_id, supplier_name, status, reason, purchase_order_id, tenant_id
        FROM awards
        WHERE id = ? AND {clause}
    """
    row = db.execute(sql, (award_id, *params)).fetchone()
    return row


def _load_purchase_order(db, tenant_id: str | None, purchase_order_id: int):
    clause, params = _tenant_clause(tenant_id)
    sql = f"""
        SELECT id, number, award_id, supplier_name, status, currency, total_amount, external_id, erp_last_error,
               created_at, updated_at, tenant_id
        FROM purchase_orders
        WHERE id = ? AND {clause}
    """
    row = db.execute(sql, (purchase_order_id, *params)).fetchone()
    return row


def _serialize_purchase_order(row) -> dict:
    return {
        "id": row["id"],
        "numero": row["number"],
        "decisao_id": row["award_id"],
        "fornecedor": row["supplier_name"],
        "status": row["status"],
        "moeda": row["currency"],
        "total": row["total_amount"],
        "external_id": row["external_id"],
        "erp_erro": row["erp_last_error"],
        "criada_em": row["created_at"],
        "atualizada_em": row["updated_at"],
    }


def _get_or_create_quote(db, rfq_id: int, supplier_id: int, tenant_id: str) -> int:
    row = db.execute(
        """
        SELECT id
        FROM quotes
        WHERE rfq_id = ? AND supplier_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (rfq_id, supplier_id, tenant_id),
    ).fetchone()
    if row:
        return int(row["id"])

    cursor = db.execute(
        """
        INSERT INTO quotes (rfq_id, supplier_id, status, currency, tenant_id)
        VALUES (?, ?, 'submitted', 'BRL', ?)
        RETURNING id
        """,
        (rfq_id, supplier_id, tenant_id),
    )
    row = cursor.fetchone()
    return int(row["id"] if isinstance(row, dict) else row[0])

def _load_inbox_cards(db, tenant_id: str | None) -> dict:
    tenant_filter, tenant_params = _tenant_filter(tenant_id, occurrences=4)
    sql = f"""
        SELECT
            (
                SELECT COUNT(*)
                FROM purchase_requests
                WHERE status = 'pending_rfq' AND {tenant_filter}
            ) AS pending_rfq,
            (
                SELECT COUNT(*)
                FROM rfqs
                WHERE status IN ('open','collecting_quotes') AND {tenant_filter}
            ) AS awaiting_quotes,
            (
                SELECT COUNT(*)
                FROM rfqs
                WHERE status = 'awarded'
                  AND {tenant_filter}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM awards a
                      WHERE a.rfq_id = rfqs.id
                        AND a.tenant_id = rfqs.tenant_id
                        AND a.purchase_order_id IS NOT NULL
                  )
            ) AS awarded_waiting_po,
            (
                SELECT COUNT(*)
                FROM purchase_orders
                WHERE status IN ('draft','approved','sent_to_erp','erp_error') AND {tenant_filter}
            ) AS awaiting_erp_push
    """
    row = db.execute(sql, tuple(tenant_params)).fetchone()
    return {
        "pending_rfq": row["pending_rfq"] if row else 0,
        "awaiting_quotes": row["awaiting_quotes"] if row else 0,
        "awarded_waiting_po": row["awarded_waiting_po"] if row else 0,
        "awaiting_erp_push": row["awaiting_erp_push"] if row else 0,
    }


def _load_inbox_items(
    db,
    tenant_id: str | None,
    limit: int,
    offset: int,
    filters: Dict[str, str],
) -> List[dict]:
    tenant_filter, tenant_params = _tenant_filter(tenant_id, occurrences=6)

    outer_conditions: List[str] = []
    outer_params: List[object] = []

    type_filter = filters.get("type")
    if type_filter:
        outer_conditions.append("type = ?")
        outer_params.append(type_filter)

    status_filter = filters.get("status")
    if status_filter:
        statuses = [value for value in status_filter.split(",") if value]
        if len(statuses) == 1:
            outer_conditions.append("status = ?")
            outer_params.append(statuses[0])
        else:
            placeholders = ",".join(["?"] * len(statuses))
            outer_conditions.append(f"status IN ({placeholders})")
            outer_params.extend(statuses)

    priority_filter = filters.get("priority")
    if priority_filter:
        outer_conditions.append("priority = ?")
        outer_params.append(priority_filter)

    search_filter = filters.get("search")
    if search_filter:
        outer_conditions.append("ref LIKE ?")
        outer_params.append(f"%{search_filter}%")

    awaiting_po_filter = filters.get("awaiting_po")
    if awaiting_po_filter:
        outer_conditions.append(
            "(type = 'rfq' AND status = 'awarded' AND award_id IS NOT NULL AND award_purchase_order_id IS NULL)"
        )

    outer_where = ""
    if outer_conditions:
        outer_where = "WHERE " + " AND ".join(outer_conditions)

    age_pr_expr = _age_days_expr(db, "created_at")
    age_rfq_expr = _age_days_expr(db, "r.created_at")
    age_po_expr = _age_days_expr(db, "created_at")

    sql = f"""
        WITH pr_pending AS (
            SELECT
                id,
                'purchase_request' AS type,
                number AS ref,
                status,
                priority,
                needed_at,
                updated_at,
                {age_pr_expr} AS age_days,
                NULL AS award_id,
                NULL AS award_status,
                NULL AS award_purchase_order_id
            FROM purchase_requests
            WHERE status IN ('pending_rfq','in_rfq') AND {tenant_filter}
        ),
        rfq_open AS (
            SELECT
                r.id,
                'rfq' AS type,
                COALESCE(r.title, CAST(r.id AS TEXT)) AS ref,
                r.status,
                NULL AS priority,
                NULL AS needed_at,
                r.updated_at,
                {age_rfq_expr} AS age_days,
                (
                    SELECT a.id
                    FROM awards a
                    WHERE a.rfq_id = r.id AND {tenant_filter}
                    ORDER BY a.id DESC
                    LIMIT 1
                ) AS award_id,
                (
                    SELECT a.status
                    FROM awards a
                    WHERE a.rfq_id = r.id AND {tenant_filter}
                    ORDER BY a.id DESC
                    LIMIT 1
                ) AS award_status,
                (
                    SELECT a.purchase_order_id
                    FROM awards a
                    WHERE a.rfq_id = r.id AND {tenant_filter}
                    ORDER BY a.id DESC
                    LIMIT 1
                ) AS award_purchase_order_id
            FROM rfqs r
            WHERE r.status IN ('open','collecting_quotes','awarded') AND {tenant_filter}
        ),
        po_pending_push AS (
            SELECT
                id,
                'purchase_order' AS type,
                COALESCE(number, CAST(id AS TEXT)) AS ref,
                status,
                NULL AS priority,
                NULL AS needed_at,
                updated_at,
                {age_po_expr} AS age_days,
                NULL AS award_id,
                NULL AS award_status,
                NULL AS award_purchase_order_id
            FROM purchase_orders
            WHERE status IN ('draft','approved','sent_to_erp','erp_error','erp_accepted') AND {tenant_filter}
        ),
        inbox_union AS (
            SELECT * FROM pr_pending
            UNION ALL
            SELECT * FROM rfq_open
            UNION ALL
            SELECT * FROM po_pending_push
        )
        SELECT *
        FROM inbox_union
        {outer_where}
        ORDER BY
            CASE priority
                WHEN 'urgent' THEN 1
                WHEN 'high' THEN 2
                WHEN 'medium' THEN 3
                WHEN 'low' THEN 4
                ELSE 5
            END,
            needed_at IS NULL,
            needed_at,
            updated_at DESC
        LIMIT ? OFFSET ?
    """

    params: List[object] = [*tenant_params, *outer_params, limit, offset]
    rows = db.execute(sql, tuple(params)).fetchall()

    items: List[dict] = []
    for row in rows:
        item_type = row["type"]
        item_status = row["status"]
        stage = "solicitacao"
        meta = {"allowed_actions": [], "primary_action": None}
        if item_type == "purchase_request":
            stage = stage_for_purchase_request_status(item_status)
            meta = flow_meta("solicitacao", item_status)
        elif item_type == "rfq":
            stage = stage_for_rfq_status(item_status)
            meta = flow_meta("cotacao", item_status)
        elif item_type == "purchase_order":
            stage = stage_for_purchase_order_status(item_status)
            meta = flow_meta("ordem_compra", item_status)

        items.append(
            {
                "type": item_type,
                "id": row["id"],
                "ref": row["ref"],
                "status": item_status,
                "priority": row["priority"],
                "needed_at": row["needed_at"],
                "age_days": row["age_days"],
                "updated_at": row["updated_at"],
                "award_id": row["award_id"],
                "award_status": row["award_status"],
                "award_purchase_order_id": row["award_purchase_order_id"],
                "process_stage": stage,
                "allowed_actions": meta["allowed_actions"],
                "primary_action": meta["primary_action"],
            }
        )
    return items


def _parse_inbox_filters(args) -> Dict[str, str]:
    filters: Dict[str, str] = {}

    type_value = (args.get("type") or "").strip()
    if type_value in ALLOWED_TYPES:
        filters["type"] = type_value

    status_value = (args.get("status") or "").strip()
    if status_value:
        raw_statuses = [value.strip().lower() for value in status_value.split(",") if value.strip()]
        allowed_statuses = [value for value in raw_statuses if value in ALLOWED_INBOX_STATUSES]
        if allowed_statuses:
            filters["status"] = ",".join(allowed_statuses)[:120]

    priority_value = (args.get("priority") or "").strip()
    if priority_value in ALLOWED_PRIORITIES:
        filters["priority"] = priority_value

    search_value = (args.get("search") or "").strip()
    if search_value:
        filters["search"] = search_value[:80]

    awaiting_po_value = (args.get("awaiting_po") or "").strip().lower()
    if awaiting_po_value in {"1", "true", "yes", "sim"}:
        filters["awaiting_po"] = "1"

    return filters


def _tenant_clause(tenant_id: str | None, alias: str | None = None) -> Tuple[str, List[str]]:
    effective_tenant_id = scoped_tenant_id(tenant_id)
    prefix = f"{alias}." if alias else ""
    return f"{prefix}tenant_id = ?", [effective_tenant_id]


def _tenant_filter(tenant_id: str | None, occurrences: int) -> Tuple[str, List[str]]:
    effective_tenant_id = scoped_tenant_id(tenant_id)
    return "tenant_id = ?", [effective_tenant_id] * occurrences


def _parse_int(value: str | None, default: int, min_value: int, max_value: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return max(min_value, min(parsed, max_value))


def _age_days_expr(db, column: str) -> str:
    if getattr(db, "backend", "sqlite") == "postgres":
        return f"CAST(GREATEST(0, DATE_PART('day', CURRENT_TIMESTAMP - {column})) AS INTEGER)"
    return f"CAST(MAX(0, julianday('now') - julianday({column})) AS INTEGER)"


def _normalize_po_status(value: str | None) -> str:
    if not value:
        return "erp_accepted"
    normalized = value.strip().lower()
    if normalized in ALLOWED_PO_STATUSES:
        return normalized
    if normalized in {"queued", "processing", "sent"}:
        return "sent_to_erp"
    if normalized in {"accepted", "approved", "ok", "success"}:
        return "erp_accepted"
    if normalized in {"error", "failed", "rejected"}:
        return "erp_error"
    return "erp_accepted"


def _normalize_erp_key(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _extract_erp_field(record: dict, keys: Tuple[str, ...]) -> str | None:
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        value_str = str(value).strip()
        if value_str:
            return value_str

    normalized_record = {_normalize_erp_key(k): v for k, v in record.items()}
    for key in keys:
        normalized = _normalize_erp_key(key)
        if normalized in normalized_record:
            value = normalized_record.get(normalized)
            if value is None:
                continue
            value_str = str(value).strip()
            if value_str:
                return value_str
    return None


def _start_sync_run(
    db,
    tenant_id: str,
    scope: str,
    attempt: int = 1,
    parent_sync_run_id: int | None = None,
) -> int:
    cursor = db.execute(
        """
        INSERT INTO sync_runs (system, scope, status, attempt, parent_sync_run_id, started_at, tenant_id)
        VALUES ('senior', ?, 'running', ?, ?, CURRENT_TIMESTAMP, ?)
        RETURNING id
        """,
        (scope, attempt, parent_sync_run_id, tenant_id),
    )
    row = cursor.fetchone()
    return int(row["id"] if isinstance(row, dict) else row[0])


def _finish_sync_run(
    db,
    tenant_id: str,
    sync_run_id: int,
    status: str,
    records_in: int,
    records_upserted: int,
) -> None:
    duration_expr = "CAST((julianday(CURRENT_TIMESTAMP) - julianday(started_at)) * 86400000 AS INTEGER)"
    if getattr(db, "backend", "sqlite") == "postgres":
        duration_expr = "CAST(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)) * 1000 AS INTEGER)"
    db.execute(
        f"""
        UPDATE sync_runs
        SET status = ?,
            finished_at = CURRENT_TIMESTAMP,
            duration_ms = {duration_expr},
            records_in = ?,
            records_upserted = ?
        WHERE id = ? AND tenant_id = ?
        """,
        (status, records_in, records_upserted, sync_run_id, tenant_id),
    )


def _load_integration_watermark(db, tenant_id: str, entity: str) -> dict | None:
    row = db.execute(
        """
        SELECT last_success_source_updated_at, last_success_source_id, last_success_cursor
        FROM integration_watermarks
        WHERE tenant_id = ? AND system = 'senior' AND entity = ?
        """,
        (tenant_id, entity),
    ).fetchone()
    if not row:
        return None
    return {
        "updated_at": row["last_success_source_updated_at"],
        "source_id": row["last_success_source_id"],
        "cursor": row["last_success_cursor"],
    }


def _sync_from_erp(db, tenant_id: str, entity: str, limit: int) -> dict:
    watermark = _load_integration_watermark(db, tenant_id, entity)
    records = fetch_erp_records(
        entity,
        watermark["updated_at"] if watermark else None,
        watermark["source_id"] if watermark else None,
        limit=limit,
    )

    records_upserted = 0
    for record in records:
        if entity == "supplier":
            records_upserted += _upsert_supplier(db, tenant_id, record)
        elif entity == "purchase_request":
            records_upserted += _upsert_purchase_request(db, tenant_id, record)
        elif entity == "purchase_order":
            records_upserted += _upsert_purchase_order(db, tenant_id, record)
        elif entity == "receipt":
            records_upserted += _upsert_receipt(db, tenant_id, record)
        elif entity == "quote":
            records_upserted += _upsert_erp_supplier_quote(db, tenant_id, record)
        elif entity == "quote_process":
            records_upserted += _upsert_erp_quote_process(db, tenant_id, record)
        elif entity == "quote_supplier":
            records_upserted += _upsert_erp_quote_supplier(db, tenant_id, record)

    if records:
        last = records[-1]
        _upsert_integration_watermark(
            db,
            tenant_id,
            entity=entity,
            source_updated_at=last.get("updated_at"),
            source_id=last.get("external_id"),
            cursor=None,
        )

    return {"records_in": len(records), "records_upserted": records_upserted}


def _upsert_supplier(db, tenant_id: str, record: dict) -> int:
    external_id = record.get("external_id") or _extract_erp_field(record, ("CodFor", "cod_for", "codigo_fornecedor"))
    name = record.get("name") or _extract_erp_field(record, ("NomFor", "nome_fornecedor")) or external_id or "Fornecedor"
    tax_id = record.get("tax_id") or _extract_erp_field(record, ("CgcFor", "CnpjFor", "CpfCgc", "tax_id"))
    risk_flags = record.get("risk_flags") or DEFAULT_RISK_FLAGS
    risk_flags_json = json.dumps(risk_flags, separators=(",", ":"), ensure_ascii=True)
    updated_at = record.get("updated_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    existing = db.execute(
        """
        SELECT id
        FROM suppliers
        WHERE external_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (external_id, tenant_id),
    ).fetchone()

    if existing:
        db.execute(
            """
            UPDATE suppliers
            SET name = ?, tax_id = ?, risk_flags = ?, updated_at = ?
            WHERE id = ? AND tenant_id = ?
            """,
            (name, tax_id, risk_flags_json, updated_at, existing["id"], tenant_id),
        )
        return 1

    db.execute(
        """
        INSERT INTO suppliers (name, external_id, tax_id, risk_flags, tenant_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (name, external_id, tax_id, risk_flags_json, tenant_id, updated_at, updated_at),
    )
    return 1


def _table_exists_runtime(db, table_name: str) -> bool:
    if getattr(db, "backend", "sqlite") == "postgres":
        row = db.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ?
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()
        return bool(row)
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return bool(row)


def _column_exists_runtime(db, table_name: str, column_name: str) -> bool:
    if getattr(db, "backend", "sqlite") == "postgres":
        row = db.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ? AND column_name = ?
            LIMIT 1
            """,
            (table_name, column_name),
        ).fetchone()
        return bool(row)
    rows = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def _hydrate_purchase_requests_from_erp_raw(db, tenant_id: str | None) -> None:
    """Populate domain requests from E405SOL mirror when domain has requests without items."""
    effective_tenant_id = tenant_id or DEFAULT_TENANT_ID
    if not _table_exists_runtime(db, "e405sol"):
        return
    if not _column_exists_runtime(db, "e405sol", "numsol"):
        return

    counts_row = db.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM purchase_request_items WHERE tenant_id = ?) AS item_total
        """,
        (effective_tenant_id,),
    ).fetchone()
    item_total = int(counts_row["item_total"] or 0)
    if item_total > 0:
        return

    mirror_to_erp = {
        "numsol": "NumSol",
        "seqsol": "SeqSol",
        "qtdapr": "QtdApr",
        "qtdsol": "QtdSol",
        "unimed": "UniMed",
        "proser": "ProSer",
        "codpro": "CodPro",
        "codder": "CodDer",
        "codser": "CodSer",
        "obssol": "ObsSol",
        "datefc": "DatEfc",
        "numcot": "NumCot",
        "numpct": "NumPct",
        "coddep": "CodDep",
    }
    selected_columns = [column for column in mirror_to_erp if _column_exists_runtime(db, "e405sol", column)]
    if "numsol" not in selected_columns:
        return

    rows = db.execute(
        f"""
        SELECT {", ".join(selected_columns)}
        FROM e405sol
        WHERE tenant_id = ?
        ORDER BY numsol, COALESCE(seqsol, '0')
        LIMIT 10000
        """,
        (effective_tenant_id,),
    ).fetchall()
    if not rows:
        return

    imported = 0
    for row in rows:
        record: Dict[str, str] = {}
        for column in selected_columns:
            value = row[column]
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            record[mirror_to_erp[column]] = text
        if not record.get("NumSol"):
            continue
        _upsert_purchase_request(db, effective_tenant_id, record)
        imported += 1

    if imported > 0:
        db.commit()


def _upsert_purchase_request(db, tenant_id: str, record: dict) -> int:
    external_id = _extract_erp_field(record, ("NumSol", "num_sol", "numero_solicitacao")) or record.get("external_id")
    number = record.get("number") or _extract_erp_field(record, ("NumSol", "num_sol", "numero_solicitacao")) or external_id
    status = record.get("status") or "pending_rfq"
    priority = record.get("priority") or "medium"
    requested_by = record.get("requested_by") or _extract_erp_field(record, ("NomSol", "nom_sol", "solicitante"))
    department = record.get("department") or _extract_erp_field(record, ("CodDep", "cod_dep", "departamento"))
    needed_at = record.get("needed_at") or _extract_erp_field(record, ("DatPrv", "dat_prv", "data_prevista"))
    erp_num_cot = _extract_erp_field(record, ("num_cot", "numcot", "NumCot", "numero_cotacao", "numero_cot"))
    erp_num_pct = _extract_erp_field(record, ("num_pct", "numpct", "NumPct", "numero_processo", "numero_pct"))
    erp_sent_at = _extract_erp_field(record, ("dat_efc", "datEfc", "DatEfc", "data_envio_fornecedor", "sent_at"))
    updated_at = record.get("updated_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    if status not in ALLOWED_PR_STATUSES:
        status = "pending_rfq"
    if priority not in ALLOWED_PRIORITIES:
        priority = "medium"

    existing = db.execute(
        """
        SELECT id
        FROM purchase_requests
        WHERE external_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (external_id, tenant_id),
    ).fetchone()

    purchase_request_id: int
    if existing:
        db.execute(
            """
            UPDATE purchase_requests
            SET number = ?, status = ?, priority = ?, requested_by = ?, department = ?, needed_at = ?,
                erp_num_cot = ?, erp_num_pct = ?, erp_sent_at = ?, updated_at = ?
            WHERE id = ? AND tenant_id = ?
            """,
            (
                number,
                status,
                priority,
                requested_by,
                department,
                needed_at,
                erp_num_cot,
                erp_num_pct,
                erp_sent_at,
                updated_at,
                existing["id"],
                tenant_id,
            ),
        )
        purchase_request_id = int(existing["id"])
    else:
        cursor = db.execute(
            """
            INSERT INTO purchase_requests (
                number,
                status,
                priority,
                requested_by,
                department,
                needed_at,
                erp_num_cot,
                erp_num_pct,
                erp_sent_at,
                external_id,
                tenant_id,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (
                number,
                status,
                priority,
                requested_by,
                department,
                needed_at,
                erp_num_cot,
                erp_num_pct,
                erp_sent_at,
                external_id,
                tenant_id,
                updated_at,
                updated_at,
            ),
        )
        created = cursor.fetchone()
        purchase_request_id = int(created["id"] if isinstance(created, dict) else created[0])

    _upsert_purchase_request_item_from_record(
        db=db,
        tenant_id=tenant_id,
        purchase_request_id=purchase_request_id,
        record=record,
    )
    return 1


def _upsert_purchase_request_item_from_record(db, tenant_id: str, purchase_request_id: int, record: dict) -> None:
    line_no = _parse_optional_int(
        _extract_erp_field(
            record,
            ("SeqSol", "seq_sol", "line_no", "linha"),
        )
    )
    quantity = _parse_optional_float(
        _extract_erp_field(
            record,
            ("QtdApr", "qtd_apr", "QtdSol", "qtd_sol", "quantity", "quantidade"),
        )
    )
    if quantity is None or quantity <= 0:
        quantity = 1.0

    uom = _extract_erp_field(record, ("UniMed", "uni_med", "uom", "unidade")) or "UN"
    category = _extract_erp_field(record, ("ProSer", "pro_ser", "category", "categoria"))
    product_code = _extract_erp_field(record, ("CodPro", "cod_pro", "produto"))
    product_variant = _extract_erp_field(record, ("CodDer", "cod_der"))
    service_code = _extract_erp_field(record, ("CodSer", "cod_ser", "servico"))
    note = _extract_erp_field(record, ("ObsSol", "obs_sol", "observacao", "descricao"))

    description_parts: List[str] = []
    if product_code:
        if product_variant:
            description_parts.append(f"Produto {product_code}/{product_variant}")
        else:
            description_parts.append(f"Produto {product_code}")
    elif service_code:
        description_parts.append(f"Servico {service_code}")
    elif category:
        description_parts.append(f"Item {category}")
    else:
        description_parts.append("Item ERP")

    if note:
        clean_note = " ".join(str(note).split())
        if clean_note:
            description_parts.append(clean_note[:120])
    description = " | ".join(description_parts)

    if line_no is None or line_no <= 0:
        existing_same = db.execute(
            """
            SELECT id, line_no
            FROM purchase_request_items
            WHERE purchase_request_id = ? AND tenant_id = ? AND description = ?
            LIMIT 1
            """,
            (purchase_request_id, tenant_id, description),
        ).fetchone()
        if existing_same:
            line_no = int(existing_same["line_no"] or 1)
        else:
            next_line = db.execute(
                """
                SELECT COALESCE(MAX(line_no), 0) + 1 AS next_line
                FROM purchase_request_items
                WHERE purchase_request_id = ? AND tenant_id = ?
                """,
                (purchase_request_id, tenant_id),
            ).fetchone()
            line_no = int(next_line["next_line"] or 1)

    existing_item = db.execute(
        """
        SELECT id
        FROM purchase_request_items
        WHERE purchase_request_id = ? AND line_no = ? AND tenant_id = ?
        LIMIT 1
        """,
        (purchase_request_id, line_no, tenant_id),
    ).fetchone()
    if existing_item:
        db.execute(
            """
            UPDATE purchase_request_items
            SET description = ?, quantity = ?, uom = ?, category = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (description, quantity, uom, category, int(existing_item["id"]), tenant_id),
        )
        return

    db.execute(
        """
        INSERT INTO purchase_request_items (
            purchase_request_id, line_no, description, quantity, uom, category, tenant_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (purchase_request_id, line_no, description, quantity, uom, category, tenant_id),
    )


def _upsert_purchase_order(db, tenant_id: str, record: dict) -> int:
    external_id = record.get("external_id") or _extract_erp_field(record, ("NumOcp", "num_ocp", "numero_ocp", "numero_oc"))
    if not external_id:
        return 0

    erp_num_ocp = _extract_erp_field(record, ("NumOcp", "num_ocp", "numero_ocp", "numero_oc"))
    number = record.get("number") or erp_num_ocp or external_id
    status = record.get("status") or "draft"
    if status not in ALLOWED_PO_STATUSES:
        status = "draft"

    supplier_name = record.get("supplier_name") or _extract_erp_field(record, ("NomFor", "nom_for", "fornecedor"))
    currency = record.get("currency") or _extract_erp_field(record, ("CodMoe", "cod_moe", "moeda")) or "BRL"
    total_amount = record.get("total_amount") or _extract_erp_field(record, ("VlrOcp", "vlr_ocp", "valor_total"))
    updated_at = record.get("updated_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    existing = db.execute(
        """
        SELECT id
        FROM purchase_orders
        WHERE external_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (external_id, tenant_id),
    ).fetchone()

    if existing:
        db.execute(
            """
            UPDATE purchase_orders
            SET number = ?, status = ?, supplier_name = ?, currency = ?, total_amount = ?, updated_at = ?
            WHERE id = ? AND tenant_id = ?
            """,
            (number, status, supplier_name, currency, total_amount, updated_at, existing["id"], tenant_id),
        )
        _upsert_erp_purchase_order_items(
            db,
            tenant_id,
            erp_num_ocp=erp_num_ocp or str(number),
            items=record.get("items"),
        )
        return 1

    db.execute(
        """
        INSERT INTO purchase_orders (
            number, status, supplier_name, currency, total_amount, external_id, tenant_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (number, status, supplier_name, currency, total_amount, external_id, tenant_id, updated_at, updated_at),
    )

    _upsert_erp_purchase_order_items(
        db,
        tenant_id,
        erp_num_ocp=erp_num_ocp or str(number),
        items=record.get("items"),
    )
    return 1


def _upsert_receipt(db, tenant_id: str, record: dict) -> int:
    external_id = record.get("external_id") or _extract_erp_field(record, ("NumNfc", "num_nfc", "numero_nf"))
    if not external_id:
        return 0

    erp_num_nfc = _extract_erp_field(record, ("NumNfc", "num_nfc", "numero_nf"))
    purchase_order_external_id = record.get("purchase_order_external_id") or _extract_erp_field(
        record,
        ("NumOcp", "num_ocp", "numero_ocp", "numero_oc"),
    )
    status = record.get("status") or "received"
    if status not in ALLOWED_RECEIPT_STATUSES:
        status = "received"

    received_at = record.get("received_at") or _extract_erp_field(record, ("DatRec", "dat_rec", "data_recebimento"))
    updated_at = record.get("updated_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    purchase_order_id = None
    if purchase_order_external_id:
        row = db.execute(
            """
            SELECT id
            FROM purchase_orders
            WHERE external_id = ? AND tenant_id = ?
            LIMIT 1
            """,
            (purchase_order_external_id, tenant_id),
        ).fetchone()
        if row:
            purchase_order_id = row["id"]
            if status in {"partially_received", "received"}:
                db.execute(
                    """
                    UPDATE purchase_orders
                    SET status = ?, updated_at = ?
                    WHERE id = ? AND tenant_id = ?
                    """,
                    (status, updated_at, purchase_order_id, tenant_id),
                )

    existing = db.execute(
        """
        SELECT id
        FROM receipts
        WHERE external_id = ? AND tenant_id = ?
        LIMIT 1
        """,
        (external_id, tenant_id),
    ).fetchone()

    if existing:
        db.execute(
            """
            UPDATE receipts
            SET purchase_order_id = ?, purchase_order_external_id = ?, status = ?, received_at = ?, updated_at = ?
            WHERE id = ? AND tenant_id = ?
            """,
            (
                purchase_order_id,
                purchase_order_external_id,
                status,
                received_at,
                updated_at,
                existing["id"],
                tenant_id,
            ),
        )
        _upsert_erp_receipt_items(
            db,
            tenant_id,
            erp_num_nfc=erp_num_nfc or str(external_id),
            erp_num_ocp=purchase_order_external_id,
            items=record.get("items"),
        )
        return 1

    db.execute(
        """
        INSERT INTO receipts (
            external_id, purchase_order_id, purchase_order_external_id, status, received_at, tenant_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            external_id,
            purchase_order_id,
            purchase_order_external_id,
            status,
            received_at,
            tenant_id,
            updated_at,
            updated_at,
        ),
    )

    _upsert_erp_receipt_items(
        db,
        tenant_id,
        erp_num_nfc=erp_num_nfc or str(external_id),
        erp_num_ocp=purchase_order_external_id,
        items=record.get("items"),
    )
    return 1


def _parse_optional_int(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _normalize_int_list(value) -> List[int]:
    if not isinstance(value, list):
        return []
    result: List[int] = []
    for item in value:
        parsed = _parse_optional_int(item)
        if parsed is None:
            continue
        result.append(parsed)
    # Preserva ordem e remove duplicidade.
    return list(dict.fromkeys(result))


def _parse_csv_values(value: str | None) -> List[str]:
    if not value:
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _parse_optional_float(value) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _upsert_erp_purchase_order_items(db, tenant_id: str, erp_num_ocp: str, items: object) -> None:
    if not erp_num_ocp or not isinstance(items, list):
        return
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        line_no = _parse_optional_int(item.get("line_no")) or idx
        product_code = item.get("product_code") or item.get("CodPro")
        description = item.get("description") or item.get("DesPro")
        quantity = _parse_optional_float(item.get("quantity") or item.get("QtdPed") or item.get("QtdOcp"))
        unit_price = _parse_optional_float(item.get("unit_price") or item.get("PreUni") or item.get("VlrUni"))
        total_price = _parse_optional_float(item.get("total_price") or item.get("VlrTot"))
        source_table = str(item.get("source_table") or "E420IPO")
        external_id = item.get("external_id")

        db.execute(
            """
            INSERT INTO erp_purchase_order_items (
                tenant_id,
                erp_num_ocp,
                line_no,
                product_code,
                description,
                quantity,
                unit_price,
                total_price,
                source_table,
                external_id,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(tenant_id, erp_num_ocp, line_no, source_table) DO UPDATE SET
                product_code = excluded.product_code,
                description = excluded.description,
                quantity = excluded.quantity,
                unit_price = excluded.unit_price,
                total_price = excluded.total_price,
                external_id = excluded.external_id,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                tenant_id,
                str(erp_num_ocp),
                line_no,
                product_code,
                description,
                quantity,
                unit_price,
                total_price,
                source_table,
                external_id,
            ),
        )


def _upsert_erp_receipt_items(db, tenant_id: str, erp_num_nfc: str, erp_num_ocp: str | None, items: object) -> None:
    if not erp_num_nfc or not isinstance(items, list):
        return
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        line_no = _parse_optional_int(item.get("line_no")) or idx
        product_code = item.get("product_code") or item.get("CodPro")
        quantity_received = _parse_optional_float(item.get("quantity_received") or item.get("QtdRec") or item.get("QtdEnt"))
        source_table = str(item.get("source_table") or "E440IPC")
        external_id = item.get("external_id")
        item_erp_num_ocp = item.get("NumOcp") or erp_num_ocp

        db.execute(
            """
            INSERT INTO erp_receipt_items (
                tenant_id,
                erp_num_nfc,
                erp_num_ocp,
                line_no,
                product_code,
                quantity_received,
                source_table,
                external_id,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(tenant_id, erp_num_nfc, line_no, source_table) DO UPDATE SET
                erp_num_ocp = excluded.erp_num_ocp,
                product_code = excluded.product_code,
                quantity_received = excluded.quantity_received,
                external_id = excluded.external_id,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                tenant_id,
                str(erp_num_nfc),
                item_erp_num_ocp,
                line_no,
                product_code,
                quantity_received,
                source_table,
                external_id,
            ),
        )


def _upsert_erp_supplier_quote(db, tenant_id: str, record: dict) -> int:
    erp_num_cot = _extract_erp_field(record, ("num_cot", "numcot", "NumCot"))
    erp_num_pct = _extract_erp_field(record, ("num_pct", "numpct", "NumPct"))
    supplier_external_id = _extract_erp_field(
        record,
        ("cod_for", "codfor", "CodFor", "supplier_external_id", "supplier_id"),
    )
    quote_date = _extract_erp_field(record, ("dat_cot", "datcot", "DatCot", "data_cotacao"))
    quote_time = _extract_erp_field(record, ("hor_cot", "horcot", "HorCot", "hora_cotacao"))
    source_table = record.get("source_table") or "E410COT"
    external_id = record.get("external_id") or _extract_erp_field(record, ("id", "codigo"))
    quote_datetime = _combine_erp_datetime(quote_date, quote_time)
    updated_at = record.get("updated_at") or quote_datetime or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    if not (erp_num_cot or erp_num_pct):
        return 0

    db.execute(
        """
        INSERT INTO erp_supplier_quotes (
            tenant_id,
            erp_num_cot,
            erp_num_pct,
            supplier_external_id,
            quote_date,
            quote_time,
            quote_datetime,
            source_table,
            external_id,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(
            tenant_id,
            erp_num_cot,
            erp_num_pct,
            supplier_external_id,
            quote_datetime,
            source_table
        ) DO UPDATE SET
            quote_date = excluded.quote_date,
            quote_time = excluded.quote_time,
            quote_datetime = excluded.quote_datetime,
            external_id = excluded.external_id,
            updated_at = excluded.updated_at
        """,
        (
            tenant_id,
            erp_num_cot,
            erp_num_pct,
            supplier_external_id,
            quote_date,
            quote_time,
            quote_datetime,
            source_table,
            external_id,
            updated_at,
        ),
    )
    return 1


def _upsert_erp_quote_process(db, tenant_id: str, record: dict) -> int:
    erp_num_pct = _extract_erp_field(record, ("NumPct", "num_pct", "numpct"))
    if not erp_num_pct:
        return 0
    opened_at = _extract_erp_field(record, ("DatAbe", "dat_abe", "data_abertura"))
    opened_at = _combine_erp_datetime(opened_at, None)
    external_id = record.get("external_id") or _extract_erp_field(record, ("id", "codigo"))
    updated_at = record.get("updated_at") or opened_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    db.execute(
        """
        INSERT INTO erp_quote_processes (
            tenant_id,
            erp_num_pct,
            opened_at,
            source_table,
            external_id,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, 'E410PCT', ?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(tenant_id, erp_num_pct, source_table) DO UPDATE SET
            opened_at = excluded.opened_at,
            external_id = excluded.external_id,
            updated_at = excluded.updated_at
        """,
        (tenant_id, erp_num_pct, opened_at, external_id, updated_at),
    )
    return 1


def _upsert_erp_quote_supplier(db, tenant_id: str, record: dict) -> int:
    erp_num_pct = _extract_erp_field(record, ("NumPct", "num_pct", "numpct"))
    supplier_external_id = _extract_erp_field(record, ("CodFor", "cod_for", "codfor"))
    if not erp_num_pct or not supplier_external_id:
        return 0
    external_id = record.get("external_id") or _extract_erp_field(record, ("id", "codigo"))
    updated_at = record.get("updated_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    db.execute(
        """
        INSERT INTO erp_quote_suppliers (
            tenant_id,
            erp_num_pct,
            supplier_external_id,
            source_table,
            external_id,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, 'E410FPC', ?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(tenant_id, erp_num_pct, supplier_external_id, source_table) DO UPDATE SET
            external_id = excluded.external_id,
            updated_at = excluded.updated_at
        """,
        (tenant_id, erp_num_pct, supplier_external_id, external_id, updated_at),
    )
    return 1


def _upsert_integration_watermark(
    db,
    tenant_id: str,
    entity: str,
    source_updated_at: str | None,
    source_id: str | None,
    cursor: str | None = None,
) -> None:
    if not source_updated_at:
        source_updated_at = (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

    db.execute(
        """
        INSERT INTO integration_watermarks (
            tenant_id,
            system,
            entity,
            last_success_source_updated_at,
            last_success_source_id,
            last_success_cursor,
            last_success_at
        ) VALUES (?, 'senior', ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(tenant_id, system, entity) DO UPDATE SET
            last_success_source_updated_at = excluded.last_success_source_updated_at,
            last_success_source_id = excluded.last_success_source_id,
            last_success_cursor = excluded.last_success_cursor,
            last_success_at = excluded.last_success_at
        """,
        (tenant_id, entity, source_updated_at, source_id, cursor),
    )


def _load_status_events(db, tenant_id: str | None, entity: str, entity_id: int, limit: int = 60) -> List[dict]:
    clause, params = _tenant_clause(tenant_id)
    rows = db.execute(
        f"""
        SELECT id, entity, entity_id, from_status, to_status, reason, occurred_at
        FROM status_events
        WHERE entity = ? AND entity_id = ? AND {clause}
        ORDER BY occurred_at DESC, id DESC
        LIMIT ?
        """,
        (entity, entity_id, *params, limit),
    ).fetchall()

    return [
        {
            "id": row["id"],
            "entity": row["entity"],
            "entity_id": row["entity_id"],
            "from_status": row["from_status"],
            "to_status": row["to_status"],
            "reason": row["reason"],
            "occurred_at": row["occurred_at"],
        }
        for row in rows
    ]


def _load_recent_status_events(db, tenant_id: str | None, limit: int = 80) -> List[dict]:
    clause, params = _tenant_clause(tenant_id)
    rows = db.execute(
        f"""
        SELECT id, entity, entity_id, from_status, to_status, reason, occurred_at
        FROM status_events
        WHERE {clause}
        ORDER BY occurred_at DESC, id DESC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()

    return [
        {
            "id": row["id"],
            "entity": row["entity"],
            "entity_id": row["entity_id"],
            "from_status": row["from_status"],
            "to_status": row["to_status"],
            "reason": row["reason"],
            "occurred_at": row["occurred_at"],
        }
        for row in rows
    ]


def _load_sync_runs(db, tenant_id: str | None, scope: str | None, limit: int = 50) -> List[dict]:
    clause, params = _tenant_clause(tenant_id)

    scope_clause = ""
    scope_params: List[object] = []
    if scope:
        aliases = SCOPE_ALIASES.get(scope, (scope,))
        placeholders = ",".join(["?"] * len(aliases))
        scope_clause = f"AND scope IN ({placeholders})"
        scope_params.extend(aliases)

    rows = db.execute(
        f"""
        SELECT id, scope, status, attempt, started_at, finished_at, duration_ms,
               records_in, records_upserted, records_failed, error_summary
        FROM sync_runs
        WHERE {clause} {scope_clause}
        ORDER BY started_at DESC, id DESC
        LIMIT ?
        """,
        (*params, *scope_params, limit),
    ).fetchall()

    return [
        {
            "id": row["id"],
            "scope": row["scope"],
            "status": row["status"],
            "attempt": row["attempt"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "duration_ms": row["duration_ms"],
            "records_in": row["records_in"],
            "records_upserted": row["records_upserted"],
            "records_failed": row["records_failed"],
            "error_summary": row["error_summary"],
        }
        for row in rows
    ]






