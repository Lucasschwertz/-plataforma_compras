
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from flask import Blueprint, current_app, jsonify, render_template, request

from app.db import get_db, get_read_db
from app.erp_client import DEFAULT_RISK_FLAGS, ErpError, fetch_erp_records, push_purchase_order
from app.tenant import DEFAULT_TENANT_ID, current_tenant_id


procurement_bp = Blueprint("procurement", __name__)


ALLOWED_TYPES = {"purchase_request", "rfq", "purchase_order"}
ALLOWED_PRIORITIES = {"low", "medium", "high", "urgent"}
ALLOWED_PR_STATUSES = {
    "pending_rfq",
    "in_rfq",
    "awarded",
    "ordered",
    "partially_received",
    "received",
    "cancelled",
}
ALLOWED_PO_STATUSES = {
    "draft",
    "approved",
    "sent_to_erp",
    "erp_accepted",
    "partially_received",
    "received",
    "cancelled",
    "erp_error",
}
ALLOWED_RECEIPT_STATUSES = {"pending", "partially_received", "received"}
ALLOWED_RFQ_STATUSES = {"draft", "open", "collecting_quotes", "closed", "awarded", "cancelled"}
ALLOWED_INBOX_STATUSES = ALLOWED_PR_STATUSES | ALLOWED_PO_STATUSES | ALLOWED_RFQ_STATUSES

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
    return render_template("procurement_inbox.html", tenant_id=tenant_id)


@procurement_bp.route("/procurement/cotacoes/<int:rfq_id>", methods=["GET"])
def cotacao_detail_page(rfq_id: int):
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template("procurement_cotacao.html", tenant_id=tenant_id, rfq_id=rfq_id)


@procurement_bp.route("/procurement/purchase-orders/<int:purchase_order_id>", methods=["GET"])
def purchase_order_detail_page(purchase_order_id: int):
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template(
        "procurement_purchase_order.html",
        tenant_id=tenant_id,
        purchase_order_id=purchase_order_id,
    )


@procurement_bp.route("/procurement/integrations/logs", methods=["GET"])
def integration_logs_page():
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template("procurement_integration_logs.html", tenant_id=tenant_id)


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


@procurement_bp.route("/api/procurement/purchase-requests/open", methods=["GET"])
def purchase_requests_open():
    db = get_read_db()
    tenant_id = current_tenant_id()
    limit = _parse_int(request.args.get("limit"), default=80, min_value=1, max_value=200)
    items = _load_open_purchase_requests(db, tenant_id, limit)
    return jsonify({"items": items})


@procurement_bp.route("/api/procurement/purchase-request-items/open", methods=["GET"])
def purchase_request_items_open():
    db = get_read_db()
    tenant_id = current_tenant_id()
    limit = _parse_int(request.args.get("limit"), default=120, min_value=1, max_value=300)
    items = _load_open_purchase_request_items(db, tenant_id, limit)
    return jsonify({"items": items})


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
            jsonify({"error": "rfq_not_found", "message": "Cotacao nao encontrada.", "rfq_id": rfq_id}),
            404,
        )

    award = _load_latest_award_for_rfq(db, tenant_id, rfq_id)
    purchase_order = None
    if award and award.get("purchase_order_id"):
        purchase_order = _load_purchase_order(db, tenant_id, int(award["purchase_order_id"]))

    itens = _load_rfq_items_with_quotes(db, tenant_id, rfq_id)
    events = _load_status_events(db, tenant_id, entity="rfq", entity_id=rfq_id, limit=80)
    award_events: List[dict] = []
    if award:
        award_events = _load_status_events(db, tenant_id, entity="award", entity_id=int(award["id"]), limit=40)

    return jsonify(
        {
            "cotacao": {
                "id": rfq["id"],
                "titulo": rfq["title"],
                "status": rfq["status"],
                "criada_em": rfq["created_at"],
                "atualizada_em": rfq["updated_at"],
            },
            "itens": itens,
            "decisao": award,
            "ordem_compra": _serialize_purchase_order(purchase_order) if purchase_order else None,
            "eventos_cotacao": events,
            "eventos_decisao": award_events,
        }
    )

@procurement_bp.route("/api/procurement/cotacoes/<int:rfq_id>/itens/<int:rfq_item_id>/fornecedores", methods=["POST"])
def cotacao_item_fornecedores_api(rfq_id: int, rfq_item_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}

    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": "Cotacao nao encontrada.", "rfq_id": rfq_id}), 404

    rfq_item = _load_rfq_item(db, tenant_id, rfq_item_id)
    if not rfq_item or int(rfq_item["rfq_id"]) != rfq_id:
        return (
            jsonify(
                {
                    "error": "rfq_item_not_found",
                    "message": "Item da cotacao nao encontrado.",
                    "rfq_item_id": rfq_item_id,
                }
            ),
            404,
        )

    supplier_ids = payload.get("supplier_ids") or []
    if not isinstance(supplier_ids, list) or not supplier_ids:
        return jsonify({"error": "supplier_ids_required", "message": "Informe supplier_ids."}), 400

    valid_supplier_ids = {s["id"] for s in _load_suppliers(db, tenant_id)}

    for supplier_id in supplier_ids:
        if supplier_id not in valid_supplier_ids:
            continue
        db.execute(
            """
            INSERT INTO rfq_item_suppliers (rfq_item_id, supplier_id, tenant_id)
            VALUES (?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            (rfq_item_id, supplier_id, tenant_id),
        )

    db.commit()
    itens = _load_rfq_items_with_quotes(db, tenant_id, rfq_id)
    return jsonify({"itens": itens})


@procurement_bp.route("/api/procurement/cotacoes/<int:rfq_id>/propostas", methods=["POST"])
def cotacao_propostas_api(rfq_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}

    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": "Cotacao nao encontrada.", "rfq_id": rfq_id}), 404

    supplier_id = payload.get("supplier_id")
    items = payload.get("items") or []

    if not supplier_id:
        return jsonify({"error": "supplier_id_required", "message": "Informe supplier_id."}), 400
    if not isinstance(items, list) or not items:
        return jsonify({"error": "items_required", "message": "Informe items."}), 400

    valid_supplier_ids = {s["id"] for s in _load_suppliers(db, tenant_id)}
    if supplier_id not in valid_supplier_ids:
        return jsonify({"error": "supplier_not_found", "message": "Fornecedor nao encontrado."}), 404

    quote_id = _get_or_create_quote(db, rfq_id, supplier_id, tenant_id)

    for item in items:
        rfq_item_id = item.get("rfq_item_id")
        unit_price = item.get("unit_price")
        lead_time_days = item.get("lead_time_days")

        if not rfq_item_id or unit_price is None:
            continue

        rfq_item = _load_rfq_item(db, tenant_id, int(rfq_item_id))
        if not rfq_item or int(rfq_item["rfq_id"]) != rfq_id:
            continue

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
            INSERT INTO quote_items (quote_id, rfq_item_id, unit_price, lead_time_days, tenant_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (quote_id, rfq_item_id, tenant_id) DO UPDATE SET
                unit_price = excluded.unit_price,
                lead_time_days = excluded.lead_time_days,
                updated_at = CURRENT_TIMESTAMP
            """,
            (quote_id, rfq_item_id, float(unit_price), lead_time_days, tenant_id),
        )

    db.commit()
    itens = _load_rfq_items_with_quotes(db, tenant_id, rfq_id)
    return jsonify({"itens": itens, "quote_id": quote_id})


@procurement_bp.route("/api/procurement/purchase-orders/<int:purchase_order_id>", methods=["GET"])
def purchase_order_detail_api(purchase_order_id: int):
    db = get_read_db()
    tenant_id = current_tenant_id()

    po = _load_purchase_order(db, tenant_id, purchase_order_id)
    if not po:
        return (
            jsonify(
                {
                    "error": "purchase_order_not_found",
                    "message": "Ordem de compra nao encontrada.",
                    "purchase_order_id": purchase_order_id,
                }
            ),
            404,
        )

    events = _load_status_events(db, tenant_id, entity="purchase_order", entity_id=purchase_order_id)
    sync_runs = _load_sync_runs(db, tenant_id, scope="purchase_order", limit=20)

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
            },
            "events": events,
            "sync_runs": sync_runs,
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


@procurement_bp.route("/api/procurement/integrations/sync", methods=["POST"])
def integration_sync_api():
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}

    scope_value = (payload.get("scope") or request.args.get("scope") or "").strip()
    if not scope_value:
        return jsonify({"error": "scope_required", "message": "Informe scope."}), 400

    canonical_scope = SCOPE_ALIASES.get(scope_value, (scope_value,))[0]
    if canonical_scope not in SYNC_SUPPORTED_SCOPES:
        return (
            jsonify(
                {
                    "error": "scope_not_supported",
                    "message": "Scope nao suportado neste MVP.",
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
        return (
            jsonify({"error": "sync_failed", "message": "Falha ao sincronizar.", "details": str(exc)[:200]}),
            500,
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
    clause, params = _tenant_clause(tenant_id)

    rows = db.execute(
        f"""
        SELECT id, title, status, updated_at
        FROM rfqs
        WHERE {clause}
        ORDER BY updated_at DESC, id DESC
        LIMIT 100
        """,
        tuple(params),
    ).fetchall()

    items = [
        {
            "id": row["id"],
            "title": row["title"],
            "status": row["status"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]
    return jsonify({"items": items})


@procurement_bp.route("/api/procurement/rfqs", methods=["POST"])
def create_rfq():
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    payload = request.get_json(silent=True) or {}

    item_ids_raw = payload.get("purchase_request_item_ids") or []
    if not isinstance(item_ids_raw, list):
        return (
            jsonify(
                {
                    "error": "purchase_request_item_ids_invalid",
                    "message": "purchase_request_item_ids deve ser lista.",
                }
            ),
            400,
        )

    item_ids: List[int] = []
    for item in item_ids_raw:
        try:
            item_ids.append(int(item))
        except (TypeError, ValueError):
            continue
    item_ids = list(dict.fromkeys(item_ids))

    if not item_ids:
        return (
            jsonify(
                {
                    "error": "purchase_request_item_ids_required",
                    "message": "Selecione ao menos um item de solicitacao.",
                }
            ),
            400,
        )

    request_items = _load_purchase_request_items_by_ids(db, tenant_id, item_ids)
    if not request_items:
        return (
            jsonify(
                {
                    "error": "purchase_request_items_not_found",
                    "message": "Itens de solicitacao nao encontrados ou ja em cotacao.",
                }
            ),
            400,
        )

    title = (payload.get("title") or "Nova Cotacao").strip()
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

    db.commit()
    return (
        jsonify(
            {
                "id": rfq_id,
                "status": status,
                "title": title,
                "created_at": created_row["created_at"] if created_row else None,
                "rfq_items": rfq_items_payload,
            }
        ),
        201,
    )


@procurement_bp.route("/api/procurement/rfqs/<int:rfq_id>/comparison", methods=["GET"])
def rfq_comparison(rfq_id: int):
    db = get_read_db()
    tenant_id = current_tenant_id()

    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return (
            jsonify({"error": "rfq_not_found", "message": "Cotacao nao encontrada.", "rfq_id": rfq_id}),
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

    rfq = _load_rfq(db, tenant_id, rfq_id)
    if not rfq:
        return (
            jsonify({"error": "rfq_not_found", "message": "Cotacao nao encontrada.", "rfq_id": rfq_id}),
            404,
        )

    reason = (payload.get("reason") or "").strip()
    if not reason:
        return jsonify({"error": "reason_required", "message": "Motivo da decisao e obrigatorio."}), 400

    supplier_name = (payload.get("supplier_name") or "Fornecedor selecionado").strip()

    cursor = db.execute(
        """
        INSERT INTO awards (rfq_id, supplier_name, status, reason, tenant_id)
        VALUES (?, ?, 'awarded', ?, ?)
        RETURNING id
        """,
        (rfq_id, supplier_name, reason, tenant_id),
    )
    award_row = cursor.fetchone()
    award_id = award_row["id"] if isinstance(award_row, dict) else award_row[0]

    db.execute(
        "UPDATE rfqs SET status = 'awarded' WHERE id = ? AND tenant_id = ?",
        (rfq_id, tenant_id),
    )

    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
        VALUES ('rfq', ?, ?, 'awarded', 'rfq_awarded', ?)
        """,
        (rfq_id, rfq["status"], tenant_id),
    )
    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
        VALUES ('award', ?, NULL, 'awarded', ?, ?)
        """,
        (award_id, reason, tenant_id),
    )

    db.commit()
    return jsonify({"award_id": award_id, "rfq_id": rfq_id, "status": "awarded"}), 201

@procurement_bp.route("/api/procurement/awards/<int:award_id>/purchase-orders", methods=["POST"])
def create_purchase_order_from_award(award_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID

    award = _load_award(db, tenant_id, award_id)
    if not award:
        return jsonify({"error": "award_not_found", "message": "Decisao nao encontrada.", "award_id": award_id}), 404

    if award["purchase_order_id"]:
        return (
            jsonify(
                {
                    "error": "purchase_order_already_exists",
                    "message": "Esta decisao ja possui uma ordem de compra.",
                    "purchase_order_id": award["purchase_order_id"],
                }
            ),
            409,
        )

    po_number = f"OC-{award_id:04d}"
    supplier_name = award["supplier_name"] or "Fornecedor selecionado"

    cursor = db.execute(
        """
        INSERT INTO purchase_orders (number, award_id, supplier_name, status, total_amount, tenant_id)
        VALUES (?, ?, ?, 'approved', ?, ?)
        RETURNING id
        """,
        (po_number, award_id, supplier_name, 0.0, tenant_id),
    )
    po_row = cursor.fetchone()
    purchase_order_id = po_row["id"] if isinstance(po_row, dict) else po_row[0]

    db.execute(
        "UPDATE awards SET status = 'converted_to_po', purchase_order_id = ? WHERE id = ? AND tenant_id = ?",
        (purchase_order_id, award_id, tenant_id),
    )

    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
        VALUES ('award', ?, ?, 'converted_to_po', 'award_converted_to_po', ?)
        """,
        (award_id, award["status"], tenant_id),
    )
    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
        VALUES ('purchase_order', ?, NULL, 'approved', 'po_created_from_award', ?)
        """,
        (purchase_order_id, tenant_id),
    )

    db.commit()
    return jsonify({"purchase_order_id": purchase_order_id, "status": "approved"}), 201


@procurement_bp.route("/api/procurement/purchase-orders/<int:purchase_order_id>/push-to-erp", methods=["POST"])
def push_purchase_order_to_erp(purchase_order_id: int):
    db = get_db()
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID

    po = _load_purchase_order(db, tenant_id, purchase_order_id)
    if not po:
        return (
            jsonify(
                {
                    "error": "purchase_order_not_found",
                    "message": "Ordem de compra nao encontrada.",
                    "purchase_order_id": purchase_order_id,
                }
            ),
            404,
        )

    if po["status"] == "erp_accepted":
        return jsonify(
            {
                "status": "erp_accepted",
                "external_id": po["external_id"],
                "message": "Ordem ja aceita no ERP.",
            }
        )

    sync_run_id = _start_sync_run(db, tenant_id, scope="purchase_order")

    db.execute(
        "UPDATE purchase_orders SET status = 'sent_to_erp' WHERE id = ? AND tenant_id = ?",
        (purchase_order_id, tenant_id),
    )
    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
        VALUES ('purchase_order', ?, ?, 'sent_to_erp', 'po_push_started', ?)
        """,
        (purchase_order_id, po["status"], tenant_id),
    )

    try:
        result = push_purchase_order(dict(po))
    except ErpError as exc:
        error_message = str(exc)[:200]
        db.execute(
            """
            UPDATE purchase_orders
            SET status = 'erp_error', erp_last_error = ?
            WHERE id = ? AND tenant_id = ?
            """,
            (error_message, purchase_order_id, tenant_id),
        )
        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('purchase_order', ?, 'sent_to_erp', 'erp_error', 'po_push_failed', ?)
            """,
            (purchase_order_id, tenant_id),
        )
        _finish_sync_run(db, tenant_id, sync_run_id, status="failed", records_in=0, records_upserted=0)
        db.commit()
        return (
            jsonify({"error": "erp_push_failed", "message": "Falha ao enviar ao ERP.", "details": error_message}),
            500,
        )

    external_id = result.get("external_id")
    resolved_status = _normalize_po_status(result.get("status"))
    reason = "po_push_succeeded" if resolved_status != "sent_to_erp" else "po_push_queued"

    db.execute(
        """
        UPDATE purchase_orders
        SET status = ?, external_id = ?, erp_last_error = NULL
        WHERE id = ? AND tenant_id = ?
        """,
        (resolved_status, external_id, purchase_order_id, tenant_id),
    )
    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
        VALUES ('purchase_order', ?, 'sent_to_erp', ?, ?, ?)
        """,
        (purchase_order_id, resolved_status, reason, tenant_id),
    )

    _finish_sync_run(db, tenant_id, sync_run_id, status="succeeded", records_in=1, records_upserted=1)
    _upsert_integration_watermark(
        db,
        tenant_id,
        entity="purchase_order",
        source_updated_at=None,
        source_id=external_id,
    )

    db.commit()
    return jsonify(
        {
            "purchase_order_id": purchase_order_id,
            "status": resolved_status,
            "external_id": external_id,
            "sync_run_id": sync_run_id,
            "message": result.get("message") or "Ordem enviada ao ERP.",
        }
    )


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
        SELECT id, name, external_id, tax_id, tenant_id
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

    demo_names = ["Fornecedor Atlas", "Fornecedor Nexo", "Fornecedor Prisma"]
    for name in demo_names:
        db.execute(
            "INSERT INTO suppliers (name, tenant_id) VALUES (?, ?)",
            (name, tenant_id),
        )


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
            max_cover = max(t["items_quoted"] for t in totals_list)
            candidates = [t for t in totals_list if t["items_quoted"] == max_cover]
            suggestion_reason = "maior cobertura, menor total"

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
        items.append(
            {
                "type": row["type"],
                "id": row["id"],
                "ref": row["ref"],
                "status": row["status"],
                "priority": row["priority"],
                "needed_at": row["needed_at"],
                "age_days": row["age_days"],
                "updated_at": row["updated_at"],
                "award_id": row["award_id"],
                "award_status": row["award_status"],
                "award_purchase_order_id": row["award_purchase_order_id"],
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


def _tenant_clause(tenant_id: str | None) -> Tuple[str, List[str]]:
    effective_tenant_id = tenant_id or DEFAULT_TENANT_ID
    return "tenant_id = ?", [effective_tenant_id]


def _tenant_filter(tenant_id: str | None, occurrences: int) -> Tuple[str, List[str]]:
    effective_tenant_id = tenant_id or DEFAULT_TENANT_ID
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


def _upsert_purchase_request(db, tenant_id: str, record: dict) -> int:
    external_id = record.get("external_id") or _extract_erp_field(record, ("NumSol", "num_sol", "numero_solicitacao"))
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
        return 1

    db.execute(
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
    return 1


def _upsert_purchase_order(db, tenant_id: str, record: dict) -> int:
    external_id = record.get("external_id") or _extract_erp_field(record, ("NumOcp", "num_ocp", "numero_ocp", "numero_oc"))
    if not external_id:
        return 0

    number = record.get("number") or _extract_erp_field(record, ("NumOcp", "num_ocp", "numero_ocp", "numero_oc")) or external_id
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
        return 1

    db.execute(
        """
        INSERT INTO purchase_orders (
            number, status, supplier_name, currency, total_amount, external_id, tenant_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (number, status, supplier_name, currency, total_amount, external_id, tenant_id, updated_at, updated_at),
    )
    return 1


def _upsert_receipt(db, tenant_id: str, record: dict) -> int:
    external_id = record.get("external_id") or _extract_erp_field(record, ("NumNfc", "num_nfc", "numero_nf"))
    if not external_id:
        return 0

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
    return 1


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






