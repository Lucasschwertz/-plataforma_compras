
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from flask import Blueprint, jsonify, render_template, request

from app.db import get_db
from app.erp_mock import DEFAULT_RISK_FLAGS, fetch_erp_records
from app.tenant import current_company_id


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
}

SYNC_SUPPORTED_SCOPES = {"supplier", "purchase_request"}


@procurement_bp.route("/procurement/inbox", methods=["GET"])
def procurement_inbox_page():
    company_id = current_company_id() or 1
    return render_template("procurement_inbox.html", company_id=company_id)


@procurement_bp.route("/procurement/cotacoes/<int:rfq_id>", methods=["GET"])
def cotacao_detail_page(rfq_id: int):
    company_id = current_company_id() or 1
    return render_template("procurement_cotacao.html", company_id=company_id, rfq_id=rfq_id)


@procurement_bp.route("/procurement/purchase-orders/<int:purchase_order_id>", methods=["GET"])
def purchase_order_detail_page(purchase_order_id: int):
    company_id = current_company_id() or 1
    return render_template(
        "procurement_purchase_order.html",
        company_id=company_id,
        purchase_order_id=purchase_order_id,
    )


@procurement_bp.route("/procurement/integrations/logs", methods=["GET"])
def integration_logs_page():
    company_id = current_company_id() or 1
    return render_template("procurement_integration_logs.html", company_id=company_id)


@procurement_bp.route("/api/procurement/inbox", methods=["GET"])
def procurement_inbox():
    db = get_db()
    company_id = current_company_id()

    limit = _parse_int(request.args.get("limit"), default=50, min_value=1, max_value=200)
    offset = _parse_int(request.args.get("offset"), default=0, min_value=0, max_value=10_000)
    filters = _parse_inbox_filters(request.args)

    cards = _load_inbox_cards(db, company_id)
    items = _load_inbox_items(db, company_id, limit, offset, filters)

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


@procurement_bp.route("/api/procurement/fornecedores", methods=["GET"])
def fornecedores_api():
    db = get_db()
    company_id = current_company_id()
    suppliers = _load_suppliers(db, company_id)
    return jsonify({"items": suppliers})


@procurement_bp.route("/api/procurement/cotacoes/<int:rfq_id>", methods=["GET"])
def cotacao_detail_api(rfq_id: int):
    db = get_db()
    company_id = current_company_id()

    rfq = _load_rfq(db, company_id, rfq_id)
    if not rfq:
        return (
            jsonify({"error": "rfq_not_found", "message": "Cotacao nao encontrada.", "rfq_id": rfq_id}),
            404,
        )

    award = _load_latest_award_for_rfq(db, company_id, rfq_id)
    purchase_order = None
    if award and award.get("purchase_order_id"):
        purchase_order = _load_purchase_order(db, company_id, int(award["purchase_order_id"]))

    itens = _load_rfq_items_with_quotes(db, company_id, rfq_id)
    events = _load_status_events(db, company_id, entity="rfq", entity_id=rfq_id, limit=80)
    award_events: List[dict] = []
    if award:
        award_events = _load_status_events(db, company_id, entity="award", entity_id=int(award["id"]), limit=40)

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
    company_id = current_company_id() or 1
    payload = request.get_json(silent=True) or {}

    rfq = _load_rfq(db, company_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": "Cotacao nao encontrada.", "rfq_id": rfq_id}), 404

    rfq_item = _load_rfq_item(db, company_id, rfq_item_id)
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

    valid_supplier_ids = {s["id"] for s in _load_suppliers(db, company_id)}

    for supplier_id in supplier_ids:
        if supplier_id not in valid_supplier_ids:
            continue
        db.execute(
            """
            INSERT OR IGNORE INTO rfq_item_suppliers (rfq_item_id, supplier_id, company_id)
            VALUES (?, ?, ?)
            """,
            (rfq_item_id, supplier_id, company_id),
        )

    db.commit()
    itens = _load_rfq_items_with_quotes(db, company_id, rfq_id)
    return jsonify({"itens": itens})


@procurement_bp.route("/api/procurement/cotacoes/<int:rfq_id>/propostas", methods=["POST"])
def cotacao_propostas_api(rfq_id: int):
    db = get_db()
    company_id = current_company_id() or 1
    payload = request.get_json(silent=True) or {}

    rfq = _load_rfq(db, company_id, rfq_id)
    if not rfq:
        return jsonify({"error": "rfq_not_found", "message": "Cotacao nao encontrada.", "rfq_id": rfq_id}), 404

    supplier_id = payload.get("supplier_id")
    items = payload.get("items") or []

    if not supplier_id:
        return jsonify({"error": "supplier_id_required", "message": "Informe supplier_id."}), 400
    if not isinstance(items, list) or not items:
        return jsonify({"error": "items_required", "message": "Informe items."}), 400

    valid_supplier_ids = {s["id"] for s in _load_suppliers(db, company_id)}
    if supplier_id not in valid_supplier_ids:
        return jsonify({"error": "supplier_not_found", "message": "Fornecedor nao encontrado."}), 404

    quote_id = _get_or_create_quote(db, rfq_id, supplier_id, company_id)

    for item in items:
        rfq_item_id = item.get("rfq_item_id")
        unit_price = item.get("unit_price")
        lead_time_days = item.get("lead_time_days")

        if not rfq_item_id or unit_price is None:
            continue

        rfq_item = _load_rfq_item(db, company_id, int(rfq_item_id))
        if not rfq_item or int(rfq_item["rfq_id"]) != rfq_id:
            continue

        db.execute(
            """
            INSERT OR IGNORE INTO rfq_item_suppliers (rfq_item_id, supplier_id, company_id)
            VALUES (?, ?, ?)
            """,
            (rfq_item_id, supplier_id, company_id),
        )

        db.execute(
            """
            INSERT OR REPLACE INTO quote_items (quote_id, rfq_item_id, unit_price, lead_time_days, company_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (quote_id, rfq_item_id, float(unit_price), lead_time_days, company_id),
        )

    db.commit()
    itens = _load_rfq_items_with_quotes(db, company_id, rfq_id)
    return jsonify({"itens": itens, "quote_id": quote_id})


@procurement_bp.route("/api/procurement/purchase-orders/<int:purchase_order_id>", methods=["GET"])
def purchase_order_detail_api(purchase_order_id: int):
    db = get_db()
    company_id = current_company_id()

    po = _load_purchase_order(db, company_id, purchase_order_id)
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

    events = _load_status_events(db, company_id, entity="purchase_order", entity_id=purchase_order_id)
    sync_runs = _load_sync_runs(db, company_id, scope="purchase_order", limit=20)

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
    db = get_db()
    company_id = current_company_id()

    scope = (request.args.get("scope") or "").strip() or None
    limit = _parse_int(request.args.get("limit"), default=50, min_value=1, max_value=200)

    sync_runs = _load_sync_runs(db, company_id, scope=scope, limit=limit)
    recent_events = _load_recent_status_events(db, company_id, limit=80)

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
    company_id = current_company_id() or 1
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

    sync_run_id = _start_sync_run(db, company_id, scope=canonical_scope)

    try:
        result = _sync_from_erp(db, company_id, canonical_scope, limit=limit)
        _finish_sync_run(
            db,
            company_id,
            sync_run_id,
            status="succeeded",
            records_in=result["records_in"],
            records_upserted=result["records_upserted"],
        )
        db.commit()
    except Exception as exc:  # noqa: BLE001 - MVP: loga erro resumido
        _finish_sync_run(db, company_id, sync_run_id, status="failed", records_in=0, records_upserted=0)
        db.execute(
            """
            UPDATE sync_runs
            SET error_summary = ?
            WHERE id = ? AND company_id = ?
            """,
            (str(exc)[:200], sync_run_id, company_id),
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
    company_id = current_company_id() or 1

    db.execute(
        "INSERT OR IGNORE INTO empresas (id, nome, subdomain) VALUES (?, ?, ?)",
        (company_id, f"Empresa {company_id}", f"empresa-{company_id}"),
    )

    existing = db.execute(
        "SELECT COUNT(*) AS total FROM purchase_requests WHERE company_id = ?",
        (company_id,),
    ).fetchone()["total"]
    if existing:
        cards = _load_inbox_cards(db, company_id)
        return jsonify(
            {
                "seeded": False,
                "company_id": company_id,
                "kpis": cards,
                "hint": "Seed ja aplicado. Use GET /api/procurement/inbox",
            }
        )

    db.execute(
        """
        INSERT INTO purchase_requests (number, status, priority, requested_by, department, needed_at, company_id)
        VALUES (?, ?, ?, ?, ?, date('now', '+3 day'), ?)
        """,
        ("SR-1001", "pending_rfq", "high", "Joao", "Manutencao", company_id),
    )
    db.execute(
        """
        INSERT INTO purchase_requests (number, status, priority, requested_by, department, needed_at, company_id)
        VALUES (?, ?, ?, ?, ?, date('now', '+1 day'), ?)
        """,
        ("SR-1002", "in_rfq", "urgent", "Maria", "Operacoes", company_id),
    )

    db.execute(
        "INSERT INTO rfqs (title, status, company_id) VALUES (?, ?, ?)",
        ("Cotacao - Rolamentos", "collecting_quotes", company_id),
    )
    db.execute(
        "INSERT INTO rfqs (title, status, company_id) VALUES (?, ?, ?)",
        ("Cotacao - EPIs", "awarded", company_id),
    )

    db.execute(
        """
        INSERT INTO purchase_orders (number, status, company_id, supplier_name, total_amount)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("OC-2001", "approved", company_id, "Fornecedor A", 12450.90),
    )
    db.execute(
        """
        INSERT INTO purchase_orders (number, status, company_id, supplier_name, erp_last_error, total_amount)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("OC-2002", "erp_error", company_id, "Fornecedor B", "Fornecedor sem codigo no ERP", 9870.00),
    )

    db.commit()

    cards = _load_inbox_cards(db, company_id)
    return jsonify(
        {
            "seeded": True,
            "company_id": company_id,
            "kpis": cards,
            "hint": "Agora use GET /api/procurement/inbox",
        }
    )


@procurement_bp.route("/api/procurement/rfqs", methods=["GET"])
def list_rfqs():
    db = get_db()
    company_id = current_company_id()
    clause, params = _company_clause(company_id)

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
    company_id = current_company_id() or 1
    payload = request.get_json(silent=True) or {}

    title = (payload.get("title") or "Nova Cotacao").strip()
    status = "open"

    cursor = db.execute(
        "INSERT INTO rfqs (title, status, company_id) VALUES (?, ?, ?)",
        (title, status, company_id),
    )
    rfq_id = cursor.lastrowid

    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, company_id)
        VALUES ('rfq', ?, NULL, ?, 'rfq_created', ?)
        """,
        (rfq_id, status, company_id),
    )

    db.commit()
    return jsonify({"id": rfq_id, "status": status, "title": title}), 201


@procurement_bp.route("/api/procurement/rfqs/<int:rfq_id>/comparison", methods=["GET"])
def rfq_comparison(rfq_id: int):
    db = get_db()
    company_id = current_company_id()

    rfq = _load_rfq(db, company_id, rfq_id)
    if not rfq:
        return (
            jsonify({"error": "rfq_not_found", "message": "Cotacao nao encontrada.", "rfq_id": rfq_id}),
            404,
        )

    return jsonify(
        {
            "rfq_id": rfq["id"],
            "rfq": {
                "title": rfq["title"],
                "status": rfq["status"],
                "updated_at": rfq["updated_at"],
            },
            "items": [],
            "suppliers": [],
            "suggested_supplier_id": None,
            "suggestion_reason": "comparacao_indisponivel_no_mvp",
        }
    )


@procurement_bp.route("/api/procurement/rfqs/<int:rfq_id>/award", methods=["POST"])
def rfq_award(rfq_id: int):
    db = get_db()
    company_id = current_company_id() or 1
    payload = request.get_json(silent=True) or {}

    rfq = _load_rfq(db, company_id, rfq_id)
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
        INSERT INTO awards (rfq_id, supplier_name, status, reason, company_id)
        VALUES (?, ?, 'awarded', ?, ?)
        """,
        (rfq_id, supplier_name, reason, company_id),
    )
    award_id = cursor.lastrowid

    db.execute(
        "UPDATE rfqs SET status = 'awarded' WHERE id = ? AND company_id = ?",
        (rfq_id, company_id),
    )

    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, company_id)
        VALUES ('rfq', ?, ?, 'awarded', 'rfq_awarded', ?)
        """,
        (rfq_id, rfq["status"], company_id),
    )
    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, company_id)
        VALUES ('award', ?, NULL, 'awarded', ?, ?)
        """,
        (award_id, reason, company_id),
    )

    db.commit()
    return jsonify({"award_id": award_id, "rfq_id": rfq_id, "status": "awarded"}), 201

@procurement_bp.route("/api/procurement/awards/<int:award_id>/purchase-orders", methods=["POST"])
def create_purchase_order_from_award(award_id: int):
    db = get_db()
    company_id = current_company_id() or 1

    award = _load_award(db, company_id, award_id)
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
        INSERT INTO purchase_orders (number, award_id, supplier_name, status, total_amount, company_id)
        VALUES (?, ?, ?, 'approved', ?, ?)
        """,
        (po_number, award_id, supplier_name, 0.0, company_id),
    )
    purchase_order_id = cursor.lastrowid

    db.execute(
        "UPDATE awards SET status = 'converted_to_po', purchase_order_id = ? WHERE id = ? AND company_id = ?",
        (purchase_order_id, award_id, company_id),
    )

    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, company_id)
        VALUES ('award', ?, ?, 'converted_to_po', 'award_converted_to_po', ?)
        """,
        (award_id, award["status"], company_id),
    )
    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, company_id)
        VALUES ('purchase_order', ?, NULL, 'approved', 'po_created_from_award', ?)
        """,
        (purchase_order_id, company_id),
    )

    db.commit()
    return jsonify({"purchase_order_id": purchase_order_id, "status": "approved"}), 201


@procurement_bp.route("/api/procurement/purchase-orders/<int:purchase_order_id>/push-to-erp", methods=["POST"])
def push_purchase_order_to_erp(purchase_order_id: int):
    db = get_db()
    company_id = current_company_id() or 1

    po = _load_purchase_order(db, company_id, purchase_order_id)
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

    sync_run_id = _start_sync_run(db, company_id, scope="purchase_order")

    db.execute(
        "UPDATE purchase_orders SET status = 'sent_to_erp' WHERE id = ? AND company_id = ?",
        (purchase_order_id, company_id),
    )
    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, company_id)
        VALUES ('purchase_order', ?, ?, 'sent_to_erp', 'po_push_started', ?)
        """,
        (purchase_order_id, po["status"], company_id),
    )

    external_id = f"SENIOR-OC-{purchase_order_id:06d}"
    db.execute(
        """
        UPDATE purchase_orders
        SET status = 'erp_accepted', external_id = ?, erp_last_error = NULL
        WHERE id = ? AND company_id = ?
        """,
        (external_id, purchase_order_id, company_id),
    )
    db.execute(
        """
        INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, company_id)
        VALUES ('purchase_order', ?, 'sent_to_erp', 'erp_accepted', 'po_push_succeeded', ?)
        """,
        (purchase_order_id, company_id),
    )

    _finish_sync_run(db, company_id, sync_run_id, status="succeeded", records_in=1, records_upserted=1)
    _upsert_integration_watermark(
        db,
        company_id,
        entity="purchase_order",
        source_updated_at=None,
        source_id=external_id,
    )

    db.commit()
    return jsonify(
        {
            "purchase_order_id": purchase_order_id,
            "status": "erp_accepted",
            "external_id": external_id,
            "sync_run_id": sync_run_id,
            "message": "Ordem enviada e aceita no ERP (simulado).",
        }
    )


def _load_suppliers(db, company_id: int | None) -> List[dict]:
    clause, params = _company_clause(company_id)
    rows = db.execute(
        f"""
        SELECT id, name, external_id, tax_id, company_id
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


def _load_rfq(db, company_id: int | None, rfq_id: int):
    clause, params = _company_clause(company_id)
    sql = f"SELECT id, title, status, created_at, updated_at, company_id FROM rfqs WHERE id = ? AND {clause}"
    row = db.execute(sql, (rfq_id, *params)).fetchone()
    return row


def _load_rfq_item(db, company_id: int | None, rfq_item_id: int):
    clause, params = _company_clause(company_id)
    row = db.execute(
        f"""
        SELECT id, rfq_id, description, quantity, uom, company_id, created_at, updated_at
        FROM rfq_items
        WHERE id = ? AND {clause}
        LIMIT 1
        """,
        (rfq_item_id, *params),
    ).fetchone()
    return row


def _load_rfq_items_with_quotes(db, company_id: int | None, rfq_id: int) -> List[dict]:
    company_filter, company_params = _company_filter(company_id, occurrences=5)

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
            ON ris.rfq_item_id = ri.id AND {company_filter}
        LEFT JOIN suppliers s
            ON s.id = ris.supplier_id
        LEFT JOIN quotes q
            ON q.rfq_id = ri.rfq_id AND q.supplier_id = s.id AND {company_filter}
        LEFT JOIN quote_items qi
            ON qi.quote_id = q.id AND qi.rfq_item_id = ri.id AND {company_filter}
        WHERE ri.rfq_id = ? AND {company_filter}
        ORDER BY ri.id, s.name
        """,
        (*company_params, *company_params, *company_params, rfq_id, *company_params),
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


def _load_latest_award_for_rfq(db, company_id: int | None, rfq_id: int) -> dict | None:
    clause, params = _company_clause(company_id)
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


def _load_award(db, company_id: int | None, award_id: int):
    clause, params = _company_clause(company_id)
    sql = f"""
        SELECT id, rfq_id, supplier_name, status, reason, purchase_order_id, company_id
        FROM awards
        WHERE id = ? AND {clause}
    """
    row = db.execute(sql, (award_id, *params)).fetchone()
    return row


def _load_purchase_order(db, company_id: int | None, purchase_order_id: int):
    clause, params = _company_clause(company_id)
    sql = f"""
        SELECT id, number, award_id, supplier_name, status, currency, total_amount, external_id, erp_last_error,
               created_at, updated_at, company_id
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


def _get_or_create_quote(db, rfq_id: int, supplier_id: int, company_id: int | None) -> int:
    row = db.execute(
        """
        SELECT id
        FROM quotes
        WHERE rfq_id = ? AND supplier_id = ? AND (company_id IS NULL OR company_id = ?)
        LIMIT 1
        """,
        (rfq_id, supplier_id, company_id),
    ).fetchone()
    if row:
        return int(row["id"])

    cursor = db.execute(
        """
        INSERT INTO quotes (rfq_id, supplier_id, status, currency, company_id)
        VALUES (?, ?, 'submitted', 'BRL', ?)
        """,
        (rfq_id, supplier_id, company_id),
    )
    return int(cursor.lastrowid)

def _load_inbox_cards(db, company_id: int | None) -> dict:
    company_filter, company_params = _company_filter(company_id, occurrences=4)
    sql = f"""
        SELECT
            (
                SELECT COUNT(*)
                FROM purchase_requests
                WHERE status = 'pending_rfq' AND {company_filter}
            ) AS pending_rfq,
            (
                SELECT COUNT(*)
                FROM rfqs
                WHERE status IN ('open','collecting_quotes') AND {company_filter}
            ) AS awaiting_quotes,
            (
                SELECT COUNT(*)
                FROM rfqs
                WHERE status = 'awarded' AND {company_filter}
            ) AS awarded_waiting_po,
            (
                SELECT COUNT(*)
                FROM purchase_orders
                WHERE status IN ('draft','approved','sent_to_erp','erp_error') AND {company_filter}
            ) AS awaiting_erp_push
    """
    row = db.execute(sql, tuple(company_params)).fetchone()
    return {
        "pending_rfq": row["pending_rfq"] if row else 0,
        "awaiting_quotes": row["awaiting_quotes"] if row else 0,
        "awarded_waiting_po": row["awarded_waiting_po"] if row else 0,
        "awaiting_erp_push": row["awaiting_erp_push"] if row else 0,
    }


def _load_inbox_items(
    db,
    company_id: int | None,
    limit: int,
    offset: int,
    filters: Dict[str, str],
) -> List[dict]:
    company_filter, company_params = _company_filter(company_id, occurrences=6)

    outer_conditions: List[str] = []
    outer_params: List[object] = []

    type_filter = filters.get("type")
    if type_filter:
        outer_conditions.append("type = ?")
        outer_params.append(type_filter)

    status_filter = filters.get("status")
    if status_filter:
        outer_conditions.append("status = ?")
        outer_params.append(status_filter)

    priority_filter = filters.get("priority")
    if priority_filter:
        outer_conditions.append("priority = ?")
        outer_params.append(priority_filter)

    search_filter = filters.get("search")
    if search_filter:
        outer_conditions.append("ref LIKE ?")
        outer_params.append(f"%{search_filter}%")

    outer_where = ""
    if outer_conditions:
        outer_where = "WHERE " + " AND ".join(outer_conditions)

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
                CAST(MAX(0, julianday('now') - julianday(created_at)) AS INTEGER) AS age_days,
                NULL AS award_id,
                NULL AS award_status,
                NULL AS award_purchase_order_id
            FROM purchase_requests
            WHERE status IN ('pending_rfq','in_rfq') AND {company_filter}
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
                CAST(MAX(0, julianday('now') - julianday(r.created_at)) AS INTEGER) AS age_days,
                (
                    SELECT a.id
                    FROM awards a
                    WHERE a.rfq_id = r.id AND {company_filter}
                    ORDER BY a.id DESC
                    LIMIT 1
                ) AS award_id,
                (
                    SELECT a.status
                    FROM awards a
                    WHERE a.rfq_id = r.id AND {company_filter}
                    ORDER BY a.id DESC
                    LIMIT 1
                ) AS award_status,
                (
                    SELECT a.purchase_order_id
                    FROM awards a
                    WHERE a.rfq_id = r.id AND {company_filter}
                    ORDER BY a.id DESC
                    LIMIT 1
                ) AS award_purchase_order_id
            FROM rfqs r
            WHERE r.status IN ('open','collecting_quotes','awarded') AND {company_filter}
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
                CAST(MAX(0, julianday('now') - julianday(created_at)) AS INTEGER) AS age_days,
                NULL AS award_id,
                NULL AS award_status,
                NULL AS award_purchase_order_id
            FROM purchase_orders
            WHERE status IN ('draft','approved','sent_to_erp','erp_error','erp_accepted') AND {company_filter}
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

    params: List[object] = [*company_params, *outer_params, limit, offset]
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
        filters["status"] = status_value[:40]

    priority_value = (args.get("priority") or "").strip()
    if priority_value in ALLOWED_PRIORITIES:
        filters["priority"] = priority_value

    search_value = (args.get("search") or "").strip()
    if search_value:
        filters["search"] = search_value[:80]

    return filters


def _company_clause(company_id: int | None) -> Tuple[str, List[int]]:
    if not company_id:
        return "1=1", []
    return "(company_id IS NULL OR company_id = ?)", [company_id]


def _company_filter(company_id: int | None, occurrences: int) -> Tuple[str, List[int]]:
    if not company_id:
        return "1=1", []
    return "(company_id IS NULL OR company_id = ?)", [company_id] * occurrences


def _parse_int(value: str | None, default: int, min_value: int, max_value: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return max(min_value, min(parsed, max_value))


def _start_sync_run(db, company_id: int, scope: str) -> int:
    cursor = db.execute(
        """
        INSERT INTO sync_runs (system, scope, status, started_at, company_id)
        VALUES ('senior', ?, 'running', CURRENT_TIMESTAMP, ?)
        """,
        (scope, company_id),
    )
    return cursor.lastrowid


def _finish_sync_run(
    db,
    company_id: int,
    sync_run_id: int,
    status: str,
    records_in: int,
    records_upserted: int,
) -> None:
    db.execute(
        """
        UPDATE sync_runs
        SET status = ?,
            finished_at = CURRENT_TIMESTAMP,
            duration_ms = CAST((julianday(CURRENT_TIMESTAMP) - julianday(started_at)) * 86400000 AS INTEGER),
            records_in = ?,
            records_upserted = ?
        WHERE id = ? AND company_id = ?
        """,
        (status, records_in, records_upserted, sync_run_id, company_id),
    )


def _load_integration_watermark(db, company_id: int, entity: str) -> dict | None:
    row = db.execute(
        """
        SELECT last_success_source_updated_at, last_success_source_id, last_success_cursor
        FROM integration_watermarks
        WHERE company_id = ? AND system = 'senior' AND entity = ?
        """,
        (company_id, entity),
    ).fetchone()
    if not row:
        return None
    return {
        "updated_at": row["last_success_source_updated_at"],
        "source_id": row["last_success_source_id"],
        "cursor": row["last_success_cursor"],
    }


def _sync_from_erp(db, company_id: int, entity: str, limit: int) -> dict:
    watermark = _load_integration_watermark(db, company_id, entity)
    records = fetch_erp_records(
        entity,
        watermark["updated_at"] if watermark else None,
        watermark["source_id"] if watermark else None,
        limit=limit,
    )

    records_upserted = 0
    for record in records:
        if entity == "supplier":
            records_upserted += _upsert_supplier(db, company_id, record)
        elif entity == "purchase_request":
            records_upserted += _upsert_purchase_request(db, company_id, record)

    if records:
        last = records[-1]
        _upsert_integration_watermark(
            db,
            company_id,
            entity=entity,
            source_updated_at=last.get("updated_at"),
            source_id=last.get("external_id"),
            cursor=None,
        )

    return {"records_in": len(records), "records_upserted": records_upserted}


def _upsert_supplier(db, company_id: int, record: dict) -> int:
    external_id = record.get("external_id")
    name = record.get("name") or external_id or "Fornecedor"
    tax_id = record.get("tax_id")
    risk_flags = record.get("risk_flags") or DEFAULT_RISK_FLAGS
    risk_flags_json = json.dumps(risk_flags, separators=(",", ":"), ensure_ascii=True)
    updated_at = record.get("updated_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    existing = db.execute(
        """
        SELECT id
        FROM suppliers
        WHERE external_id = ? AND (company_id IS NULL OR company_id = ?)
        LIMIT 1
        """,
        (external_id, company_id),
    ).fetchone()

    if existing:
        db.execute(
            """
            UPDATE suppliers
            SET name = ?, tax_id = ?, risk_flags = ?, updated_at = ?
            WHERE id = ? AND (company_id IS NULL OR company_id = ?)
            """,
            (name, tax_id, risk_flags_json, updated_at, existing["id"], company_id),
        )
        return 1

    db.execute(
        """
        INSERT INTO suppliers (name, external_id, tax_id, risk_flags, company_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (name, external_id, tax_id, risk_flags_json, company_id, updated_at, updated_at),
    )
    return 1


def _upsert_purchase_request(db, company_id: int, record: dict) -> int:
    external_id = record.get("external_id")
    number = record.get("number") or external_id
    status = record.get("status") or "pending_rfq"
    priority = record.get("priority") or "medium"
    requested_by = record.get("requested_by")
    department = record.get("department")
    needed_at = record.get("needed_at")
    updated_at = record.get("updated_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    if status not in ALLOWED_PR_STATUSES:
        status = "pending_rfq"
    if priority not in ALLOWED_PRIORITIES:
        priority = "medium"

    existing = db.execute(
        """
        SELECT id
        FROM purchase_requests
        WHERE external_id = ? AND (company_id IS NULL OR company_id = ?)
        LIMIT 1
        """,
        (external_id, company_id),
    ).fetchone()

    if existing:
        db.execute(
            """
            UPDATE purchase_requests
            SET number = ?, status = ?, priority = ?, requested_by = ?, department = ?, needed_at = ?, updated_at = ?
            WHERE id = ? AND (company_id IS NULL OR company_id = ?)
            """,
            (number, status, priority, requested_by, department, needed_at, updated_at, existing["id"], company_id),
        )
        return 1

    db.execute(
        """
        INSERT INTO purchase_requests (
            number, status, priority, requested_by, department, needed_at, external_id, company_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (number, status, priority, requested_by, department, needed_at, external_id, company_id, updated_at, updated_at),
    )
    return 1


def _upsert_integration_watermark(
    db,
    company_id: int,
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
            company_id,
            system,
            entity,
            last_success_source_updated_at,
            last_success_source_id,
            last_success_cursor,
            last_success_at
        ) VALUES (?, 'senior', ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(company_id, system, entity) DO UPDATE SET
            last_success_source_updated_at = excluded.last_success_source_updated_at,
            last_success_source_id = excluded.last_success_source_id,
            last_success_cursor = excluded.last_success_cursor,
            last_success_at = excluded.last_success_at
        """,
        (company_id, entity, source_updated_at, source_id, cursor),
    )


def _load_status_events(db, company_id: int | None, entity: str, entity_id: int, limit: int = 60) -> List[dict]:
    clause, params = _company_clause(company_id)
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


def _load_recent_status_events(db, company_id: int | None, limit: int = 80) -> List[dict]:
    clause, params = _company_clause(company_id)
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


def _load_sync_runs(db, company_id: int | None, scope: str | None, limit: int = 50) -> List[dict]:
    clause, params = _company_clause(company_id)

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


