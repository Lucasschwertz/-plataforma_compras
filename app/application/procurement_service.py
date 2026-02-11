from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from app.application.erp_outbox_service import ErpOutboxService
from app.domain.contracts import (
    PurchaseOrderErpIntentInput,
    PurchaseOrderFromAwardInput,
    PurchaseRequestCreateInput,
    RfqAwardInput,
    RfqCreateInput,
    ServiceOutput,
)
from app.errors import IntegrationError, classify_erp_failure


class ProcurementService:
    def __init__(self, outbox_service: ErpOutboxService | None = None) -> None:
        self.outbox_service = outbox_service or ErpOutboxService()

    def create_purchase_request(
        self,
        db,
        *,
        tenant_id: str,
        create_input: PurchaseRequestCreateInput,
        parse_optional_int_fn,
        parse_optional_float_fn,
        err_fn,
    ) -> ServiceOutput:
        cursor = db.execute(
            """
            INSERT INTO purchase_requests (number, status, priority, requested_by, department, needed_at, tenant_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (
                create_input.number,
                create_input.status,
                create_input.priority,
                create_input.requested_by,
                create_input.department,
                create_input.needed_at,
                tenant_id,
            ),
        )
        row = cursor.fetchone()
        purchase_request_id = int(row["id"] if isinstance(row, dict) else row[0])

        created_items = 0
        for idx, item in enumerate(create_input.items, start=1):
            if not isinstance(item, dict):
                continue
            description = (item.get("description") or "").strip()
            if not description:
                continue
            line_no = parse_optional_int_fn(item.get("line_no")) or idx
            quantity = parse_optional_float_fn(item.get("quantity"))
            if quantity is None or quantity <= 0:
                quantity = 1
            uom = (item.get("uom") or "UN").strip() or "UN"
            category = (item.get("category") or "").strip() or None

            db.execute(
                """
                INSERT INTO purchase_request_items (
                    purchase_request_id, line_no, description, quantity, uom, category, tenant_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (purchase_request_id, line_no, description, quantity, uom, category, tenant_id),
            )
            created_items += 1

        if created_items == 0:
            db.execute(
                "DELETE FROM purchase_requests WHERE id = ? AND tenant_id = ?",
                (purchase_request_id, tenant_id),
            )
            return ServiceOutput(
                payload={"error": "items_required", "message": err_fn("items_required")},
                status_code=400,
            )

        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('purchase_request', ?, NULL, ?, 'purchase_request_created', ?)
            """,
            (purchase_request_id, create_input.status, tenant_id),
        )
        return ServiceOutput(
            payload={
                "id": purchase_request_id,
                "status": create_input.status,
                "priority": create_input.priority,
                "items_created": created_items,
            },
            status_code=201,
        )

    def update_purchase_request(
        self,
        db,
        *,
        tenant_id: str,
        purchase_request_id: int,
        payload: Dict[str, Any],
        is_delete: bool,
        allowed_priorities: set[str],
        allowed_statuses: set[str],
        flow_action_allowed_fn,
        forbidden_action_fn,
        require_confirmation_fn,
        err_fn,
    ) -> ServiceOutput:
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
            return ServiceOutput(
                payload={
                    "error": "purchase_request_not_found",
                    "message": err_fn("purchase_request_not_found"),
                    "purchase_request_id": purchase_request_id,
                },
                status_code=404,
            )

        previous_status = row["status"]
        is_erp_managed = bool(row["external_id"] or row["erp_num_cot"] or row["erp_num_pct"])
        if is_erp_managed:
            return ServiceOutput(
                payload={
                    "error": "erp_managed_request_readonly",
                    "message": err_fn("erp_managed_request_readonly"),
                },
                status_code=409,
            )

        if is_delete:
            if not flow_action_allowed_fn("solicitacao", previous_status, "cancel_request"):
                forbidden_action_fn("solicitacao", previous_status, "cancel_request")
            require_confirmation_fn(
                "cancel_request",
                entity="purchase_request",
                entity_id=purchase_request_id,
                payload=payload,
            )
            if previous_status == "cancelled":
                return ServiceOutput(
                    payload={"status": "cancelled", "purchase_request_id": purchase_request_id},
                    status_code=200,
                )
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
            return ServiceOutput(
                payload={"status": "cancelled", "purchase_request_id": purchase_request_id},
                status_code=200,
            )

        if not flow_action_allowed_fn("solicitacao", previous_status, "edit_request"):
            forbidden_action_fn("solicitacao", previous_status, "edit_request")

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
            if priority and priority not in allowed_priorities:
                return ServiceOutput(
                    payload={"error": "priority_invalid", "message": err_fn("priority_invalid")},
                    status_code=400,
                )
            updates.append("priority = ?")
            params.append(priority or "medium")

        next_status = previous_status
        if "status" in payload:
            if not flow_action_allowed_fn("solicitacao", previous_status, "update_request_status"):
                forbidden_action_fn("solicitacao", previous_status, "update_request_status")
            candidate = str(payload.get("status") or "").strip()
            if candidate not in allowed_statuses:
                return ServiceOutput(
                    payload={"error": "status_invalid", "message": err_fn("status_invalid")},
                    status_code=400,
                )
            if candidate == "cancelled" and candidate != previous_status:
                require_confirmation_fn(
                    "cancel_request",
                    entity="purchase_request",
                    entity_id=purchase_request_id,
                    payload=payload,
                )
            next_status = candidate
            updates.append("status = ?")
            params.append(candidate)

        if not updates:
            return ServiceOutput(
                payload={"error": "no_changes", "message": err_fn("no_changes")},
                status_code=400,
            )

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
        return ServiceOutput(
            payload={"purchase_request_id": purchase_request_id, "status": next_status},
            status_code=200,
        )

    def create_purchase_request_item(
        self,
        db,
        *,
        tenant_id: str,
        purchase_request_id: int,
        payload: Dict[str, Any],
        flow_action_allowed_fn,
        forbidden_action_fn,
        parse_optional_int_fn,
        parse_optional_float_fn,
        err_fn,
    ) -> ServiceOutput:
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
            return ServiceOutput(
                payload={"error": "purchase_request_not_found", "message": err_fn("purchase_request_not_found")},
                status_code=404,
            )
        if request_row["external_id"] or request_row["erp_num_cot"] or request_row["erp_num_pct"]:
            return ServiceOutput(
                payload={"error": "erp_managed_request_readonly", "message": err_fn("erp_managed_request_readonly")},
                status_code=409,
            )
        if not flow_action_allowed_fn("solicitacao", request_row["status"], "add_request_item"):
            forbidden_action_fn("solicitacao", request_row["status"], "add_request_item")
        if request_row["status"] != "pending_rfq":
            return ServiceOutput(
                payload={"error": "request_locked", "message": err_fn("request_locked")},
                status_code=400,
            )

        description = (payload.get("description") or "").strip()
        if not description:
            return ServiceOutput(
                payload={"error": "description_required", "message": err_fn("description_required")},
                status_code=400,
            )
        quantity = parse_optional_float_fn(payload.get("quantity"))
        if quantity is None or quantity <= 0:
            quantity = 1
        uom = (payload.get("uom") or "UN").strip() or "UN"
        category = (payload.get("category") or "").strip() or None
        line_no = parse_optional_int_fn(payload.get("line_no"))
        if line_no is None:
            next_line = db.execute(
                """
                SELECT COALESCE(MAX(line_no), 0) + 1 AS next_line
                FROM purchase_request_items
                WHERE purchase_request_id = ? AND tenant_id = ?
                """,
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
        return ServiceOutput(
            payload={"id": int(item_row["id"] if isinstance(item_row, dict) else item_row[0])},
            status_code=201,
        )

    def update_purchase_request_item(
        self,
        db,
        *,
        tenant_id: str,
        purchase_request_id: int,
        item_id: int,
        payload: Dict[str, Any],
        is_delete: bool,
        flow_action_allowed_fn,
        forbidden_action_fn,
        parse_optional_int_fn,
        parse_optional_float_fn,
        err_fn,
    ) -> ServiceOutput:
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
            return ServiceOutput(
                payload={"error": "purchase_request_not_found", "message": err_fn("purchase_request_not_found")},
                status_code=404,
            )
        if request_row["external_id"] or request_row["erp_num_cot"] or request_row["erp_num_pct"]:
            return ServiceOutput(
                payload={"error": "erp_managed_request_readonly", "message": err_fn("erp_managed_request_readonly")},
                status_code=409,
            )

        requested_action = "delete_request_item" if is_delete else "edit_request_item"
        if not flow_action_allowed_fn("solicitacao", request_row["status"], requested_action):
            forbidden_action_fn("solicitacao", request_row["status"], requested_action)
        if request_row["status"] != "pending_rfq":
            return ServiceOutput(
                payload={"error": "request_locked", "message": err_fn("request_locked")},
                status_code=400,
            )

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
            return ServiceOutput(
                payload={"error": "item_not_found", "message": err_fn("item_not_found")},
                status_code=404,
            )

        if is_delete:
            db.execute(
                """
                DELETE FROM purchase_request_items
                WHERE id = ? AND purchase_request_id = ? AND tenant_id = ?
                """,
                (item_id, purchase_request_id, tenant_id),
            )
            return ServiceOutput(payload={"deleted": True, "item_id": item_id}, status_code=200)

        updates: List[str] = []
        params: List[object] = []
        if "description" in payload:
            description = (payload.get("description") or "").strip()
            if not description:
                return ServiceOutput(
                    payload={"error": "description_required", "message": err_fn("description_required")},
                    status_code=400,
                )
            updates.append("description = ?")
            params.append(description)
        if "quantity" in payload:
            quantity = parse_optional_float_fn(payload.get("quantity"))
            if quantity is None or quantity <= 0:
                return ServiceOutput(
                    payload={"error": "quantity_invalid", "message": err_fn("quantity_invalid")},
                    status_code=400,
                )
            updates.append("quantity = ?")
            params.append(quantity)
        if "uom" in payload:
            updates.append("uom = ?")
            params.append((payload.get("uom") or "UN").strip() or "UN")
        if "line_no" in payload:
            line_no = parse_optional_int_fn(payload.get("line_no"))
            if line_no is None or line_no <= 0:
                return ServiceOutput(
                    payload={"error": "line_no_invalid", "message": err_fn("line_no_invalid")},
                    status_code=400,
                )
            updates.append("line_no = ?")
            params.append(line_no)
        if "category" in payload:
            updates.append("category = ?")
            params.append((payload.get("category") or "").strip() or None)

        if not updates:
            return ServiceOutput(
                payload={"error": "no_changes", "message": err_fn("no_changes")},
                status_code=400,
            )

        params.extend([item_id, purchase_request_id, tenant_id])
        db.execute(
            f"""
            UPDATE purchase_request_items
            SET {", ".join(updates)}, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND purchase_request_id = ? AND tenant_id = ?
            """,
            tuple(params),
        )
        return ServiceOutput(payload={"item_id": item_id, "updated": True}, status_code=200)

    def create_rfq(
        self,
        db,
        *,
        tenant_id: str,
        create_input: RfqCreateInput,
        create_rfq_core_fn,
    ) -> ServiceOutput:
        created, error_payload, status_code = create_rfq_core_fn(
            db,
            tenant_id,
            create_input.title,
            create_input.purchase_request_item_ids,
        )
        if error_payload:
            return ServiceOutput(payload=error_payload, status_code=status_code)
        return ServiceOutput(payload=created, status_code=201)

    def list_rfqs(
        self,
        db,
        *,
        tenant_id: str | None,
        status_values: List[str],
        allowed_statuses: set[str],
        tenant_clause_fn,
        stage_for_rfq_status_fn,
        flow_meta_fn,
    ) -> ServiceOutput:
        clause, params = tenant_clause_fn(tenant_id, alias="r")
        filters: List[str] = []
        normalized_statuses = [status for status in status_values if status in allowed_statuses]
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
            stage = stage_for_rfq_status_fn(status)
            meta = flow_meta_fn("cotacao", status)
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
        return ServiceOutput(payload={"items": items}, status_code=200)

    def run_integration_sync(
        self,
        db,
        *,
        tenant_id: str,
        scope_value: str,
        limit: int,
        scope_aliases: Dict[str, tuple[str, ...]],
        supported_scopes: set[str],
        start_sync_run_fn,
        finish_sync_run_fn,
        sync_from_erp_fn,
        integration_error_cls,
        err_fn,
    ) -> ServiceOutput:
        scope_raw = str(scope_value or "").strip()
        if not scope_raw:
            return ServiceOutput(
                payload={"error": "scope_required", "message": err_fn("scope_required")},
                status_code=400,
            )

        canonical_scope = scope_aliases.get(scope_raw, (scope_raw,))[0]
        if canonical_scope not in supported_scopes:
            return ServiceOutput(
                payload={
                    "error": "scope_not_supported",
                    "message": err_fn("scope_not_supported"),
                    "scope": canonical_scope,
                },
                status_code=400,
            )

        sync_run_id = start_sync_run_fn(db, tenant_id, scope=canonical_scope)
        try:
            result = sync_from_erp_fn(db, tenant_id, canonical_scope, limit=limit)
            finish_sync_run_fn(
                db,
                tenant_id,
                sync_run_id,
                status="succeeded",
                records_in=result["records_in"],
                records_upserted=result["records_upserted"],
            )
        except Exception as exc:  # noqa: BLE001
            finish_sync_run_fn(db, tenant_id, sync_run_id, status="failed", records_in=0, records_upserted=0)
            db.execute(
                """
                UPDATE sync_runs
                SET error_summary = ?
                WHERE id = ? AND tenant_id = ?
                """,
                (str(exc)[:200], sync_run_id, tenant_id),
            )
            raise integration_error_cls(
                code="sync_failed",
                message_key="sync_failed",
                http_status=500,
                critical=False,
                details=str(exc),
                payload={"scope": canonical_scope},
            )

        return ServiceOutput(
            payload={
                "status": "succeeded",
                "scope": canonical_scope,
                "sync_run_id": sync_run_id,
                "result": result,
            },
            status_code=200,
        )

    def seed_procurement_data(
        self,
        db,
        *,
        tenant_id: str,
        load_inbox_cards_fn,
        ensure_demo_suppliers_fn,
        seed_demo_items_for_pending_requests_fn,
    ) -> ServiceOutput:
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
                seed_demo_items_for_pending_requests_fn(db, tenant_id)
            ensure_demo_suppliers_fn(db, tenant_id)
            cards = load_inbox_cards_fn(db, tenant_id)
            return ServiceOutput(
                payload={
                    "seeded": open_items == 0,
                    "tenant_id": tenant_id,
                    "kpis": cards,
                    "hint": "Seed ja aplicado. Use GET /api/procurement/inbox",
                },
                status_code=200,
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

        ensure_demo_suppliers_fn(db, tenant_id)
        cards = load_inbox_cards_fn(db, tenant_id)
        return ServiceOutput(
            payload={
                "seeded": True,
                "tenant_id": tenant_id,
                "kpis": cards,
                "hint": "Agora use GET /api/procurement/inbox",
            },
            status_code=200,
        )

    def update_rfq(
        self,
        db,
        *,
        tenant_id: str,
        rfq_id: int,
        payload: Dict[str, Any],
        is_delete: bool,
        allowed_statuses: set[str],
        load_rfq_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        require_confirmation_fn,
        err_fn,
    ) -> ServiceOutput:
        rfq = load_rfq_fn(db, tenant_id, rfq_id)
        if not rfq:
            return ServiceOutput(
                payload={"error": "rfq_not_found", "message": err_fn("rfq_not_found"), "rfq_id": rfq_id},
                status_code=404,
            )

        previous_status = rfq["status"]
        if is_delete:
            if not flow_action_allowed_fn("cotacao", previous_status, "cancel_rfq"):
                forbidden_action_fn("cotacao", previous_status, "cancel_rfq")
            require_confirmation_fn(
                "cancel_rfq",
                entity="rfq",
                entity_id=rfq_id,
                payload=payload,
            )
            if previous_status == "cancelled":
                return ServiceOutput(payload={"rfq_id": rfq_id, "status": "cancelled"}, status_code=200)
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
            return ServiceOutput(payload={"rfq_id": rfq_id, "status": "cancelled"}, status_code=200)

        updates: List[str] = []
        params: List[object] = []
        next_status = previous_status

        if "title" in payload:
            if not flow_action_allowed_fn("cotacao", previous_status, "edit_rfq"):
                forbidden_action_fn("cotacao", previous_status, "edit_rfq")
            updates.append("title = ?")
            params.append((payload.get("title") or "").strip() or None)

        if "status" in payload:
            status = str(payload.get("status") or "").strip()
            if status not in allowed_statuses:
                return ServiceOutput(
                    payload={"error": "status_invalid", "message": err_fn("status_invalid")},
                    status_code=400,
                )
            required_action = "cancel_rfq" if status == "cancelled" else "update_rfq_status"
            if status == "awarded":
                required_action = "award_rfq"
            if not flow_action_allowed_fn("cotacao", previous_status, required_action):
                forbidden_action_fn("cotacao", previous_status, required_action)
            if status == "awarded":
                forbidden_action_fn("cotacao", previous_status, "award_rfq", http_status=400)
            if status == "cancelled" and status != previous_status:
                require_confirmation_fn("cancel_rfq", entity="rfq", entity_id=rfq_id, payload=payload)
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
            return ServiceOutput(
                payload={"error": "no_changes", "message": err_fn("no_changes")},
                status_code=400,
            )

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
        return ServiceOutput(payload={"rfq_id": rfq_id, "status": next_status}, status_code=200)

    def create_rfq_with_suppliers(
        self,
        db,
        *,
        tenant_id: str,
        title: str,
        purchase_request_item_ids: List[int],
        supplier_ids: List[int],
        invite_valid_days: int,
        create_rfq_core_fn,
        load_suppliers_fn,
        create_rfq_supplier_invites_fn,
        err_fn,
    ) -> ServiceOutput:
        if not supplier_ids:
            return ServiceOutput(
                payload={"error": "supplier_ids_required", "message": err_fn("supplier_ids_required")},
                status_code=400,
            )
        valid_supplier_ids = {int(item["id"]) for item in load_suppliers_fn(db, tenant_id)}
        if not any(supplier_id in valid_supplier_ids for supplier_id in supplier_ids):
            return ServiceOutput(
                payload={"error": "suppliers_not_found", "message": err_fn("suppliers_not_found")},
                status_code=400,
            )

        created, error_payload, status_code = create_rfq_core_fn(db, tenant_id, title, purchase_request_item_ids)
        if error_payload:
            return ServiceOutput(payload=error_payload, status_code=status_code)

        invite_result = create_rfq_supplier_invites_fn(
            db=db,
            tenant_id=tenant_id,
            rfq_id=int(created["id"]),
            rfq_items=created["rfq_items"],
            supplier_ids=supplier_ids,
            valid_days=invite_valid_days,
        )
        return ServiceOutput(payload={"rfq": created, "invites": invite_result}, status_code=201)

    def invite_suppliers(
        self,
        db,
        *,
        tenant_id: str,
        rfq_id: int,
        supplier_ids: List[int],
        requested_item_ids: List[int],
        invite_valid_days: int,
        load_rfq_fn,
        load_rfq_items_fn,
        create_rfq_supplier_invites_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        err_fn,
    ) -> ServiceOutput:
        rfq = load_rfq_fn(db, tenant_id, rfq_id)
        if not rfq:
            return ServiceOutput(
                payload={"error": "rfq_not_found", "message": err_fn("rfq_not_found"), "rfq_id": rfq_id},
                status_code=404,
            )
        if not flow_action_allowed_fn("cotacao", rfq["status"], "invite_supplier"):
            forbidden_action_fn("cotacao", rfq["status"], "invite_supplier")

        if not supplier_ids:
            return ServiceOutput(
                payload={"error": "supplier_ids_required", "message": err_fn("supplier_ids_required")},
                status_code=400,
            )

        all_items = load_rfq_items_fn(db, tenant_id, rfq_id)
        if requested_item_ids:
            wanted = set(requested_item_ids)
            items = [item for item in all_items if int(item["id"]) in wanted]
        else:
            items = all_items
        if not items:
            return ServiceOutput(
                payload={"error": "rfq_items_required", "message": err_fn("rfq_items_required")},
                status_code=400,
            )

        invite_result = create_rfq_supplier_invites_fn(
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
            valid_days=invite_valid_days,
        )
        if invite_result["supplier_count"] == 0:
            return ServiceOutput(
                payload={"error": "suppliers_not_found", "message": err_fn("suppliers_not_found")},
                status_code=400,
            )
        return ServiceOutput(payload={"rfq_id": rfq_id, "invites": invite_result}, status_code=200)

    def manage_rfq_invite(
        self,
        db,
        *,
        tenant_id: str,
        rfq_id: int,
        invite_id: int,
        payload: Dict[str, Any],
        is_delete: bool,
        valid_days: int,
        load_rfq_fn,
        load_invite_fn,
        serialize_invite_fn,
        remove_supplier_from_rfq_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        require_confirmation_fn,
        err_fn,
    ) -> ServiceOutput:
        rfq = load_rfq_fn(db, tenant_id, rfq_id)
        if not rfq:
            return ServiceOutput(
                payload={"error": "rfq_not_found", "message": err_fn("rfq_not_found"), "rfq_id": rfq_id},
                status_code=404,
            )
        invite = load_invite_fn(db, tenant_id, rfq_id, invite_id)
        if not invite:
            return ServiceOutput(
                payload={"error": "invite_not_found", "message": err_fn("invite_not_found")},
                status_code=404,
            )

        if is_delete:
            if not flow_action_allowed_fn("cotacao", rfq["status"], "cancel_invite"):
                forbidden_action_fn("cotacao", rfq["status"], "cancel_invite")
            if not flow_action_allowed_fn("fornecedor", invite["status"], "cancel_invite"):
                forbidden_action_fn("fornecedor", invite["status"], "cancel_invite")
            require_confirmation_fn(
                "cancel_invite",
                entity="rfq_supplier_invite",
                entity_id=invite_id,
                payload=payload,
            )
            remove_supplier_from_rfq_fn(db, tenant_id, rfq_id, int(invite["supplier_id"]))
            db.execute(
                """
                UPDATE rfq_supplier_invites
                SET status = 'cancelled',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND tenant_id = ?
                """,
                (invite_id, tenant_id),
            )
            return ServiceOutput(payload={"deleted": True, "invite_id": invite_id}, status_code=200)

        action = str(payload.get("action") or "").strip().lower()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=valid_days)).replace(microsecond=0).isoformat()
        action_map = {"reopen": "reopen_invite", "extend": "extend_invite", "cancel": "cancel_invite"}
        requested_action = action_map.get(action)
        if not requested_action:
            return ServiceOutput(
                payload={"error": "action_invalid", "message": err_fn("action_invalid")},
                status_code=400,
            )
        if not flow_action_allowed_fn("cotacao", rfq["status"], requested_action):
            forbidden_action_fn("cotacao", rfq["status"], requested_action)
        if not flow_action_allowed_fn("fornecedor", invite["status"], requested_action):
            forbidden_action_fn("fornecedor", invite["status"], requested_action)
        if action == "cancel":
            require_confirmation_fn(
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

        updated = load_invite_fn(db, tenant_id, rfq_id, invite_id)
        if not updated:
            return ServiceOutput(
                payload={"error": "invite_not_found", "message": err_fn("invite_not_found")},
                status_code=404,
            )
        return ServiceOutput(payload={"invite": serialize_invite_fn(updated)}, status_code=200)

    def assign_rfq_item_suppliers(
        self,
        db,
        *,
        tenant_id: str,
        rfq_id: int,
        rfq_item_id: int,
        supplier_ids: List[Any],
        load_rfq_fn,
        load_rfq_item_fn,
        load_suppliers_fn,
        load_rfq_items_with_quotes_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        err_fn,
    ) -> ServiceOutput:
        rfq = load_rfq_fn(db, tenant_id, rfq_id)
        if not rfq:
            return ServiceOutput(
                payload={"error": "rfq_not_found", "message": err_fn("rfq_not_found"), "rfq_id": rfq_id},
                status_code=404,
            )
        if not flow_action_allowed_fn("cotacao", rfq["status"], "manage_item_supplier"):
            forbidden_action_fn("cotacao", rfq["status"], "manage_item_supplier")

        rfq_item = load_rfq_item_fn(db, tenant_id, rfq_item_id)
        if not rfq_item or int(rfq_item["rfq_id"]) != rfq_id:
            return ServiceOutput(
                payload={
                    "error": "rfq_item_not_found",
                    "message": err_fn("rfq_item_not_found"),
                    "rfq_item_id": rfq_item_id,
                },
                status_code=404,
            )

        if not isinstance(supplier_ids, list) or not supplier_ids:
            return ServiceOutput(
                payload={"error": "supplier_ids_required", "message": err_fn("supplier_ids_required")},
                status_code=400,
            )

        valid_supplier_ids = {s["id"] for s in load_suppliers_fn(db, tenant_id)}
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

        itens = load_rfq_items_with_quotes_fn(db, tenant_id, rfq_id)
        return ServiceOutput(payload={"itens": itens}, status_code=200)

    def save_supplier_quote(
        self,
        db,
        *,
        tenant_id: str,
        rfq_id: int,
        payload: Dict[str, Any],
        load_rfq_fn,
        load_suppliers_fn,
        get_or_create_quote_fn,
        upsert_quote_item_fn,
        load_rfq_items_with_quotes_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        err_fn,
    ) -> ServiceOutput:
        rfq = load_rfq_fn(db, tenant_id, rfq_id)
        if not rfq:
            return ServiceOutput(
                payload={"error": "rfq_not_found", "message": err_fn("rfq_not_found"), "rfq_id": rfq_id},
                status_code=404,
            )
        if not flow_action_allowed_fn("cotacao", rfq["status"], "save_supplier_quote"):
            forbidden_action_fn("cotacao", rfq["status"], "save_supplier_quote")

        supplier_id = payload.get("supplier_id")
        items = payload.get("items") or []
        if not supplier_id:
            return ServiceOutput(
                payload={"error": "supplier_id_required", "message": err_fn("supplier_id_required")},
                status_code=400,
            )
        if not isinstance(items, list) or not items:
            return ServiceOutput(
                payload={"error": "items_required", "message": err_fn("items_required")},
                status_code=400,
            )

        try:
            supplier_id = int(supplier_id)
        except (TypeError, ValueError):
            return ServiceOutput(
                payload={"error": "supplier_id_invalid", "message": err_fn("supplier_id_invalid")},
                status_code=400,
            )

        valid_supplier_ids = {s["id"] for s in load_suppliers_fn(db, tenant_id)}
        if supplier_id not in valid_supplier_ids:
            return ServiceOutput(
                payload={"error": "supplier_not_found", "message": err_fn("supplier_not_found")},
                status_code=404,
            )

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
            return ServiceOutput(
                payload={"error": "valid_items_required", "message": err_fn("valid_items_required")},
                status_code=400,
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
            return ServiceOutput(
                payload={
                    "error": "rfq_items_not_found",
                    "message": err_fn("rfq_items_not_found"),
                    "rfq_item_ids": invalid_item_ids,
                },
                status_code=400,
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
            return ServiceOutput(
                payload={
                    "error": "supplier_not_invited_for_items",
                    "message": err_fn("supplier_not_invited_for_items"),
                    "rfq_item_ids": uninvited_item_ids,
                    "supplier_id": supplier_id,
                },
                status_code=400,
            )

        quote_id = get_or_create_quote_fn(db, rfq_id, supplier_id, tenant_id)
        for item in normalized_items:
            upsert_quote_item_fn(
                db,
                quote_id=int(quote_id),
                rfq_item_id=int(item["rfq_item_id"]),
                unit_price=float(item["unit_price"]),
                lead_time_days=item["lead_time_days"],
                tenant_id=tenant_id,
            )

        itens = load_rfq_items_with_quotes_fn(db, tenant_id, rfq_id)
        return ServiceOutput(
            payload={"itens": itens, "quote_id": quote_id, "saved_items": len(normalized_items)},
            status_code=200,
        )

    def get_supplier_quote_detail(
        self,
        db,
        *,
        tenant_id: str,
        rfq_id: int,
        supplier_id: int,
        load_rfq_fn,
        err_fn,
    ) -> ServiceOutput:
        rfq = load_rfq_fn(db, tenant_id, rfq_id)
        if not rfq:
            return ServiceOutput(
                payload={"error": "rfq_not_found", "message": err_fn("rfq_not_found"), "rfq_id": rfq_id},
                status_code=404,
            )

        supplier = db.execute(
            "SELECT id, name FROM suppliers WHERE id = ? AND tenant_id = ? LIMIT 1",
            (supplier_id, tenant_id),
        ).fetchone()
        if not supplier:
            return ServiceOutput(
                payload={"error": "supplier_not_found", "message": err_fn("supplier_not_found")},
                status_code=404,
            )

        quote = db.execute(
            """
            SELECT id
            FROM quotes
            WHERE rfq_id = ? AND supplier_id = ? AND tenant_id = ?
            LIMIT 1
            """,
            (rfq_id, supplier_id, tenant_id),
        ).fetchone()
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

        return ServiceOutput(
            payload={
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
            },
            status_code=200,
        )

    def delete_supplier_quote(
        self,
        db,
        *,
        tenant_id: str,
        rfq_id: int,
        supplier_id: int,
        payload: Dict[str, Any],
        load_rfq_fn,
        require_confirmation_fn,
        err_fn,
    ) -> ServiceOutput:
        rfq = load_rfq_fn(db, tenant_id, rfq_id)
        if not rfq:
            return ServiceOutput(
                payload={"error": "rfq_not_found", "message": err_fn("rfq_not_found"), "rfq_id": rfq_id},
                status_code=404,
            )
        supplier = db.execute(
            "SELECT id FROM suppliers WHERE id = ? AND tenant_id = ? LIMIT 1",
            (supplier_id, tenant_id),
        ).fetchone()
        if not supplier:
            return ServiceOutput(
                payload={"error": "supplier_not_found", "message": err_fn("supplier_not_found")},
                status_code=404,
            )
        quote = db.execute(
            """
            SELECT id
            FROM quotes
            WHERE rfq_id = ? AND supplier_id = ? AND tenant_id = ?
            LIMIT 1
            """,
            (rfq_id, supplier_id, tenant_id),
        ).fetchone()
        if not quote:
            return ServiceOutput(
                payload={"error": "quote_not_found", "message": err_fn("quote_not_found")},
                status_code=404,
            )
        quote_id = int(quote["id"])
        require_confirmation_fn("delete_supplier_proposal", entity="quote", entity_id=quote_id, payload=payload)
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
        return ServiceOutput(
            payload={"deleted": True, "rfq_id": rfq_id, "supplier_id": supplier_id},
            status_code=200,
        )

    def open_supplier_invite(
        self,
        db,
        *,
        token: str,
        load_supplier_invite_by_token_fn,
        invite_is_expired_fn,
        build_supplier_invite_payload_fn,
        err_fn,
    ) -> ServiceOutput:
        invite = load_supplier_invite_by_token_fn(db, token)
        if not invite:
            return ServiceOutput(
                payload={"error": "invite_not_found", "message": err_fn("invite_not_found")},
                status_code=404,
            )
        if invite_is_expired_fn(invite):
            db.execute(
                "UPDATE rfq_supplier_invites SET status = 'expired', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (invite["id"],),
            )
            return ServiceOutput(
                payload={"error": "invite_expired", "message": err_fn("invite_expired")},
                status_code=410,
            )
        if invite["status"] == "pending":
            db.execute(
                """
                UPDATE rfq_supplier_invites
                SET status = 'opened', opened_at = COALESCE(opened_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (invite["id"],),
            )
        payload = build_supplier_invite_payload_fn(db, invite)
        return ServiceOutput(payload=payload, status_code=200)

    def submit_supplier_invite(
        self,
        db,
        *,
        token: str,
        payload: Dict[str, Any],
        load_supplier_invite_by_token_fn,
        invite_is_expired_fn,
        load_rfq_fn,
        parse_optional_int_fn,
        parse_optional_float_fn,
        get_or_create_quote_fn,
        upsert_quote_item_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        err_fn,
    ) -> ServiceOutput:
        invite = load_supplier_invite_by_token_fn(db, token)
        if not invite:
            return ServiceOutput(
                payload={"error": "invite_not_found", "message": err_fn("invite_not_found")},
                status_code=404,
            )
        if invite_is_expired_fn(invite):
            db.execute(
                "UPDATE rfq_supplier_invites SET status = 'expired', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (invite["id"],),
            )
            return ServiceOutput(
                payload={"error": "invite_expired", "message": err_fn("invite_expired")},
                status_code=410,
            )

        items = payload.get("items") or []
        if not isinstance(items, list) or not items:
            return ServiceOutput(
                payload={"error": "items_required", "message": err_fn("items_required")},
                status_code=400,
            )

        rfq_id = int(invite["rfq_id"])
        supplier_id = int(invite["supplier_id"])
        tenant_id = str(invite["tenant_id"])
        rfq = load_rfq_fn(db, tenant_id, rfq_id)
        if not rfq:
            return ServiceOutput(
                payload={"error": "rfq_not_found", "message": err_fn("rfq_not_found")},
                status_code=404,
            )
        if not flow_action_allowed_fn("fornecedor", invite["status"], "submit_quote"):
            forbidden_action_fn("fornecedor", invite["status"], "submit_quote")
        if not flow_action_allowed_fn("cotacao", rfq["status"], "save_supplier_quote"):
            forbidden_action_fn("cotacao", rfq["status"], "save_supplier_quote")
        if rfq["status"] not in {"open", "collecting_quotes"}:
            return ServiceOutput(
                payload={"error": "rfq_closed_for_quotes", "message": err_fn("rfq_closed_for_quotes")},
                status_code=400,
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
            return ServiceOutput(
                payload={"error": "rfq_items_not_found", "message": err_fn("rfq_items_not_found")},
                status_code=400,
            )

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
            return ServiceOutput(
                payload={"error": "supplier_not_invited", "message": err_fn("supplier_not_invited")},
                status_code=400,
            )

        normalized_by_id: Dict[int, dict] = {}
        for item in items:
            rfq_item_id = parse_optional_int_fn(item.get("rfq_item_id"))
            unit_price = parse_optional_float_fn(item.get("unit_price"))
            lead_time_days = parse_optional_int_fn(item.get("lead_time_days"))
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
            return ServiceOutput(
                payload={"error": "valid_items_required", "message": err_fn("valid_items_required")},
                status_code=400,
            )

        quote_id = get_or_create_quote_fn(db, rfq_id, supplier_id, tenant_id)
        for item in normalized_items:
            upsert_quote_item_fn(
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
            """
            UPDATE rfqs
            SET status = 'collecting_quotes', updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (rfq_id, tenant_id),
        )
        return ServiceOutput(
            payload={"status": "submitted", "quote_id": quote_id, "saved_items": len(normalized_items)},
            status_code=200,
        )

    def get_rfq_detail(
        self,
        db,
        *,
        tenant_id: str | None,
        rfq_id: int,
        load_rfq_fn,
        load_latest_award_for_rfq_fn,
        load_purchase_order_fn,
        load_rfq_items_with_quotes_fn,
        load_rfq_supplier_invites_fn,
        load_status_events_fn,
        flow_meta_fn,
        stage_for_rfq_status_fn,
        stage_for_award_status_fn,
        stage_for_purchase_order_status_fn,
        build_process_steps_fn,
        flow_action_allowed_fn,
        serialize_purchase_order_fn,
        err_fn,
    ) -> ServiceOutput:
        rfq = load_rfq_fn(db, tenant_id, rfq_id)
        if not rfq:
            return ServiceOutput(
                payload={"error": "rfq_not_found", "message": err_fn("rfq_not_found"), "rfq_id": rfq_id},
                status_code=404,
            )

        award = load_latest_award_for_rfq_fn(db, tenant_id, rfq_id)
        purchase_order = None
        if award and award.get("purchase_order_id"):
            purchase_order = load_purchase_order_fn(db, tenant_id, int(award["purchase_order_id"]))

        itens = load_rfq_items_with_quotes_fn(db, tenant_id, rfq_id)
        convites = load_rfq_supplier_invites_fn(db, tenant_id, rfq_id)
        events = load_status_events_fn(db, tenant_id, entity="rfq", entity_id=rfq_id, limit=80)
        award_events: List[dict] = []
        if award:
            award_events = load_status_events_fn(db, tenant_id, entity="award", entity_id=int(award["id"]), limit=40)

        rfq_status = rfq["status"]
        rfq_flow = flow_meta_fn("cotacao", rfq_status)
        award_flow = flow_meta_fn("decisao", award["status"] if award else None)
        po_flow = flow_meta_fn("ordem_compra", purchase_order["status"] if purchase_order else None)

        process_stage = stage_for_rfq_status_fn(rfq_status)
        if award:
            process_stage = stage_for_award_status_fn(award.get("status"))
        if purchase_order:
            process_stage = stage_for_purchase_order_status_fn(purchase_order["status"])

        invite_items: List[dict] = []
        for invite in convites:
            invite_status = invite.get("status")
            invite_meta = flow_meta_fn("fornecedor", invite_status)
            allowed_invite_actions: List[str] = []
            for action in ("reopen_invite", "extend_invite", "cancel_invite"):
                if flow_action_allowed_fn("fornecedor", invite_status, action) and flow_action_allowed_fn("cotacao", rfq_status, action):
                    allowed_invite_actions.append(action)
            if invite.get("access_url") and flow_action_allowed_fn("fornecedor", invite_status, "open_invite_portal"):
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
                "process_stage": stage_for_award_status_fn(award.get("status")),
            }

        ordem_compra_payload = None
        if purchase_order:
            ordem_compra_payload = {
                **serialize_purchase_order_fn(purchase_order),
                "allowed_actions": po_flow["allowed_actions"],
                "primary_action": po_flow["primary_action"],
                "process_stage": stage_for_purchase_order_status_fn(purchase_order["status"]),
            }

        return ServiceOutput(
            payload={
                "cotacao": {
                    "id": rfq["id"],
                    "titulo": rfq["title"],
                    "status": rfq_status,
                    "criada_em": rfq["created_at"],
                    "atualizada_em": rfq["updated_at"],
                    "allowed_actions": rfq_flow["allowed_actions"],
                    "primary_action": rfq_flow["primary_action"],
                    "process_stage": stage_for_rfq_status_fn(rfq_status),
                },
                "itens": itens,
                "convites": invite_items,
                "decisao": decisao_payload,
                "ordem_compra": ordem_compra_payload,
                "eventos_cotacao": events,
                "eventos_decisao": award_events,
                "flow": {
                    "process_stage": process_stage,
                    "process_steps": build_process_steps_fn(process_stage),
                },
            },
            status_code=200,
        )

    def get_purchase_order_detail(
        self,
        db,
        *,
        tenant_id: str,
        purchase_order_id: int,
        include_history: bool,
        load_purchase_order_fn,
        load_status_events_fn,
        build_erp_timeline_fn,
        load_sync_runs_fn,
        flow_meta_fn,
        stage_for_purchase_order_status_fn,
        erp_status_payload_fn,
        erp_next_action_key_fn,
        erp_action_label_fn,
        build_process_steps_fn,
        err_fn,
    ) -> ServiceOutput:
        po = load_purchase_order_fn(db, tenant_id, purchase_order_id)
        if not po:
            return ServiceOutput(
                payload={
                    "error": "purchase_order_not_found",
                    "message": err_fn("purchase_order_not_found"),
                    "purchase_order_id": purchase_order_id,
                },
                status_code=404,
            )

        events: List[dict] = []
        erp_timeline: List[dict] = []
        sync_runs: List[dict] = []
        if include_history:
            events = load_status_events_fn(db, tenant_id, entity="purchase_order", entity_id=purchase_order_id)
            erp_timeline = build_erp_timeline_fn(events)
            sync_runs = load_sync_runs_fn(db, tenant_id, scope="purchase_order", limit=20)
        po_flow = flow_meta_fn("ordem_compra", po["status"])
        process_stage = stage_for_purchase_order_status_fn(po["status"])
        last_erp_update = (erp_timeline[0]["occurred_at"] if erp_timeline else None) or po["updated_at"]
        erp_status = erp_status_payload_fn(
            po["status"],
            erp_last_error=po["erp_last_error"],
            last_updated_at=last_erp_update,
        )
        next_action_key = erp_next_action_key_fn(
            po["status"],
            list(po_flow.get("allowed_actions") or []),
            po_flow.get("primary_action"),
            str(erp_status.get("key") or ""),
        )
        next_action_label = erp_action_label_fn(next_action_key, str(erp_status.get("key") or ""))

        return ServiceOutput(
            payload={
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
                    "process_steps": build_process_steps_fn(process_stage),
                },
            },
            status_code=200,
        )

    def list_erp_followup_orders(
        self,
        *,
        items: List[dict],
        status_filter_values: set[str],
        erp_status_payload_fn,
        erp_next_action_key_fn,
        erp_action_label_fn,
        get_ui_text_fn,
    ) -> ServiceOutput:
        rows: List[dict] = []
        for item in items:
            erp_status = item.get("erp_status") or erp_status_payload_fn(
                item.get("status"),
                erp_last_error=item.get("erp_last_error"),
                last_updated_at=item.get("erp_last_attempt_at") or item.get("updated_at"),
            )
            erp_key = str(erp_status.get("key") or "")
            if status_filter_values and erp_key not in status_filter_values:
                continue

            allowed_actions = list(item.get("allowed_actions") or [])
            next_action_key = erp_next_action_key_fn(
                item.get("status"),
                allowed_actions,
                item.get("primary_action"),
                erp_key,
            )
            can_resend = "push_to_erp" in set(allowed_actions) and erp_key in {"rejeitado", "reenvio_necessario"}
            next_action_label = erp_action_label_fn(next_action_key, erp_key)
            if next_action_key == "refresh_order":
                next_action_label = get_ui_text_fn("erp.next_action.await_response", "Aguardar retorno do ERP.")
            elif can_resend:
                next_action_label = get_ui_text_fn("erp.next_action.resend", "Reenviar ao ERP")
            elif erp_key == "rejeitado":
                next_action_label = get_ui_text_fn("erp.next_action.review_data", "Revisar dados da ordem e reenviar ao ERP.")

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
        return ServiceOutput(payload={"items": rows}, status_code=200)

    def award_rfq(
        self,
        db,
        *,
        tenant_id: str,
        award_input: RfqAwardInput,
        load_rfq_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        require_confirmation_fn,
        err_fn,
    ) -> ServiceOutput:
        rfq = load_rfq_fn(db, tenant_id, award_input.rfq_id)
        if not rfq:
            return ServiceOutput(
                payload={"error": "rfq_not_found", "message": err_fn("rfq_not_found"), "rfq_id": award_input.rfq_id},
                status_code=404,
            )
        if not flow_action_allowed_fn("cotacao", rfq["status"], "award_rfq"):
            forbidden_action_fn("cotacao", rfq["status"], "award_rfq")

        reason = (award_input.reason or "").strip()
        if not reason:
            return ServiceOutput(
                payload={"error": "reason_required", "message": err_fn("reason_required")},
                status_code=400,
            )

        require_confirmation_fn(
            "award_rfq",
            entity="rfq",
            entity_id=award_input.rfq_id,
            payload=award_input.payload,
        )
        supplier_name = (award_input.supplier_name or "Fornecedor selecionado").strip()
        cursor = db.execute(
            """
            INSERT INTO awards (rfq_id, supplier_name, status, reason, tenant_id)
            VALUES (?, ?, 'awarded', ?, ?)
            RETURNING id
            """,
            (award_input.rfq_id, supplier_name, reason, tenant_id),
        )
        award_row = cursor.fetchone()
        award_id = award_row["id"] if isinstance(award_row, dict) else award_row[0]
        db.execute(
            "UPDATE rfqs SET status = 'awarded' WHERE id = ? AND tenant_id = ?",
            (award_input.rfq_id, tenant_id),
        )
        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('rfq', ?, ?, 'awarded', 'rfq_awarded', ?)
            """,
            (award_input.rfq_id, rfq["status"], tenant_id),
        )
        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('award', ?, NULL, 'awarded', ?, ?)
            """,
            (award_id, reason, tenant_id),
        )
        return ServiceOutput(
            payload={"award_id": award_id, "rfq_id": award_input.rfq_id, "status": "awarded"},
            status_code=201,
        )

    def create_purchase_order(
        self,
        db,
        *,
        tenant_id: str,
        payload: Dict[str, Any],
        allowed_statuses: set[str],
        parse_optional_float_fn,
        err_fn,
    ) -> ServiceOutput:
        number = (payload.get("number") or "").strip() or None
        supplier_name = (payload.get("supplier_name") or payload.get("fornecedor") or "").strip() or None
        if not supplier_name:
            return ServiceOutput(
                payload={"error": "supplier_name_required", "message": err_fn("supplier_name_required")},
                status_code=400,
            )

        status = str(payload.get("status") or "draft").strip()
        if status not in allowed_statuses:
            return ServiceOutput(
                payload={"error": "status_invalid", "message": err_fn("status_invalid")},
                status_code=400,
            )

        currency = (payload.get("currency") or "BRL").strip() or "BRL"
        total_amount = parse_optional_float_fn(payload.get("total_amount"))
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
        return ServiceOutput(payload={"id": purchase_order_id, "status": status}, status_code=201)

    def cancel_purchase_order(
        self,
        db,
        *,
        tenant_id: str,
        purchase_order_id: int,
        payload: Dict[str, Any],
        load_purchase_order_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        require_confirmation_fn,
        err_fn,
    ) -> ServiceOutput:
        po = load_purchase_order_fn(db, tenant_id, purchase_order_id)
        if not po:
            return ServiceOutput(
                payload={
                    "error": "purchase_order_not_found",
                    "message": err_fn("purchase_order_not_found"),
                    "purchase_order_id": purchase_order_id,
                },
                status_code=404,
            )
        if not flow_action_allowed_fn("ordem_compra", po["status"], "cancel_order"):
            forbidden_action_fn("ordem_compra", po["status"], "cancel_order")
        if po["external_id"]:
            return ServiceOutput(
                payload={
                    "error": "erp_managed_purchase_order_readonly",
                    "message": err_fn("erp_managed_purchase_order_readonly"),
                },
                status_code=409,
            )
        require_confirmation_fn(
            "cancel_order",
            entity="purchase_order",
            entity_id=purchase_order_id,
            payload=payload,
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
        return ServiceOutput(
            payload={"purchase_order_id": purchase_order_id, "status": "cancelled"},
            status_code=200,
        )

    def update_purchase_order(
        self,
        db,
        *,
        tenant_id: str,
        purchase_order_id: int,
        payload: Dict[str, Any],
        allowed_statuses: set[str],
        load_purchase_order_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        require_confirmation_fn,
        parse_optional_float_fn,
        err_fn,
    ) -> ServiceOutput:
        po = load_purchase_order_fn(db, tenant_id, purchase_order_id)
        if not po:
            return ServiceOutput(
                payload={
                    "error": "purchase_order_not_found",
                    "message": err_fn("purchase_order_not_found"),
                    "purchase_order_id": purchase_order_id,
                },
                status_code=404,
            )
        if not flow_action_allowed_fn("ordem_compra", po["status"], "edit_order"):
            forbidden_action_fn("ordem_compra", po["status"], "edit_order")
        if po["external_id"]:
            return ServiceOutput(
                payload={
                    "error": "erp_managed_purchase_order_readonly",
                    "message": err_fn("erp_managed_purchase_order_readonly"),
                },
                status_code=409,
            )

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
            total_amount = parse_optional_float_fn(payload.get("total_amount"))
            if total_amount is None:
                return ServiceOutput(
                    payload={"error": "total_amount_invalid", "message": err_fn("total_amount_invalid")},
                    status_code=400,
                )
            updates.append("total_amount = ?")
            params.append(total_amount)

        next_status = po["status"]
        if "status" in payload:
            status = str(payload.get("status") or "").strip()
            if status not in allowed_statuses:
                return ServiceOutput(
                    payload={"error": "status_invalid", "message": err_fn("status_invalid")},
                    status_code=400,
                )
            if status in {"sent_to_erp", "erp_accepted", "erp_error", "partially_received", "received"} and status != po["status"]:
                forbidden_action_fn("ordem_compra", po["status"], "push_to_erp", http_status=400)
            required_action = "cancel_order" if status == "cancelled" else "edit_order"
            if not flow_action_allowed_fn("ordem_compra", po["status"], required_action):
                forbidden_action_fn("ordem_compra", po["status"], required_action)
            if status == "cancelled" and status != po["status"]:
                require_confirmation_fn(
                    "cancel_order",
                    entity="purchase_order",
                    entity_id=purchase_order_id,
                    payload=payload,
                )
            next_status = status
            updates.append("status = ?")
            params.append(status)

        if not updates:
            return ServiceOutput(
                payload={"error": "no_changes", "message": err_fn("no_changes")},
                status_code=400,
            )

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
        return ServiceOutput(
            payload={"purchase_order_id": purchase_order_id, "status": next_status},
            status_code=200,
        )

    def create_purchase_order_from_award(
        self,
        db,
        *,
        tenant_id: str,
        create_input: PurchaseOrderFromAwardInput,
        load_award_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        require_confirmation_fn,
        err_fn,
    ) -> ServiceOutput:
        award = load_award_fn(db, tenant_id, create_input.award_id)
        if not award:
            return ServiceOutput(
                payload={
                    "error": "award_not_found",
                    "message": err_fn("award_not_found"),
                    "award_id": create_input.award_id,
                },
                status_code=404,
            )
        if not flow_action_allowed_fn("decisao", award["status"], "create_purchase_order"):
            forbidden_action_fn("decisao", award["status"], "create_purchase_order")
        if award["purchase_order_id"]:
            return ServiceOutput(
                payload={
                    "error": "purchase_order_already_exists",
                    "message": err_fn("purchase_order_already_exists"),
                    "purchase_order_id": award["purchase_order_id"],
                },
                status_code=409,
            )

        require_confirmation_fn(
            "create_purchase_order",
            entity="award",
            entity_id=create_input.award_id,
            payload=create_input.payload,
        )

        po_number = f"OC-{create_input.award_id:04d}"
        supplier_name = award["supplier_name"] or "Fornecedor selecionado"
        cursor = db.execute(
            """
            INSERT INTO purchase_orders (number, award_id, supplier_name, status, total_amount, tenant_id)
            VALUES (?, ?, ?, 'approved', ?, ?)
            RETURNING id
            """,
            (po_number, create_input.award_id, supplier_name, 0.0, tenant_id),
        )
        po_row = cursor.fetchone()
        purchase_order_id = po_row["id"] if isinstance(po_row, dict) else po_row[0]

        db.execute(
            "UPDATE awards SET status = 'converted_to_po', purchase_order_id = ? WHERE id = ? AND tenant_id = ?",
            (purchase_order_id, create_input.award_id, tenant_id),
        )
        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('award', ?, ?, 'converted_to_po', 'award_converted_to_po', ?)
            """,
            (create_input.award_id, award["status"], tenant_id),
        )
        db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES ('purchase_order', ?, NULL, 'approved', 'po_created_from_award', ?)
            """,
            (purchase_order_id, tenant_id),
        )
        return ServiceOutput(
            payload={"purchase_order_id": purchase_order_id, "status": "approved"},
            status_code=201,
        )

    def register_erp_intent(
        self,
        db,
        *,
        tenant_id: str,
        intent_input: PurchaseOrderErpIntentInput,
        load_purchase_order_fn,
        find_pending_push_fn,
        flow_action_allowed_fn,
        forbidden_action_fn,
        require_confirmation_fn,
        queue_push_fn,
        process_outbox_fn=None,
        push_purchase_order_fn=None,
        immediate_response: bool = False,
        err_fn,
        ok_fn,
    ) -> ServiceOutput:
        po = load_purchase_order_fn(db, tenant_id, intent_input.purchase_order_id)
        if not po:
            return ServiceOutput(
                payload={
                    "error": "purchase_order_not_found",
                    "message": err_fn("purchase_order_not_found"),
                    "purchase_order_id": intent_input.purchase_order_id,
                },
                status_code=404,
            )
        if po["status"] == "erp_accepted":
            return ServiceOutput(
                payload={
                    "status": "erp_accepted",
                    "external_id": po["external_id"],
                    "message": ok_fn("order_already_accepted"),
                },
                status_code=200,
            )

        pending_run = self.outbox_service.find_pending(
            db,
            tenant_id=tenant_id,
            purchase_order_id=intent_input.purchase_order_id,
            find_pending_fn=find_pending_push_fn,
        )
        if pending_run:
            queued_payload = {
                "purchase_order_id": intent_input.purchase_order_id,
                "status": "sent_to_erp",
                "external_id": po["external_id"],
                "sync_run_id": int(pending_run["id"]),
                "queued": True,
                "message": ok_fn("order_sent_to_erp"),
            }
            if not immediate_response:
                return ServiceOutput(
                    payload=queued_payload,
                    status_code=200,
                )

            if process_outbox_fn and push_purchase_order_fn:
                summary = process_outbox_fn(
                    db,
                    tenant_id=tenant_id,
                    limit=1,
                    push_fn=push_purchase_order_fn,
                )
                latest_po = load_purchase_order_fn(db, tenant_id, intent_input.purchase_order_id)
                latest_status = str((latest_po or {}).get("status") or "").strip().lower()
                if latest_status == "erp_accepted":
                    return ServiceOutput(
                        payload={
                            "purchase_order_id": intent_input.purchase_order_id,
                            "status": "erp_accepted",
                            "external_id": (latest_po or {}).get("external_id"),
                            "sync_run_id": int(pending_run["id"]),
                            "queued": False,
                            "message": ok_fn("erp_accepted"),
                        },
                        status_code=200,
                    )
                if latest_status == "erp_error":
                    error_details = str((latest_po or {}).get("erp_last_error") or "").strip()
                    error_code, message_key, http_status = classify_erp_failure(error_details)
                    raise IntegrationError(
                        code=error_code,
                        message_key=message_key,
                        http_status=http_status,
                        critical=False,
                        details=error_details or None,
                        payload={
                            "purchase_order_id": intent_input.purchase_order_id,
                            "sync_run_id": int(pending_run["id"]),
                        },
                    )
                if (summary or {}).get("processed", 0) and not (summary or {}).get("succeeded", 0):
                    run = db.execute(
                        """
                        SELECT error_summary, error_details
                        FROM sync_runs
                        WHERE id = ? AND tenant_id = ?
                        LIMIT 1
                        """,
                        (int(pending_run["id"]), tenant_id),
                    ).fetchone()
                    details = str((run or {}).get("error_details") or "erp_push_failed")
                    raise RuntimeError(details)
            return ServiceOutput(
                payload={
                    **queued_payload,
                },
                status_code=200,
            )

        if not flow_action_allowed_fn("ordem_compra", po["status"], "push_to_erp"):
            forbidden_action_fn("ordem_compra", po["status"], "push_to_erp")

        require_confirmation_fn(
            "push_to_erp",
            entity="purchase_order",
            entity_id=intent_input.purchase_order_id,
            payload=intent_input.payload,
        )
        queued_result = self.outbox_service.register_erp_intent(
            db,
            tenant_id=tenant_id,
            purchase_order=po,
            intent_input=intent_input,
            queue_push_fn=queue_push_fn,
            success_message_fn=ok_fn,
        )
        if not immediate_response:
            return queued_result

        if process_outbox_fn and push_purchase_order_fn:
            summary = process_outbox_fn(
                db,
                tenant_id=tenant_id,
                limit=1,
                push_fn=push_purchase_order_fn,
            )
            latest_po = load_purchase_order_fn(db, tenant_id, intent_input.purchase_order_id)
            latest_status = str((latest_po or {}).get("status") or "").strip().lower()
            if latest_status == "erp_accepted":
                return ServiceOutput(
                    payload={
                        "purchase_order_id": intent_input.purchase_order_id,
                        "status": "erp_accepted",
                        "external_id": (latest_po or {}).get("external_id"),
                        "sync_run_id": queued_result.payload.get("sync_run_id"),
                        "queued": False,
                        "message": ok_fn("erp_accepted"),
                    },
                    status_code=200,
                )
            if latest_status == "erp_error":
                error_details = str((latest_po or {}).get("erp_last_error") or "").strip()
                error_code, message_key, http_status = classify_erp_failure(error_details)
                raise IntegrationError(
                    code=error_code,
                    message_key=message_key,
                    http_status=http_status,
                    critical=False,
                    details=error_details or None,
                    payload={
                        "purchase_order_id": intent_input.purchase_order_id,
                        "sync_run_id": queued_result.payload.get("sync_run_id"),
                    },
                )
            if (summary or {}).get("processed", 0) and not (summary or {}).get("succeeded", 0):
                sync_run_id = int(queued_result.payload.get("sync_run_id") or 0)
                run = db.execute(
                    """
                    SELECT error_summary, error_details
                    FROM sync_runs
                    WHERE id = ? AND tenant_id = ?
                    LIMIT 1
                    """,
                    (sync_run_id, tenant_id),
                ).fetchone()
                details = str((run or {}).get("error_details") or "erp_push_failed")
                raise RuntimeError(details)
        return queued_result

    def send_po_to_erp_intent(self, db, **kwargs) -> ServiceOutput:
        return self.register_erp_intent(db, **kwargs)
