from __future__ import annotations

from app.application.erp_outbox_service import ErpOutboxService
from app.domain.contracts import (
    PurchaseOrderErpIntentInput,
    PurchaseOrderFromAwardInput,
    PurchaseRequestCreateInput,
    RfqAwardInput,
    RfqCreateInput,
    ServiceOutput,
)


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
            return ServiceOutput(
                payload={
                    "purchase_order_id": intent_input.purchase_order_id,
                    "status": "sent_to_erp",
                    "external_id": po["external_id"],
                    "sync_run_id": int(pending_run["id"]),
                    "queued": True,
                    "message": ok_fn("order_sent_to_erp"),
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
        return self.outbox_service.register_erp_intent(
            db,
            tenant_id=tenant_id,
            purchase_order=po,
            intent_input=intent_input,
            queue_push_fn=queue_push_fn,
            success_message_fn=ok_fn,
        )

