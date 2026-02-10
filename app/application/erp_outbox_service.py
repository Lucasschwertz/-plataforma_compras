from __future__ import annotations

from app.domain.contracts import PurchaseOrderErpIntentInput, ServiceOutput


class ErpOutboxService:
    def find_pending(self, db, *, tenant_id: str, purchase_order_id: int, find_pending_fn) -> dict | None:
        return find_pending_fn(db, tenant_id, purchase_order_id)

    def register_erp_intent(
        self,
        db,
        *,
        tenant_id: str,
        purchase_order: dict,
        intent_input: PurchaseOrderErpIntentInput,
        queue_push_fn,
        success_message_fn,
    ) -> ServiceOutput:
        queue_result = queue_push_fn(
            db,
            tenant_id,
            dict(purchase_order),
            request_id=intent_input.request_id,
        )
        return ServiceOutput(
            payload={
                "purchase_order_id": intent_input.purchase_order_id,
                "status": "sent_to_erp",
                "external_id": purchase_order.get("external_id"),
                "sync_run_id": int(queue_result["sync_run_id"]),
                "queued": True,
                "already_queued": bool(queue_result.get("already_queued")),
                "message": success_message_fn("erp_send_queued", success_message_fn("order_sent_to_erp")),
            },
            status_code=200,
        )

