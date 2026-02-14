from __future__ import annotations

import logging

from app.core import EventBus, PurchaseOrderCreated
from app.domain.contracts import PurchaseOrderErpIntentInput, ServiceOutput


def _row_to_dict(value) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    keys = getattr(value, "keys", None)
    if callable(keys):
        return {key: value[key] for key in value.keys()}
    return {}


class ErpOutboxService:
    def __init__(self) -> None:
        self._logger = logging.getLogger("app")
        self._event_handlers_registered = False

    def register_event_handlers(self, event_bus: EventBus) -> None:
        if self._event_handlers_registered:
            return
        event_bus.subscribe(PurchaseOrderCreated, self._on_purchase_order_created)
        self._event_handlers_registered = True

    def _on_purchase_order_created(self, event: PurchaseOrderCreated) -> None:
        # ERP context listener hook for future orchestration without direct coupling.
        self._logger.debug(
            "erp_listener_purchase_order_created",
            extra={
                "tenant_id": event.tenant_id,
                "purchase_order_id": event.purchase_order_id,
                "event_id": event.event_id,
            },
        )

    def find_pending(self, db, *, tenant_id: str, purchase_order_id: int, find_pending_fn) -> dict | None:
        return find_pending_fn(db, tenant_id, purchase_order_id)

    def register_erp_intent(
        self,
        db,
        *,
        tenant_id: str,
        purchase_order,
        intent_input: PurchaseOrderErpIntentInput,
        queue_push_fn,
        success_message_fn,
    ) -> ServiceOutput:
        purchase_order_data = _row_to_dict(purchase_order)
        queue_result = queue_push_fn(
            db,
            tenant_id,
            dict(purchase_order_data),
            request_id=intent_input.request_id,
        )
        queue_payload = _row_to_dict(queue_result)
        sync_run_id = int(queue_payload.get("sync_run_id") or 0)
        return ServiceOutput(
            payload={
                "purchase_order_id": intent_input.purchase_order_id,
                "status": "sent_to_erp",
                "external_id": purchase_order_data.get("external_id"),
                "sync_run_id": sync_run_id,
                "queued": True,
                "already_queued": bool(queue_payload.get("already_queued")),
                "message": success_message_fn("erp_send_queued", success_message_fn("order_sent_to_erp")),
            },
            status_code=200,
        )
