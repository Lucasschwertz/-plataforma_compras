from __future__ import annotations

from app.core import (
    EventBus,
    PurchaseOrderCreated,
    PurchaseRequestCreated,
    RfqAwarded,
    RfqCreated,
    get_event_bus,
)
from app.contexts.erp.application.outbox_service import ErpOutboxService
from app.contexts.procurement.infrastructure.repositories import LegacyProcurementRepository
from app.domain.contracts import ServiceOutput


class ProcurementService:
    """Application facade: delegates data access and SQL-heavy operations to repositories."""

    def __init__(
        self,
        outbox_service: ErpOutboxService | None = None,
        repository: LegacyProcurementRepository | None = None,
        event_bus: EventBus | None = None,
    ) -> None:
        self.outbox_service = outbox_service or ErpOutboxService()
        self.repository = repository or LegacyProcurementRepository(outbox_service=self.outbox_service)
        self.event_bus = event_bus or get_event_bus()

    @staticmethod
    def _is_success(result: ServiceOutput) -> bool:
        return 200 <= int(result.status_code) < 300

    @staticmethod
    def _tenant_id(value) -> str:
        return str(value or "").strip()

    @staticmethod
    def _as_int(value) -> int | None:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    def _publish(self, event) -> None:
        self.event_bus.publish(event)

    def create_purchase_request(self, *args, **kwargs) -> ServiceOutput:
        result = self.repository.create_purchase_request(*args, **kwargs)
        if self._is_success(result):
            payload = dict(result.payload or {})
            purchase_request_id = self._as_int(payload.get("id"))
            tenant_id = self._tenant_id(kwargs.get("tenant_id"))
            if purchase_request_id and tenant_id:
                self._publish(
                    PurchaseRequestCreated(
                        tenant_id=tenant_id,
                        purchase_request_id=purchase_request_id,
                        status=str(payload.get("status") or ""),
                        items_created=int(payload.get("items_created") or 0),
                    )
                )
        return result

    def create_rfq(self, *args, **kwargs) -> ServiceOutput:
        result = self.repository.create_rfq(*args, **kwargs)
        if self._is_success(result):
            payload = dict(result.payload or {})
            rfq_id = self._as_int(payload.get("id"))
            tenant_id = self._tenant_id(kwargs.get("tenant_id"))
            if rfq_id and tenant_id:
                self._publish(
                    RfqCreated(
                        tenant_id=tenant_id,
                        rfq_id=rfq_id,
                        title=str(payload.get("title") or ""),
                    )
                )
        return result

    def create_rfq_with_suppliers(self, *args, **kwargs) -> ServiceOutput:
        result = self.repository.create_rfq_with_suppliers(*args, **kwargs)
        if self._is_success(result):
            payload = dict(result.payload or {})
            rfq_block = payload.get("rfq") if isinstance(payload.get("rfq"), dict) else {}
            rfq_id = self._as_int((rfq_block or {}).get("id") or payload.get("id"))
            tenant_id = self._tenant_id(kwargs.get("tenant_id"))
            if rfq_id and tenant_id:
                title = str((rfq_block or {}).get("title") or payload.get("title") or "")
                self._publish(
                    RfqCreated(
                        tenant_id=tenant_id,
                        rfq_id=rfq_id,
                        title=title,
                    )
                )
        return result

    def award_rfq(self, *args, **kwargs) -> ServiceOutput:
        result = self.repository.award_rfq(*args, **kwargs)
        if self._is_success(result):
            payload = dict(result.payload or {})
            rfq_id = self._as_int(payload.get("rfq_id"))
            award_id = self._as_int(payload.get("award_id"))
            tenant_id = self._tenant_id(kwargs.get("tenant_id"))
            if rfq_id and award_id and tenant_id:
                self._publish(
                    RfqAwarded(
                        tenant_id=tenant_id,
                        rfq_id=rfq_id,
                        award_id=award_id,
                    )
                )
        return result

    def create_purchase_order(self, *args, **kwargs) -> ServiceOutput:
        result = self.repository.create_purchase_order(*args, **kwargs)
        if self._is_success(result):
            payload = dict(result.payload or {})
            purchase_order_id = self._as_int(payload.get("id"))
            tenant_id = self._tenant_id(kwargs.get("tenant_id"))
            if purchase_order_id and tenant_id:
                self._publish(
                    PurchaseOrderCreated(
                        tenant_id=tenant_id,
                        purchase_order_id=purchase_order_id,
                        status=str(payload.get("status") or ""),
                        source="manual",
                    )
                )
        return result

    def create_purchase_order_from_award(self, *args, **kwargs) -> ServiceOutput:
        result = self.repository.create_purchase_order_from_award(*args, **kwargs)
        if self._is_success(result):
            payload = dict(result.payload or {})
            purchase_order_id = self._as_int(payload.get("purchase_order_id") or payload.get("id"))
            tenant_id = self._tenant_id(kwargs.get("tenant_id"))
            if purchase_order_id and tenant_id:
                self._publish(
                    PurchaseOrderCreated(
                        tenant_id=tenant_id,
                        purchase_order_id=purchase_order_id,
                        status=str(payload.get("status") or ""),
                        source="award",
                    )
                )
        return result

    def __getattr__(self, item: str):
        try:
            return getattr(self.repository, item)
        except AttributeError as exc:  # pragma: no cover - defensive guard
            raise AttributeError(f"{self.__class__.__name__} has no attribute '{item}'") from exc
