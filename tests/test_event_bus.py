import unittest

from app.application.procurement_service import ProcurementService
from app.contexts.analytics.application.service import AnalyticsService
from app.core import EventBus, PurchaseOrderCreated, PurchaseRequestCreated, RfqCreated
from app.domain.contracts import ServiceOutput


class _FakeProcurementRepository:
    def create_purchase_request(self, *args, **kwargs):
        _ = (args, kwargs)
        return ServiceOutput(
            payload={"id": 41, "status": "pending_rfq", "items_created": 2},
            status_code=201,
        )


class EventBusTest(unittest.TestCase):
    def test_handler_execution_order_is_predictable(self) -> None:
        bus = EventBus()
        execution_trace = []

        def first_handler(_event):
            execution_trace.append("first")

        def second_handler(_event):
            execution_trace.append("second")

        bus.subscribe(PurchaseRequestCreated, first_handler)
        bus.subscribe(PurchaseRequestCreated, second_handler)
        bus.publish(
            PurchaseRequestCreated(
                tenant_id="tenant-a",
                purchase_request_id=1,
                status="pending_rfq",
                items_created=1,
            )
        )

        self.assertEqual(execution_trace, ["first", "second"])

    def test_procurement_service_emits_purchase_request_created(self) -> None:
        bus = EventBus()
        received_events = []
        bus.subscribe(PurchaseRequestCreated, received_events.append)

        service = ProcurementService(repository=_FakeProcurementRepository(), event_bus=bus)
        result = service.create_purchase_request(db=None, tenant_id="tenant-a")

        self.assertEqual(result.status_code, 201)
        self.assertEqual(len(received_events), 1)
        event = received_events[0]
        self.assertEqual(event.tenant_id, "tenant-a")
        self.assertEqual(event.purchase_request_id, 41)
        self.assertEqual(event.items_created, 2)

    def test_analytics_cache_handler_runs_when_event_is_published(self) -> None:
        bus = EventBus()
        service = AnalyticsService(ttl_seconds=60)
        cache_key = ("tenant-a", "overview")
        service._cache_set(cache_key, {"kpi": "value"})
        self.assertIsNotNone(service._cache_get(cache_key))

        service.register_event_handlers(bus)
        bus.publish(
            PurchaseOrderCreated(
                tenant_id="tenant-a",
                purchase_order_id=7,
                status="approved",
                source="manual",
            )
        )

        self.assertIsNone(service._cache_get(cache_key))

    def test_analytics_handler_also_receives_rfq_events(self) -> None:
        bus = EventBus()
        service = AnalyticsService(ttl_seconds=60)
        calls = {"count": 0}

        original_clear_cache = service.clear_cache

        def counted_clear_cache():
            calls["count"] += 1
            original_clear_cache()

        service.clear_cache = counted_clear_cache  # type: ignore[method-assign]
        service.register_event_handlers(bus)
        bus.publish(RfqCreated(tenant_id="tenant-a", rfq_id=55, title="RFQ Test"))

        self.assertEqual(calls["count"], 1)


if __name__ == "__main__":
    unittest.main()
