import unittest

from app import create_app
from app.config import Config
from app.db import close_db
from app.ui_strings import error_message
from tests.helpers.temp_db import TempDbSandbox


class CriticalActionConfirmationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="critical_confirm")
        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=True,
            AUTH_ENABLED=False,
            SYNC_SCHEDULER_ENABLED=False,
        )

        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.tenant_id = "tenant-confirm"
        self.headers = {"X-Tenant-Id": self.tenant_id}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()

    def _create_rfq(self) -> int:
        seed_res = self.client.post("/api/procurement/seed", headers=self.headers)
        self.assertIn(seed_res.status_code, (200, 201))

        items_res = self.client.get("/api/procurement/purchase-request-items/open", headers=self.headers)
        self.assertEqual(items_res.status_code, 200)
        item_ids = [item["id"] for item in (items_res.get_json() or {}).get("items", [])][:2]
        self.assertTrue(item_ids)

        rfq_res = self.client.post(
            "/api/procurement/rfqs",
            headers=self.headers,
            json={"title": "RFQ Confirmacao", "purchase_request_item_ids": item_ids},
        )
        self.assertEqual(rfq_res.status_code, 201)
        return int(rfq_res.get_json()["id"])

    def _create_purchase_order(self) -> int:
        rfq_id = self._create_rfq()
        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "confirm_test", "supplier_name": "Fornecedor Confirmado", "confirm": True},
        )
        self.assertEqual(award_res.status_code, 201)
        award_id = int(award_res.get_json()["award_id"])

        po_res = self.client.post(
            f"/api/procurement/awards/{award_id}/purchase-orders?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(po_res.status_code, 201)
        return int(po_res.get_json()["purchase_order_id"])

    def test_push_to_erp_without_confirmation_fails(self) -> None:
        purchase_order_id = self._create_purchase_order()

        response = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertEqual(payload.get("error"), "confirmation_required")
        self.assertEqual(payload.get("message"), error_message("confirmation_required"))
        self.assertEqual((payload.get("confirmation") or {}).get("action_key"), "push_to_erp")

    def test_push_to_erp_with_confirmation_passes(self) -> None:
        purchase_order_id = self._create_purchase_order()

        response = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload.get("status"), "sent_to_erp")

    def test_award_without_confirmation_blocks_direct_api(self) -> None:
        rfq_id = self._create_rfq()

        response = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "direct_call_without_confirm", "supplier_name": "Fornecedor X"},
        )
        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertEqual(payload.get("error"), "confirmation_required")
        self.assertEqual((payload.get("confirmation") or {}).get("action_key"), "award_rfq")

    def test_cancel_request_returns_friendly_confirmation_message(self) -> None:
        create_res = self.client.post(
            "/api/procurement/solicitacoes",
            headers=self.headers,
            json={
                "number": "SR-CONFIRM-1",
                "priority": "high",
                "requested_by": "Comprador Confirmacao",
                "department": "Compras",
                "needed_at": "2026-02-28",
                "items": [{"description": "Item teste", "quantity": 1, "uom": "UN"}],
            },
        )
        self.assertEqual(create_res.status_code, 201)
        request_id = int(create_res.get_json()["id"])

        cancel_res = self.client.delete(
            f"/api/procurement/solicitacoes/{request_id}",
            headers=self.headers,
        )
        self.assertEqual(cancel_res.status_code, 400)
        payload = cancel_res.get_json()
        self.assertEqual(payload.get("error"), "confirmation_required")
        self.assertEqual(payload.get("message"), error_message("confirmation_required"))
        self.assertEqual((payload.get("confirmation") or {}).get("action_key"), "cancel_request")


if __name__ == "__main__":
    unittest.main()
