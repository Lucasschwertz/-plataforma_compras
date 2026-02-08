import os
import tempfile
import unittest

from app import create_app
from app.config import Config
from app.db import close_db


class ProcurementFlowPolicyTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(
            prefix="pc_flow_test_",
            dir=os.getcwd(),
            ignore_cleanup_errors=True,
        )
        db_path = os.path.join(self._tmpdir.name, "plataforma_compras_test.db")

        class TempConfig(Config):
            DATABASE_DIR = self._tmpdir.name
            DB_PATH = db_path
            TESTING = True

        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.tenant_id = "tenant-1"
        self.headers = {"X-Tenant-Id": self.tenant_id}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._tmpdir.cleanup()

    @staticmethod
    def _json(response):
        return response.get_json()

    def _create_rfq(self) -> int:
        seed_res = self.client.post("/api/procurement/seed", headers=self.headers)
        self.assertIn(seed_res.status_code, (200, 201))

        items_res = self.client.get("/api/procurement/purchase-request-items/open", headers=self.headers)
        self.assertEqual(items_res.status_code, 200)
        item_ids = [item["id"] for item in self._json(items_res).get("items", [])][:2]
        self.assertTrue(item_ids)

        rfq_res = self.client.post(
            "/api/procurement/rfqs",
            headers=self.headers,
            json={"title": "RFQ Flow Policy", "purchase_request_item_ids": item_ids},
        )
        self.assertEqual(rfq_res.status_code, 201)
        return int(self._json(rfq_res)["id"])

    def _award_and_create_po(self, rfq_id: int) -> int:
        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "flow_policy_award", "supplier_name": "Fornecedor Fluxo", "confirm": True},
        )
        self.assertEqual(award_res.status_code, 201)
        award_id = int(self._json(award_res)["award_id"])

        po_res = self.client.post(
            f"/api/procurement/awards/{award_id}/purchase-orders?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(po_res.status_code, 201)
        return int(self._json(po_res)["purchase_order_id"])

    def test_invalid_action_is_blocked_by_status(self) -> None:
        rfq_id = self._create_rfq()
        purchase_order_id = self._award_and_create_po(rfq_id)

        push_res = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(push_res.status_code, 200)
        self.assertEqual(self._json(push_res)["status"], "erp_accepted")

        cancel_res = self.client.delete(
            f"/api/procurement/purchase-orders/{purchase_order_id}",
            headers=self.headers,
        )
        self.assertEqual(cancel_res.status_code, 409)
        payload = self._json(cancel_res)
        self.assertEqual(payload.get("error"), "action_not_allowed_for_status")

    def test_primary_action_executes_successfully(self) -> None:
        rfq_id = self._create_rfq()
        purchase_order_id = self._award_and_create_po(rfq_id)

        detail_before = self.client.get(
            f"/api/procurement/purchase-orders/{purchase_order_id}",
            headers=self.headers,
        )
        self.assertEqual(detail_before.status_code, 200)
        detail_payload = self._json(detail_before)
        self.assertEqual(detail_payload["purchase_order"]["primary_action"], "push_to_erp")
        self.assertIn("push_to_erp", detail_payload["purchase_order"]["allowed_actions"])

        push_res = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(push_res.status_code, 200)
        self.assertEqual(self._json(push_res)["status"], "erp_accepted")

    def test_happy_flow_stage_progression(self) -> None:
        rfq_id = self._create_rfq()

        detail_rfq = self.client.get(f"/api/procurement/cotacoes/{rfq_id}", headers=self.headers)
        self.assertEqual(detail_rfq.status_code, 200)
        self.assertEqual(self._json(detail_rfq)["flow"]["process_stage"], "cotacao")

        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "flow_progress", "supplier_name": "Fornecedor Progresso", "confirm": True},
        )
        self.assertEqual(award_res.status_code, 201)
        award_id = int(self._json(award_res)["award_id"])

        detail_awarded = self.client.get(f"/api/procurement/cotacoes/{rfq_id}", headers=self.headers)
        self.assertEqual(detail_awarded.status_code, 200)
        self.assertEqual(self._json(detail_awarded)["flow"]["process_stage"], "decisao")

        po_res = self.client.post(
            f"/api/procurement/awards/{award_id}/purchase-orders?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(po_res.status_code, 201)
        purchase_order_id = int(self._json(po_res)["purchase_order_id"])

        detail_po = self.client.get(
            f"/api/procurement/purchase-orders/{purchase_order_id}",
            headers=self.headers,
        )
        self.assertEqual(detail_po.status_code, 200)
        self.assertEqual(self._json(detail_po)["purchase_order"]["process_stage"], "ordem_compra")

        push_res = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(push_res.status_code, 200)
        self.assertEqual(self._json(push_res)["status"], "erp_accepted")

        detail_erp = self.client.get(
            f"/api/procurement/purchase-orders/{purchase_order_id}",
            headers=self.headers,
        )
        self.assertEqual(detail_erp.status_code, 200)
        self.assertEqual(self._json(detail_erp)["purchase_order"]["process_stage"], "erp")


if __name__ == "__main__":
    unittest.main()
