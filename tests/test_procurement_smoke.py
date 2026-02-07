import os
import tempfile
import unittest

from app import create_app
from app.config import Config
from app.db import close_db


class ProcurementSmokeTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
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

    def _json(self, response):
        return response.get_json()

    def test_happy_path_flow(self) -> None:
        # Seed data for the company.
        seed_res = self.client.post("/api/procurement/seed", headers=self.headers)
        self.assertIn(seed_res.status_code, (200, 201))

        inbox_res = self.client.get("/api/procurement/inbox", headers=self.headers)
        self.assertEqual(inbox_res.status_code, 200)
        inbox = self._json(inbox_res)
        self.assertTrue(inbox["items"], "expected seeded inbox items")

        items_res = self.client.get(
            "/api/procurement/purchase-request-items/open",
            headers=self.headers,
        )
        self.assertEqual(items_res.status_code, 200)
        items_payload = self._json(items_res)
        item_ids = [item["id"] for item in items_payload.get("items", [])]
        self.assertTrue(item_ids, "expected purchase request items for rfq creation")

        rfq_res = self.client.post(
            "/api/procurement/rfqs",
            headers=self.headers,
            json={"title": "Smoke RFQ", "purchase_request_item_ids": item_ids[:2]},
        )
        self.assertEqual(rfq_res.status_code, 201)
        rfq_payload = self._json(rfq_res)
        rfq_id = rfq_payload["id"]
        self.assertTrue(rfq_payload.get("created_at"))
        self.assertEqual(len(rfq_payload.get("rfq_items", [])), 2)

        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "smoke_award", "supplier_name": "Fornecedor Smoke"},
        )
        self.assertEqual(award_res.status_code, 201)
        award_id = self._json(award_res)["award_id"]

        po_res = self.client.post(
            f"/api/procurement/awards/{award_id}/purchase-orders",
            headers=self.headers,
        )
        self.assertEqual(po_res.status_code, 201)
        purchase_order_id = self._json(po_res)["purchase_order_id"]

        push_res = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp",
            headers=self.headers,
        )
        self.assertEqual(push_res.status_code, 200)
        push_payload = self._json(push_res)
        self.assertEqual(push_payload["status"], "erp_accepted")

        detail_res = self.client.get(
            f"/api/procurement/purchase-orders/{purchase_order_id}",
            headers=self.headers,
        )
        self.assertEqual(detail_res.status_code, 200)
        detail_payload = self._json(detail_res)
        self.assertEqual(detail_payload["purchase_order"]["status"], "erp_accepted")

        logs_res = self.client.get(
            "/api/procurement/integrations/logs?scope=purchase_order",
            headers=self.headers,
        )
        self.assertEqual(logs_res.status_code, 200)
        logs_payload = self._json(logs_res)
        self.assertTrue(
            any(run["scope"] == "purchase_order" for run in logs_payload["sync_runs"]),
            "expected at least one purchase_order sync run",
        )

    def test_proposal_requires_supplier_invite_per_item(self) -> None:
        seed_res = self.client.post("/api/procurement/seed", headers=self.headers)
        self.assertIn(seed_res.status_code, (200, 201))

        items_res = self.client.get(
            "/api/procurement/purchase-request-items/open",
            headers=self.headers,
        )
        self.assertEqual(items_res.status_code, 200)
        item_ids = [item["id"] for item in self._json(items_res).get("items", [])][:2]
        self.assertEqual(len(item_ids), 2, "expected at least two items for invitation flow test")

        rfq_res = self.client.post(
            "/api/procurement/rfqs",
            headers=self.headers,
            json={"title": "RFQ convite por item", "purchase_request_item_ids": item_ids},
        )
        self.assertEqual(rfq_res.status_code, 201)
        rfq_id = self._json(rfq_res)["id"]

        suppliers_res = self.client.get("/api/procurement/fornecedores", headers=self.headers)
        self.assertEqual(suppliers_res.status_code, 200)
        suppliers = self._json(suppliers_res).get("items", [])
        self.assertTrue(suppliers, "expected at least one supplier")
        supplier_id = suppliers[0]["id"]

        detail_res = self.client.get(f"/api/procurement/cotacoes/{rfq_id}", headers=self.headers)
        self.assertEqual(detail_res.status_code, 200)
        rfq_items = self._json(detail_res).get("itens", [])
        self.assertEqual(len(rfq_items), 2)
        rfq_item_ids = [item["rfq_item_id"] for item in rfq_items]
        first_item_id, second_item_id = rfq_item_ids[0], rfq_item_ids[1]

        blocked_res = self.client.post(
            f"/api/procurement/cotacoes/{rfq_id}/propostas",
            headers=self.headers,
            json={
                "supplier_id": supplier_id,
                "items": [
                    {"rfq_item_id": first_item_id, "unit_price": 10, "lead_time_days": 7},
                    {"rfq_item_id": second_item_id, "unit_price": 20, "lead_time_days": 8},
                ],
            },
        )
        self.assertEqual(blocked_res.status_code, 400)
        blocked_payload = self._json(blocked_res)
        self.assertEqual(blocked_payload.get("error"), "supplier_not_invited_for_items")
        self.assertIn(first_item_id, blocked_payload.get("rfq_item_ids", []))
        self.assertIn(second_item_id, blocked_payload.get("rfq_item_ids", []))

        invite_res = self.client.post(
            f"/api/procurement/cotacoes/{rfq_id}/itens/{first_item_id}/fornecedores",
            headers=self.headers,
            json={"supplier_ids": [supplier_id]},
        )
        self.assertEqual(invite_res.status_code, 200)

        partial_block_res = self.client.post(
            f"/api/procurement/cotacoes/{rfq_id}/propostas",
            headers=self.headers,
            json={
                "supplier_id": supplier_id,
                "items": [
                    {"rfq_item_id": first_item_id, "unit_price": 11, "lead_time_days": 6},
                    {"rfq_item_id": second_item_id, "unit_price": 21, "lead_time_days": 9},
                ],
            },
        )
        self.assertEqual(partial_block_res.status_code, 400)
        partial_block_payload = self._json(partial_block_res)
        self.assertEqual(partial_block_payload.get("error"), "supplier_not_invited_for_items")
        self.assertIn(second_item_id, partial_block_payload.get("rfq_item_ids", []))
        self.assertNotIn(first_item_id, partial_block_payload.get("rfq_item_ids", []))

        success_res = self.client.post(
            f"/api/procurement/cotacoes/{rfq_id}/propostas",
            headers=self.headers,
            json={
                "supplier_id": supplier_id,
                "items": [
                    {"rfq_item_id": first_item_id, "unit_price": 12, "lead_time_days": 5},
                ],
            },
        )
        self.assertEqual(success_res.status_code, 200)
        success_payload = self._json(success_res)
        self.assertEqual(success_payload.get("saved_items"), 1)

        updated_detail_res = self.client.get(f"/api/procurement/cotacoes/{rfq_id}", headers=self.headers)
        self.assertEqual(updated_detail_res.status_code, 200)
        updated_items = self._json(updated_detail_res).get("itens", [])
        by_item_id = {item["rfq_item_id"]: item for item in updated_items}

        first_item_suppliers = by_item_id[first_item_id].get("fornecedores", [])
        self.assertTrue(
            any(
                supplier["supplier_id"] == supplier_id and supplier.get("unit_price") is not None
                for supplier in first_item_suppliers
            )
        )

        second_item_suppliers = by_item_id[second_item_id].get("fornecedores", [])
        self.assertFalse(
            any(
                supplier["supplier_id"] == supplier_id and supplier.get("unit_price") is not None
                for supplier in second_item_suppliers
            )
        )


if __name__ == "__main__":
    unittest.main()

