import os
import tempfile
import unittest
from unittest.mock import patch

from app import create_app
from app.config import Config
from app.db import close_db, get_db
from app.erp_client import ErpError


class ErpIntegrationFollowupTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        db_path = os.path.join(self._tmpdir.name, "plataforma_compras_test.db")

        class TempConfig(Config):
            DATABASE_DIR = self._tmpdir.name
            DB_PATH = db_path
            TESTING = True
            AUTH_ENABLED = False

        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.tenant_id = "tenant-erp-followup"
        self.headers = {"X-Tenant-Id": self.tenant_id}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._tmpdir.cleanup()

    def _set_role(self, role: str) -> None:
        with self.client.session_transaction() as session:
            session["tenant_id"] = self.tenant_id
            session["user_role"] = role
            session["user_email"] = f"{role}@demo.com"

    def _create_purchase_order(self) -> int:
        seed_res = self.client.post("/api/procurement/seed", headers=self.headers)
        self.assertIn(seed_res.status_code, (200, 201))

        items_res = self.client.get("/api/procurement/purchase-request-items/open", headers=self.headers)
        self.assertEqual(items_res.status_code, 200)
        item_ids = [item["id"] for item in (items_res.get_json() or {}).get("items", [])][:2]
        self.assertTrue(item_ids)

        rfq_res = self.client.post(
            "/api/procurement/rfqs",
            headers=self.headers,
            json={"title": "RFQ ERP Followup", "purchase_request_item_ids": item_ids},
        )
        self.assertEqual(rfq_res.status_code, 201)
        rfq_id = int(rfq_res.get_json()["id"])

        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "erp_followup_award", "supplier_name": "Fornecedor ERP", "confirm": True},
        )
        self.assertEqual(award_res.status_code, 201)
        award_id = int(award_res.get_json()["award_id"])

        po_res = self.client.post(
            f"/api/procurement/awards/{award_id}/purchase-orders?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(po_res.status_code, 201)
        return int(po_res.get_json()["purchase_order_id"])

    def test_purchase_order_detail_exposes_erp_status_and_timeline(self) -> None:
        purchase_order_id = self._create_purchase_order()

        push_res = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(push_res.status_code, 200)

        detail_res = self.client.get(
            f"/api/procurement/purchase-orders/{purchase_order_id}",
            headers=self.headers,
        )
        self.assertEqual(detail_res.status_code, 200)
        payload = detail_res.get_json()

        po = payload.get("purchase_order") or {}
        self.assertIn("erp_status", po)
        self.assertEqual((po.get("erp_status") or {}).get("key"), "aceito")
        self.assertTrue(((po.get("erp_status") or {}).get("message") or "").strip())

        timeline = payload.get("erp_timeline") or []
        self.assertTrue(timeline)
        event_types = {item.get("event_type") for item in timeline}
        self.assertIn("envio", event_types)
        self.assertIn("resposta", event_types)

        page_res = self.client.get(
            f"/procurement/purchase-orders/{purchase_order_id}",
            headers=self.headers,
        )
        self.assertEqual(page_res.status_code, 200)
        html = page_res.get_data(as_text=True)
        self.assertIn("Status ERP", html)

    def test_push_to_erp_is_idempotent_after_acceptance(self) -> None:
        purchase_order_id = self._create_purchase_order()

        first_push = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(first_push.status_code, 200)
        self.assertEqual(first_push.get_json().get("status"), "erp_accepted")

        with self.app.app_context():
            db = get_db()
            before = db.execute(
                """
                SELECT COUNT(*) AS total
                FROM sync_runs
                WHERE tenant_id = ? AND scope IN ('purchase_order', 'purchase_orders')
                """,
                (self.tenant_id,),
            ).fetchone()["total"]

        second_push = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(second_push.status_code, 200)
        self.assertEqual(second_push.get_json().get("status"), "erp_accepted")

        with self.app.app_context():
            db = get_db()
            after = db.execute(
                """
                SELECT COUNT(*) AS total
                FROM sync_runs
                WHERE tenant_id = ? AND scope IN ('purchase_order', 'purchase_orders')
                """,
                (self.tenant_id,),
            ).fetchone()["total"]
        self.assertEqual(before, after)

    def test_purchase_order_detail_can_skip_history_payload(self) -> None:
        purchase_order_id = self._create_purchase_order()

        response = self.client.get(
            f"/api/procurement/purchase-orders/{purchase_order_id}?include_history=0",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}

        self.assertFalse(payload.get("history_loaded"))
        self.assertEqual(payload.get("erp_timeline"), [])
        self.assertEqual(payload.get("sync_runs"), [])
        self.assertIn("erp_status", (payload.get("purchase_order") or {}))

    def test_erp_followup_requires_manager_or_admin(self) -> None:
        denied_page = self.client.get(
            "/procurement/integrations/erp",
            headers=self.headers,
        )
        self.assertEqual(denied_page.status_code, 403)

        denied_api = self.client.get(
            "/api/procurement/integrations/erp/orders",
            headers=self.headers,
        )
        self.assertEqual(denied_api.status_code, 403)

        self._set_role("manager")
        allowed_page = self.client.get(
            "/procurement/integrations/erp",
            headers=self.headers,
        )
        self.assertEqual(allowed_page.status_code, 200)

        allowed_api = self.client.get(
            "/api/procurement/integrations/erp/orders",
            headers=self.headers,
        )
        self.assertEqual(allowed_api.status_code, 200)

    def test_erp_followup_lists_rejected_order_for_resend(self) -> None:
        self._set_role("manager")
        purchase_order_id = self._create_purchase_order()

        with patch("app.routes.procurement_routes.push_purchase_order", side_effect=ErpError("ERP HTTP 422 rejected")):
            response = self.client.post(
                f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
                headers=self.headers,
            )
        self.assertEqual(response.status_code, 422)

        monitor_res = self.client.get(
            "/api/procurement/integrations/erp/orders",
            headers=self.headers,
        )
        self.assertEqual(monitor_res.status_code, 200)
        items = (monitor_res.get_json() or {}).get("items", [])
        row = next((item for item in items if int(item.get("purchase_order_id")) == purchase_order_id), None)
        self.assertIsNotNone(row)
        erp_status = row.get("erp_status") or {}
        self.assertEqual(erp_status.get("key"), "rejeitado")
        self.assertNotIn("ERP HTTP", erp_status.get("message") or "")
        self.assertTrue(row.get("can_resend"))
        self.assertEqual(row.get("next_action"), "push_to_erp")


if __name__ == "__main__":
    unittest.main()
