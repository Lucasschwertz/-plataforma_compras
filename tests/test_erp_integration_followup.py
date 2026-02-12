import unittest

from app import create_app
from app.config import Config
from app.db import close_db, get_db
from app.domain.erp_gateway import ErpGatewayError
from tests.helpers.temp_db import TempDbSandbox
from tests.outbox_utils import process_erp_outbox_once


class ErpIntegrationFollowupTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="erp_followup")
        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=True,
            AUTH_ENABLED=False,
        )

        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.tenant_id = "tenant-erp-followup"
        self.headers = {"X-Tenant-Id": self.tenant_id}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()

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
        self.assertEqual((push_res.get_json() or {}).get("status"), "sent_to_erp")
        process_erp_outbox_once(self.app, tenant_id=self.tenant_id)

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
        self.assertEqual(first_push.get_json().get("status"), "sent_to_erp")
        process_erp_outbox_once(self.app, tenant_id=self.tenant_id)

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

        queued = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(queued.status_code, 200)
        self.assertEqual((queued.get_json() or {}).get("status"), "sent_to_erp")
        def _reject_once(_po: dict) -> dict:
            raise ErpGatewayError("ERP HTTP 422 rejected", definitive=True)

        process_erp_outbox_once(self.app, tenant_id=self.tenant_id, push_fn=_reject_once)

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

    def test_outbox_retry_for_temporary_erp_error_then_success(self) -> None:
        purchase_order_id = self._create_purchase_order()
        queued = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(queued.status_code, 200)
        self.assertEqual((queued.get_json() or {}).get("status"), "sent_to_erp")

        attempts = {"count": 0}

        def _retry_then_success(_po: dict) -> dict:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise ErpGatewayError("temporary timeout")
            return {"external_id": "ERP-ASYNC-1", "status": "accepted"}

        first = process_erp_outbox_once(self.app, tenant_id=self.tenant_id, push_fn=_retry_then_success)
        self.assertEqual(first.get("requeued"), 1)

        with self.app.app_context():
            db = get_db()
            payload_ref = (
                '{"kind":"po_push","purchase_order_id":'
                + str(purchase_order_id)
                + ',"next_attempt_at":"2000-01-01T00:00:00Z"}'
            )
            db.execute(
                """
                UPDATE sync_runs
                SET payload_ref = ?
                WHERE tenant_id = ? AND scope = 'purchase_order'
                """,
                (payload_ref, self.tenant_id),
            )
            db.commit()

        second = process_erp_outbox_once(self.app, tenant_id=self.tenant_id, push_fn=_retry_then_success)
        self.assertEqual(second.get("succeeded"), 1)

        detail = self.client.get(
            f"/api/procurement/purchase-orders/{purchase_order_id}",
            headers=self.headers,
        )
        self.assertEqual(detail.status_code, 200)
        self.assertEqual((detail.get_json() or {}).get("purchase_order", {}).get("status"), "erp_accepted")

    def test_outbox_enqueue_is_idempotent_before_worker_processing(self) -> None:
        purchase_order_id = self._create_purchase_order()

        first = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(first.status_code, 200)
        first_payload = first.get_json() or {}

        second = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp",
            headers=self.headers,
        )
        self.assertEqual(second.status_code, 200)
        second_payload = second.get_json() or {}
        self.assertEqual(int(first_payload.get("sync_run_id") or 0), int(second_payload.get("sync_run_id") or 0))

        with self.app.app_context():
            db = get_db()
            runs = db.execute(
                """
                SELECT COUNT(*) AS total
                FROM sync_runs
                WHERE tenant_id = ? AND scope = 'purchase_order'
                """,
                (self.tenant_id,),
            ).fetchone()["total"]
        self.assertEqual(runs, 1)


if __name__ == "__main__":
    unittest.main()
