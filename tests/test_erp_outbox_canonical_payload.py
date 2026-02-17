from __future__ import annotations

import json
import unittest

from app import create_app
from app.config import Config
from app.db import close_db, get_db
from app.ui_strings import error_message
from tests.helpers.temp_db import TempDbSandbox
from tests.outbox_utils import process_erp_outbox_once


class ErpOutboxCanonicalPayloadTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="erp_outbox_canonical")
        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=True,
            AUTH_ENABLED=False,
        )
        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.tenant_id = "tenant-erp-canonical"
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
            json={"title": "RFQ Outbox Canonical", "purchase_request_item_ids": item_ids},
        )
        self.assertEqual(rfq_res.status_code, 201)
        rfq_id = int((rfq_res.get_json() or {}).get("id") or 0)
        self.assertGreater(rfq_id, 0)

        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "outbox_acl", "supplier_name": "Fornecedor ACL", "confirm": True},
        )
        self.assertEqual(award_res.status_code, 201)
        award_id = int((award_res.get_json() or {}).get("award_id") or 0)
        self.assertGreater(award_id, 0)

        po_res = self.client.post(
            f"/api/procurement/awards/{award_id}/purchase-orders?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(po_res.status_code, 201)
        return int((po_res.get_json() or {}).get("purchase_order_id") or 0)

    def _queue_purchase_order(self, purchase_order_id: int) -> None:
        queued = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(queued.status_code, 200)
        self.assertEqual((queued.get_json() or {}).get("status"), "sent_to_erp")

    def test_queue_stores_canonical_contract_payload(self) -> None:
        purchase_order_id = self._create_purchase_order()
        self._queue_purchase_order(purchase_order_id)

        with self.app.app_context():
            db = get_db()
            row = db.execute(
                """
                SELECT payload_ref
                FROM sync_runs
                WHERE tenant_id = ? AND scope = 'purchase_order'
                ORDER BY id DESC
                LIMIT 1
                """,
                (self.tenant_id,),
            ).fetchone()
            payload_ref = json.loads(str((dict(row) if row else {}).get("payload_ref") or "{}"))

        canonical = payload_ref.get("canonical_po") or {}
        self.assertEqual(canonical.get("schema_name"), "erp.purchase_order")
        self.assertEqual(int(canonical.get("schema_version") or 0), 1)
        self.assertEqual(canonical.get("workspace_id"), self.tenant_id)
        self.assertEqual(str(canonical.get("external_ref") or ""), str(purchase_order_id))

    def test_canonical_payload_is_processed_by_worker(self) -> None:
        purchase_order_id = self._create_purchase_order()
        self._queue_purchase_order(purchase_order_id)

        result = process_erp_outbox_once(
            self.app,
            tenant_id=self.tenant_id,
            push_fn=lambda _po: {"status": "accepted", "external_id": "ERP-CANONICAL-1"},
        )
        self.assertEqual(result.get("succeeded"), 1)

        with self.app.app_context():
            db = get_db()
            po_row = db.execute(
                "SELECT status, external_id FROM purchase_orders WHERE id = ? AND tenant_id = ?",
                (purchase_order_id, self.tenant_id),
            ).fetchone()
            po = dict(po_row) if po_row else {}
            self.assertEqual(po.get("status"), "erp_accepted")
            self.assertEqual(po.get("external_id"), "ERP-CANONICAL-1")

    def test_invalid_contract_marks_definitive_rejection(self) -> None:
        purchase_order_id = self._create_purchase_order()
        self._queue_purchase_order(purchase_order_id)

        calls = {"count": 0}

        with self.app.app_context():
            db = get_db()
            row = db.execute(
                """
                SELECT id, payload_ref
                FROM sync_runs
                WHERE tenant_id = ? AND scope = 'purchase_order'
                ORDER BY id DESC
                LIMIT 1
                """,
                (self.tenant_id,),
            ).fetchone()
            run = dict(row) if row else {}
            payload_ref = json.loads(str(run.get("payload_ref") or "{}"))
            canonical = dict(payload_ref.get("canonical_po") or {})
            canonical["lines"] = []
            payload_ref["next_attempt_at"] = "2000-01-01T00:00:00Z"
            payload_ref["canonical_po"] = canonical
            db.execute(
                "UPDATE sync_runs SET payload_ref = ? WHERE id = ? AND tenant_id = ?",
                (json.dumps(payload_ref, ensure_ascii=True, separators=(",", ":")), int(run.get("id") or 0), self.tenant_id),
            )
            db.commit()

        result = process_erp_outbox_once(
            self.app,
            tenant_id=self.tenant_id,
            push_fn=lambda _po: calls.__setitem__("count", calls["count"] + 1) or {"status": "accepted"},
        )
        self.assertEqual(result.get("failed"), 1)
        self.assertEqual(calls["count"], 0)

        with self.app.app_context():
            db = get_db()
            run_row = db.execute(
                """
                SELECT status, payload_ref
                FROM sync_runs
                WHERE tenant_id = ? AND scope = 'purchase_order'
                ORDER BY id DESC
                LIMIT 1
                """,
                (self.tenant_id,),
            ).fetchone()
            run = dict(run_row) if run_row else {}
            self.assertEqual(run.get("status"), "failed")
            run_meta = json.loads(str(run.get("payload_ref") or "{}"))
            self.assertTrue(bool(run_meta.get("dead_letter")))

            po_row = db.execute(
                "SELECT status, erp_last_error FROM purchase_orders WHERE id = ? AND tenant_id = ?",
                (purchase_order_id, self.tenant_id),
            ).fetchone()
            po = dict(po_row) if po_row else {}
            self.assertEqual(po.get("status"), "erp_error")
            self.assertEqual(po.get("erp_last_error"), error_message("erp_contract_invalid"))

        metrics = self.client.get("/metrics").get_data(as_text=True)
        self.assertIn("erp_contract_invalid_total", metrics)

    def test_contract_health_endpoint_is_admin_only(self) -> None:
        self._set_role("buyer")
        denied = self.client.get("/internal/erp/contract-health", headers=self.headers)
        self.assertEqual(denied.status_code, 403)

        self._set_role("admin")
        allowed = self.client.get("/internal/erp/contract-health", headers=self.headers)
        self.assertEqual(allowed.status_code, 200)
        payload = allowed.get_json() or {}
        self.assertIn("invalid_contract_total", payload)
        self.assertIn("mapper_validation_failed_total", payload)
        self.assertIn("last_10_failures", payload)


if __name__ == "__main__":
    unittest.main()

