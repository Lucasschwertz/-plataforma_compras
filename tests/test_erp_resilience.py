import json
import unittest
from datetime import datetime, timezone

from app import create_app
from app.config import Config
from app.contexts.erp.domain.gateway import ErpGatewayError
from app.contexts.erp.infrastructure.circuit_breaker import reset_erp_circuit_breaker_for_tests
from app.db import close_db, get_db
from app.observability import reset_metrics_for_tests
from tests.helpers.temp_db import TempDbSandbox
from tests.outbox_utils import process_erp_outbox_once


class ErpResilienceTest(unittest.TestCase):
    def _build_app(self, **config_overrides):
        temp_db = TempDbSandbox(prefix="erp_resilience")
        TempConfig = temp_db.make_config(
            Config,
            TESTING=True,
            AUTH_ENABLED=False,
            **config_overrides,
        )
        app = create_app(TempConfig)
        return temp_db, app

    def setUp(self) -> None:
        self._temp_db, self.app = self._build_app()
        self.client = self.app.test_client()
        self.tenant_id = "tenant-erp-resilience"
        self.headers = {"X-Tenant-Id": self.tenant_id}
        reset_metrics_for_tests()
        reset_erp_circuit_breaker_for_tests()

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()
        reset_metrics_for_tests()
        reset_erp_circuit_breaker_for_tests()

    def _rebuild_with_config(self, **config_overrides):
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()
        self._temp_db, self.app = self._build_app(**config_overrides)
        self.client = self.app.test_client()
        reset_metrics_for_tests()
        reset_erp_circuit_breaker_for_tests()

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
            json={"title": "RFQ ERP Resilience", "purchase_request_item_ids": item_ids},
        )
        self.assertEqual(rfq_res.status_code, 201)
        rfq_id = int((rfq_res.get_json() or {}).get("id") or 0)
        self.assertGreater(rfq_id, 0)

        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "erp_resilience_award", "supplier_name": "Fornecedor ERP", "confirm": True},
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

    def _queue_po(self, purchase_order_id: int) -> None:
        queued = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(queued.status_code, 200)
        self.assertEqual((queued.get_json() or {}).get("status"), "sent_to_erp")

    def _force_run_due(self, purchase_order_id: int) -> None:
        payload_ref = (
            '{"kind":"po_push","purchase_order_id":'
            + str(purchase_order_id)
            + ',"next_attempt_at":"2000-01-01T00:00:00Z"}'
        )
        with self.app.app_context():
            db = get_db()
            db.execute(
                """
                UPDATE sync_runs
                SET payload_ref = ?
                WHERE tenant_id = ? AND scope = 'purchase_order'
                """,
                (payload_ref, self.tenant_id),
            )
            db.commit()

    def _latest_run(self) -> dict:
        with self.app.app_context():
            db = get_db()
            row = db.execute(
                """
                SELECT status, attempt, payload_ref
                FROM sync_runs
                WHERE tenant_id = ? AND scope = 'purchase_order'
                ORDER BY id DESC
                LIMIT 1
                """,
                (self.tenant_id,),
            ).fetchone()
            return dict(row) if row else {}

    def test_circuit_breaker_opens_and_blocks_outbox_call(self) -> None:
        self._rebuild_with_config(
            ERP_CIRCUIT_ENABLED=True,
            ERP_CIRCUIT_ERROR_RATE_THRESHOLD=1.0,
            ERP_CIRCUIT_MIN_SAMPLES=1,
            ERP_CIRCUIT_OPEN_SECONDS=120,
            ERP_OUTBOX_BACKOFF_SECONDS=1,
            ERP_OUTBOX_MAX_BACKOFF_SECONDS=2,
            ERP_OUTBOX_MAX_ATTEMPTS=5,
        )

        purchase_order_id = self._create_purchase_order()
        self._queue_po(purchase_order_id)

        calls = {"count": 0}

        def _always_fail(_po: dict) -> dict:
            calls["count"] += 1
            raise ErpGatewayError("temporary timeout")

        first = process_erp_outbox_once(self.app, tenant_id=self.tenant_id, push_fn=_always_fail)
        self.assertEqual(first.get("requeued"), 1)
        self.assertEqual(calls["count"], 1)

        self._force_run_due(purchase_order_id)

        def _would_succeed(_po: dict) -> dict:
            calls["count"] += 1
            return {"external_id": "ERP-CB-1", "status": "accepted"}

        second = process_erp_outbox_once(self.app, tenant_id=self.tenant_id, push_fn=_would_succeed)
        self.assertEqual(second.get("requeued"), 1)
        self.assertEqual(calls["count"], 1)

        run = self._latest_run()
        self.assertEqual(int(run.get("attempt") or 0), 1)

        metrics = self.client.get("/metrics").get_data(as_text=True)
        self.assertIn('erp_circuit_state{state="open"} 1', metrics)

    def test_dead_letter_marks_failed_job_and_exposes_metric(self) -> None:
        self._rebuild_with_config(
            ERP_CIRCUIT_ENABLED=False,
            ERP_OUTBOX_MAX_ATTEMPTS=1,
        )
        purchase_order_id = self._create_purchase_order()
        self._queue_po(purchase_order_id)

        def _definitive_failure(_po: dict) -> dict:
            raise ErpGatewayError("ERP HTTP 422 rejected", definitive=True)

        result = process_erp_outbox_once(self.app, tenant_id=self.tenant_id, push_fn=_definitive_failure)
        self.assertEqual(result.get("failed"), 1)

        run = self._latest_run()
        self.assertEqual(str(run.get("status") or ""), "failed")
        meta = json.loads(str(run.get("payload_ref") or "{}"))
        self.assertTrue(meta.get("dead_letter"))
        self.assertTrue(str(meta.get("dead_letter_reason") or "").strip())

        metrics = self.client.get("/metrics").get_data(as_text=True)
        self.assertIn("erp_dead_letter_total 1", metrics)

    def test_retry_backoff_uses_jitter_and_exposes_metric(self) -> None:
        self._rebuild_with_config(
            ERP_CIRCUIT_ENABLED=False,
            ERP_OUTBOX_MAX_ATTEMPTS=4,
            ERP_OUTBOX_BACKOFF_SECONDS=10,
            ERP_OUTBOX_MAX_BACKOFF_SECONDS=10,
            ERP_OUTBOX_BACKOFF_JITTER_RATIO=0.5,
        )
        purchase_order_id = self._create_purchase_order()
        self._queue_po(purchase_order_id)

        def _temporary_failure(_po: dict) -> dict:
            raise ErpGatewayError("temporary timeout")

        result = process_erp_outbox_once(self.app, tenant_id=self.tenant_id, push_fn=_temporary_failure)
        self.assertEqual(result.get("requeued"), 1)

        run = self._latest_run()
        meta = json.loads(str(run.get("payload_ref") or "{}"))
        next_attempt_at = str(meta.get("next_attempt_at") or "")
        self.assertTrue(next_attempt_at)
        normalized = next_attempt_at[:-1] + "+00:00" if next_attempt_at.endswith("Z") else next_attempt_at
        scheduled = datetime.fromisoformat(normalized)
        if scheduled.tzinfo is None:
            scheduled = scheduled.replace(tzinfo=timezone.utc)
        delay_seconds = (scheduled - datetime.now(timezone.utc)).total_seconds()
        self.assertGreaterEqual(delay_seconds, 1.0)
        self.assertLessEqual(delay_seconds, 16.0)

        metrics = self.client.get("/metrics").get_data(as_text=True)
        self.assertIn("erp_retry_backoff_seconds_bucket", metrics)
        self.assertIn("erp_retry_backoff_seconds_count 1", metrics)


if __name__ == "__main__":
    unittest.main()
