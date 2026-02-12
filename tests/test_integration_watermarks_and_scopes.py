import unittest

from app import create_app
from app.config import Config
from app.db import close_db, get_db
from tests.helpers.temp_db import TempDbSandbox
from tests.outbox_utils import process_erp_outbox_once


class IntegrationWatermarksAndScopesTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="integration_watermarks")
        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=True,
        )

        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.tenant_id = "tenant-1"
        self.headers = {"X-Tenant-Id": self.tenant_id}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()

    def _create_purchase_request_item(self) -> int:
        with self.app.app_context():
            db = get_db()
            cursor = db.execute(
                """
                INSERT INTO purchase_requests (
                    number, status, priority, requested_by, department, needed_at, tenant_id
                ) VALUES (?, 'pending_rfq', 'medium', 'Test', 'Compras', date('now', '+3 day'), ?)
                """,
                ("SR-TEST", self.tenant_id),
            )
            pr_id = cursor.lastrowid
            cursor = db.execute(
                """
                INSERT INTO purchase_request_items (
                    purchase_request_id, line_no, description, quantity, uom, tenant_id
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (pr_id, 1, "Item teste", 1, "UN", self.tenant_id),
            )
            db.commit()
            return cursor.lastrowid

    def test_integration_watermarks_columns(self) -> None:
        expected_columns = {
            "tenant_id",
            "system",
            "entity",
            "last_success_source_updated_at",
            "last_success_source_id",
            "last_success_cursor",
            "last_success_at",
            "updated_at",
        }

        with self.app.app_context():
            db = get_db()
            rows = db.execute("PRAGMA table_info(integration_watermarks)").fetchall()
            columns = {row["name"] for row in rows}
            self.assertTrue(expected_columns.issubset(columns))

            db.execute(
                """
                INSERT INTO integration_watermarks (
                    tenant_id,
                    system,
                    entity,
                    last_success_source_updated_at,
                    last_success_source_id,
                    last_success_cursor
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    self.tenant_id,
                    "senior",
                    "purchase_order",
                    "2026-01-01T00:00:00Z",
                    "PO-1",
                    "cursor-1",
                ),
            )
            db.commit()

            row = db.execute(
                """
                SELECT *
                FROM integration_watermarks
                WHERE tenant_id = ? AND system = ? AND entity = ?
                """,
                (self.tenant_id, "senior", "purchase_order"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["last_success_source_id"], "PO-1")
            self.assertIsNotNone(row["last_success_at"])

    def test_scope_aliases_accept_plural(self) -> None:
        item_id = self._create_purchase_request_item()
        rfq_res = self.client.post(
            "/api/procurement/rfqs",
            headers=self.headers,
            json={"title": "Alias RFQ", "purchase_request_item_ids": [item_id]},
        )
        self.assertEqual(rfq_res.status_code, 201)
        rfq_id = rfq_res.get_json()["id"]

        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "alias_award", "supplier_name": "Fornecedor Alias", "confirm": True},
        )
        self.assertEqual(award_res.status_code, 201)
        award_id = award_res.get_json()["award_id"]

        po_res = self.client.post(
            f"/api/procurement/awards/{award_id}/purchase-orders?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(po_res.status_code, 201)
        purchase_order_id = po_res.get_json()["purchase_order_id"]

        push_res = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(push_res.status_code, 200)
        self.assertEqual((push_res.get_json() or {}).get("status"), "sent_to_erp")
        process_erp_outbox_once(self.app, tenant_id=self.tenant_id)

        logs_singular = self.client.get(
            "/api/procurement/integrations/logs?scope=purchase_order",
            headers=self.headers,
        )
        self.assertEqual(logs_singular.status_code, 200)
        singular_payload = logs_singular.get_json()
        self.assertTrue(
            any(run["scope"] == "purchase_order" for run in singular_payload["sync_runs"]),
            "expected purchase_order sync run for singular scope",
        )

        logs_plural = self.client.get(
            "/api/procurement/integrations/logs?scope=purchase_orders",
            headers=self.headers,
        )
        self.assertEqual(logs_plural.status_code, 200)
        plural_payload = logs_plural.get_json()
        self.assertTrue(
            any(run["scope"] == "purchase_order" for run in plural_payload["sync_runs"]),
            "expected purchase_order sync run for plural scope alias",
        )

    def test_push_idempotent_updates_watermark(self) -> None:
        with self.app.app_context():
            db = get_db()
            db.execute(
                """
                INSERT INTO integration_watermarks (
                    tenant_id,
                    system,
                    entity,
                    last_success_source_updated_at,
                    last_success_source_id,
                    last_success_cursor
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    1,
                    "senior",
                    "purchase_order",
                    "2000-01-01T00:00:00Z",
                    "PO-OLD",
                    "cursor-old",
                ),
            )
            db.commit()

        item_id = self._create_purchase_request_item()
        rfq_res = self.client.post(
            "/api/procurement/rfqs",
            headers=self.headers,
            json={"title": "Idempotency RFQ", "purchase_request_item_ids": [item_id]},
        )
        self.assertEqual(rfq_res.status_code, 201)
        rfq_id = rfq_res.get_json()["id"]

        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "idempotency_award", "supplier_name": "Fornecedor Idempotente", "confirm": True},
        )
        self.assertEqual(award_res.status_code, 201)
        award_id = award_res.get_json()["award_id"]

        po_res = self.client.post(
            f"/api/procurement/awards/{award_id}/purchase-orders?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(po_res.status_code, 201)
        purchase_order_id = po_res.get_json()["purchase_order_id"]

        push_res = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(push_res.status_code, 200)
        self.assertEqual((push_res.get_json() or {}).get("status"), "sent_to_erp")
        process_erp_outbox_once(self.app, tenant_id=self.tenant_id)

        with self.app.app_context():
            db = get_db()
            po_row = db.execute(
                "SELECT external_id FROM purchase_orders WHERE id = ? AND tenant_id = ?",
                (purchase_order_id, self.tenant_id),
            ).fetchone()
            external_id = po_row["external_id"]
            self.assertTrue(str(external_id or "").strip())
            row = db.execute(
                """
                SELECT *
                FROM integration_watermarks
                WHERE tenant_id = ? AND system = ? AND entity = ?
                """,
                (self.tenant_id, "senior", "purchase_order"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["last_success_source_id"], external_id)
            self.assertNotEqual(row["last_success_source_updated_at"], "2000-01-01T00:00:00Z")
            self.assertIsNotNone(row["last_success_at"])

            runs = db.execute(
                """
                SELECT COUNT(*) AS total
                FROM sync_runs
                WHERE tenant_id = ? AND scope IN ('purchase_order', 'purchase_orders')
                """,
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(runs, 1)

        push_again = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp",
            headers=self.headers,
        )
        self.assertEqual(push_again.status_code, 200)
        self.assertEqual(push_again.get_json()["status"], "erp_accepted")

        with self.app.app_context():
            db = get_db()
            row_after = db.execute(
                """
                SELECT *
                FROM integration_watermarks
                WHERE tenant_id = ? AND system = ? AND entity = ?
                """,
                (self.tenant_id, "senior", "purchase_order"),
            ).fetchone()
            self.assertIsNotNone(row_after)
            self.assertEqual(row_after["last_success_source_id"], external_id)

            runs_after = db.execute(
                """
                SELECT COUNT(*) AS total
                FROM sync_runs
                WHERE tenant_id = ? AND scope IN ('purchase_order', 'purchase_orders')
                """,
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(runs_after, 1)


if __name__ == "__main__":
    unittest.main()

