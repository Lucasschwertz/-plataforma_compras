import unittest

from app import create_app
from app.config import Config
from app.db import close_db, get_db
from app.erp_mock import fetch_erp_records
from tests.helpers.temp_db import TempDbSandbox


class ErpMockSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="erp_mock_sync")
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

    def test_sync_suppliers_and_watermark(self) -> None:
        records = fetch_erp_records("supplier", None, None, limit=100)
        last_record = records[-1]

        res = self.client.post(
            "/api/procurement/integrations/sync?scope=supplier",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(res.status_code, 200)
        payload = res.get_json()
        self.assertEqual(payload["result"]["records_in"], len(records))

        with self.app.app_context():
            db = get_db()
            total = db.execute(
                "SELECT COUNT(*) AS total FROM suppliers WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(total, len(records))

            watermark = db.execute(
                """
                SELECT last_success_source_updated_at, last_success_source_id
                FROM integration_watermarks
                WHERE tenant_id = ? AND system = 'senior' AND entity = ?
                """,
                (self.tenant_id, "supplier"),
            ).fetchone()
            self.assertIsNotNone(watermark)
            self.assertEqual(watermark["last_success_source_updated_at"], last_record["updated_at"])
            self.assertEqual(watermark["last_success_source_id"], last_record["external_id"])

    def test_sync_purchase_requests_is_incremental(self) -> None:
        records = fetch_erp_records("purchase_request", None, None, limit=100)

        res = self.client.post(
            "/api/procurement/integrations/sync?scope=purchase_request",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(res.status_code, 200)
        payload = res.get_json()
        self.assertEqual(payload["result"]["records_in"], len(records))

        res_again = self.client.post(
            "/api/procurement/integrations/sync?scope=purchase_request",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(res_again.status_code, 200)
        payload_again = res_again.get_json()
        self.assertEqual(payload_again["result"]["records_in"], 0)

        with self.app.app_context():
            db = get_db()
            total = db.execute(
                "SELECT COUNT(*) AS total FROM purchase_requests WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(total, len(records))

    def test_sync_purchase_orders_updates_status(self) -> None:
        records = fetch_erp_records("purchase_order", None, None, limit=100)

        res = self.client.post(
            "/api/procurement/integrations/sync?scope=purchase_order",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(res.status_code, 200)
        payload = res.get_json()
        self.assertEqual(payload["result"]["records_in"], len(records))

        with self.app.app_context():
            db = get_db()
            total = db.execute(
                "SELECT COUNT(*) AS total FROM purchase_orders WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(total, len(records))

            row = db.execute(
                """
                SELECT status
                FROM purchase_orders
                WHERE tenant_id = ? AND external_id = ?
                """,
                (self.tenant_id, "SENIOR-OC-000002"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["status"], "partially_received")

    def test_sync_receipts_updates_purchase_order(self) -> None:
        self.client.post(
            "/api/procurement/integrations/sync?scope=purchase_order",
            headers=self.headers,
            json={"limit": 100},
        )

        receipts = fetch_erp_records("receipt", None, None, limit=100)

        res = self.client.post(
            "/api/procurement/integrations/sync?scope=receipt",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(res.status_code, 200)
        payload = res.get_json()
        self.assertEqual(payload["result"]["records_in"], len(receipts))

        with self.app.app_context():
            db = get_db()
            total = db.execute(
                "SELECT COUNT(*) AS total FROM receipts WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(total, len(receipts))

            row = db.execute(
                """
                SELECT status
                FROM purchase_orders
                WHERE tenant_id = ? AND external_id = ?
                """,
                (self.tenant_id, "SENIOR-OC-000003"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["status"], "received")


if __name__ == "__main__":
    unittest.main()

