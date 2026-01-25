import os
import tempfile
import unittest

from app import create_app
from app.config import Config
from app.db import close_db, get_db
from app.erp_mock import fetch_erp_records


class ErpMockSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        db_path = os.path.join(self._tmpdir.name, "plataforma_compras_test.db")

        class TempConfig(Config):
            DATABASE_DIR = self._tmpdir.name
            DB_PATH = db_path
            TESTING = True

        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.headers = {"X-Company-Id": "1"}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._tmpdir.cleanup()

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
                "SELECT COUNT(*) AS total FROM suppliers WHERE company_id = ?",
                (1,),
            ).fetchone()["total"]
            self.assertEqual(total, len(records))

            watermark = db.execute(
                """
                SELECT last_success_source_updated_at, last_success_source_id
                FROM integration_watermarks
                WHERE company_id = ? AND system = 'senior' AND entity = ?
                """,
                (1, "supplier"),
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
                "SELECT COUNT(*) AS total FROM purchase_requests WHERE company_id = ?",
                (1,),
            ).fetchone()["total"]
            self.assertEqual(total, len(records))


if __name__ == "__main__":
    unittest.main()
