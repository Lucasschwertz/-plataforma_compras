import os
import unittest
from datetime import datetime, timedelta, timezone

from app import create_app
from app.config import Config
from app.core import ErpOrderRejected, EventBus, PurchaseOrderCreated, PurchaseRequestCreated
from app.db import close_db, get_db
from tests.helpers.temp_db import TempDbSandbox


class AnalyticsReadModelRebuildTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="analytics_read_model_rebuild")
        self._prev_env = os.environ.get("FLASK_ENV")
        os.environ["FLASK_ENV"] = "development"

        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=False,
            DB_AUTO_INIT=False,
            AUTH_ENABLED=False,
            SYNC_SCHEDULER_ENABLED=False,
            ANALYTICS_PROJECTION_ENABLED=True,
        )
        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.workspace_id = "tenant-rebuild"
        self.headers = {"X-Tenant-Id": self.workspace_id}

        runner = self.app.test_cli_runner()
        upgrade = runner.invoke(args=["db", "upgrade"])
        self.assertEqual(upgrade.exit_code, 0, msg=upgrade.output)

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        if self._prev_env is None:
            os.environ.pop("FLASK_ENV", None)
        else:
            os.environ["FLASK_ENV"] = self._prev_env
        self._temp_db.cleanup()

    def _set_role(self, role: str) -> None:
        with self.client.session_transaction() as session:
            session["tenant_id"] = self.workspace_id
            session["user_role"] = role
            session["display_name"] = f"{role} user"
            session["user_email"] = f"{role}@demo.com"

    def _publish_events(self, suffix: str) -> None:
        base = datetime(2026, 2, 1, 10, 0, 0, tzinfo=timezone.utc)
        with self.app.app_context():
            db = get_db()
            bus = EventBus()
            bus.publish(
                PurchaseRequestCreated(
                    tenant_id=self.workspace_id,
                    purchase_request_id=100,
                    status="pending_rfq",
                    items_created=1,
                    event_id=f"evt-pr-{suffix}",
                    occurred_at=base,
                )
            )
            bus.publish(
                PurchaseOrderCreated(
                    tenant_id=self.workspace_id,
                    purchase_order_id=200,
                    status="approved",
                    source="manual",
                    event_id=f"evt-po-{suffix}",
                    occurred_at=base + timedelta(hours=1),
                )
            )
            bus.publish(
                ErpOrderRejected(
                    tenant_id=self.workspace_id,
                    purchase_order_id=200,
                    sync_run_id=1,
                    reason="ERP rejected",
                    event_id=f"evt-erp-{suffix}",
                    occurred_at=base + timedelta(hours=2),
                )
            )
            db.commit()

    def test_publish_persists_events_to_event_store(self) -> None:
        self._publish_events("persist")
        with self.app.app_context():
            db = get_db()
            rows = db.execute(
                """
                SELECT event_type
                FROM ar_event_store
                WHERE workspace_id = ?
                ORDER BY occurred_at, event_id
                """,
                (self.workspace_id,),
            ).fetchall()
        self.assertEqual(len(rows), 3)
        self.assertEqual([row["event_type"] for row in rows], ["PurchaseRequestCreated", "PurchaseOrderCreated", "ErpOrderRejected"])

    def test_rebuild_full_clears_and_reconstructs_workspace(self) -> None:
        self._publish_events("full")
        with self.app.app_context():
            db = get_db()
            db.execute(
                """
                INSERT INTO ar_kpi_daily (workspace_id, day, metric, value_num, value_int, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (self.workspace_id, "2020-01-01", "backlog_open", "0", 999),
            )
            db.execute(
                """
                INSERT INTO ar_event_dedupe (workspace_id, event_id, projector, processed_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (self.workspace_id, "legacy-event", "procurement_lifecycle"),
            )
            db.commit()

        self._set_role("manager")
        response = self.client.post(
            "/api/procurement/analytics/read-model/rebuild",
            headers=self.headers,
            json={"workspace_id": self.workspace_id, "mode": "full"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}
        self.assertEqual(payload.get("mode"), "full")
        self.assertEqual(int(payload.get("total_events") or 0), 3)
        self.assertEqual(int(payload.get("failed") or 0), 0)
        self.assertGreaterEqual(int(payload.get("processed") or 0), 3)

        with self.app.app_context():
            db = get_db()
            stale = db.execute(
                """
                SELECT 1
                FROM ar_kpi_daily
                WHERE workspace_id = ? AND day = ? AND metric = ?
                """,
                (self.workspace_id, "2020-01-01", "backlog_open"),
            ).fetchone()
            self.assertIsNone(stale)

            backlog = db.execute(
                """
                SELECT value_int
                FROM ar_kpi_daily
                WHERE workspace_id = ? AND metric = ?
                """,
                (self.workspace_id, "backlog_open"),
            ).fetchone()
            self.assertIsNotNone(backlog)
            self.assertEqual(int(backlog["value_int"] or 0), 1)

            rejections = db.execute(
                """
                SELECT value_int
                FROM ar_kpi_daily
                WHERE workspace_id = ? AND metric = ?
                """,
                (self.workspace_id, "erp_rejections"),
            ).fetchone()
            self.assertIsNotNone(rejections)
            self.assertEqual(int(rejections["value_int"] or 0), 1)

    def test_rebuild_range_respects_dedupe(self) -> None:
        self._publish_events("range")
        self._set_role("manager")

        first = self.client.post(
            "/api/procurement/analytics/read-model/rebuild",
            headers=self.headers,
            json={"workspace_id": self.workspace_id, "mode": "full"},
        )
        self.assertEqual(first.status_code, 200)

        with self.app.app_context():
            db = get_db()
            before = db.execute(
                """
                SELECT metric, value_int
                FROM ar_kpi_daily
                WHERE workspace_id = ? AND metric IN ('backlog_open', 'erp_rejections')
                ORDER BY metric
                """,
                (self.workspace_id,),
            ).fetchall()
            before_values = {row["metric"]: int(row["value_int"] or 0) for row in before}

        second = self.client.post(
            "/api/procurement/analytics/read-model/rebuild",
            headers=self.headers,
            json={
                "workspace_id": self.workspace_id,
                "mode": "range",
                "start_date": "2026-02-01",
                "end_date": "2026-02-01",
            },
        )
        self.assertEqual(second.status_code, 200)
        second_payload = second.get_json() or {}
        self.assertEqual(second_payload.get("mode"), "range")
        self.assertEqual(int(second_payload.get("failed") or 0), 0)
        self.assertGreaterEqual(int(second_payload.get("skipped_dedupe") or 0), 3)

        with self.app.app_context():
            db = get_db()
            after = db.execute(
                """
                SELECT metric, value_int
                FROM ar_kpi_daily
                WHERE workspace_id = ? AND metric IN ('backlog_open', 'erp_rejections')
                ORDER BY metric
                """,
                (self.workspace_id,),
            ).fetchall()
            after_values = {row["metric"]: int(row["value_int"] or 0) for row in after}
        self.assertEqual(before_values, after_values)

    def test_rebuild_endpoint_requires_manager_or_admin(self) -> None:
        self._set_role("buyer")
        buyer_response = self.client.post(
            "/api/procurement/analytics/read-model/rebuild",
            headers=self.headers,
            json={"workspace_id": self.workspace_id, "mode": "full"},
        )
        self.assertEqual(buyer_response.status_code, 403)
        self.assertEqual((buyer_response.get_json() or {}).get("error"), "permission_denied")

        self._set_role("supplier")
        supplier_response = self.client.post(
            "/api/procurement/analytics/read-model/rebuild",
            headers=self.headers,
            json={"workspace_id": self.workspace_id, "mode": "full"},
        )
        self.assertEqual(supplier_response.status_code, 403)
        self.assertEqual((supplier_response.get_json() or {}).get("error"), "permission_denied")


if __name__ == "__main__":
    unittest.main()
