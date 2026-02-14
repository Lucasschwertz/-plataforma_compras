import os
import unittest
from datetime import datetime, timezone

from app import create_app
from app.config import Config
from app.contexts.analytics.application.service import AnalyticsService
from app.core import EventBus, PurchaseRequestCreated
from app.db import close_db, get_db
from tests.helpers.temp_db import TempDbSandbox


class AnalyticsProjectorsIdempotentTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="analytics_projectors")
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
        runner = self.app.test_cli_runner()
        result = runner.invoke(args=["db", "upgrade"])
        if result.exit_code != 0:
            raise RuntimeError(result.output)

        self.event_bus = EventBus()
        self.analytics_service = AnalyticsService(ttl_seconds=60, projection_enabled=True)
        self.analytics_service.register_event_handlers(self.event_bus, projection_enabled=True)
        self.workspace_id = "tenant-analytics-projection"

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        if self._prev_env is None:
            os.environ.pop("FLASK_ENV", None)
        else:
            os.environ["FLASK_ENV"] = self._prev_env
        self._temp_db.cleanup()

    def test_duplicate_event_id_is_processed_once(self) -> None:
        fixed_time = datetime(2026, 2, 14, 12, 0, 0, tzinfo=timezone.utc)
        event = PurchaseRequestCreated(
            tenant_id=self.workspace_id,
            purchase_request_id=11,
            status="pending_rfq",
            items_created=2,
            event_id="evt-analytics-dup-1",
            occurred_at=fixed_time,
        )

        with self.app.app_context():
            self.event_bus.publish(event)
            self.event_bus.publish(event)

            db = get_db()
            dedupe_count = db.execute(
                """
                SELECT COUNT(*) AS total
                FROM ar_event_dedupe
                WHERE workspace_id = ? AND projector = ? AND event_id = ?
                """,
                (self.workspace_id, "procurement_lifecycle", "evt-analytics-dup-1"),
            ).fetchone()["total"]
            self.assertEqual(int(dedupe_count or 0), 1)

            kpi_row = db.execute(
                """
                SELECT value_int
                FROM ar_kpi_daily
                WHERE workspace_id = ? AND day = ? AND metric = ?
                """,
                (self.workspace_id, fixed_time.date().isoformat(), "backlog_open"),
            ).fetchone()
            self.assertIsNotNone(kpi_row)
            self.assertEqual(int(kpi_row["value_int"] or 0), 1)

            stage_row = db.execute(
                """
                SELECT count
                FROM ar_process_stage_daily
                WHERE workspace_id = ? AND day = ? AND stage = ?
                """,
                (self.workspace_id, fixed_time.date().isoformat(), "SR"),
            ).fetchone()
            self.assertIsNotNone(stage_row)
            self.assertEqual(int(stage_row["count"] or 0), 1)


if __name__ == "__main__":
    unittest.main()
