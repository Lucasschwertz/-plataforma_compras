import json
import os
import unittest
from datetime import datetime, timezone

from app import create_app
from app.config import Config
from app.contexts.analytics.application.service import AnalyticsService
from app.contexts.analytics.projections.base import Projector
from app.contexts.analytics.projections.projectors import AnalyticsProjectionDispatcher, default_projection_dispatcher
from app.core import DomainEvent, PurchaseRequestCreated
from app.core.event_schemas import validate_event
from app.core.event_upcasters import upcast
from app.db import close_db, get_db
from app.observability import prometheus_metrics_text, reset_metrics_for_tests
from tests.helpers.temp_db import TempDbSandbox


class _FailingProjector(Projector):
    name = "failing_projector"
    handled_events = (PurchaseRequestCreated,)

    def handle(self, event, db, workspace_id: str) -> None:  # noqa: ARG002
        raise RuntimeError("forced_projection_failure")


class EventSchemaVersioningTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="event_schema_versioning")
        self._prev_env = os.environ.get("FLASK_ENV")
        os.environ["FLASK_ENV"] = "development"
        reset_metrics_for_tests()

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

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        if self._prev_env is None:
            os.environ.pop("FLASK_ENV", None)
        else:
            os.environ["FLASK_ENV"] = self._prev_env
        self._temp_db.cleanup()
        reset_metrics_for_tests()

    def test_legacy_events_receive_schema_defaults(self) -> None:
        event = PurchaseRequestCreated(
            tenant_id="tenant-defaults",
            purchase_request_id=10,
            status="pending_rfq",
            items_created=1,
        )
        self.assertEqual(event.schema_name, "PurchaseRequestCreated")
        self.assertEqual(event.schema_version, 1)

    def test_validate_event_marks_invalid_and_updates_metric(self) -> None:
        event = DomainEvent(workspace_id="tenant-invalid", schema_name="PurchaseRequestCreated")
        self.assertFalse(validate_event(event))

        metrics = prometheus_metrics_text(outbox_state={"queue": {}})
        self.assertIn("domain_event_schema_invalid_total", metrics)
        self.assertIn('schema_name="PurchaseRequestCreated"', metrics)

    def test_upcaster_transforms_purchase_request_payload_v1_to_v2(self) -> None:
        payload_v1 = {
            "tenant_id": "tenant-upcast",
            "purchase_request_id": 7,
            "status": "pending_rfq",
            "items_count": 3,
        }
        payload_v2 = upcast("PurchaseRequestCreated", 1, payload_v1, 2)
        self.assertEqual(payload_v2.get("schema_version"), 2)
        self.assertEqual(payload_v2.get("items_created"), 3)
        self.assertNotIn("items_count", payload_v2)

    def test_replay_uses_upcasted_payload(self) -> None:
        row = {
            "event_type": "PurchaseRequestCreated",
            "event_id": "evt-replay-upcast",
            "occurred_at": "2026-02-15T11:00:00Z",
            "payload_json": json.dumps(
                {
                    "schema_name": "PurchaseRequestCreated",
                    "schema_version": 1,
                    "tenant_id": "tenant-replay",
                    "purchase_request_id": 22,
                    "status": "pending_rfq",
                    "items_count": 9,
                },
                ensure_ascii=True,
            ),
        }
        event = AnalyticsService._event_from_store_row(row, workspace_id="tenant-replay")
        self.assertIsNotNone(event)
        self.assertIsInstance(event, PurchaseRequestCreated)
        self.assertEqual(event.schema_name, "PurchaseRequestCreated")
        self.assertEqual(event.schema_version, 2)
        self.assertEqual(event.items_created, 9)

    def test_projection_handler_audit_records_ok_and_failed(self) -> None:
        with self.app.app_context():
            db = get_db()
            workspace_id = "tenant-audit"
            occurred_at = datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc)

            ok_dispatcher = default_projection_dispatcher()
            ok_summary = ok_dispatcher.process(
                PurchaseRequestCreated(
                    tenant_id=workspace_id,
                    purchase_request_id=1,
                    status="pending_rfq",
                    items_created=1,
                    event_id="evt-audit-ok",
                    occurred_at=occurred_at,
                ),
                db,
                workspace_id,
            )
            self.assertGreaterEqual(int(ok_summary.get("processed") or 0), 1)

            failed_dispatcher = AnalyticsProjectionDispatcher(projectors=[_FailingProjector()])
            failed_summary = failed_dispatcher.process(
                PurchaseRequestCreated(
                    tenant_id=workspace_id,
                    purchase_request_id=2,
                    status="pending_rfq",
                    items_created=1,
                    event_id="evt-audit-failed",
                    occurred_at=occurred_at,
                ),
                db,
                workspace_id,
            )
            self.assertEqual(int(failed_summary.get("failed") or 0), 1)

            rows = db.execute(
                """
                SELECT handler_name, status, error_code
                FROM ar_event_handler_audit
                WHERE workspace_id = ?
                ORDER BY id ASC
                """,
                (workspace_id,),
            ).fetchall()

            ok_rows = [
                row
                for row in rows
                if str(row["handler_name"] or "") == "procurement_lifecycle" and str(row["status"] or "") == "ok"
            ]
            self.assertTrue(ok_rows)

            failed_rows = [
                row
                for row in rows
                if str(row["handler_name"] or "") == "failing_projector" and str(row["status"] or "") == "failed"
            ]
            self.assertTrue(failed_rows)
            self.assertIn("RuntimeError", str(failed_rows[-1]["error_code"] or ""))


if __name__ == "__main__":
    unittest.main()
