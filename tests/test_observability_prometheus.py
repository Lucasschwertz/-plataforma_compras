import json
import logging
import unittest

from app import create_app
from app.config import Config
from app.core import PurchaseRequestCreated, get_event_bus
from app.db import close_db
from app.observability import JsonLogFormatter, reset_metrics_for_tests, set_log_request_id
from tests.helpers.temp_db import TempDbSandbox


class _MetricsConfig(Config):
    TESTING = False
    DB_AUTO_INIT = False
    AUTH_ENABLED = False
    SYNC_SCHEDULER_ENABLED = False


class ObservabilityPrometheusTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="observability_metrics")
        cfg = self._temp_db.make_config(_MetricsConfig)
        self.app = create_app(cfg)
        self.client = self.app.test_client()
        reset_metrics_for_tests()

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()
        reset_metrics_for_tests()

    def test_metrics_endpoint_exposes_prometheus_metrics(self) -> None:
        self.client.get("/api/unknown")
        get_event_bus().publish(
            PurchaseRequestCreated(
                tenant_id="tenant-metrics",
                purchase_request_id=99,
                status="pending_rfq",
                items_created=1,
            )
        )

        response = self.client.get("/metrics")
        self.assertEqual(response.status_code, 200)
        content_type = response.headers.get("Content-Type") or ""
        self.assertIn("text/plain", content_type)

        payload = response.get_data(as_text=True)
        self.assertIn("http_request_total", payload)
        self.assertIn("http_request_duration_ms_bucket", payload)
        self.assertIn("erp_outbox_queue_size", payload)
        self.assertIn("erp_outbox_retry_count", payload)
        self.assertIn("erp_dead_letter_total", payload)
        self.assertIn("erp_outbox_processing_time_bucket", payload)
        self.assertIn("erp_retry_backoff_seconds_bucket", payload)
        self.assertIn("erp_circuit_state", payload)
        self.assertIn("domain_event_emitted_total", payload)
        self.assertIn("analytics_projection_processed_total", payload)
        self.assertIn("analytics_projection_failed_total", payload)
        self.assertIn("analytics_projection_lag_seconds", payload)
        self.assertIn("analytics_read_model_lag_seconds", payload)
        self.assertIn("analytics_projection_last_success_timestamp", payload)
        self.assertIn("analytics_read_model_hits_total", payload)
        self.assertIn("analytics_event_store_persisted_total", payload)
        self.assertIn("analytics_event_store_failed_total", payload)
        self.assertIn("analytics_read_model_rebuild_total", payload)
        self.assertIn("analytics_read_model_rebuild_duration_seconds_bucket", payload)
        self.assertIn("analytics_shadow_compare_total", payload)
        self.assertIn("analytics_shadow_compare_diff_fields_total", payload)
        self.assertIn("analytics_shadow_compare_latency_ms_bucket", payload)
        self.assertIn("analytics_shadow_compare_last_diff_timestamp", payload)
        self.assertIn("analytics_shadow_compare_diff_rate", payload)
        self.assertIn("analytics_shadow_compare_diff_persisted_total", payload)
        self.assertIn('event_type="PurchaseRequestCreated"', payload)

    def test_log_formatter_includes_request_id_outside_request_context(self) -> None:
        set_log_request_id("worker-req-123")
        formatter = JsonLogFormatter()
        record = logging.LogRecord(
            name="app",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="worker_log",
            args=(),
            exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        self.assertEqual(parsed.get("request_id"), "worker-req-123")

    def test_health_reports_checks_and_backlog_flag(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}
        checks = payload.get("checks") or {}

        self.assertIn("db", checks)
        self.assertIn("worker", checks)
        self.assertIn("backlog", checks)
        self.assertIn("worker", payload)
        self.assertIn("backlog_critical", payload.get("worker") or {})


if __name__ == "__main__":
    unittest.main()
