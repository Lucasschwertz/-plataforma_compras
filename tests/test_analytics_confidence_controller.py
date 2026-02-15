import unittest

from flask import Flask

from app import create_app
from app.config import Config
from app.contexts.analytics.application.confidence_controller import (
    get_read_model_confidence,
    record_shadow_compare_result,
    reset_confidence_controller_for_tests,
)
from app.contexts.analytics.application.service import AnalyticsService
from app.db import close_db
from app.domain.contracts import AnalyticsRequestInput
from app.observability import prometheus_metrics_text, reset_metrics_for_tests
from tests.helpers.temp_db import TempDbSandbox


def _parse_filters(_args, workspace_id: str):
    return {
        "start_date": None,
        "end_date": None,
        "supplier": "",
        "buyer": "",
        "status": [],
        "purchase_type": [],
        "period_basis": "pr_created_at",
        "workspace_id": workspace_id,
        "raw": {
            "workspace_id": workspace_id,
        },
    }


def _resolve_visibility(_role, _user_email, _display_name, _team_members):
    return {"role": "admin", "scope": "workspace", "actors": []}


class _FakeTransRepository:
    def build_dashboard_payload(self, _db, **_kwargs):
        return {
            "section": {"key": "overview", "label": "Visao Geral"},
            "filters": {},
            "visibility": {"role": "admin", "scope": "workspace"},
            "meta": {"records_count": 1, "comparison_records_count": 0, "generated_at": "2026-02-15T00:00:00Z"},
            "alerts": [],
            "alerts_meta": {"active_count": 0, "has_active": False},
            "kpis": [],
            "charts": [],
            "drilldown": {"title": "Itens", "columns": [], "column_keys": [], "rows": []},
        }


class _FakeReadModelRepository:
    def get_kpis(self, _db, workspace_id=None, filters=None, section=None):
        _ = (workspace_id, filters, section)
        return {"backlog_open": {"value_int": 1, "value_num": 0.0, "avg_num": 0.0, "rows": 1}}

    def get_supplier_metrics(self, _db, workspace_id=None, filters=None):
        _ = (workspace_id, filters)
        return []

    def get_stage_metrics(self, _db, workspace_id=None, filters=None):
        _ = (workspace_id, filters)
        return []

    def get_meta(self, _db, workspace_id=None, filters=None):
        _ = (workspace_id, filters)
        return {
            "records_count": 1,
            "comparison_records_count": 0,
            "generated_at": "2026-02-15T00:00:00Z",
        }


class AnalyticsConfidenceControllerTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_metrics_for_tests()
        reset_confidence_controller_for_tests()

    def tearDown(self) -> None:
        reset_metrics_for_tests()
        reset_confidence_controller_for_tests()

    def test_confidence_status_healthy(self) -> None:
        app = Flask(__name__)
        app.config["ANALYTICS_CONFIDENCE_ENABLED"] = True
        app.config["ANALYTICS_CONFIDENCE_MIN_SAMPLES"] = 4
        app.config["ANALYTICS_CONFIDENCE_MAX_DIFF_RATE_PERCENT"] = 60.0
        app.config["ANALYTICS_CONFIDENCE_WINDOW_MINUTES"] = 60

        with app.app_context():
            record_shadow_compare_result("tenant-confidence", "overview", "equal")
            record_shadow_compare_result("tenant-confidence", "overview", "equal")
            record_shadow_compare_result("tenant-confidence", "overview", "equal")
            record_shadow_compare_result("tenant-confidence", "overview", "diff")
            confidence = get_read_model_confidence("tenant-confidence", "overview")

        self.assertEqual(confidence.get("status"), "healthy")
        self.assertEqual(int(confidence.get("compare_count") or 0), 4)

    def test_confidence_status_degraded(self) -> None:
        app = Flask(__name__)
        app.config["ANALYTICS_CONFIDENCE_ENABLED"] = True
        app.config["ANALYTICS_CONFIDENCE_MIN_SAMPLES"] = 4
        app.config["ANALYTICS_CONFIDENCE_MAX_DIFF_RATE_PERCENT"] = 10.0
        app.config["ANALYTICS_CONFIDENCE_WINDOW_MINUTES"] = 60

        with app.app_context():
            record_shadow_compare_result("tenant-confidence", "overview", "equal")
            record_shadow_compare_result("tenant-confidence", "overview", "equal")
            record_shadow_compare_result("tenant-confidence", "overview", "diff")
            record_shadow_compare_result("tenant-confidence", "overview", "diff")
            confidence = get_read_model_confidence("tenant-confidence", "overview")

        self.assertEqual(confidence.get("status"), "degraded")
        self.assertGreater(float(confidence.get("diff_rate_percent") or 0.0), 10.0)

    def test_confidence_status_insufficient_data(self) -> None:
        app = Flask(__name__)
        app.config["ANALYTICS_CONFIDENCE_ENABLED"] = True
        app.config["ANALYTICS_CONFIDENCE_MIN_SAMPLES"] = 10
        app.config["ANALYTICS_CONFIDENCE_MAX_DIFF_RATE_PERCENT"] = 0.5
        app.config["ANALYTICS_CONFIDENCE_WINDOW_MINUTES"] = 60

        with app.app_context():
            record_shadow_compare_result("tenant-confidence", "overview", "diff")
            confidence = get_read_model_confidence("tenant-confidence", "overview")

        self.assertEqual(confidence.get("status"), "insufficient_data")
        self.assertEqual(int(confidence.get("compare_count") or 0), 1)

    def test_forced_fallback_when_confidence_degraded(self) -> None:
        app = Flask(__name__)
        app.config["ANALYTICS_READ_MODEL_ENABLED"] = True
        app.config["ANALYTICS_CONFIDENCE_ENABLED"] = True
        app.config["ANALYTICS_CONFIDENCE_MIN_SAMPLES"] = 1
        app.config["ANALYTICS_CONFIDENCE_MAX_DIFF_RATE_PERCENT"] = 0.0
        app.config["ANALYTICS_CONFIDENCE_WINDOW_MINUTES"] = 60
        app.config["ANALYTICS_SHADOW_COMPARE_ENABLED"] = False

        service = AnalyticsService(
            repository_factory=lambda _tenant: _FakeTransRepository(),
            read_model_repository_factory=lambda _tenant: _FakeReadModelRepository(),
        )
        request_input = AnalyticsRequestInput(
            section="overview",
            role="admin",
            tenant_id="tenant-confidence",
            request_args={},
            user_email="admin@demo.com",
            display_name="Admin",
            team_members=[],
        )

        with app.app_context():
            record_shadow_compare_result("tenant-confidence", "overview", "diff")
            with app.test_request_context("/api/procurement/analytics/overview"):
                payload = service.build_dashboard_payload(
                    db=None,
                    request_input=request_input,
                    parse_filters_fn=_parse_filters,
                    resolve_visibility_fn=_resolve_visibility,
                    build_payload_fn=lambda *_args, **_kwargs: {},
                )
            metrics = prometheus_metrics_text()

        self.assertEqual(payload.get("source"), "transacional")
        self.assertEqual(payload.get("confidence_status"), "degraded")
        self.assertIn("analytics_read_model_forced_fallback_total", metrics)
        self.assertIn('analytics_read_model_confidence_status{status="degraded"}', metrics)

    def test_confidence_endpoint_admin_only(self) -> None:
        temp_db = TempDbSandbox(prefix="analytics_confidence_endpoint")
        TempConfig = temp_db.make_config(
            Config,
            TESTING=False,
            DB_AUTO_INIT=False,
            AUTH_ENABLED=False,
            SYNC_SCHEDULER_ENABLED=False,
        )
        app = create_app(TempConfig)
        client = app.test_client()
        headers = {"X-Tenant-Id": "tenant-confidence-endpoint"}

        with client.session_transaction() as session:
            session["tenant_id"] = "tenant-confidence-endpoint"
            session["user_role"] = "buyer"
            session["display_name"] = "Buyer"
            session["user_email"] = "buyer@demo.com"

        denied = client.get("/internal/analytics/read-model-confidence", headers=headers)
        self.assertEqual(denied.status_code, 403)

        with client.session_transaction() as session:
            session["tenant_id"] = "tenant-confidence-endpoint"
            session["user_role"] = "admin"
            session["display_name"] = "Admin"
            session["user_email"] = "admin@demo.com"

        with app.app_context():
            record_shadow_compare_result("tenant-confidence-endpoint", "overview", "equal")
            record_shadow_compare_result("tenant-confidence-endpoint", "overview", "diff")

        allowed = client.get("/internal/analytics/read-model-confidence", headers=headers)
        self.assertEqual(allowed.status_code, 200)
        payload = allowed.get_json() or {}
        self.assertEqual(payload.get("workspace_id"), "tenant-confidence-endpoint")
        self.assertTrue(isinstance(payload.get("confidence"), list))

        with app.app_context():
            close_db()
        temp_db.cleanup()


if __name__ == "__main__":
    unittest.main()
