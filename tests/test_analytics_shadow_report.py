import unittest
from unittest.mock import patch

from app import create_app
from app.config import Config
from app.db import close_db, get_db
from app.observability import observe_analytics_shadow_compare, reset_metrics_for_tests
from app.routes import procurement_routes
from tests.helpers.temp_db import TempDbSandbox


class AnalyticsShadowReportTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="analytics_shadow_report")
        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=False,
            DB_AUTO_INIT=False,
            AUTH_ENABLED=False,
            SYNC_SCHEDULER_ENABLED=False,
            ANALYTICS_READ_MODEL_ENABLED=True,
            ANALYTICS_SHADOW_COMPARE_ENABLED=True,
            ANALYTICS_SHADOW_COMPARE_SAMPLE_RATE=1.0,
            ANALYTICS_SHADOW_COMPARE_MAX_DIFF_LOGS_PER_MIN=20,
        )
        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.workspace_id = "tenant-shadow-report"
        self.headers = {"X-Tenant-Id": self.workspace_id}
        reset_metrics_for_tests()
        procurement_routes._clear_analytics_cache_for_tests()

        runner = self.app.test_cli_runner()
        upgrade = runner.invoke(args=["db", "upgrade"])
        self.assertEqual(upgrade.exit_code, 0, msg=upgrade.output)

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        procurement_routes._clear_analytics_cache_for_tests()
        reset_metrics_for_tests()
        self._temp_db.cleanup()

    def _set_role(self, role: str) -> None:
        with self.client.session_transaction() as session:
            session["tenant_id"] = self.workspace_id
            session["user_role"] = role
            session["display_name"] = f"{role} user"
            session["user_email"] = f"{role}@demo.com"

    @staticmethod
    def _payload(value: int) -> dict:
        return {
            "section": {"key": "overview", "label": "Visao Geral"},
            "filters": {},
            "visibility": {"role": "admin", "scope": "workspace"},
            "meta": {"records_count": 1, "comparison_records_count": 0, "generated_at": "2026-02-15T00:00:00Z"},
            "alerts": [],
            "alerts_meta": {"active_count": 0, "has_active": False},
            "kpis": [
                {
                    "key": "backlog_open",
                    "label": "Backlog aberto",
                    "value": value,
                    "display_value": str(value),
                    "tooltip": "Test KPI",
                    "trend": {"direction": "flat", "delta_pct": 0.0, "display": "+0.0%", "label": "Estavel"},
                }
            ],
            "charts": [],
            "drilldown": {"title": "Itens", "columns": [], "column_keys": [], "rows": []},
        }

    def _count_shadow_diffs(self) -> int:
        with self.app.app_context():
            db = get_db()
            row = db.execute("SELECT COUNT(*) AS total FROM analytics_shadow_diff_log WHERE workspace_id = ?", (self.workspace_id,)).fetchone()
            return int(row["total"] or 0) if row else 0

    def test_internal_endpoint_requires_admin(self) -> None:
        self._set_role("buyer")
        response = self.client.get("/internal/analytics/shadow-report", headers=self.headers)
        self.assertEqual(response.status_code, 403)
        self.assertEqual((response.get_json() or {}).get("error"), "permission_denied")

    def test_diff_persistence_occurs_only_for_diff_result(self) -> None:
        self._set_role("admin")
        procurement_routes._clear_analytics_cache_for_tests()
        service = procurement_routes._ANALYTICS_SERVICE

        with patch.object(service, "_build_dashboard_payload_from_read_model", return_value=self._payload(1)):
            with patch.object(service, "_build_dashboard_payload_transacional", return_value=self._payload(1)):
                equal_res = self.client.get("/api/procurement/analytics/overview", headers=self.headers)
        self.assertEqual(equal_res.status_code, 200)
        self.assertEqual(self._count_shadow_diffs(), 0)

        procurement_routes._clear_analytics_cache_for_tests()
        with patch.object(service, "_build_dashboard_payload_from_read_model", return_value=self._payload(1)):
            with patch.object(service, "_build_dashboard_payload_transacional", return_value=self._payload(9)):
                diff_res = self.client.get("/api/procurement/analytics/overview", headers=self.headers)
        self.assertEqual(diff_res.status_code, 200)
        self.assertEqual(self._count_shadow_diffs(), 1)

    def test_shadow_report_returns_expected_stats_and_filters(self) -> None:
        self._set_role("admin")
        observe_analytics_shadow_compare("equal", "read_model")
        observe_analytics_shadow_compare("equal", "fallback")
        observe_analytics_shadow_compare("diff", "read_model")
        observe_analytics_shadow_compare("error", "read_model")

        with self.app.app_context():
            db = get_db()
            db.execute(
                """
                INSERT INTO analytics_shadow_diff_log (
                    occurred_at, workspace_id, section, primary_source, primary_hash, shadow_hash, diff_summary, diff_count, request_id
                )
                VALUES
                    ('2026-02-14 10:00:00', ?, 'overview', 'read_model', 'h1', 'h2', '{"summary":{"kpis":1},"fields":[]}', 1, 'req-1'),
                    ('2026-02-15 11:00:00', ?, 'costs', 'read_model', 'h3', 'h4', '{"summary":{"charts":2},"fields":[]}', 2, 'req-2'),
                    ('2026-02-15 12:00:00', ?, 'overview', 'fallback', 'h5', 'h6', '{"summary":{"drilldown":1},"fields":[]}', 1, 'req-3')
                """,
                (self.workspace_id, self.workspace_id, self.workspace_id),
            )
            db.commit()

        response = self.client.get(
            "/internal/analytics/shadow-report?start_date=2026-02-15&end_date=2026-02-15&section=overview",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}

        self.assertEqual(int(payload.get("total_compares") or 0), 4)
        self.assertEqual(int(payload.get("total_equal") or 0), 2)
        self.assertEqual(int(payload.get("total_diff") or 0), 1)
        self.assertEqual(int(payload.get("total_error") or 0), 1)
        self.assertAlmostEqual(float(payload.get("diff_rate_percent") or 0.0), 25.0, places=2)

        sections = list(payload.get("sections_breakdown") or [])
        self.assertEqual(len(sections), 1)
        self.assertEqual(sections[0].get("section"), "overview")
        self.assertEqual(int(sections[0].get("diff_count") or 0), 1)

        recent = list(payload.get("recent_diffs") or [])
        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0].get("section"), "overview")
        self.assertEqual(recent[0].get("request_id"), "req-3")


if __name__ == "__main__":
    unittest.main()
