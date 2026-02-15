import json
import unittest

from flask import Flask

from app import create_app
from app.config import Config
from app.contexts.analytics.application.service import AnalyticsService
from app.core.governance import get_worker_fairness, reset_governance_for_tests
from app.db import close_db, get_db
from app.domain.contracts import AnalyticsRequestInput
from app.observability import prometheus_metrics_text, reset_metrics_for_tests
from tests.helpers.temp_db import TempDbSandbox
from tests.outbox_utils import process_erp_outbox_once


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
        "raw": {"workspace_id": workspace_id},
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
            "kpis": [{"key": "backlog_open", "value": 1, "display_value": "1", "label": "Backlog", "trend": {}, "tooltip": ""}],
            "charts": [],
            "drilldown": {"title": "Itens", "columns": [], "column_keys": [], "rows": []},
        }


class _FailReadModelRepository:
    def get_kpis(self, _db, workspace_id=None, filters=None, section=None):
        _ = (workspace_id, filters, section)
        raise RuntimeError("forced read model failure")

    def get_supplier_metrics(self, _db, workspace_id=None, filters=None):
        _ = (workspace_id, filters)
        return []

    def get_stage_metrics(self, _db, workspace_id=None, filters=None):
        _ = (workspace_id, filters)
        return []

    def get_meta(self, _db, workspace_id=None, filters=None):
        _ = (workspace_id, filters)
        return {"records_count": 0, "comparison_records_count": 0, "generated_at": "2026-02-15T00:00:00Z"}


class GovernanceAnalyticsApiTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_metrics_for_tests()
        reset_governance_for_tests()
        self._temp_db = TempDbSandbox(prefix="governance_analytics")
        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=True,
            AUTH_ENABLED=False,
            GOV_ENABLED=True,
            GOV_ANALYTICS_MAX_RPM_PER_WORKSPACE=1,
            GOV_ANALYTICS_SOFT_DEGRADE_ON_LIMIT=True,
            GOV_ANALYTICS_DEGRADE_TTL_SECONDS=120,
            GOV_ANALYTICS_CACHE_TTL_SECONDS_WHEN_DEGRADED=180,
            GOV_ANALYTICS_SHADOW_DISABLE_WHEN_DEGRADED=True,
        )
        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.tenant_id = "tenant-governance-analytics"
        self.headers = {"X-Tenant-Id": self.tenant_id}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()
        reset_metrics_for_tests()
        reset_governance_for_tests()

    def _set_role(self, role: str) -> None:
        with self.client.session_transaction() as session:
            session["tenant_id"] = self.tenant_id
            session["user_role"] = role
            session["display_name"] = role
            session["user_email"] = f"{role}@demo.com"

    def test_analytics_degrades_with_cache_and_min_payload(self) -> None:
        self._set_role("admin")

        first = self.client.get("/api/procurement/analytics/overview", headers=self.headers)
        self.assertEqual(first.status_code, 200)
        first_payload = first.get_json() or {}
        self.assertIn("kpis", first_payload)

        second = self.client.get("/api/procurement/analytics/overview", headers=self.headers)
        self.assertEqual(second.status_code, 200)
        second_payload = second.get_json() or {}
        governance = second_payload.get("governance") or {}
        self.assertTrue(governance.get("degraded"))
        self.assertIn("retry_after_seconds", governance)
        self.assertIn("kpis", second_payload)

        third = self.client.get("/api/procurement/analytics/efficiency", headers=self.headers)
        self.assertEqual(third.status_code, 200)
        third_payload = third.get_json() or {}
        third_gov = third_payload.get("governance") or {}
        self.assertTrue(third_gov.get("degraded"))
        self.assertEqual(third_payload.get("kpis"), [])
        self.assertEqual(third_payload.get("charts"), [])
        self.assertIn("drilldown", third_payload)

    def test_internal_governance_status_admin_only(self) -> None:
        self._set_role("buyer")
        denied = self.client.get("/internal/governance/status", headers=self.headers)
        self.assertEqual(denied.status_code, 403)

        self._set_role("admin")
        allowed = self.client.get("/internal/governance/status", headers=self.headers)
        self.assertEqual(allowed.status_code, 200)
        payload = allowed.get_json() or {}
        self.assertIn("analytics", payload)
        self.assertIn("worker", payload)
        self.assertIn("degraded_active_count", payload.get("analytics") or {})
        self.assertIn("throttled_last_minute", payload.get("worker") or {})


class GovernanceShadowCompareTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_metrics_for_tests()
        reset_governance_for_tests()

    def tearDown(self) -> None:
        reset_metrics_for_tests()
        reset_governance_for_tests()

    def test_shadow_compare_skipped_when_workspace_degraded(self) -> None:
        app = Flask(__name__)
        app.config["ANALYTICS_READ_MODEL_ENABLED"] = True
        app.config["ANALYTICS_SHADOW_COMPARE_ENABLED"] = True
        app.config["ANALYTICS_SHADOW_COMPARE_SAMPLE_RATE"] = 1.0
        app.config["ANALYTICS_SHADOW_COMPARE_MAX_DIFF_LOGS_PER_MIN"] = 20
        app.config["GOV_ENABLED"] = True
        app.config["GOV_ANALYTICS_MAX_RPM_PER_WORKSPACE"] = 1
        app.config["GOV_ANALYTICS_SOFT_DEGRADE_ON_LIMIT"] = True
        app.config["GOV_ANALYTICS_SHADOW_DISABLE_WHEN_DEGRADED"] = True
        app.config["GOV_ANALYTICS_DEGRADE_TTL_SECONDS"] = 120

        service = AnalyticsService(
            repository_factory=lambda _tenant: _FakeTransRepository(),
            read_model_repository_factory=lambda _tenant: _FailReadModelRepository(),
        )
        req = AnalyticsRequestInput(
            section="overview",
            role="admin",
            tenant_id="tenant-gov-shadow",
            request_args={},
            user_email="admin@demo.com",
            display_name="Admin",
            team_members=[],
        )

        with app.app_context():
            with app.test_request_context("/api/procurement/analytics/overview"):
                first = service.build_dashboard_payload(
                    db=None,
                    request_input=req,
                    parse_filters_fn=_parse_filters,
                    resolve_visibility_fn=_resolve_visibility,
                    build_payload_fn=lambda *_args, **_kwargs: {},
                )
            self.assertEqual(first.get("source"), "fallback")
            metrics_first = prometheus_metrics_text()
            self.assertIn('analytics_shadow_compare_total{primary_source="fallback",result="equal"} 1', metrics_first)

            with app.test_request_context("/api/procurement/analytics/overview"):
                second = service.build_dashboard_payload(
                    db=None,
                    request_input=req,
                    parse_filters_fn=_parse_filters,
                    resolve_visibility_fn=_resolve_visibility,
                    build_payload_fn=lambda *_args, **_kwargs: {},
                )
            self.assertTrue((second.get("governance") or {}).get("degraded"))
            metrics_second = prometheus_metrics_text()
            self.assertIn('analytics_shadow_compare_total{primary_source="fallback",result="equal"} 1', metrics_second)


class GovernanceWorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_metrics_for_tests()
        reset_governance_for_tests()
        self._temp_db = TempDbSandbox(prefix="governance_worker")
        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=True,
            AUTH_ENABLED=False,
            GOV_ENABLED=True,
            ERP_CIRCUIT_ENABLED=False,
            GOV_WORKER_MAX_CONCURRENT_PER_WORKSPACE=1,
            GOV_WORKER_MAX_QUEUE_BACKLOG_PER_WORKSPACE=500,
            GOV_WORKER_BACKOFF_ON_LIMIT_SECONDS=30,
            GOV_WORKER_DEADLETTER_ON_OVERFLOW=False,
        )
        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.tenant_id = "tenant-governance-worker"
        self.headers = {"X-Tenant-Id": self.tenant_id}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()
        reset_metrics_for_tests()
        reset_governance_for_tests()

    def _create_purchase_order(self) -> int:
        seed_res = self.client.post("/api/procurement/seed", headers=self.headers)
        self.assertIn(seed_res.status_code, (200, 201))
        items_res = self.client.get("/api/procurement/purchase-request-items/open", headers=self.headers)
        self.assertEqual(items_res.status_code, 200)
        item_ids = [item["id"] for item in (items_res.get_json() or {}).get("items", [])][:2]
        if not item_ids:
            with self.app.app_context():
                db = get_db()
                row = db.execute(
                    "SELECT COALESCE(MAX(id), 0) AS max_id FROM purchase_orders WHERE tenant_id = ?",
                    (self.tenant_id,),
                ).fetchone()
                next_id = int((row["max_id"] if row else 0) or 0) + 1
                number = f"OC-GOV-{next_id}"
                db.execute(
                    """
                    INSERT INTO purchase_orders (
                        id, number, award_id, supplier_name, status, currency, total_amount,
                        erp_last_error, external_id, tenant_id, created_at, updated_at
                    )
                    VALUES (?, ?, NULL, ?, 'approved', 'BRL', 100, NULL, NULL, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (next_id, number, "Fornecedor GOV", self.tenant_id),
                )
                db.commit()
            return next_id

        rfq_res = self.client.post(
            "/api/procurement/rfqs",
            headers=self.headers,
            json={"title": "RFQ GOV", "purchase_request_item_ids": item_ids},
        )
        self.assertEqual(rfq_res.status_code, 201)
        rfq_id = int((rfq_res.get_json() or {}).get("id") or 0)
        self.assertGreater(rfq_id, 0)

        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "gov", "supplier_name": "Fornecedor GOV", "confirm": True},
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

    def _latest_run(self) -> dict:
        with self.app.app_context():
            db = get_db()
            row = db.execute(
                """
                SELECT id, status, attempt, error_summary, payload_ref
                FROM sync_runs
                WHERE tenant_id = ? AND scope = 'purchase_order'
                ORDER BY id DESC
                LIMIT 1
                """,
                (self.tenant_id,),
            ).fetchone()
            return dict(row) if row else {}

    def test_worker_concurrency_throttles_and_defers_without_error(self) -> None:
        po_id = self._create_purchase_order()
        self._queue_po(po_id)

        fairness = get_worker_fairness()
        with fairness.enter_workspace(self.tenant_id) as acquired:
            self.assertTrue(acquired)
            result = process_erp_outbox_once(self.app, tenant_id=self.tenant_id, push_fn=lambda _po: {"status": "accepted"})

        self.assertEqual(result.get("deferred"), 1)
        self.assertEqual(result.get("failed"), 0)
        run = self._latest_run()
        self.assertEqual(int(run.get("attempt") or 0), 0)
        self.assertEqual(str(run.get("status") or ""), "queued")
        self.assertIn("governance_throttled", str(run.get("error_summary") or ""))

    def test_worker_overflow_respects_deadletter_flag(self) -> None:
        with self.app.app_context():
            self.app.config["GOV_WORKER_MAX_QUEUE_BACKLOG_PER_WORKSPACE"] = 1
            self.app.config["GOV_WORKER_DEADLETTER_ON_OVERFLOW"] = False

        po_a = self._create_purchase_order()
        po_b = self._create_purchase_order()
        self._queue_po(po_a)
        self._queue_po(po_b)

        deferred_result = process_erp_outbox_once(self.app, tenant_id=self.tenant_id, limit=1, push_fn=lambda _po: {"status": "accepted"})
        self.assertEqual(deferred_result.get("deferred"), 1)
        self.assertEqual(deferred_result.get("failed"), 0)

        with self.app.app_context():
            self.app.config["GOV_WORKER_DEADLETTER_ON_OVERFLOW"] = True

        failed_result = process_erp_outbox_once(self.app, tenant_id=self.tenant_id, limit=1, push_fn=lambda _po: {"status": "accepted"})
        self.assertEqual(failed_result.get("failed"), 1)
        latest = self._latest_run()
        self.assertEqual(str(latest.get("status") or ""), "failed")
        payload_ref = json.loads(str(latest.get("payload_ref") or "{}"))
        self.assertTrue(payload_ref.get("dead_letter"))

        metrics = prometheus_metrics_text()
        self.assertIn("governance_worker_throttled_total", metrics)
        self.assertIn("governance_worker_deferred_total", metrics)
        self.assertIn("governance_worker_overflow_total", metrics)


if __name__ == "__main__":
    unittest.main()
