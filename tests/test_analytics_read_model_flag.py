import unittest

from flask import Flask

from app.contexts.analytics.application.service import AnalyticsService
from app.domain.contracts import AnalyticsRequestInput
from app.observability import prometheus_metrics_text, reset_metrics_for_tests


class _FakeTransRepository:
    def __init__(self) -> None:
        self.calls = 0

    def build_dashboard_payload(self, _db, **_kwargs):
        self.calls += 1
        return {
            "section": {"key": "overview"},
            "filters": {},
            "visibility": {"role": "admin", "scope": "workspace"},
            "meta": {"records_count": 0, "comparison_records_count": 0, "generated_at": "2026-02-14T00:00:00Z"},
            "alerts": [],
            "alerts_meta": {"active_count": 0, "has_active": False},
            "kpis": [],
            "charts": [],
            "drilldown": {},
        }


class _FakeReadModelRepository:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls = 0
        self.fail = fail

    def get_kpis(self, _db, workspace_id=None, filters=None, section=None):
        _ = (workspace_id, filters, section)
        self.calls += 1
        if self.fail:
            raise RuntimeError("forced read model failure")
        return {
            "backlog_open": {"value_int": 3, "value_num": 0, "avg_num": 0, "rows": 1},
            "economy_abs": {"value_int": 0, "value_num": 120.5, "avg_num": 120.5, "rows": 1},
            "awaiting_erp": {"value_int": 1, "value_num": 0, "avg_num": 0, "rows": 1},
            "erp_rejections": {"value_int": 1, "value_num": 0, "avg_num": 0, "rows": 1},
        }

    def get_supplier_metrics(self, _db, workspace_id=None, filters=None):
        _ = (workspace_id, filters)
        return []

    def get_stage_metrics(self, _db, workspace_id=None, filters=None):
        _ = (workspace_id, filters)
        return []

    def get_meta(self, _db, workspace_id=None, filters=None):
        _ = (workspace_id, filters)
        return {
            "records_count": 4,
            "comparison_records_count": 0,
            "generated_at": "2026-02-14T00:00:00Z",
        }


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
            "start_date": "",
            "end_date": "",
            "supplier": "",
            "buyer": "",
            "status": "",
            "purchase_type": "",
            "period_basis": "pr_created_at",
            "workspace_id": workspace_id,
        },
    }


def _resolve_visibility(_role, _user_email, _display_name, _team_members):
    return {"role": "admin", "scope": "workspace", "actors": []}


class AnalyticsReadModelFlagTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_metrics_for_tests()

    def tearDown(self) -> None:
        reset_metrics_for_tests()

    @staticmethod
    def _request_input() -> AnalyticsRequestInput:
        return AnalyticsRequestInput(
            section="overview",
            role="admin",
            tenant_id="tenant-read-model",
            request_args={},
            user_email="admin@demo.com",
            display_name="Admin",
            team_members=[],
        )

    def test_flag_disabled_uses_transacional_source(self) -> None:
        app = Flask(__name__)
        app.config["ANALYTICS_READ_MODEL_ENABLED"] = False
        trans_repo = _FakeTransRepository()
        read_repo = _FakeReadModelRepository()
        service = AnalyticsService(
            repository_factory=lambda _tenant: trans_repo,
            read_model_repository_factory=lambda _tenant: read_repo,
        )

        with app.app_context():
            payload = service.build_dashboard_payload(
                db=None,
                request_input=self._request_input(),
                parse_filters_fn=_parse_filters,
                resolve_visibility_fn=_resolve_visibility,
                build_payload_fn=lambda *_args, **_kwargs: {},
            )

        self.assertEqual(payload.get("source"), "transacional")
        self.assertEqual(trans_repo.calls, 1)
        self.assertEqual(read_repo.calls, 0)
        metrics = prometheus_metrics_text()
        self.assertIn('analytics_read_model_hits_total{source="transacional"} 1', metrics)

    def test_flag_enabled_uses_read_model_source(self) -> None:
        app = Flask(__name__)
        app.config["ANALYTICS_READ_MODEL_ENABLED"] = True
        trans_repo = _FakeTransRepository()
        read_repo = _FakeReadModelRepository()
        service = AnalyticsService(
            repository_factory=lambda _tenant: trans_repo,
            read_model_repository_factory=lambda _tenant: read_repo,
        )

        with app.app_context():
            payload = service.build_dashboard_payload(
                db=None,
                request_input=self._request_input(),
                parse_filters_fn=_parse_filters,
                resolve_visibility_fn=_resolve_visibility,
                build_payload_fn=lambda *_args, **_kwargs: {},
            )

        self.assertEqual(payload.get("source"), "read_model")
        self.assertEqual(read_repo.calls, 1)
        self.assertEqual(trans_repo.calls, 0)
        self.assertIn("kpis", payload)
        self.assertIn("charts", payload)
        self.assertIn("drilldown", payload)
        metrics = prometheus_metrics_text()
        self.assertIn('analytics_read_model_hits_total{source="read_model"} 1', metrics)

    def test_flag_enabled_falls_back_when_read_model_fails(self) -> None:
        app = Flask(__name__)
        app.config["ANALYTICS_READ_MODEL_ENABLED"] = True
        trans_repo = _FakeTransRepository()
        read_repo = _FakeReadModelRepository(fail=True)
        service = AnalyticsService(
            repository_factory=lambda _tenant: trans_repo,
            read_model_repository_factory=lambda _tenant: read_repo,
        )

        with app.app_context():
            payload = service.build_dashboard_payload(
                db=None,
                request_input=self._request_input(),
                parse_filters_fn=_parse_filters,
                resolve_visibility_fn=_resolve_visibility,
                build_payload_fn=lambda *_args, **_kwargs: {},
            )

        self.assertEqual(payload.get("source"), "fallback")
        self.assertEqual(read_repo.calls, 1)
        self.assertEqual(trans_repo.calls, 1)
        metrics = prometheus_metrics_text()
        self.assertIn('analytics_read_model_hits_total{source="fallback"} 1', metrics)


if __name__ == "__main__":
    unittest.main()
