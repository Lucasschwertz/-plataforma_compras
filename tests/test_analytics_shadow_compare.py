import copy
import unittest
from unittest.mock import patch

from flask import Flask

from app.contexts.analytics.application.service import AnalyticsService
from app.contexts.analytics.application.shadow_compare import (
    _reset_shadow_compare_log_limiter_for_tests,
    diff_payload,
    hash_payload,
    normalize_payload,
)
from app.domain.contracts import AnalyticsRequestInput
from app.observability import prometheus_metrics_text, reset_metrics_for_tests


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


class _FakeTransRepository:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls = 0
        self.fail = fail

    def build_dashboard_payload(self, _db, **_kwargs):
        self.calls += 1
        if self.fail:
            raise RuntimeError("forced transacional failure")
        return {
            "section": {"key": "overview", "label": "Visao Geral"},
            "filters": {},
            "visibility": {"role": "admin", "scope": "workspace"},
            "meta": {"records_count": 1, "comparison_records_count": 0, "generated_at": "2026-02-14T00:00:00Z"},
            "alerts": [],
            "alerts_meta": {"active_count": 0, "has_active": False},
            "kpis": [
                {
                    "key": "backlog_open",
                    "label": "Backlog aberto",
                    "value": 3,
                    "display_value": "3",
                    "tooltip": "Solicitacoes ainda em aberto.",
                    "trend": {"direction": "flat", "delta_pct": 0.0, "display": "+0.0%", "label": "Estavel"},
                }
            ],
            "charts": [],
            "drilldown": {"title": "Itens recentes", "columns": [], "column_keys": [], "rows": []},
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
            "backlog_open": {"value_int": 3, "value_num": 0.0, "avg_num": 0.0, "rows": 1},
            "active_quotes": {"value_int": 2, "value_num": 0.0, "avg_num": 0.0, "rows": 1},
            "orders_in_progress": {"value_int": 1, "value_num": 0.0, "avg_num": 0.0, "rows": 1},
            "awaiting_erp": {"value_int": 1, "value_num": 0.0, "avg_num": 0.0, "rows": 1},
            "erp_rejections": {"value_int": 0, "value_num": 0.0, "avg_num": 0.0, "rows": 1},
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
            "records_count": 5,
            "comparison_records_count": 0,
            "generated_at": "2026-02-14T00:00:00Z",
        }


class AnalyticsShadowCompareTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_metrics_for_tests()
        _reset_shadow_compare_log_limiter_for_tests()

    def tearDown(self) -> None:
        reset_metrics_for_tests()
        _reset_shadow_compare_log_limiter_for_tests()

    @staticmethod
    def _request_input() -> AnalyticsRequestInput:
        return AnalyticsRequestInput(
            section="overview",
            role="admin",
            tenant_id="tenant-shadow",
            request_args={},
            user_email="admin@demo.com",
            display_name="Admin",
            team_members=[],
        )

    @staticmethod
    def _app(*, shadow_enabled: bool, sample_rate: float = 1.0) -> Flask:
        app = Flask(__name__)
        app.config["ANALYTICS_READ_MODEL_ENABLED"] = True
        app.config["ANALYTICS_SHADOW_COMPARE_ENABLED"] = shadow_enabled
        app.config["ANALYTICS_SHADOW_COMPARE_SAMPLE_RATE"] = sample_rate
        app.config["ANALYTICS_SHADOW_COMPARE_MAX_DIFF_LOGS_PER_MIN"] = 20
        return app

    def test_shadow_disabled_does_not_increment_metrics(self) -> None:
        app = self._app(shadow_enabled=False)
        trans_repo = _FakeTransRepository()
        read_repo = _FakeReadModelRepository()
        service = AnalyticsService(
            repository_factory=lambda _tenant: trans_repo,
            read_model_repository_factory=lambda _tenant: read_repo,
        )

        with app.app_context():
            with app.test_request_context("/api/procurement/analytics/overview"):
                payload = service.build_dashboard_payload(
                    db=None,
                    request_input=self._request_input(),
                    parse_filters_fn=_parse_filters,
                    resolve_visibility_fn=_resolve_visibility,
                    build_payload_fn=lambda *_args, **_kwargs: {},
                )

        self.assertEqual(payload.get("source"), "read_model")
        metrics = prometheus_metrics_text()
        self.assertNotIn("analytics_shadow_compare_total{", metrics)

    def test_shadow_enabled_equal_result(self) -> None:
        app = self._app(shadow_enabled=True, sample_rate=1.0)
        trans_repo = _FakeTransRepository()
        read_repo = _FakeReadModelRepository(fail=True)
        service = AnalyticsService(
            repository_factory=lambda _tenant: trans_repo,
            read_model_repository_factory=lambda _tenant: read_repo,
        )

        with app.app_context():
            with app.test_request_context("/api/procurement/analytics/overview"):
                payload = service.build_dashboard_payload(
                    db=None,
                    request_input=self._request_input(),
                    parse_filters_fn=_parse_filters,
                    resolve_visibility_fn=_resolve_visibility,
                    build_payload_fn=lambda *_args, **_kwargs: {},
                )

        self.assertEqual(payload.get("source"), "fallback")
        metrics = prometheus_metrics_text()
        self.assertIn('analytics_shadow_compare_total{primary_source="fallback",result="equal"} 1', metrics)

    def test_shadow_enabled_diff_logs_warning(self) -> None:
        app = self._app(shadow_enabled=True, sample_rate=1.0)
        trans_repo = _FakeTransRepository()
        read_repo = _FakeReadModelRepository()
        service = AnalyticsService(
            repository_factory=lambda _tenant: trans_repo,
            read_model_repository_factory=lambda _tenant: read_repo,
        )

        with app.app_context():
            with patch.object(app.logger, "warning") as warning_mock:
                with app.test_request_context("/api/procurement/analytics/overview"):
                    payload = service.build_dashboard_payload(
                        db=None,
                        request_input=self._request_input(),
                        parse_filters_fn=_parse_filters,
                        resolve_visibility_fn=_resolve_visibility,
                        build_payload_fn=lambda *_args, **_kwargs: {},
                    )
        self.assertEqual(payload.get("source"), "read_model")
        self.assertGreaterEqual(warning_mock.call_count, 1)
        metrics = prometheus_metrics_text()
        self.assertIn('analytics_shadow_compare_total{primary_source="read_model",result="diff"} 1', metrics)
        self.assertIn("analytics_shadow_compare_diff_fields_total", metrics)
        self.assertIn("analytics_shadow_compare_last_diff_timestamp", metrics)

    def test_shadow_error_does_not_break_response(self) -> None:
        app = self._app(shadow_enabled=True, sample_rate=1.0)
        trans_repo = _FakeTransRepository(fail=True)
        read_repo = _FakeReadModelRepository()
        service = AnalyticsService(
            repository_factory=lambda _tenant: trans_repo,
            read_model_repository_factory=lambda _tenant: read_repo,
        )

        with app.app_context():
            with app.test_request_context("/api/procurement/analytics/overview"):
                payload = service.build_dashboard_payload(
                    db=None,
                    request_input=self._request_input(),
                    parse_filters_fn=_parse_filters,
                    resolve_visibility_fn=_resolve_visibility,
                    build_payload_fn=lambda *_args, **_kwargs: {},
                )

        self.assertEqual(payload.get("source"), "read_model")
        metrics = prometheus_metrics_text()
        self.assertIn('analytics_shadow_compare_total{primary_source="read_model",result="error"} 1', metrics)

    def test_normalization_is_deterministic_and_ignores_volatile_fields(self) -> None:
        payload_a = {
            "source": "read_model",
            "request_id": "req-a",
            "meta": {"generated_at": "2026-02-14T00:00:00Z", "records_count": 1},
            "kpis": [
                {"key": "b", "label": "B", "value": 2.0},
                {"key": "a", "label": "A", "value": 1.004},
            ],
            "charts": [{"label": "  Ranking  ", "items": [{"label": "Y", "value": 2}, {"label": "X", "value": 1}]}],
            "drilldown": {"rows": [{"id": 2}, {"id": 1}]},
        }
        payload_b = {
            "request_id": "req-b",
            "source": "fallback",
            "meta": {"generated_at": "2026-02-14T10:00:00Z", "records_count": 1},
            "kpis": [
                {"label": "A", "key": "a", "value": 1.0},
                {"label": "B", "key": "b", "value": 2},
            ],
            "charts": [{"label": "Ranking", "items": [{"value": 1, "label": "X"}, {"value": 2, "label": "Y"}]}],
            "drilldown": {"rows": [{"id": 1}, {"id": 2}]},
        }

        normalized_a = normalize_payload(copy.deepcopy(payload_a))
        normalized_b = normalize_payload(copy.deepcopy(payload_b))
        comparison = diff_payload(payload_a, payload_b)

        self.assertEqual(normalized_a, normalized_b)
        self.assertTrue(comparison.get("equal"))
        self.assertEqual(hash_payload(payload_a), hash_payload(payload_b))


if __name__ == "__main__":
    unittest.main()

