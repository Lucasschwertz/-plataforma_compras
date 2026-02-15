from __future__ import annotations

import os
import random
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from app import create_app
from app.config import Config
from app.db import close_db
from app.observability_slo import get_error_rate_percent, get_p95_ms, reset_http_metrics
from tests.helpers.temp_db import TempDbSandbox


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float_env(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass
class _LoadStats:
    analytics_total: int = 0
    analytics_degraded_total: int = 0
    analytics_invalid_source_total: int = 0


@unittest.skipUnless(_bool_env("RUN_SLO_SYNTHETIC", False), "Synthetic SLO test runs only when RUN_SLO_SYNTHETIC=1")
class SyntheticSloLoadTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_http_metrics()
        self._temp_db = TempDbSandbox(prefix="slo_synthetic")
        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=True,
            AUTH_ENABLED=False,
            RATE_LIMIT_ENABLED=False,
            GOV_ENABLED=True,
            GOV_ANALYTICS_MAX_RPM_PER_WORKSPACE=20,
            GOV_ANALYTICS_MAX_CONCURRENT_PER_WORKSPACE=3,
            GOV_ANALYTICS_SOFT_DEGRADE_ON_LIMIT=True,
            GOV_ANALYTICS_DEGRADE_TTL_SECONDS=60,
            GOV_ANALYTICS_SHADOW_DISABLE_WHEN_DEGRADED=True,
            GOV_ANALYTICS_CACHE_TTL_SECONDS_WHEN_DEGRADED=120,
            ANALYTICS_SHADOW_COMPARE_ENABLED=False,
            ANALYTICS_READ_MODEL_ENABLED=False,
            SLO_ENABLED=True,
        )
        self.app = create_app(TempConfig)

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()
        reset_http_metrics()

    def test_slo_synthetic_load_with_governance(self) -> None:
        if not bool(self.app.config.get("SLO_ENABLED", True)):
            self.skipTest("SLO gate disabled by config.")

        workspace_count = max(1, int(self.app.config.get("SLO_TEST_WORKSPACES", _int_env("SLO_TEST_WORKSPACES", 5))))
        concurrency = max(1, int(self.app.config.get("SLO_TEST_CONCURRENCY", _int_env("SLO_TEST_CONCURRENCY", 12))))
        duration_seconds = max(1, int(self.app.config.get("SLO_TEST_DURATION_SECONDS", _int_env("SLO_TEST_DURATION_SECONDS", 8))))
        analytics_p95_limit = float(self.app.config.get("SLO_ANALYTICS_P95_MS", _int_env("SLO_ANALYTICS_P95_MS", 1200)))
        http_error_limit = float(
            self.app.config.get("SLO_HTTP_ERROR_RATE_MAX_PERCENT", _float_env("SLO_HTTP_ERROR_RATE_MAX_PERCENT", 1.0))
        )
        degrade_allowed = bool(self.app.config.get("SLO_ANALYTICS_DEGRADE_ALLOWED", _bool_env("SLO_ANALYTICS_DEGRADE_ALLOWED", True)))

        workspaces = [f"tenant-slo-{index}" for index in range(workspace_count)]
        endpoints = (
            "/health",
            "/api/procurement/analytics?section=overview",
            "/api/procurement/analytics?section=quality_erp&period_basis=po_updated_at",
        )
        deadline = time.perf_counter() + float(duration_seconds)

        def _worker(worker_idx: int) -> _LoadStats:
            rng = random.Random(42 + worker_idx)
            stats = _LoadStats()
            with self.app.test_client() as client:
                while time.perf_counter() < deadline:
                    workspace = workspaces[rng.randrange(len(workspaces))]
                    endpoint = endpoints[rng.randrange(len(endpoints))]
                    response = client.get(endpoint, headers={"X-Tenant-Id": workspace})
                    if endpoint.startswith("/api/procurement/analytics"):
                        stats.analytics_total += 1
                        payload = response.get_json(silent=True) or {}
                        governance = payload.get("governance") if isinstance(payload, dict) else None
                        if isinstance(governance, dict) and governance.get("degraded") is True:
                            stats.analytics_degraded_total += 1
                        source = payload.get("source") if isinstance(payload, dict) else None
                        if not isinstance(source, str) or not source.strip():
                            stats.analytics_invalid_source_total += 1
                    time.sleep(0.001)
            return stats

        aggregated = _LoadStats()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(_worker, idx) for idx in range(concurrency)]
            for future in futures:
                partial = future.result()
                aggregated.analytics_total += int(partial.analytics_total)
                aggregated.analytics_degraded_total += int(partial.analytics_degraded_total)
                aggregated.analytics_invalid_source_total += int(partial.analytics_invalid_source_total)

        analytics_p95 = get_p95_ms("/api/procurement/analytics")
        analytics_error_rate = get_error_rate_percent("/api/procurement/analytics")
        http_error_rate = get_error_rate_percent("")

        self.assertLessEqual(
            http_error_rate,
            http_error_limit,
            msg=f"Global HTTP error rate above SLO: {http_error_rate:.3f}% > {http_error_limit:.3f}%",
        )
        self.assertLessEqual(
            analytics_error_rate,
            http_error_limit,
            msg=f"Analytics error rate above SLO: {analytics_error_rate:.3f}% > {http_error_limit:.3f}%",
        )
        self.assertEqual(
            aggregated.analytics_invalid_source_total,
            0,
            msg="Analytics payload returned invalid source in synthetic load.",
        )

        if analytics_p95 <= analytics_p95_limit:
            return

        if not degrade_allowed:
            self.fail(
                f"Analytics p95 above SLO and degraded mode is not allowed: {analytics_p95:.2f}ms > {analytics_p95_limit:.2f}ms"
            )

        self.assertGreater(
            aggregated.analytics_degraded_total,
            0,
            msg="Expected degraded analytics responses when p95 exceeds SLO threshold.",
        )


if __name__ == "__main__":
    unittest.main()
