import re
import unittest

from app import create_app
from app.config import Config
from app.db import close_db
from app.observability import reset_metrics_for_tests
from app.security import reset_rate_limiter_for_tests
from app.ui_strings import error_message
from tests.helpers.temp_db import TempDbSandbox


def _build_temp_config(temp_db: TempDbSandbox, **overrides):
    attrs = {
        "DATABASE_DIR": temp_db.temp_dir,
        "DB_PATH": temp_db.db_path,
        "TESTING": False,
        "DB_AUTO_INIT": False,
        "AUTH_ENABLED": False,
        "SYNC_SCHEDULER_ENABLED": False,
        "RATE_LIMIT_ENABLED": True,
        "RATE_LIMIT_WINDOW_SECONDS": 60,
        "RATE_LIMIT_MAX_REQUESTS": 300,
    }
    attrs.update(overrides)
    return type("TempConfig", (Config,), attrs)


class SecurityHardeningTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="security_hardening")
        temp_config = _build_temp_config(self._temp_db)
        self.app = create_app(temp_config)
        self.client = self.app.test_client()
        reset_rate_limiter_for_tests()
        reset_metrics_for_tests()

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()
        reset_rate_limiter_for_tests()
        reset_metrics_for_tests()

    def test_login_requires_valid_csrf(self) -> None:
        get_res = self.client.get("/login")
        self.assertEqual(get_res.status_code, 200)
        body = get_res.get_data(as_text=True)
        match = re.search(r'name="csrf_token"\s+value="([^"]+)"', body)
        self.assertIsNotNone(match)
        csrf_value = match.group(1)

        missing_token = self.client.post(
            "/login",
            data={"email": "user@demo.com", "password": "wrong"},
        )
        self.assertEqual(missing_token.status_code, 200)
        self.assertIn(error_message("csrf_invalid"), missing_token.get_data(as_text=True))
        self.assertTrue(csrf_value.strip())

    def test_rate_limit_blocks_excessive_api_calls(self) -> None:
        with self.app.app_context():
            self.app.config["RATE_LIMIT_MAX_REQUESTS"] = 2

        first = self.client.get("/api/unknown")
        second = self.client.get("/api/unknown")
        third = self.client.get("/api/unknown")

        self.assertEqual(first.status_code, 404)
        self.assertEqual(second.status_code, 404)
        self.assertEqual(third.status_code, 429)
        payload = third.get_json() or {}
        self.assertEqual(payload.get("error"), "rate_limit_exceeded")
        self.assertEqual(payload.get("message"), error_message("rate_limit_exceeded"))
        self.assertGreaterEqual(int(payload.get("retry_after") or 0), 0)

    def test_health_exposes_worker_queue_and_http_metrics(self) -> None:
        self.client.get("/api/unknown")

        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}
        self.assertIn("metrics", payload)
        self.assertIn("worker", payload)
        self.assertIn("http", payload.get("metrics") or {})
        self.assertIn("queue", payload.get("worker") or {})
        self.assertGreaterEqual(int((payload.get("metrics") or {}).get("http", {}).get("requests_total", 0)), 1)

    def test_security_headers_present(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertEqual(response.headers.get("X-Frame-Options"), "DENY")
        self.assertEqual(response.headers.get("Referrer-Policy"), "strict-origin-when-cross-origin")
        self.assertTrue((response.headers.get("Content-Security-Policy") or "").strip())
        self.assertTrue((response.headers.get("X-Request-Id") or "").strip())
        self.assertTrue((response.headers.get("X-Response-Time-Ms") or "").strip())


if __name__ == "__main__":
    unittest.main()
