from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path


class ErpGatewayGuardTest(unittest.TestCase):
    def _run_python(self, script: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        repo_root = Path(__file__).resolve().parents[1]
        return subprocess.run(
            [sys.executable, "-c", script],
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_erp_client_import_outside_worker_is_blocked(self) -> None:
        env = os.environ.copy()
        env.pop("ERP_CLIENT_CONTEXT", None)
        result = self._run_python("import app.erp_client", env)
        self.assertNotEqual(result.returncode, 0)
        combined_output = f"{result.stdout}\n{result.stderr}"
        self.assertIn("restricted to worker context", combined_output)

    def test_erp_client_import_in_worker_context_is_allowed(self) -> None:
        env = os.environ.copy()
        env["ERP_CLIENT_CONTEXT"] = "worker"
        result = self._run_python("import app.erp_client; print('ok')", env)
        self.assertEqual(result.returncode, 0)
        self.assertIn("ok", result.stdout)

    def test_worker_runtime_builds_push_callable_using_gateway(self) -> None:
        from app.workers import erp_runtime

        erp_runtime._gateway.cache_clear()
        os.environ.pop("ERP_CLIENT_CONTEXT", None)
        push_fn = erp_runtime.build_worker_push_fn()
        payload = push_fn({"id": 11, "number": "PO-11"})

        self.assertEqual(os.environ.get("ERP_CLIENT_CONTEXT"), "worker")
        self.assertEqual(payload.get("status"), "erp_accepted")
        self.assertEqual(payload.get("external_id"), "SENIOR-OC-000011")
        erp_runtime._gateway.cache_clear()


if __name__ == "__main__":
    unittest.main()
