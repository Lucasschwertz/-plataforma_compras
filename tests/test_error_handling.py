import os
import tempfile
import unittest
from unittest.mock import patch

from app import create_app
from app.config import Config
from app.db import close_db
from app.erp_client import ErpError
from app.ui_strings import error_message


def _build_temp_app(tmpdir: str, **overrides):
    db_path = os.path.join(tmpdir, "plataforma_compras_test.db")
    attrs = {
        "DATABASE_DIR": tmpdir,
        "DB_PATH": db_path,
        "SYNC_SCHEDULER_ENABLED": False,
        "PROPAGATE_EXCEPTIONS": False,
    }
    attrs.update(overrides)
    temp_config = type("TempConfig", (Config,), attrs)
    return create_app(temp_config)


class ErrorPermissionTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(
            prefix="pc_error_perm_",
            dir=os.getcwd(),
            ignore_cleanup_errors=True,
        )
        self.app = _build_temp_app(
            self._tmpdir.name,
            TESTING=False,
            AUTH_ENABLED=True,
        )
        self.client = self.app.test_client()
        self.headers = {"X-Tenant-Id": "tenant-perm"}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._tmpdir.cleanup()

    def test_permission_error_for_unauthenticated_api(self) -> None:
        response = self.client.get("/api/procurement/inbox", headers=self.headers)
        self.assertEqual(response.status_code, 401)

        payload = response.get_json()
        self.assertEqual(payload.get("error"), "auth_required")
        self.assertEqual(payload.get("message"), error_message("auth_required"))
        self.assertTrue((payload.get("request_id") or "").strip())
        self.assertNotIn("Traceback", response.get_data(as_text=True))


class ErrorHandlingApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(
            prefix="pc_error_api_",
            dir=os.getcwd(),
            ignore_cleanup_errors=True,
        )
        self.app = _build_temp_app(
            self._tmpdir.name,
            TESTING=True,
            AUTH_ENABLED=False,
        )
        self.client = self.app.test_client()
        self.tenant_id = "tenant-error-api"
        self.headers = {"X-Tenant-Id": self.tenant_id}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._tmpdir.cleanup()

    def _create_purchase_order(self) -> int:
        seed_res = self.client.post("/api/procurement/seed", headers=self.headers)
        self.assertIn(seed_res.status_code, (200, 201))

        items_res = self.client.get("/api/procurement/purchase-request-items/open", headers=self.headers)
        self.assertEqual(items_res.status_code, 200)
        item_ids = [item["id"] for item in (items_res.get_json() or {}).get("items", [])][:2]
        self.assertTrue(item_ids)

        rfq_res = self.client.post(
            "/api/procurement/rfqs",
            headers=self.headers,
            json={"title": "RFQ Error Test", "purchase_request_item_ids": item_ids},
        )
        self.assertEqual(rfq_res.status_code, 201)
        rfq_id = int(rfq_res.get_json()["id"])

        award_res = self.client.post(
            f"/api/procurement/rfqs/{rfq_id}/award",
            headers=self.headers,
            json={"reason": "error_test", "supplier_name": "Fornecedor Erro", "confirm": True},
        )
        self.assertEqual(award_res.status_code, 201)
        award_id = int(award_res.get_json()["award_id"])

        po_res = self.client.post(
            f"/api/procurement/awards/{award_id}/purchase-orders?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(po_res.status_code, 201)
        return int(po_res.get_json()["purchase_order_id"])

    def test_validation_error_for_invalid_flow_action(self) -> None:
        purchase_order_id = self._create_purchase_order()

        push_res = self.client.post(
            f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(push_res.status_code, 200)
        self.assertEqual(push_res.get_json().get("status"), "erp_accepted")

        cancel_res = self.client.delete(
            f"/api/procurement/purchase-orders/{purchase_order_id}",
            headers=self.headers,
        )
        self.assertEqual(cancel_res.status_code, 409)
        payload = cancel_res.get_json()
        self.assertEqual(payload.get("error"), "action_not_allowed_for_status")
        self.assertEqual(payload.get("message"), error_message("action_not_allowed_for_status"))
        self.assertTrue((payload.get("request_id") or "").strip())

    def test_integration_error_for_erp_rejection(self) -> None:
        purchase_order_id = self._create_purchase_order()

        with patch("app.routes.procurement_routes.push_purchase_order", side_effect=ErpError("ERP HTTP 422: rejected")):
            response = self.client.post(
                f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
                headers=self.headers,
            )

        self.assertEqual(response.status_code, 422)
        payload = response.get_json()
        self.assertEqual(payload.get("error"), "erp_order_rejected")
        self.assertEqual(payload.get("message"), error_message("erp_order_rejected"))
        self.assertNotIn("details", payload)
        self.assertNotIn("ERP HTTP", response.get_data(as_text=True))

    def test_stack_trace_not_exposed_for_unhandled_error(self) -> None:
        purchase_order_id = self._create_purchase_order()

        with patch("app.routes.procurement_routes.push_purchase_order", side_effect=RuntimeError("stack_secret_token")):
            response = self.client.post(
                f"/api/procurement/purchase-orders/{purchase_order_id}/push-to-erp?confirm=true",
                headers=self.headers,
            )

        self.assertEqual(response.status_code, 500)
        payload = response.get_json()
        self.assertEqual(payload.get("error"), "unexpected_error")
        self.assertEqual(payload.get("message"), error_message("unexpected_error"))
        body = response.get_data(as_text=True)
        self.assertNotIn("Traceback", body)
        self.assertNotIn("stack_secret_token", body)


if __name__ == "__main__":
    unittest.main()
