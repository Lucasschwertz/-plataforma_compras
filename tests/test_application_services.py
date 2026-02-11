import unittest

from app.application.analytics_service import AnalyticsService
from app.application.auth_service import AuthService
from app.application.erp_outbox_service import ErpOutboxService
from app.application.procurement_service import ProcurementService
from app.errors import IntegrationError
from app.domain.contracts import (
    AnalyticsRequestInput,
    AuthLoginInput,
    AuthRegisterInput,
    PurchaseOrderErpIntentInput,
    RfqCreateInput,
)


class _FakeAuthRepo:
    def __init__(self) -> None:
        self.created = []
        self.tenants = []
        self._users = {}

    def find_user_by_email(self, _db, email: str):
        return self._users.get(email)

    def email_exists(self, _db, email: str) -> bool:
        return email in self._users

    def ensure_tenant(self, _db, tenant_id: str, name: str) -> None:
        self.tenants.append((tenant_id, name))

    def create_user(self, _db, *, email: str, password: str, display_name: str | None, tenant_id: str) -> None:
        self.created.append((email, tenant_id))
        self._users[email] = {
            "email": email,
            "display_name": display_name,
            "tenant_id": tenant_id,
            "password_hash": "unused-in-this-test",
        }


class ApplicationServicesTest(unittest.TestCase):
    def test_auth_service_login_from_env_users(self) -> None:
        service = AuthService(repository=_FakeAuthRepo())
        user = service.login(
            db=None,
            auth_input=AuthLoginInput(email="manager@demo.com", password="123"),
            raw_users="manager@demo.com:123:tenant-ops:Manager:manager",
        )
        self.assertIsNotNone(user)
        self.assertEqual(user.tenant_id, "tenant-ops")
        self.assertEqual(user.role, "manager")

    def test_auth_service_register_uses_repository(self) -> None:
        repo = _FakeAuthRepo()
        service = AuthService(repository=repo)
        user = service.register(
            db=None,
            auth_input=AuthRegisterInput(
                email="novo@demo.com",
                password="segredo",
                display_name="Novo",
                company_name="Empresa Nova",
            ),
        )
        self.assertEqual(user.email, "novo@demo.com")
        self.assertTrue(repo.created)
        self.assertTrue(repo.tenants)

    def test_analytics_service_cache_avoids_recompute(self) -> None:
        service = AnalyticsService(ttl_seconds=60)
        calls = {"count": 0}

        def parse_filters(args, workspace):
            return {"raw": {"workspace_id": workspace, "status": args.get("status", "")}}

        def resolve_visibility(*_args, **_kwargs):
            return {"scope": "all", "actors": []}

        def build_payload(*_args, **_kwargs):
            calls["count"] += 1
            return {"ok": True}

        req = AnalyticsRequestInput(
            section="overview",
            role="admin",
            tenant_id="tenant-1",
            request_args={"status": "ordered"},
            user_email="admin@demo.com",
            display_name="Admin",
            team_members=[],
        )
        first = service.build_dashboard_payload(
            db=None,
            request_input=req,
            parse_filters_fn=parse_filters,
            resolve_visibility_fn=resolve_visibility,
            build_payload_fn=build_payload,
        )
        second = service.build_dashboard_payload(
            db=None,
            request_input=req,
            parse_filters_fn=parse_filters,
            resolve_visibility_fn=resolve_visibility,
            build_payload_fn=build_payload,
        )
        self.assertEqual(first, second)
        self.assertEqual(calls["count"], 1)

    def test_procurement_service_create_rfq_delegates_to_core(self) -> None:
        service = ProcurementService()

        def core(_db, _tenant_id, title, item_ids):
            self.assertEqual(title, "Nova RFQ")
            self.assertEqual(item_ids, [1, 2])
            return ({"id": 99, "title": title}, None, 201)

        result = service.create_rfq(
            db=None,
            tenant_id="tenant-1",
            create_input=RfqCreateInput(title="Nova RFQ", purchase_request_item_ids=[1, 2]),
            create_rfq_core_fn=core,
        )
        self.assertEqual(result.status_code, 201)
        self.assertEqual(result.payload.get("id"), 99)

    def test_erp_outbox_service_register_intent(self) -> None:
        outbox = ErpOutboxService()
        result = outbox.register_erp_intent(
            db=None,
            tenant_id="tenant-1",
            purchase_order={"external_id": "PO-1"},
            intent_input=PurchaseOrderErpIntentInput(
                purchase_order_id=123,
                request_id="req-1",
                payload={},
            ),
            queue_push_fn=lambda *_args, **_kwargs: {"sync_run_id": 77, "already_queued": False},
            success_message_fn=lambda key, fallback=None: key if fallback is None else key,
        )
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.payload.get("sync_run_id"), 77)
        self.assertEqual(result.payload.get("status"), "sent_to_erp")

    def test_procurement_service_send_po_immediate_success(self) -> None:
        service = ProcurementService()
        calls = {"count": 0}

        def load_po(_db, _tenant, _po_id):
            calls["count"] += 1
            if calls["count"] == 1:
                return {"id": 123, "status": "approved", "external_id": "PO-123"}
            return {"id": 123, "status": "erp_accepted", "external_id": "PO-123"}

        result = service.send_po_to_erp_intent(
            db=None,
            tenant_id="tenant-1",
            intent_input=PurchaseOrderErpIntentInput(
                purchase_order_id=123,
                request_id="req-1",
                payload={"confirm": True},
            ),
            load_purchase_order_fn=load_po,
            find_pending_push_fn=lambda *_args, **_kwargs: None,
            flow_action_allowed_fn=lambda *_args, **_kwargs: True,
            forbidden_action_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not block")),
            require_confirmation_fn=lambda *_args, **_kwargs: None,
            queue_push_fn=lambda *_args, **_kwargs: {"sync_run_id": 11, "already_queued": False},
            process_outbox_fn=lambda *_args, **_kwargs: {"processed": 1, "succeeded": 1, "failed": 0, "requeued": 0},
            push_purchase_order_fn=lambda po: {"external_id": po.get("external_id"), "status": "erp_accepted"},
            immediate_response=True,
            err_fn=lambda key, fallback=None: key if fallback is None else key,
            ok_fn=lambda key, fallback=None: key if fallback is None else key,
        )
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.payload.get("status"), "erp_accepted")
        self.assertEqual(result.payload.get("sync_run_id"), 11)

    def test_procurement_service_send_po_immediate_rejection_raises(self) -> None:
        service = ProcurementService()
        calls = {"count": 0}

        def load_po(_db, _tenant, _po_id):
            calls["count"] += 1
            if calls["count"] == 1:
                return {"id": 123, "status": "approved", "external_id": "PO-123"}
            return {"id": 123, "status": "erp_error", "external_id": "PO-123", "erp_last_error": "ERP HTTP 422 rejected"}

        with self.assertRaises(IntegrationError):
            service.send_po_to_erp_intent(
                db=None,
                tenant_id="tenant-1",
                intent_input=PurchaseOrderErpIntentInput(
                    purchase_order_id=123,
                    request_id="req-1",
                    payload={"confirm": True},
                ),
                load_purchase_order_fn=load_po,
                find_pending_push_fn=lambda *_args, **_kwargs: None,
                flow_action_allowed_fn=lambda *_args, **_kwargs: True,
                forbidden_action_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not block")),
                require_confirmation_fn=lambda *_args, **_kwargs: None,
                queue_push_fn=lambda *_args, **_kwargs: {"sync_run_id": 11, "already_queued": False},
                process_outbox_fn=lambda *_args, **_kwargs: {"processed": 1, "succeeded": 0, "failed": 1, "requeued": 0},
                push_purchase_order_fn=lambda _po: {"status": "erp_error"},
                immediate_response=True,
                err_fn=lambda key, fallback=None: key if fallback is None else key,
                ok_fn=lambda key, fallback=None: key if fallback is None else key,
            )


if __name__ == "__main__":
    unittest.main()
