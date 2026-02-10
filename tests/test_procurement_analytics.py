import os
import tempfile
import unittest
from unittest.mock import patch

from app import create_app
from app.config import Config
from app.db import close_db, get_db
from app.routes import procurement_routes
from app.ui_strings import error_message


class ProcurementAnalyticsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(
            prefix="pc_analytics_test_",
            dir=os.getcwd(),
            ignore_cleanup_errors=True,
        )
        db_path = os.path.join(self._tmpdir.name, "plataforma_compras_test.db")

        class TempConfig(Config):
            DATABASE_DIR = self._tmpdir.name
            DB_PATH = db_path
            TESTING = True
            AUTH_ENABLED = False

        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.tenant_id = "tenant-analytics"
        self.headers = {"X-Tenant-Id": self.tenant_id}
        procurement_routes._clear_analytics_cache_for_tests()
        self._seed_fixture()

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        procurement_routes._clear_analytics_cache_for_tests()
        self._tmpdir.cleanup()

    def _set_role(self, role: str, display_name: str, *, team_members=None) -> None:
        with self.client.session_transaction() as session:
            session["tenant_id"] = self.tenant_id
            session["user_role"] = role
            session["display_name"] = display_name
            session["user_email"] = f"{display_name.lower().replace(' ', '.')}@demo.com"
            if team_members is not None:
                session["team_members"] = team_members
            else:
                session.pop("team_members", None)

    def _seed_fixture(self) -> None:
        with self.app.app_context():
            db = get_db()
            tid = self.tenant_id

            db.execute(
                """
                INSERT INTO suppliers (id, name, risk_flags, tenant_id, created_at, updated_at)
                VALUES
                  (9101, 'Fornecedor A', '{"late_delivery": false}', ?, '2026-01-01 00:00:00', '2026-01-01 00:00:00'),
                  (9102, 'Fornecedor B', '{"late_delivery": true}', ?, '2026-01-01 00:00:00', '2026-01-01 00:00:00')
                """,
                (tid, tid),
            )

            db.execute(
                """
                INSERT INTO purchase_requests (
                    id, number, status, priority, requested_by, department, needed_at, tenant_id, created_at, updated_at
                ) VALUES
                  (9201, 'SR-001', 'ordered', 'urgent', 'Buyer One', 'Compras', '2026-01-03', ?, '2026-01-01 08:00:00', '2026-01-02 13:00:00'),
                  (9202, 'SR-002', 'ordered', 'medium', 'Buyer Two', 'Compras', '2026-01-10', ?, '2026-01-05 08:00:00', '2026-01-06 13:00:00'),
                  (9203, 'SR-003', 'pending_rfq', 'low', 'Buyer One', 'Compras', '2020-01-01', ?, '2026-01-20 08:00:00', '2026-01-20 09:00:00')
                """,
                (tid, tid, tid),
            )

            db.execute(
                """
                INSERT INTO purchase_request_items (
                    id, purchase_request_id, line_no, description, quantity, uom, tenant_id, created_at, updated_at
                ) VALUES
                  (9211, 9201, 1, 'Item SR1', 2, 'UN', ?, '2026-01-01 08:10:00', '2026-01-01 08:10:00'),
                  (9212, 9202, 1, 'Item SR2', 2, 'UN', ?, '2026-01-05 08:10:00', '2026-01-05 08:10:00'),
                  (9213, 9203, 1, 'Item SR3', 1, 'UN', ?, '2026-01-20 08:10:00', '2026-01-20 08:10:00')
                """,
                (tid, tid, tid),
            )

            db.execute(
                """
                INSERT INTO rfqs (id, title, status, tenant_id, created_at, updated_at)
                VALUES
                  (9301, 'Cotacao SR1', 'awarded', ?, '2026-01-01 12:00:00', '2026-01-02 10:00:00'),
                  (9302, 'Cotacao SR2', 'awarded', ?, '2026-01-05 12:00:00', '2026-01-06 10:00:00')
                """,
                (tid, tid),
            )

            db.execute(
                """
                INSERT INTO rfq_items (
                    id, rfq_id, purchase_request_item_id, description, quantity, uom, tenant_id, created_at, updated_at
                ) VALUES
                  (9311, 9301, 9211, 'Item RFQ1', 2, 'UN', ?, '2026-01-01 12:05:00', '2026-01-01 12:05:00'),
                  (9312, 9302, 9212, 'Item RFQ2', 2, 'UN', ?, '2026-01-05 12:05:00', '2026-01-05 12:05:00')
                """,
                (tid, tid),
            )

            db.execute(
                """
                INSERT INTO rfq_supplier_invites (
                    id, rfq_id, supplier_id, token, status, opened_at, submitted_at, tenant_id, created_at, updated_at
                ) VALUES
                  (9401, 9301, 9101, 'invite-9401', 'submitted', '2026-01-01 13:00:00', '2026-01-01 18:00:00', ?, '2026-01-01 12:10:00', '2026-01-01 18:00:00'),
                  (9402, 9301, 9102, 'invite-9402', 'submitted', '2026-01-01 14:00:00', '2026-01-02 04:00:00', ?, '2026-01-01 12:15:00', '2026-01-02 04:00:00'),
                  (9403, 9302, 9102, 'invite-9403', 'submitted', '2026-01-05 13:00:00', '2026-01-05 16:00:00', ?, '2026-01-05 12:10:00', '2026-01-05 16:00:00')
                """,
                (tid, tid, tid),
            )

            db.execute(
                """
                INSERT INTO quotes (id, rfq_id, supplier_id, status, currency, tenant_id, created_at, updated_at)
                VALUES
                  (9501, 9301, 9101, 'submitted', 'BRL', ?, '2026-01-01 18:05:00', '2026-01-01 18:05:00'),
                  (9502, 9301, 9102, 'submitted', 'BRL', ?, '2026-01-02 04:05:00', '2026-01-02 04:05:00'),
                  (9503, 9302, 9102, 'submitted', 'BRL', ?, '2026-01-05 16:05:00', '2026-01-05 16:05:00')
                """,
                (tid, tid, tid),
            )

            db.execute(
                """
                INSERT INTO quote_items (
                    id, quote_id, rfq_item_id, unit_price, lead_time_days, tenant_id, created_at, updated_at
                ) VALUES
                  (9601, 9501, 9311, 50, 5, ?, '2026-01-01 18:10:00', '2026-01-01 18:10:00'),
                  (9602, 9502, 9311, 60, 8, ?, '2026-01-02 04:10:00', '2026-01-02 04:10:00'),
                  (9603, 9503, 9312, 100, 6, ?, '2026-01-05 16:10:00', '2026-01-05 16:10:00')
                """,
                (tid, tid, tid),
            )

            db.execute(
                """
                INSERT INTO awards (
                    id, rfq_id, supplier_name, status, reason, purchase_order_id, tenant_id, created_at, updated_at
                ) VALUES
                  (9701, 9301, 'Fornecedor A', 'converted_to_po', 'decisao padrao', 9801, ?, '2026-01-02 09:00:00', '2026-01-02 09:00:00'),
                  (9702, 9302, 'Fornecedor B', 'converted_to_po', 'Excecao aprovada sem concorrencia', 9802, ?, '2026-01-06 09:00:00', '2026-01-06 09:00:00')
                """,
                (tid, tid),
            )

            db.execute(
                """
                INSERT INTO purchase_orders (
                    id, number, award_id, supplier_name, status, currency, total_amount, erp_last_error, external_id, tenant_id, created_at, updated_at
                ) VALUES
                  (9801, 'OC-001', 9701, 'Fornecedor A', 'erp_accepted', 'BRL', 100, NULL, 'ERP-001', ?, '2026-01-02 12:00:00', '2026-01-02 14:00:00'),
                  (9802, 'OC-002', 9702, 'Fornecedor B', 'erp_error', 'BRL', 200, 'ERP HTTP 422 rejected', NULL, ?, '2026-01-06 12:00:00', '2026-01-06 14:00:00')
                """,
                (tid, tid),
            )

            db.execute(
                """
                INSERT INTO status_events (
                    entity, entity_id, from_status, to_status, reason, occurred_at, tenant_id
                ) VALUES
                  ('purchase_order', 9801, 'approved', 'sent_to_erp', 'po_push_started', '2026-01-02 12:30:00', ?),
                  ('purchase_order', 9801, 'sent_to_erp', 'erp_accepted', 'po_push_succeeded', '2026-01-02 13:00:00', ?),
                  ('purchase_order', 9802, 'approved', 'sent_to_erp', 'po_push_started', '2026-01-06 12:30:00', ?),
                  ('purchase_order', 9802, 'sent_to_erp', 'erp_error', 'po_push_rejected', '2026-01-06 13:00:00', ?),
                  ('purchase_order', 9802, 'erp_error', 'sent_to_erp', 'po_push_retry_started', '2026-01-06 13:30:00', ?),
                  ('purchase_order', 9802, 'sent_to_erp', 'erp_error', 'po_push_failed', '2026-01-06 14:00:00', ?),
                  ('purchase_request', 9203, 'pending_rfq', 'cancelled', 'purchase_request_cancelled', '2026-01-20 11:00:00', ?),
                  ('rfq', 9302, 'collecting_quotes', 'awarded', 'rfq_awarded', '2026-01-06 09:00:00', ?)
                """,
                (tid, tid, tid, tid, tid, tid, tid, tid),
            )

            db.commit()

    def test_analytics_kpis_and_filters(self) -> None:
        self._set_role("admin", "Admin Ops")

        response = self.client.get(
            "/api/procurement/analytics/costs?start_date=2026-01-01&end_date=2026-01-31",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}
        kpis = {item["key"]: item for item in payload.get("kpis", [])}

        self.assertAlmostEqual(float(kpis["economy_abs"]["value"]), 10.0, places=2)
        self.assertGreater(float(kpis["economy_pct"]["value"]), 3.0)
        self.assertEqual(int(kpis["emergency_count"]["value"]), 1)

        filtered = self.client.get(
            "/api/procurement/analytics/overview?supplier=Fornecedor%20A&status=erp_accepted&purchase_type=emergencial",
            headers=self.headers,
        )
        self.assertEqual(filtered.status_code, 200)
        filtered_payload = filtered.get_json() or {}
        self.assertEqual(filtered_payload.get("meta", {}).get("records_count"), 1)
        rows = (filtered_payload.get("drilldown") or {}).get("rows", [])
        self.assertTrue(rows)
        self.assertEqual(rows[0].get("fornecedor"), "Fornecedor A")

        multi_select = self.client.get(
            "/api/procurement/analytics/overview?status=ordered,pending_rfq&purchase_type=regular,emergencial",
            headers=self.headers,
        )
        self.assertEqual(multi_select.status_code, 200)
        multi_payload = multi_select.get_json() or {}
        self.assertEqual(multi_payload.get("meta", {}).get("records_count"), 3)

        default_quality = self.client.get(
            "/api/procurement/analytics/quality_erp?start_date=2026-01-02&end_date=2026-01-02",
            headers=self.headers,
        )
        self.assertEqual(default_quality.status_code, 200)
        self.assertEqual((default_quality.get_json() or {}).get("meta", {}).get("records_count"), 0)

        toggle_quality = self.client.get(
            "/api/procurement/analytics/quality_erp?start_date=2026-01-02&end_date=2026-01-02&period_basis=po_updated_at",
            headers=self.headers,
        )
        self.assertEqual(toggle_quality.status_code, 200)
        self.assertEqual((toggle_quality.get_json() or {}).get("meta", {}).get("records_count"), 1)

    def test_analytics_efficiency_and_compliance(self) -> None:
        self._set_role("admin", "Admin Ops")

        efficiency = self.client.get("/api/procurement/analytics/efficiency", headers=self.headers)
        self.assertEqual(efficiency.status_code, 200)
        eff_payload = efficiency.get_json() or {}
        eff_kpis = {item["key"]: item for item in eff_payload.get("kpis", [])}

        self.assertAlmostEqual(float(eff_kpis["avg_sr_to_oc"]["value"]), 28.0, places=2)
        self.assertGreaterEqual(int(eff_kpis["late_processes"]["value"]), 1)
        self.assertTrue((eff_kpis["avg_stage_time"]["tooltip"] or "").strip())

        eff_charts = {item.get("key"): item for item in eff_payload.get("charts", [])}
        self.assertIn("stage_breakdown_bar", eff_charts)
        stage_breakdown = eff_charts["stage_breakdown_bar"]
        labels = [item.get("label") for item in stage_breakdown.get("items", [])]
        self.assertEqual(labels, ["SR", "Cotacao", "Decisao", "OC", "ERP"])

        compliance = self.client.get("/api/procurement/analytics/compliance", headers=self.headers)
        self.assertEqual(compliance.status_code, 200)
        comp_payload = compliance.get_json() or {}
        comp_kpis = {item["key"]: item for item in comp_payload.get("kpis", [])}

        self.assertGreaterEqual(int(comp_kpis["no_competition"]["value"]), 1)
        self.assertGreaterEqual(int(comp_kpis["approved_exceptions"]["value"]), 1)
        self.assertGreaterEqual(int(comp_kpis["critical_actions"]["value"]), 1)
        self.assertIn("ate 1 convite", (comp_kpis["no_competition"].get("tooltip") or ""))

    def test_analytics_access_control_by_role(self) -> None:
        self._set_role("buyer", "Buyer One")
        buyer_payload = self.client.get("/api/procurement/analytics/overview", headers=self.headers).get_json() or {}
        self.assertEqual(buyer_payload.get("meta", {}).get("records_count"), 2)

        buyer_filters = self.client.get("/api/procurement/analytics/filters", headers=self.headers).get_json() or {}
        buyer_names = [item.get("key") for item in buyer_filters.get("buyers", [])]
        self.assertIn("Buyer One", buyer_names)
        self.assertNotIn("Buyer Two", buyer_names)

        self._set_role("manager", "Manager Ops", team_members="Buyer Two")
        manager_payload = self.client.get("/api/procurement/analytics/overview", headers=self.headers).get_json() or {}
        self.assertEqual(manager_payload.get("meta", {}).get("records_count"), 1)
        manager_rows = (manager_payload.get("drilldown") or {}).get("rows", [])
        self.assertTrue(manager_rows)
        self.assertEqual(manager_rows[0].get("comprador"), "Buyer Two")

        self._set_role("admin", "Admin Ops")
        admin_payload = self.client.get("/api/procurement/analytics/overview", headers=self.headers).get_json() or {}
        self.assertEqual(admin_payload.get("meta", {}).get("records_count"), 3)

    def test_analytics_actionable_kpi_mapping_and_contextual_actions(self) -> None:
        self._set_role("admin", "Admin Ops")

        efficiency_payload = self.client.get("/api/procurement/analytics/efficiency", headers=self.headers).get_json() or {}
        eff_kpis = {item["key"]: item for item in efficiency_payload.get("kpis", [])}
        late = eff_kpis.get("late_processes") or {}
        self.assertTrue(late.get("actionable"))
        self.assertEqual(late.get("action_type"), "open_list")
        self.assertTrue((late.get("action_label") or "").strip())
        self.assertEqual((late.get("action_context") or {}).get("kpi_key"), "late_processes")

        suppliers_payload = self.client.get("/api/procurement/analytics/suppliers", headers=self.headers).get_json() or {}
        supplier_kpis = {item["key"]: item for item in suppliers_payload.get("kpis", [])}
        supplier_rate = supplier_kpis.get("supplier_response_rate") or {}
        self.assertFalse(bool(supplier_rate.get("actionable")))

        quality_payload = self.client.get("/api/procurement/analytics/quality_erp", headers=self.headers).get_json() or {}
        quality_kpis = {item["key"]: item for item in quality_payload.get("kpis", [])}
        erp_rejections = quality_kpis.get("erp_rejections") or {}
        self.assertTrue(erp_rejections.get("actionable"))
        self.assertEqual(erp_rejections.get("action_type"), "open_list")

        quality_rows = (quality_payload.get("drilldown") or {}).get("rows", [])
        self.assertTrue(quality_rows)
        quality_action = (quality_rows[0].get("_action") or {})
        self.assertEqual(quality_action.get("action_key"), "push_to_erp")
        self.assertEqual(quality_action.get("action_type"), "direct_action")
        self.assertTrue(bool(quality_action.get("requires_confirmation")))

        overview_payload = self.client.get("/api/procurement/analytics/overview", headers=self.headers).get_json() or {}
        overview_rows = (overview_payload.get("drilldown") or {}).get("rows", [])
        accepted_row = next((row for row in overview_rows if row.get("ordem") == "OC-001"), None)
        self.assertIsNotNone(accepted_row)
        accepted_action = (accepted_row or {}).get("_action") or {}
        self.assertNotEqual(accepted_action.get("action_key"), "push_to_erp")

    def test_blocked_action_returns_friendly_message(self) -> None:
        self._set_role("admin", "Admin Ops")
        with self.app.app_context():
            db = get_db()
            db.execute(
                "UPDATE purchase_orders SET status = 'sent_to_erp' WHERE id = ? AND tenant_id = ?",
                (9802, self.tenant_id),
            )
            db.commit()

        response = self.client.post(
            "/api/procurement/purchase-orders/9802/push-to-erp?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 409)
        payload = response.get_json() or {}
        self.assertEqual(payload.get("error"), "action_not_allowed_for_status")
        self.assertEqual(payload.get("message"), error_message("action_not_allowed_for_status"))

    def test_analytics_cache_keeps_same_payload_without_recompute(self) -> None:
        self._set_role("admin", "Admin Ops")
        procurement_routes._clear_analytics_cache_for_tests()

        with patch(
            "app.routes.procurement_routes.build_analytics_payload",
            wraps=procurement_routes.build_analytics_payload,
        ) as wrapped_builder:
            first = self.client.get(
                "/api/procurement/analytics/costs?start_date=2026-01-01&end_date=2026-01-31&status=ordered",
                headers=self.headers,
            )
            second = self.client.get(
                "/api/procurement/analytics/costs?start_date=2026-01-01&end_date=2026-01-31&status=ordered",
                headers=self.headers,
            )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(wrapped_builder.call_count, 1)
        self.assertEqual(first.get_json(), second.get_json())


if __name__ == "__main__":
    unittest.main()
