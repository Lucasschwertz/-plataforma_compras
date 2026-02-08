import os
import tempfile
import unittest

from app import create_app
from app.config import Config
from app.db import close_db, get_db


def _schema_line(table: str, field_name: str, order: int) -> str:
    row = [""] * 13
    row[1] = table
    row[3] = field_name
    row[12] = str(order)
    return ";".join(row)


class ErpCsvSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(
            prefix="pc_test_",
            dir=os.getcwd(),
            ignore_cleanup_errors=True,
        )
        self._create_csv_fixture_files(self._tmpdir.name)
        db_path = os.path.join(self._tmpdir.name, "plataforma_compras_test.db")

        class TempConfig(Config):
            DATABASE_DIR = self._tmpdir.name
            DB_PATH = db_path
            TESTING = True
            ERP_MODE = "senior_csv"
            ERP_CSV_SCHEMA = os.path.join(self._tmpdir.name, "tabelas.csv")
            ERP_CSV_E405SOL = os.path.join(self._tmpdir.name, "e405sol.csv")
            ERP_CSV_E410COT = os.path.join(self._tmpdir.name, "e410cot.csv")
            ERP_CSV_E410PCT = os.path.join(self._tmpdir.name, "e410pct.csv")
            ERP_CSV_E410FPC = os.path.join(self._tmpdir.name, "e410fpc.csv")
            ERP_CSV_E420OCP = os.path.join(self._tmpdir.name, "e420ocp.csv")
            ERP_CSV_E420IPO = os.path.join(self._tmpdir.name, "e420ipo.csv")
            ERP_CSV_E440NFC = os.path.join(self._tmpdir.name, "e440nfc.csv")
            ERP_CSV_E440IPC = os.path.join(self._tmpdir.name, "e440ipc.csv")

        self.app = create_app(TempConfig)
        self.client = self.app.test_client()
        self.tenant_id = "tenant-csv"
        self.headers = {"X-Tenant-Id": self.tenant_id}

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._tmpdir.cleanup()

    def _create_csv_fixture_files(self, base_dir: str) -> None:
        schema_lines = [
            _schema_line("E405SOL", "NumSol", 1),
            _schema_line("E405SOL", "DatEfc", 2),
            _schema_line("E405SOL", "NumCot", 3),
            _schema_line("E405SOL", "NumPct", 4),
            _schema_line("E405SOL", "CodDep", 5),
            _schema_line("E410COT", "NumCot", 1),
            _schema_line("E410COT", "NumPct", 2),
            _schema_line("E410COT", "CodFor", 3),
            _schema_line("E410COT", "DatCot", 4),
            _schema_line("E410COT", "HorCot", 5),
            _schema_line("E410PCT", "NumPct", 1),
            _schema_line("E410PCT", "DatAbe", 2),
            _schema_line("E410FPC", "NumPct", 1),
            _schema_line("E410FPC", "CodFor", 2),
            _schema_line("E420OCP", "NumOcp", 1),
            _schema_line("E420OCP", "CodFor", 2),
            _schema_line("E420OCP", "CodMoe", 3),
            _schema_line("E420OCP", "VlrOcp", 4),
            _schema_line("E420OCP", "SitOcp", 5),
            _schema_line("E420OCP", "DatEmi", 6),
            _schema_line("E420IPO", "NumOcp", 1),
            _schema_line("E420IPO", "SeqIpo", 2),
            _schema_line("E420IPO", "CodPro", 3),
            _schema_line("E420IPO", "DesPro", 4),
            _schema_line("E420IPO", "QtdPed", 5),
            _schema_line("E420IPO", "PreUni", 6),
            _schema_line("E420IPO", "VlrTot", 7),
            _schema_line("E440NFC", "NumNfc", 1),
            _schema_line("E440NFC", "NumOcp", 2),
            _schema_line("E440NFC", "DatRec", 3),
            _schema_line("E440NFC", "SitNfc", 4),
            _schema_line("E440IPC", "NumNfc", 1),
            _schema_line("E440IPC", "NumOcp", 2),
            _schema_line("E440IPC", "SeqIpc", 3),
            _schema_line("E440IPC", "CodPro", 4),
            _schema_line("E440IPC", "QtdRec", 5),
        ]
        with open(os.path.join(base_dir, "tabelas.csv"), "w", encoding="utf-8") as handle:
            handle.write("\n".join(schema_lines))

        with open(os.path.join(base_dir, "e405sol.csv"), "w", encoding="utf-8") as handle:
            handle.write("NumSol;DatEfc;NumCot;NumPct;CodDep\n")
            handle.write("1001;2026-01-01;5001;7001;D1\n")
            handle.write("1002;2026-01-02;5002;7002;D2\n")

        with open(os.path.join(base_dir, "e410cot.csv"), "w", encoding="utf-8") as handle:
            handle.write("NumCot;NumPct;CodFor;DatCot;HorCot\n")
            handle.write("5001;7001;900;2026-01-03;0815\n")
            handle.write("5002;7002;901;2026-01-04;0910\n")

        with open(os.path.join(base_dir, "e410pct.csv"), "w", encoding="utf-8") as handle:
            handle.write("NumPct;DatAbe\n")
            handle.write("7001;2026-01-01\n")
            handle.write("7002;2026-01-02\n")

        with open(os.path.join(base_dir, "e410fpc.csv"), "w", encoding="utf-8") as handle:
            handle.write("NumPct;CodFor\n")
            handle.write("7001;900\n")
            handle.write("7002;901\n")

        with open(os.path.join(base_dir, "e420ocp.csv"), "w", encoding="utf-8") as handle:
            handle.write("NumOcp;CodFor;CodMoe;VlrOcp;SitOcp;DatEmi\n")
            handle.write("6001;900;BRL;1500,50;approved;2026-01-05\n")
            handle.write("6002;901;BRL;980.00;approved;2026-01-06\n")

        with open(os.path.join(base_dir, "e420ipo.csv"), "w", encoding="utf-8") as handle:
            handle.write("NumOcp;SeqIpo;CodPro;DesPro;QtdPed;PreUni;VlrTot\n")
            handle.write("6001;1;ROL-01;Rolamento 6202;10;9,00;90,00\n")
            handle.write("6001;2;COR-01;Correia dentada;4;12,00;48,00\n")
            handle.write("6002;1;ROL-02;Rolamento 6203;5;20,00;100,00\n")

        with open(os.path.join(base_dir, "e440nfc.csv"), "w", encoding="utf-8") as handle:
            handle.write("NumNfc;NumOcp;DatRec;SitNfc\n")
            handle.write("8001;6001;2026-01-07;partially\n")
            handle.write("8002;6002;2026-01-08;received\n")

        with open(os.path.join(base_dir, "e440ipc.csv"), "w", encoding="utf-8") as handle:
            handle.write("NumNfc;NumOcp;SeqIpc;CodPro;QtdRec\n")
            handle.write("8001;6001;1;ROL-01;6\n")
            handle.write("8001;6001;2;COR-01;2\n")
            handle.write("8002;6002;1;ROL-02;5\n")

    def test_purchase_request_sync_is_incremental(self) -> None:
        first = self.client.post(
            "/api/procurement/integrations/sync?scope=purchase_request",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.get_json()["result"]["records_in"], 2)

        second = self.client.post(
            "/api/procurement/integrations/sync?scope=purchase_request",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.get_json()["result"]["records_in"], 0)

        with self.app.app_context():
            db = get_db()
            total = db.execute(
                "SELECT COUNT(*) AS total FROM purchase_requests WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(total, 2)
            item_total = db.execute(
                "SELECT COUNT(*) AS total FROM purchase_request_items WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertGreaterEqual(item_total, 2)

    def test_supplier_and_quote_scopes_sync_from_csv(self) -> None:
        supplier_sync = self.client.post(
            "/api/procurement/integrations/sync?scope=supplier",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(supplier_sync.status_code, 200)
        self.assertEqual(supplier_sync.get_json()["result"]["records_in"], 2)

        quote_sync = self.client.post(
            "/api/procurement/integrations/sync?scope=quote",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(quote_sync.status_code, 200)
        self.assertEqual(quote_sync.get_json()["result"]["records_in"], 2)

        process_sync = self.client.post(
            "/api/procurement/integrations/sync?scope=quote_process",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(process_sync.status_code, 200)
        self.assertEqual(process_sync.get_json()["result"]["records_in"], 2)

        supplier_process_sync = self.client.post(
            "/api/procurement/integrations/sync?scope=quote_supplier",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(supplier_process_sync.status_code, 200)
        self.assertEqual(supplier_process_sync.get_json()["result"]["records_in"], 2)

        supplier_sync_incremental = self.client.post(
            "/api/procurement/integrations/sync?scope=supplier",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(supplier_sync_incremental.status_code, 200)
        self.assertEqual(supplier_sync_incremental.get_json()["result"]["records_in"], 0)

        with self.app.app_context():
            db = get_db()
            suppliers_total = db.execute(
                "SELECT COUNT(*) AS total FROM suppliers WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(suppliers_total, 2)

            quotes_total = db.execute(
                "SELECT COUNT(*) AS total FROM erp_supplier_quotes WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(quotes_total, 2)

    def test_purchase_order_and_receipt_scopes_sync_from_csv(self) -> None:
        po_sync = self.client.post(
            "/api/procurement/integrations/sync?scope=purchase_order",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(po_sync.status_code, 200)
        self.assertEqual(po_sync.get_json()["result"]["records_in"], 2)

        po_sync_incremental = self.client.post(
            "/api/procurement/integrations/sync?scope=purchase_order",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(po_sync_incremental.status_code, 200)
        self.assertEqual(po_sync_incremental.get_json()["result"]["records_in"], 0)

        receipt_sync = self.client.post(
            "/api/procurement/integrations/sync?scope=receipt",
            headers=self.headers,
            json={"limit": 100},
        )
        self.assertEqual(receipt_sync.status_code, 200)
        self.assertEqual(receipt_sync.get_json()["result"]["records_in"], 2)

        with self.app.app_context():
            db = get_db()
            purchase_orders_total = db.execute(
                "SELECT COUNT(*) AS total FROM purchase_orders WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(purchase_orders_total, 2)

            receipts_total = db.execute(
                "SELECT COUNT(*) AS total FROM receipts WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(receipts_total, 2)

            po_items_total = db.execute(
                "SELECT COUNT(*) AS total FROM erp_purchase_order_items WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(po_items_total, 3)

            receipt_items_total = db.execute(
                "SELECT COUNT(*) AS total FROM erp_receipt_items WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchone()["total"]
            self.assertEqual(receipt_items_total, 3)

            po_row = db.execute(
                """
                SELECT status
                FROM purchase_orders
                WHERE tenant_id = ? AND external_id = ?
                """,
                (self.tenant_id, "6002"),
            ).fetchone()
            self.assertIsNotNone(po_row)
            self.assertEqual(po_row["status"], "received")

    def test_purchase_order_crud_for_local_orders(self) -> None:
        create = self.client.post(
            "/api/procurement/purchase-orders",
            headers=self.headers,
            json={
                "number": "OC-LOCAL-1",
                "supplier_name": "Fornecedor Local",
                "status": "draft",
                "currency": "BRL",
                "total_amount": 123.45,
            },
        )
        self.assertEqual(create.status_code, 201)
        created_id = create.get_json()["id"]

        update = self.client.patch(
            f"/api/procurement/purchase-orders/{created_id}",
            headers=self.headers,
            json={"status": "approved", "total_amount": 150.0},
        )
        self.assertEqual(update.status_code, 200)
        self.assertEqual(update.get_json()["status"], "approved")

        read = self.client.get(
            f"/api/procurement/purchase-orders/{created_id}",
            headers=self.headers,
        )
        self.assertEqual(read.status_code, 200)
        self.assertEqual(read.get_json()["purchase_order"]["status"], "approved")

        delete = self.client.delete(
            f"/api/procurement/purchase-orders/{created_id}?confirm=true",
            headers=self.headers,
        )
        self.assertEqual(delete.status_code, 200)
        self.assertEqual(delete.get_json()["status"], "cancelled")


if __name__ == "__main__":
    unittest.main()
