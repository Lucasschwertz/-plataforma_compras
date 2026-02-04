import os
import sys
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from app import create_app
from app.db import get_db, init_db
from database.import_erp_csv import import_e405sol, import_e410cot, import_e410fpc, import_e410pct


app = create_app()


if __name__ == "__main__":
    with app.app_context():
        init_db()
        if os.environ.get("ERP_CSV_IMPORT", "0").strip() in {"1", "true", "yes", "sim"}:
            schema_path = os.environ.get("ERP_CSV_SCHEMA", "tabelas.csv")
            e405_path = os.environ.get("ERP_CSV_E405SOL")
            e410_path = os.environ.get("ERP_CSV_E410COT")
            tenant_id = os.environ.get("ERP_CSV_TENANT", "tenant-demo")
            e410pct_path = os.environ.get("ERP_CSV_E410PCT")
            e410fpc_path = os.environ.get("ERP_CSV_E410FPC")

            if not e405_path or not e410_path:
                raise RuntimeError("ERP_CSV_E405SOL e ERP_CSV_E410COT sao obrigatorios quando ERP_CSV_IMPORT=1.")

            db = get_db()
            import_e405sol(db, tenant_id, Path(schema_path), Path(e405_path))
            import_e410cot(db, tenant_id, Path(schema_path), Path(e410_path))
            if e410pct_path:
                import_e410pct(db, tenant_id, Path(schema_path), Path(e410pct_path))
            if e410fpc_path:
                import_e410fpc(db, tenant_id, Path(schema_path), Path(e410fpc_path))
            db.commit()
    print("Database initialized.")
