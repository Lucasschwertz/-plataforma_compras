import unittest

from app import create_app
from app.config import Config
from app.db import close_db, get_db
from app.infrastructure.repositories import TenantScopeRequiredError
from app.infrastructure.repositories.procurement import PurchaseRequestRepository
from tests.helpers.temp_db import TempDbSandbox


class ProcurementRepositoryTenantScopeTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="repo_scope")
        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=True,
            AUTH_ENABLED=False,
        )
        self.app = create_app(TempConfig)

    def tearDown(self) -> None:
        with self.app.app_context():
            close_db()
        self._temp_db.cleanup()

    def test_repository_requires_tenant_scope(self) -> None:
        with self.assertRaises(TenantScopeRequiredError):
            PurchaseRequestRepository()

    def test_purchase_request_repository_isolates_tenant_data(self) -> None:
        with self.app.app_context():
            db = get_db()
            repo_a = PurchaseRequestRepository(tenant_id="tenant-a")
            repo_b = PurchaseRequestRepository(tenant_id="tenant-b")

            a_id = repo_a.create(
                db,
                number="PR-A-001",
                status="pending_rfq",
                priority="medium",
                requested_by="Comprador A",
                department="Compras",
                needed_at=None,
            )
            b_id = repo_b.create(
                db,
                number="PR-B-001",
                status="pending_rfq",
                priority="medium",
                requested_by="Comprador B",
                department="Compras",
                needed_at=None,
            )
            db.commit()

            tenant_a_rows = repo_a.list_summary(db, limit=20)
            tenant_b_rows = repo_b.list_summary(db, limit=20)

            self.assertTrue(any(int(row["id"]) == a_id for row in tenant_a_rows))
            self.assertFalse(any(int(row["id"]) == b_id for row in tenant_a_rows))
            self.assertTrue(any(int(row["id"]) == b_id for row in tenant_b_rows))
            self.assertFalse(any(int(row["id"]) == a_id for row in tenant_b_rows))

            self.assertIsNone(repo_a.get_by_id(db, b_id))
            self.assertIsNone(repo_b.get_by_id(db, a_id))


if __name__ == "__main__":
    unittest.main()

