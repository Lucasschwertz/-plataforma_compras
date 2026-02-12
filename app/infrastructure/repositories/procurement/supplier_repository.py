from __future__ import annotations

from app.infrastructure.repositories.base import BaseRepository


class SupplierRepository(BaseRepository):
    def get_by_id(self, db, supplier_id: int) -> dict | None:
        row = db.execute(
            """
            SELECT *
            FROM suppliers
            WHERE id = ? AND tenant_id = ?
            LIMIT 1
            """,
            (supplier_id, self.tenant_id),
        ).fetchone()
        return dict(row) if row else None

    def list_all(self, db) -> list[dict]:
        rows = db.execute(
            """
            SELECT id, name, email, external_id, tenant_id
            FROM suppliers
            WHERE tenant_id = ?
            ORDER BY name ASC, id ASC
            """,
            (self.tenant_id,),
        ).fetchall()
        return self.rows_to_dicts(rows)

