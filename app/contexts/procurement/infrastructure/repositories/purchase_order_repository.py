from __future__ import annotations

from app.infrastructure.repositories.base import BaseRepository


class PurchaseOrderRepository(BaseRepository):
    def get_by_id(self, db, purchase_order_id: int) -> dict | None:
        row = db.execute(
            """
            SELECT *
            FROM purchase_orders
            WHERE id = ? AND tenant_id = ?
            LIMIT 1
            """,
            (purchase_order_id, self.tenant_id),
        ).fetchone()
        return dict(row) if row else None

    def create(
        self,
        db,
        *,
        number: str | None,
        award_id: int | None,
        supplier_name: str | None,
        status: str,
        total_amount: float | None,
        currency: str = "BRL",
    ) -> int:
        cursor = db.execute(
            """
            INSERT INTO purchase_orders (number, award_id, supplier_name, status, total_amount, currency, tenant_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (number, award_id, supplier_name, status, total_amount, currency, self.tenant_id),
        )
        row = cursor.fetchone()
        return int(row["id"] if isinstance(row, dict) else row[0])

    def update_status(self, db, purchase_order_id: int, status: str) -> None:
        db.execute(
            """
            UPDATE purchase_orders
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (status, purchase_order_id, self.tenant_id),
        )

    def list_followup(self, db, *, limit: int = 120) -> list[dict]:
        rows = db.execute(
            """
            SELECT id, number, status, supplier_name, external_id, updated_at, tenant_id
            FROM purchase_orders
            WHERE tenant_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (self.tenant_id, int(limit)),
        ).fetchall()
        return self.rows_to_dicts(rows)

