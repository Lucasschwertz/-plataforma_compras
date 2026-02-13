from __future__ import annotations

from typing import Any

from app.infrastructure.repositories.base import BaseRepository


class PurchaseRequestItemRepository(BaseRepository):
    def next_line_no(self, db, purchase_request_id: int) -> int:
        row = db.execute(
            """
            SELECT COALESCE(MAX(line_no), 0) + 1 AS next_line
            FROM purchase_request_items
            WHERE purchase_request_id = ? AND tenant_id = ?
            """,
            (purchase_request_id, self.tenant_id),
        ).fetchone()
        if not row:
            return 1
        return int(row["next_line"] if isinstance(row, dict) else row[0])

    def create(
        self,
        db,
        *,
        purchase_request_id: int,
        line_no: int,
        description: str,
        quantity: float,
        uom: str,
        category: str | None,
    ) -> int:
        cursor = db.execute(
            """
            INSERT INTO purchase_request_items (
                purchase_request_id, line_no, description, quantity, uom, category, tenant_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (purchase_request_id, line_no, description, quantity, uom, category, self.tenant_id),
        )
        row = cursor.fetchone()
        return int(row["id"] if isinstance(row, dict) else row[0])

    def get_by_id(self, db, item_id: int) -> dict | None:
        row = db.execute(
            """
            SELECT *
            FROM purchase_request_items
            WHERE id = ? AND tenant_id = ?
            LIMIT 1
            """,
            (item_id, self.tenant_id),
        ).fetchone()
        return dict(row) if row else None

    def update_fields(self, db, item_id: int, fields: dict[str, Any]) -> None:
        if not fields:
            return
        updates = [f"{key} = ?" for key in fields.keys()]
        params = list(fields.values())
        params.extend([item_id, self.tenant_id])
        db.execute(
            f"""
            UPDATE purchase_request_items
            SET {", ".join(updates)}, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            tuple(params),
        )

    def delete_by_id(self, db, item_id: int) -> None:
        db.execute(
            "DELETE FROM purchase_request_items WHERE id = ? AND tenant_id = ?",
            (item_id, self.tenant_id),
        )

