from __future__ import annotations

from typing import Any

from app.infrastructure.repositories.base import BaseRepository


class PurchaseRequestRepository(BaseRepository):
    def create(
        self,
        db,
        *,
        number: str | None,
        status: str,
        priority: str,
        requested_by: str | None,
        department: str | None,
        needed_at: str | None,
    ) -> int:
        cursor = db.execute(
            """
            INSERT INTO purchase_requests (number, status, priority, requested_by, department, needed_at, tenant_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (number, status, priority, requested_by, department, needed_at, self.tenant_id),
        )
        row = cursor.fetchone()
        return int(row["id"] if isinstance(row, dict) else row[0])

    def get_by_id(self, db, purchase_request_id: int) -> dict | None:
        row = db.execute(
            """
            SELECT *
            FROM purchase_requests
            WHERE id = ? AND tenant_id = ?
            LIMIT 1
            """,
            (purchase_request_id, self.tenant_id),
        ).fetchone()
        return dict(row) if row else None

    def delete_by_id(self, db, purchase_request_id: int) -> None:
        db.execute(
            "DELETE FROM purchase_requests WHERE id = ? AND tenant_id = ?",
            (purchase_request_id, self.tenant_id),
        )

    def update_fields(self, db, purchase_request_id: int, fields: dict[str, Any]) -> None:
        if not fields:
            return
        updates = [f"{key} = ?" for key in fields.keys()]
        params = list(fields.values())
        params.extend([purchase_request_id, self.tenant_id])
        db.execute(
            f"""
            UPDATE purchase_requests
            SET {", ".join(updates)}, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            tuple(params),
        )

    def list_summary(self, db, *, limit: int = 200) -> list[dict]:
        rows = db.execute(
            """
            SELECT id, number, status, priority, requested_by, department, needed_at, tenant_id
            FROM purchase_requests
            WHERE tenant_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (self.tenant_id, int(limit)),
        ).fetchall()
        return self.rows_to_dicts(rows)

    def count(self, db) -> int:
        row = db.execute(
            "SELECT COUNT(*) AS total FROM purchase_requests WHERE tenant_id = ?",
            (self.tenant_id,),
        ).fetchone()
        return int((row or {}).get("total") if isinstance(row, dict) else (row[0] if row else 0))

