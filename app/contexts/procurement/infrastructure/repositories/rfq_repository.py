from __future__ import annotations

from app.infrastructure.repositories.base import BaseRepository


class RfqRepository(BaseRepository):
    def create(self, db, *, title: str | None, status: str = "draft") -> int:
        cursor = db.execute(
            """
            INSERT INTO rfqs (title, status, tenant_id)
            VALUES (?, ?, ?)
            RETURNING id
            """,
            (title, status, self.tenant_id),
        )
        row = cursor.fetchone()
        return int(row["id"] if isinstance(row, dict) else row[0])

    def get_by_id(self, db, rfq_id: int) -> dict | None:
        row = db.execute(
            """
            SELECT *
            FROM rfqs
            WHERE id = ? AND tenant_id = ?
            LIMIT 1
            """,
            (rfq_id, self.tenant_id),
        ).fetchone()
        return dict(row) if row else None

    def update_status(self, db, rfq_id: int, status: str) -> None:
        db.execute(
            """
            UPDATE rfqs
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND tenant_id = ?
            """,
            (status, rfq_id, self.tenant_id),
        )

    def list_summary(self, db, *, limit: int = 120) -> list[dict]:
        rows = db.execute(
            """
            SELECT id, title, status, tenant_id, created_at, updated_at
            FROM rfqs
            WHERE tenant_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (self.tenant_id, int(limit)),
        ).fetchall()
        return self.rows_to_dicts(rows)

