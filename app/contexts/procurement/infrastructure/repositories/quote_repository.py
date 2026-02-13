from __future__ import annotations

from app.infrastructure.repositories.base import BaseRepository


class QuoteRepository(BaseRepository):
    def get_by_id(self, db, quote_id: int) -> dict | None:
        row = db.execute(
            """
            SELECT *
            FROM quotes
            WHERE id = ? AND tenant_id = ?
            LIMIT 1
            """,
            (quote_id, self.tenant_id),
        ).fetchone()
        return dict(row) if row else None

    def list_by_rfq(self, db, rfq_id: int) -> list[dict]:
        rows = db.execute(
            """
            SELECT *
            FROM quotes
            WHERE rfq_id = ? AND tenant_id = ?
            ORDER BY id DESC
            """,
            (rfq_id, self.tenant_id),
        ).fetchall()
        return self.rows_to_dicts(rows)

    def delete_quote_and_items(self, db, quote_id: int) -> None:
        db.execute("DELETE FROM quote_items WHERE quote_id = ? AND tenant_id = ?", (quote_id, self.tenant_id))
        db.execute("DELETE FROM quotes WHERE id = ? AND tenant_id = ?", (quote_id, self.tenant_id))

