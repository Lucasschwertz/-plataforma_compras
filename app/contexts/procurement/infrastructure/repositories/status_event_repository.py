from __future__ import annotations

from app.infrastructure.repositories.base import BaseRepository


class StatusEventRepository(BaseRepository):
    def add_event(
        self,
        db,
        *,
        entity: str,
        entity_id: int,
        from_status: str | None,
        to_status: str | None,
        reason: str | None,
    ) -> int:
        cursor = db.execute(
            """
            INSERT INTO status_events (entity, entity_id, from_status, to_status, reason, tenant_id)
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (entity, entity_id, from_status, to_status, reason, self.tenant_id),
        )
        row = cursor.fetchone()
        return int(row["id"] if isinstance(row, dict) else row[0])

    def list_for_entity(self, db, *, entity: str, entity_id: int, limit: int = 120) -> list[dict]:
        rows = db.execute(
            """
            SELECT id, entity, entity_id, from_status, to_status, reason, occurred_at, tenant_id
            FROM status_events
            WHERE entity = ? AND entity_id = ? AND tenant_id = ?
            ORDER BY occurred_at DESC, id DESC
            LIMIT ?
            """,
            (entity, entity_id, self.tenant_id, int(limit)),
        ).fetchall()
        return self.rows_to_dicts(rows)

