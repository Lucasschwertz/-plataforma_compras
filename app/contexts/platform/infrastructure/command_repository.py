from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from uuid import UUID

from app.contexts.platform.domain.command import Command
from app.infrastructure.repositories.base import BaseRepository


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_db_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    resolved = value
    if resolved.tzinfo is None:
        resolved = resolved.replace(tzinfo=timezone.utc)
    else:
        resolved = resolved.astimezone(timezone.utc)
    # Persist as UTC without offset to stay compatible with timestamp columns.
    return resolved.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")


def _to_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    raw = str(value).strip()
    if not raw:
        return None
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_payload(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, Mapping):
        return dict(value.items())
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return {}
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(loaded, dict):
            return dict(loaded)
        return {}
    return {}


def _row_to_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    keys = getattr(row, "keys", None)
    if callable(keys):
        return {key: row[key] for key in row.keys()}
    return {}


class CommandRepository(BaseRepository):
    def __init__(self, *, tenant_id: UUID | str, db) -> None:
        super().__init__(tenant_id=str(tenant_id))
        self._db = db

    def _sql(self, statement: str) -> str:
        backend = str(getattr(self._db, "backend", "") or "").lower()
        if backend.startswith("postgres"):
            return statement
        # Keep compatibility with sqlite test databases.
        return statement.replace("%s", "?")

    def create(self, command: Command) -> None:
        if str(command.tenant_id) != self.tenant_id:
            raise ValueError("command tenant_id does not match repository tenant scope")

        payload_json = json.dumps(command.payload or {}, ensure_ascii=True, separators=(",", ":"))
        self._db.execute(
            self._sql(
                """
            INSERT INTO commands (
                id,
                tenant_id,
                command_type,
                entity_type,
                entity_id,
                payload,
                status,
                requested_by,
                requested_at,
                executed_at,
                failed_at,
                error_message,
                idempotency_key,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            ),
            (
                str(command.id),
                str(command.tenant_id),
                command.command_type,
                command.entity_type,
                command.entity_id,
                payload_json,
                command.status or "pending",
                command.requested_by,
                _to_db_datetime(command.requested_at),
                _to_db_datetime(command.executed_at),
                _to_db_datetime(command.failed_at),
                command.error_message,
                command.idempotency_key,
                _to_db_datetime(command.created_at),
                _to_db_datetime(command.updated_at),
            ),
        )

    def get_by_id(self, command_id: UUID) -> Optional[Command]:
        row = self._db.execute(
            self._sql(
                """
            SELECT *
            FROM commands
            WHERE id = %s AND tenant_id = %s
            LIMIT 1
            """
            ),
            (str(command_id), self.tenant_id),
        ).fetchone()
        return self._to_entity(row) if row else None

    def get_by_idempotency_key(
        self,
        tenant_id: UUID,
        idempotency_key: str,
    ) -> Optional[Command]:
        scoped_tenant = str(tenant_id)
        if scoped_tenant != self.tenant_id:
            return None
        row = self._db.execute(
            self._sql(
                """
            SELECT *
            FROM commands
            WHERE tenant_id = %s AND idempotency_key = %s
            LIMIT 1
            """
            ),
            (scoped_tenant, idempotency_key),
        ).fetchone()
        return self._to_entity(row) if row else None

    def mark_running(self, command_id: UUID) -> None:
        now = _to_db_datetime(_utc_now())
        result = self._db.execute(
            self._sql(
                """
            UPDATE commands
            SET status = 'running',
                updated_at = %s
            WHERE id = %s AND tenant_id = %s
            """
            ),
            (now, str(command_id), self.tenant_id),
        )
        if int(getattr(result, "rowcount", 0) or 0) == 0:
            raise ValueError("command not found or tenant mismatch")

    def mark_success(self, command_id: UUID) -> None:
        now = _to_db_datetime(_utc_now())
        result = self._db.execute(
            self._sql(
                """
            UPDATE commands
            SET status = 'success',
                executed_at = %s,
                updated_at = %s
            WHERE id = %s AND tenant_id = %s
            """
            ),
            (now, now, str(command_id), self.tenant_id),
        )
        if int(getattr(result, "rowcount", 0) or 0) == 0:
            raise ValueError("command not found or tenant mismatch")

    def mark_failed(self, command_id: UUID, error_message: str) -> None:
        now = _to_db_datetime(_utc_now())
        result = self._db.execute(
            self._sql(
                """
            UPDATE commands
            SET status = 'failed',
                failed_at = %s,
                error_message = %s,
                updated_at = %s
            WHERE id = %s AND tenant_id = %s
            """
            ),
            (now, error_message, now, str(command_id), self.tenant_id),
        )
        if int(getattr(result, "rowcount", 0) or 0) == 0:
            raise ValueError("command not found or tenant mismatch")

    def _to_entity(self, row: Any) -> Command:
        data = _row_to_dict(row)
        created_at = _to_datetime(data.get("created_at")) or _utc_now()
        updated_at = _to_datetime(data.get("updated_at")) or created_at
        requested_at = _to_datetime(data.get("requested_at")) or created_at
        return Command(
            id=_to_uuid(data.get("id")),
            tenant_id=_to_uuid(data.get("tenant_id")),
            command_type=str(data.get("command_type") or ""),
            entity_type=str(data.get("entity_type") or ""),
            entity_id=str(data.get("entity_id") or ""),
            payload=_to_payload(data.get("payload")),
            status=str(data.get("status") or "pending"),
            requested_by=str(data.get("requested_by") or ""),
            requested_at=requested_at,
            executed_at=_to_datetime(data.get("executed_at")),
            failed_at=_to_datetime(data.get("failed_at")),
            error_message=data.get("error_message"),
            idempotency_key=data.get("idempotency_key"),
            created_at=created_at,
            updated_at=updated_at,
        )
