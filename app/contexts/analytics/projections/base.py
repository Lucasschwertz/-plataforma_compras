from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Iterable, Type

from app.core import DomainEvent


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_workspace_id(workspace_id: str | None) -> str:
    normalized = str(workspace_id or "").strip()
    if not normalized:
        raise ValueError("workspace_id is required for analytics projections")
    return normalized


def _normalize_projector_name(projector: str | None) -> str:
    normalized = str(projector or "").strip()
    if not normalized:
        raise ValueError("projector name is required for analytics projections")
    return normalized


def _normalize_event_id(event_id: str | None) -> str:
    normalized = str(event_id or "").strip()
    if not normalized:
        raise ValueError("event_id is required for idempotent projection handling")
    return normalized


def _timestamp_value(value: datetime | None) -> str:
    resolved = value or _utc_now()
    if resolved.tzinfo is None:
        resolved = resolved.replace(tzinfo=timezone.utc)
    return resolved.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class Projector(ABC):
    name: str = "projector"
    handled_events: tuple[Type[DomainEvent], ...] = ()

    @abstractmethod
    def handle(self, event: DomainEvent, db, workspace_id: str) -> None:
        raise NotImplementedError


def ensure_idempotent(
    db,
    *,
    workspace_id: str,
    projector: str,
    event_id: str,
) -> bool:
    normalized_workspace_id = _normalize_workspace_id(workspace_id)
    normalized_projector = _normalize_projector_name(projector)
    normalized_event_id = _normalize_event_id(event_id)

    cursor = db.execute(
        """
        INSERT INTO ar_event_dedupe (workspace_id, event_id, projector, processed_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(workspace_id, projector, event_id) DO NOTHING
        """,
        (normalized_workspace_id, normalized_event_id, normalized_projector),
    )
    return int(getattr(cursor, "rowcount", 0) or 0) > 0


def update_state(
    db,
    *,
    workspace_id: str,
    projector: str,
    status: str,
    last_event_id: str | None = None,
    last_error: str | None = None,
    last_processed_at: datetime | None = None,
) -> None:
    normalized_workspace_id = _normalize_workspace_id(workspace_id)
    normalized_projector = _normalize_projector_name(projector)
    normalized_status = str(status or "").strip() or "running"

    db.execute(
        """
        INSERT INTO ar_projection_state (
            workspace_id, projector, last_event_id, last_processed_at, status, last_error, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(workspace_id, projector) DO UPDATE SET
            last_event_id = excluded.last_event_id,
            last_processed_at = excluded.last_processed_at,
            status = excluded.status,
            last_error = excluded.last_error,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            normalized_workspace_id,
            normalized_projector,
            str(last_event_id or "").strip() or None,
            _timestamp_value(last_processed_at),
            normalized_status,
            (str(last_error or "")[:2000] if last_error else None),
        ),
    )


def matches_event_type(projector: Projector, event: DomainEvent) -> bool:
    handled_types: Iterable[type] = projector.handled_events or ()
    return any(isinstance(event, event_type) for event_type in handled_types)
