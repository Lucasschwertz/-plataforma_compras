from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID, uuid4


@dataclass
class Command:
    id: UUID
    tenant_id: UUID
    command_type: str
    entity_type: str
    entity_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    requested_by: str = ""
    requested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    executed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    idempotency_key: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @staticmethod
    def create(
        tenant_id: UUID,
        command_type: str,
        entity_type: str,
        entity_id: str,
        payload: dict[str, Any],
        requested_by: str,
        idempotency_key: Optional[str] = None,
    ) -> "Command":
        now = datetime.now(timezone.utc)
        return Command(
            id=uuid4(),
            tenant_id=tenant_id,
            command_type=command_type,
            entity_type=entity_type,
            entity_id=entity_id,
            payload=dict(payload or {}),
            status="pending",
            requested_by=requested_by,
            requested_at=now,
            executed_at=None,
            failed_at=None,
            error_message=None,
            idempotency_key=idempotency_key,
            created_at=now,
            updated_at=now,
        )

