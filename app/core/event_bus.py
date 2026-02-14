from __future__ import annotations

import logging
import json
import uuid
from dataclasses import asdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Callable, Dict, List, Type

from app.observability import (
    observe_analytics_event_store_failed,
    observe_analytics_event_store_persisted,
    observe_domain_event_emitted,
)


EventHandler = Callable[["DomainEvent"], None]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, kw_only=True)
class DomainEvent:
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    occurred_at: datetime = field(default_factory=_utc_now)
    workspace_id: str = ""

    def __post_init__(self) -> None:
        normalized_event_id = str(self.event_id or "").strip() or uuid.uuid4().hex
        normalized_occurred_at = self.occurred_at if isinstance(self.occurred_at, datetime) else _utc_now()
        if normalized_occurred_at.tzinfo is None:
            normalized_occurred_at = normalized_occurred_at.replace(tzinfo=timezone.utc)
        normalized_occurred_at = normalized_occurred_at.astimezone(timezone.utc)

        normalized_workspace = str(self.workspace_id or "").strip()
        if not normalized_workspace:
            normalized_workspace = str(getattr(self, "tenant_id", "") or "").strip()
        if not normalized_workspace:
            normalized_workspace = "unknown"

        object.__setattr__(self, "event_id", normalized_event_id)
        object.__setattr__(self, "occurred_at", normalized_occurred_at)
        object.__setattr__(self, "workspace_id", normalized_workspace)


@dataclass(frozen=True, kw_only=True)
class PurchaseRequestCreated(DomainEvent):
    tenant_id: str
    purchase_request_id: int
    status: str
    items_created: int = 0


@dataclass(frozen=True, kw_only=True)
class RfqCreated(DomainEvent):
    tenant_id: str
    rfq_id: int
    title: str = ""


@dataclass(frozen=True, kw_only=True)
class RfqAwarded(DomainEvent):
    tenant_id: str
    rfq_id: int
    award_id: int


@dataclass(frozen=True, kw_only=True)
class PurchaseOrderCreated(DomainEvent):
    tenant_id: str
    purchase_order_id: int
    status: str
    source: str = "manual"


@dataclass(frozen=True, kw_only=True)
class ErpOrderAccepted(DomainEvent):
    tenant_id: str
    purchase_order_id: int
    sync_run_id: int
    external_id: str | None = None


@dataclass(frozen=True, kw_only=True)
class ErpOrderRejected(DomainEvent):
    tenant_id: str
    purchase_order_id: int
    sync_run_id: int
    reason: str = ""


class EventBus:
    def __init__(self) -> None:
        self._lock = RLock()
        self._handlers: Dict[Type[DomainEvent], List[EventHandler]] = {}
        self._logger = logging.getLogger("app")

    def subscribe(self, event_type: Type[DomainEvent], handler: EventHandler) -> None:
        with self._lock:
            handlers = self._handlers.setdefault(event_type, [])
            handlers.append(handler)

    def publish(self, event: DomainEvent) -> None:
        observe_domain_event_emitted(type(event).__name__)
        self._persist_event_best_effort(event)
        with self._lock:
            handlers = list(self._handlers.get(type(event), []))
        for handler in handlers:
            try:
                handler(event)
            except Exception:  # noqa: BLE001
                self._logger.exception("event_handler_failed", extra={"event_type": type(event).__name__})

    def _persist_event_best_effort(self, event: DomainEvent) -> None:
        try:
            from flask import current_app, has_app_context

            if not has_app_context():
                return
            if not bool(current_app.config.get("ANALYTICS_PROJECTION_ENABLED", True)):
                return

            from app.contexts.analytics.infrastructure.read_model_repository import AnalyticsReadModelRepository
            from app.db import get_db

            workspace_id = str(getattr(event, "workspace_id", "") or getattr(event, "tenant_id", "")).strip()
            if not workspace_id:
                workspace_id = "unknown"

            payload = self._serialize_event_payload(event)
            db = get_db()
            repository = AnalyticsReadModelRepository(workspace_id=workspace_id)
            inserted = repository.append_event_store(
                db,
                workspace_id=workspace_id,
                event_id=str(getattr(event, "event_id", "") or ""),
                event_type=type(event).__name__,
                occurred_at=getattr(event, "occurred_at", None),
                payload=payload,
            )
            if inserted:
                observe_analytics_event_store_persisted(type(event).__name__)
        except Exception:  # noqa: BLE001
            observe_analytics_event_store_failed(1)
            self._logger.exception(
                "analytics_event_store_persist_failed",
                extra={
                    "event_type": type(event).__name__,
                    "event_id": str(getattr(event, "event_id", "") or ""),
                },
            )

    @staticmethod
    def _serialize_event_payload(event: DomainEvent) -> Dict[str, object]:
        raw = asdict(event)
        payload: Dict[str, object] = {}
        for key, value in raw.items():
            if isinstance(value, datetime):
                resolved = value
                if resolved.tzinfo is None:
                    resolved = resolved.replace(tzinfo=timezone.utc)
                payload[key] = resolved.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            else:
                payload[key] = value

        # Defensive serialization to avoid non-JSON values leaking into payload_json.
        json.loads(json.dumps(payload, default=str))
        return payload

    def clear(self) -> None:
        with self._lock:
            self._handlers.clear()


_DEFAULT_EVENT_BUS = EventBus()


def get_event_bus() -> EventBus:
    return _DEFAULT_EVENT_BUS


def reset_event_bus_for_tests() -> None:
    _DEFAULT_EVENT_BUS.clear()
