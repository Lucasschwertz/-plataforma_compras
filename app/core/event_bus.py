from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Callable, Dict, List, Type

from app.observability import observe_domain_event_emitted


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
        with self._lock:
            handlers = list(self._handlers.get(type(event), []))
        for handler in handlers:
            try:
                handler(event)
            except Exception:  # noqa: BLE001
                self._logger.exception("event_handler_failed", extra={"event_type": type(event).__name__})

    def clear(self) -> None:
        with self._lock:
            self._handlers.clear()


_DEFAULT_EVENT_BUS = EventBus()


def get_event_bus() -> EventBus:
    return _DEFAULT_EVENT_BUS


def reset_event_bus_for_tests() -> None:
    _DEFAULT_EVENT_BUS.clear()
