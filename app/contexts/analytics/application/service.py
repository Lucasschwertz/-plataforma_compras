from __future__ import annotations

import copy
import os
import threading
import time
from typing import Any, Callable, Dict, List

from flask import current_app, has_app_context

from app.core import (
    ErpOrderAccepted,
    ErpOrderRejected,
    EventBus,
    PurchaseOrderCreated,
    PurchaseRequestCreated,
    RfqAwarded,
    RfqCreated,
)
from app.contexts.analytics.infrastructure.repository import AnalyticsRepository
from app.contexts.analytics.projections import AnalyticsProjectionDispatcher, default_projection_dispatcher
from app.db import get_db
from app.domain.contracts import AnalyticsRequestInput


class AnalyticsService:
    def __init__(
        self,
        ttl_seconds: int = 60,
        repository_factory: Callable[[str], AnalyticsRepository] | None = None,
        projection_enabled: bool | None = None,
        projection_dispatcher: AnalyticsProjectionDispatcher | None = None,
    ) -> None:
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._cache: Dict[tuple, dict] = {}
        self._lock = threading.Lock()
        self._repository_factory = repository_factory or (lambda tenant_id: AnalyticsRepository(tenant_id=tenant_id))
        self._projection_enabled_override = projection_enabled
        self._projection_dispatcher = projection_dispatcher or default_projection_dispatcher()
        self._event_handlers_registered = False
        self._projection_handlers_registered = False

    @staticmethod
    def _normalize_csv_filter_value(raw_value: str | None) -> str:
        values = [part.strip() for part in str(raw_value or "").split(",") if part.strip()]
        if not values:
            return ""
        return ",".join(sorted(dict.fromkeys(values)))

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        raw = os.environ.get(name)
        if raw is None:
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def _projection_enabled(self, override: bool | None = None) -> bool:
        if override is not None:
            return bool(override)
        if self._projection_enabled_override is not None:
            return bool(self._projection_enabled_override)
        if has_app_context():
            return bool(current_app.config.get("ANALYTICS_PROJECTION_ENABLED", True))
        return self._env_bool("ANALYTICS_PROJECTION_ENABLED", True)

    def _cache_key(
        self,
        request_input: AnalyticsRequestInput,
        *,
        visibility: Dict[str, Any],
        filters: Dict[str, Any],
    ) -> tuple:
        raw_filters = filters.get("raw") if isinstance(filters.get("raw"), dict) else {}
        raw_filters = raw_filters or {}
        actors = tuple(
            sorted(
                {
                    str(value).strip().lower()
                    for value in (visibility.get("actors") or [])
                    if str(value).strip()
                }
            )
        )
        normalized_filter_block = (
            str(raw_filters.get("start_date") or "").strip(),
            str(raw_filters.get("end_date") or "").strip(),
            str(raw_filters.get("supplier") or "").strip().lower(),
            str(raw_filters.get("buyer") or "").strip().lower(),
            self._normalize_csv_filter_value(str(raw_filters.get("status") or "")),
            self._normalize_csv_filter_value(str(raw_filters.get("purchase_type") or "")),
            str(raw_filters.get("period_basis") or "pr_created_at").strip().lower(),
            str(raw_filters.get("workspace_id") or request_input.tenant_id).strip().lower(),
        )
        return (
            str(request_input.tenant_id).strip().lower(),
            str(visibility.get("scope") or "").strip().lower(),
            str(request_input.section).strip().lower(),
            actors,
            normalized_filter_block,
        )

    def _cache_get(self, cache_key: tuple) -> dict | None:
        now = time.time()
        with self._lock:
            entry = self._cache.get(cache_key)
            if not entry:
                return None
            if float(entry.get("expires_at") or 0) <= now:
                self._cache.pop(cache_key, None)
                return None
            return copy.deepcopy(entry.get("payload") or {})

    def _cache_set(self, cache_key: tuple, payload: dict) -> None:
        with self._lock:
            self._cache[cache_key] = {
                "expires_at": time.time() + self.ttl_seconds,
                "payload": copy.deepcopy(payload),
            }

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()

    def register_event_handlers(self, event_bus: EventBus, *, projection_enabled: bool | None = None) -> None:
        enable_projection = self._projection_enabled(projection_enabled)

        subscribe_cache = False
        subscribe_projection = False
        with self._lock:
            if projection_enabled is not None:
                self._projection_enabled_override = bool(projection_enabled)
            if not self._event_handlers_registered:
                self._event_handlers_registered = True
                subscribe_cache = True
            if enable_projection and not self._projection_handlers_registered:
                self._projection_handlers_registered = True
                subscribe_projection = True

        if subscribe_cache:
            event_bus.subscribe(PurchaseRequestCreated, self._on_domain_event)
            event_bus.subscribe(RfqCreated, self._on_domain_event)
            event_bus.subscribe(RfqAwarded, self._on_domain_event)
            event_bus.subscribe(PurchaseOrderCreated, self._on_domain_event)
            event_bus.subscribe(ErpOrderAccepted, self._on_domain_event)
            event_bus.subscribe(ErpOrderRejected, self._on_domain_event)

        if subscribe_projection:
            event_bus.subscribe(PurchaseRequestCreated, self._on_projection_event)
            event_bus.subscribe(RfqCreated, self._on_projection_event)
            event_bus.subscribe(RfqAwarded, self._on_projection_event)
            event_bus.subscribe(PurchaseOrderCreated, self._on_projection_event)
            event_bus.subscribe(ErpOrderAccepted, self._on_projection_event)
            event_bus.subscribe(ErpOrderRejected, self._on_projection_event)

    def _on_domain_event(self, _event) -> None:
        self.clear_cache()

    def _on_projection_event(self, event) -> None:
        if not self._projection_enabled():
            return
        if not has_app_context():
            return

        workspace_id = str(getattr(event, "workspace_id", "") or getattr(event, "tenant_id", "")).strip()
        if not workspace_id:
            return

        try:
            db = get_db()
            self._projection_dispatcher.process(event, db, workspace_id)
            db.commit()
        except Exception:  # noqa: BLE001
            current_app.logger.exception(
                "analytics_projection_dispatch_failed",
                extra={
                    "event_type": type(event).__name__,
                    "workspace_id": workspace_id,
                    "event_id": str(getattr(event, "event_id", "") or "").strip() or None,
                },
            )

    def _repository(self, tenant_id: str) -> AnalyticsRepository:
        return self._repository_factory(tenant_id)

    def build_filters_payload(
        self,
        db,
        request_input: AnalyticsRequestInput,
        *,
        parse_filters_fn,
        resolve_visibility_fn,
        build_filter_options_fn,
    ) -> dict:
        visibility = resolve_visibility_fn(
            request_input.role,
            request_input.user_email,
            request_input.display_name,
            request_input.team_members,
        )
        filters = parse_filters_fn(request_input.request_args, request_input.tenant_id)
        repo = self._repository(request_input.tenant_id)
        payload = repo.build_filter_options(
            db,
            visibility=visibility,
            selected_filters=filters,
            build_filter_options_fn=build_filter_options_fn,
        )
        if request_input.role not in {"manager", "admin"}:
            payload["sections"] = [item for item in list(payload.get("sections") or []) if item.get("key") != "executive"]
        return payload

    def build_dashboard_payload(
        self,
        db,
        request_input: AnalyticsRequestInput,
        *,
        parse_filters_fn,
        resolve_visibility_fn,
        build_payload_fn,
    ) -> dict:
        visibility = resolve_visibility_fn(
            request_input.role,
            request_input.user_email,
            request_input.display_name,
            request_input.team_members,
        )
        filters = parse_filters_fn(request_input.request_args, request_input.tenant_id)
        cache_key = self._cache_key(request_input, visibility=visibility, filters=filters)
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        repo = self._repository(request_input.tenant_id)
        payload = repo.build_dashboard_payload(
            db,
            section_key=request_input.section,
            filters=filters,
            visibility=visibility,
            build_payload_fn=build_payload_fn,
        )
        self._cache_set(cache_key, payload)
        return payload

    def filter_sections_for_role(self, role: str, sections: List[dict]) -> List[dict]:
        if role not in {"manager", "admin"}:
            return [item for item in list(sections or []) if item.get("key") != "executive"]
        return list(sections or [])
