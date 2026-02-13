from __future__ import annotations

import copy
import threading
import time
from typing import Any, Callable, Dict, List

from app.domain.contracts import AnalyticsRequestInput
from app.contexts.analytics.infrastructure.repository import AnalyticsRepository


class AnalyticsService:
    def __init__(
        self,
        ttl_seconds: int = 60,
        repository_factory: Callable[[str], AnalyticsRepository] | None = None,
    ) -> None:
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._cache: Dict[tuple, dict] = {}
        self._lock = threading.Lock()
        self._repository_factory = repository_factory or (lambda tenant_id: AnalyticsRepository(tenant_id=tenant_id))

    @staticmethod
    def _normalize_csv_filter_value(raw_value: str | None) -> str:
        values = [part.strip() for part in str(raw_value or "").split(",") if part.strip()]
        if not values:
            return ""
        return ",".join(sorted(dict.fromkeys(values)))

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

