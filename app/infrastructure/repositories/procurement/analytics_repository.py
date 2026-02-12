from __future__ import annotations

from typing import Any, Dict

from app.infrastructure.repositories.base import BaseRepository


class AnalyticsRepository(BaseRepository):
    """Read-only repository wrapper for analytics payload builders."""

    def build_filter_options(
        self,
        db,
        *,
        visibility: Dict[str, Any],
        selected_filters: Dict[str, Any],
        build_filter_options_fn,
    ) -> Dict[str, Any]:
        return build_filter_options_fn(
            db,
            self.tenant_id,
            visibility,
            selected_filters,
        )

    def build_dashboard_payload(
        self,
        db,
        *,
        section_key: str,
        filters: Dict[str, Any],
        visibility: Dict[str, Any],
        build_payload_fn,
    ) -> Dict[str, Any]:
        normalized_filters = dict(filters or {})
        raw = dict(normalized_filters.get("raw") or {})
        raw["workspace_id"] = self.workspace_id
        normalized_filters["raw"] = raw
        return build_payload_fn(
            db,
            self.tenant_id,
            section_key,
            normalized_filters,
            visibility,
        )

