from __future__ import annotations

import copy
import json
import os
import random
import threading
import time
from dataclasses import fields as dataclass_fields
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List

from flask import current_app, has_app_context, has_request_context, request

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
from app.contexts.analytics.infrastructure.read_model_repository import AnalyticsReadModelRepository
from app.contexts.analytics.projections import AnalyticsProjectionDispatcher, default_projection_dispatcher
from app.contexts.analytics.application.shadow_compare import (
    diff_payload,
    hash_payload,
    should_emit_diff_log,
)
from app.observability import (
    analytics_shadow_compare_totals,
    current_request_id,
    observe_analytics_read_model_hit,
    observe_analytics_read_model_rebuild,
    observe_analytics_shadow_compare,
    observe_analytics_shadow_compare_diff_fields,
    observe_analytics_shadow_compare_diff_persisted,
    observe_analytics_shadow_compare_last_diff_timestamp,
    observe_analytics_shadow_compare_latency,
)
from app.contexts.analytics.application.payload import normalize_section_key, section_meta
from app.db import get_db
from app.domain.contracts import AnalyticsRequestInput


class AnalyticsService:
    def __init__(
        self,
        ttl_seconds: int = 60,
        repository_factory: Callable[[str], AnalyticsRepository] | None = None,
        read_model_repository_factory: Callable[[str], AnalyticsReadModelRepository] | None = None,
        projection_enabled: bool | None = None,
        projection_dispatcher: AnalyticsProjectionDispatcher | None = None,
    ) -> None:
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._cache: Dict[tuple, dict] = {}
        self._lock = threading.Lock()
        self._repository_factory = repository_factory or (lambda tenant_id: AnalyticsRepository(tenant_id=tenant_id))
        self._read_model_repository_factory = read_model_repository_factory or (
            lambda tenant_id: AnalyticsReadModelRepository(workspace_id=tenant_id)
        )
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

    def _read_model_enabled(self) -> bool:
        if has_app_context():
            return bool(current_app.config.get("ANALYTICS_READ_MODEL_ENABLED", False))
        return self._env_bool("ANALYTICS_READ_MODEL_ENABLED", False)

    def _shadow_compare_enabled(self) -> bool:
        if has_app_context():
            return bool(current_app.config.get("ANALYTICS_SHADOW_COMPARE_ENABLED", False))
        return self._env_bool("ANALYTICS_SHADOW_COMPARE_ENABLED", False)

    @staticmethod
    def _clamp_sample_rate(value: Any) -> float:
        try:
            sample = float(value)
        except (TypeError, ValueError):
            return 0.05
        if sample < 0.0:
            return 0.0
        if sample > 1.0:
            return 1.0
        return sample

    def _shadow_compare_sample_rate(self) -> float:
        default = 0.05
        if has_app_context():
            return self._clamp_sample_rate(current_app.config.get("ANALYTICS_SHADOW_COMPARE_SAMPLE_RATE", default))
        raw = os.environ.get("ANALYTICS_SHADOW_COMPARE_SAMPLE_RATE")
        if raw is None:
            return default
        return self._clamp_sample_rate(raw)

    def _shadow_compare_max_diff_logs_per_min(self) -> int:
        default = 20
        if has_app_context():
            try:
                value = int(current_app.config.get("ANALYTICS_SHADOW_COMPARE_MAX_DIFF_LOGS_PER_MIN", default))
            except (TypeError, ValueError):
                value = default
            return max(0, value)
        raw = os.environ.get("ANALYTICS_SHADOW_COMPARE_MAX_DIFF_LOGS_PER_MIN")
        if raw is None:
            return default
        try:
            return max(0, int(raw))
        except ValueError:
            return default

    @staticmethod
    def _is_analytics_path(path: str) -> bool:
        normalized = str(path or "").strip().lower()
        return normalized.startswith("/api/procurement/analytics") or normalized.startswith("/procurement/analises")

    def _should_run_shadow_compare(self, request_input: AnalyticsRequestInput, *, read_model_enabled: bool) -> bool:
        if not read_model_enabled:
            return False
        if not self._shadow_compare_enabled():
            return False
        role = str(request_input.role or "").strip().lower()
        if role == "supplier" or role not in {"buyer", "manager", "admin", "approver"}:
            return False
        if not has_request_context():
            return False
        if not self._is_analytics_path(request.path):
            return False
        sample_rate = self._shadow_compare_sample_rate()
        if sample_rate <= 0.0:
            return False
        return random.random() < sample_rate

    def _cache_key(
        self,
        request_input: AnalyticsRequestInput,
        *,
        visibility: Dict[str, Any],
        filters: Dict[str, Any],
        read_model_enabled: bool,
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
            "read_model" if read_model_enabled else "transacional",
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

    def _read_model_repository(self, tenant_id: str) -> AnalyticsReadModelRepository:
        return self._read_model_repository_factory(tenant_id)

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
        read_model_enabled = self._read_model_enabled()
        cache_key = self._cache_key(
            request_input,
            visibility=visibility,
            filters=filters,
            read_model_enabled=read_model_enabled,
        )
        cached = self._cache_get(cache_key)
        if cached is not None:
            source = str(cached.get("source") or "").strip().lower() or "transacional"
            if read_model_enabled:
                self._maybe_run_shadow_compare(
                    db,
                    request_input=request_input,
                    filters=filters,
                    visibility=visibility,
                    build_payload_fn=build_payload_fn,
                    payload_primary=cached,
                    primary_source=source,
                    read_model_enabled=read_model_enabled,
                )
            observe_analytics_read_model_hit(source)
            return cached

        if read_model_enabled:
            try:
                payload = self._build_dashboard_payload_from_read_model(
                    db,
                    request_input=request_input,
                    filters=filters,
                    visibility=visibility,
                )
                payload["source"] = "read_model"
            except Exception:  # noqa: BLE001
                if has_app_context():
                    current_app.logger.exception(
                        "analytics_read_model_fallback",
                        extra={
                            "tenant_id": request_input.tenant_id,
                            "section": request_input.section,
                        },
                    )
                payload = self._build_dashboard_payload_transacional(
                    db,
                    request_input=request_input,
                    filters=filters,
                    visibility=visibility,
                    build_payload_fn=build_payload_fn,
                )
                payload["source"] = "fallback"
        else:
            payload = self._build_dashboard_payload_transacional(
                db,
                request_input=request_input,
                filters=filters,
                visibility=visibility,
                build_payload_fn=build_payload_fn,
            )
            payload["source"] = "transacional"

        primary_source = str(payload.get("source") or "transacional").strip().lower() or "transacional"
        if read_model_enabled:
            self._maybe_run_shadow_compare(
                db,
                request_input=request_input,
                filters=filters,
                visibility=visibility,
                build_payload_fn=build_payload_fn,
                payload_primary=payload,
                primary_source=primary_source,
                read_model_enabled=read_model_enabled,
            )

        observe_analytics_read_model_hit(str(payload.get("source") or "transacional"))
        self._cache_set(cache_key, payload)
        return payload

    def _maybe_run_shadow_compare(
        self,
        db,
        *,
        request_input: AnalyticsRequestInput,
        filters: Dict[str, Any],
        visibility: Dict[str, Any],
        build_payload_fn,
        payload_primary: Dict[str, Any],
        primary_source: str,
        read_model_enabled: bool,
    ) -> None:
        if not self._should_run_shadow_compare(request_input, read_model_enabled=read_model_enabled):
            return

        started = time.perf_counter()
        try:
            payload_shadow = self._build_dashboard_payload_transacional(
                db,
                request_input=request_input,
                filters=filters,
                visibility=visibility,
                build_payload_fn=build_payload_fn,
            )
            payload_shadow["source"] = "transacional"

            compare_result = diff_payload(payload_primary, payload_shadow, max_diffs=20)
            if bool(compare_result.get("equal")):
                observe_analytics_shadow_compare("equal", primary_source)
                return

            summary = dict(compare_result.get("summary") or {})
            observe_analytics_shadow_compare("diff", primary_source)
            observe_analytics_shadow_compare_diff_fields(summary)
            observe_analytics_shadow_compare_last_diff_timestamp(time.time())
            should_store_or_log = False
            if has_app_context():
                should_store_or_log = should_emit_diff_log(self._shadow_compare_max_diff_logs_per_min())
            if should_store_or_log:
                self._persist_shadow_diff(
                    request_input=request_input,
                    primary_source=primary_source,
                    payload_primary=payload_primary,
                    payload_shadow=payload_shadow,
                    compare_result=compare_result,
                )
                raw_filters = dict(filters.get("raw") or {}) if isinstance(filters.get("raw"), dict) else {}
                current_app.logger.warning(
                    "analytics_shadow_compare_diff",
                    extra={
                        "request_id": current_request_id(default="n/a"),
                        "workspace_id": request_input.tenant_id,
                        "section": request_input.section,
                        "filters": raw_filters,
                        "primary_source": primary_source,
                        "shadow_source": "transacional",
                        "primary_hash": hash_payload(payload_primary),
                        "shadow_hash": hash_payload(payload_shadow),
                        "diff_summary": summary,
                        "diffs": list(compare_result.get("diffs") or []),
                    },
                )
        except Exception:  # noqa: BLE001
            observe_analytics_shadow_compare("error", primary_source)
            if has_app_context():
                current_app.logger.exception(
                    "analytics_shadow_compare_error",
                    extra={
                        "request_id": current_request_id(default="n/a"),
                        "workspace_id": request_input.tenant_id,
                        "section": request_input.section,
                        "primary_source": primary_source,
                    },
                )
        finally:
            observe_analytics_shadow_compare_latency((time.perf_counter() - started) * 1000.0)

    def _persist_shadow_diff(
        self,
        *,
        request_input: AnalyticsRequestInput,
        primary_source: str,
        payload_primary: Dict[str, Any],
        payload_shadow: Dict[str, Any],
        compare_result: Dict[str, Any],
    ) -> None:
        if not has_app_context():
            return
        if "DB_PATH" not in current_app.config:
            return

        summary = dict(compare_result.get("summary") or {})
        fields = list(compare_result.get("diffs") or [])[:10]
        diff_count = 0
        for key in ("kpis", "charts", "drilldown"):
            try:
                diff_count += max(0, int(summary.get(key) or 0))
            except (TypeError, ValueError):
                continue
        if diff_count <= 0:
            diff_count = len(fields)

        payload_summary = {
            "summary": summary,
            "fields": fields,
        }

        try:
            write_db = get_db()
            repo = self._read_model_repository(request_input.tenant_id)
            inserted = repo.append_shadow_diff_log(
                write_db,
                workspace_id=request_input.tenant_id,
                section=request_input.section,
                primary_source=primary_source,
                primary_hash=hash_payload(payload_primary),
                shadow_hash=hash_payload(payload_shadow),
                diff_summary=payload_summary,
                diff_count=diff_count,
                request_id=current_request_id(default="n/a"),
                occurred_at=datetime.now(timezone.utc),
            )
            if inserted:
                write_db.commit()
                observe_analytics_shadow_compare_diff_persisted(1)
        except Exception:  # noqa: BLE001
            current_app.logger.exception(
                "analytics_shadow_compare_diff_persist_failed",
                extra={
                    "request_id": current_request_id(default="n/a"),
                    "workspace_id": request_input.tenant_id,
                    "section": request_input.section,
                    "primary_source": primary_source,
                },
            )

    def build_shadow_compare_report(
        self,
        db,
        *,
        workspace_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
        section: str | None = None,
        limit: int = 50,
    ) -> dict:
        normalized_workspace = str(workspace_id or "").strip()
        if not normalized_workspace:
            raise ValueError("workspace_id is required")
        read_repo = self._read_model_repository(normalized_workspace)
        try:
            persisted = read_repo.build_shadow_diff_report(
                db,
                workspace_id=normalized_workspace,
                start_date=start_date,
                end_date=end_date,
                section=section,
                limit=limit,
            )
        except Exception:  # noqa: BLE001
            if has_app_context():
                current_app.logger.exception(
                    "analytics_shadow_report_query_failed",
                    extra={
                        "request_id": current_request_id(default="n/a"),
                        "workspace_id": normalized_workspace,
                    },
                )
            persisted = {"sections_breakdown": [], "recent_diffs": []}
        counters = analytics_shadow_compare_totals()
        total_compares = int(counters.get("total_compares") or 0)
        total_equal = int(counters.get("total_equal") or 0)
        total_diff = int(counters.get("total_diff") or 0)
        total_error = int(counters.get("total_error") or 0)
        diff_rate_percent = float(counters.get("diff_rate_percent") or 0.0)
        if total_compares <= 0:
            diff_rate_percent = 0.0

        return {
            "total_compares": total_compares,
            "total_equal": total_equal,
            "total_diff": total_diff,
            "total_error": total_error,
            "diff_rate_percent": round(diff_rate_percent, 2),
            "sections_breakdown": list(persisted.get("sections_breakdown") or []),
            "recent_diffs": list(persisted.get("recent_diffs") or []),
        }

    def _build_dashboard_payload_transacional(
        self,
        db,
        *,
        request_input: AnalyticsRequestInput,
        filters: Dict[str, Any],
        visibility: Dict[str, Any],
        build_payload_fn,
    ) -> dict:
        repo = self._repository(request_input.tenant_id)
        return repo.build_dashboard_payload(
            db,
            section_key=request_input.section,
            filters=filters,
            visibility=visibility,
            build_payload_fn=build_payload_fn,
        )

    def _build_dashboard_payload_from_read_model(
        self,
        db,
        *,
        request_input: AnalyticsRequestInput,
        filters: Dict[str, Any],
        visibility: Dict[str, Any],
    ) -> dict:
        read_repo = self._read_model_repository(request_input.tenant_id)
        workspace_id = request_input.tenant_id
        section_key = normalize_section_key(request_input.section)
        section_info = section_meta(section_key)
        raw_filters = dict(filters.get("raw") or {})

        kpi_map = read_repo.get_kpis(db, workspace_id=workspace_id, filters=filters, section=section_key)
        supplier_metrics = read_repo.get_supplier_metrics(db, workspace_id=workspace_id, filters=filters)
        stage_metrics = read_repo.get_stage_metrics(db, workspace_id=workspace_id, filters=filters)
        meta = read_repo.get_meta(db, workspace_id=workspace_id, filters=filters)
        section_payload = self._build_read_model_section_payload(
            section_key=section_key,
            kpi_map=kpi_map,
            supplier_metrics=supplier_metrics,
            stage_metrics=stage_metrics,
        )

        return {
            "section": section_info,
            "filters": raw_filters,
            "visibility": {
                "role": visibility.get("role"),
                "scope": visibility.get("scope"),
            },
            "meta": {
                "records_count": int(meta.get("records_count") or 0),
                "comparison_records_count": int(meta.get("comparison_records_count") or 0),
                "generated_at": str(meta.get("generated_at") or self._iso_now()),
            },
            "alerts": [],
            "alerts_meta": {
                "active_count": 0,
                "has_active": False,
            },
            **section_payload,
        }

    @staticmethod
    def _iso_now() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _metric_value(cls, kpi_map: Dict[str, Dict[str, Any]], metric: str, *, prefer_avg: bool = False) -> float:
        raw = dict(kpi_map.get(metric) or {})
        if prefer_avg:
            avg_value = cls._to_float(raw.get("avg_num"))
            if avg_value:
                return avg_value
        num_value = cls._to_float(raw.get("value_num"))
        int_value = cls._to_float(raw.get("value_int"))
        rows = int(raw.get("rows") or 0)
        if rows > 0 and num_value == 0.0 and int_value == 0.0:
            return 0.0
        return num_value if num_value != 0.0 else int_value

    @staticmethod
    def _fmt_int(value: Any) -> str:
        try:
            return f"{int(round(float(value or 0.0))):,}".replace(",", ".")
        except (TypeError, ValueError):
            return "0"

    @staticmethod
    def _fmt_number(value: Any) -> str:
        try:
            numeric = float(value or 0.0)
        except (TypeError, ValueError):
            return "0"
        if float(numeric).is_integer():
            return AnalyticsService._fmt_int(numeric)
        return f"{numeric:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    @staticmethod
    def _fmt_currency(value: Any) -> str:
        try:
            numeric = float(value or 0.0)
        except (TypeError, ValueError):
            numeric = 0.0
        formatted = f"{numeric:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {formatted}"

    @staticmethod
    def _fmt_percent(value: Any) -> str:
        try:
            numeric = float(value or 0.0)
        except (TypeError, ValueError):
            numeric = 0.0
        return f"{numeric:,.1f}%".replace(".", ",")

    @staticmethod
    def _fmt_duration_hours(value: Any) -> str:
        try:
            numeric = float(value or 0.0)
        except (TypeError, ValueError):
            numeric = 0.0
        if numeric <= 0:
            return "0h"
        if numeric >= 24:
            return f"{numeric / 24.0:.1f} dias".replace(".", ",")
        return f"{numeric:.1f} h".replace(".", ",")

    @staticmethod
    def _trend_flat() -> Dict[str, Any]:
        return {
            "direction": "flat",
            "delta_pct": 0.0,
            "display": "+0.0%",
            "label": "Estavel",
        }

    @classmethod
    def _kpi(
        cls,
        key: str,
        label: str,
        value: float | int,
        display_value: str,
        tooltip: str,
    ) -> Dict[str, Any]:
        return {
            "key": key,
            "label": label,
            "value": value,
            "display_value": display_value,
            "tooltip": tooltip,
            "trend": cls._trend_flat(),
        }

    @staticmethod
    def _chart_items_from_mapping(mapping: Dict[str, float], formatter) -> List[Dict[str, Any]]:
        if not mapping:
            return []
        max_value = max(float(value or 0.0) for value in mapping.values()) if mapping else 0.0
        items: List[Dict[str, Any]] = []
        for label, value in mapping.items():
            numeric = float(value or 0.0)
            ratio = (numeric / max_value * 100.0) if max_value > 0 else 0.0
            items.append(
                {
                    "label": label,
                    "value": numeric,
                    "display_value": formatter(numeric),
                    "ratio": round(ratio, 2),
                }
            )
        return items

    @staticmethod
    def _stage_metric_lookup(stage_metrics: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        lookup: Dict[str, Dict[str, Any]] = {}
        for item in list(stage_metrics or []):
            stage = str(item.get("stage") or "").strip().upper()
            if not stage:
                continue
            lookup[stage] = dict(item)
        return lookup

    def _build_read_model_section_payload(
        self,
        *,
        section_key: str,
        kpi_map: Dict[str, Dict[str, Any]],
        supplier_metrics: List[Dict[str, Any]],
        stage_metrics: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        backlog_open = int(round(self._metric_value(kpi_map, "backlog_open")))
        late_processes = int(round(self._metric_value(kpi_map, "late_processes")))
        economy_abs = self._metric_value(kpi_map, "economy_abs")
        supplier_response_rate = self._metric_value(kpi_map, "supplier_response_rate")
        awaiting_erp = int(round(self._metric_value(kpi_map, "awaiting_erp")))
        erp_rejections = int(round(self._metric_value(kpi_map, "erp_rejections")))
        no_competition = int(round(self._metric_value(kpi_map, "no_competition")))
        emergency_without_competition = int(round(self._metric_value(kpi_map, "emergency_without_competition")))
        avg_sr_to_oc = self._metric_value(kpi_map, "avg_sr_to_oc", prefer_avg=True)

        stage_lookup = self._stage_metric_lookup(stage_metrics)
        stage_sr = self._to_float((stage_lookup.get("SR") or {}).get("avg_hours"))
        stage_rfq = self._to_float((stage_lookup.get("RFQ") or {}).get("avg_hours"))
        stage_award = self._to_float((stage_lookup.get("AWARD") or {}).get("avg_hours"))
        stage_po = self._to_float((stage_lookup.get("PO") or {}).get("avg_hours"))
        stage_erp = self._to_float((stage_lookup.get("ERP") or {}).get("avg_hours"))
        avg_stage_time = self._to_float(
            (stage_sr + stage_rfq + stage_award + stage_po + stage_erp) / 5.0
            if any(value > 0 for value in (stage_sr, stage_rfq, stage_award, stage_po, stage_erp))
            else 0.0
        )

        supplier_sorted = sorted(
            [dict(item) for item in list(supplier_metrics or []) if str(item.get("supplier_key") or "").strip()],
            key=lambda item: float(item.get("savings_abs") or 0.0),
            reverse=True,
        )
        top_supplier = supplier_sorted[0]["supplier_key"] if supplier_sorted else "Nenhum"
        worst_delay = sorted(
            supplier_sorted,
            key=lambda item: float(item.get("avg_response_hours") or 0.0),
            reverse=True,
        )
        top_delay_supplier = worst_delay[0]["supplier_key"] if worst_delay else "Nenhum"

        section_defaults = {
            "charts": [],
            "drilldown": {},
        }

        if section_key == "overview":
            section_defaults["kpis"] = [
                self._kpi("backlog_open", "Backlog aberto", backlog_open, self._fmt_int(backlog_open), "Solicitacoes ainda em aberto."),
                self._kpi(
                    "active_quotes",
                    "Cotacoes ativas",
                    int(round(self._metric_value(kpi_map, "active_quotes"))),
                    self._fmt_int(self._metric_value(kpi_map, "active_quotes")),
                    "Cotacoes em andamento aguardando proposta ou decisao.",
                ),
                self._kpi(
                    "orders_in_progress",
                    "Ordens em andamento",
                    int(round(self._metric_value(kpi_map, "orders_in_progress"))),
                    self._fmt_int(self._metric_value(kpi_map, "orders_in_progress")),
                    "Ordens abertas em fluxo operacional.",
                ),
                self._kpi(
                    "erp_attention",
                    "Pendencias ERP",
                    awaiting_erp + erp_rejections,
                    self._fmt_int(awaiting_erp + erp_rejections),
                    "Ordens com pendencia de retorno ERP.",
                ),
            ]
            section_defaults["charts"] = [
                {
                    "key": "funnel_bar",
                    "type": "bar",
                    "title": "Funil operacional do periodo",
                    "items": self._chart_items_from_mapping(
                        {
                            "Backlog aberto": float(backlog_open),
                            "Pendencias ERP": float(awaiting_erp + erp_rejections),
                        },
                        self._fmt_number,
                    ),
                }
            ]
            section_defaults["drilldown"] = {"title": "Itens recentes do fluxo", "columns": [], "column_keys": [], "rows": []}
            return section_defaults

        if section_key == "efficiency":
            section_defaults["kpis"] = [
                self._kpi(
                    "avg_sr_to_oc",
                    "Tempo medio SR para OC",
                    avg_sr_to_oc,
                    self._fmt_duration_hours(avg_sr_to_oc),
                    "Tempo medio entre solicitacao e ordem de compra.",
                ),
                self._kpi(
                    "avg_stage_time",
                    "Tempo medio por etapa",
                    avg_stage_time,
                    self._fmt_duration_hours(avg_stage_time),
                    "Media das etapas SR, Cotacao, Decisao, OC e ERP.",
                ),
                self._kpi(
                    "late_processes",
                    "Processos em atraso",
                    late_processes,
                    self._fmt_int(late_processes),
                    "Processos com atraso no periodo.",
                ),
                self._kpi(
                    "backlog_open",
                    "Backlog aberto",
                    backlog_open,
                    self._fmt_int(backlog_open),
                    "Quantidade de processos abertos.",
                ),
            ]
            section_defaults["charts"] = [
                {
                    "key": "stage_breakdown_bar",
                    "type": "bar",
                    "title": "Breakdown por etapa do processo",
                    "items": self._chart_items_from_mapping(
                        {
                            "SR": stage_sr,
                            "Cotacao": stage_rfq,
                            "Decisao": stage_award,
                            "OC": stage_po,
                            "ERP": stage_erp,
                        },
                        self._fmt_duration_hours,
                    ),
                }
            ]
            section_defaults["drilldown"] = {"title": "Processos em atraso", "columns": [], "column_keys": [], "rows": []}
            return section_defaults

        if section_key == "costs":
            economy_pct = self._metric_value(kpi_map, "economy_pct")
            winner_vs_avg = self._metric_value(kpi_map, "winner_vs_avg")
            emergency_count = int(round(self._metric_value(kpi_map, "emergency_count")))
            section_defaults["kpis"] = [
                self._kpi("economy_abs", "Economia absoluta", economy_abs, self._fmt_currency(economy_abs), "Economia acumulada do periodo."),
                self._kpi("economy_pct", "Economia percentual", economy_pct, self._fmt_percent(economy_pct), "Economia percentual sobre baseline."),
                self._kpi("winner_vs_avg", "Preco vencedor vs media", winner_vs_avg, self._fmt_percent(winner_vs_avg), "Razao media entre vencedor e media cotada."),
                self._kpi("emergency_count", "Compras emergenciais", emergency_count, self._fmt_int(emergency_count), "Quantidade de compras emergenciais."),
            ]
            section_defaults["charts"] = [
                {
                    "key": "supplier_economy_ranking",
                    "type": "ranking",
                    "title": "Ranking por economia",
                    "items": self._chart_items_from_mapping(
                        {
                            str(item.get("supplier_key") or "Nao informado"): self._to_float(item.get("savings_abs"))
                            for item in supplier_sorted[:8]
                        },
                        self._fmt_currency,
                    ),
                }
            ]
            section_defaults["drilldown"] = {"title": "Itens com impacto financeiro", "columns": [], "column_keys": [], "rows": []}
            return section_defaults

        if section_key == "suppliers":
            avg_response_time = self._metric_value(kpi_map, "supplier_avg_response_time")
            section_defaults["kpis"] = [
                self._kpi(
                    "supplier_response_rate",
                    "Taxa de resposta",
                    supplier_response_rate,
                    self._fmt_percent(supplier_response_rate),
                    "Percentual de respostas sobre convites.",
                ),
                self._kpi(
                    "supplier_avg_response_time",
                    "Tempo medio de resposta",
                    avg_response_time,
                    self._fmt_duration_hours(avg_response_time),
                    "Tempo medio para resposta de fornecedores.",
                ),
                self._kpi(
                    "supplier_top_economy",
                    "Ranking por economia",
                    self._to_float((supplier_sorted[0] if supplier_sorted else {}).get("savings_abs")),
                    top_supplier,
                    "Fornecedor com maior economia acumulada.",
                ),
                self._kpi(
                    "supplier_top_delay",
                    "Ranking por atraso",
                    self._to_float((worst_delay[0] if worst_delay else {}).get("avg_response_hours")),
                    top_delay_supplier,
                    "Fornecedor com maior tempo medio de resposta.",
                ),
            ]
            section_defaults["charts"] = [
                {
                    "key": "supplier_economy",
                    "type": "ranking",
                    "title": "Fornecedores por economia",
                    "items": self._chart_items_from_mapping(
                        {
                            str(item.get("supplier_key") or "Nao informado"): self._to_float(item.get("savings_abs"))
                            for item in supplier_sorted[:8]
                        },
                        self._fmt_currency,
                    ),
                }
            ]
            section_defaults["drilldown"] = {"title": "Desempenho da base fornecedora", "columns": [], "column_keys": [], "rows": []}
            return section_defaults

        if section_key == "quality_erp":
            retries = int(round(self._metric_value(kpi_map, "erp_retries")))
            integration_errors = int(round(self._metric_value(kpi_map, "integration_errors")))
            section_defaults["kpis"] = [
                self._kpi("erp_rejections", "Rejeicoes ERP", erp_rejections, self._fmt_int(erp_rejections), "Rejeicoes registradas no ERP."),
                self._kpi("erp_retries", "Reenvios", retries, self._fmt_int(retries), "Quantidade de reenvios realizados."),
                self._kpi(
                    "integration_errors",
                    "Erros de integracao",
                    integration_errors,
                    self._fmt_int(integration_errors),
                    "Erros de integracao detectados.",
                ),
                self._kpi("awaiting_erp", "Aguardando ERP", awaiting_erp, self._fmt_int(awaiting_erp), "Ordens aguardando resposta do ERP."),
            ]
            section_defaults["charts"] = [
                {
                    "key": "erp_status_bar",
                    "type": "bar",
                    "title": "Distribuicao de status ERP",
                    "items": self._chart_items_from_mapping(
                        {
                            "Aguardando ERP": float(awaiting_erp),
                            "Rejeitado": float(erp_rejections),
                        },
                        self._fmt_number,
                    ),
                }
            ]
            section_defaults["drilldown"] = {"title": "Ocorrencias ERP para acompanhamento", "columns": [], "column_keys": [], "rows": []}
            return section_defaults

        if section_key == "compliance":
            approved_exceptions = int(round(self._metric_value(kpi_map, "approved_exceptions")))
            critical_actions = int(round(self._metric_value(kpi_map, "critical_actions")))
            section_defaults["kpis"] = [
                self._kpi("no_competition", "Compras sem concorrencia", no_competition, self._fmt_int(no_competition), "Processos sem concorrencia suficiente."),
                self._kpi(
                    "approved_exceptions",
                    "Excecoes aprovadas",
                    approved_exceptions,
                    self._fmt_int(approved_exceptions),
                    "Excecoes aprovadas no periodo.",
                ),
                self._kpi("critical_actions", "Acoes criticas", critical_actions, self._fmt_int(critical_actions), "Eventos de acoes criticas."),
                self._kpi(
                    "emergency_without_competition",
                    "Emergenciais sem concorrencia",
                    emergency_without_competition,
                    self._fmt_int(emergency_without_competition),
                    "Compras emergenciais sem concorrencia.",
                ),
            ]
            section_defaults["charts"] = [
                {
                    "key": "compliance_findings",
                    "type": "bar",
                    "title": "Ocorrencias de compliance",
                    "items": self._chart_items_from_mapping(
                        {
                            "Sem concorrencia": float(no_competition),
                            "Excecoes aprovadas": float(approved_exceptions),
                            "Acoes criticas": float(critical_actions),
                            "Emergenciais sem concorrencia": float(emergency_without_competition),
                        },
                        self._fmt_number,
                    ),
                }
            ]
            section_defaults["drilldown"] = {"title": "Itens com ponto de atencao", "columns": [], "column_keys": [], "rows": []}
            return section_defaults

        erp_pending = erp_rejections + awaiting_erp
        out_of_standard = no_competition + emergency_without_competition
        section_defaults["kpis"] = [
            self._kpi("economy_abs", "Economia acumulada", economy_abs, self._fmt_currency(economy_abs), "Economia acumulada do periodo."),
            self._kpi("avg_sr_to_oc", "Tempo medio SR para OC", avg_sr_to_oc, self._fmt_duration_hours(avg_sr_to_oc), "Tempo medio entre SR e OC."),
            self._kpi("late_processes", "Compras em atraso", late_processes, self._fmt_int(late_processes), "Quantidade de compras em atraso."),
            self._kpi("erp_pending", "Pendencias ERP", erp_pending, self._fmt_int(erp_pending), "Rejeicoes e ordens aguardando retorno do ERP."),
            self._kpi("out_of_standard", "Compras fora do padrao", out_of_standard, self._fmt_int(out_of_standard), "Compras fora do padrao esperado."),
        ]
        section_defaults["charts"] = []
        section_defaults["drilldown"] = {}
        return section_defaults

    def filter_sections_for_role(self, role: str, sections: List[dict]) -> List[dict]:
        if role not in {"manager", "admin"}:
            return [item for item in list(sections or []) if item.get("key") != "executive"]
        return list(sections or [])

    def rebuild_read_model(
        self,
        db,
        *,
        workspace_id: str,
        mode: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        normalized_workspace = str(workspace_id or "").strip()
        if not normalized_workspace:
            raise ValueError("workspace_id is required")
        normalized_mode = str(mode or "").strip().lower() or "full"
        if normalized_mode not in {"full", "range"}:
            raise ValueError("mode must be 'full' or 'range'")

        started_at = time.perf_counter()
        result = "success"
        total_events = 0
        processed = 0
        skipped_dedupe = 0
        failed = 0

        try:
            read_repo = self._read_model_repository(normalized_workspace)
            if normalized_mode == "full":
                read_repo.clear_projection_workspace(db, workspace_id=normalized_workspace)

            events = read_repo.list_event_store_events(
                db,
                workspace_id=normalized_workspace,
                start_date=start_date,
                end_date=end_date,
            )
            total_events = len(events)

            for row in events:
                event = self._event_from_store_row(row, workspace_id=normalized_workspace)
                if event is None:
                    failed += 1
                    continue
                event_summary = self._projection_dispatcher.process(event, db, normalized_workspace)
                processed += int(event_summary.get("processed") or 0)
                skipped_dedupe += int(event_summary.get("skipped_dedupe") or 0)
                failed += int(event_summary.get("failed") or 0)

            self.clear_cache()
        except Exception:
            result = "failed"
            raise
        finally:
            duration_seconds = max(0.0, time.perf_counter() - started_at)
            observe_analytics_read_model_rebuild(normalized_mode, result, duration_seconds)

        duration_ms = int(round(max(0.0, time.perf_counter() - started_at) * 1000.0))
        return {
            "workspace_id": normalized_workspace,
            "mode": normalized_mode,
            "total_events": int(total_events),
            "processed": int(processed),
            "skipped_dedupe": int(skipped_dedupe),
            "failed": int(failed),
            "duration_ms": duration_ms,
        }

    @staticmethod
    def _event_from_store_row(row: Dict[str, Any], *, workspace_id: str):
        payload_json = str(row.get("payload_json") or "").strip()
        try:
            payload = json.loads(payload_json) if payload_json else {}
        except json.JSONDecodeError:
            payload = {}

        event_type = str(row.get("event_type") or payload.get("event_type") or "").strip()
        event_id = str(row.get("event_id") or payload.get("event_id") or "").strip()
        occurred_at = AnalyticsService._parse_datetime(
            row.get("occurred_at") or payload.get("occurred_at")
        )
        event_registry = {
            "PurchaseRequestCreated": PurchaseRequestCreated,
            "RfqCreated": RfqCreated,
            "RfqAwarded": RfqAwarded,
            "PurchaseOrderCreated": PurchaseOrderCreated,
            "ErpOrderAccepted": ErpOrderAccepted,
            "ErpOrderRejected": ErpOrderRejected,
        }
        event_cls = event_registry.get(event_type)
        if event_cls is None:
            return None

        payload = dict(payload or {})
        payload["event_id"] = event_id or payload.get("event_id")
        payload["occurred_at"] = occurred_at or payload.get("occurred_at")
        payload["workspace_id"] = str(payload.get("workspace_id") or workspace_id).strip() or workspace_id
        payload["tenant_id"] = str(payload.get("tenant_id") or workspace_id).strip() or workspace_id

        allowed_fields = {field.name for field in dataclass_fields(event_cls)}
        event_kwargs = {key: payload.get(key) for key in allowed_fields if key in payload}
        event_kwargs["workspace_id"] = str(event_kwargs.get("workspace_id") or workspace_id).strip() or workspace_id
        event_kwargs["tenant_id"] = str(event_kwargs.get("tenant_id") or workspace_id).strip() or workspace_id
        if event_id:
            event_kwargs["event_id"] = event_id
        if occurred_at is not None:
            event_kwargs["occurred_at"] = occurred_at

        try:
            return event_cls(**event_kwargs)
        except TypeError:
            return None

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            resolved = value
        else:
            raw = str(value or "").strip()
            if not raw:
                return None
            normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            try:
                resolved = datetime.fromisoformat(normalized)
            except ValueError:
                return None

        if resolved.tzinfo is None:
            resolved = resolved.replace(tzinfo=timezone.utc)
        return resolved.astimezone(timezone.utc)
