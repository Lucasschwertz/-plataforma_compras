from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict

from flask import current_app, g, has_request_context, request


_HTTP_DURATION_BUCKETS_MS = (5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0)
_OUTBOX_PROCESSING_BUCKETS_MS = (10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0)
_OUTBOX_BACKOFF_BUCKETS_SECONDS = (1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0)
_ANALYTICS_REBUILD_DURATION_BUCKETS_SECONDS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)
_ANALYTICS_SHADOW_COMPARE_LATENCY_BUCKETS_MS = (1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0)

_LOG_REQUEST_ID_CTX: contextvars.ContextVar[str] = contextvars.ContextVar("log_request_id", default="")


def _normalize_request_id(value: str | None) -> str:
    return str(value or "").strip() or "n/a"


def set_log_request_id(request_id: str | None) -> None:
    _LOG_REQUEST_ID_CTX.set(_normalize_request_id(request_id))


@contextlib.contextmanager
def bind_request_id(request_id: str | None):
    token = _LOG_REQUEST_ID_CTX.set(_normalize_request_id(request_id))
    try:
        yield _LOG_REQUEST_ID_CTX.get()
    finally:
        _LOG_REQUEST_ID_CTX.reset(token)


def _background_request_id(default: str | None = None) -> str:
    request_id = str(_LOG_REQUEST_ID_CTX.get() or "").strip()
    if request_id:
        return request_id
    return default or "n/a"


class JsonLogFormatter(logging.Formatter):
    _base_keys = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, object] = {
            "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        if has_request_context():
            payload["request_id"] = current_request_id(default="n/a")
            payload["path"] = request.path
            payload["method"] = request.method
            if request.url_rule is not None:
                payload["route"] = request.url_rule.rule
        else:
            record_request_id = str(getattr(record, "request_id", "") or "").strip()
            payload["request_id"] = record_request_id or _background_request_id(default="n/a")

        for key, value in record.__dict__.items():
            if key in self._base_keys or key.startswith("_"):
                continue
            if key in payload:
                continue
            if callable(value):
                continue
            payload[key] = value

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def configure_json_logging(app) -> None:
    if not bool(app.config.get("LOG_JSON", True)):
        return
    level_name = str(app.config.get("LOG_LEVEL", "INFO")).strip().upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level)
    app.logger.handlers = []
    app.logger.propagate = True


def ensure_request_id() -> str:
    request_id = str(getattr(g, "request_id", "") or "").strip()
    if request_id:
        set_log_request_id(request_id)
        return request_id
    incoming = str(request.headers.get("X-Request-Id") or "").strip()
    request_id = incoming or str(uuid.uuid4())
    g.request_id = request_id
    set_log_request_id(request_id)
    return request_id


def current_request_id(default: str | None = None) -> str:
    if has_request_context():
        request_id = str(getattr(g, "request_id", "") or "").strip()
        if request_id:
            return request_id
    return _background_request_id(default=default)


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests_total = 0
        self._errors_total = 0
        self._by_route: Dict[str, Dict[str, float]] = {}

        self._http_request_total: Dict[tuple[str, str, str], int] = {}
        self._http_request_duration_ms: Dict[tuple[str, str], dict] = {}

        self._erp_outbox_retry_count = 0
        self._erp_dead_letter_total = 0
        self._erp_outbox_processing_time = self._new_histogram_state(_OUTBOX_PROCESSING_BUCKETS_MS)
        self._erp_retry_backoff_seconds = self._new_histogram_state(_OUTBOX_BACKOFF_BUCKETS_SECONDS)

        self._domain_event_emitted_total: Dict[str, int] = {}
        self._analytics_projection_processed_total: Dict[tuple[str, str], int] = {}
        self._analytics_projection_failed_total: Dict[tuple[str, str], int] = {}
        self._analytics_projection_lag_seconds: Dict[str, float] = {}
        self._analytics_projection_last_success_timestamp: Dict[str, float] = {}
        self._analytics_read_model_hits_total: Dict[str, int] = {}
        self._analytics_read_model_confidence_status: Dict[str, int] = {}
        self._analytics_read_model_forced_fallback_total = 0
        self._analytics_event_store_persisted_total: Dict[str, int] = {}
        self._analytics_event_store_failed_total = 0
        self._analytics_read_model_rebuild_total: Dict[tuple[str, str], int] = {}
        self._analytics_read_model_rebuild_duration_seconds = self._new_histogram_state(
            _ANALYTICS_REBUILD_DURATION_BUCKETS_SECONDS
        )
        self._analytics_shadow_compare_total: Dict[tuple[str, str], int] = {}
        self._analytics_shadow_compare_diff_fields_total: Dict[str, int] = {}
        self._analytics_shadow_compare_latency_ms = self._new_histogram_state(
            _ANALYTICS_SHADOW_COMPARE_LATENCY_BUCKETS_MS
        )
        self._analytics_shadow_compare_last_diff_timestamp = 0.0
        self._analytics_shadow_compare_diff_rate = 0.0
        self._analytics_shadow_compare_diff_persisted_total = 0

    @staticmethod
    def _bucket_label(limit: float) -> str:
        return f"{limit:g}"

    @classmethod
    def _new_histogram_state(cls, limits: tuple[float, ...]) -> dict:
        return {
            "count": 0,
            "sum": 0.0,
            "buckets": {cls._bucket_label(limit): 0 for limit in limits} | {"+Inf": 0},
        }

    @classmethod
    def _observe_histogram(cls, state: dict, value: float, limits: tuple[float, ...]) -> None:
        duration = max(0.0, float(value))
        state["count"] += 1
        state["sum"] += duration
        for limit in limits:
            if duration <= limit:
                key = cls._bucket_label(limit)
                state["buckets"][key] = int(state["buckets"].get(key, 0)) + 1
        state["buckets"]["+Inf"] = int(state["count"])

    def observe_http(self, method: str, route: str, status_code: int, duration_ms: float) -> None:
        method_key = str(method or "GET").strip().upper() or "GET"
        route_key = str(route or "unknown").strip() or "unknown"
        status_key = str(int(status_code))

        key = f"{method_key} {route_key}"
        with self._lock:
            bucket = self._by_route.setdefault(
                key,
                {
                    "requests": 0.0,
                    "errors": 0.0,
                    "latency_sum_ms": 0.0,
                    "latency_max_ms": 0.0,
                },
            )
            bucket["requests"] += 1
            bucket["latency_sum_ms"] += max(0.0, float(duration_ms))
            bucket["latency_max_ms"] = max(bucket["latency_max_ms"], max(0.0, float(duration_ms)))
            self._requests_total += 1
            if int(status_code) >= 400:
                bucket["errors"] += 1
                self._errors_total += 1

            self._http_request_total[(method_key, route_key, status_key)] = (
                int(self._http_request_total.get((method_key, route_key, status_key), 0)) + 1
            )
            hist_key = (method_key, route_key)
            histogram = self._http_request_duration_ms.setdefault(
                hist_key,
                self._new_histogram_state(_HTTP_DURATION_BUCKETS_MS),
            )
            self._observe_histogram(histogram, duration_ms, _HTTP_DURATION_BUCKETS_MS)

    def observe_erp_outbox_retry(self, count: int = 1) -> None:
        increment = max(0, int(count or 0))
        if increment <= 0:
            return
        with self._lock:
            self._erp_outbox_retry_count += increment

    def observe_erp_outbox_dead_letter(self, count: int = 1) -> None:
        increment = max(0, int(count or 0))
        if increment <= 0:
            return
        with self._lock:
            self._erp_dead_letter_total += increment

    def observe_erp_outbox_retry_backoff(self, backoff_seconds: float) -> None:
        with self._lock:
            self._observe_histogram(self._erp_retry_backoff_seconds, float(backoff_seconds), _OUTBOX_BACKOFF_BUCKETS_SECONDS)

    def observe_erp_outbox_processing(self, duration_ms: float) -> None:
        with self._lock:
            self._observe_histogram(self._erp_outbox_processing_time, duration_ms, _OUTBOX_PROCESSING_BUCKETS_MS)

    def observe_domain_event_emitted(self, event_type: str) -> None:
        key = str(event_type or "unknown").strip() or "unknown"
        with self._lock:
            self._domain_event_emitted_total[key] = int(self._domain_event_emitted_total.get(key, 0)) + 1

    def observe_analytics_projection_processed(self, projector: str, event_type: str) -> None:
        projector_key = str(projector or "unknown").strip() or "unknown"
        event_key = str(event_type or "unknown").strip() or "unknown"
        key = (projector_key, event_key)
        with self._lock:
            self._analytics_projection_processed_total[key] = int(self._analytics_projection_processed_total.get(key, 0)) + 1

    def observe_analytics_projection_failed(self, projector: str, event_type: str) -> None:
        projector_key = str(projector or "unknown").strip() or "unknown"
        event_key = str(event_type or "unknown").strip() or "unknown"
        key = (projector_key, event_key)
        with self._lock:
            self._analytics_projection_failed_total[key] = int(self._analytics_projection_failed_total.get(key, 0)) + 1

    def observe_analytics_projection_lag(self, projector: str, occurred_at: datetime | float | int | None) -> None:
        projector_key = str(projector or "unknown").strip() or "unknown"
        now_ts = time.time()
        if isinstance(occurred_at, datetime):
            value = occurred_at
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            occurred_ts = value.astimezone(timezone.utc).timestamp()
        elif occurred_at is None:
            occurred_ts = now_ts
        else:
            try:
                occurred_ts = float(occurred_at)
            except (TypeError, ValueError):
                occurred_ts = now_ts
        lag_seconds = max(0.0, now_ts - occurred_ts)
        with self._lock:
            self._analytics_projection_lag_seconds[projector_key] = lag_seconds
            self._analytics_projection_last_success_timestamp[projector_key] = now_ts

    def observe_analytics_read_model_hit(self, source: str) -> None:
        source_key = str(source or "unknown").strip().lower() or "unknown"
        with self._lock:
            self._analytics_read_model_hits_total[source_key] = (
                int(self._analytics_read_model_hits_total.get(source_key, 0)) + 1
            )

    def observe_analytics_read_model_confidence_status(self, status: str) -> None:
        status_key = str(status or "unknown").strip().lower() or "unknown"
        with self._lock:
            self._analytics_read_model_confidence_status[status_key] = (
                int(self._analytics_read_model_confidence_status.get(status_key, 0)) + 1
            )

    def observe_analytics_read_model_forced_fallback(self, count: int = 1) -> None:
        increment = max(0, int(count or 0))
        if increment <= 0:
            return
        with self._lock:
            self._analytics_read_model_forced_fallback_total += increment

    def observe_analytics_event_store_persisted(self, event_type: str) -> None:
        event_key = str(event_type or "unknown").strip() or "unknown"
        with self._lock:
            self._analytics_event_store_persisted_total[event_key] = (
                int(self._analytics_event_store_persisted_total.get(event_key, 0)) + 1
            )

    def observe_analytics_event_store_failed(self, count: int = 1) -> None:
        increment = max(0, int(count or 0))
        if increment <= 0:
            return
        with self._lock:
            self._analytics_event_store_failed_total += increment

    def observe_analytics_read_model_rebuild(self, mode: str, result: str, duration_seconds: float) -> None:
        mode_key = str(mode or "unknown").strip().lower() or "unknown"
        result_key = str(result or "unknown").strip().lower() or "unknown"
        with self._lock:
            key = (mode_key, result_key)
            self._analytics_read_model_rebuild_total[key] = int(self._analytics_read_model_rebuild_total.get(key, 0)) + 1
            self._observe_histogram(
                self._analytics_read_model_rebuild_duration_seconds,
                float(duration_seconds or 0.0),
                _ANALYTICS_REBUILD_DURATION_BUCKETS_SECONDS,
            )

    def observe_analytics_shadow_compare(self, result: str, primary_source: str) -> None:
        result_key = str(result or "unknown").strip().lower() or "unknown"
        source_key = str(primary_source or "unknown").strip().lower() or "unknown"
        with self._lock:
            key = (result_key, source_key)
            self._analytics_shadow_compare_total[key] = int(self._analytics_shadow_compare_total.get(key, 0)) + 1
            total_compares = int(sum(self._analytics_shadow_compare_total.values()))
            total_diff = int(
                sum(value for (res, _src), value in self._analytics_shadow_compare_total.items() if res == "diff")
            )
            if total_compares <= 0:
                self._analytics_shadow_compare_diff_rate = 0.0
            else:
                self._analytics_shadow_compare_diff_rate = (float(total_diff) * 100.0) / float(total_compares)

    def observe_analytics_shadow_compare_diff_fields(self, summary: Dict[str, int] | None) -> None:
        source = dict(summary or {})
        with self._lock:
            for field in ("kpis", "charts", "drilldown"):
                increment = max(0, int(source.get(field) or 0))
                if increment <= 0:
                    continue
                self._analytics_shadow_compare_diff_fields_total[field] = int(
                    self._analytics_shadow_compare_diff_fields_total.get(field, 0)
                ) + increment

    def observe_analytics_shadow_compare_latency(self, duration_ms: float) -> None:
        with self._lock:
            self._observe_histogram(
                self._analytics_shadow_compare_latency_ms,
                float(duration_ms or 0.0),
                _ANALYTICS_SHADOW_COMPARE_LATENCY_BUCKETS_MS,
            )

    def observe_analytics_shadow_compare_last_diff_timestamp(self, timestamp: float | int | None = None) -> None:
        try:
            value = float(timestamp if timestamp is not None else time.time())
        except (TypeError, ValueError):
            value = time.time()
        with self._lock:
            self._analytics_shadow_compare_last_diff_timestamp = max(0.0, value)

    def observe_analytics_shadow_compare_diff_persisted(self, count: int = 1) -> None:
        increment = max(0, int(count or 0))
        if increment <= 0:
            return
        with self._lock:
            self._analytics_shadow_compare_diff_persisted_total += increment

    def snapshot(self) -> dict:
        with self._lock:
            route_stats = []
            for route, bucket in self._by_route.items():
                requests_count = int(bucket["requests"])
                avg_ms = 0.0
                if requests_count > 0:
                    avg_ms = float(bucket["latency_sum_ms"]) / requests_count
                route_stats.append(
                    {
                        "route": route,
                        "requests": requests_count,
                        "errors": int(bucket["errors"]),
                        "avg_latency_ms": round(avg_ms, 2),
                        "max_latency_ms": round(float(bucket["latency_max_ms"]), 2),
                    }
                )
            route_stats.sort(key=lambda item: item["requests"], reverse=True)
            return {
                "requests_total": int(self._requests_total),
                "errors_total": int(self._errors_total),
                "by_route": route_stats[:40],
                "erp_outbox": {
                    "retry_count": int(self._erp_outbox_retry_count),
                    "dead_letter_total": int(self._erp_dead_letter_total),
                    "processing_count": int(self._erp_outbox_processing_time["count"]),
                    "retry_backoff_count": int(self._erp_retry_backoff_seconds["count"]),
                },
                "domain_events": {
                    "emitted_total": int(sum(self._domain_event_emitted_total.values())),
                    "by_type": dict(sorted(self._domain_event_emitted_total.items())),
                },
                "analytics_projections": {
                    "processed_total": int(sum(self._analytics_projection_processed_total.values())),
                    "failed_total": int(sum(self._analytics_projection_failed_total.values())),
                    "projectors": sorted(set(self._analytics_projection_lag_seconds.keys())),
                },
                "analytics_read_model": {
                    "hits_total": int(sum(self._analytics_read_model_hits_total.values())),
                    "by_source": dict(sorted(self._analytics_read_model_hits_total.items())),
                    "confidence_status": dict(sorted(self._analytics_read_model_confidence_status.items())),
                    "forced_fallback_total": int(self._analytics_read_model_forced_fallback_total),
                },
                "analytics_event_store": {
                    "persisted_total": int(sum(self._analytics_event_store_persisted_total.values())),
                    "failed_total": int(self._analytics_event_store_failed_total),
                },
                "analytics_read_model_rebuild": {
                    "total": int(sum(self._analytics_read_model_rebuild_total.values())),
                },
                "analytics_shadow_compare": {
                    "total": int(sum(self._analytics_shadow_compare_total.values())),
                    "last_diff_timestamp": float(self._analytics_shadow_compare_last_diff_timestamp),
                    "diff_rate_percent": float(self._analytics_shadow_compare_diff_rate),
                    "diff_persisted_total": int(self._analytics_shadow_compare_diff_persisted_total),
                },
            }

    def analytics_shadow_compare_totals(self) -> dict:
        with self._lock:
            total_compares = int(sum(self._analytics_shadow_compare_total.values()))
            total_diff = int(
                sum(value for (result, _source), value in self._analytics_shadow_compare_total.items() if result == "diff")
            )
            total_equal = int(
                sum(value for (result, _source), value in self._analytics_shadow_compare_total.items() if result == "equal")
            )
            total_error = int(
                sum(value for (result, _source), value in self._analytics_shadow_compare_total.items() if result == "error")
            )
            rate = float(self._analytics_shadow_compare_diff_rate)
            if total_compares <= 0:
                rate = 0.0
            return {
                "total_compares": total_compares,
                "total_equal": total_equal,
                "total_diff": total_diff,
                "total_error": total_error,
                "diff_rate_percent": rate,
            }

    def prometheus_snapshot(self) -> dict:
        with self._lock:
            http_totals = [
                {
                    "method": method,
                    "route": route,
                    "status": status,
                    "value": int(value),
                }
                for (method, route, status), value in sorted(self._http_request_total.items())
            ]
            http_histograms = []
            for (method, route), histogram in sorted(self._http_request_duration_ms.items()):
                http_histograms.append(
                    {
                        "method": method,
                        "route": route,
                        "count": int(histogram["count"]),
                        "sum": float(histogram["sum"]),
                        "buckets": {label: int(count) for label, count in histogram["buckets"].items()},
                    }
                )

            outbox_hist = {
                "count": int(self._erp_outbox_processing_time["count"]),
                "sum": float(self._erp_outbox_processing_time["sum"]),
                "buckets": {
                    label: int(count)
                    for label, count in self._erp_outbox_processing_time["buckets"].items()
                },
            }
            backoff_hist = {
                "count": int(self._erp_retry_backoff_seconds["count"]),
                "sum": float(self._erp_retry_backoff_seconds["sum"]),
                "buckets": {
                    label: int(count)
                    for label, count in self._erp_retry_backoff_seconds["buckets"].items()
                },
            }
            return {
                "http_request_total": http_totals,
                "http_request_duration_ms": http_histograms,
                "erp_outbox_retry_count": int(self._erp_outbox_retry_count),
                "erp_dead_letter_total": int(self._erp_dead_letter_total),
                "erp_outbox_processing_time": outbox_hist,
                "erp_retry_backoff_seconds": backoff_hist,
                "domain_event_emitted_total": dict(sorted(self._domain_event_emitted_total.items())),
                "analytics_projection_processed_total": {
                    key: int(value)
                    for key, value in sorted(
                        self._analytics_projection_processed_total.items(),
                        key=lambda item: (item[0][0], item[0][1]),
                    )
                },
                "analytics_projection_failed_total": {
                    key: int(value)
                    for key, value in sorted(
                        self._analytics_projection_failed_total.items(),
                        key=lambda item: (item[0][0], item[0][1]),
                    )
                },
                "analytics_projection_lag_seconds": {
                    key: float(value)
                    for key, value in sorted(self._analytics_projection_lag_seconds.items())
                },
                "analytics_projection_last_success_timestamp": {
                    key: float(value)
                    for key, value in sorted(self._analytics_projection_last_success_timestamp.items())
                },
                "analytics_read_model_hits_total": {
                    key: int(value)
                    for key, value in sorted(self._analytics_read_model_hits_total.items())
                },
                "analytics_read_model_confidence_status": {
                    key: int(value)
                    for key, value in sorted(self._analytics_read_model_confidence_status.items())
                },
                "analytics_read_model_forced_fallback_total": int(self._analytics_read_model_forced_fallback_total),
                "analytics_event_store_persisted_total": {
                    key: int(value)
                    for key, value in sorted(self._analytics_event_store_persisted_total.items())
                },
                "analytics_event_store_failed_total": int(self._analytics_event_store_failed_total),
                "analytics_read_model_rebuild_total": {
                    key: int(value)
                    for key, value in sorted(
                        self._analytics_read_model_rebuild_total.items(),
                        key=lambda item: (item[0][0], item[0][1]),
                    )
                },
                "analytics_read_model_rebuild_duration_seconds": {
                    "count": int(self._analytics_read_model_rebuild_duration_seconds["count"]),
                    "sum": float(self._analytics_read_model_rebuild_duration_seconds["sum"]),
                    "buckets": {
                        label: int(count)
                        for label, count in self._analytics_read_model_rebuild_duration_seconds["buckets"].items()
                    },
                },
                "analytics_shadow_compare_total": {
                    key: int(value)
                    for key, value in sorted(
                        self._analytics_shadow_compare_total.items(),
                        key=lambda item: (item[0][0], item[0][1]),
                    )
                },
                "analytics_shadow_compare_diff_fields_total": {
                    key: int(value)
                    for key, value in sorted(self._analytics_shadow_compare_diff_fields_total.items())
                },
                "analytics_shadow_compare_latency_ms": {
                    "count": int(self._analytics_shadow_compare_latency_ms["count"]),
                    "sum": float(self._analytics_shadow_compare_latency_ms["sum"]),
                    "buckets": {
                        label: int(count)
                        for label, count in self._analytics_shadow_compare_latency_ms["buckets"].items()
                    },
                },
                "analytics_shadow_compare_last_diff_timestamp": float(
                    self._analytics_shadow_compare_last_diff_timestamp
                ),
                "analytics_shadow_compare_diff_rate": float(self._analytics_shadow_compare_diff_rate),
                "analytics_shadow_compare_diff_persisted_total": int(
                    self._analytics_shadow_compare_diff_persisted_total
                ),
            }

    def reset(self) -> None:
        with self._lock:
            self._requests_total = 0
            self._errors_total = 0
            self._by_route.clear()
            self._http_request_total.clear()
            self._http_request_duration_ms.clear()
            self._erp_outbox_retry_count = 0
            self._erp_dead_letter_total = 0
            self._erp_outbox_processing_time = self._new_histogram_state(_OUTBOX_PROCESSING_BUCKETS_MS)
            self._erp_retry_backoff_seconds = self._new_histogram_state(_OUTBOX_BACKOFF_BUCKETS_SECONDS)
            self._domain_event_emitted_total.clear()
            self._analytics_projection_processed_total.clear()
            self._analytics_projection_failed_total.clear()
            self._analytics_projection_lag_seconds.clear()
            self._analytics_projection_last_success_timestamp.clear()
            self._analytics_read_model_hits_total.clear()
            self._analytics_read_model_confidence_status.clear()
            self._analytics_read_model_forced_fallback_total = 0
            self._analytics_event_store_persisted_total.clear()
            self._analytics_event_store_failed_total = 0
            self._analytics_read_model_rebuild_total.clear()
            self._analytics_read_model_rebuild_duration_seconds = self._new_histogram_state(
                _ANALYTICS_REBUILD_DURATION_BUCKETS_SECONDS
            )
            self._analytics_shadow_compare_total.clear()
            self._analytics_shadow_compare_diff_fields_total.clear()
            self._analytics_shadow_compare_latency_ms = self._new_histogram_state(
                _ANALYTICS_SHADOW_COMPARE_LATENCY_BUCKETS_MS
            )
            self._analytics_shadow_compare_last_diff_timestamp = 0.0
            self._analytics_shadow_compare_diff_rate = 0.0
            self._analytics_shadow_compare_diff_persisted_total = 0


_METRICS = MetricsRegistry()


def mark_request_start() -> None:
    g._request_started_at = time.perf_counter()


def observe_response(response):
    started = float(getattr(g, "_request_started_at", 0.0) or 0.0)
    elapsed_ms = 0.0
    if started > 0.0:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
    route = request.url_rule.rule if request.url_rule is not None else request.path
    _METRICS.observe_http(request.method, route, int(response.status_code), elapsed_ms)
    response.headers["X-Response-Time-Ms"] = f"{elapsed_ms:.2f}"
    return response


def metrics_snapshot() -> dict:
    return _METRICS.snapshot()


def analytics_shadow_compare_totals() -> dict:
    return _METRICS.analytics_shadow_compare_totals()


def observe_erp_outbox_retry(count: int = 1) -> None:
    _METRICS.observe_erp_outbox_retry(count)


def observe_erp_outbox_dead_letter(count: int = 1) -> None:
    _METRICS.observe_erp_outbox_dead_letter(count)


def observe_erp_outbox_retry_backoff(backoff_seconds: float) -> None:
    _METRICS.observe_erp_outbox_retry_backoff(backoff_seconds)


def observe_erp_outbox_processing(duration_ms: float) -> None:
    _METRICS.observe_erp_outbox_processing(duration_ms)


def observe_domain_event_emitted(event_type: str) -> None:
    _METRICS.observe_domain_event_emitted(event_type)


def observe_analytics_projection_processed(projector: str, event_type: str) -> None:
    _METRICS.observe_analytics_projection_processed(projector, event_type)


def observe_analytics_projection_failed(projector: str, event_type: str) -> None:
    _METRICS.observe_analytics_projection_failed(projector, event_type)


def observe_analytics_projection_lag(projector: str, occurred_at: datetime | float | int | None) -> None:
    _METRICS.observe_analytics_projection_lag(projector, occurred_at)


def observe_analytics_read_model_hit(source: str) -> None:
    _METRICS.observe_analytics_read_model_hit(source)


def observe_analytics_read_model_confidence_status(status: str) -> None:
    _METRICS.observe_analytics_read_model_confidence_status(status)


def observe_analytics_read_model_forced_fallback(count: int = 1) -> None:
    _METRICS.observe_analytics_read_model_forced_fallback(count)


def observe_analytics_event_store_persisted(event_type: str) -> None:
    _METRICS.observe_analytics_event_store_persisted(event_type)


def observe_analytics_event_store_failed(count: int = 1) -> None:
    _METRICS.observe_analytics_event_store_failed(count)


def observe_analytics_read_model_rebuild(mode: str, result: str, duration_seconds: float) -> None:
    _METRICS.observe_analytics_read_model_rebuild(mode, result, duration_seconds)


def observe_analytics_shadow_compare(result: str, primary_source: str) -> None:
    _METRICS.observe_analytics_shadow_compare(result, primary_source)


def observe_analytics_shadow_compare_diff_fields(summary: Dict[str, int] | None) -> None:
    _METRICS.observe_analytics_shadow_compare_diff_fields(summary)


def observe_analytics_shadow_compare_latency(duration_ms: float) -> None:
    _METRICS.observe_analytics_shadow_compare_latency(duration_ms)


def observe_analytics_shadow_compare_last_diff_timestamp(timestamp: float | int | None = None) -> None:
    _METRICS.observe_analytics_shadow_compare_last_diff_timestamp(timestamp)


def observe_analytics_shadow_compare_diff_persisted(count: int = 1) -> None:
    _METRICS.observe_analytics_shadow_compare_diff_persisted(count)


def _prom_label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _prom_line(name: str, value: int | float, labels: dict[str, object] | None = None) -> str:
    if labels:
        labels_blob = ",".join(f'{key}="{_prom_label(val)}"' for key, val in sorted(labels.items()))
        return f"{name}{{{labels_blob}}} {value}"
    return f"{name} {value}"


def prometheus_metrics_text(*, outbox_state: dict | None = None) -> str:
    snapshot = _METRICS.prometheus_snapshot()
    lines: list[str] = []

    lines.append("# HELP http_request_total Total HTTP requests by method, route and status.")
    lines.append("# TYPE http_request_total counter")
    for sample in snapshot["http_request_total"]:
        lines.append(
            _prom_line(
                "http_request_total",
                int(sample["value"]),
                labels={
                    "method": sample["method"],
                    "route": sample["route"],
                    "status": sample["status"],
                },
            )
        )

    lines.append("# HELP http_request_duration_ms HTTP request duration in milliseconds.")
    lines.append("# TYPE http_request_duration_ms histogram")
    for hist in snapshot["http_request_duration_ms"]:
        base_labels = {"method": hist["method"], "route": hist["route"]}
        for le_label, bucket_value in hist["buckets"].items():
            lines.append(
                _prom_line(
                    "http_request_duration_ms_bucket",
                    int(bucket_value),
                    labels=base_labels | {"le": le_label},
                )
            )
        lines.append(_prom_line("http_request_duration_ms_sum", float(hist["sum"]), labels=base_labels))
        lines.append(_prom_line("http_request_duration_ms_count", int(hist["count"]), labels=base_labels))

    outbox_queue = ((outbox_state or {}).get("queue") or {}) if isinstance(outbox_state, dict) else {}
    lines.append("# HELP erp_outbox_queue_size ERP outbox queue size by state.")
    lines.append("# TYPE erp_outbox_queue_size gauge")
    lines.append(_prom_line("erp_outbox_queue_size", int(outbox_queue.get("pending_jobs") or 0), labels={"state": "pending"}))
    lines.append(_prom_line("erp_outbox_queue_size", int(outbox_queue.get("running_jobs") or 0), labels={"state": "running"}))
    lines.append(_prom_line("erp_outbox_queue_size", int(outbox_queue.get("failed_jobs") or 0), labels={"state": "failed"}))
    lines.append(_prom_line("erp_outbox_queue_size", int(outbox_queue.get("completed_jobs") or 0), labels={"state": "completed"}))

    lines.append("# HELP erp_outbox_retry_count Total outbox retries triggered.")
    lines.append("# TYPE erp_outbox_retry_count counter")
    lines.append(_prom_line("erp_outbox_retry_count", int(snapshot["erp_outbox_retry_count"])))

    lines.append("# HELP erp_dead_letter_total Total outbox jobs moved to dead letter.")
    lines.append("# TYPE erp_dead_letter_total counter")
    lines.append(_prom_line("erp_dead_letter_total", int(snapshot["erp_dead_letter_total"])))

    lines.append("# HELP erp_outbox_processing_time ERP outbox processing time in milliseconds.")
    lines.append("# TYPE erp_outbox_processing_time histogram")
    processing_hist = snapshot["erp_outbox_processing_time"]
    for le_label, bucket_value in processing_hist["buckets"].items():
        lines.append(_prom_line("erp_outbox_processing_time_bucket", int(bucket_value), labels={"le": le_label}))
    lines.append(_prom_line("erp_outbox_processing_time_sum", float(processing_hist["sum"])))
    lines.append(_prom_line("erp_outbox_processing_time_count", int(processing_hist["count"])))

    lines.append("# HELP erp_retry_backoff_seconds ERP outbox retry backoff delay in seconds.")
    lines.append("# TYPE erp_retry_backoff_seconds histogram")
    backoff_hist = snapshot["erp_retry_backoff_seconds"]
    for le_label, bucket_value in backoff_hist["buckets"].items():
        lines.append(_prom_line("erp_retry_backoff_seconds_bucket", int(bucket_value), labels={"le": le_label}))
    lines.append(_prom_line("erp_retry_backoff_seconds_sum", float(backoff_hist["sum"])))
    lines.append(_prom_line("erp_retry_backoff_seconds_count", int(backoff_hist["count"])))

    lines.append("# HELP erp_circuit_state ERP circuit breaker state (1 active, 0 inactive).")
    lines.append("# TYPE erp_circuit_state gauge")
    circuit_state = "closed"
    try:
        from app.contexts.erp.infrastructure.circuit_breaker import erp_circuit_snapshot

        circuit_state = str((erp_circuit_snapshot() or {}).get("state") or "closed").strip().lower() or "closed"
    except Exception:
        circuit_state = "closed"
    for state_key in ("closed", "open", "half_open"):
        lines.append(
            _prom_line(
                "erp_circuit_state",
                1 if circuit_state == state_key else 0,
                labels={"state": state_key},
            )
        )

    lines.append("# HELP domain_event_emitted_total Domain events emitted by event type.")
    lines.append("# TYPE domain_event_emitted_total counter")
    for event_type, total in snapshot["domain_event_emitted_total"].items():
        lines.append(_prom_line("domain_event_emitted_total", int(total), labels={"event_type": event_type}))

    lines.append("# HELP analytics_projection_processed_total Total processed analytics projection events.")
    lines.append("# TYPE analytics_projection_processed_total counter")
    for (projector, event_type), total in snapshot["analytics_projection_processed_total"].items():
        lines.append(
            _prom_line(
                "analytics_projection_processed_total",
                int(total),
                labels={"projector": projector, "event_type": event_type},
            )
        )

    lines.append("# HELP analytics_projection_failed_total Total failed analytics projection events.")
    lines.append("# TYPE analytics_projection_failed_total counter")
    for (projector, event_type), total in snapshot["analytics_projection_failed_total"].items():
        lines.append(
            _prom_line(
                "analytics_projection_failed_total",
                int(total),
                labels={"projector": projector, "event_type": event_type},
            )
        )

    lines.append("# HELP analytics_projection_lag_seconds Projection lag in seconds by projector.")
    lines.append("# TYPE analytics_projection_lag_seconds gauge")
    for projector, value in snapshot["analytics_projection_lag_seconds"].items():
        lines.append(
            _prom_line(
                "analytics_projection_lag_seconds",
                float(value),
                labels={"projector": projector},
            )
        )

    lines.append("# HELP analytics_read_model_lag_seconds Read model projection lag in seconds by projector.")
    lines.append("# TYPE analytics_read_model_lag_seconds gauge")
    for projector, value in snapshot["analytics_projection_lag_seconds"].items():
        lines.append(
            _prom_line(
                "analytics_read_model_lag_seconds",
                float(value),
                labels={"projector": projector},
            )
        )

    lines.append("# HELP analytics_projection_last_success_timestamp Unix timestamp of the last projection success.")
    lines.append("# TYPE analytics_projection_last_success_timestamp gauge")
    for projector, value in snapshot["analytics_projection_last_success_timestamp"].items():
        lines.append(
            _prom_line(
                "analytics_projection_last_success_timestamp",
                float(value),
                labels={"projector": projector},
            )
        )

    lines.append("# HELP analytics_read_model_hits_total Total analytics payload hits by source path.")
    lines.append("# TYPE analytics_read_model_hits_total counter")
    for source, total in snapshot["analytics_read_model_hits_total"].items():
        lines.append(
            _prom_line(
                "analytics_read_model_hits_total",
                int(total),
                labels={"source": source},
            )
        )

    lines.append("# HELP analytics_read_model_confidence_status Read model confidence evaluations grouped by status.")
    lines.append("# TYPE analytics_read_model_confidence_status gauge")
    for status, total in snapshot["analytics_read_model_confidence_status"].items():
        lines.append(
            _prom_line(
                "analytics_read_model_confidence_status",
                int(total),
                labels={"status": status},
            )
        )

    lines.append("# HELP analytics_read_model_forced_fallback_total Total forced fallback executions due to degraded confidence.")
    lines.append("# TYPE analytics_read_model_forced_fallback_total counter")
    lines.append(
        _prom_line(
            "analytics_read_model_forced_fallback_total",
            int(snapshot["analytics_read_model_forced_fallback_total"]),
        )
    )

    lines.append("# HELP analytics_event_store_persisted_total Total domain events persisted in analytics event store.")
    lines.append("# TYPE analytics_event_store_persisted_total counter")
    for event_type, total in snapshot["analytics_event_store_persisted_total"].items():
        lines.append(
            _prom_line(
                "analytics_event_store_persisted_total",
                int(total),
                labels={"event_type": event_type},
            )
        )

    lines.append("# HELP analytics_event_store_failed_total Total failures while persisting domain events in analytics event store.")
    lines.append("# TYPE analytics_event_store_failed_total counter")
    lines.append(_prom_line("analytics_event_store_failed_total", int(snapshot["analytics_event_store_failed_total"])))

    lines.append("# HELP analytics_read_model_rebuild_total Total analytics read model rebuild runs by mode and result.")
    lines.append("# TYPE analytics_read_model_rebuild_total counter")
    for (mode, result), total in snapshot["analytics_read_model_rebuild_total"].items():
        lines.append(
            _prom_line(
                "analytics_read_model_rebuild_total",
                int(total),
                labels={"mode": mode, "result": result},
            )
        )

    lines.append("# HELP analytics_read_model_rebuild_duration_seconds Analytics read model rebuild duration in seconds.")
    lines.append("# TYPE analytics_read_model_rebuild_duration_seconds histogram")
    rebuild_hist = snapshot["analytics_read_model_rebuild_duration_seconds"]
    for le_label, bucket_value in rebuild_hist["buckets"].items():
        lines.append(
            _prom_line(
                "analytics_read_model_rebuild_duration_seconds_bucket",
                int(bucket_value),
                labels={"le": le_label},
            )
        )
    lines.append(
        _prom_line(
            "analytics_read_model_rebuild_duration_seconds_sum",
            float(rebuild_hist["sum"]),
        )
    )
    lines.append(
        _prom_line(
            "analytics_read_model_rebuild_duration_seconds_count",
            int(rebuild_hist["count"]),
        )
    )

    lines.append("# HELP analytics_shadow_compare_total Total shadow compare executions by result and primary source.")
    lines.append("# TYPE analytics_shadow_compare_total counter")
    for (result, primary_source), total in snapshot["analytics_shadow_compare_total"].items():
        lines.append(
            _prom_line(
                "analytics_shadow_compare_total",
                int(total),
                labels={"result": result, "primary_source": primary_source},
            )
        )

    lines.append("# HELP analytics_shadow_compare_diff_fields_total Total diff fields grouped by payload area.")
    lines.append("# TYPE analytics_shadow_compare_diff_fields_total counter")
    for field, total in snapshot["analytics_shadow_compare_diff_fields_total"].items():
        lines.append(
            _prom_line(
                "analytics_shadow_compare_diff_fields_total",
                int(total),
                labels={"field": field},
            )
        )

    lines.append("# HELP analytics_shadow_compare_latency_ms Shadow compare latency in milliseconds.")
    lines.append("# TYPE analytics_shadow_compare_latency_ms histogram")
    shadow_hist = snapshot["analytics_shadow_compare_latency_ms"]
    for le_label, bucket_value in shadow_hist["buckets"].items():
        lines.append(
            _prom_line(
                "analytics_shadow_compare_latency_ms_bucket",
                int(bucket_value),
                labels={"le": le_label},
            )
        )
    lines.append(
        _prom_line(
            "analytics_shadow_compare_latency_ms_sum",
            float(shadow_hist["sum"]),
        )
    )
    lines.append(
        _prom_line(
            "analytics_shadow_compare_latency_ms_count",
            int(shadow_hist["count"]),
        )
    )

    lines.append("# HELP analytics_shadow_compare_last_diff_timestamp Unix timestamp of the last shadow diff detected.")
    lines.append("# TYPE analytics_shadow_compare_last_diff_timestamp gauge")
    lines.append(
        _prom_line(
            "analytics_shadow_compare_last_diff_timestamp",
            float(snapshot["analytics_shadow_compare_last_diff_timestamp"]),
        )
    )

    lines.append("# HELP analytics_shadow_compare_diff_rate Current shadow compare diff rate percentage.")
    lines.append("# TYPE analytics_shadow_compare_diff_rate gauge")
    lines.append(
        _prom_line(
            "analytics_shadow_compare_diff_rate",
            float(snapshot["analytics_shadow_compare_diff_rate"]),
        )
    )

    lines.append("# HELP analytics_shadow_compare_diff_persisted_total Total persisted shadow compare diffs.")
    lines.append("# TYPE analytics_shadow_compare_diff_persisted_total counter")
    lines.append(
        _prom_line(
            "analytics_shadow_compare_diff_persisted_total",
            int(snapshot["analytics_shadow_compare_diff_persisted_total"]),
        )
    )

    return "\n".join(lines) + "\n"


def reset_metrics_for_tests() -> None:
    _METRICS.reset()
    set_log_request_id(None)


def _parse_timestamp(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
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


def _outbox_critical_thresholds() -> tuple[int, int]:
    age_seconds = 900
    pending_jobs = 50
    try:
        age_seconds = int(current_app.config.get("ERP_OUTBOX_CRITICAL_AGE_SECONDS", age_seconds) or age_seconds)
        pending_jobs = int(current_app.config.get("ERP_OUTBOX_CRITICAL_PENDING_JOBS", pending_jobs) or pending_jobs)
    except Exception:
        pass
    return max(1, age_seconds), max(1, pending_jobs)


def outbox_health(db) -> dict:
    rows = db.execute(
        """
        SELECT status, started_at, finished_at, duration_ms
        FROM sync_runs
        WHERE scope = 'purchase_order'
          AND payload_hash LIKE 'po_push:%'
        ORDER BY id DESC
        LIMIT 500
        """
    ).fetchall()
    counters = {"queued": 0, "running": 0, "succeeded": 0, "failed": 0}
    durations = []
    now = datetime.now(timezone.utc)
    oldest_queued_age = None
    last_started_at = None
    last_finished_at = None

    for row in rows:
        status = str(row["status"] or "").strip().lower()
        if status in counters:
            counters[status] += 1
        started_at = _parse_timestamp(row["started_at"])
        finished_at = _parse_timestamp(row["finished_at"])
        duration_ms = row["duration_ms"]

        if started_at and (last_started_at is None or started_at > last_started_at):
            last_started_at = started_at
        if finished_at and (last_finished_at is None or finished_at > last_finished_at):
            last_finished_at = finished_at
        if status == "queued" and started_at:
            age = int((now - started_at).total_seconds())
            oldest_queued_age = age if oldest_queued_age is None else max(oldest_queued_age, age)
        if duration_ms is not None:
            try:
                durations.append(float(duration_ms))
            except (TypeError, ValueError):
                continue

    avg_processing_ms = round(sum(durations) / len(durations), 2) if durations else 0.0
    stale_after = 120
    worker_state = "idle"
    if counters["running"] > 0:
        worker_state = "running"
    elif counters["queued"] > 0:
        if last_finished_at is None:
            worker_state = "stalled"
        else:
            age_since_last_finish = int((now - last_finished_at).total_seconds())
            worker_state = "stalled" if age_since_last_finish > stale_after else "draining"

    critical_age_seconds, critical_pending_jobs = _outbox_critical_thresholds()
    oldest_age = oldest_queued_age or 0
    backlog_critical = counters["queued"] >= critical_pending_jobs or oldest_age >= critical_age_seconds
    worker_active = worker_state in {"running", "draining"}

    return {
        "worker_status": worker_state,
        "worker_active": worker_active,
        "backlog_critical": backlog_critical,
        "queue": {
            "pending_jobs": counters["queued"],
            "running_jobs": counters["running"],
            "failed_jobs": counters["failed"],
            "completed_jobs": counters["succeeded"],
            "avg_processing_ms": avg_processing_ms,
            "oldest_pending_age_seconds": oldest_age,
            "last_started_at": last_started_at.isoformat().replace("+00:00", "Z") if last_started_at else None,
            "last_finished_at": last_finished_at.isoformat().replace("+00:00", "Z") if last_finished_at else None,
            "backlog_critical": backlog_critical,
        },
    }
