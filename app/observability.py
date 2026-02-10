from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict

from flask import g, has_request_context, request


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
            payload["request_id"] = current_request_id(default="-")
            payload["path"] = request.path
            payload["method"] = request.method
            if request.url_rule is not None:
                payload["route"] = request.url_rule.rule
        else:
            request_id = str(getattr(record, "request_id", "") or "").strip()
            if request_id:
                payload["request_id"] = request_id

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
        return request_id
    incoming = str(request.headers.get("X-Request-Id") or "").strip()
    request_id = incoming or str(uuid.uuid4())
    g.request_id = request_id
    return request_id


def current_request_id(default: str | None = None) -> str:
    request_id = str(getattr(g, "request_id", "") or "").strip()
    if request_id:
        return request_id
    return default or "n/a"


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests_total = 0
        self._errors_total = 0
        self._by_route: Dict[str, Dict[str, float]] = {}

    def observe_http(self, method: str, route: str, status_code: int, duration_ms: float) -> None:
        key = f"{method.upper()} {route}"
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
        }

    def reset(self) -> None:
        with self._lock:
            self._requests_total = 0
            self._errors_total = 0
            self._by_route.clear()


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


def reset_metrics_for_tests() -> None:
    _METRICS.reset()


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

    return {
        "worker_status": worker_state,
        "queue": {
            "pending_jobs": counters["queued"],
            "running_jobs": counters["running"],
            "failed_jobs": counters["failed"],
            "completed_jobs": counters["succeeded"],
            "avg_processing_ms": avg_processing_ms,
            "oldest_pending_age_seconds": oldest_queued_age or 0,
            "last_started_at": last_started_at.isoformat().replace("+00:00", "Z") if last_started_at else None,
            "last_finished_at": last_finished_at.isoformat().replace("+00:00", "Z") if last_finished_at else None,
        },
    }
