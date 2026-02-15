from __future__ import annotations

import contextlib
import os
import threading
import time
from collections import deque
from typing import Deque, Dict

from flask import current_app, has_app_context


def _parse_bool(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _cfg_bool(name: str, default: bool) -> bool:
    if has_app_context():
        return _parse_bool(current_app.config.get(name, default), default)
    return _parse_bool(os.environ.get(name), default)


def _cfg_int(name: str, default: int) -> int:
    raw = current_app.config.get(name, default) if has_app_context() else os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _normalized_workspace_id(workspace_id: str | None) -> str:
    return str(workspace_id or "").strip().lower()


class WorkspaceLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._request_window_by_workspace: Dict[str, Deque[float]] = {}
        self._active_analytics_by_workspace: Dict[str, int] = {}
        self._degraded_until_by_workspace: Dict[str, float] = {}
        self._analytics_minute_counters: Dict[int, Dict[str, int]] = {}

    @staticmethod
    def _minute_key(ts: float | None = None) -> int:
        return int((ts if ts is not None else time.time()) // 60)

    @staticmethod
    def _cleanup_minute_counters(counters: Dict[int, Dict[str, int]], now_minute: int) -> None:
        min_allowed = now_minute - 5
        for key in list(counters.keys()):
            if key < min_allowed:
                counters.pop(key, None)

    def _cleanup_expired_workspace_state(self, workspace_id: str, now_ts: float) -> None:
        window = self._request_window_by_workspace.get(workspace_id)
        if window is not None:
            while window and (now_ts - float(window[0])) >= 60.0:
                window.popleft()
            if not window:
                self._request_window_by_workspace.pop(workspace_id, None)

        degraded_until = float(self._degraded_until_by_workspace.get(workspace_id, 0.0) or 0.0)
        if degraded_until > 0.0 and degraded_until <= now_ts:
            self._degraded_until_by_workspace.pop(workspace_id, None)

        if int(self._active_analytics_by_workspace.get(workspace_id, 0) or 0) <= 0:
            self._active_analytics_by_workspace.pop(workspace_id, None)

    def _is_degraded_nolock(self, workspace_id: str, now_ts: float) -> bool:
        degraded_until = float(self._degraded_until_by_workspace.get(workspace_id, 0.0) or 0.0)
        if degraded_until <= 0.0:
            return False
        if degraded_until <= now_ts:
            self._degraded_until_by_workspace.pop(workspace_id, None)
            return False
        return True

    def _record_analytics_result_nolock(self, result: str, now_ts: float) -> None:
        minute = self._minute_key(now_ts)
        bucket = self._analytics_minute_counters.setdefault(
            minute,
            {"allowed": 0, "degraded": 0, "blocked": 0},
        )
        key = str(result or "").strip().lower()
        if key not in bucket:
            key = "blocked"
        bucket[key] = int(bucket.get(key, 0) or 0) + 1
        self._cleanup_minute_counters(self._analytics_minute_counters, minute)

    def check_analytics(self, workspace_id: str | None) -> dict:
        normalized_workspace = _normalized_workspace_id(workspace_id)
        if not normalized_workspace:
            return {"allowed": True, "degraded": False, "retry_after": 0}

        if not _cfg_bool("GOV_ENABLED", True):
            return {"allowed": True, "degraded": False, "retry_after": 0}

        now_ts = time.time()
        max_rpm = max(1, _cfg_int("GOV_ANALYTICS_MAX_RPM_PER_WORKSPACE", 60))
        max_concurrent = max(1, _cfg_int("GOV_ANALYTICS_MAX_CONCURRENT_PER_WORKSPACE", 4))

        with self._lock:
            self._cleanup_expired_workspace_state(normalized_workspace, now_ts)

            active = int(self._active_analytics_by_workspace.get(normalized_workspace, 0) or 0)
            if active >= max_concurrent:
                self._record_analytics_result_nolock("blocked", now_ts)
                return {"allowed": False, "degraded": self._is_degraded_nolock(normalized_workspace, now_ts), "retry_after": 1}

            window = self._request_window_by_workspace.setdefault(normalized_workspace, deque())
            while window and (now_ts - float(window[0])) >= 60.0:
                window.popleft()

            if len(window) >= max_rpm:
                oldest = float(window[0]) if window else now_ts
                retry_after = max(1, int(round(60.0 - (now_ts - oldest))))
                self._record_analytics_result_nolock("blocked", now_ts)
                return {
                    "allowed": False,
                    "degraded": self._is_degraded_nolock(normalized_workspace, now_ts),
                    "retry_after": retry_after,
                }

            window.append(now_ts)
            degraded = self._is_degraded_nolock(normalized_workspace, now_ts)
            self._record_analytics_result_nolock("degraded" if degraded else "allowed", now_ts)
            return {"allowed": True, "degraded": degraded, "retry_after": 0}

    @contextlib.contextmanager
    def enter_analytics(self, workspace_id: str | None):
        normalized_workspace = _normalized_workspace_id(workspace_id)
        if not normalized_workspace or not _cfg_bool("GOV_ENABLED", True):
            yield True
            return

        max_concurrent = max(1, _cfg_int("GOV_ANALYTICS_MAX_CONCURRENT_PER_WORKSPACE", 4))
        acquired = False
        with self._lock:
            active = int(self._active_analytics_by_workspace.get(normalized_workspace, 0) or 0)
            if active < max_concurrent:
                self._active_analytics_by_workspace[normalized_workspace] = active + 1
                acquired = True

        try:
            yield acquired
        finally:
            if not acquired:
                return
            with self._lock:
                current = int(self._active_analytics_by_workspace.get(normalized_workspace, 0) or 0)
                if current <= 1:
                    self._active_analytics_by_workspace.pop(normalized_workspace, None)
                else:
                    self._active_analytics_by_workspace[normalized_workspace] = current - 1

    def mark_degraded(self, workspace_id: str | None, ttl_seconds: int) -> None:
        normalized_workspace = _normalized_workspace_id(workspace_id)
        if not normalized_workspace:
            return
        if not _cfg_bool("GOV_ENABLED", True):
            return

        ttl = max(1, int(ttl_seconds or 0))
        now_ts = time.time()
        until_ts = now_ts + float(ttl)
        with self._lock:
            previous = float(self._degraded_until_by_workspace.get(normalized_workspace, 0.0) or 0.0)
            self._degraded_until_by_workspace[normalized_workspace] = max(previous, until_ts)
            self._cleanup_expired_workspace_state(normalized_workspace, now_ts)

    def is_degraded(self, workspace_id: str | None) -> bool:
        normalized_workspace = _normalized_workspace_id(workspace_id)
        if not normalized_workspace:
            return False
        if not _cfg_bool("GOV_ENABLED", True):
            return False

        now_ts = time.time()
        with self._lock:
            return self._is_degraded_nolock(normalized_workspace, now_ts)

    def degraded_active_count(self) -> int:
        if not _cfg_bool("GOV_ENABLED", True):
            return 0
        now_ts = time.time()
        with self._lock:
            count = 0
            for workspace_id in list(self._degraded_until_by_workspace.keys()):
                if self._is_degraded_nolock(workspace_id, now_ts):
                    count += 1
            return count

    def analytics_last_minute_counters(self) -> dict:
        now_minute = self._minute_key()
        with self._lock:
            self._cleanup_minute_counters(self._analytics_minute_counters, now_minute)
            total = {"allowed": 0, "degraded": 0, "blocked": 0}
            for minute, values in self._analytics_minute_counters.items():
                if minute < now_minute - 1:
                    continue
                for key in total:
                    total[key] += int(values.get(key, 0) or 0)
            return total

    def reset(self) -> None:
        with self._lock:
            self._request_window_by_workspace.clear()
            self._active_analytics_by_workspace.clear()
            self._degraded_until_by_workspace.clear()
            self._analytics_minute_counters.clear()


class WorkerFairness:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_worker_by_workspace: Dict[str, int] = {}
        self._worker_minute_counters: Dict[int, Dict[str, int]] = {}

    @staticmethod
    def _minute_key(ts: float | None = None) -> int:
        return int((ts if ts is not None else time.time()) // 60)

    @staticmethod
    def _cleanup_minute_counters(counters: Dict[int, Dict[str, int]], now_minute: int) -> None:
        min_allowed = now_minute - 5
        for key in list(counters.keys()):
            if key < min_allowed:
                counters.pop(key, None)

    def _record_worker_event_nolock(self, event: str, now_ts: float) -> None:
        minute = self._minute_key(now_ts)
        bucket = self._worker_minute_counters.setdefault(
            minute,
            {"throttled": 0, "deferred": 0, "overflow": 0},
        )
        key = str(event or "").strip().lower()
        if key not in bucket:
            return
        bucket[key] = int(bucket.get(key, 0) or 0) + 1
        self._cleanup_minute_counters(self._worker_minute_counters, minute)

    def can_process_job(self, workspace_id: str | None, backlog_size: int) -> dict:
        normalized_workspace = _normalized_workspace_id(workspace_id)
        if not normalized_workspace:
            return {"allowed": True, "retry_after": 0, "reason": None}
        if not _cfg_bool("GOV_ENABLED", True):
            return {"allowed": True, "retry_after": 0, "reason": None}

        max_concurrent = max(1, _cfg_int("GOV_WORKER_MAX_CONCURRENT_PER_WORKSPACE", 1))
        max_backlog = max(1, _cfg_int("GOV_WORKER_MAX_QUEUE_BACKLOG_PER_WORKSPACE", 500))
        retry_after = max(1, _cfg_int("GOV_WORKER_BACKOFF_ON_LIMIT_SECONDS", 30))
        backlog = max(0, int(backlog_size or 0))
        now_ts = time.time()

        with self._lock:
            if backlog > max_backlog:
                self._record_worker_event_nolock("throttled", now_ts)
                self._record_worker_event_nolock("overflow", now_ts)
                return {"allowed": False, "retry_after": retry_after, "reason": "backlog"}

            active = int(self._active_worker_by_workspace.get(normalized_workspace, 0) or 0)
            if active >= max_concurrent:
                self._record_worker_event_nolock("throttled", now_ts)
                return {"allowed": False, "retry_after": retry_after, "reason": "concurrency"}

        return {"allowed": True, "retry_after": 0, "reason": None}

    @contextlib.contextmanager
    def enter_workspace(self, workspace_id: str | None):
        normalized_workspace = _normalized_workspace_id(workspace_id)
        if not normalized_workspace or not _cfg_bool("GOV_ENABLED", True):
            yield True
            return

        max_concurrent = max(1, _cfg_int("GOV_WORKER_MAX_CONCURRENT_PER_WORKSPACE", 1))
        acquired = False
        with self._lock:
            active = int(self._active_worker_by_workspace.get(normalized_workspace, 0) or 0)
            if active < max_concurrent:
                self._active_worker_by_workspace[normalized_workspace] = active + 1
                acquired = True
            else:
                self._record_worker_event_nolock("throttled", time.time())

        try:
            yield acquired
        finally:
            if not acquired:
                return
            with self._lock:
                current = int(self._active_worker_by_workspace.get(normalized_workspace, 0) or 0)
                if current <= 1:
                    self._active_worker_by_workspace.pop(normalized_workspace, None)
                else:
                    self._active_worker_by_workspace[normalized_workspace] = current - 1

    def note_deferred(self) -> None:
        with self._lock:
            self._record_worker_event_nolock("deferred", time.time())

    def note_overflow(self) -> None:
        with self._lock:
            self._record_worker_event_nolock("overflow", time.time())

    def worker_last_minute_counters(self) -> dict:
        now_minute = self._minute_key()
        with self._lock:
            self._cleanup_minute_counters(self._worker_minute_counters, now_minute)
            total = {"throttled": 0, "deferred": 0, "overflow": 0}
            for minute, values in self._worker_minute_counters.items():
                if minute < now_minute - 1:
                    continue
                for key in total:
                    total[key] += int(values.get(key, 0) or 0)
            return total

    def reset(self) -> None:
        with self._lock:
            self._active_worker_by_workspace.clear()
            self._worker_minute_counters.clear()


_WORKSPACE_LIMITER = WorkspaceLimiter()
_WORKER_FAIRNESS = WorkerFairness()


def get_workspace_limiter() -> WorkspaceLimiter:
    return _WORKSPACE_LIMITER


def get_worker_fairness() -> WorkerFairness:
    return _WORKER_FAIRNESS


def governance_status_snapshot() -> dict:
    analytics = _WORKSPACE_LIMITER.analytics_last_minute_counters()
    worker = _WORKER_FAIRNESS.worker_last_minute_counters()
    return {
        "analytics": {
            "degraded_active_count": int(_WORKSPACE_LIMITER.degraded_active_count()),
            "blocked_last_minute": int(analytics.get("blocked", 0)),
            "allowed_last_minute": int(analytics.get("allowed", 0)),
        },
        "worker": {
            "throttled_last_minute": int(worker.get("throttled", 0)),
            "deferred_last_minute": int(worker.get("deferred", 0)),
            "overflow_last_minute": int(worker.get("overflow", 0)),
        },
    }


def reset_governance_for_tests() -> None:
    _WORKSPACE_LIMITER.reset()
    _WORKER_FAIRNESS.reset()
