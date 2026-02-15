from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, Tuple

from flask import current_app, has_app_context


_RESULT_KEYS = {"equal", "diff", "error"}


def _normalize(value: str | None, default: str = "") -> str:
    normalized = str(value or "").strip().lower()
    return normalized or default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _confidence_enabled() -> bool:
    if has_app_context():
        return bool(current_app.config.get("ANALYTICS_CONFIDENCE_ENABLED", True))
    return _env_bool("ANALYTICS_CONFIDENCE_ENABLED", True)


def _confidence_min_samples() -> int:
    if has_app_context():
        try:
            return max(1, int(current_app.config.get("ANALYTICS_CONFIDENCE_MIN_SAMPLES", 100)))
        except (TypeError, ValueError):
            return 100
    return max(1, _env_int("ANALYTICS_CONFIDENCE_MIN_SAMPLES", 100))


def _confidence_max_diff_rate_percent() -> float:
    if has_app_context():
        try:
            return max(0.0, float(current_app.config.get("ANALYTICS_CONFIDENCE_MAX_DIFF_RATE_PERCENT", 0.5)))
        except (TypeError, ValueError):
            return 0.5
    return max(0.0, _env_float("ANALYTICS_CONFIDENCE_MAX_DIFF_RATE_PERCENT", 0.5))


def _confidence_window_minutes() -> int:
    if has_app_context():
        try:
            return max(1, int(current_app.config.get("ANALYTICS_CONFIDENCE_WINDOW_MINUTES", 60)))
        except (TypeError, ValueError):
            return 60
    return max(1, _env_int("ANALYTICS_CONFIDENCE_WINDOW_MINUTES", 60))


class ConfidenceController:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._samples: Dict[Tuple[str, str, int], Dict[str, int]] = {}

    @staticmethod
    def _minute_bucket(timestamp: float | None = None) -> int:
        ts = float(timestamp if timestamp is not None else time.time())
        return int(ts // 60)

    def record(self, workspace_id: str, section: str, result: str, *, timestamp: float | None = None) -> None:
        workspace = _normalize(workspace_id)
        section_key = _normalize(section, default="overview")
        result_key = _normalize(result)
        if not workspace or result_key not in _RESULT_KEYS:
            return

        minute = self._minute_bucket(timestamp)
        key = (workspace, section_key, minute)
        with self._lock:
            bucket = self._samples.setdefault(key, {"equal": 0, "diff": 0, "error": 0})
            bucket[result_key] = int(bucket.get(result_key, 0)) + 1

            # Lazy cleanup: keep a small tail to avoid unbounded growth.
            min_allowed = minute - 24 * 60
            for sample_key in list(self._samples.keys()):
                if sample_key[2] < min_allowed:
                    self._samples.pop(sample_key, None)

    def get(self, workspace_id: str, section: str, *, window_minutes: int, min_samples: int, threshold_percent: float) -> Dict[str, Any]:
        workspace = _normalize(workspace_id)
        section_key = _normalize(section, default="overview")
        if not workspace:
            return {
                "status": "insufficient_data",
                "diff_rate_percent": 0.0,
                "compare_count": 0,
                "threshold_percent": float(threshold_percent),
                "window_minutes": int(window_minutes),
            }

        now_bucket = self._minute_bucket()
        start_bucket = now_bucket - max(1, int(window_minutes)) + 1
        total_equal = 0
        total_diff = 0
        total_error = 0
        with self._lock:
            for (ws, sec, minute), counts in self._samples.items():
                if ws != workspace or sec != section_key:
                    continue
                if minute < start_bucket or minute > now_bucket:
                    continue
                total_equal += int(counts.get("equal", 0))
                total_diff += int(counts.get("diff", 0))
                total_error += int(counts.get("error", 0))

        compare_count = total_equal + total_diff + total_error
        diff_rate_percent = (float(total_diff) * 100.0 / float(compare_count)) if compare_count > 0 else 0.0
        if compare_count < max(1, int(min_samples)):
            status = "insufficient_data"
        elif diff_rate_percent <= float(threshold_percent):
            status = "healthy"
        else:
            status = "degraded"
        return {
            "status": status,
            "diff_rate_percent": round(diff_rate_percent, 4),
            "compare_count": int(compare_count),
            "threshold_percent": float(threshold_percent),
            "window_minutes": int(window_minutes),
        }

    def reset(self) -> None:
        with self._lock:
            self._samples.clear()


_CONFIDENCE = ConfidenceController()


def record_shadow_compare_result(workspace_id: str, section: str, result: str, *, timestamp: float | None = None) -> None:
    if not _confidence_enabled():
        return
    _CONFIDENCE.record(workspace_id, section, result, timestamp=timestamp)


def get_read_model_confidence(workspace_id: str, section: str) -> Dict[str, Any]:
    window = _confidence_window_minutes()
    min_samples = _confidence_min_samples()
    threshold = _confidence_max_diff_rate_percent()
    return _CONFIDENCE.get(
        workspace_id,
        section,
        window_minutes=window,
        min_samples=min_samples,
        threshold_percent=threshold,
    )


def reset_confidence_controller_for_tests() -> None:
    _CONFIDENCE.reset()
