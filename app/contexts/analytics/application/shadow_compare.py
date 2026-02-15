from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from numbers import Number
from typing import Any, Dict, List

from app.core.governance import get_workspace_limiter


_VOLATILE_KEYS = {"generated_at", "duration_ms", "source", "request_id", "confidence_status"}
_SUMMARY_KEYS = ("kpis", "charts", "drilldown")
_LOG_LIMIT_LOCK = threading.Lock()
_LOG_LIMIT_BY_MINUTE: Dict[int, int] = {}
_MISSING = object()


def _normalize_string(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _is_meta_timestamp(path: tuple[str, ...], key: str) -> bool:
    parent = path[-1] if path else ""
    lowered_key = key.lower()
    return parent == "meta" and (lowered_key.endswith("_at") or lowered_key.endswith("_timestamp") or lowered_key == "timestamp")


def _should_drop_key(path: tuple[str, ...], key: str) -> bool:
    lowered = str(key or "").strip().lower()
    if lowered in _VOLATILE_KEYS:
        return True
    if _is_meta_timestamp(path, lowered):
        return True
    return False


def _numeric_value(value: Number) -> int | float:
    if isinstance(value, bool):
        return value
    try:
        numeric = round(float(value), 2)
    except (TypeError, ValueError):
        return 0.0
    rounded_int = int(round(numeric))
    if abs(numeric - float(rounded_int)) < 1e-9:
        return rounded_int
    return numeric


def _normalize_list_item(value: Any, path: tuple[str, ...]) -> Any:
    return _normalize_value(value, path + ("[]",))


def _list_sort_key(value: Any) -> tuple[str, str]:
    if isinstance(value, dict):
        key = _normalize_string(value.get("key", ""))
        label = _normalize_string(value.get("label", ""))
        if key or label:
            return ("dict-key-label", f"{key.lower()}::{label.lower()}")
    serialized = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return ("json", serialized)


def _normalize_value(value: Any, path: tuple[str, ...]) -> Any:
    if isinstance(value, dict):
        normalized: Dict[str, Any] = {}
        for raw_key in sorted(value.keys(), key=lambda item: _normalize_string(str(item)).lower()):
            key = _normalize_string(str(raw_key))
            if not key:
                continue
            if _should_drop_key(path, key):
                continue
            normalized[key] = _normalize_value(value[raw_key], path + (key,))
        return normalized

    if isinstance(value, list):
        normalized_list = [_normalize_list_item(item, path) for item in value]
        return sorted(normalized_list, key=_list_sort_key)

    if isinstance(value, str):
        return _normalize_string(value)

    if isinstance(value, Number):
        return _numeric_value(value)

    return value


def normalize_payload(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    source = dict(payload or {})
    normalized = _normalize_value(source, tuple())
    return normalized if isinstance(normalized, dict) else {}


def _path_join(base: str, part: str) -> str:
    if not base:
        return part
    if part.startswith("["):
        return f"{base}{part}"
    return f"{base}.{part}"


def _summary_key(path: str) -> str | None:
    for key in _SUMMARY_KEYS:
        if path == key or path.startswith(f"{key}.") or path.startswith(f"{key}["):
            return key
    return None


def _append_diff(
    diffs: List[Dict[str, Any]],
    summary: Dict[str, int],
    path: str,
    value_a: Any,
    value_b: Any,
    *,
    max_diffs: int,
) -> int:
    section = _summary_key(path)
    if section:
        summary[section] = int(summary.get(section, 0)) + 1
    if len(diffs) < max_diffs:
        diffs.append(
            {
                "path": path or "$",
                "a": value_a if value_a is not _MISSING else None,
                "b": value_b if value_b is not _MISSING else None,
            }
        )
    return 1


def _diff_values(
    value_a: Any,
    value_b: Any,
    *,
    path: str,
    diffs: List[Dict[str, Any]],
    summary: Dict[str, int],
    max_diffs: int,
) -> int:
    if isinstance(value_a, dict) and isinstance(value_b, dict):
        count = 0
        keys = sorted(set(value_a.keys()) | set(value_b.keys()))
        for key in keys:
            next_path = _path_join(path, key)
            count += _diff_values(
                value_a.get(key, _MISSING),
                value_b.get(key, _MISSING),
                path=next_path,
                diffs=diffs,
                summary=summary,
                max_diffs=max_diffs,
            )
        return count

    if isinstance(value_a, list) and isinstance(value_b, list):
        count = 0
        total = max(len(value_a), len(value_b))
        for index in range(total):
            next_path = _path_join(path, f"[{index}]")
            left = value_a[index] if index < len(value_a) else _MISSING
            right = value_b[index] if index < len(value_b) else _MISSING
            count += _diff_values(
                left,
                right,
                path=next_path,
                diffs=diffs,
                summary=summary,
                max_diffs=max_diffs,
            )
        return count

    if value_a is _MISSING or value_b is _MISSING:
        return _append_diff(diffs, summary, path, value_a, value_b, max_diffs=max_diffs)

    if value_a != value_b:
        return _append_diff(diffs, summary, path, value_a, value_b, max_diffs=max_diffs)

    return 0


def diff_payload(a: Dict[str, Any] | None, b: Dict[str, Any] | None, *, max_diffs: int = 20) -> Dict[str, Any]:
    normalized_a = normalize_payload(a)
    normalized_b = normalize_payload(b)

    diffs: List[Dict[str, Any]] = []
    summary = {key: 0 for key in _SUMMARY_KEYS}
    total = _diff_values(
        normalized_a,
        normalized_b,
        path="",
        diffs=diffs,
        summary=summary,
        max_diffs=max(1, int(max_diffs)),
    )
    return {
        "equal": total == 0,
        "diffs": diffs,
        "summary": summary,
    }


def hash_payload(payload: Dict[str, Any] | None) -> str:
    normalized = normalize_payload(payload)
    encoded = json.dumps(normalized, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def should_emit_diff_log(max_logs_per_minute: int) -> bool:
    limit = max(0, int(max_logs_per_minute or 0))
    if limit <= 0:
        return False
    minute_key = int(time.time() // 60)
    with _LOG_LIMIT_LOCK:
        for key in list(_LOG_LIMIT_BY_MINUTE.keys()):
            if key < minute_key:
                _LOG_LIMIT_BY_MINUTE.pop(key, None)
        current = int(_LOG_LIMIT_BY_MINUTE.get(minute_key, 0))
        if current >= limit:
            return False
        _LOG_LIMIT_BY_MINUTE[minute_key] = current + 1
        return True


def should_skip_shadow_compare(workspace_id: str | None, *, disable_when_degraded: bool) -> bool:
    if not bool(disable_when_degraded):
        return False
    try:
        limiter = get_workspace_limiter()
        return bool(limiter.is_degraded(workspace_id))
    except Exception:
        return False


def _reset_shadow_compare_log_limiter_for_tests() -> None:
    with _LOG_LIMIT_LOCK:
        _LOG_LIMIT_BY_MINUTE.clear()
