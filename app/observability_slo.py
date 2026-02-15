from __future__ import annotations

from typing import Dict, List

from app.observability import http_metrics_snapshot, reset_metrics_for_tests


def _normalize_prefix(value: str | None) -> str:
    return str(value or "").strip()


def _route_matches(route: str, route_prefix: str) -> bool:
    if not route_prefix:
        return True
    return str(route or "").startswith(route_prefix)


def _sort_bucket_labels(labels: List[str]) -> List[str]:
    finite = []
    has_inf = False
    for label in labels:
        if str(label) == "+Inf":
            has_inf = True
            continue
        try:
            finite.append((float(label), str(label)))
        except (TypeError, ValueError):
            continue
    finite.sort(key=lambda item: item[0])
    ordered = [label for _value, label in finite]
    if has_inf:
        ordered.append("+Inf")
    return ordered


def get_p95_ms(route_prefix: str | None = None) -> float:
    prefix = _normalize_prefix(route_prefix)
    snapshot = http_metrics_snapshot()
    histograms = list(snapshot.get("http_request_duration_ms") or [])

    total_count = 0
    cumulative_by_bucket: Dict[str, int] = {}
    for histogram in histograms:
        route = str(histogram.get("route") or "")
        if not _route_matches(route, prefix):
            continue
        count = int(histogram.get("count") or 0)
        if count <= 0:
            continue
        total_count += count
        buckets = dict(histogram.get("buckets") or {})
        for label, value in buckets.items():
            cumulative_by_bucket[str(label)] = int(cumulative_by_bucket.get(str(label), 0)) + int(value or 0)

    if total_count <= 0:
        return 0.0

    target = max(1, int(round(total_count * 0.95)))
    ordered_labels = _sort_bucket_labels(list(cumulative_by_bucket.keys()))
    last_finite = 0.0
    for label in ordered_labels:
        cumulative = int(cumulative_by_bucket.get(label, 0) or 0)
        if label == "+Inf":
            return float(last_finite)
        try:
            bound = float(label)
        except (TypeError, ValueError):
            continue
        last_finite = bound
        if cumulative >= target:
            return bound
    return float(last_finite)


def get_error_rate_percent(route_prefix: str | None = None) -> float:
    prefix = _normalize_prefix(route_prefix)
    snapshot = http_metrics_snapshot()
    totals = list(snapshot.get("http_request_total") or [])

    total_requests = 0
    total_errors = 0
    for sample in totals:
        route = str(sample.get("route") or "")
        if not _route_matches(route, prefix):
            continue
        value = int(sample.get("value") or 0)
        if value <= 0:
            continue
        total_requests += value
        try:
            status_code = int(sample.get("status") or 0)
        except (TypeError, ValueError):
            status_code = 0
        if status_code >= 400:
            total_errors += value

    if total_requests <= 0:
        return 0.0
    return round((float(total_errors) * 100.0) / float(total_requests), 4)


def reset_http_metrics() -> None:
    reset_metrics_for_tests()

