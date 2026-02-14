from __future__ import annotations

import time
from collections import deque
from threading import Lock


class ErpCircuitBreaker:
    def __init__(self) -> None:
        self._lock = Lock()
        self._enabled = True
        self._error_rate_threshold = 0.6
        self._min_samples = 5
        self._window_seconds = 120
        self._open_seconds = 30
        self._half_open_max_calls = 1

        self._state = "closed"
        self._opened_at = 0.0
        self._half_open_calls = 0
        self._events: deque[tuple[float, bool]] = deque()

    @staticmethod
    def _clamp_float(value, default: float, minimum: float, maximum: float) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _clamp_int(value, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, min(maximum, parsed))

    def configure(
        self,
        *,
        enabled: bool,
        error_rate_threshold: float,
        min_samples: int,
        window_seconds: int,
        open_seconds: int,
        half_open_max_calls: int,
    ) -> None:
        with self._lock:
            self._enabled = bool(enabled)
            self._error_rate_threshold = self._clamp_float(error_rate_threshold, 0.6, 0.05, 1.0)
            self._min_samples = self._clamp_int(min_samples, 5, 1, 1000)
            self._window_seconds = self._clamp_int(window_seconds, 120, 5, 3600)
            self._open_seconds = self._clamp_int(open_seconds, 30, 1, 3600)
            self._half_open_max_calls = self._clamp_int(half_open_max_calls, 1, 1, 100)
            if not self._enabled:
                self._state = "closed"
                self._opened_at = 0.0
                self._half_open_calls = 0
                self._events.clear()

    def _prune(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def _open(self, now: float) -> None:
        self._state = "open"
        self._opened_at = now
        self._half_open_calls = 0

    def _close(self) -> None:
        self._state = "closed"
        self._opened_at = 0.0
        self._half_open_calls = 0
        self._events.clear()

    def _failure_rate(self, now: float) -> tuple[int, int, float]:
        self._prune(now)
        samples = len(self._events)
        if samples <= 0:
            return 0, 0, 0.0
        failures = sum(1 for _ts, success in self._events if not success)
        return samples, failures, float(failures) / float(samples)

    def before_call(self) -> tuple[bool, str]:
        now = time.monotonic()
        with self._lock:
            if not self._enabled:
                return True, "disabled"

            if self._state == "open":
                if (now - self._opened_at) >= self._open_seconds:
                    self._state = "half_open"
                    self._half_open_calls = 0
                else:
                    return False, "open"

            if self._state == "half_open":
                if self._half_open_calls >= self._half_open_max_calls:
                    return False, "half_open"
                self._half_open_calls += 1
                return True, "half_open"

            return True, "closed"

    def record_success(self) -> None:
        now = time.monotonic()
        with self._lock:
            if not self._enabled:
                return
            if self._state == "half_open":
                self._close()
                return
            self._events.append((now, True))
            self._prune(now)

    def record_failure(self) -> None:
        now = time.monotonic()
        with self._lock:
            if not self._enabled:
                return

            if self._state == "half_open":
                self._events.append((now, False))
                self._prune(now)
                self._open(now)
                return

            if self._state == "open":
                return

            self._events.append((now, False))
            samples, _failures, failure_rate = self._failure_rate(now)
            if samples >= self._min_samples and failure_rate >= self._error_rate_threshold:
                self._open(now)

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self._lock:
            samples, failures, failure_rate = self._failure_rate(now)
            opened_seconds_ago = 0.0
            if self._state == "open" and self._opened_at > 0.0:
                opened_seconds_ago = max(0.0, now - self._opened_at)
            return {
                "state": self._state,
                "enabled": self._enabled,
                "samples": samples,
                "failures": failures,
                "failure_rate": round(failure_rate, 4),
                "opened_seconds_ago": round(opened_seconds_ago, 2),
                "half_open_calls": int(self._half_open_calls),
            }

    def reset_for_tests(self) -> None:
        with self._lock:
            self._enabled = True
            self._state = "closed"
            self._opened_at = 0.0
            self._half_open_calls = 0
            self._events.clear()


_ERP_CIRCUIT_BREAKER = ErpCircuitBreaker()


def get_erp_circuit_breaker() -> ErpCircuitBreaker:
    return _ERP_CIRCUIT_BREAKER


def erp_circuit_snapshot() -> dict:
    return _ERP_CIRCUIT_BREAKER.snapshot()


def reset_erp_circuit_breaker_for_tests() -> None:
    _ERP_CIRCUIT_BREAKER.reset_for_tests()
