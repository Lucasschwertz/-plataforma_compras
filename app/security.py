from __future__ import annotations

import secrets
import threading
import time
from typing import Dict, Tuple

from flask import current_app, request, session

from app.errors import ValidationError
from app.ui_strings import error_message


CSRF_SESSION_KEY = "_csrf_token"
CSRF_FORM_FIELD = "csrf_token"
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}
_FORM_CONTENT_TYPES = {"application/x-www-form-urlencoded", "multipart/form-data"}


def csrf_token() -> str:
    token = str(session.get(CSRF_SESSION_KEY) or "").strip()
    if token:
        return token
    token = secrets.token_urlsafe(24)
    session[CSRF_SESSION_KEY] = token
    return token


def validate_csrf_token(token: str | None) -> bool:
    if not bool(current_app.config.get("CSRF_ENABLED", True)):
        return True
    expected = str(session.get(CSRF_SESSION_KEY) or "").strip()
    provided = str(token or "").strip()
    if not expected or not provided:
        return False
    return secrets.compare_digest(expected, provided)


def enforce_form_csrf() -> None:
    if request.method in _SAFE_METHODS:
        return
    if request.path.startswith("/api/"):
        return
    if request.path in {"/login", "/register"}:
        return
    if not bool(current_app.config.get("CSRF_ENABLED", True)):
        return
    content_type = str(request.mimetype or "").strip().lower()
    if content_type not in _FORM_CONTENT_TYPES:
        return
    if validate_csrf_token(request.form.get(CSRF_FORM_FIELD)):
        return
    raise ValidationError(
        code="csrf_invalid",
        message_key="csrf_invalid",
        http_status=400,
        critical=False,
    )


class SimpleRateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: Dict[str, Tuple[float, int]] = {}

    def allow(self, key: str, *, limit: int, window_seconds: int) -> tuple[bool, int]:
        now = time.monotonic()
        with self._lock:
            start, count = self._entries.get(key, (now, 0))
            if now - start >= window_seconds:
                start = now
                count = 0
            count += 1
            self._entries[key] = (start, count)
            if len(self._entries) > 10_000:
                cutoff = now - (window_seconds * 2)
                self._entries = {
                    cached_key: value
                    for cached_key, value in self._entries.items()
                    if value[0] >= cutoff
                }
            retry_after = max(0, int(window_seconds - (now - start)))
            return count <= limit, retry_after

    def reset(self) -> None:
        with self._lock:
            self._entries.clear()


_RATE_LIMITER = SimpleRateLimiter()


def _rate_limit_key() -> str:
    user = str(session.get("user_email") or "").strip().lower() or "anon"
    ip = str(request.remote_addr or "").strip() or "unknown"
    route = request.url_rule.rule if request.url_rule else request.path
    return f"{ip}|{user}|{request.method}|{route}"


def enforce_rate_limit():
    if not bool(current_app.config.get("RATE_LIMIT_ENABLED", True)):
        return None
    if request.method == "OPTIONS":
        return None
    if request.path.startswith("/static/"):
        return None

    window_seconds = max(1, int(current_app.config.get("RATE_LIMIT_WINDOW_SECONDS", 60) or 60))
    max_requests = max(1, int(current_app.config.get("RATE_LIMIT_MAX_REQUESTS", 300) or 300))
    allowed, retry_after = _RATE_LIMITER.allow(
        _rate_limit_key(),
        limit=max_requests,
        window_seconds=window_seconds,
    )
    if allowed:
        return None

    if request.path.startswith("/api/"):
        raise ValidationError(
            code="rate_limit_exceeded",
            message_key="rate_limit_exceeded",
            http_status=429,
            critical=False,
            payload={"retry_after": retry_after},
        )
    return (
        error_message("rate_limit_exceeded", "Muitas requisicoes. Tente novamente em instantes."),
        429,
        {"Retry-After": str(retry_after)},
    )


def apply_security_headers(response):
    if not bool(current_app.config.get("SECURITY_HEADERS_ENABLED", True)):
        return response
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), geolocation=(), microphone=()",
    )
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    response.headers.setdefault(
        "Content-Security-Policy",
        "; ".join(
            [
                "default-src 'self'",
                "img-src 'self' data:",
                "style-src 'self' 'unsafe-inline'",
                "script-src 'self' 'unsafe-inline'",
                "font-src 'self' data:",
                "connect-src 'self'",
                "frame-ancestors 'none'",
                "base-uri 'self'",
                "form-action 'self'",
            ]
        ),
    )
    if request.is_secure:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


def reset_rate_limiter_for_tests() -> None:
    _RATE_LIMITER.reset()
