from __future__ import annotations

import re
from typing import Any, Dict

from app.ui_strings import error_message


class AppError(Exception):
    default_code = "system_error"
    default_message_key = "unexpected_error"
    default_http_status = 500
    default_critical = True

    def __init__(
        self,
        code: str | None = None,
        message_key: str | None = None,
        http_status: int | None = None,
        critical: bool | None = None,
        details: str | None = None,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        self.code = (code or self.default_code).strip()
        self.message_key = (message_key or self.default_message_key).strip()
        self.http_status = int(http_status or self.default_http_status)
        self.critical = bool(self.default_critical if critical is None else critical)
        self.details = (details or "").strip() or None
        self.payload = dict(payload or {})
        super().__init__(self.details or self.code)

    def user_message(self) -> str:
        fallback = error_message("unexpected_error", "Nao foi possivel concluir a operacao.")
        return error_message(self.message_key, fallback)

    def to_response_payload(self, request_id: str) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "error": self.code,
            "message": self.user_message(),
            "request_id": request_id,
        }
        if self.payload:
            payload.update(self.payload)
        return payload


class UserActionError(AppError):
    default_code = "action_invalid"
    default_message_key = "action_invalid"
    default_http_status = 400
    default_critical = False


class ValidationError(UserActionError):
    default_code = "validation_error"
    default_message_key = "status_invalid"
    default_http_status = 400
    default_critical = False


class PermissionError(UserActionError):
    default_code = "permission_denied"
    default_message_key = "permission_denied"
    default_http_status = 403
    default_critical = False


class IntegrationError(AppError):
    default_code = "integration_error"
    default_message_key = "erp_temporarily_unavailable"
    default_http_status = 502
    default_critical = False


class SystemError(AppError):
    default_code = "system_error"
    default_message_key = "unexpected_error"
    default_http_status = 500
    default_critical = True


_ERP_HTTP_CODE_PATTERN = re.compile(r"erp http\s+(\d{3})", re.IGNORECASE)


def classify_erp_failure(details: str | None) -> tuple[str, str, int]:
    normalized = (details or "").strip().lower()
    code_match = _ERP_HTTP_CODE_PATTERN.search(normalized)
    if code_match:
        http_code = int(code_match.group(1))
        if 400 <= http_code < 500 and http_code not in {408, 429}:
            return ("erp_order_rejected", "erp_order_rejected", 422)

    rejection_markers = ("recusou", "rejeitou", "invalid", "invalido", "rejected")
    if any(marker in normalized for marker in rejection_markers):
        return ("erp_order_rejected", "erp_order_rejected", 422)

    return ("erp_temporarily_unavailable", "erp_temporarily_unavailable", 502)
