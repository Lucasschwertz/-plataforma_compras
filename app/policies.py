from __future__ import annotations

from typing import Iterable, Set

from flask import session

from app.errors import PermissionError as AppPermissionError


VALID_ROLES: Set[str] = {"buyer", "admin", "approver", "manager", "supplier"}


def normalize_role(role: str | None, default: str = "buyer") -> str:
    normalized = str(role or "").strip().lower()
    if normalized in VALID_ROLES:
        return normalized
    return default if default in VALID_ROLES else ""


def current_role() -> str:
    return normalize_role(session.get("user_role"), default="buyer")


def normalize_allowed_roles(roles: Iterable[str]) -> Set[str]:
    allowed: Set[str] = set()
    for role in roles:
        normalized = normalize_role(role, default="")
        if normalized:
            allowed.add(normalized)
    return allowed


def has_any_role(role: str | None, allowed_roles: Iterable[str]) -> bool:
    normalized_role = normalize_role(role, default="buyer")
    allowed = normalize_allowed_roles(allowed_roles)
    return not allowed or normalized_role in allowed


def require_roles(*allowed_roles: str, role: str | None = None) -> str:
    normalized_role = normalize_role(role, default="buyer") if role is not None else current_role()
    if has_any_role(normalized_role, allowed_roles):
        return normalized_role
    raise AppPermissionError(
        code="permission_denied",
        message_key="permission_denied",
        http_status=403,
        critical=False,
    )
