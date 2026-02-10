from flask import session, g


DEFAULT_TENANT_ID = "tenant-demo"


def current_tenant_id() -> str | None:
    return normalize_tenant_id(session.get("tenant_id")) or normalize_tenant_id(getattr(g, "tenant_id", None))


def normalize_tenant_id(value: str | None) -> str | None:
    tenant_id = str(value or "").strip()
    return tenant_id or None


def scoped_tenant_id(value: str | None = None) -> str:
    return normalize_tenant_id(value) or current_tenant_id() or DEFAULT_TENANT_ID
