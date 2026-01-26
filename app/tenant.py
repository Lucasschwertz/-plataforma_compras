from flask import session, g


DEFAULT_TENANT_ID = "tenant-demo"


def current_tenant_id() -> str | None:
    return session.get("tenant_id") or getattr(g, "tenant_id", None)
