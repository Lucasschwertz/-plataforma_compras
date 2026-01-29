import os


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


class Config:
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    DATABASE_DIR = os.path.join(BASE_DIR, "database")
    DB_PATH = os.path.join(DATABASE_DIR, "plataforma_compras.db")

    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-plataforma-compras")
    AUTH_ENABLED = _bool_env("AUTH_ENABLED", True)
    APP_USERS = os.environ.get("APP_USERS", "admin@demo.com:admin123:tenant-demo")
    SYNC_SCHEDULER_ENABLED = _bool_env("SYNC_SCHEDULER_ENABLED", True)
    SYNC_SCHEDULER_INTERVAL_SECONDS = _int_env("SYNC_SCHEDULER_INTERVAL_SECONDS", 120)
    SYNC_SCHEDULER_MIN_BACKOFF_SECONDS = _int_env("SYNC_SCHEDULER_MIN_BACKOFF_SECONDS", 30)
    SYNC_SCHEDULER_MAX_BACKOFF_SECONDS = _int_env("SYNC_SCHEDULER_MAX_BACKOFF_SECONDS", 600)
    SYNC_SCHEDULER_LIMIT = _int_env("SYNC_SCHEDULER_LIMIT", 200)
    SYNC_SCHEDULER_SCOPES = os.environ.get(
        "SYNC_SCHEDULER_SCOPES",
        "supplier,purchase_request,purchase_order,receipt",
    )
    ERP_MODE = os.environ.get("ERP_MODE", "mock")
    ERP_BASE_URL = os.environ.get("ERP_BASE_URL")
    ERP_TOKEN = os.environ.get("ERP_TOKEN")
    ERP_API_KEY = os.environ.get("ERP_API_KEY")
    ERP_ENTITY_ENDPOINTS = os.environ.get("ERP_ENTITY_ENDPOINTS")
    ERP_TIMEOUT_SECONDS = _int_env("ERP_TIMEOUT_SECONDS", 20)
    ERP_VERIFY_SSL = _bool_env("ERP_VERIFY_SSL", True)
    ERP_RETRY_ATTEMPTS = _int_env("ERP_RETRY_ATTEMPTS", 2)
    ERP_RETRY_BACKOFF_MS = _int_env("ERP_RETRY_BACKOFF_MS", 300)
    ERP_RETRY_ON_POST = _bool_env("ERP_RETRY_ON_POST", False)
    RFQ_SLA_DAYS = _int_env("RFQ_SLA_DAYS", 5)
