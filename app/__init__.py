from flask import Flask, g, request

from app.config import Config
from app.db import close_db, init_db


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    _ensure_database_dir(app)
    _register_auth(app)
    _register_tenant(app)
    _register_blueprints(app)
    _register_health(app)

    with app.app_context():
        init_db()

    _register_scheduler(app)
    app.teardown_appcontext(close_db)
    return app


def _ensure_database_dir(app: Flask) -> None:
    database_dir = app.config.get("DATABASE_DIR")
    if database_dir:
        import os

        os.makedirs(database_dir, exist_ok=True)


def _register_blueprints(app: Flask) -> None:
    from app.routes.home_routes import home_bp
    from app.routes.procurement_routes import procurement_bp

    app.register_blueprint(home_bp)
    app.register_blueprint(procurement_bp)


def _register_auth(app: Flask) -> None:
    from app.auth import register_auth

    register_auth(app)


def _register_scheduler(app: Flask) -> None:
    from app.scheduler import start_sync_scheduler

    start_sync_scheduler(app)


def _register_tenant(app: Flask) -> None:
    @app.before_request
    def load_company() -> None:
        # Prototype: allow overriding tenant via header for testing.
        header_tenant = (request.headers.get("X-Tenant-Id") or "").strip()
        if header_tenant:
            g.tenant_id = header_tenant
            return

        header_company = (request.headers.get("X-Company-Id") or "").strip()
        if header_company:
            g.tenant_id = f"tenant-{header_company}"
            return

        g.tenant_id = None


def _register_health(app: Flask) -> None:
    @app.route("/health")
    def health():
        db_path = app.config.get("DB_PATH") or "unknown"
        backend = "postgres" if str(db_path).startswith("postgres") else "sqlite"
        return {"status": "ok", "db": backend, "env": app.config.get("ENV", "unknown")}, 200
