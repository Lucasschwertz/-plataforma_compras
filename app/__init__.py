from flask import Flask, g, request, session

from app.config import Config
from app.db import close_db, init_db


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    _ensure_database_dir(app)
    _register_auth(app)
    _register_tenant(app)
    _register_template_context(app)
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
        session_tenant = (session.get("tenant_id") or "").strip()
        requested_workspace = (request.args.get("workspace_id") or "").strip()

        # Workspace switch via UI is allowed only for admin users.
        user_role = (session.get("user_role") or "buyer").strip().lower()
        if requested_workspace and requested_workspace != session_tenant:
            if not session_tenant:
                session["tenant_id"] = requested_workspace
                session_tenant = requested_workspace
            elif user_role == "admin" and _tenant_exists(requested_workspace):
                session["tenant_id"] = requested_workspace
                session_tenant = requested_workspace

        if session_tenant:
            g.tenant_id = session_tenant
            return

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


def _tenant_exists(tenant_id: str) -> bool:
    if not tenant_id:
        return False
    try:
        from app.db import get_read_db

        db = get_read_db()
        row = db.execute("SELECT 1 FROM tenants WHERE id = ? LIMIT 1", (tenant_id,)).fetchone()
        return bool(row)
    except Exception:
        return False


def _register_template_context(app: Flask) -> None:
    @app.context_processor
    def inject_ui_context():
        from app.db import get_read_db
        from app.tenant import DEFAULT_TENANT_ID, current_tenant_id

        session_tenant = (session.get("tenant_id") or "").strip()
        header_tenant = (current_tenant_id() or "").strip()
        workspace_id = session_tenant or header_tenant or DEFAULT_TENANT_ID

        role = (session.get("user_role") or "buyer").strip().lower()
        if role not in {"buyer", "admin", "approver", "manager", "supplier"}:
            role = "buyer"

        workspace_name = f"Empresa {workspace_id}"
        workspace_options = [{"id": workspace_id, "name": workspace_name}]

        try:
            db = get_read_db()
            if role == "admin":
                tenant_rows = db.execute(
                    "SELECT id, name FROM tenants ORDER BY name, id",
                ).fetchall()
                if tenant_rows:
                    workspace_options = [{"id": str(row["id"]), "name": str(row["name"])} for row in tenant_rows]
                    selected = next((opt for opt in workspace_options if opt["id"] == workspace_id), None)
                    if selected:
                        workspace_name = selected["name"]
            else:
                row = db.execute(
                    "SELECT id, name FROM tenants WHERE id = ?",
                    (workspace_id,),
                ).fetchone()
                if row and row["name"]:
                    workspace_name = str(row["name"])
                    workspace_options = [{"id": str(row["id"]), "name": workspace_name}]
        except Exception:
            # UI resiliente: nunca quebrar render por falha de lookup.
            pass

        return {
            "role": role,
            "workspace_id": workspace_id,
            "workspace_name": workspace_name,
            "workspace_options": workspace_options,
            "workspace_user_name": session.get("display_name") or "usuario",
        }


def _register_health(app: Flask) -> None:
    @app.route("/health")
    def health():
        db_path = app.config.get("DB_PATH") or "unknown"
        backend = "postgres" if str(db_path).startswith("postgres") else "sqlite"
        return {"status": "ok", "db": backend, "env": app.config.get("ENV", "unknown")}, 200
