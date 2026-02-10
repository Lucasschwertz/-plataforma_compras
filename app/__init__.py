import os
import uuid

from flask import Flask, g, jsonify, request, session
from werkzeug.exceptions import HTTPException

from app.config import Config
from app.db import close_db, init_db
from app.db_migrations import register_db_cli
from app.procurement.critical_actions import CRITICAL_ACTIONS
from app.procurement.flow_policy import build_process_steps
from app.ui_strings import confirm_message, get_ui_text, template_bundle


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    _ensure_database_dir(app)
    _register_error_handlers(app)
    _register_auth(app)
    _register_tenant(app)
    _register_template_context(app)
    _register_blueprints(app)
    _register_health(app)
    register_db_cli(app)
    _maybe_init_schema(app)

    _register_scheduler(app)
    app.teardown_appcontext(close_db)
    return app


def _ensure_database_dir(app: Flask) -> None:
    database_dir = app.config.get("DATABASE_DIR")
    if database_dir:
        import os

        os.makedirs(database_dir, exist_ok=True)


def _maybe_init_schema(app: Flask) -> None:
    auto_init = bool(app.config.get("DB_AUTO_INIT", False))
    if app.testing:
        # Testes continuam isolados e autodidata sem depender de migration externa.
        auto_init = True
    if not auto_init:
        return

    flask_env = (os.environ.get("FLASK_ENV", "development") or "development").strip().lower()
    if not app.testing and flask_env != "development":
        app.logger.warning("DB_AUTO_INIT ignorado fora de development.")
        return

    with app.app_context():
        init_db()


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


def _register_error_handlers(app: Flask) -> None:
    from app.erp_client import ErpError
    from app.errors import AppError, IntegrationError, SystemError, classify_erp_failure

    def _resolve_request_id() -> str:
        request_id = (getattr(g, "request_id", None) or "").strip()
        if request_id:
            return request_id
        incoming = (request.headers.get("X-Request-Id") or "").strip()
        request_id = incoming or str(uuid.uuid4())
        g.request_id = request_id
        return request_id

    @app.before_request
    def _ensure_request_id() -> None:
        _resolve_request_id()

    @app.after_request
    def _append_request_id(response):
        response.headers["X-Request-Id"] = _resolve_request_id()
        return response

    def _log_error(error: AppError, request_id: str) -> None:
        log_method = app.logger.error if error.critical else app.logger.warning
        log_method(
            "request_id=%s code=%s status=%s message_key=%s details=%s path=%s method=%s",
            request_id,
            error.code,
            error.http_status,
            error.message_key,
            error.details,
            request.path,
            request.method,
            exc_info=error.critical,
        )

    @app.errorhandler(AppError)
    def _handle_app_error(exc: AppError):
        request_id = _resolve_request_id()
        _log_error(exc, request_id)
        return jsonify(exc.to_response_payload(request_id)), exc.http_status

    @app.errorhandler(ErpError)
    def _handle_erp_error(exc: ErpError):
        request_id = _resolve_request_id()
        code, message_key, http_status = classify_erp_failure(str(exc))
        mapped = IntegrationError(
            code=code,
            message_key=message_key,
            http_status=http_status,
            critical=False,
            details=str(exc),
        )
        _log_error(mapped, request_id)
        return jsonify(mapped.to_response_payload(request_id)), mapped.http_status

    @app.errorhandler(Exception)
    def _handle_unexpected(exc: Exception):
        if isinstance(exc, HTTPException):
            return exc

        request_id = _resolve_request_id()
        mapped = SystemError(
            code="unexpected_error",
            message_key="unexpected_error",
            http_status=500,
            critical=True,
            details=str(exc),
        )
        app.logger.exception(
            "request_id=%s code=%s path=%s method=%s",
            request_id,
            mapped.code,
            request.path,
            request.method,
        )
        return jsonify(mapped.to_response_payload(request_id)), mapped.http_status


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

        ui_bundle = template_bundle()
        critical_actions_bundle = {}
        for action_key, meta in CRITICAL_ACTIONS.items():
            confirm_key = meta.get("confirm_message_key") or action_key
            impact_key = meta.get("impact_text_key") or f"impact.{action_key}"
            critical_actions_bundle[action_key] = {
                "action_key": action_key,
                "confirm_key": confirm_key,
                "confirm_message": confirm_message(confirm_key, confirm_key),
                "impact_key": impact_key,
                "impact": get_ui_text(impact_key, impact_key),
            }
        frontend_bundle = dict(ui_bundle.get("ui_frontend_bundle") or {})
        frontend_bundle["critical_actions"] = critical_actions_bundle
        ui_bundle["ui_frontend_bundle"] = frontend_bundle

        return {
            "role": role,
            "workspace_id": workspace_id,
            "workspace_name": workspace_name,
            "workspace_options": workspace_options,
            "workspace_user_name": session.get("display_name") or "usuario",
            "ui_text": get_ui_text,
            "ui_process_steps": build_process_steps,
            "ui_critical_actions": critical_actions_bundle,
            **ui_bundle,
        }


def _register_health(app: Flask) -> None:
    @app.route("/health")
    def health():
        db_path = app.config.get("DB_PATH") or "unknown"
        backend = "postgres" if str(db_path).startswith("postgres") else "sqlite"
        return {"status": "ok", "db": backend, "env": app.config.get("ENV", "unknown")}, 200
