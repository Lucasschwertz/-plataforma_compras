from __future__ import annotations

from flask import Blueprint, current_app, redirect, render_template, request, session, url_for

from app.contexts.auth.application.service import AuthService
from app.db import get_db
from app.domain.contracts import AuthLoginInput, AuthRegisterInput
from app.errors import PermissionError as AppPermissionError
from app.errors import ValidationError
from app.policies import normalize_role
from app.security import validate_csrf_token
from app.ui_strings import error_message


auth_bp = Blueprint("auth", __name__)
_auth_service = AuthService()


def _err(key: str, fallback: str | None = None) -> str:
    return error_message(key, fallback)


def register_auth(app) -> None:
    app.register_blueprint(auth_bp)

    @app.before_request
    def _require_login():
        if not app.config.get("AUTH_ENABLED", True):
            return None
        if app.config.get("TESTING"):
            return None

        path = request.path or "/"
        if path.startswith("/static/"):
            return None
        if path in {"/login", "/logout", "/register", "/health", "/metrics"}:
            return None
        if path.startswith("/fornecedor/convite/") or path.startswith("/api/fornecedor/convite/"):
            return None

        if session.get("user_email"):
            return None

        if path.startswith("/api/"):
            raise AppPermissionError(
                code="auth_required",
                message_key="auth_required",
                http_status=401,
                critical=False,
            )

        next_url = request.full_path or "/"
        if next_url.endswith("?"):
            next_url = next_url[:-1]
        return redirect(url_for("auth.login", next=next_url))


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_email"):
        return redirect(_safe_next_url() or url_for("home.home"))

    error = None
    if request.method == "POST":
        if not validate_csrf_token(request.form.get("csrf_token")):
            error = _err("csrf_invalid")
        else:
            email = (request.form.get("email") or "").strip().lower()
            password = request.form.get("password") or ""
            db = get_db()
            user = _auth_service.login(
                db,
                AuthLoginInput(email=email, password=password),
                current_app.config.get("APP_USERS"),
            )
            if user:
                session["user_email"] = user.email
                session["display_name"] = user.display_name
                session["tenant_id"] = user.tenant_id
                session["user_role"] = normalize_role(user.role, default="buyer")
                return redirect(_safe_next_url() or url_for("home.home"))

            error = _err("auth_invalid_credentials")

    return render_template("login.html", error=error)


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if session.get("user_email"):
        return redirect(url_for("home.home"))

    error = None
    if request.method == "POST":
        if not validate_csrf_token(request.form.get("csrf_token")):
            error = _err("csrf_invalid")
        else:
            email = (request.form.get("email") or "").strip().lower()
            password = request.form.get("password") or ""
            display_name = (request.form.get("display_name") or "").strip() or None
            company_name = (request.form.get("company_name") or "").strip()

            if not email or not password:
                error = _err("auth_missing_credentials")
            else:
                db = get_db()
                try:
                    user = _auth_service.register(
                        db,
                        AuthRegisterInput(
                            email=email,
                            password=password,
                            display_name=display_name,
                            company_name=company_name or None,
                        ),
                    )
                except ValidationError as exc:
                    error = exc.user_message()
                else:
                    db.commit()
                    session["user_email"] = user.email
                    session["display_name"] = user.display_name
                    session["tenant_id"] = user.tenant_id
                    session["user_role"] = normalize_role(user.role, default="buyer")
                    return redirect(url_for("home.home"))

    return render_template("register.html", error=error)


@auth_bp.route("/logout", methods=["GET"])
def logout():
    session.clear()
    return redirect(url_for("auth.login"))


def _safe_next_url() -> str | None:
    raw_next = request.args.get("next") or request.form.get("next") if request.method == "POST" else None
    if not raw_next:
        return None
    if raw_next.startswith("http://") or raw_next.startswith("https://"):
        return None
    if not raw_next.startswith("/"):
        return None
    return raw_next

