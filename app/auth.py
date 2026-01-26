from __future__ import annotations

from typing import Iterable

from flask import Blueprint, current_app, jsonify, redirect, render_template, request, session, url_for


auth_bp = Blueprint("auth", __name__)


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
        if path in {"/login", "/logout"}:
            return None

        if session.get("user_email"):
            return None

        if path.startswith("/api/"):
            return jsonify({"error": "auth_required", "message": "Autenticacao necessaria."}), 401

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
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        user = _find_user(email, password, current_app.config.get("APP_USERS"))
        if user:
            session["user_email"] = user["email"]
            session["display_name"] = user["display_name"]
            session["tenant_id"] = user["tenant_id"]
            return redirect(_safe_next_url() or url_for("home.home"))

        error = "Credenciais invalidas. Tente novamente."

    return render_template("login.html", error=error)


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


def _find_user(email: str, password: str, raw_users: object) -> dict | None:
    for user in _parse_users(raw_users):
        if user["email"] == email and user["password"] == password:
            return user
    return None


def _parse_users(raw_users: object) -> Iterable[dict]:
    if not raw_users:
        return []
    if isinstance(raw_users, str):
        entries = []
        for chunk in raw_users.replace("\n", ",").replace(";", ",").split(","):
            entry = chunk.strip()
            if entry:
                entries.append(entry)
    elif isinstance(raw_users, (list, tuple, set)):
        entries = [str(item).strip() for item in raw_users if str(item).strip()]
    else:
        return []

    users = []
    for entry in entries:
        parts = [part.strip() for part in entry.split(":")]
        if len(parts) < 3:
            continue
        email, password, tenant_id = parts[0].lower(), parts[1], parts[2]
        display_name = parts[3] if len(parts) > 3 and parts[3] else email.split("@")[0]
        users.append(
            {
                "email": email,
                "password": password,
                "tenant_id": tenant_id,
                "display_name": display_name,
            }
        )
    return users
