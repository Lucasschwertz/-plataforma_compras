from __future__ import annotations

from typing import Iterable

import re

from flask import Blueprint, current_app, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from app.db import get_db
from app.tenant import DEFAULT_TENANT_ID


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
        if path in {"/login", "/logout", "/register", "/health"}:
            return None
        if path.startswith("/fornecedor/convite/") or path.startswith("/api/fornecedor/convite/"):
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
            session["user_role"] = user.get("role", "buyer")
            return redirect(_safe_next_url() or url_for("home.home"))

        error = "Credenciais invalidas. Tente novamente."

    return render_template("login.html", error=error)


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if session.get("user_email"):
        return redirect(url_for("home.home"))

    error = None
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        display_name = (request.form.get("display_name") or "").strip() or None
        company_name = (request.form.get("company_name") or "").strip()

        if not email or not password:
            error = "Informe email e senha."
        else:
            tenant_id = _resolve_tenant_id(company_name)
            try:
                user = _create_user(email, password, display_name, tenant_id, company_name or None)
            except ValueError as exc:
                error = str(exc)
            else:
                session["user_email"] = user["email"]
                session["display_name"] = user["display_name"]
                session["tenant_id"] = user["tenant_id"]
                session["user_role"] = user.get("role", "buyer")
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


def _find_user(email: str, password: str, raw_users: object) -> dict | None:
    db_user = _find_user_in_db(email)
    if db_user and check_password_hash(db_user["password_hash"], password):
        return {
            "email": db_user["email"],
            "display_name": db_user["display_name"] or db_user["email"].split("@")[0],
            "tenant_id": db_user["tenant_id"],
            "role": "buyer",
        }

    for user in _parse_users(raw_users):
        if user["email"] == email and user["password"] == password:
            return user
    return None


def _find_user_in_db(email: str) -> dict | None:
    db = get_db()
    row = db.execute(
        """
        SELECT email, password_hash, display_name, tenant_id
        FROM auth_users
        WHERE email = ?
        """,
        (email,),
    ).fetchone()
    if not row:
        return None
    return dict(row)


def _create_user(
    email: str,
    password: str,
    display_name: str | None,
    tenant_id: str,
    company_name: str | None,
) -> dict:
    db = get_db()
    existing = db.execute(
        "SELECT 1 FROM auth_users WHERE email = ?",
        (email,),
    ).fetchone()
    if existing:
        raise ValueError("Email ja cadastrado. Use outro email ou faca login.")

    _ensure_tenant(db, tenant_id, company_name or f"Tenant {tenant_id}")

    password_hash = generate_password_hash(password)
    db.execute(
        """
        INSERT INTO auth_users (email, password_hash, display_name, tenant_id)
        VALUES (?, ?, ?, ?)
        """,
        (email, password_hash, display_name, tenant_id),
    )
    db.commit()

    return {
        "email": email,
        "display_name": display_name or email.split("@")[0],
        "tenant_id": tenant_id,
        "role": "buyer",
    }


def _ensure_tenant(db, tenant_id: str, name: str) -> None:
    db.execute(
        """
        INSERT INTO tenants (id, name, subdomain)
        VALUES (?, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        (tenant_id, name, tenant_id),
    )


def _resolve_tenant_id(company_name: str) -> str:
    if not company_name:
        return DEFAULT_TENANT_ID
    slug = _slugify(company_name)
    if not slug:
        return DEFAULT_TENANT_ID
    return f"tenant-{slug}"


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^\w\s-]", "", value)
    value = re.sub(r"[\s_-]+", "-", value)
    return value.strip("-")


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
        role = parts[4].lower() if len(parts) > 4 and parts[4] else "buyer"
        if role not in {"buyer", "admin", "approver", "manager", "supplier"}:
            role = "buyer"
        users.append(
            {
                "email": email,
                "password": password,
                "tenant_id": tenant_id,
                "display_name": display_name,
                "role": role,
            }
        )
    return users
