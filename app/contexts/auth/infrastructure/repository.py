from __future__ import annotations

from werkzeug.security import generate_password_hash


class AuthRepository:
    def find_user_by_email(self, db, email: str) -> dict | None:
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

    def email_exists(self, db, email: str) -> bool:
        row = db.execute(
            "SELECT 1 FROM auth_users WHERE email = ?",
            (email,),
        ).fetchone()
        return bool(row)

    def ensure_tenant(self, db, tenant_id: str, name: str) -> None:
        db.execute(
            """
            INSERT INTO tenants (id, name, subdomain)
            VALUES (?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            (tenant_id, name, tenant_id),
        )

    def create_user(
        self,
        db,
        *,
        email: str,
        password: str,
        display_name: str | None,
        tenant_id: str,
    ) -> None:
        password_hash = generate_password_hash(password)
        db.execute(
            """
            INSERT INTO auth_users (email, password_hash, display_name, tenant_id)
            VALUES (?, ?, ?, ?)
            """,
            (email, password_hash, display_name, tenant_id),
        )

