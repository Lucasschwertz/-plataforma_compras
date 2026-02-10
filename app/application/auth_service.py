from __future__ import annotations

import re
from typing import Iterable

from werkzeug.security import check_password_hash

from app.domain.contracts import AuthLoginInput, AuthRegisterInput, AuthUser
from app.errors import ValidationError
from app.infrastructure.auth_repository import AuthRepository
from app.policies import normalize_role
from app.tenant import DEFAULT_TENANT_ID


class AuthService:
    def __init__(self, repository: AuthRepository | None = None) -> None:
        self.repository = repository or AuthRepository()

    def login(self, db, auth_input: AuthLoginInput, raw_users: object) -> AuthUser | None:
        email = (auth_input.email or "").strip().lower()
        password = auth_input.password or ""
        if not email or not password:
            return None

        db_user = self.repository.find_user_by_email(db, email)
        if db_user and check_password_hash(db_user["password_hash"], password):
            return AuthUser(
                email=db_user["email"],
                display_name=db_user.get("display_name") or db_user["email"].split("@")[0],
                tenant_id=db_user["tenant_id"],
                role="buyer",
            )

        for user in self._parse_users(raw_users):
            if user["email"] == email and user["password"] == password:
                return AuthUser(
                    email=user["email"],
                    display_name=user["display_name"],
                    tenant_id=user["tenant_id"],
                    role=user["role"],
                )
        return None

    def register(self, db, auth_input: AuthRegisterInput) -> AuthUser:
        email = (auth_input.email or "").strip().lower()
        password = auth_input.password or ""
        display_name = (auth_input.display_name or "").strip() or None
        company_name = (auth_input.company_name or "").strip() or None
        tenant_id = self.resolve_tenant_id(company_name or "")

        if self.repository.email_exists(db, email):
            raise ValidationError(
                code="email_already_registered",
                message_key="email_already_registered",
                http_status=400,
                critical=False,
            )

        self.repository.ensure_tenant(db, tenant_id, company_name or f"Tenant {tenant_id}")
        self.repository.create_user(
            db,
            email=email,
            password=password,
            display_name=display_name,
            tenant_id=tenant_id,
        )

        return AuthUser(
            email=email,
            display_name=display_name or email.split("@")[0],
            tenant_id=tenant_id,
            role="buyer",
        )

    def resolve_tenant_id(self, company_name: str) -> str:
        if not company_name:
            return DEFAULT_TENANT_ID
        slug = self._slugify(company_name)
        if not slug:
            return DEFAULT_TENANT_ID
        return f"tenant-{slug}"

    @staticmethod
    def _slugify(value: str) -> str:
        normalized = value.strip().lower()
        normalized = re.sub(r"[^\w\s-]", "", normalized)
        normalized = re.sub(r"[\s_-]+", "-", normalized)
        return normalized.strip("-")

    @staticmethod
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
            role = normalize_role(parts[4] if len(parts) > 4 else "buyer", default="buyer")
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

