from __future__ import annotations

from uuid import NAMESPACE_URL, UUID, uuid5

from flask import Blueprint, jsonify, request, session

from app.contexts.platform.application.command_bus import CommandBus
from app.contexts.platform.domain.command import Command
from app.contexts.platform.infrastructure.command_repository import CommandRepository
from app.db import get_db
from app.errors import ValidationError
from app.policies import require_roles
from app.tenant import scoped_tenant_id


platform_commands_bp = Blueprint("platform_commands", __name__, url_prefix="/commands")


def _required_string(payload: dict, field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str):
        raise ValidationError(
            code="validation_error",
            message_key="action_invalid",
            http_status=400,
            critical=False,
            payload={"field": field_name},
        )
    normalized = value.strip()
    if not normalized:
        raise ValidationError(
            code="validation_error",
            message_key="action_invalid",
            http_status=400,
            critical=False,
            payload={"field": field_name},
        )
    return normalized


def _optional_dict(payload: dict, field_name: str) -> dict:
    value = payload.get(field_name, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValidationError(
            code="validation_error",
            message_key="action_invalid",
            http_status=400,
            critical=False,
            payload={"field": field_name},
        )
    return dict(value)


def _optional_string(payload: dict, field_name: str) -> str | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValidationError(
            code="validation_error",
            message_key="action_invalid",
            http_status=400,
            critical=False,
            payload={"field": field_name},
        )
    normalized = value.strip()
    return normalized or None


def _tenant_uuid_from_context() -> UUID:
    tenant_raw = str(scoped_tenant_id() or "").strip()
    if not tenant_raw:
        raise ValidationError(
            code="validation_error",
            message_key="action_invalid",
            http_status=400,
            critical=False,
            payload={"field": "tenant_id"},
        )
    try:
        return UUID(tenant_raw)
    except ValueError:
        # Keep deterministic tenant isolation even when auth context uses slug tenant IDs.
        return uuid5(NAMESPACE_URL, tenant_raw)


def _requested_by_from_context() -> str:
    return str(session.get("user_email") or session.get("display_name") or "anonymous").strip() or "anonymous"


@platform_commands_bp.route("/", methods=["POST"])
def create_command_http():
    require_roles("buyer", "manager", "admin", "approver")
    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        raise ValidationError(
            code="validation_error",
            message_key="action_invalid",
            http_status=400,
            critical=False,
        )

    command_type = _required_string(body, "command_type")
    entity_type = _required_string(body, "entity_type")
    entity_id = _required_string(body, "entity_id")
    payload = _optional_dict(body, "payload")
    idempotency_key = _optional_string(body, "idempotency_key")

    tenant_id = _tenant_uuid_from_context()
    requested_by = _requested_by_from_context()

    command = Command.create(
        tenant_id=tenant_id,
        command_type=command_type,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=payload or {},
        requested_by=requested_by,
        idempotency_key=idempotency_key,
    )

    db = get_db()
    bus = CommandBus(CommandRepository(tenant_id=tenant_id, db=db))
    dispatched = bus.dispatch(command)
    db.commit()

    status_code = 201
    if idempotency_key and dispatched.id != command.id:
        status_code = 200

    return (
        jsonify(
            {
                "command_id": str(dispatched.id),
                "status": dispatched.status,
            }
        ),
        status_code,
    )

