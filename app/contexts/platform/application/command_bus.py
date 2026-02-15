from __future__ import annotations

from uuid import UUID

from app.contexts.platform.domain.command import Command
from app.contexts.platform.infrastructure.command_repository import CommandRepository


class CommandBus:
    def __init__(self, command_repository: CommandRepository) -> None:
        self._command_repository = command_repository

    def dispatch(self, command: Command) -> Command:
        if command.idempotency_key is not None:
            existing = self._command_repository.get_by_idempotency_key(
                command.tenant_id,
                command.idempotency_key,
            )
            if existing is not None:
                return existing

        try:
            self._command_repository.create(command)
        except Exception as exc:  # noqa: BLE001
            if command.idempotency_key is not None and self._is_idempotency_unique_violation(exc):
                existing = self._command_repository.get_by_idempotency_key(
                    command.tenant_id,
                    command.idempotency_key,
                )
                if existing is not None:
                    return existing
            raise
        return command

    def mark_running(self, command_id: UUID) -> None:
        self._command_repository.mark_running(command_id)

    def mark_success(self, command_id: UUID) -> None:
        self._command_repository.mark_success(command_id)

    def mark_failed(self, command_id: UUID, error_message: str) -> None:
        self._command_repository.mark_failed(command_id, error_message)

    @staticmethod
    def _is_idempotency_unique_violation(exc: Exception) -> bool:
        pg_code = str(getattr(exc, "pgcode", "") or "").strip()
        message = str(exc or "").lower()
        if getattr(exc, "__cause__", None) is not None:
            message = f"{message} {str(exc.__cause__ or '').lower()}"

        if "idempotency_key" not in message and "uq_commands_tenant_idempotency_key" not in message:
            return False

        if pg_code == "23505":
            return True

        if "unique constraint failed" in message:
            return True
        if "duplicate key value violates unique constraint" in message:
            return True
        return "unique" in message
