from __future__ import annotations

from app.application.erp_outbox_service import ErpOutboxService
from app.infrastructure.repositories.procurement import LegacyProcurementRepository


class ProcurementService:
    """Application facade: delegates data access and SQL-heavy operations to repositories."""

    def __init__(
        self,
        outbox_service: ErpOutboxService | None = None,
        repository: LegacyProcurementRepository | None = None,
    ) -> None:
        self.outbox_service = outbox_service or ErpOutboxService()
        self.repository = repository or LegacyProcurementRepository(outbox_service=self.outbox_service)

    def __getattr__(self, item: str):
        try:
            return getattr(self.repository, item)
        except AttributeError as exc:  # pragma: no cover - defensive guard
            raise AttributeError(f"{self.__class__.__name__} has no attribute '{item}'") from exc
