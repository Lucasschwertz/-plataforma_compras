from __future__ import annotations

from typing import List

from app.contexts.erp.domain.gateway import ErpGateway, ErpGatewayError
from app.contexts.erp.infrastructure.client import ErpError, fetch_erp_records, push_purchase_order


class SeniorErpGateway(ErpGateway):
    def fetch_records(
        self,
        entity: str,
        since_updated_at: str | None,
        since_id: str | None,
        *,
        limit: int = 100,
    ) -> List[dict]:
        try:
            return fetch_erp_records(entity, since_updated_at, since_id, limit=limit)
        except ErpError as exc:
            raise ErpGatewayError(str(exc)) from exc

    def push_purchase_order(self, purchase_order: dict) -> dict:
        try:
            return push_purchase_order(purchase_order)
        except ErpError as exc:
            raise ErpGatewayError(str(exc)) from exc



