from __future__ import annotations

from typing import List

from app.contexts.erp.domain.contracts import ErpPurchaseOrderV1, ErpPushResultV1
from app.contexts.erp.domain.gateway import ErpGateway, ErpGatewayError
from app.contexts.erp.infrastructure.client import ErpError, fetch_erp_records, push_purchase_order
from app.contexts.erp.infrastructure.mappers.senior_errors import is_definitive_failure, normalize_rejection_code
from app.contexts.erp.infrastructure.mappers.senior_po_mapper import (
    map_canonical_po_to_senior_payload,
    map_senior_response_to_push_result,
)
from app.errors import ValidationError


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

    def push_purchase_order(self, purchase_order: ErpPurchaseOrderV1) -> ErpPushResultV1:
        try:
            senior_payload = map_canonical_po_to_senior_payload(purchase_order)
            raw_response = push_purchase_order(senior_payload)
            return map_senior_response_to_push_result(
                raw_response,
                workspace_id=purchase_order.workspace_id,
                external_ref=purchase_order.external_ref,
            )
        except ValidationError as exc:
            raise ErpGatewayError(
                str(exc.user_message() if hasattr(exc, "user_message") else str(exc)),
                code="erp_payload_invalid_for_erp",
                definitive=True,
            ) from exc
        except ErpError as exc:
            details = str(exc)
            raise ErpGatewayError(
                details,
                code=normalize_rejection_code(details),
                definitive=is_definitive_failure(details),
            ) from exc
