from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import List

from app.contexts.erp.domain.contracts import ErpPurchaseOrderV1, ErpPushResultV1
from app.contexts.erp.domain.gateway import ErpGateway
from app.contexts.erp.infrastructure.mock import fetch_erp_records
from app.observability import observe_erp_simulator_result
from app.ui_strings import error_message, success_message


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class DeterministicErpSimulator:
    def __init__(self, seed: int = 42) -> None:
        self.seed = int(seed)

    def _bucket(self, external_ref: str) -> int:
        digest = hashlib.sha256(f"{self.seed}:{external_ref}".encode("utf-8")).hexdigest()
        return int(digest[:8], 16) % 100

    def _rejection_code(self, external_ref: str) -> str:
        digest = hashlib.sha256(f"code:{self.seed}:{external_ref}".encode("utf-8")).hexdigest()
        idx = int(digest[:2], 16) % 3
        return ("VALIDATION", "SUPPLIER", "PRICE")[idx]

    def push_purchase_order(self, po: ErpPurchaseOrderV1) -> ErpPushResultV1:
        for line in po.lines:
            if float(line.qty) <= 0:
                observe_erp_simulator_result("rejected")
                return ErpPushResultV1(
                    workspace_id=po.workspace_id,
                    external_ref=po.external_ref,
                    erp_document_number=None,
                    status="rejected",
                    rejection_code="VALIDATION",
                    message=error_message("erp_order_rejected"),
                    occurred_at=_iso_now(),
                )
        if not str(po.supplier_code or "").strip():
            observe_erp_simulator_result("rejected")
            return ErpPushResultV1(
                workspace_id=po.workspace_id,
                external_ref=po.external_ref,
                erp_document_number=None,
                status="rejected",
                rejection_code="SUPPLIER",
                message=error_message("erp_order_rejected"),
                occurred_at=_iso_now(),
            )

        bucket = self._bucket(po.external_ref)
        if bucket < 70:
            observe_erp_simulator_result("accepted")
            doc_number = f"SIM-OC-{abs(hash((self.seed, po.external_ref))) % 1_000_000:06d}"
            return ErpPushResultV1(
                workspace_id=po.workspace_id,
                external_ref=po.external_ref,
                erp_document_number=doc_number,
                status="accepted",
                rejection_code=None,
                message=success_message("erp_accepted"),
                occurred_at=_iso_now(),
            )
        if bucket < 90:
            observe_erp_simulator_result("temporary_failure")
            return ErpPushResultV1(
                workspace_id=po.workspace_id,
                external_ref=po.external_ref,
                erp_document_number=None,
                status="temporary_failure",
                rejection_code=None,
                message=error_message("erp_temporarily_unavailable"),
                occurred_at=_iso_now(),
            )

        observe_erp_simulator_result("rejected")
        return ErpPushResultV1(
            workspace_id=po.workspace_id,
            external_ref=po.external_ref,
            erp_document_number=None,
            status="rejected",
            rejection_code=self._rejection_code(po.external_ref),
            message=error_message("erp_order_rejected"),
            occurred_at=_iso_now(),
        )


class DeterministicErpSimulatorGateway(ErpGateway):
    def __init__(self, seed: int = 42) -> None:
        self._simulator = DeterministicErpSimulator(seed=seed)

    def fetch_records(
        self,
        entity: str,
        since_updated_at: str | None,
        since_id: str | None,
        *,
        limit: int = 100,
    ) -> List[dict]:
        return fetch_erp_records(entity, since_updated_at, since_id, limit=limit)

    def push_purchase_order(self, purchase_order: ErpPurchaseOrderV1) -> ErpPushResultV1:
        return self._simulator.push_purchase_order(purchase_order)

