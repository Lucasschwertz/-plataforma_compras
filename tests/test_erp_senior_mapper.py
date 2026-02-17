from __future__ import annotations

import unittest

from app.contexts.erp.domain.contracts import ErpPurchaseOrderLineV1, ErpPurchaseOrderV1
from app.contexts.erp.infrastructure.mappers.senior_po_mapper import (
    map_canonical_po_to_senior_payload,
    map_senior_response_to_push_result,
)
from app.errors import ValidationError


class ErpSeniorMapperTest(unittest.TestCase):
    def _sample_po(self) -> ErpPurchaseOrderV1:
        return ErpPurchaseOrderV1(
            workspace_id="tenant-mapper",
            external_ref="300",
            supplier_code="SUP-300",
            supplier_name="Fornecedor Mapper",
            currency="BRL",
            payment_terms="30D",
            issued_at="2026-02-15T12:00:00Z",
            lines=[
                ErpPurchaseOrderLineV1(
                    line_id="300:1",
                    product_code="ITEM-300",
                    description="Item 300",
                    qty=2.0,
                    unit_price=17.5,
                    uom="UN",
                    cost_center="CC-300",
                    delivery_date="2026-03-10",
                )
            ],
            totals={"gross_total": 35.0, "net_total": 34.0},
        )

    def test_map_canonical_to_senior_payload(self) -> None:
        payload = map_canonical_po_to_senior_payload(self._sample_po())
        self.assertEqual(payload.get("number"), "300")
        self.assertEqual(payload.get("currency"), "BRL")
        self.assertEqual(payload.get("total_amount"), 35.0)
        self.assertEqual(payload.get("source"), "plataforma_compras")
        self.assertEqual(payload.get("id"), 300)

    def test_map_canonical_validation(self) -> None:
        po = self._sample_po()
        po.lines[0].qty = 0
        with self.assertRaises(ValidationError):
            map_canonical_po_to_senior_payload(po)

    def test_map_response_to_push_result_classification(self) -> None:
        accepted = map_senior_response_to_push_result(
            {"status": "accepted", "external_id": "ERP-300"},
            workspace_id="tenant-mapper",
            external_ref="300",
        )
        self.assertEqual(accepted.status, "accepted")
        self.assertEqual(accepted.erp_document_number, "ERP-300")

        rejected = map_senior_response_to_push_result(
            {"status": "error", "message": "ERP HTTP 422 rejected by validation"},
            workspace_id="tenant-mapper",
            external_ref="300",
        )
        self.assertEqual(rejected.status, "rejected")
        self.assertEqual(rejected.rejection_code, "VALIDATION")

        temporary = map_senior_response_to_push_result(
            {"status": "error", "message": "temporary timeout"},
            workspace_id="tenant-mapper",
            external_ref="300",
        )
        self.assertEqual(temporary.status, "temporary_failure")
        self.assertIsNone(temporary.rejection_code)


if __name__ == "__main__":
    unittest.main()

