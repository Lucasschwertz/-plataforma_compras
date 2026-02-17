from __future__ import annotations

import unittest

from app.contexts.erp.domain.contracts import (
    ErpPurchaseOrderLineV1,
    ErpPurchaseOrderV1,
    ErpPushResultV1,
    from_dict,
    to_dict,
    validate_contract,
)
from app.contexts.erp.domain.schemas import validate_schema


class ErpAclContractsTest(unittest.TestCase):
    def _sample_purchase_order_dict(self) -> dict:
        return {
            "schema_name": "erp.purchase_order",
            "schema_version": 1,
            "workspace_id": "tenant-acl",
            "external_ref": "PO-100",
            "supplier_code": "SUP-1",
            "supplier_name": "Fornecedor ACL",
            "currency": "BRL",
            "payment_terms": "30D",
            "issued_at": "2026-02-15T10:00:00Z",
            "lines": [
                {
                    "line_id": "PO-100:1",
                    "product_code": "ITEM-1",
                    "description": "Item ACL",
                    "qty": 2.0,
                    "unit_price": 10.5,
                    "uom": "UN",
                    "cost_center": "CC-01",
                    "delivery_date": "2026-03-01",
                }
            ],
            "totals": {
                "gross_total": 21.0,
                "net_total": 20.0,
            },
        }

    def test_validate_schema_required_optional(self) -> None:
        payload = self._sample_purchase_order_dict()
        ok, errors = validate_schema("erp.purchase_order", 1, payload)
        self.assertTrue(ok)
        self.assertEqual(errors, [])

        invalid_payload = dict(payload)
        invalid_payload.pop("lines", None)
        ok_invalid, errors_invalid = validate_schema("erp.purchase_order", 1, invalid_payload)
        self.assertFalse(ok_invalid)
        self.assertTrue(any("missing required field: lines" in err for err in errors_invalid))

    def test_to_dict_from_dict_versioned(self) -> None:
        line = ErpPurchaseOrderLineV1(
            line_id="PO-200:1",
            product_code="ITEM-2",
            description="Item 2",
            qty=3.0,
            unit_price=15.0,
            uom="UN",
            cost_center="CC-02",
            delivery_date="2026-03-02",
        )
        po = ErpPurchaseOrderV1(
            workspace_id="tenant-acl",
            external_ref="PO-200",
            supplier_code="SUP-2",
            supplier_name="Fornecedor 2",
            currency="BRL",
            payment_terms="28D",
            issued_at="2026-02-15T11:00:00Z",
            lines=[line],
            totals={"gross_total": 45.0, "net_total": 43.0},
        )
        po_dict = to_dict(po)
        parsed_po = from_dict(po_dict)
        self.assertIsInstance(parsed_po, ErpPurchaseOrderV1)
        self.assertEqual(parsed_po.schema_name, "erp.purchase_order")
        self.assertEqual(parsed_po.schema_version, 1)
        self.assertEqual(parsed_po.external_ref, "PO-200")
        self.assertEqual(len(parsed_po.lines), 1)

        push_result = ErpPushResultV1(
            workspace_id="tenant-acl",
            external_ref="PO-200",
            erp_document_number="ERP-200",
            status="accepted",
            rejection_code=None,
            message="ok",
            occurred_at="2026-02-15T11:01:00Z",
        )
        result_dict = to_dict(push_result)
        parsed_result = from_dict(result_dict)
        self.assertIsInstance(parsed_result, ErpPushResultV1)
        self.assertEqual(parsed_result.schema_name, "erp.push_result")
        self.assertEqual(parsed_result.schema_version, 1)
        self.assertEqual(parsed_result.status, "accepted")

    def test_validate_contract_returns_errors_without_exception(self) -> None:
        payload = self._sample_purchase_order_dict()
        payload["lines"][0]["qty"] = 0
        payload["totals"] = {}
        errors = validate_contract(payload)
        self.assertTrue(errors)
        self.assertTrue(any("qty must be > 0" in item for item in errors))
        self.assertTrue(any("totals.gross_total is required" in item for item in errors))


if __name__ == "__main__":
    unittest.main()

