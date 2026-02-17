from __future__ import annotations

import unittest

from app.contexts.erp.domain.contracts import ErpPurchaseOrderLineV1, ErpPurchaseOrderV1
from app.contexts.erp.infrastructure.simulator.deterministic_erp import DeterministicErpSimulator


class ErpSimulatorDeterministicTest(unittest.TestCase):
    def _po(self, external_ref: str, *, supplier_code: str = "SUP-1", qty: float = 1.0) -> ErpPurchaseOrderV1:
        return ErpPurchaseOrderV1(
            workspace_id="tenant-sim",
            external_ref=external_ref,
            supplier_code=supplier_code,
            supplier_name="Fornecedor Sim",
            currency="BRL",
            payment_terms=None,
            issued_at="2026-02-15T13:00:00Z",
            lines=[
                ErpPurchaseOrderLineV1(
                    line_id=f"{external_ref}:1",
                    product_code="ITEM-SIM",
                    description="Item simulador",
                    qty=qty,
                    unit_price=10.0,
                    uom="UN",
                    cost_center=None,
                    delivery_date=None,
                )
            ],
            totals={"gross_total": 10.0, "net_total": None},
        )

    def test_same_seed_and_ref_generate_same_result(self) -> None:
        simulator = DeterministicErpSimulator(seed=42)
        po = self._po("PO-DET-1")
        result_a = simulator.push_purchase_order(po)
        result_b = simulator.push_purchase_order(po)
        self.assertEqual(result_a.status, result_b.status)
        self.assertEqual(result_a.rejection_code, result_b.rejection_code)
        self.assertEqual(result_a.erp_document_number, result_b.erp_document_number)

    def test_distribution_is_stable_with_more_accepted_than_rejected(self) -> None:
        simulator = DeterministicErpSimulator(seed=7)
        accepted = 0
        rejected = 0
        temporary = 0
        for idx in range(200):
            result = simulator.push_purchase_order(self._po(f"PO-DIST-{idx}"))
            if result.status == "accepted":
                accepted += 1
            elif result.status == "rejected":
                rejected += 1
            elif result.status == "temporary_failure":
                temporary += 1
        self.assertGreater(accepted, rejected)
        self.assertGreater(temporary, 0)

    def test_domain_guards_override_distribution(self) -> None:
        simulator = DeterministicErpSimulator(seed=42)
        invalid_qty = simulator.push_purchase_order(self._po("PO-BAD-QTY", qty=0))
        self.assertEqual(invalid_qty.status, "rejected")
        self.assertEqual(invalid_qty.rejection_code, "VALIDATION")

        missing_supplier = simulator.push_purchase_order(self._po("PO-BAD-SUP", supplier_code=""))
        self.assertEqual(missing_supplier.status, "rejected")
        self.assertEqual(missing_supplier.rejection_code, "SUPPLIER")


if __name__ == "__main__":
    unittest.main()

