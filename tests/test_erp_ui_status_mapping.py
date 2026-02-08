import unittest

from app.ui_strings import erp_status_key, erp_status_payload


class ErpUiStatusMappingTest(unittest.TestCase):
    def test_maps_technical_status_to_erp_ui_status(self) -> None:
        self.assertEqual(erp_status_key("draft"), "nao_enviado")
        self.assertEqual(erp_status_key("approved"), "nao_enviado")
        self.assertEqual(erp_status_key("sent_to_erp"), "enviado")
        self.assertEqual(erp_status_key("erp_accepted"), "aceito")
        self.assertEqual(erp_status_key("partially_received"), "aceito")
        self.assertEqual(erp_status_key("received"), "aceito")

    def test_maps_erp_error_to_rejected_or_retry(self) -> None:
        self.assertEqual(erp_status_key("erp_error", erp_last_error="ERP HTTP 422 rejected"), "rejeitado")
        self.assertEqual(
            erp_status_key("erp_error", erp_last_error="timeout communicating with ERP"),
            "reenvio_necessario",
        )

    def test_payload_has_friendly_text(self) -> None:
        payload = erp_status_payload("erp_error", erp_last_error="temporary unavailable")
        self.assertEqual(payload["key"], "reenvio_necessario")
        self.assertTrue((payload.get("label") or "").strip())
        self.assertTrue((payload.get("description") or "").strip())
        self.assertTrue((payload.get("message") or "").strip())


if __name__ == "__main__":
    unittest.main()
