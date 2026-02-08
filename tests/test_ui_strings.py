import unittest

from app.ui_strings import ERP_STATUS_GROUP, STATUS_GROUPS


class UiStringsStatusGroupsTest(unittest.TestCase):
    def test_required_status_groups_exist(self) -> None:
        required_groups = {"solicitacao", "cotacao", "decisao", "ordem_compra", "fornecedor"}
        self.assertTrue(required_groups.issubset(set(STATUS_GROUPS.keys())))

    def test_status_groups_are_not_empty(self) -> None:
        for group_name in ("solicitacao", "cotacao", "decisao", "ordem_compra", "fornecedor"):
            self.assertIn(group_name, STATUS_GROUPS)
            self.assertTrue(STATUS_GROUPS[group_name], f"grupo vazio: {group_name}")

    def test_status_labels_are_not_empty(self) -> None:
        for group_name, statuses in STATUS_GROUPS.items():
            for status in statuses:
                label = (status.get("label") or "").strip()
                self.assertTrue(label, f"label vazio em {group_name}:{status.get('key')}")

    def test_status_descriptions_are_not_empty(self) -> None:
        for group_name, statuses in STATUS_GROUPS.items():
            for status in statuses:
                description = (status.get("description") or "").strip()
                self.assertTrue(description, f"descricao vazia em {group_name}:{status.get('key')}")

    def test_required_erp_statuses_exist(self) -> None:
        expected = {"nao_enviado", "enviado", "aceito", "rejeitado", "reenvio_necessario"}
        keys = {str(item.get("key") or "").strip() for item in ERP_STATUS_GROUP}
        self.assertTrue(expected.issubset(keys))

    def test_erp_status_labels_and_descriptions_are_not_empty(self) -> None:
        for item in ERP_STATUS_GROUP:
            label = (item.get("label") or "").strip()
            description = (item.get("description") or "").strip()
            self.assertTrue(label, f"label ERP vazio: {item.get('key')}")
            self.assertTrue(description, f"descricao ERP vazia: {item.get('key')}")


if __name__ == "__main__":
    unittest.main()
