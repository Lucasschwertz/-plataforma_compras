import unittest

from app.procurement.flow_policy import FLOW_POLICY, PROCESS_STAGES


class FlowPolicyTest(unittest.TestCase):
    def test_required_process_stages_exist(self) -> None:
        keys = [item["key"] for item in PROCESS_STAGES]
        self.assertEqual(keys, ["solicitacao", "cotacao", "decisao", "ordem_compra", "erp"])

    def test_all_stage_statuses_have_actions(self) -> None:
        for stage_name, status_map in FLOW_POLICY.items():
            self.assertTrue(status_map, f"stage sem status: {stage_name}")
            for status_name, policy in status_map.items():
                actions = policy.get("allowed_actions") or []
                self.assertTrue(actions, f"acoes vazias em {stage_name}:{status_name}")

    def test_primary_action_is_in_allowed_actions(self) -> None:
        for stage_name, status_map in FLOW_POLICY.items():
            for status_name, policy in status_map.items():
                primary_action = policy.get("primary_action")
                allowed_actions = policy.get("allowed_actions") or []
                if primary_action:
                    self.assertIn(primary_action, allowed_actions, f"primary fora de allowed em {stage_name}:{status_name}")


if __name__ == "__main__":
    unittest.main()

