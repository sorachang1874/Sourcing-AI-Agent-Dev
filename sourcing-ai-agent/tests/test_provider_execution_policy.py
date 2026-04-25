import unittest

from sourcing_agent.provider_execution_policy import normalize_former_member_search_contract


class ProviderExecutionPolicyTest(unittest.TestCase):
    def test_former_member_search_contract_forces_primary_harvest_lane(self) -> None:
        contract = normalize_former_member_search_contract(
            strategy_type="former_employee_search",
            employment_statuses=["former"],
            search_channel_order=["web_search"],
            cost_policy={"provider_people_search_mode": "fallback_only"},
            min_expected_results=50,
        )

        self.assertEqual(contract["cost_policy"]["provider_people_search_mode"], "primary_only")
        self.assertEqual(contract["cost_policy"]["provider_people_search_min_expected_results"], 50)
        self.assertEqual(contract["search_channel_order"], ["harvest_profile_search"])
        self.assertTrue(contract["provider_search_only"])

    def test_non_former_task_does_not_promote_profile_search_contract(self) -> None:
        contract = normalize_former_member_search_contract(
            strategy_type="full_company_roster",
            employment_statuses=["current"],
            search_channel_order=["web_search"],
            cost_policy={"provider_people_search_mode": "fallback_only"},
            min_expected_results=50,
        )

        self.assertEqual(contract["cost_policy"]["provider_people_search_mode"], "fallback_only")
        self.assertEqual(contract["search_channel_order"], ["web_search"])
        self.assertFalse(contract["provider_search_only"])


if __name__ == "__main__":
    unittest.main()
