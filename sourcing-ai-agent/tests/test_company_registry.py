import unittest

from sourcing_agent.company_registry import builtin_company_identity, infer_target_company_from_text
from sourcing_agent.domain import JobRequest


class CompanyRegistryTest(unittest.TestCase):
    def test_infer_target_company_from_text_detects_anthropic(self) -> None:
        inferred = infer_target_company_from_text("帮我找出Anthropic的所有华人成员")

        self.assertEqual(inferred["company_key"], "anthropic")
        self.assertEqual(inferred["canonical_name"], "Anthropic")

    def test_infer_target_company_from_text_detects_humansand(self) -> None:
        inferred = infer_target_company_from_text("我想了解 Humans& 的 Coding 方向 Researcher")

        self.assertEqual(inferred["company_key"], "humansand")
        self.assertEqual(inferred["canonical_name"], "Humans&")

    def test_job_request_uses_company_inference_when_target_company_missing(self) -> None:
        request = JobRequest.from_payload({"raw_user_request": "帮我找出Anthropic的所有华人成员"})

        self.assertEqual(request.target_company, "Anthropic")
        self.assertEqual(request.keywords, ["Greater China experience", "Chinese bilingual outreach"])

    def test_builtin_company_identity_contains_humansand(self) -> None:
        company_key, builtin = builtin_company_identity("Humans&")

        self.assertEqual(company_key, "humansand")
        self.assertIsNotNone(builtin)
        assert builtin is not None
        self.assertEqual(builtin["linkedin_slug"], "humansand")


if __name__ == "__main__":
    unittest.main()
