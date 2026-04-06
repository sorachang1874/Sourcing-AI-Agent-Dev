import tempfile
import unittest
from pathlib import Path

from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.seed_discovery import (
    SearchSeedAcquirer,
    extract_linkedin_slug,
    extract_web_search_results,
    infer_name_from_result_title,
    infer_public_names_from_result_title,
)


class SeedDiscoveryTest(unittest.TestCase):
    def test_extract_web_search_results_and_slug(self) -> None:
        html = """
        <a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.linkedin.com%2Fin%2Fyuntao-bai%2F">
        Yuntao Bai - Anthropic - LinkedIn
        </a>
        """
        results = extract_web_search_results(html)
        self.assertEqual(len(results), 1)
        self.assertEqual(extract_linkedin_slug(results[0]["url"]), "yuntao-bai")

    def test_infer_name_from_result_title(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            aliases=["anthropicresearch"],
        )
        self.assertEqual(infer_name_from_result_title("Yuntao Bai - Anthropic - LinkedIn", identity), "Yuntao Bai")

    def test_infer_public_names_from_result_title(self) -> None:
        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            aliases=["Google DeepMind", "Gemini"],
        )
        names = infer_public_names_from_result_title(
            "Interview with Logan Kilpatrick and Tulsee Doshi on Gemini research roadmap",
            identity,
        )
        self.assertIn("Logan Kilpatrick", names)
        self.assertIn("Tulsee Doshi", names)

    def test_former_paid_fallback_prefers_past_company_only_harvest_query(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                self.queries.append(kwargs.get("query_text", ""))
                return {"raw_path": self.tempdir / "fake.json", "rows": []}

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["Thinking Machines Lab Employee former"],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                employment_status="former",
                limit=25,
                cost_policy={},
            )
            self.assertEqual(entries, [])
            self.assertEqual(errors, [])
            self.assertEqual(accounts, [])
            self.assertGreaterEqual(len(summaries), 1)
            self.assertEqual(fake.queries[0], "")
