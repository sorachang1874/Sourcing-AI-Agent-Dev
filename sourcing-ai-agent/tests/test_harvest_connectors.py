import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.settings import HarvestActorSettings
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.connectors import CompanyRosterSnapshot, build_candidates_from_roster
from sourcing_agent.domain import Candidate
from sourcing_agent.enrichment import _classify_profile_membership, _merge_profile_into_candidate
from sourcing_agent.harvest_connectors import (
    HarvestCompanyEmployeesConnector,
    HarvestProfileConnector,
    HarvestProfileSearchConnector,
    _profile_scraper_mode,
    parse_harvest_company_employee_rows,
    parse_harvest_profile_payload,
    parse_harvest_search_rows,
)


class HarvestConnectorTest(unittest.TestCase):
    def test_parse_harvest_profile_payload(self) -> None:
        payload = {
            "firstName": "Jane",
            "lastName": "Doe",
            "headline": "Research Engineer at xAI",
            "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
            "publicIdentifier": "jane-doe",
            "about": "Works on reinforcement learning systems.",
            "location": {"linkedinText": "San Francisco Bay Area"},
            "currentPosition": [{"companyName": "xAI"}],
            "experiences": [{"companyName": "xAI", "title": "Research Engineer"}],
            "education": [{"schoolName": "MIT"}],
            "moreProfiles": [{"url": "https://www.linkedin.com/in/jane-doe-2/"}],
        }
        parsed = parse_harvest_profile_payload(payload)
        self.assertEqual(parsed["full_name"], "Jane Doe")
        self.assertEqual(parsed["profile_url"], "https://www.linkedin.com/in/jane-doe/")
        self.assertEqual(parsed["current_company"], "xAI")
        self.assertEqual(parsed["location"], "San Francisco Bay Area")
        self.assertEqual(len(parsed["more_profiles"]), 1)

    def test_lead_profile_merge_upgrades_membership(self) -> None:
        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            aliases=["x.ai"],
        )
        lead = Candidate(
            candidate_id="abc123",
            name_en="Jane Doe",
            display_name="Jane Doe",
            category="lead",
            target_company="xAI",
            organization="xAI",
            source_dataset="publication_lead",
            source_path="/tmp/source.json",
        )
        profile = {
            "full_name": "Jane Doe",
            "headline": "Research Engineer at xAI",
            "profile_url": "https://www.linkedin.com/in/jane-doe/",
            "public_identifier": "jane-doe",
            "summary": "Works on reinforcement learning systems.",
            "location": "SF",
            "current_company": "xAI",
            "experience": [{"company": "xAI", "title": "Research Engineer", "is_current": True}],
            "education": [],
            "publications": [],
            "more_profiles": [],
        }
        merged = _merge_profile_into_candidate(lead, profile, Path("/tmp/profile.json"), "harvest_profile_scraper", identity)
        self.assertEqual(merged.category, "employee")
        self.assertEqual(merged.employment_status, "current")
        label = _classify_profile_membership(profile, identity)
        self.assertEqual(label, ("employee", "current"))

    def test_parse_harvest_search_rows(self) -> None:
        payload = [
            {
                "fullName": "Jane Doe",
                "headline": "Research Scientist",
                "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                "publicIdentifier": "jane-doe",
                "currentCompany": "Thinking Machines Lab",
                "location": "San Francisco",
            }
        ]
        rows = parse_harvest_search_rows(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["full_name"], "Jane Doe")
        self.assertEqual(rows[0]["username"], "jane-doe")

    def test_parse_harvest_company_employee_rows(self) -> None:
        payload = [
            {
                "fullName": "John Smith",
                "headline": "Engineer",
                "linkedinUrl": "https://www.linkedin.com/in/john-smith/",
                "publicIdentifier": "john-smith",
                "location": "San Francisco",
            }
        ]
        rows = parse_harvest_company_employee_rows(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["full_name"], "John Smith")
        self.assertEqual(rows[0]["member_key"], "john-smith")
        self.assertFalse(rows[0]["is_headless"])

    def test_profile_scraper_mode_uses_current_actor_enum(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        self.assertEqual(_profile_scraper_mode(settings), "Profile details no email ($4 per 1k)")

    def test_harvest_profile_search_supports_multi_page_former_search(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short", max_paid_items=50)
        connector = HarvestProfileSearchConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            capture = {}

            def _fake_run(actor_settings, payload):
                capture["payload"] = dict(payload)
                capture["max_total_charge_usd"] = actor_settings.max_total_charge_usd
                return []

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                result = connector.search_profiles(
                    query_text="Thinking Machines Lab",
                    filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                    employment_status="former",
                    discovery_dir=Path(tempdir),
                    limit=25,
                    pages=2,
                )
        self.assertIsNotNone(result)
        self.assertEqual(capture["payload"]["takePages"], 2)
        self.assertEqual(capture["payload"]["maxItems"], 50)
        self.assertGreaterEqual(capture["max_total_charge_usd"], 0.2)
        self.assertEqual(
            capture["payload"]["pastCompanies"],
            ["https://www.linkedin.com/company/thinkingmachinesai/"],
        )

    def test_harvest_profile_search_reuses_matching_live_test_asset_without_token(self) -> None:
        settings = HarvestActorSettings(enabled=False, api_token="", actor_id="actor", default_mode="short", max_paid_items=50)
        connector = HarvestProfileSearchConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            discovery_dir = runtime_dir / "company_assets" / "thinkingmachineslab" / "snap1" / "search_seed_discovery"
            live_tests_dir = runtime_dir / "live_tests" / "harvest_former_tml_search_variants"
            discovery_dir.mkdir(parents=True, exist_ok=True)
            live_tests_dir.mkdir(parents=True, exist_ok=True)
            summary_path = live_tests_dir / "past_only_2pages_summary.json"
            raw_path = live_tests_dir / "past_only_2pages_raw.json"
            summary_path.write_text(
                """
{
  "variant": "past_only_2pages",
  "input_payload": {
    "profileScraperMode": "Short",
    "maxItems": 50,
    "startPage": 1,
    "takePages": 2,
    "pastCompanies": ["https://www.linkedin.com/company/thinkingmachinesai/"]
  }
}
""".strip()
            )
            raw_path.write_text(
                """
[
  {
    "firstName": "Alexis",
    "lastName": "Dunn",
    "linkedinUrl": "https://www.linkedin.com/in/alexis-aleyza-dunn/",
    "headline": "CEO @ ARI Health"
  }
]
""".strip()
            )
            result = connector.search_profiles(
                query_text="",
                filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                employment_status="former",
                discovery_dir=discovery_dir,
                limit=50,
                pages=2,
            )
        self.assertIsNotNone(result)
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0]["full_name"], "Alexis Dunn")

    def test_harvest_profile_connector_batch_call_no_longer_references_take_pages(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            capture = {}

            def _fake_run(_settings, payload):
                capture["payload"] = dict(payload)
                return [
                    {
                        "firstName": "Jane",
                        "lastName": "Doe",
                        "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                        "publicIdentifier": "jane-doe",
                    }
                ]

            with patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                side_effect=_fake_run,
            ):
                result = connector.fetch_profiles_by_urls(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    Path(tempdir),
                )
        self.assertIn("https://www.linkedin.com/in/jane-doe/", result)
        self.assertEqual(result["https://www.linkedin.com/in/jane-doe/"]["parsed"]["public_identifier"], "jane-doe")
        self.assertIn("profileUrls", capture["payload"])
        self.assertNotIn("urls", capture["payload"])

    def test_harvest_company_employees_reuses_matching_live_test_asset_without_token(self) -> None:
        settings = HarvestActorSettings(enabled=False, api_token="", actor_id="actor", default_mode="short", max_paid_items=25)
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "thinkingmachineslab" / "snap1"
            live_tests_dir = runtime_dir / "live_tests" / "harvest_tml_company_employees"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            live_tests_dir.mkdir(parents=True, exist_ok=True)
            (live_tests_dir / "tml_company_employees_summary.json").write_text(
                """
{
  "raw_path": "RUNTIME/live_tests/harvest_tml_company_employees/tml_company_employees_raw.json",
  "input_payload": {
    "profileScraperMode": "Short ($4 per 1k)",
    "maxItems": 25,
    "takePages": 1,
    "companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]
  }
}
""".replace("RUNTIME", str(runtime_dir)).strip()
            )
            (live_tests_dir / "tml_company_employees_raw.json").write_text(
                """
[
  {
    "firstName": "Mira",
    "lastName": "Murati",
    "linkedinUrl": "https://www.linkedin.com/in/ACwAAA4HOMcBjHQNGyUbyfYCY-sOZshkNFC30Jk",
    "headline": "Thinking Machines Lab",
    "location": {"linkedinText": "San Francisco"}
  }
]
""".strip()
            )
            snapshot = connector.fetch_company_roster(identity, snapshot_dir, max_pages=1, page_limit=25)
        self.assertEqual(len(snapshot.visible_entries), 1)
        self.assertEqual(snapshot.visible_entries[0]["full_name"], "Mira Murati")

    def test_build_candidates_from_roster_carries_linkedin_url(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        snapshot = CompanyRosterSnapshot(
            snapshot_id="snap1",
            target_company="Thinking Machines Lab",
            company_identity=identity,
            snapshot_dir=Path("/tmp/snap1"),
            raw_entries=[],
            visible_entries=[
                {
                    "member_key": "john-smith",
                    "full_name": "John Smith",
                    "headline": "Member of Technical Staff at Thinking Machines Lab",
                    "location": "San Francisco",
                    "linkedin_url": "https://www.linkedin.com/in/john-smith/",
                    "page": 1,
                    "source_account_id": "harvest_company_employees",
                }
            ],
            headless_entries=[],
            page_summaries=[],
            accounts_used=["harvest_company_employees"],
            errors=[],
            stop_reason="completed",
            merged_path=Path("/tmp/snap1/merged.json"),
            visible_path=Path("/tmp/snap1/visible.json"),
            headless_path=Path("/tmp/snap1/headless.json"),
            summary_path=Path("/tmp/snap1/summary.json"),
        )
        candidates, evidence = build_candidates_from_roster(snapshot)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].linkedin_url, "https://www.linkedin.com/in/john-smith/")
        self.assertEqual(candidates[0].metadata.get("profile_url"), "https://www.linkedin.com/in/john-smith/")
        self.assertEqual(evidence[0].metadata.get("profile_url"), "https://www.linkedin.com/in/john-smith/")
