import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.settings import HarvestActorSettings
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.connectors import CompanyRosterSnapshot, build_candidates_from_roster
from sourcing_agent.domain import Candidate
from sourcing_agent.enrichment import _classify_profile_membership, _merge_profile_into_candidate, _names_match, _profile_matches_candidate
from sourcing_agent.harvest_connectors import (
    HarvestCompanyEmployeesConnector,
    HarvestProfileConnector,
    HarvestProfileSearchConnector,
    _profile_scraper_mode,
    parse_harvest_company_employee_rows,
    parse_harvest_profile_payload,
    parse_harvest_search_rows,
)
from sourcing_agent.model_provider import DeterministicModelClient


class _AliasJudgingModelClient(DeterministicModelClient):
    def judge_company_equivalence(self, payload: dict[str, object]) -> dict[str, str]:
        observed = list(payload.get("observed_companies") or [])
        label = ""
        if observed and isinstance(observed[0], dict):
            label = str(observed[0].get("label") or "").strip()
        if label == "Acme Research Laboratory":
            return {
                "decision": "same_company",
                "matched_label": label,
                "confidence_label": "medium",
                "rationale": "Observed label is a long-form rendering of the same Acme Research Labs org.",
            }
        return super().judge_company_equivalence(payload)


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

    def test_profile_match_accepts_normalized_linkedin_identifier(self) -> None:
        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            aliases=["x.ai"],
        )
        candidate = Candidate(
            candidate_id="abc124",
            name_en="Jane Doe",
            display_name="Jane Doe",
            category="employee",
            target_company="xAI",
            employment_status="current",
            linkedin_url="https://linkedin.com/in/Jane-Doe/?trk=public-profile",
            metadata={"public_identifier": "JANE-DOE"},
            source_dataset="test_seed",
        )
        profile = {
            "full_name": "Jane Doe",
            "profile_url": "https://www.linkedin.com/in/jane-doe/",
            "public_identifier": "jane-doe",
            "current_company": "OtherCo",
            "experience": [],
        }
        self.assertTrue(_profile_matches_candidate(profile, candidate, identity))

    def test_profile_match_accepts_company_name_experience_for_former_membership(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            aliases=["thinking machines ai", "tml"],
        )
        candidate = Candidate(
            candidate_id="former123",
            name_en="Andrew Tulloch",
            display_name="Andrew Tulloch",
            category="former_employee",
            target_company="Thinking Machines Lab",
            employment_status="former",
            linkedin_url="https://www.linkedin.com/in/ACwAAAl87CYB51syHouA0_6lVOsv4VY3kpl4IH0",
            metadata={"seed_slug": "ACwAAAl87CYB51syHouA0_6lVOsv4VY3kpl4IH0"},
            source_dataset="test_seed",
        )
        profile = {
            "full_name": "Andrew Tulloch",
            "profile_url": "https://www.linkedin.com/in/andrew-tulloch-17238745",
            "public_identifier": "andrew-tulloch-17238745",
            "current_company": "Meta",
            "experience": [
                {"companyName": "Meta", "title": "AI Researcher"},
                {"companyName": "Thinking Machines Lab", "title": "Member of Technical Staff"},
            ],
        }
        self.assertTrue(_profile_matches_candidate(profile, candidate, identity))
        self.assertEqual(_classify_profile_membership(profile, identity), ("former_employee", "former"))

    def test_profile_match_rejects_distinct_neighbor_company_names(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            aliases=["thinking machines ai", "tml"],
        )
        candidate = Candidate(
            candidate_id="former124",
            name_en="Jasmine Santos",
            display_name="Jasmine Santos",
            category="former_employee",
            target_company="Thinking Machines Lab",
            employment_status="former",
            linkedin_url="https://www.linkedin.com/in/ACwAADPGf08BNsyS-C6NCOhA8sC6g-dK9Ke2c0c",
            metadata={"seed_slug": "ACwAADPGf08BNsyS-C6NCOhA8sC6g-dK9Ke2c0c"},
            source_dataset="test_seed",
        )
        profile = {
            "full_name": "Jasmine Santos",
            "profile_url": "https://www.linkedin.com/in/jasminemcsantos",
            "public_identifier": "jasminemcsantos",
            "current_company": "MedGrocer",
            "experience": [
                {"companyName": "MedGrocer", "title": "Population Health Supervisor"},
                {"companyName": "Thinking Machines Data Science", "title": "Geospatial Analytics Intern"},
            ],
        }
        self.assertFalse(_profile_matches_candidate(profile, candidate, identity))
        self.assertEqual(_classify_profile_membership(profile, identity), ("lead", ""))

        profile["experience"] = [
            {"companyName": "OpenAI", "title": "Researcher"},
            {"companyName": "Thinking Machines Corporation", "title": "Engineer"},
        ]
        self.assertFalse(_profile_matches_candidate(profile, candidate, identity))
        self.assertEqual(_classify_profile_membership(profile, identity), ("lead", ""))

    def test_profile_match_uses_ai_alias_fallback_only_for_similar_org_labels(self) -> None:
        identity = CompanyIdentity(
            requested_name="Acme Research Labs",
            canonical_name="Acme Research Labs",
            company_key="acmeresearchlabs",
            linkedin_slug="acme-research-labs",
        )
        candidate = Candidate(
            candidate_id="former125",
            name_en="Taylor Example",
            display_name="Taylor Example",
            category="former_employee",
            target_company="Acme Research Labs",
            employment_status="former",
            linkedin_url="https://www.linkedin.com/in/ACwAATaylorExample/",
            metadata={"seed_slug": "ACwAATaylorExample"},
            source_dataset="test_seed",
        )
        profile = {
            "full_name": "Taylor Example",
            "profile_url": "https://www.linkedin.com/in/taylor-example/",
            "public_identifier": "taylor-example",
            "current_company": "OtherCo",
            "experience": [
                {"companyName": "OtherCo", "title": "Research Engineer"},
                {"companyName": "Acme Research Laboratory", "title": "Research Engineer"},
            ],
        }
        self.assertFalse(_profile_matches_candidate(profile, candidate, identity))
        self.assertEqual(_classify_profile_membership(profile, identity), ("lead", ""))
        model_client = _AliasJudgingModelClient()
        self.assertTrue(_profile_matches_candidate(profile, candidate, identity, model_client=model_client))
        self.assertEqual(
            _classify_profile_membership(profile, identity, model_client=model_client),
            ("former_employee", "former"),
        )

    def test_names_match_supports_unicode_scripts(self) -> None:
        self.assertTrue(_names_match("الشيخ محمد علي الخليفة", "الشيخ محمد علي الخليفة"))
        self.assertTrue(_names_match("محمد علي", "الشيخ محمد علي الخليفة"))

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
                "pictureUrl": "https://cdn.example.com/john.png",
                "_meta": {"pagination": {"pageNumber": 3}},
            }
        ]
        rows = parse_harvest_company_employee_rows(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["full_name"], "John Smith")
        self.assertEqual(rows[0]["member_key"], "john-smith")
        self.assertEqual(rows[0]["avatar_url"], "https://cdn.example.com/john.png")
        self.assertEqual(rows[0]["page"], 3)
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
                request_manifest_path = Path(str(result["raw_path"])).with_name(f"{Path(str(result['raw_path'])).stem}.request.json")
                self.assertTrue(request_manifest_path.exists())
                manifest = json.loads(request_manifest_path.read_text())
                self.assertEqual(manifest["request_payload"]["pastCompanies"], ["https://www.linkedin.com/company/thinkingmachinesai/"])
                self.assertEqual(manifest["request_context"]["query_text"], "Thinking Machines Lab")
                self.assertEqual(manifest["request_context"]["employment_status"], "former")
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

    def test_harvest_profile_connector_batch_call_uses_urls_field(self) -> None:
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
        self.assertIn("urls", capture["payload"])
        self.assertNotIn("profileUrls", capture["payload"])

    def test_harvest_profile_connector_matches_batch_results_by_profile_url_not_order(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            requests_seen = []

            def _fake_run(_settings, payload):
                requests_seen.append(dict(payload))
                return [
                    {
                        "firstName": "John",
                        "lastName": "Smith",
                        "linkedinUrl": "https://www.linkedin.com/in/john-smith/",
                        "publicIdentifier": "john-smith",
                    },
                    {
                        "firstName": "Jane",
                        "lastName": "Doe",
                        "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                        "publicIdentifier": "jane-doe",
                    },
                ]

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                result = connector.fetch_profiles_by_urls(
                    [
                        "https://www.linkedin.com/in/jane-doe/",
                        "https://www.linkedin.com/in/john-smith/",
                    ],
                    Path(tempdir),
                    use_cache=False,
                )
                self.assertEqual(len(requests_seen), 1)
                self.assertEqual(result["https://www.linkedin.com/in/jane-doe/"]["parsed"]["full_name"], "Jane Doe")
                self.assertEqual(result["https://www.linkedin.com/in/john-smith/"]["parsed"]["full_name"], "John Smith")
                batch_request_paths = list((Path(tempdir) / "harvest_profiles").glob("harvest_profile_batch_*.request.json"))
                self.assertEqual(len(batch_request_paths), 1)
                batch_request_path = batch_request_paths[0]
                manifest = json.loads(batch_request_path.read_text())
                self.assertEqual(manifest["logical_name"], "harvest_profile_scraper_batch")
                self.assertEqual(
                    set(manifest["request_payload"]["urls"]),
                    {
                        "https://www.linkedin.com/in/jane-doe/",
                        "https://www.linkedin.com/in/john-smith/",
                    },
                )

    def test_harvest_profile_connector_single_url_falls_back_to_public_identifier(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            requests_seen = []

            def _fake_run(_settings, payload):
                requests_seen.append(dict(payload))
                if "urls" in payload:
                    return None
                if "publicIdentifiers" in payload:
                    return [
                        {
                            "firstName": "Ken",
                            "lastName": "Haase",
                            "linkedinUrl": "https://www.linkedin.com/in/kennethhaase",
                            "publicIdentifier": "kennethhaase",
                        }
                    ]
                return None

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                result = connector.fetch_profile_by_url(
                    "https://www.linkedin.com/in/ACwAAAAB0RcB6mT77J7DveIwR9POlj4mvnoMxhU",
                    Path(tempdir),
                    use_cache=False,
                )
        self.assertIsNotNone(result)
        self.assertEqual(result["parsed"]["full_name"], "Ken Haase")
        self.assertIn("urls", requests_seen[0])
        self.assertIn("publicIdentifiers", requests_seen[1])

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
            request_manifest_path = snapshot_dir / "harvest_company_employees" / "harvest_company_employees_raw.request.json"
            self.assertTrue(request_manifest_path.exists())
            manifest = json.loads(request_manifest_path.read_text())
            self.assertEqual(
                manifest["request_payload"]["companies"],
                ["https://www.linkedin.com/company/thinkingmachinesai/"],
            )
            self.assertEqual(manifest["request_context"]["cache_status"], "live_test_bridge")
        self.assertEqual(len(snapshot.visible_entries), 1)
        self.assertEqual(snapshot.visible_entries[0]["full_name"], "Mira Murati")

    def test_harvest_company_employees_supports_multi_page_fetch_budget(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="token",
            actor_id="actor",
            default_mode="short",
            max_total_charge_usd=0.2,
            max_paid_items=25,
        )
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "thinkingmachineslab" / "snap2"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            capture = {}

            def _fake_run(actor_settings, payload):
                capture["payload"] = dict(payload)
                capture["max_paid_items"] = actor_settings.max_paid_items
                capture["max_total_charge_usd"] = actor_settings.max_total_charge_usd
                return [
                    {
                        "firstName": "Ada",
                        "lastName": "Lovelace",
                        "linkedinUrl": "https://www.linkedin.com/in/ada-lovelace/",
                        "publicIdentifier": "ada-lovelace",
                        "_meta": {"pagination": {"pageNumber": 1}},
                    },
                    {
                        "firstName": "Grace",
                        "lastName": "Hopper",
                        "linkedinUrl": "https://www.linkedin.com/in/grace-hopper/",
                        "publicIdentifier": "grace-hopper",
                        "_meta": {"pagination": {"pageNumber": 2}},
                    },
                ]

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                snapshot = connector.fetch_company_roster(identity, snapshot_dir, max_pages=8, page_limit=50)

        self.assertEqual(capture["payload"]["takePages"], 8)
        self.assertEqual(capture["payload"]["maxItems"], 200)
        self.assertGreaterEqual(capture["max_paid_items"], 200)
        self.assertGreaterEqual(capture["max_total_charge_usd"], 0.8)
        self.assertEqual(len(snapshot.visible_entries), 2)
        self.assertEqual([item["page"] for item in snapshot.page_summaries], [1, 2])
        self.assertEqual(snapshot.page_summaries[0]["entry_count"], 1)
        self.assertEqual(snapshot.page_summaries[1]["entry_count"], 1)

    def test_harvest_profile_batch_execute_with_checkpoint_submits_async_run(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "xai" / "snap-async"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch(
                "sourcing_agent.harvest_connectors._load_cached_harvest_payload",
                return_value=(None, None, None),
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                return_value={"data": {"id": "run-123", "defaultDatasetId": "dataset-123", "status": "RUNNING"}},
            ):
                result = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                )
        self.assertTrue(result.pending)
        self.assertEqual(result.checkpoint["run_id"], "run-123")
        self.assertEqual(result.checkpoint["dataset_id"], "dataset-123")
        self.assertEqual(result.checkpoint["status"], "submitted")
        self.assertEqual(result.artifacts[0].label, "run_post")

    def test_harvest_company_execute_with_checkpoint_polls_and_caches_dataset(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            linkedin_company_url="https://www.linkedin.com/company/xai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "xai" / "snap-cache"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch(
                "sourcing_agent.harvest_connectors._load_cached_harvest_payload",
                return_value=(None, None, None),
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                return_value={"data": {"id": "run-456", "defaultDatasetId": "dataset-456", "status": "SUCCEEDED"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
                return_value=[
                    {
                        "firstName": "Ada",
                        "lastName": "Lovelace",
                        "linkedinUrl": "https://www.linkedin.com/in/ada-lovelace/",
                    }
                ],
            ):
                result = connector.execute_with_checkpoint(
                    identity,
                    snapshot_dir,
                    max_pages=1,
                    page_limit=25,
                    checkpoint={"run_id": "run-456", "dataset_id": "dataset-456", "status": "running"},
                )
                cache_files = list((Path(tempdir) / "runtime" / "provider_cache" / "harvest_company_employees").glob("*.json"))
        self.assertFalse(result.pending)
        self.assertEqual(result.checkpoint["status"], "completed")
        self.assertEqual(len(result.body), 1)
        self.assertTrue(any(path.name.endswith(".request.json") for path in cache_files))
        self.assertTrue(any(path.name.endswith(".json") and not path.name.endswith(".request.json") for path in cache_files))

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
