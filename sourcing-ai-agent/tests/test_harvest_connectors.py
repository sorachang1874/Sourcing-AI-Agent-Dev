import base64
import json
import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha1
from pathlib import Path
from typing import Any
from unittest.mock import call, patch
from urllib import parse as urlparse

from sourcing_agent.connectors import (
    CompanyIdentity,
    CompanyRosterSnapshot,
    _canonical_company_key_for_identity,
    build_candidates_from_roster,
)
from sourcing_agent.domain import Candidate
from sourcing_agent.enrichment import (
    _classify_profile_membership,
    _merge_profile_into_candidate,
    _names_match,
    _profile_matches_candidate,
)
from sourcing_agent.harvest_connectors import (
    HarvestCompanyEmployeesConnector,
    HarvestProfileConnector,
    HarvestProfileSearchConnector,
    HarvestRetryableRequestError,
    _apply_harvest_search_filters,
    _get_harvest_actor_run,
    _get_harvest_dataset_items,
    _harvest_json_request,
    _load_cached_harvest_payload,
    _persist_shared_harvest_payload,
    _profile_scraper_mode,
    _recommended_harvest_company_timeout_seconds,
    _recommended_harvest_profile_charge_cap_usd,
    _recommended_harvest_profile_timeout_seconds,
    _run_harvest_actor_via_async_dataset,
    _runtime_dir_from_path,
    _submit_harvest_actor_run,
    parse_harvest_company_employee_rows,
    parse_harvest_company_employee_run_log,
    parse_harvest_profile_payload,
    parse_harvest_search_rows,
)
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.runtime_environment import LiveProviderAccessError
from sourcing_agent.scripted_provider_scenario import load_scripted_provider_invocations
from sourcing_agent.settings import HarvestActorSettings
from tests.fake_apify_provider import FakeApifyProvider
from tests.fake_provider_http import FakeProviderHTTPServer


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
    def setUp(self) -> None:
        # The provider-mode default is now fail-closed (simulate) — see
        # runtime_environment.SAFE_DEFAULT_PROVIDER_MODE. This suite predominantly
        # exercises the LIVE harvest dispatch path (with the network mocked), so it
        # must opt into live explicitly, including the non-production dual-confirm.
        # Tests that want a non-live mode override SOURCING_EXTERNAL_PROVIDER_MODE
        # in their own patch.dict(...).
        live_env_patch = patch.dict(
            os.environ,
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
                "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
            },
            clear=False,
        )
        live_env_patch.start()
        self.addCleanup(live_env_patch.stop)

    def _openai_agent_streaming_scenario_path(self) -> Path:
        return (
            Path(__file__).resolve().parents[1]
            / "configs"
            / "scripted"
            / "openai_agent_scoped_delta_streaming.json"
        )

    def _openai_chatgpt_streaming_scenario_path(self) -> Path:
        return (
            Path(__file__).resolve().parents[1]
            / "configs"
            / "scripted"
            / "openai_chatgpt_scoped_delta_streaming.json"
        )

    def _lovable_live_roster_scenario_path(self) -> Path:
        return Path(__file__).resolve().parents[1] / "configs" / "scripted" / "lovable_live_roster.json"

    def test_runtime_dir_from_path_prefers_configured_nested_test_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_runtime = Path(tempdir) / "runtime"
            isolated_runtime = repo_runtime / "test_env" / "scripted_case"
            snapshot_dir = isolated_runtime / "company_assets" / "google" / "snap-1"
            snapshot_dir.mkdir(parents=True, exist_ok=True)

            with patch.dict("os.environ", {"SOURCING_RUNTIME_DIR": str(isolated_runtime)}):
                self.assertEqual(_runtime_dir_from_path(snapshot_dir).resolve(), isolated_runtime.resolve())

    def test_runtime_dir_from_path_prefers_nested_test_runtime_over_outer_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_runtime = Path(tempdir) / "runtime"
            isolated_runtime = repo_runtime / "test_env" / "scripted_case"
            snapshot_dir = isolated_runtime / "company_assets" / "openai" / "snap-1"
            snapshot_dir.mkdir(parents=True, exist_ok=True)

            with patch.dict("os.environ", {"SOURCING_RUNTIME_DIR": str(repo_runtime)}):
                self.assertEqual(_runtime_dir_from_path(snapshot_dir).resolve(), isolated_runtime.resolve())

    def test_runtime_dir_from_path_prefers_nested_test_runtime_without_env(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            isolated_runtime = Path(tempdir) / "runtime" / "test_env" / "scripted_case"
            snapshot_dir = isolated_runtime / "company_assets" / "openai" / "snap-1"
            snapshot_dir.mkdir(parents=True, exist_ok=True)

            with patch.dict("os.environ", {}, clear=True):
                self.assertEqual(_runtime_dir_from_path(snapshot_dir).resolve(), isolated_runtime.resolve())

    def test_canonical_company_key_for_identity_uses_registry_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            identity_path = (
                runtime_dir
                / "company_assets"
                / "physicalintelligence"
                / "20260415T010203"
                / "identity.json"
            )
            identity_path.parent.mkdir(parents=True, exist_ok=True)
            identity_path.write_text(
                json.dumps(
                    {
                        "requested_name": "Physical Intelligence",
                        "canonical_name": "Physical Intelligence",
                        "company_key": "physicalintelligence",
                        "linkedin_slug": "physical-intelligence-company",
                        "aliases": ["pi"],
                        "confidence": "high",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                self.assertEqual(
                    _canonical_company_key_for_identity(
                        requested_name="PI",
                        label="Physical Intelligence",
                        linkedin_slug="physical-intelligence-company",
                        fallback_company_key="pi",
                    ),
                    "physicalintelligence",
                )

    def test_apply_harvest_search_filters_carries_locations_for_former_scope(self) -> None:
        payload = {}
        _apply_harvest_search_filters(
            payload,
            {
                "past_companies": ["https://www.linkedin.com/company/anthropicresearch/"],
                "locations": ["United States"],
                "exclude_locations": ["Canada"],
                "function_ids": ["8", "24"],
                "exclude_function_ids": ["25"],
                "keywords": ["Anthropic"],
            },
            "former",
        )
        self.assertEqual(payload["pastCompanies"], ["https://www.linkedin.com/company/anthropicresearch/"])
        self.assertEqual(payload["locations"], ["United States"])
        self.assertEqual(payload["excludeLocations"], ["Canada"])
        self.assertEqual(payload["functionIds"], ["8", "24"])
        self.assertEqual(payload["excludeFunctionIds"], ["25"])
        self.assertNotIn("searchQuery", payload)

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
            "languages": [{"name": "English"}, {"name": "Mandarin"}],
            "skills": [{"name": "Python"}, {"name": "LLMs"}],
            "emails": [
                {
                    "email": "jane@x.ai",
                    "status": "valid",
                    "qualityScore": 92,
                    "foundInLinkedInProfile": True,
                }
            ],
            "moreProfiles": [{"url": "https://www.linkedin.com/in/jane-doe-2/"}],
        }
        parsed = parse_harvest_profile_payload(payload)
        self.assertEqual(parsed["full_name"], "Jane Doe")
        self.assertEqual(parsed["profile_url"], "https://www.linkedin.com/in/jane-doe/")
        self.assertEqual(parsed["current_company"], "xAI")
        self.assertEqual(parsed["location"], "San Francisco Bay Area")
        self.assertEqual(parsed["primary_email"], "jane@x.ai")
        self.assertEqual(parsed["primary_email_metadata"]["source"], "harvestapi")
        self.assertEqual(parsed["primary_email_metadata"]["qualityScore"], 92)
        self.assertTrue(parsed["primary_email_metadata"]["foundInLinkedInProfile"])
        self.assertEqual(len(parsed["languages"]), 2)
        self.assertEqual(len(parsed["skills"]), 2)
        self.assertEqual(len(parsed["more_profiles"]), 1)

    def test_parse_harvest_profile_payload_rejects_risky_low_quality_email(self) -> None:
        payload = {
            "firstName": "Jane",
            "lastName": "Doe",
            "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
            "emails": [
                {
                    "email": "risky@x.ai",
                    "status": "risky",
                    "qualityScore": 60,
                }
            ],
        }
        parsed = parse_harvest_profile_payload(payload)
        self.assertEqual(parsed["primary_email"], "")
        self.assertEqual(parsed["primary_email_metadata"]["status"], "risky")
        self.assertEqual(parsed["primary_email_metadata"]["qualityScore"], 60)

    def test_parse_harvest_profile_payload_keeps_provider_profile_url_even_with_requested_opaque_url(self) -> None:
        payload = {
            "_harvest_request": {
                "kind": "url",
                "value": "https://www.linkedin.com/in/ACwAACi6kRQBOm_dcRKuOnhoIVJZLitAECAHwp0",
                "profile_url": "https://www.linkedin.com/in/ACwAACi6kRQBOm_dcRKuOnhoIVJZLitAECAHwp0",
            },
            "item": {
                "firstName": "Edison",
                "lastName": "Li",
                "headline": "Anthropic Senior Staff AI Research Scientist",
                "linkedinUrl": "https://www.linkedin.com/in/ellamine-ibrahim",
                "publicIdentifier": "ellamine-ibrahim",
            },
        }
        parsed = parse_harvest_profile_payload(payload)
        self.assertEqual(
            parsed["profile_url"],
            "https://www.linkedin.com/in/ellamine-ibrahim",
        )
        self.assertEqual(
            parsed["requested_profile_url"],
            "https://www.linkedin.com/in/ACwAACi6kRQBOm_dcRKuOnhoIVJZLitAECAHwp0",
        )

    def test_parse_harvest_profile_payload_keeps_provider_vanity_when_name_aligned(self) -> None:
        payload = {
            "_harvest_request": {
                "kind": "url",
                "value": "https://www.linkedin.com/in/ACwAAOldFormer",
                "profile_url": "https://www.linkedin.com/in/ACwAAOldFormer",
            },
            "item": {
                "firstName": "Former",
                "lastName": "Example",
                "headline": "Research Engineer at NewCo",
                "linkedinUrl": "https://www.linkedin.com/in/former-example/",
                "publicIdentifier": "former-example",
            },
        }
        parsed = parse_harvest_profile_payload(payload)
        self.assertEqual(parsed["profile_url"], "https://www.linkedin.com/in/former-example/")

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

    def test_former_profile_merge_can_upgrade_to_current_membership(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
            aliases=["anthropicresearch"],
        )
        candidate = Candidate(
            candidate_id="former-upgrade-1",
            name_en="Deanna Graham",
            display_name="Deanna Graham",
            category="former_employee",
            target_company="Anthropic",
            employment_status="former",
            linkedin_url="https://www.linkedin.com/in/deannagraham2023",
            source_dataset="test_seed",
        )
        profile = {
            "full_name": "Deanna Graham",
            "headline": "Head of Marketing Insights & Research at Anthropic",
            "profile_url": "https://www.linkedin.com/in/deannagraham2023",
            "public_identifier": "deannagraham2023",
            "summary": "Marketing and insights leader.",
            "location": "San Francisco, California, United States",
            "current_company": "Anthropic",
            "experience": [{"companyName": "Anthropic", "title": "Head of Marketing Insights & Research"}],
            "education": [],
            "publications": [],
            "more_profiles": [],
        }
        merged = _merge_profile_into_candidate(candidate, profile, Path("/tmp/profile.json"), "harvest_profile_scraper", identity)
        self.assertEqual(merged.category, "employee")
        self.assertEqual(merged.employment_status, "current")

    def test_current_profile_merge_can_downgrade_to_former_membership(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            aliases=["thinking machines ai", "tml"],
        )
        candidate = Candidate(
            candidate_id="current-downgrade-1",
            name_en="Andrew Tulloch",
            display_name="Andrew Tulloch",
            category="employee",
            target_company="Thinking Machines Lab",
            employment_status="current",
            linkedin_url="https://www.linkedin.com/in/andrew-tulloch-17238745",
            source_dataset="test_seed",
        )
        profile = {
            "full_name": "Andrew Tulloch",
            "headline": "AI Researcher at Meta",
            "profile_url": "https://www.linkedin.com/in/andrew-tulloch-17238745",
            "public_identifier": "andrew-tulloch-17238745",
            "summary": "AI Researcher at Meta.",
            "location": "London, England, United Kingdom",
            "current_company": "Meta",
            "experience": [
                {"companyName": "Meta", "title": "AI Researcher"},
                {"companyName": "Thinking Machines Lab", "title": "Member of Technical Staff"},
            ],
            "education": [],
            "publications": [],
            "more_profiles": [],
        }
        merged = _merge_profile_into_candidate(candidate, profile, Path("/tmp/profile.json"), "harvest_profile_scraper", identity)
        self.assertEqual(merged.category, "former_employee")
        self.assertEqual(merged.employment_status, "former")

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

    def test_profile_match_accepts_requested_opaque_identifier_even_when_profile_name_is_blank(self) -> None:
        identity = CompanyIdentity(
            requested_name="Physical Intelligence",
            canonical_name="Physical Intelligence",
            company_key="physicalintelligence",
            linkedin_slug="physical-intelligence-company",
            aliases=["pi"],
        )
        candidate = Candidate(
            candidate_id="opaque124",
            name_en="Mallorie Kiunke",
            display_name="Mallorie Kiunke",
            category="employee",
            target_company="Physical Intelligence",
            employment_status="current",
            linkedin_url="https://www.linkedin.com/in/ACwAAGHgIsUB7ek0tCifNrkXZIdXlPTOcOVq5k8",
            source_dataset="test_seed",
        )
        profile = {
            "full_name": "",
            "requested_profile_url": "https://www.linkedin.com/in/ACwAAGHgIsUB7ek0tCifNrkXZIdXlPTOcOVq5k8",
            "profile_url": "https://www.linkedin.com/in/mallorie-kiunke-199b19399",
            "public_identifier": "mallorie-kiunke-199b19399",
            "experience": [
                {"companyName": "Physical Intelligence", "title": "Research Engineer"},
            ],
            "education": [{"schoolName": "UC Berkeley"}],
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

    def test_persist_profiles_from_batch_body_writes_individual_profile_payloads(self) -> None:
        connector = HarvestProfileConnector(
            HarvestActorSettings(
                enabled=True,
                api_token="token",
                actor_id="actor",
            )
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            requested_urls = [
                "https://www.linkedin.com/in/jane-doe/",
                "https://www.linkedin.com/in/john-smith/",
            ]
            body = [
                {
                    "firstName": "Jane",
                    "lastName": "Doe",
                    "headline": "Research Engineer at Anthropic",
                    "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                    "publicIdentifier": "jane-doe",
                },
                {
                    "firstName": "John",
                    "lastName": "Smith",
                    "headline": "Engineer at Anthropic",
                    "linkedinUrl": "https://www.linkedin.com/in/john-smith/",
                    "publicIdentifier": "john-smith",
                },
            ]
            persisted = connector.persist_profiles_from_batch_body(
                requested_urls,
                body,
                snapshot_dir,
            )
            self.assertEqual(len(persisted["profiles"]), 2)
            self.assertEqual(persisted["unresolved_urls"], [])
            for url in requested_urls:
                item = persisted["profiles"][url]
                self.assertTrue(Path(item["raw_path"]).exists())
                self.assertEqual(item["account_id"], "harvest_profile_scraper")
                self.assertEqual(item["parsed"]["profile_url"], url)

    def test_persist_profiles_from_batch_body_keeps_error_payloads_unresolved(self) -> None:
        connector = HarvestProfileConnector(
            HarvestActorSettings(
                enabled=True,
                api_token="token",
                actor_id="actor",
            )
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            requested_urls = [
                "https://www.linkedin.com/in/jane-doe/",
                "https://www.linkedin.com/in/ACwAAOpaque404",
            ]
            body = [
                {
                    "firstName": "Jane",
                    "lastName": "Doe",
                    "headline": "Research Engineer at Anthropic",
                    "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                    "publicIdentifier": "jane-doe",
                },
                {
                    "originalQuery": {"url": "https://www.linkedin.com/in/ACwAAOpaque404"},
                    "status": 404,
                    "error": "Could not find profileId or publicIdentifier",
                },
            ]
            persisted = connector.persist_profiles_from_batch_body(
                requested_urls,
                body,
                snapshot_dir,
            )
            self.assertEqual(set(persisted["profiles"].keys()), {"https://www.linkedin.com/in/jane-doe/"})
            self.assertEqual(persisted["unresolved_urls"], ["https://www.linkedin.com/in/ACwAAOpaque404"])

    def test_harvest_profile_connector_ignores_invalid_cached_raw_payload_and_refetches(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        requested_url = "https://www.linkedin.com/in/ACwAAOpaque404"
        with tempfile.TemporaryDirectory() as tempdir:
            raw_path = Path(tempdir) / "harvest_profiles" / f"{sha1(requested_url.encode('utf-8')).hexdigest()[:16]}.json"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(
                json.dumps(
                    {
                        "_harvest_request": {"kind": "url", "value": requested_url, "profile_url": requested_url},
                        "item": {
                            "status": 404,
                            "error": "Could not find profileId or publicIdentifier",
                            "originalQuery": {"url": requested_url},
                        },
                    }
                ),
                encoding="utf-8",
            )

            def _fake_run(_settings, payload, **kwargs):
                return [
                    {
                        "firstName": "Mallorie",
                        "lastName": "Kiunke",
                        "linkedinUrl": "https://www.linkedin.com/in/mallorie-kiunke-199b19399",
                        "publicIdentifier": "mallorie-kiunke-199b19399",
                        "originalQuery": {"url": requested_url},
                    }
                ]

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                result = connector.fetch_profiles_by_urls([requested_url], Path(tempdir), use_cache=True)

        self.assertIn(requested_url, result)
        self.assertEqual(
            result[requested_url]["parsed"]["profile_url"],
            "https://www.linkedin.com/in/mallorie-kiunke-199b19399",
        )

    def test_large_harvest_company_timeout_is_more_conservative(self) -> None:
        self.assertEqual(_recommended_harvest_company_timeout_seconds(25), 300)
        self.assertEqual(_recommended_harvest_company_timeout_seconds(2500), 1500)

    def test_fetch_profiles_by_urls_emits_batch_result_callback_after_provider_response(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        requested_url = "https://www.linkedin.com/in/streaming-profile/"
        batches: list[dict[str, Any]] = []

        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "openai" / "snap-stream"
            snapshot_dir.mkdir(parents=True, exist_ok=True)

            def _fake_run(_settings, payload, **kwargs):
                self.assertEqual(payload["urls"], [requested_url])
                return [
                    {
                        "fullName": "Streaming Profile",
                        "headline": "Research Engineer",
                        "linkedinUrl": requested_url,
                        "publicIdentifier": "streaming-profile",
                        "originalQuery": {"url": requested_url},
                    }
                ]

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                result = connector.fetch_profiles_by_urls(
                    [requested_url],
                    snapshot_dir,
                    use_cache=False,
                    on_batch_result=batches.append,
                )

        self.assertIn(requested_url, result)
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["response_level"], "batch")
        self.assertEqual(batches[0]["requested_urls"], [requested_url])
        self.assertEqual(batches[0]["profile_count"], 1)
        self.assertIn(requested_url, batches[0]["profiles"])

    def test_large_harvest_profile_timeout_and_charge_budget_are_more_conservative(self) -> None:
        self.assertEqual(_recommended_harvest_profile_timeout_seconds(25, collect_email=False), 300)
        self.assertEqual(_recommended_harvest_profile_timeout_seconds(100, collect_email=True), 900)
        self.assertEqual(
            _recommended_harvest_profile_charge_cap_usd(
                "Profile details + email search ($10 per 1k)",
                100,
                fallback_per_1k=10.0,
            ),
            1.25,
        )

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

        identifier_only_rows = parse_harvest_company_employee_rows(
            [{"fullName": "Ada Lovelace", "publicIdentifier": "ada-lovelace"}]
        )
        self.assertEqual(identifier_only_rows[0]["linkedin_url"], "https://www.linkedin.com/in/ada-lovelace/")
        self.assertEqual(identifier_only_rows[0]["public_identifier"], "ada-lovelace")

    def test_profile_scraper_mode_uses_current_actor_enum(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        self.assertEqual(_profile_scraper_mode(settings), "Profile details no email ($4 per 1k)")

    def test_harvest_profile_batch_payload_explicitly_disables_email_lookup_when_collect_email_is_false(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="token",
            actor_id="actor",
            default_mode="full",
            collect_email=False,
        )
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "openai" / "snap-email-off"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            captured: dict[str, object] = {}

            def _fake_submit(actor_settings, payload, **kwargs):
                captured["payload"] = dict(payload)
                return {"data": {"id": "run-email-off", "defaultDatasetId": "dataset-email-off", "status": "RUNNING"}}

            with patch(
                "sourcing_agent.harvest_connectors._load_cached_harvest_payload",
                return_value=(None, None, None),
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=_fake_submit,
            ):
                connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                )

        self.assertEqual(
            captured["payload"],
            {
                "urls": ["https://www.linkedin.com/in/jane-doe/"],
                "profileScraperMode": "Profile details no email ($4 per 1k)",
                "findEmail": False,
            },
        )

    def test_harvest_profile_search_supports_multi_page_former_search(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short", max_paid_items=25)
        connector = HarvestProfileSearchConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            capture = {}

            def _fake_run(actor_settings, payload, **kwargs):
                capture["payload"] = dict(payload)
                capture["max_total_charge_usd"] = actor_settings.max_total_charge_usd
                capture["timeout_seconds"] = actor_settings.timeout_seconds
                return [
                    {
                        "firstName": "Alexis",
                        "lastName": "Dunn",
                        "linkedinUrl": "https://www.linkedin.com/in/alexis-aleyza-dunn/",
                        "_meta": {
                            "pagination": {
                                "totalElements": 559,
                                "totalPages": 23,
                                "pageNumber": 1,
                                "pageSize": 25,
                            }
                        },
                    }
                ]

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                result = connector.search_profiles(
                    query_text="Thinking Machines Lab",
                    filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                    employment_status="former",
                    discovery_dir=Path(tempdir),
                    limit=500,
                    pages=20,
                )
                request_manifest_path = Path(str(result["raw_path"])).with_name(f"{Path(str(result['raw_path'])).stem}.request.json")
                self.assertTrue(request_manifest_path.exists())
                manifest = json.loads(request_manifest_path.read_text())
                self.assertEqual(manifest["request_payload"]["pastCompanies"], ["https://www.linkedin.com/company/thinkingmachinesai/"])
                self.assertEqual(manifest["request_context"]["query_text"], "Thinking Machines Lab")
                self.assertEqual(manifest["request_context"]["employment_status"], "former")
        self.assertIsNotNone(result)
        self.assertEqual(capture["payload"]["takePages"], 20)
        self.assertEqual(capture["payload"]["maxItems"], 500)
        self.assertGreaterEqual(capture["max_total_charge_usd"], 2.3)
        self.assertGreaterEqual(capture["timeout_seconds"], 400)
        self.assertEqual(
            capture["payload"]["pastCompanies"],
            ["https://www.linkedin.com/company/thinkingmachinesai/"],
        )
        self.assertEqual(result["pagination"]["total_elements"], 559)
        self.assertEqual(result["pagination"]["total_pages"], 23)

    def test_harvest_profile_search_caps_request_to_provider_limit(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short", max_paid_items=25)
        connector = HarvestProfileSearchConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            capture = {}

            def _fake_run(actor_settings, payload, **kwargs):
                capture["payload"] = dict(payload)
                capture["max_paid_items"] = actor_settings.max_paid_items
                return []

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                connector.search_profiles(
                    query_text="",
                    filter_hints={"past_companies": ["https://www.linkedin.com/company/google/"]},
                    employment_status="former",
                    discovery_dir=Path(tempdir),
                    limit=100000,
                    pages=500,
                    auto_probe=False,
                )

        self.assertEqual(capture["payload"]["takePages"], 100)
        self.assertEqual(capture["payload"]["maxItems"], 2500)
        self.assertEqual(capture["max_paid_items"], 2500)

    def test_harvest_profile_search_serializes_duplicate_payload_dispatch(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short", max_paid_items=25)
        connector = HarvestProfileSearchConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            discovery_dir = Path(tempdir)
            release_first_call = threading.Event()
            first_call_started = threading.Event()

            def _fake_run(_actor_settings, _payload, **_kwargs):
                first_call_started.set()
                release_first_call.wait(timeout=5)
                return [
                    {
                        "firstName": "Alex",
                        "lastName": "Agent",
                        "linkedinUrl": "https://www.linkedin.com/in/alex-agent/",
                        "_meta": {
                            "pagination": {
                                "totalElements": 1,
                                "totalPages": 1,
                                "pageNumber": 1,
                                "pageSize": 25,
                            }
                        },
                    }
                ]

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run) as run_mock:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    first_future = executor.submit(
                        connector.search_profiles,
                        query_text="",
                        filter_hints={"past_companies": ["https://www.linkedin.com/company/lovable/"]},
                        employment_status="former",
                        discovery_dir=discovery_dir,
                        limit=173,
                        pages=7,
                        auto_probe=False,
                        allow_shared_provider_cache=False,
                    )
                    self.assertTrue(first_call_started.wait(timeout=5))
                    second_future = executor.submit(
                        connector.search_profiles,
                        query_text="",
                        filter_hints={"past_companies": ["https://www.linkedin.com/company/lovable/"]},
                        employment_status="former",
                        discovery_dir=discovery_dir,
                        limit=173,
                        pages=7,
                        auto_probe=False,
                        allow_shared_provider_cache=False,
                    )
                    release_first_call.set()
                    first_result = first_future.result(timeout=5)
                    second_result = second_future.result(timeout=5)

            self.assertEqual(run_mock.call_count, 1)
            self.assertEqual(first_result["pagination"]["total_elements"], 1)
            self.assertEqual(second_result["pagination"]["total_elements"], 1)
            self.assertEqual(first_result["rows"][0]["full_name"], "Alex Agent")

    def test_harvest_profile_search_waits_for_dispatch_lock_raw_cache(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short", max_paid_items=25)
        connector = HarvestProfileSearchConnector(settings)
        body = [
            {
                "firstName": "Recovered",
                "lastName": "Agent",
                "linkedinUrl": "https://www.linkedin.com/in/recovered-agent/",
                "_meta": {
                    "pagination": {
                        "totalElements": 1,
                        "totalPages": 1,
                        "pageNumber": 1,
                        "pageSize": 25,
                    }
                },
            }
        ]
        with tempfile.TemporaryDirectory() as tempdir:
            discovery_dir = Path(tempdir)
            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", return_value=body):
                primed = connector.search_profiles(
                    query_text="",
                    filter_hints={"past_companies": ["https://www.linkedin.com/company/openai/"]},
                    employment_status="former",
                    discovery_dir=discovery_dir,
                    limit=78,
                    pages=4,
                    auto_probe=False,
                    allow_shared_provider_cache=False,
                )
            raw_path = Path(str(primed["raw_path"]))
            lock_path = raw_path.with_name(f"{raw_path.stem}.dispatch.lock")
            raw_path.unlink()
            lock_path.write_text(
                json.dumps(
                    {
                        "payload_key": raw_path.stem,
                        "created_epoch_seconds": time.time(),
                        "pid": 999999,
                    }
                ),
                encoding="utf-8",
            )

            def _publish_raw_cache() -> None:
                time.sleep(0.05)
                raw_path.write_text(json.dumps(body), encoding="utf-8")

            publisher = threading.Thread(target=_publish_raw_cache)
            publisher.start()
            try:
                with patch("sourcing_agent.harvest_connectors._run_harvest_actor") as run_mock:
                    result = connector.search_profiles(
                        query_text="",
                        filter_hints={"past_companies": ["https://www.linkedin.com/company/openai/"]},
                        employment_status="former",
                        discovery_dir=discovery_dir,
                        limit=78,
                        pages=4,
                        auto_probe=False,
                        allow_shared_provider_cache=False,
                    )
            finally:
                publisher.join(timeout=5)
                lock_path.unlink(missing_ok=True)

        run_mock.assert_not_called()
        self.assertEqual(result["pagination"]["total_elements"], 1)
        self.assertEqual(result["rows"][0]["full_name"], "Recovered Agent")

    def test_harvest_profile_search_retries_transient_zero_result(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short", max_paid_items=25)
        connector = HarvestProfileSearchConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            bodies = [
                [],
                [
                    {
                        "firstName": "Gemini",
                        "lastName": "Researcher",
                        "linkedinUrl": "https://www.linkedin.com/in/gemini-researcher/",
                        "_meta": {
                            "pagination": {
                                "totalElements": 1,
                                "totalPages": 1,
                                "pageNumber": 1,
                                "pageSize": 25,
                            }
                        },
                    }
                ],
            ]

            def _fake_run(_actor_settings, _payload, **_kwargs):
                return bodies.pop(0)

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run) as run_mock:
                result = connector.search_profiles(
                    query_text="Gemini",
                    filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                    employment_status="current",
                    discovery_dir=Path(tempdir),
                    limit=25,
                    pages=1,
                    auto_probe=False,
                    zero_result_retry_attempts=2,
                )

        self.assertEqual(run_mock.call_count, 2)
        self.assertEqual(result["rows"][0]["full_name"], "Gemini Researcher")
        self.assertEqual(result["zero_result_retry"]["retry_count"], 1)
        self.assertFalse(result["zero_result_retry"]["exhausted"])

    def test_harvest_profile_search_ignores_cached_zero_result_when_retrying_live(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short", max_paid_items=25)
        connector = HarvestProfileSearchConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            first_body = []
            second_body = [
                {
                    "firstName": "Recovered",
                    "lastName": "Lead",
                    "linkedinUrl": "https://www.linkedin.com/in/recovered-lead/",
                    "_meta": {
                        "pagination": {
                            "totalElements": 1,
                            "totalPages": 1,
                            "pageNumber": 1,
                            "pageSize": 25,
                        }
                    },
                }
            ]

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", return_value=first_body):
                cached_empty = connector.search_profiles(
                    query_text="Gemini",
                    filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                    employment_status="current",
                    discovery_dir=Path(tempdir),
                    limit=25,
                    pages=1,
                    auto_probe=False,
                    zero_result_retry_attempts=0,
                )
            self.assertEqual(cached_empty["rows"], [])

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", return_value=second_body) as run_mock:
                recovered = connector.search_profiles(
                    query_text="Gemini",
                    filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                    employment_status="current",
                    discovery_dir=Path(tempdir),
                    limit=25,
                    pages=1,
                    auto_probe=False,
                    zero_result_retry_attempts=1,
                )

        self.assertEqual(run_mock.call_count, 1)
        self.assertEqual(recovered["rows"][0]["full_name"], "Recovered Lead")

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

            def _fake_run(_settings, payload, **kwargs):
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

    def test_harvest_profile_connector_single_url_uses_batch_manifest_before_fallback(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            requests_seen = []

            def _fake_run(_settings, payload, **kwargs):
                requests_seen.append(dict(payload))
                return [
                    {
                        "firstName": "Jane",
                        "lastName": "Doe",
                        "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                        "publicIdentifier": "jane-doe",
                    }
                ]

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                result = connector.fetch_profiles_by_urls(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    Path(tempdir),
                    use_cache=False,
                )
            self.assertEqual(len(requests_seen), 1)
            self.assertEqual(
                requests_seen[0]["urls"],
                ["https://www.linkedin.com/in/jane-doe/"],
            )
            batch_request_paths = list((Path(tempdir) / "harvest_profiles").glob("harvest_profile_batch_*.request.json"))
            self.assertEqual(len(batch_request_paths), 1)
            manifest = json.loads(batch_request_paths[0].read_text())
            self.assertEqual(manifest["logical_name"], "harvest_profile_scraper_batch")
            self.assertEqual(
                manifest["request_payload"]["urls"],
                ["https://www.linkedin.com/in/jane-doe/"],
            )
            self.assertIn("https://www.linkedin.com/in/jane-doe/", result)

    def test_harvest_profile_connector_matches_batch_results_by_profile_url_not_order(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            requests_seen = []

            def _fake_run(_settings, payload, **kwargs):
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

    def test_harvest_profile_connector_matches_batch_results_by_original_query_url(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            requests_seen = []

            def _fake_run(_settings, payload, **kwargs):
                requests_seen.append(dict(payload))
                return [
                    {
                        "firstName": "Edison",
                        "lastName": "Li",
                        "linkedinUrl": "https://www.linkedin.com/in/ellamine-ibrahim",
                        "publicIdentifier": "ellamine-ibrahim",
                        "originalQuery": {
                            "url": "https://www.linkedin.com/in/ACwAAA0rngkBg1Y2wZtwA_at2ZRrf-3jQ4oMfqY"
                        },
                    },
                    {
                        "firstName": "Sai",
                        "lastName": "Ponnaganti",
                        "linkedinUrl": "https://www.linkedin.com/in/sai-ponnaganti",
                        "publicIdentifier": "sai-ponnaganti",
                        "originalQuery": {
                            "url": "https://www.linkedin.com/in/ACwAAAExampleOpaqueSai"
                        },
                    },
                ]

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                result = connector.fetch_profiles_by_urls(
                    [
                        "https://www.linkedin.com/in/ACwAAA0rngkBg1Y2wZtwA_at2ZRrf-3jQ4oMfqY",
                        "https://www.linkedin.com/in/ACwAAAExampleOpaqueSai",
                    ],
                    Path(tempdir),
                    use_cache=False,
                )
                self.assertEqual(len(requests_seen), 1)
                self.assertEqual(
                    result["https://www.linkedin.com/in/ACwAAA0rngkBg1Y2wZtwA_at2ZRrf-3jQ4oMfqY"]["parsed"]["profile_url"],
                    "https://www.linkedin.com/in/ellamine-ibrahim",
                )
                self.assertEqual(
                    result["https://www.linkedin.com/in/ACwAAAExampleOpaqueSai"]["parsed"]["profile_url"],
                    "https://www.linkedin.com/in/sai-ponnaganti",
                )

    def test_harvest_profile_connector_retries_smaller_batches_before_direct_fallback(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            requests_seen = []
            requested_urls = [f"https://www.linkedin.com/in/opaque-{index}" for index in range(6)]

            def _fake_run(_settings, payload, **kwargs):
                requests_seen.append(dict(payload))
                if "publicIdentifiers" in payload or "profileIds" in payload:
                    raise AssertionError("expected smaller batch retries before single fallback")
                urls = list(payload.get("urls") or [])
                if len(urls) == 6:
                    return None
                return [
                    {
                        "firstName": f"Person{index}",
                        "lastName": "Example",
                        "linkedinUrl": f"https://www.linkedin.com/in/person-{index}",
                        "publicIdentifier": f"person-{index}",
                        "originalQuery": {"url": url},
                    }
                    for index, url in enumerate(urls, start=1)
                ]

            with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}, clear=False), patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                side_effect=_fake_run,
            ):
                result = connector.fetch_profiles_by_urls(
                    requested_urls,
                    Path(tempdir),
                    use_cache=False,
                )
                self.assertEqual(len(result), 6)
                self.assertEqual(
                    [len(payload.get("urls") or []) for payload in requests_seen],
                    [6, 5, 1],
                )

    def test_harvest_profile_connector_mixed_success_retries_only_unresolved_urls(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        requested_urls = [f"https://www.linkedin.com/in/mixed-success-{index}/" for index in range(12)]
        initially_successful_urls = set(requested_urls[:6])
        with tempfile.TemporaryDirectory() as tempdir:
            requests_seen: list[list[str]] = []

            def _profile_payload(url: str) -> dict[str, Any]:
                slug = url.rstrip("/").rsplit("/", 1)[-1]
                return {
                    "fullName": slug.replace("-", " ").title(),
                    "linkedinUrl": url,
                    "publicIdentifier": slug,
                    "originalQuery": {"url": url},
                }

            def _fake_run(_settings, payload, **kwargs):
                urls = list(payload.get("urls") or [])
                requests_seen.append(urls)
                if len(urls) == len(requested_urls):
                    return [_profile_payload(url) for url in requested_urls[:6]]
                return [_profile_payload(url) for url in urls]

            with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}, clear=False), patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                side_effect=_fake_run,
            ):
                result = connector.fetch_profiles_by_urls(
                    requested_urls,
                    Path(tempdir),
                    use_cache=False,
                )

        self.assertEqual(set(result), set(requested_urls))
        self.assertEqual([len(urls) for urls in requests_seen], [12, 5, 1])
        retried_urls = {url for urls in requests_seen[1:] for url in urls}
        self.assertFalse(initially_successful_urls & retried_urls)
        self.assertEqual(retried_urls, set(requested_urls[6:]))

    def test_harvest_profile_connector_does_not_fan_out_large_unresolved_batches_into_single_requests(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            requests_seen = []
            requested_urls = [f"https://www.linkedin.com/in/opaque-{index}" for index in range(12)]

            def _fake_run(_settings, payload, **kwargs):
                requests_seen.append(dict(payload))
                if "publicIdentifiers" in payload or "profileIds" in payload:
                    raise AssertionError("unexpected direct single fallback for large unresolved batch")
                return None

            with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}, clear=False), patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                side_effect=_fake_run,
            ):
                result = connector.fetch_profiles_by_urls(
                    requested_urls,
                    Path(tempdir),
                    use_cache=False,
                )
                self.assertEqual(result, {})
                self.assertEqual(
                    [len(payload.get("urls") or []) for payload in requests_seen],
                    [12, 10, 2, 5, 5, 2],
                )

    def test_harvest_profile_connector_live_mode_does_not_fragment_large_unresolved_tail(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            requests_seen = []
            requested_urls = [f"https://www.linkedin.com/in/live-opaque-{index}" for index in range(12)]

            def _fake_run(_settings, payload, **kwargs):
                requests_seen.append(dict(payload))
                return None

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                result = connector.fetch_profiles_by_urls(
                    requested_urls,
                    Path(tempdir),
                    use_cache=False,
                )
                self.assertEqual(result, {})
                self.assertEqual([len(payload.get("urls") or []) for payload in requests_seen], [12])

    def test_harvest_profile_connector_single_url_falls_back_to_public_identifier(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            requests_seen = []

            def _fake_run(_settings, payload, **kwargs):
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

            def _fake_run(actor_settings, payload, **kwargs):
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

    def test_harvest_company_employees_probe_expands_default_budget_for_full_roster(self) -> None:
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
            requested_name="Mistral AI",
            canonical_name="Mistral AI",
            company_key="mistralai",
            linkedin_slug="mistralai",
            linkedin_company_url="https://www.linkedin.com/company/mistralai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "mistralai" / "snap-expand"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            capture = {}

            def _fake_run(actor_settings, payload, **kwargs):
                capture["payload"] = dict(payload)
                capture["max_paid_items"] = actor_settings.max_paid_items
                return [
                    {
                        "firstName": "Ada",
                        "lastName": "Example",
                        "linkedinUrl": "https://www.linkedin.com/in/ada-example/",
                        "publicIdentifier": "ada-example",
                    }
                ]

            with patch("sourcing_agent.harvest_connectors._load_cached_harvest_payload", return_value=(None, None, None)), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                return_value={"data": {"id": "run-probe", "defaultDatasetId": "dataset-probe", "status": "RUNNING"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                return_value={"data": {"id": "run-probe", "defaultDatasetId": "dataset-probe", "status": "SUCCEEDED"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run_log",
                return_value=(
                    '2026-04-26T08:34:12.531Z Found 1055 profiles total for input '
                    '{"currentCompanies":["https://www.linkedin.com/company/mistralai/"]}\n'
                ),
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
                return_value=[{"firstName": "Probe", "lastName": "Only"}],
            ), patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                side_effect=_fake_run,
            ), patch(
                "sourcing_agent.harvest_connectors.time.sleep",
                return_value=None,
            ):
                snapshot = connector.fetch_company_roster(identity, snapshot_dir, max_pages=20, page_limit=25)

            summary = json.loads(
                (snapshot_dir / "harvest_company_employees" / "harvest_company_employees_summary.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(capture["payload"]["takePages"], 43)
        self.assertEqual(capture["payload"]["maxItems"], 1055)
        self.assertGreaterEqual(capture["max_paid_items"], 1055)
        self.assertEqual(snapshot.stop_reason, "completed")
        self.assertFalse(summary["partial_result"])
        self.assertEqual(summary["probe"]["requested_items_before_probe"], 500)
        self.assertTrue(summary["probe"]["expanded_after_probe"])
        self.assertTrue(summary["probe"]["requested_limit_would_truncate"])
        self.assertEqual(summary["effective_item_count"], 1055)

    def test_harvest_company_employees_probe_marks_provider_cap_partial(self) -> None:
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
            requested_name="LargeCo",
            canonical_name="LargeCo",
            company_key="largeco",
            linkedin_slug="largeco",
            linkedin_company_url="https://www.linkedin.com/company/largeco/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "largeco" / "snap-cap"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            capture = {}

            def _fake_run(actor_settings, payload, **kwargs):
                capture["payload"] = dict(payload)
                return [
                    {
                        "firstName": "Cap",
                        "lastName": "Example",
                        "linkedinUrl": "https://www.linkedin.com/in/cap-example/",
                        "publicIdentifier": "cap-example",
                    }
                ]

            with patch("sourcing_agent.harvest_connectors._load_cached_harvest_payload", return_value=(None, None, None)), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                return_value={"data": {"id": "run-probe", "defaultDatasetId": "dataset-probe", "status": "RUNNING"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                return_value={"data": {"id": "run-probe", "defaultDatasetId": "dataset-probe", "status": "SUCCEEDED"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run_log",
                return_value=(
                    '2026-04-26T08:34:12.531Z Found 3001 profiles total for input '
                    '{"currentCompanies":["https://www.linkedin.com/company/largeco/"]}\n'
                    "2026-04-26T08:34:12.532Z The search results are limited to 2500 items "
                    "(out of total 3001) because LinkedIn does not allow to scrape more for one query.\n"
                ),
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
                return_value=[{"firstName": "Probe", "lastName": "Only"}],
            ), patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                side_effect=_fake_run,
            ), patch(
                "sourcing_agent.harvest_connectors.time.sleep",
                return_value=None,
            ):
                snapshot = connector.fetch_company_roster(identity, snapshot_dir, max_pages=20, page_limit=25)

            summary = json.loads(
                (snapshot_dir / "harvest_company_employees" / "harvest_company_employees_summary.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(capture["payload"]["takePages"], 100)
        self.assertEqual(capture["payload"]["maxItems"], 2500)
        self.assertEqual(snapshot.stop_reason, "provider_cap_reached")
        self.assertTrue(summary["partial_result"])
        self.assertTrue(summary["provider_cap_hit"])
        self.assertEqual(summary["effective_item_count"], 2500)

    def test_harvest_company_checkpoint_probe_expansion_reaches_async_worker_payload(self) -> None:
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
            requested_name="Mistral AI",
            canonical_name="Mistral AI",
            company_key="mistralai",
            linkedin_slug="mistralai",
            linkedin_company_url="https://www.linkedin.com/company/mistralai/",
        )
        submitted_payloads: list[dict[str, object]] = []

        def _submit(_settings, payload, **_kwargs):
            submitted_payloads.append(dict(payload))
            if len(submitted_payloads) == 1:
                return {"data": {"id": "run-probe", "defaultDatasetId": "dataset-probe", "status": "RUNNING"}}
            return {"data": {"id": "run-full", "defaultDatasetId": "dataset-full", "status": "RUNNING"}}

        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "mistralai" / "snap-worker"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch("sourcing_agent.harvest_connectors._load_cached_harvest_payload", return_value=(None, None, None)), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=_submit,
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                return_value={"data": {"id": "run-probe", "defaultDatasetId": "dataset-probe", "status": "SUCCEEDED"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run_log",
                return_value=(
                    '2026-04-26T08:34:12.531Z Found 1055 profiles total for input '
                    '{"currentCompanies":["https://www.linkedin.com/company/mistralai/"]}\n'
                ),
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
                return_value=[{"firstName": "Probe", "lastName": "Only"}],
            ), patch(
                "sourcing_agent.harvest_connectors.time.sleep",
                return_value=None,
            ):
                result = connector.execute_with_checkpoint(identity, snapshot_dir, max_pages=20, page_limit=25)

        self.assertTrue(result.pending)
        self.assertEqual(len(submitted_payloads), 2)
        self.assertEqual(submitted_payloads[1]["takePages"], 43)
        self.assertEqual(submitted_payloads[1]["maxItems"], 1055)
        request_context = dict(result.checkpoint.get("request_context") or {})
        self.assertEqual(request_context["effective_max_items"], 1055)
        self.assertEqual(request_context["probe"]["requested_items_before_probe"], 500)

    def test_harvest_company_employees_applies_company_filters(self) -> None:
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
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "anthropic" / "snap-filtered"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            capture = {}

            def _fake_run(actor_settings, payload, **kwargs):
                capture["payload"] = dict(payload)
                return [
                    {
                        "firstName": "Ada",
                        "lastName": "Example",
                        "linkedinUrl": "https://www.linkedin.com/in/ada-example/",
                        "publicIdentifier": "ada-example",
                        "location": {"linkedinText": "San Francisco Bay Area"},
                        "_meta": {"pagination": {"pageNumber": 1}},
                    }
                ]

            with patch("sourcing_agent.harvest_connectors._run_harvest_actor", side_effect=_fake_run):
                snapshot = connector.fetch_company_roster(
                    identity,
                    snapshot_dir,
                    max_pages=8,
                    page_limit=50,
                    company_filters={
                        "companies": [
                            "https://www.linkedin.com/company/google/",
                            "https://www.linkedin.com/company/deepmind/",
                        ],
                        "locations": ["United States"],
                        "function_ids": ["8"],
                        "exclude_function_ids": ["24"],
                    },
                )

        self.assertEqual(
            capture["payload"]["companies"],
            [
                "https://www.linkedin.com/company/google/",
                "https://www.linkedin.com/company/deepmind/",
            ],
        )
        self.assertEqual(capture["payload"]["locations"], ["United States"])
        self.assertEqual(capture["payload"]["functionIds"], ["8"])
        self.assertEqual(capture["payload"]["excludeFunctionIds"], ["24"])
        self.assertEqual(snapshot.visible_entries[0]["location_normalized"]["raw_text"], "San Francisco Bay Area")

    def test_parse_harvest_company_employee_run_log_extracts_total_count_and_limit(self) -> None:
        log_text = """
2026-04-08T20:55:45.131Z Scraping query: {"currentCompanies":["https://www.linkedin.com/company/anthropicresearch/"]}
2026-04-08T20:55:49.785Z Found 4845 profiles total for input {"currentCompanies":["https://www.linkedin.com/company/anthropicresearch/"]}
2026-04-08T20:55:49.867Z  [WARNING]
2026-04-08T20:55:49.868Z The search results are limited to 2500 items (out of total 4845) because LinkedIn does not allow to scrape more for one query.
2026-04-08T20:55:49.864Z Scraped search page 1. Found 25 profiles on the page.
2026-04-08T20:55:49.884Z Max items limit reached: 2500
""".strip()

        summary = parse_harvest_company_employee_run_log(log_text)

        self.assertEqual(summary["estimated_total_count"], 4845)
        self.assertTrue(summary["provider_result_limited"])
        self.assertTrue(summary["max_items_limit_reached"])
        self.assertEqual(summary["max_items_limit"], 2500)
        self.assertEqual(summary["scraped_page_count"], 1)

    def test_harvest_company_probe_company_roster_query_records_probe_summary(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "anthropic" / "probe-snap"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                return_value={"data": {"id": "run-probe-1", "defaultDatasetId": "dataset-probe-1", "status": "RUNNING"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                return_value={"data": {"id": "run-probe-1", "defaultDatasetId": "dataset-probe-1", "status": "SUCCEEDED"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run_log",
                return_value=(
                    '2026-04-09T00:01:40.783Z Found 1100 profiles total for input '
                    '{"location":["United States"],"functionIds":["8"]}\n'
                    "2026-04-09T00:01:40.852Z Scraped search page 1. Found 25 profiles on the page.\n"
                ),
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
                return_value=[{"firstName": "Ada"}],
            ), patch(
                "sourcing_agent.harvest_connectors.time.sleep",
                return_value=None,
            ):
                summary = connector.probe_company_roster_query(
                    identity,
                    snapshot_dir,
                    company_filters={"locations": ["United States"], "function_ids": ["8"]},
                    probe_id="engineering",
                    title="United States / Engineering",
                )

        self.assertEqual(summary["estimated_total_count"], 1100)
        self.assertEqual(summary["returned_item_count"], 1)
        self.assertTrue(summary["summary_path"].endswith(".summary.json"))
        self.assertTrue(summary["log_path"].endswith(".log.txt"))

    def test_harvest_company_fetch_reuses_complete_probe_dataset_without_second_run(self) -> None:
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
            requested_name="Monica AI",
            canonical_name="Monica AI",
            company_key="monicaai",
            linkedin_slug="monica-im",
            linkedin_company_url="https://www.linkedin.com/company/monica-im/",
        )
        probe_items = [
            {
                "firstName": "Ada",
                "lastName": "Lovelace",
                "linkedinUrl": "https://www.linkedin.com/in/ada-lovelace/",
                "publicIdentifier": "ada-lovelace",
            }
            for _ in range(8)
        ]
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "monicaai" / "snap"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                return_value={"data": {"id": "run-probe-1", "defaultDatasetId": "dataset-probe-1", "status": "RUNNING"}},
            ) as submit_run, patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                return_value={"data": {"id": "run-probe-1", "defaultDatasetId": "dataset-probe-1", "status": "SUCCEEDED"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run_log",
                return_value=(
                    '2026-04-24T09:54:28.033Z Found 8 profiles total for input '
                    '{"currentCompanies":["https://www.linkedin.com/company/monica-im/"]}\n'
                    "2026-04-24T09:54:28.100Z Scraped search page 1. Found 8 profiles on the page.\n"
                ),
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
                return_value=probe_items,
            ), patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                side_effect=AssertionError("full run should not be called when probe proves complete"),
            ), patch(
                "sourcing_agent.harvest_connectors.time.sleep",
                return_value=None,
            ):
                snapshot = connector.fetch_company_roster(identity, snapshot_dir, max_pages=8, page_limit=25)
                request_manifest = json.loads(
                    (
                        snapshot_dir / "harvest_company_employees" / "harvest_company_employees_raw.request.json"
                    ).read_text(encoding="utf-8")
                )

        self.assertEqual(submit_run.call_count, 1)
        self.assertEqual(len(snapshot.visible_entries), 1)
        self.assertEqual(request_manifest["request_context"]["cache_status"], "probe_complete_dataset")

    def test_harvest_company_checkpoint_reuses_complete_probe_dataset_without_second_run(self) -> None:
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
            requested_name="Monica AI",
            canonical_name="Monica AI",
            company_key="monicaai",
            linkedin_slug="monica-im",
            linkedin_company_url="https://www.linkedin.com/company/monica-im/",
        )
        probe_items = [
            {
                "firstName": "Ada",
                "lastName": "Lovelace",
                "linkedinUrl": "https://www.linkedin.com/in/ada-lovelace/",
                "publicIdentifier": "ada-lovelace",
            }
        ]
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "monicaai" / "snap"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                return_value={"data": {"id": "run-probe-1", "defaultDatasetId": "dataset-probe-1", "status": "RUNNING"}},
            ) as submit_run, patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                return_value={"data": {"id": "run-probe-1", "defaultDatasetId": "dataset-probe-1", "status": "SUCCEEDED"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run_log",
                return_value=(
                    '2026-04-24T09:54:28.033Z Found 1 profiles total for input '
                    '{"currentCompanies":["https://www.linkedin.com/company/monica-im/"]}\n'
                    "2026-04-24T09:54:28.100Z Scraped search page 1. Found 1 profiles on the page.\n"
                ),
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
                return_value=probe_items,
            ), patch(
                "sourcing_agent.harvest_connectors.time.sleep",
                return_value=None,
            ):
                result = connector.execute_with_checkpoint(identity, snapshot_dir, max_pages=8, page_limit=25)

        self.assertEqual(submit_run.call_count, 1)
        self.assertFalse(result.pending)
        self.assertEqual(result.checkpoint["cache_source"], "probe_complete_dataset")
        self.assertEqual(result.body, probe_items)
        self.assertEqual([artifact.label for artifact in result.artifacts], ["dataset_items"])

    def test_harvest_company_probe_company_roster_query_replay_cache_miss_returns_unknown_total(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Surge AI",
            canonical_name="Surge AI",
            company_key="surgeai",
            linkedin_slug="surge-ai",
            linkedin_company_url="https://www.linkedin.com/company/surge-ai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "surgeai" / "probe-replay"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "replay"}), patch(
                "sourcing_agent.harvest_connectors._load_cached_harvest_payload",
                return_value=(None, None, None),
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("replay mode should not submit live Harvest runs"),
            ):
                summary = connector.probe_company_roster_query(
                    identity,
                    snapshot_dir,
                    probe_id="surgeai",
                    title="Surge AI full roster",
                )

        self.assertEqual(summary["estimated_total_count"], 0)
        self.assertFalse(summary["provider_result_limited"])
        self.assertIn("unknown", str(summary["detail"] or "").lower())

    def test_harvest_company_probe_company_roster_query_request_scoped_fast_smoke_shortens_poll_interval(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "anthropic" / "probe-fast-smoke"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                return_value={"data": {"id": "run-probe-fast", "defaultDatasetId": "dataset-probe-fast", "status": "RUNNING"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                return_value={"data": {"id": "run-probe-fast", "defaultDatasetId": "dataset-probe-fast", "status": "SUCCEEDED"}},
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run_log",
                return_value="2026-04-09T00:01:40.783Z Found 100 profiles total for input {}\n",
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
                return_value=[{"firstName": "Ada"}],
            ), patch(
                "sourcing_agent.harvest_connectors.time.sleep",
                return_value=None,
            ) as sleep_mock:
                summary = connector.probe_company_roster_query(
                    identity,
                    snapshot_dir,
                    runtime_timing_overrides={"runtime_tuning_profile": "fast_smoke"},
                )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(sleep_mock.call_args_list, [call(0.25)])

    def test_load_cached_harvest_payload_reuses_live_probe_summary_via_request_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "anthropic" / "snap-cache"
            live_probe_dir = runtime_dir / "live_tests" / "adaptive_probe" / "harvest_company_employees" / "probes"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            live_probe_dir.mkdir(parents=True, exist_ok=True)

            payload = {
                "profileScraperMode": "Short ($4 per 1k)",
                "companies": ["https://www.linkedin.com/company/anthropicresearch/"],
                "takePages": 1,
                "maxItems": 25,
                "locations": ["United States"],
            }
            summary_payload = {
                "probe_id": "anthropic_us_root_smoke",
                "title": "Anthropic / United States",
                "status": "completed",
                "estimated_total_count": 2837,
                "provider_result_limited": True,
            }
            summary_path = live_probe_dir / "harvest_company_employees_probe_anthropic_us_root_smoke.summary.json"
            request_path = live_probe_dir / "harvest_company_employees_probe_anthropic_us_root_smoke.request.json"
            summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2))
            request_path.write_text(
                json.dumps(
                    {
                        "logical_name": "harvest_company_employees_probe",
                        "payload_hash": "ignored",
                        "request_payload": payload,
                        "request_context": {"title": "Anthropic / United States"},
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )

            cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
                snapshot_dir,
                logical_name="harvest_company_employees_probe_summary",
                payload=payload,
            )
            self.assertEqual(cached_body, summary_payload)
            self.assertEqual(cache_source, "live_test_bridge_summary")
            self.assertEqual(Path(str(cache_origin)), summary_path)

            second_body, second_source, _ = _load_cached_harvest_payload(
                snapshot_dir,
                logical_name="harvest_company_employees_probe_summary",
                payload=payload,
            )
            self.assertEqual(second_body, summary_payload)
            self.assertEqual(second_source, "shared_cache")

    def test_load_cached_harvest_payload_discards_offline_shared_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "surgeai" / "snap-live"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            cache_dir = runtime_dir / "provider_cache" / "harvest_company_employees"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_path = cache_dir / "4af5cf964987ed7d.json"
            request_path = cache_dir / "4af5cf964987ed7d.request.json"
            cache_path.write_text(
                json.dumps(
                    [
                        {
                            "_offline": True,
                            "_provider_mode": "replay",
                            "linkedinUrl": "https://www.linkedin.com/in/surge-ai-offline-1/",
                        }
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            request_path.write_text(
                json.dumps(
                    {
                        "logical_name": "harvest_company_employees",
                        "payload_hash": "4af5cf964987ed7d",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            payload = {
                "profileScraperMode": "Short ($4 per 1k)",
                "companies": ["https://www.linkedin.com/company/surge-ai/"],
                "takePages": 8,
                "maxItems": 180,
            }

            cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
                snapshot_dir,
                logical_name="harvest_company_employees",
                payload=payload,
            )

            self.assertIsNone(cached_body)
            self.assertIsNone(cache_source)
            self.assertIsNone(cache_origin)
            self.assertFalse(cache_path.exists())
            self.assertFalse(request_path.exists())

    def test_persist_shared_harvest_payload_skips_offline_replay_body(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "surgeai" / "snap-replay"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            payload = {
                "profileScraperMode": "Short ($4 per 1k)",
                "companies": ["https://www.linkedin.com/company/surge-ai/"],
                "takePages": 8,
                "maxItems": 180,
            }
            body = [
                {
                    "_offline": True,
                    "_provider_mode": "replay",
                    "linkedinUrl": "https://www.linkedin.com/in/surge-ai-offline-1/",
                }
            ]

            with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "replay"}):
                _persist_shared_harvest_payload(
                    snapshot_dir,
                    logical_name="harvest_company_employees",
                    payload=payload,
                    body=body,
                    request_context={"provider_mode": "replay"},
                )

            cache_dir = Path(tempdir) / "runtime" / "provider_cache" / "harvest_company_employees"
            self.assertFalse(cache_dir.exists() and any(cache_dir.iterdir()))

    def test_non_live_provider_mode_does_not_read_live_shared_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "surgeai" / "snap-live"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            payload = {
                "profileScraperMode": "Short ($4 per 1k)",
                "companies": ["https://www.linkedin.com/company/surge-ai/"],
                "takePages": 1,
                "maxItems": 25,
            }
            body = [{"linkedinUrl": "https://www.linkedin.com/in/real-surge-member/"}]

            with patch.dict(
                "os.environ",
                {
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                },
                clear=False,
            ):
                _persist_shared_harvest_payload(
                    snapshot_dir,
                    logical_name="harvest_company_employees",
                    payload=payload,
                    body=body,
                    request_context={"provider_mode": "live"},
                )

            with patch.dict(
                "os.environ",
                {
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "replay",
                    "SOURCING_RUNTIME_ENVIRONMENT": "replay",
                },
                clear=False,
            ):
                cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
                    snapshot_dir,
                    logical_name="harvest_company_employees",
                    payload=payload,
                )

            self.assertIsNone(cached_body)
            self.assertIsNone(cache_source)
            self.assertIsNone(cache_origin)

    def test_harvest_company_execute_with_checkpoint_ignores_offline_shared_cache_in_live_mode(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Surge AI",
            canonical_name="Surge AI",
            company_key="surgeai",
            linkedin_slug="surge-ai",
            linkedin_company_url="https://www.linkedin.com/company/surge-ai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "surgeai" / "snap-live"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            cache_dir = Path(tempdir) / "runtime" / "provider_cache" / "harvest_company_employees"
            cache_dir.mkdir(parents=True, exist_ok=True)
            payload_hash = "4af5cf964987ed7d"
            (cache_dir / f"{payload_hash}.json").write_text(
                json.dumps(
                    [
                        {
                            "_offline": True,
                            "_provider_mode": "replay",
                            "linkedinUrl": "https://www.linkedin.com/in/surge-ai-offline-1/",
                        }
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (cache_dir / f"{payload_hash}.request.json").write_text(
                json.dumps(
                    {
                        "logical_name": "harvest_company_employees",
                        "payload_hash": payload_hash,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                return_value={"data": {"id": "run-live-1", "defaultDatasetId": "dataset-live-1", "status": "RUNNING"}},
            ) as submit_mock:
                result = connector.execute_with_checkpoint(
                    identity,
                    snapshot_dir,
                    max_pages=20,
                    page_limit=25,
                )

            self.assertTrue(result.pending)
            self.assertEqual(result.checkpoint["run_id"], "run-live-1")
            self.assertEqual(result.checkpoint["status"], "submitted")
            self.assertEqual(submit_mock.call_count, 2)

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

    def test_harvest_profile_batch_cache_hit_preserves_remote_identifiers_from_checkpoint(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "xai" / "snap-cache-hit"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch(
                "sourcing_agent.harvest_connectors._load_cached_harvest_payload",
                return_value=([{"linkedinUrl": "https://www.linkedin.com/in/jane-doe/"}], "shared_cache", snapshot_dir / "cache.json"),
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("cache hit should not submit another actor run"),
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                side_effect=AssertionError("cache hit should not poll another actor run"),
            ):
                result = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                    checkpoint={
                        "run_id": "run-existing",
                        "dataset_id": "dataset-existing",
                        "actor_id": "actor",
                    },
                )

        self.assertFalse(result.pending)
        self.assertEqual(result.checkpoint["status"], "completed")
        self.assertEqual(result.checkpoint["run_id"], "run-existing")
        self.assertEqual(result.checkpoint["dataset_id"], "dataset-existing")
        self.assertEqual(result.artifacts[0].metadata["run_id"], "run-existing")
        self.assertEqual(result.artifacts[0].metadata["dataset_id"], "dataset-existing")

    def test_harvest_profile_batch_completion_records_provider_io_timings(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "xai" / "snap-provider-io"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with (
                patch(
                    "sourcing_agent.harvest_connectors._load_cached_harvest_payload",
                    return_value=(None, None, None),
                ),
                patch(
                    "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                    return_value={
                        "data": {
                            "id": "run-provider-io",
                            "defaultDatasetId": "dataset-provider-io",
                            "status": "SUCCEEDED",
                            "startedAt": "2026-05-04T10:00:00.000Z",
                            "finishedAt": "2026-05-04T10:00:09.000Z",
                        }
                    },
                ),
                patch(
                    "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
                    return_value=[{"linkedinUrl": "https://www.linkedin.com/in/jane-doe/"}],
                ),
            ):
                result = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                    checkpoint={"run_id": "run-provider-io", "dataset_id": "dataset-provider-io"},
                )

        self.assertFalse(result.pending)
        provider_timings = dict(result.checkpoint.get("provider_timings") or {})
        self.assertEqual(provider_timings["actor_run_duration_ms"], 9000.0)
        self.assertIn("dataset_download_duration_ms", provider_timings)
        self.assertEqual(dict(result.artifacts[-1].metadata.get("provider_timings") or {})["actor_run_duration_ms"], 9000.0)

    def test_harvest_profile_batch_execute_with_checkpoint_can_simulate_without_live_request(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "xai" / "snap-simulate"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("simulate mode should not submit live Harvest runs"),
            ):
                result = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                )

        self.assertFalse(result.pending)
        self.assertEqual(result.checkpoint["provider_mode"], "simulate")
        self.assertEqual(result.checkpoint["status"], "completed")
        self.assertEqual(len(result.body), 1)
        self.assertEqual(result.body[0]["linkedinUrl"], "https://www.linkedin.com/in/jane-doe/")

    def test_harvest_profile_batch_execute_with_checkpoint_replay_cache_miss_returns_empty_without_live_request(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "xai" / "snap-replay"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "replay"}), patch(
                "sourcing_agent.harvest_connectors._load_cached_harvest_payload",
                return_value=(None, None, None),
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("replay mode should not submit live Harvest runs"),
            ):
                result = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                )

        self.assertFalse(result.pending)
        self.assertEqual(result.checkpoint["provider_mode"], "replay")
        self.assertEqual(result.checkpoint["status"], "completed")
        self.assertEqual(len(result.body), 0)

    def test_harvest_company_execute_with_checkpoint_replay_cache_miss_returns_empty_without_live_request(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Surge AI",
            canonical_name="Surge AI",
            company_key="surgeai",
            linkedin_slug="surge-ai",
            linkedin_company_url="https://www.linkedin.com/company/surge-ai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "surgeai" / "snap-replay"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "replay"}), patch(
                "sourcing_agent.harvest_connectors._load_cached_harvest_payload",
                return_value=(None, None, None),
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("replay mode should not submit live Harvest runs"),
            ):
                result = connector.execute_with_checkpoint(
                    identity,
                    snapshot_dir,
                    max_pages=2,
                    page_limit=25,
                )

        self.assertFalse(result.pending)
        self.assertEqual(result.checkpoint["provider_mode"], "replay")
        self.assertEqual(result.checkpoint["status"], "completed")
        self.assertEqual(len(result.body), 0)

    def test_harvest_profile_batch_execute_with_checkpoint_supports_scripted_pending_rounds(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-scripted"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            scenario_path = Path(tempdir) / "scripted_harvest.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "profile_batch",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "execute_pending_rounds": 1,
                                    "body": [
                                        {
                                            "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                                            "publicIdentifier": "jane-doe",
                                            "headline": "Reasoning engineer at OpenAI",
                                            "item": {
                                                "profileUrl": "https://www.linkedin.com/in/jane-doe/",
                                                "fullName": "Jane Doe",
                                                "headline": "Reasoning engineer at OpenAI",
                                            },
                                        }
                                    ],
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                },
            ):
                first = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                )
                second = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                    checkpoint=first.checkpoint,
                )
                invocations = load_scripted_provider_invocations()

        self.assertTrue(first.pending)
        self.assertEqual(first.checkpoint["provider_mode"], "scripted")
        self.assertEqual(first.checkpoint["status"], "submitted")
        self.assertFalse(second.pending)
        self.assertEqual(second.checkpoint["provider_mode"], "scripted")
        self.assertEqual(second.checkpoint["status"], "completed")
        self.assertEqual(len(second.body), 1)
        self.assertEqual(second.body[0]["publicIdentifier"], "jane-doe")
        self.assertEqual(
            [item.get("logical_name") for item in invocations],
            ["harvest_profile_scraper_batch"],
        )

    def test_harvest_profile_batch_prefers_runtime_scoped_scripted_mode_over_ambient_live_env(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "openai_agent_scripted_case"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-scripted"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            (runtime_dir / ".scripted-local-postgres.env").write_text(
                "\n".join(
                    [
                        "SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://isolated@127.0.0.1:55432/isolated_runtime",
                        "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only",
                        "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=sourcing_scripted_openai_agent_scripted_case",
                        "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1",
                        "SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory",
                        "SOURCING_RUNTIME_ENVIRONMENT=scripted",
                        "SOURCING_EXTERNAL_PROVIDER_MODE=scripted",
                        "SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            scenario_path = Path(tempdir) / "scripted_harvest.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "profile_batch",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "body": [
                                        {
                                            "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                                            "publicIdentifier": "jane-doe",
                                            "item": {
                                                "profileUrl": "https://www.linkedin.com/in/jane-doe/",
                                                "fullName": "Jane Doe",
                                            },
                                        }
                                    ],
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                },
                clear=True,
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("runtime-scoped scripted mode should not submit live Harvest runs"),
            ):
                result = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                )

        self.assertFalse(result.pending)
        self.assertEqual(result.checkpoint["provider_mode"], "scripted")
        self.assertEqual(result.checkpoint["status"], "completed")
        self.assertEqual(len(result.body), 1)
        self.assertEqual(result.body[0]["publicIdentifier"], "jane-doe")

    def test_scripted_run_status_poll_does_not_call_live_apify(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with (
            patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False),
            patch("sourcing_agent.harvest_connectors._get_harvest_actor_run") as live_status_mock,
        ):
            status = connector.get_actor_run_status("scripted_run_harvest_profile_scraper_batch_abc123")

        live_status_mock.assert_not_called()
        self.assertEqual(status["status"], "SUCCEEDED")
        self.assertTrue(status["is_terminal"])
        self.assertEqual(status["raw"]["provider_mode"], "scripted")

    def test_scripted_run_status_poll_respects_remote_ready_time(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        overrides = {
            "provider_mode": "scripted",
            "scripted_remote_ready_epoch_ms": 1_010_000,
            "scripted_remote_wait_seconds": 10,
        }
        with (
            patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False),
            patch("sourcing_agent.harvest_connectors._get_harvest_actor_run") as live_status_mock,
            patch("sourcing_agent.harvest_connectors.time.time", return_value=1_000.0),
        ):
            pending = connector.get_actor_run_status(
                "scripted_run_harvest_profile_scraper_batch_abc123",
                runtime_timing_overrides=overrides,
            )
        with (
            patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False),
            patch("sourcing_agent.harvest_connectors._get_harvest_actor_run") as second_live_status_mock,
            patch("sourcing_agent.harvest_connectors.time.time", return_value=1_010.0),
        ):
            terminal = connector.get_actor_run_status(
                "scripted_run_harvest_profile_scraper_batch_abc123",
                runtime_timing_overrides=overrides,
            )

        live_status_mock.assert_not_called()
        second_live_status_mock.assert_not_called()
        self.assertEqual(pending["status"], "RUNNING")
        self.assertFalse(pending["is_terminal"])
        self.assertEqual(pending["finished_at"], "")
        self.assertEqual(terminal["status"], "SUCCEEDED")
        self.assertTrue(terminal["is_terminal"])
        self.assertEqual(terminal["started_at"], "1970-01-01T00:16:40.000+00:00")
        self.assertEqual(terminal["finished_at"], "1970-01-01T00:16:50.000+00:00")
        self.assertEqual(terminal["remote_completed_at"], terminal["finished_at"])
        self.assertEqual(terminal["raw"]["finishedAt"], terminal["finished_at"])
        self.assertEqual(terminal["raw"]["eventCreatedAt"], terminal["finished_at"])

    def test_scripted_terminal_provider_event_bypasses_pending_rounds(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-terminal"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            scenario_path = Path(tempdir) / "scripted_harvest.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "terminal_event_profile_batch",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "execute_pending_rounds": 5,
                                    "execute_sleep_seconds": 10,
                                    "body": [
                                        {
                                            "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                                            "publicIdentifier": "jane-doe",
                                            "fullName": "Jane Doe",
                                            "headline": "Agent engineer at OpenAI",
                                        }
                                    ],
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            checkpoint = {
                "run_id": "scripted_run_harvest_profile_scraper_batch_terminal",
                "dataset_id": "scripted_dataset_harvest_profile_scraper_batch_terminal",
                "status": "submitted",
                "scripted_execute_round": 1,
                "remote_provider_terminal_event": {"event_type": "ACTOR.RUN.SUCCEEDED"},
                "force_scripted_terminal_fetch": True,
            }
            with (
                patch.dict(
                    "os.environ",
                    {
                        "SOURCING_RUNTIME_DIR": str(runtime_dir),
                        "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                        "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                    },
                    clear=False,
                ),
                patch("sourcing_agent.harvest_connectors.time.sleep") as sleep_mock,
            ):
                result = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                    checkpoint=checkpoint,
                )

        sleep_mock.assert_not_called()
        self.assertFalse(result.pending)
        self.assertEqual(result.checkpoint["status"], "completed")
        self.assertTrue(result.checkpoint["remote_provider_terminal_event_consumed"])
        self.assertEqual(result.body[0]["publicIdentifier"], "jane-doe")

    def test_scripted_remote_wait_after_submit_returns_pending_without_blocking_submit(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-remote-wait"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            scenario_path = Path(tempdir) / "scripted_harvest.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "remote_wait_profile_batch",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "execute_pending_rounds": 5,
                                    "execute_sleep_seconds": 30,
                                    "execute_sleep_position": "remote_wait",
                                    "body": [
                                        {
                                            "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                                            "publicIdentifier": "jane-doe",
                                            "fullName": "Jane Doe",
                                            "headline": "Agent engineer at OpenAI",
                                        }
                                    ],
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with (
                patch.dict(
                    "os.environ",
                    {
                        "SOURCING_RUNTIME_DIR": str(runtime_dir),
                        "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                        "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                    },
                    clear=False,
                ),
                patch("sourcing_agent.harvest_connectors.time.sleep") as sleep_mock,
                patch("sourcing_agent.harvest_connectors.time.time", return_value=1_000.0),
            ):
                first = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                    runtime_timing_overrides={"harvest_scripted_sleep_seconds_cap": 10},
                )
                second = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                    checkpoint=first.checkpoint,
                    runtime_timing_overrides={"harvest_scripted_sleep_seconds_cap": 10},
                )
                terminal_checkpoint = {
                    **first.checkpoint,
                    "remote_provider_terminal_event": {"event_type": "ACTOR.RUN.SUCCEEDED"},
                    "force_scripted_terminal_fetch": True,
                }
                completed = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                    checkpoint=terminal_checkpoint,
                    runtime_timing_overrides={"harvest_scripted_sleep_seconds_cap": 10},
                )

        sleep_mock.assert_not_called()
        self.assertTrue(first.pending)
        self.assertTrue(second.pending)
        self.assertFalse(completed.pending)
        self.assertEqual(first.checkpoint["run_id"], second.checkpoint["run_id"])
        self.assertEqual(first.checkpoint["dataset_id"], second.checkpoint["dataset_id"])
        self.assertTrue(first.checkpoint["scripted_remote_wait_after_submit"])
        self.assertEqual(first.checkpoint["scripted_remote_wait_seconds"], 10.0)
        self.assertEqual(first.checkpoint["scripted_remote_ready_epoch_ms"], 1_010_000)
        self.assertEqual(second.checkpoint["scripted_remote_ready_epoch_ms"], 1_010_000)
        self.assertEqual(completed.body[0]["publicIdentifier"], "jane-doe")
        self.assertEqual(completed.checkpoint["provider_timings"]["actor_run_duration_ms"], 10000.0)
        self.assertEqual(
            completed.artifacts[-2].metadata["provider_timings"]["actor_run_duration_ms"],
            10000.0,
        )

    def test_scripted_remote_wait_seconds_is_not_clamped_by_local_sleep_cap(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-remote-wait-explicit"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            scenario_path = Path(tempdir) / "scripted_harvest.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "remote_wait_profile_batch",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "execute_sleep_seconds": 30,
                                    "execute_sleep_position": "remote_wait",
                                    "scripted_remote_wait_seconds": 7,
                                    "body": [
                                        {
                                            "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                                            "publicIdentifier": "jane-doe",
                                            "fullName": "Jane Doe",
                                            "headline": "Agent engineer at OpenAI",
                                        }
                                    ],
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with (
                patch.dict(
                    "os.environ",
                    {
                        "SOURCING_RUNTIME_DIR": str(runtime_dir),
                        "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                        "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                        "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0.1",
                    },
                    clear=False,
                ),
                patch("sourcing_agent.harvest_connectors.time.sleep") as sleep_mock,
                patch("sourcing_agent.harvest_connectors.time.time", return_value=1_000.0),
            ):
                result = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                )

        sleep_mock.assert_not_called()
        self.assertTrue(result.pending)
        self.assertEqual(result.checkpoint["scripted_remote_wait_seconds"], 7.0)
        self.assertEqual(result.checkpoint["scripted_remote_ready_epoch_ms"], 1_007_000)

    def test_scripted_harvest_provider_timing_overrides_are_persisted(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-scripted-provider-io"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            scenario_path = Path(tempdir) / "scripted_harvest.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "provider_io_profile_batch",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "scripted_actor_run_duration_ms": 22500,
                                    "scripted_dataset_download_duration_ms": 4300,
                                    "body": [
                                        {
                                            "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                                            "publicIdentifier": "jane-doe",
                                            "fullName": "Jane Doe",
                                        }
                                    ],
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                },
                clear=False,
            ):
                result = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                )

        provider_timings = dict(result.checkpoint.get("provider_timings") or {})
        self.assertEqual(provider_timings["actor_run_duration_ms"], 22500.0)
        self.assertEqual(provider_timings["dataset_download_duration_ms"], 4300.0)
        dataset_artifact = next(item for item in result.artifacts if item.label == "dataset_items")
        self.assertEqual(dataset_artifact.metadata["provider_timings"], provider_timings)

    def test_scripted_sample_fixture_without_fallback_fails_when_profile_url_missing(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-scripted"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            sample_path = Path(tempdir) / "profile_pool.json"
            sample_path.write_text(
                json.dumps(
                    [
                        {
                            "linkedinUrl": "https://www.linkedin.com/in/present/",
                            "publicIdentifier": "present",
                            "headline": "Present profile",
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            scenario_path = Path(tempdir) / "scripted_harvest.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "profile_batch",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "sample_body_path": str(sample_path),
                                    "sample_fallback_generated": False,
                                    "generated_body": {"kind": "profile_scraper_batch", "max_profiles": 1},
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                },
                clear=False,
            ):
                with self.assertRaisesRegex(RuntimeError, "missing requested profile URLs"):
                    connector.execute_batch_with_checkpoint(
                        ["https://www.linkedin.com/in/missing/"],
                        snapshot_dir,
                    )

    def test_scripted_real_asset_candidate_documents_drive_profile_search_and_scraper(self) -> None:
        search_settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        profile_settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        search_connector = HarvestProfileSearchConnector(search_settings)
        profile_connector = HarvestProfileConnector(profile_settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "google" / "snap-real"
            profile_dir = snapshot_dir / "harvest_profiles"
            profile_dir.mkdir(parents=True, exist_ok=True)
            profile_url = "https://www.linkedin.com/in/google-real-vision-language/"
            raw_profile_path = profile_dir / "real_profile.json"
            raw_profile_path.write_text(
                json.dumps(
                    {
                        "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                        "item": {
                            "linkedinUrl": profile_url,
                            "publicIdentifier": "google-real-vision-language",
                            "fullName": "Google Real Vision",
                            "headline": "Vision-language researcher at Google",
                            "experience": [{"companyName": "Google", "title": "Research Engineer"}],
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            candidate_documents_path = snapshot_dir / "candidate_documents.json"
            candidate_documents_path.write_text(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "candidate_id": "google-real-1",
                                "display_name": "Google Real Vision",
                                "target_company": "Google",
                                "employment_status": "current",
                                "role": "Research Engineer",
                                "focus_areas": "Vision-language models",
                                "linkedin_url": profile_url,
                                "metadata": {
                                    "public_identifier": "google-real-vision-language",
                                    "headline": "Vision-language researcher at Google",
                                    "profile_timeline_source_path": str(raw_profile_path),
                                    "profile_url": profile_url,
                                },
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            scenario_path = Path(tempdir) / "scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "real_search",
                                    "match": {"logical_name": "harvest_profile_search"},
                                    "sample_candidate_documents_path": str(candidate_documents_path),
                                    "sample_candidate_employment_scope": "current",
                                    "sample_candidate_contains": ["vision-language"],
                                    "sample_fallback_generated": False,
                                },
                                {
                                    "name": "real_scraper",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "sample_candidate_documents_path": str(candidate_documents_path),
                                    "sample_candidate_contains": ["vision-language"],
                                    "sample_fallback_generated": False,
                                },
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            env = {
                "SOURCING_RUNTIME_DIR": str(runtime_dir),
                "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
            }
            with patch.dict("os.environ", env):
                search_result = search_connector.search_profiles(
                    query_text="vision-language",
                    filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                    employment_status="current",
                    discovery_dir=snapshot_dir / "search_seed_discovery",
                    limit=25,
                    pages=1,
                    auto_probe=False,
                )
                profile_result = profile_connector.execute_batch_with_checkpoint([profile_url], snapshot_dir)

        assert search_result is not None
        self.assertEqual(search_result["pagination"]["total_elements"], 1)
        self.assertEqual(search_result["rows"][0]["profile_url"], profile_url)
        self.assertFalse(profile_result.pending)
        self.assertEqual(len(profile_result.body), 1)
        parsed = parse_harvest_profile_payload(profile_result.body[0])
        self.assertEqual(parsed["requested_profile_url"], profile_url)
        self.assertEqual(parsed["full_name"], "Google Real Vision")

    def test_scripted_real_asset_candidate_documents_prefer_candidates_over_auxiliary_items(self) -> None:
        search_settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        search_connector = HarvestProfileSearchConnector(search_settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "google" / "snap-real"
            profile_dir = snapshot_dir / "harvest_profiles"
            profile_dir.mkdir(parents=True, exist_ok=True)
            profile_url = "https://www.linkedin.com/in/google-real-vision-language-candidate/"
            raw_profile_path = profile_dir / "real_profile.json"
            raw_profile_path.write_text(
                json.dumps(
                    {
                        "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                        "item": {
                            "linkedinUrl": profile_url,
                            "publicIdentifier": "google-real-vision-language-candidate",
                            "fullName": "Google Real Candidate",
                            "headline": "Vision-language researcher at Google",
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            candidate_documents_path = snapshot_dir / "candidate_documents.json"
            candidate_documents_path.write_text(
                json.dumps(
                    {
                        "snapshot": {"snapshot_id": "snap-real"},
                        "target_company": "Google",
                        "candidate_count": 1,
                        "items": [
                            {
                                "linkedinUrl": "https://www.linkedin.com/in/wrong-auxiliary-item/",
                                "headline": "Auxiliary provider row must not drive candidate-doc replay",
                            }
                        ],
                        "candidates": [
                            {
                                "candidate_id": "google-real-candidate",
                                "display_name": "Google Real Candidate",
                                "target_company": "Google",
                                "employment_status": "current",
                                "focus_areas": "Vision-language models",
                                "linkedin_url": profile_url,
                                "metadata": {
                                    "public_identifier": "google-real-vision-language-candidate",
                                    "profile_timeline_source_path": str(raw_profile_path),
                                    "profile_url": profile_url,
                                },
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            scenario_path = Path(tempdir) / "scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "real_search",
                                    "match": {"logical_name": "harvest_profile_search"},
                                    "sample_candidate_documents_path": str(candidate_documents_path),
                                    "sample_candidate_employment_scope": "current",
                                    "sample_candidate_contains": ["vision-language"],
                                    "sample_candidate_require_profile_source": True,
                                    "sample_fallback_generated": False,
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                    "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
                },
            ):
                search_result = search_connector.search_profiles(
                    query_text="vision-language",
                    filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                    employment_status="current",
                    discovery_dir=snapshot_dir / "search_seed_discovery",
                    limit=25,
                    pages=1,
                    auto_probe=False,
                )

        assert search_result is not None
        self.assertEqual(search_result["pagination"]["total_elements"], 1)
        self.assertEqual(search_result["rows"][0]["profile_url"], profile_url)

    def test_scripted_real_asset_profile_source_paths_are_rebased_before_stale_absolute_path(self) -> None:
        profile_settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        profile_connector = HarvestProfileConnector(profile_settings)
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir) / "repo"
            runtime_dir = repo_root / "runtime"
            source_snapshot = runtime_dir / "company_assets" / "google" / "source-snap"
            sample_snapshot = runtime_dir / "company_assets" / "google" / "sample-snap"
            profile_dir = source_snapshot / "harvest_profiles"
            sample_snapshot.mkdir(parents=True, exist_ok=True)
            profile_dir.mkdir(parents=True, exist_ok=True)
            profile_url = "https://www.linkedin.com/in/google-rebased-real-profile/"
            raw_profile_path = profile_dir / "rebased_profile.json"
            raw_profile_path.write_text(
                json.dumps(
                    {
                        "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                        "item": {
                            "linkedinUrl": profile_url,
                            "publicIdentifier": "google-rebased-real-profile",
                            "fullName": "Google Rebased Profile",
                            "headline": "Vision-language researcher at Google",
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            stale_absolute_path = (
                "/home/old-host/projects/Sourcing AI Agent Dev/sourcing-ai-agent/"
                "runtime/company_assets/google/source-snap/harvest_profiles/rebased_profile.json"
            )
            candidate_documents_path = sample_snapshot / "candidate_documents.json"
            candidate_documents_path.write_text(
                json.dumps(
                    {
                        "snapshot": {"snapshot_id": "sample-snap"},
                        "target_company": "Google",
                        "candidate_count": 1,
                        "candidates": [
                            {
                                "candidate_id": "google-rebased",
                                "display_name": "Google Rebased Profile",
                                "target_company": "Google",
                                "employment_status": "current",
                                "focus_areas": "Vision-language models",
                                "linkedin_url": profile_url,
                                "metadata": {
                                    "profile_timeline_source_path": stale_absolute_path,
                                    "profile_url": profile_url,
                                },
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            scenario_path = Path(tempdir) / "scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "real_scraper",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "sample_candidate_documents_path": str(candidate_documents_path),
                                    "sample_candidate_contains": ["vision-language"],
                                    "sample_candidate_require_profile_source": True,
                                    "sample_fallback_generated": False,
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                    "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
                },
            ):
                with patch("sourcing_agent.harvest_connectors.Path.cwd", return_value=repo_root):
                    profile_result = profile_connector.execute_batch_with_checkpoint([profile_url], sample_snapshot)

        self.assertFalse(profile_result.pending)
        self.assertEqual(len(profile_result.body), 1)
        parsed = parse_harvest_profile_payload(profile_result.body[0])
        self.assertEqual(parsed["full_name"], "Google Rebased Profile")

    def test_scripted_real_asset_scraper_matches_candidate_url_alias_when_raw_profile_canonical_url_differs(
        self,
    ) -> None:
        profile_settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        profile_connector = HarvestProfileConnector(profile_settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "google" / "snap-real"
            profile_dir = snapshot_dir / "harvest_profiles"
            profile_dir.mkdir(parents=True, exist_ok=True)
            requested_profile_url = "https://www.linkedin.com/in/google-vanity-vision/"
            canonical_profile_url = "https://www.linkedin.com/in/google-canonical-vision/"
            raw_profile_path = profile_dir / "canonical_profile.json"
            raw_profile_path.write_text(
                json.dumps(
                    {
                        "_harvest_request": {
                            "kind": "url",
                            "value": "https://www.linkedin.com/in/ACwCANONICAL",
                            "profile_url": "https://www.linkedin.com/in/ACwCANONICAL",
                        },
                        "item": {
                            "linkedinUrl": canonical_profile_url,
                            "publicIdentifier": "google-canonical-vision",
                            "fullName": "Google Canonical Vision",
                            "headline": "Vision-language researcher at Google",
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            candidate_documents_path = snapshot_dir / "candidate_documents.json"
            candidate_documents_path.write_text(
                json.dumps(
                    {
                        "snapshot": {"snapshot_id": "snap-real"},
                        "target_company": "Google",
                        "candidate_count": 1,
                        "candidates": [
                            {
                                "candidate_id": "google-vanity",
                                "display_name": "Google Canonical Vision",
                                "target_company": "Google",
                                "employment_status": "current",
                                "focus_areas": "Vision-language models",
                                "linkedin_url": requested_profile_url,
                                "metadata": {
                                    "profile_timeline_source_path": str(raw_profile_path),
                                    "profile_url": requested_profile_url,
                                },
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            scenario_path = Path(tempdir) / "scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "real_scraper",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "sample_candidate_documents_path": str(candidate_documents_path),
                                    "sample_candidate_contains": ["vision-language"],
                                    "sample_candidate_require_profile_source": True,
                                    "sample_fallback_generated": False,
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                    "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
                },
            ):
                profile_result = profile_connector.execute_batch_with_checkpoint(
                    [requested_profile_url],
                    snapshot_dir,
                )

        self.assertFalse(profile_result.pending)
        self.assertEqual(len(profile_result.body), 1)
        parsed = parse_harvest_profile_payload(profile_result.body[0])
        self.assertEqual(parsed["profile_url"], canonical_profile_url)
        self.assertEqual(parsed["requested_profile_url"], "https://www.linkedin.com/in/ACwCANONICAL")

    def test_scripted_real_asset_profile_source_rebases_runtime_object_store_path(self) -> None:
        profile_settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        profile_connector = HarvestProfileConnector(profile_settings)
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir) / "repo"
            runtime_dir = repo_root / "runtime"
            object_profile_dir = (
                runtime_dir
                / "object_store"
                / "sourcing-ai-agent-dev"
                / "bundles"
                / "company_handoff"
                / "payload"
                / "company_assets"
                / "google"
                / "snap-object"
                / "harvest_profiles"
            )
            snapshot_dir = runtime_dir / "company_assets" / "google" / "snap-real"
            object_profile_dir.mkdir(parents=True, exist_ok=True)
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            profile_url = "https://www.linkedin.com/in/google-object-store-vision/"
            raw_profile_path = object_profile_dir / "object_profile.json"
            raw_profile_path.write_text(
                json.dumps(
                    {
                        "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                        "item": {
                            "linkedinUrl": profile_url,
                            "publicIdentifier": "google-object-store-vision",
                            "fullName": "Google Object Store Vision",
                            "headline": "Vision-language researcher at Google",
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            candidate_documents_path = snapshot_dir / "candidate_documents.json"
            candidate_documents_path.write_text(
                json.dumps(
                    {
                        "snapshot": {"snapshot_id": "snap-real"},
                        "target_company": "Google",
                        "candidate_count": 1,
                        "candidates": [
                            {
                                "candidate_id": "google-object-store",
                                "display_name": "Google Object Store Vision",
                                "target_company": "Google",
                                "employment_status": "current",
                                "focus_areas": "Vision-language models",
                                "linkedin_url": profile_url,
                                "metadata": {
                                    "profile_timeline_source_path": (
                                        "runtime/object_store/sourcing-ai-agent-dev/bundles/"
                                        "company_handoff/payload/company_assets/google/"
                                        "snap-object/harvest_profiles/object_profile.json"
                                    ),
                                    "profile_url": profile_url,
                                },
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            scenario_path = Path(tempdir) / "scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "real_scraper",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "sample_candidate_documents_path": str(candidate_documents_path),
                                    "sample_candidate_contains": ["vision-language"],
                                    "sample_candidate_require_profile_source": True,
                                    "sample_fallback_generated": False,
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                    "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
                },
            ):
                with patch("sourcing_agent.harvest_connectors.Path.cwd", return_value=repo_root):
                    profile_result = profile_connector.execute_batch_with_checkpoint([profile_url], snapshot_dir)

        self.assertFalse(profile_result.pending)
        self.assertEqual(len(profile_result.body), 1)
        parsed = parse_harvest_profile_payload(profile_result.body[0])
        self.assertEqual(parsed["full_name"], "Google Object Store Vision")

    def test_scripted_real_asset_profile_source_rejects_non_profile_summary_path(self) -> None:
        profile_settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        profile_connector = HarvestProfileConnector(profile_settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "google" / "snap-real"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            profile_url = "https://www.linkedin.com/in/google-summary-not-profile/"
            summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.write_text(
                json.dumps({"entries": [{"profile_url": profile_url}], "status": "completed"}),
                encoding="utf-8",
            )
            candidate_documents_path = snapshot_dir / "candidate_documents.json"
            candidate_documents_path.write_text(
                json.dumps(
                    {
                        "snapshot": {"snapshot_id": "snap-real"},
                        "target_company": "Google",
                        "candidate_count": 1,
                        "candidates": [
                            {
                                "candidate_id": "google-summary-not-profile",
                                "display_name": "Google Summary Not Profile",
                                "target_company": "Google",
                                "employment_status": "current",
                                "focus_areas": "Vision-language models",
                                "linkedin_url": profile_url,
                                "metadata": {
                                    "profile_timeline_source_path": str(summary_path),
                                    "profile_url": profile_url,
                                },
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            scenario_path = Path(tempdir) / "scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "real_scraper",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "sample_candidate_documents_path": str(candidate_documents_path),
                                    "sample_candidate_contains": ["vision-language"],
                                    "sample_candidate_require_profile_source": True,
                                    "sample_fallback_generated": False,
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                    "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
                },
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "real-asset candidate filter produced no rows|real-asset profile sample produced no readable raw profiles",
                ):
                    profile_connector.execute_batch_with_checkpoint([profile_url], snapshot_dir)

    def test_scripted_real_asset_profile_search_paginates_once(self) -> None:
        search_settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        search_connector = HarvestProfileSearchConnector(search_settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "google" / "snap-real"
            profile_dir = snapshot_dir / "harvest_profiles"
            profile_dir.mkdir(parents=True, exist_ok=True)
            candidates: list[dict[str, object]] = []
            for index in range(30):
                profile_url = f"https://www.linkedin.com/in/google-real-vision-language-{index:02d}/"
                raw_profile_path = profile_dir / f"real_profile_{index:02d}.json"
                raw_profile_path.write_text(
                    json.dumps(
                        {
                            "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                            "item": {
                                "linkedinUrl": profile_url,
                                "publicIdentifier": f"google-real-vision-language-{index:02d}",
                                "fullName": f"Google Real Vision {index:02d}",
                                "headline": "Vision-language researcher at Google",
                            },
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                candidates.append(
                    {
                        "candidate_id": f"google-real-{index:02d}",
                        "display_name": f"Google Real Vision {index:02d}",
                        "target_company": "Google",
                        "employment_status": "current",
                        "focus_areas": "Vision-language models",
                        "linkedin_url": profile_url,
                        "metadata": {
                            "public_identifier": f"google-real-vision-language-{index:02d}",
                            "profile_timeline_source_path": str(raw_profile_path),
                            "profile_url": profile_url,
                        },
                    }
                )
            candidate_documents_path = snapshot_dir / "candidate_documents.json"
            candidate_documents_path.write_text(
                json.dumps({"candidates": candidates}, ensure_ascii=False),
                encoding="utf-8",
            )
            scenario_path = Path(tempdir) / "scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "real_search",
                                    "match": {"logical_name": "harvest_profile_search"},
                                    "sample_candidate_documents_path": str(candidate_documents_path),
                                    "sample_candidate_employment_scope": "current",
                                    "sample_candidate_contains": ["vision-language"],
                                    "sample_candidate_require_profile_source": True,
                                    "sample_fallback_generated": False,
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                    "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
                },
            ):
                page_two = search_connector.search_profiles(
                    query_text="vision-language",
                    filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                    employment_status="current",
                    discovery_dir=snapshot_dir / "search_seed_discovery",
                    limit=25,
                    pages=1,
                    start_page=2,
                    auto_probe=False,
                )

        assert page_two is not None
        self.assertEqual(page_two["pagination"]["total_elements"], 30)
        self.assertEqual(len(page_two["rows"]), 5)
        self.assertEqual(page_two["rows"][0]["profile_url"], "https://www.linkedin.com/in/google-real-vision-language-25/")

    def test_scripted_real_asset_candidate_documents_fail_closed_when_profile_source_missing(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "google" / "snap-real"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            profile_url = "https://www.linkedin.com/in/google-real-missing/"
            candidate_documents_path = snapshot_dir / "candidate_documents.json"
            candidate_documents_path.write_text(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "display_name": "Google Missing",
                                "employment_status": "current",
                                "focus_areas": "Vision-language",
                                "linkedin_url": profile_url,
                                "metadata": {
                                    "profile_timeline_source_path": str(snapshot_dir / "missing.json"),
                                    "profile_url": profile_url,
                                },
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            scenario_path = Path(tempdir) / "scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "real_scraper",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "sample_candidate_documents_path": str(candidate_documents_path),
                                    "sample_candidate_contains": ["vision-language"],
                                    "sample_fallback_generated": False,
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                },
            ):
                with self.assertRaisesRegex(RuntimeError, "real-asset profile sample produced no readable raw profiles"):
                    connector.execute_batch_with_checkpoint([profile_url], snapshot_dir)

    def test_company_employees_scripted_completion_exposes_dataset_items_artifact(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Physical Intelligence",
            canonical_name="Physical Intelligence",
            company_key="physicalintelligence",
            linkedin_slug="physical-intelligence",
            linkedin_company_url="https://www.linkedin.com/company/physical-intelligence/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "physicalintelligence" / "snap-scripted"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            scenario_path = Path(tempdir) / "scripted_company_roster.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "company_roster",
                                    "match": {"logical_name": "harvest_company_employees"},
                                    "execute_pending_rounds": 1,
                                    "body": [
                                        {
                                            "linkedinUrl": "https://www.linkedin.com/in/scripted-pi-systems/",
                                            "publicIdentifier": "scripted-pi-systems",
                                            "headline": "Systems Engineer at Physical Intelligence",
                                            "currentCompany": "Physical Intelligence",
                                        }
                                    ],
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                    "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
                },
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("scripted company roster must not submit live Harvest runs"),
            ):
                first = connector.execute_with_checkpoint(identity, snapshot_dir, max_pages=1, page_limit=25)
                second = connector.execute_with_checkpoint(
                    identity,
                    snapshot_dir,
                    max_pages=1,
                    page_limit=25,
                    checkpoint=first.checkpoint,
                )
                invocations = load_scripted_provider_invocations()

        self.assertTrue(first.pending)
        self.assertFalse(second.pending)
        self.assertEqual(second.checkpoint["status"], "completed")
        self.assertEqual(len(second.body), 1)
        dataset_artifacts = [artifact for artifact in second.artifacts if artifact.label == "dataset_items"]
        self.assertEqual(len(dataset_artifacts), 1)
        self.assertEqual(dataset_artifacts[0].payload, second.body)
        self.assertEqual(
            [item.get("logical_name") for item in invocations],
            ["harvest_company_employees"],
        )

    def test_lovable_live_roster_scripted_fixture_replays_unique_100_plus_roster_sample(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Lovable",
            canonical_name="Lovable",
            company_key="lovable",
            linkedin_slug="lovable",
            linkedin_company_url="https://www.linkedin.com/company/lovable/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "lovable" / "snap-scripted"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(self._lovable_live_roster_scenario_path()),
                    "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
                },
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("scripted Lovable roster must not submit live Harvest runs"),
            ):
                checkpoint = None
                attempts = []
                for _attempt in range(3):
                    result = connector.execute_with_checkpoint(
                        identity,
                        snapshot_dir,
                        max_pages=20,
                        page_limit=25,
                        checkpoint=checkpoint,
                    )
                    attempts.append(result)
                    checkpoint = result.checkpoint
                    if not result.pending:
                        break

        self.assertEqual([attempt.pending for attempt in attempts], [True, True, False])
        second = attempts[-1]
        self.assertGreaterEqual(len(second.body), 100)
        names = [
            str(
                item.get("fullName")
                or item.get("item", {}).get("fullName")
                or " ".join(
                    part
                    for part in (str(item.get("firstName") or "").strip(), str(item.get("lastName") or "").strip())
                    if part
                )
            )
            for item in second.body
        ]
        public_ids = [
            str(item.get("publicIdentifier") or item.get("id") or item.get("linkedinUrl") or "") for item in second.body
        ]
        locations = {str(item.get("location") or "") for item in second.body if item.get("location")}
        self.assertEqual(len(set(names)), len(names))
        self.assertEqual(len(set(public_ids)), len(public_ids))
        self.assertGreater(len(locations), 1)
        self.assertTrue(
            all(
                any(str(position.get("companyName") or "") == "Lovable" for position in item.get("currentPositions") or [])
                for item in second.body
            )
        )

    def test_openai_chatgpt_scripted_fixture_replays_real_search_and_profile_samples(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="token",
            actor_id="actor",
            default_mode="full",
            max_paid_items=25,
        )
        search_connector = HarvestProfileSearchConnector(settings)
        profile_connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            discovery_dir = runtime_dir / "company_assets" / "openai" / "snap-chatgpt" / "search_seed_discovery"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-chatgpt"
            discovery_dir.mkdir(parents=True, exist_ok=True)
            env = {
                "SOURCING_RUNTIME_DIR": str(runtime_dir),
                "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(self._openai_chatgpt_streaming_scenario_path()),
                "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
            }
            with patch.dict("os.environ", env), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("scripted ChatGPT profile-search must not submit live Harvest runs"),
            ):
                search_result = search_connector.search_profiles(
                    query_text="ChatGPT",
                    filter_hints={
                        "current_companies": ["https://www.linkedin.com/company/openai/"],
                        "function_ids": ["8", "24"],
                    },
                    employment_status="current",
                    discovery_dir=discovery_dir,
                    limit=25,
                    pages=1,
                    auto_probe=False,
                )
                urls = [str(row.get("profile_url") or "") for row in list(search_result["rows"])[:20]]
                current_full = search_connector.search_profiles(
                    query_text="ChatGPT",
                    filter_hints={
                        "current_companies": ["https://www.linkedin.com/company/openai/"],
                        "function_ids": ["8", "24"],
                    },
                    employment_status="current",
                    discovery_dir=discovery_dir,
                    limit=175,
                    pages=7,
                    auto_probe=False,
                    allow_shared_provider_cache=False,
                )
                former_full = search_connector.search_profiles(
                    query_text="ChatGPT",
                    filter_hints={
                        "past_companies": ["https://www.linkedin.com/company/openai/"],
                        "function_ids": ["8", "24"],
                    },
                    employment_status="former",
                    discovery_dir=discovery_dir,
                    limit=100,
                    pages=4,
                    auto_probe=False,
                    allow_shared_provider_cache=False,
                )
                all_urls = []
                seen_urls = set()
                for row in list(current_full["rows"]) + list(former_full["rows"]):
                    profile_url = str(row.get("profile_url") or "").strip()
                    normalized_url = profile_url.rstrip("/").lower()
                    if profile_url and normalized_url not in seen_urls:
                        seen_urls.add(normalized_url)
                        all_urls.append(profile_url)

                def run_profile_batch(batch_urls: list[str]):
                    checkpoint = None
                    result = None
                    for _attempt in range(3):
                        result = profile_connector.execute_batch_with_checkpoint(
                            batch_urls,
                            snapshot_dir,
                            checkpoint=checkpoint,
                        )
                        checkpoint = result.checkpoint
                        if not result.pending:
                            break
                        if checkpoint.get("run_id") and checkpoint.get("dataset_id"):
                            checkpoint = {
                                **checkpoint,
                                "remote_provider_terminal_event": {
                                    "status": "succeeded",
                                    "run_id": checkpoint["run_id"],
                                    "dataset_id": checkpoint["dataset_id"],
                                },
                            }
                    assert result is not None
                    return result

                profile_result = run_profile_batch(urls[:10])
                second_profile_result = run_profile_batch(urls[10:20])
                full_profile_result = run_profile_batch(all_urls)

        assert profile_result is not None
        assert full_profile_result is not None
        self.assertEqual(len(search_result["rows"]), 25)
        self.assertEqual(search_result["pagination"]["total_elements"], 218)
        self.assertFalse(any("OpenAI ChatGPT openai-chatgpt" in str(row.get("full_name") or "") for row in search_result["rows"]))
        self.assertEqual(len(all_urls), 250)
        self.assertFalse(profile_result.pending)
        self.assertEqual(len(profile_result.body), 10)
        self.assertFalse(second_profile_result.pending)
        self.assertEqual(len(second_profile_result.body), 10)
        self.assertFalse(full_profile_result.pending)
        self.assertEqual(len(full_profile_result.body), 250)
        self.assertFalse(
            any("OpenAI ChatGPT" in str(row.get("fullName") or row.get("full_name") or "") for row in full_profile_result.body)
        )
        self.assertNotEqual(profile_result.checkpoint["run_id"], second_profile_result.checkpoint["run_id"])
        self.assertNotEqual(profile_result.checkpoint["dataset_id"], second_profile_result.checkpoint["dataset_id"])
        parsed_profiles = [parse_harvest_profile_payload(row) for row in profile_result.body]
        self.assertGreaterEqual(sum(bool(profile.get("experience")) for profile in parsed_profiles), 4)
        self.assertGreaterEqual(sum(bool(profile.get("education")) for profile in parsed_profiles), 4)
        self.assertGreaterEqual(len({str(profile.get("location") or "") for profile in parsed_profiles}), 5)
        self.assertTrue(any(str(profile.get("full_name") or "") == "Martin Spier" for profile in parsed_profiles))

    def test_openai_agent_scripted_profile_search_models_probe_and_scale_without_live_api(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="token",
            actor_id="actor",
            default_mode="short",
            max_paid_items=25,
        )
        connector = HarvestProfileSearchConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            discovery_dir = runtime_dir / "company_assets" / "openai" / "snap-agent" / "search_seed_discovery"
            discovery_dir.mkdir(parents=True, exist_ok=True)
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(self._openai_agent_streaming_scenario_path()),
                },
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("scripted profile-search must not submit live Harvest runs"),
            ), patch(
                "sourcing_agent.harvest_connectors.time.sleep",
                return_value=None,
            ):
                current_result = connector.search_profiles(
                    query_text="Agent",
                    filter_hints={
                        "current_companies": ["https://www.linkedin.com/company/openai/"],
                        "function_ids": ["8", "24"],
                    },
                    employment_status="current",
                    discovery_dir=discovery_dir,
                    limit=125,
                    pages=5,
                    auto_probe=True,
                )
                former_result = connector.search_profiles(
                    query_text="Agent",
                    filter_hints={
                        "past_companies": ["https://www.linkedin.com/company/openai/"],
                        "function_ids": ["8", "24"],
                    },
                    employment_status="former",
                    discovery_dir=discovery_dir,
                    limit=100,
                    pages=4,
                    auto_probe=True,
                )
                invocations = load_scripted_provider_invocations()

        assert current_result is not None
        assert former_result is not None
        self.assertEqual(len(current_result["rows"]), 125)
        self.assertEqual(current_result["pagination"]["total_elements"], 236)
        self.assertEqual(current_result["pagination"]["total_pages"], 10)
        self.assertEqual(len(former_result["rows"]), 78)
        self.assertEqual(former_result["pagination"]["total_elements"], 78)
        current_names = [
            str(row.get("fullName") or row.get("full_name") or row.get("name") or "")
            for row in current_result["rows"][:5]
        ]
        self.assertEqual(len(set(current_names)), len(current_names))
        self.assertTrue(all("openai-agent-current" in name for name in current_names))
        profile_search_invocations = [
            item for item in invocations if item.get("logical_name") == "harvest_profile_search"
        ]
        current_payloads = [
            dict(item.get("payload") or {})
            for item in profile_search_invocations
            if list(dict(item.get("payload") or {}).get("currentCompanies") or [])
        ]
        former_payloads = [
            dict(item.get("payload") or {})
            for item in profile_search_invocations
            if list(dict(item.get("payload") or {}).get("pastCompanies") or [])
        ]
        self.assertEqual([payload["maxItems"] for payload in current_payloads], [25, 125])
        self.assertEqual([payload["takePages"] for payload in current_payloads], [1, 5])
        self.assertEqual([payload["maxItems"] for payload in former_payloads], [25, 78])
        self.assertEqual([payload["takePages"] for payload in former_payloads], [1, 4])
        self.assertTrue(all(payload.get("searchQuery") == "Agent" for payload in current_payloads + former_payloads))

    def test_scripted_profile_search_honors_explicit_zero_returned_count(self) -> None:
        scenario = {
            "harvest": {
                "rules": [
                    {
                        "name": "google_gemini_zero_scaled",
                        "match": {
                            "logical_name": "harvest_profile_search",
                            "payload_equals": {"startPage": 1, "takePages": 3},
                        },
                        "generated_body": {
                            "kind": "profile_search",
                            "company": "Google",
                            "employment_scope": "current",
                            "search_query": "Gemini",
                            "estimated_total_count": 3,
                            "returned_count": 0,
                            "total_pages": 3
                        }
                    }
                ]
            }
        }
        with tempfile.TemporaryDirectory() as tempdir:
            scenario_path = Path(tempdir) / "scenario.json"
            scenario_path.write_text(json.dumps(scenario), encoding="utf-8")
            settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
            connector = HarvestProfileSearchConnector(settings)
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": tempdir,
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                },
            ), patch("sourcing_agent.harvest_connectors.time.sleep", return_value=None):
                result = connector.search_profiles(
                    query_text="Gemini",
                    filter_hints={
                        "current_companies": ["https://www.linkedin.com/company/google/"],
                    },
                    employment_status="current",
                    discovery_dir=Path(tempdir),
                    limit=75,
                    pages=3,
                    auto_probe=False,
                )

        assert result is not None
        self.assertEqual(result["rows"], [])
        self.assertEqual(result["pagination"]["total_elements"], 0)
        self.assertEqual(result["pagination"]["total_pages"], 0)

    def test_openai_agent_scripted_profile_scraper_batches_resume_and_generate_profiles(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        urls = [
            "https://www.linkedin.com/in/openai-agent-current-0001/",
            "https://www.linkedin.com/in/openai-agent-current-0002/",
            "https://www.linkedin.com/in/openai-agent-former-0003/",
        ]
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-agent"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            env = {
                "SOURCING_RUNTIME_DIR": str(runtime_dir),
                "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(self._openai_agent_streaming_scenario_path()),
            }
            fake_times = iter([1000.0, 1000.0, 1001.0, 1013.0])

            def _fake_time() -> float:
                return next(fake_times, 1013.0)

            with (
                patch.dict("os.environ", env),
                patch("sourcing_agent.harvest_connectors.time.sleep"),
                patch("sourcing_agent.harvest_connectors.time.time", side_effect=_fake_time),
            ):
                first = connector.execute_batch_with_checkpoint(urls, snapshot_dir)
                second = connector.execute_batch_with_checkpoint(urls, snapshot_dir, checkpoint=first.checkpoint)
                terminal = second
                for _ in range(20):
                    if not terminal.pending:
                        break
                    terminal = connector.execute_batch_with_checkpoint(
                        urls,
                        snapshot_dir,
                        checkpoint=terminal.checkpoint,
                    )
                invocations = load_scripted_provider_invocations()

        self.assertTrue(first.pending)
        self.assertTrue(second.pending)
        self.assertFalse(terminal.pending)
        self.assertEqual(terminal.checkpoint["provider_mode"], "scripted")
        self.assertEqual(terminal.checkpoint["status"], "completed")
        self.assertEqual(len(terminal.body), 3)
        self.assertEqual(
            [item.get("logical_name") for item in invocations],
            ["harvest_profile_scraper_batch"],
        )
        self.assertEqual(
            len({str(item.get("fullName") or "") for item in terminal.body}),
            3,
        )
        parsed = parse_harvest_profile_payload(terminal.body[0])
        self.assertEqual(parsed["current_company"], "OpenAI")
        self.assertIn("Agent Research Engineer", parsed["headline"])
        self.assertTrue(any(item.get("companyName") == "OpenAI" for item in parsed["experience"]))

    def test_scripted_profile_scraper_template_can_use_slug_index_for_roster_name_matching(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "lovable" / "snap-lovable"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            scenario_path = Path(tempdir) / "scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "match": {
                                        "logical_name": "harvest_profile_scraper_batch",
                                        "payload_contains": ["lovable-roster"],
                                    },
                                    "generated_body": {
                                        "kind": "profile_scraper_batch",
                                        "company": "Lovable",
                                        "search_query": "Product Engineering",
                                        "full_name_template": "Lovable Employee {slug_index:04d}",
                                    },
                                }
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )
            env = {
                "SOURCING_RUNTIME_DIR": str(runtime_dir),
                "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
            }
            with patch.dict("os.environ", env):
                result = connector.execute_batch_with_checkpoint(
                    [
                        "https://www.linkedin.com/in/lovable-roster-0025/",
                        "https://www.linkedin.com/in/lovable-roster-0107/",
                    ],
                    snapshot_dir,
                )

        self.assertFalse(result.pending)
        self.assertEqual([row["fullName"] for row in result.body], ["Lovable Employee 0025", "Lovable Employee 0107"])

    def test_openai_agent_scripted_profile_scraper_timing_matrix_models_out_of_order_and_timeout_tail(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)

        def _execute_until_complete(urls: list[str], snapshot_dir: Path) -> list[Any]:
            results = []
            checkpoint = None
            result = connector.execute_batch_with_checkpoint(urls, snapshot_dir, checkpoint=checkpoint)
            results.append(result)
            if not result.pending:
                return results
            checkpoint = {
                **dict(result.checkpoint or {}),
                "remote_provider_terminal_event": {"event_type": "ACTOR.RUN.SUCCEEDED"},
                "force_scripted_terminal_fetch": True,
            }
            completed = connector.execute_batch_with_checkpoint(urls, snapshot_dir, checkpoint=checkpoint)
            results.append(completed)
            return results

        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-agent-timing"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            env = {
                "SOURCING_RUNTIME_DIR": str(runtime_dir),
                "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(self._openai_agent_streaming_scenario_path()),
                "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "",
            }
            with patch.dict("os.environ", env), patch(
                "sourcing_agent.harvest_connectors.time.sleep", return_value=None
            ) as sleep_mock:
                fast_later = _execute_until_complete(
                    ["https://www.linkedin.com/in/openai-agent-current-0028/"],
                    snapshot_dir,
                )
                retryable_mid = _execute_until_complete(
                    ["https://www.linkedin.com/in/openai-agent-current-0054/"],
                    snapshot_dir,
                )
                timeout_tail = _execute_until_complete(
                    ["https://www.linkedin.com/in/openai-agent-former-0053/"],
                    snapshot_dir,
                )

        self.assertEqual([result.pending for result in fast_later], [True, False])
        self.assertEqual([result.pending for result in retryable_mid], [True, False])
        self.assertEqual([result.pending for result in timeout_tail], [True, False])
        self.assertEqual(fast_later[-1].checkpoint["scripted_rule_name"], "openai_agent_profile_scraper_current_fast_out_of_order")
        self.assertEqual(
            retryable_mid[-1].checkpoint["scripted_rule_name"],
            "openai_agent_profile_scraper_current_retryable_mid_tail",
        )
        self.assertEqual(
            timeout_tail[-1].checkpoint["scripted_rule_name"],
            "openai_agent_profile_scraper_former_timeout_long_tail",
        )
        self.assertTrue(any(artifact.label == "scripted_harvest_pending" for artifact in timeout_tail[0].artifacts))
        self.assertFalse(any(artifact.payload.get("kind") == "timeout" for artifact in timeout_tail[0].artifacts))
        self.assertTrue(fast_later[0].checkpoint["scripted_remote_wait_after_submit"])
        self.assertEqual(fast_later[0].checkpoint["scripted_remote_wait_seconds"], 1.0)
        self.assertEqual(retryable_mid[0].checkpoint["scripted_remote_wait_seconds"], 8.0)
        self.assertEqual(timeout_tail[0].checkpoint["scripted_remote_wait_seconds"], 14.0)
        self.assertTrue(fast_later[-1].checkpoint["remote_provider_terminal_event_consumed"])
        self.assertTrue(retryable_mid[-1].checkpoint["remote_provider_terminal_event_consumed"])
        self.assertTrue(timeout_tail[-1].checkpoint["remote_provider_terminal_event_consumed"])
        sleep_mock.assert_not_called()

    def test_harvest_profile_batch_execute_with_checkpoint_request_scoped_fast_smoke_caps_scripted_sleep_across_resume(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            snapshot_dir = runtime_dir / "company_assets" / "openai" / "snap-scripted-fast"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            scenario_path = Path(tempdir) / "scripted_harvest_fast.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "profile_batch_fast_smoke",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "execute_pending_rounds": 1,
                                    "execute_sleep_seconds": 2.0,
                                    "body": [
                                        {
                                            "linkedinUrl": "https://www.linkedin.com/in/jane-doe/",
                                            "publicIdentifier": "jane-doe",
                                            "headline": "Reasoning engineer at OpenAI",
                                            "item": {
                                                "profileUrl": "https://www.linkedin.com/in/jane-doe/",
                                                "fullName": "Jane Doe",
                                                "headline": "Reasoning engineer at OpenAI",
                                            },
                                        }
                                    ],
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                },
            ), patch("sourcing_agent.harvest_connectors.time.sleep", return_value=None) as sleep_mock:
                first = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                    runtime_timing_overrides={"runtime_tuning_profile": "fast_smoke"},
                )
                second = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                    checkpoint=first.checkpoint,
                )

        self.assertTrue(first.pending)
        self.assertEqual(first.checkpoint["request_context"]["runtime_tuning_profile"], "fast_smoke")
        self.assertFalse(second.pending)
        self.assertEqual(second.checkpoint["request_context"]["runtime_tuning_profile"], "fast_smoke")
        self.assertEqual(sleep_mock.call_args_list, [call(0.1), call(0.1)])

    def test_get_harvest_dataset_items_paginates_large_dataset_download(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        observed_offsets: list[int] = []
        observed_timeouts: list[int] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            observed_timeouts.append(int(timeout))
            query = urlparse.parse_qs(urlparse.urlparse(endpoint).query)
            offset = int(query.get("offset", ["0"])[0])
            limit = int(query.get("limit", ["0"])[0])
            observed_offsets.append(offset)
            self.assertEqual(limit, 100)
            if offset == 0:
                return [{"idx": index} for index in range(100)]
            if offset == 100:
                return [{"idx": index} for index in range(100, 200)]
            if offset == 200:
                return [{"idx": 200}]
            return []

        with patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request):
            items = _get_harvest_dataset_items(
                settings,
                "dataset-large",
                logical_name="harvest_company_employees",
                run_id="run-large",
                request_context={"requested_item_count": 2500},
            )

        self.assertEqual(len(items), 201)
        self.assertEqual(observed_offsets, [0, 100, 200])
        self.assertEqual(observed_timeouts, [45, 45, 45])

    def test_submit_harvest_actor_run_attaches_apify_ad_hoc_webhook_when_configured(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="token",
            actor_id="harvestapi/linkedin-profile-scraper",
            default_mode="full",
            timeout_seconds=900,
            max_total_charge_usd=1.25,
        )
        observed_endpoints: list[str] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            observed_endpoints.append(endpoint)
            return {"data": {"id": "run-webhook", "status": "RUNNING"}}

        with (
            patch.dict(
                os.environ,
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "https://runtime.example.test/api/providers/apify/webhook",
                    "SOURCING_PROVIDER_WEBHOOK_TOKEN": "provider-secret",
                },
                clear=False,
            ),
            patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request),
        ):
            _submit_harvest_actor_run(settings, {"urls": ["https://www.linkedin.com/in/example/"]})

        query = urlparse.parse_qs(urlparse.urlparse(observed_endpoints[0]).query)
        self.assertEqual(query["waitForFinish"], ["0"])
        self.assertEqual(query["timeout"], ["900"])
        self.assertEqual(query["maxTotalChargeUsd"], ["1.25"])
        webhook_defs = json.loads(base64.b64decode(query["webhooks"][0]).decode("utf-8"))
        self.assertEqual(len(webhook_defs), 1)
        webhook = webhook_defs[0]
        self.assertEqual(webhook["requestUrl"], "https://runtime.example.test/api/providers/apify/webhook")
        self.assertEqual(
            webhook["eventTypes"],
            [
                "ACTOR.RUN.SUCCEEDED",
                "ACTOR.RUN.FAILED",
                "ACTOR.RUN.TIMED_OUT",
                "ACTOR.RUN.ABORTED",
            ],
        )
        self.assertEqual(
            json.loads(webhook["headersTemplate"]),
            {
                "X-Sourcing-Provider-Webhook-Token": "provider-secret",
                "User-Agent": "SourcingAgentApifyWebhook/1.0",
            },
        )

    def test_submit_harvest_actor_run_reuses_apify_api_token_for_webhook_secret_by_default(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="actor-api-token",
            actor_id="harvestapi/linkedin-profile-scraper",
            default_mode="full",
            timeout_seconds=900,
            max_total_charge_usd=1.25,
        )
        observed_endpoints: list[str] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            observed_endpoints.append(endpoint)
            return {"data": {"id": "run-webhook", "status": "RUNNING"}}

        with (
            patch.dict(
                os.environ,
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "https://runtime.example.test/api/providers/apify/webhook",
                    "SOURCING_PROVIDER_WEBHOOK_TOKEN": "",
                    "APIFY_WEBHOOK_TOKEN": "",
                    "SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN": "",
                },
                clear=False,
            ),
            patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request),
        ):
            _submit_harvest_actor_run(settings, {"urls": ["https://www.linkedin.com/in/example/"]})

        query = urlparse.parse_qs(urlparse.urlparse(observed_endpoints[0]).query)
        webhook_defs = json.loads(base64.b64decode(query["webhooks"][0]).decode("utf-8"))
        self.assertEqual(
            json.loads(webhook_defs[0]["headersTemplate"]),
            {
                "X-Sourcing-Provider-Webhook-Token": "actor-api-token",
                "User-Agent": "SourcingAgentApifyWebhook/1.0",
            },
        )

    def test_submit_harvest_actor_run_defaults_hosted_webhook_for_production_live_runtime(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="actor-api-token",
            actor_id="harvestapi/linkedin-profile-scraper",
            default_mode="full",
            timeout_seconds=900,
            max_total_charge_usd=1.25,
        )
        observed_endpoints: list[str] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            observed_endpoints.append(endpoint)
            return {"data": {"id": "run-webhook", "status": "RUNNING"}}

        with (
            patch.dict(
                os.environ,
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_RUNTIME_ENVIRONMENT": "production",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED": "",
                    "SOURCING_PROVIDER_WEBHOOK_TOKEN": "",
                    "APIFY_WEBHOOK_TOKEN": "",
                    "SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN": "1",
                },
                clear=False,
            ),
            patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request),
        ):
            _submit_harvest_actor_run(settings, {"urls": ["https://www.linkedin.com/in/example/"]})

        query = urlparse.parse_qs(urlparse.urlparse(observed_endpoints[0]).query)
        webhook_defs = json.loads(base64.b64decode(query["webhooks"][0]).decode("utf-8"))
        self.assertEqual(webhook_defs[0]["requestUrl"], "https://api.111874.xyz/api/providers/apify/webhook")
        self.assertEqual(
            json.loads(webhook_defs[0]["headersTemplate"])["X-Sourcing-Provider-Webhook-Token"],
            "actor-api-token",
        )

    def test_submit_harvest_actor_run_defaults_local_dev_webhook_for_local_live_runtime(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="actor-api-token",
            actor_id="harvestapi/linkedin-profile-scraper",
            default_mode="full",
            timeout_seconds=900,
            max_total_charge_usd=1.25,
        )
        observed_endpoints: list[str] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            observed_endpoints.append(endpoint)
            return {"data": {"id": "run-webhook", "status": "RUNNING"}}

        with (
            patch.dict(
                os.environ,
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED": "",
                    "SOURCING_LOCAL_DEV_APIFY_WEBHOOK_URL": "https://relay.example.test/local-dev/providers/apify/webhook",
                    "SOURCING_PROVIDER_WEBHOOK_TOKEN": "local-dev-secret",
                },
                clear=False,
            ),
            patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request),
        ):
            _submit_harvest_actor_run(settings, {"urls": ["https://www.linkedin.com/in/example/"]})

        query = urlparse.parse_qs(urlparse.urlparse(observed_endpoints[0]).query)
        webhook_defs = json.loads(base64.b64decode(query["webhooks"][0]).decode("utf-8"))
        self.assertEqual(webhook_defs[0]["requestUrl"], "https://relay.example.test/local-dev/providers/apify/webhook")
        self.assertEqual(
            json.loads(webhook_defs[0]["headersTemplate"])["X-Sourcing-Provider-Webhook-Token"],
            "local-dev-secret",
        )

    def test_submit_harvest_actor_run_rejects_scripted_runtime_before_live_http(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="actor-api-token",
            actor_id="harvestapi/linkedin-profile-scraper",
            default_mode="full",
            timeout_seconds=900,
            max_total_charge_usd=1.25,
        )
        with (
            patch.dict(
                os.environ,
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_RUNTIME_ENVIRONMENT": "scripted",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED": "",
                },
                clear=False,
            ),
            patch("sourcing_agent.harvest_connectors._harvest_json_request") as request_mock,
        ):
            with self.assertRaises(LiveProviderAccessError):
                _submit_harvest_actor_run(settings, {"urls": ["https://www.linkedin.com/in/example/"]})

        request_mock.assert_not_called()

    def test_harvest_json_request_rejects_apify_endpoint_in_scripted_runtime_before_http(self) -> None:
        endpoint = "https://api.apify.com/v2/acts/harvestapi%2Flinkedin-profile-scraper/runs?token=secret"
        with (
            patch.dict(
                os.environ,
                {
                    "SOURCING_RUNTIME_ENVIRONMENT": "scripted",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                },
                clear=True,
            ),
            patch("sourcing_agent.harvest_connectors.request.urlopen") as urlopen_mock,
        ):
            with self.assertRaises(LiveProviderAccessError):
                _harvest_json_request(endpoint, payload={"urls": ["https://www.linkedin.com/in/example/"]})

        urlopen_mock.assert_not_called()

    def test_fake_apify_base_url_exercises_async_dataset_http_connector(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="fake-token",
            actor_id="harvestapi/linkedin-profile-scraper",
            default_mode="full",
            timeout_seconds=60,
            max_total_charge_usd=1.25,
        )
        actor_path = "/v2/acts/harvestapi%2Flinkedin-profile-scraper/runs"
        run_path = "/v2/actor-runs/run-1"
        dataset_path = "/v2/datasets/dataset-1/items"
        dataset_items = [
            {
                "name": "Ada Lovelace",
                "linkedinUrl": "https://www.linkedin.com/in/ada-lovelace-real/",
            }
        ]

        fake_apify = FakeApifyProvider()
        fake_apify.add_actor_run(
            actor_id="harvestapi/linkedin-profile-scraper",
            run_id="run-1",
            dataset_id="dataset-1",
            dataset_items=dataset_items,
        )
        with fake_apify:
            with patch.dict(
                os.environ,
                {
                    "SOURCING_APIFY_API_BASE_URL": fake_apify.base_url,
                    "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
                    "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
                    "SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED": "0",
                    "HARVEST_RUN_STATUS_WAIT_FOR_FINISH_SECONDS": "0",
                },
                clear=True,
            ):
                result = _run_harvest_actor_via_async_dataset(
                    settings,
                    {"urls": ["https://www.linkedin.com/in/ada-lovelace-real/"]},
                    logical_name="harvest_profile_scraper_batch",
                    request_context={
                        "harvest_poll_interval_seconds": 0.0,
                        "harvest_dataset_fetch_max_attempts": 1,
                    },
                )

        self.assertEqual(result, dataset_items)
        self.assertEqual(
            [(request["method"], request["path"]) for request in fake_apify.requests],
            [("POST", actor_path), ("GET", run_path), ("GET", dataset_path)],
        )
        submit_request = fake_apify.requests[0]
        self.assertEqual(submit_request["payload"], {"urls": ["https://www.linkedin.com/in/ada-lovelace-real/"]})
        self.assertEqual(submit_request["query"]["waitForFinish"], ["0"])
        self.assertEqual(submit_request["query"]["token"], ["fake-token"])
        dataset_request = fake_apify.requests[2]
        self.assertEqual(dataset_request["query"]["format"], ["json"])
        self.assertEqual(dataset_request["query"]["clean"], ["true"])
        self.assertEqual(dataset_request["query"]["offset"], ["0"])

    def test_fake_apify_dataset_rate_limit_maps_to_retryable_dataset_error(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="fake-token", actor_id="actor", default_mode="full")
        dataset_path = "/v2/datasets/dataset-rate-limit/items"
        fake_apify = FakeApifyProvider()
        fake_apify.add_actor_run(
            actor_id="actor",
            run_id="run-rate-limit",
            dataset_id="dataset-rate-limit",
            dataset_failures=[(429, {"error": {"message": "rate limited by fake Apify"}})],
        )
        with fake_apify:
            with patch.dict(
                os.environ,
                {
                    "SOURCING_APIFY_API_BASE_URL": fake_apify.base_url,
                    "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
                    "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
                },
                clear=True,
            ):
                with self.assertRaises(HarvestRetryableRequestError):
                    _get_harvest_dataset_items(
                        settings,
                        "dataset-rate-limit",
                        logical_name="harvest_profile_scraper_batch",
                        run_id="run-rate-limit",
                        request_context={
                            "requested_url_count": 25,
                            "harvest_dataset_fetch_max_attempts": 1,
                        },
                    )

        self.assertEqual([(request["method"], request["path"]) for request in fake_apify.requests], [("GET", dataset_path)])

    def test_fake_apify_provider_can_deliver_configured_webhook_to_local_receiver(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="fake-token",
            actor_id="harvestapi/linkedin-profile-scraper",
            default_mode="full",
            timeout_seconds=60,
            max_total_charge_usd=1.25,
        )
        fake_apify = FakeApifyProvider()
        fake_apify.add_actor_run(
            actor_id="harvestapi/linkedin-profile-scraper",
            run_id="run-webhook",
            dataset_id="dataset-webhook",
        )
        receiver_path = "/api/providers/apify/webhook"
        with FakeProviderHTTPServer({("POST", receiver_path): (200, {"ok": True})}) as receiver:
            with fake_apify:
                with patch.dict(
                    os.environ,
                    {
                        "SOURCING_APIFY_API_BASE_URL": fake_apify.base_url,
                        "SOURCING_APIFY_WEBHOOK_URL": receiver.url(receiver_path),
                        "SOURCING_PROVIDER_WEBHOOK_TOKEN": "provider-secret",
                        "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                        "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                        "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
                        "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
                    },
                    clear=True,
                ):
                    _submit_harvest_actor_run(
                        settings,
                        {"urls": ["https://www.linkedin.com/in/ada-lovelace-real/"]},
                    )
                    delivered = fake_apify.deliver_webhooks(run_id="run-webhook")

        self.assertEqual(delivered[0]["status"], 200)
        webhooks = fake_apify.submitted_webhooks()
        self.assertEqual(webhooks[0]["requestUrl"], receiver.url(receiver_path))
        self.assertEqual(receiver.requests[0]["headers"]["X-Sourcing-Provider-Webhook-Token"], "provider-secret")
        self.assertEqual(receiver.requests[0]["payload"]["eventType"], "ACTOR.RUN.SUCCEEDED")
        self.assertEqual(receiver.requests[0]["payload"]["eventData"]["actorRunId"], "run-webhook")

    def test_harvest_json_request_rejects_configured_fake_apify_endpoint_in_scripted_runtime_before_http(self) -> None:
        actor_path = "/v2/acts/harvestapi%2Flinkedin-profile-scraper/runs"
        fake_apify = FakeApifyProvider()
        fake_apify.add_actor_run(
            actor_id="harvestapi/linkedin-profile-scraper",
            run_id="run-should-not-start",
            dataset_id="dataset-should-not-start",
        )
        with fake_apify:
            endpoint = f"{fake_apify.base_url}{actor_path}?token=secret"
            with patch.dict(
                os.environ,
                {
                    "SOURCING_APIFY_API_BASE_URL": fake_apify.base_url,
                    "SOURCING_RUNTIME_ENVIRONMENT": "scripted",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                },
                clear=True,
            ):
                with self.assertRaises(LiveProviderAccessError):
                    _harvest_json_request(
                        endpoint,
                        payload={"urls": ["https://www.linkedin.com/in/ada-lovelace-real/"]},
                    )

        self.assertEqual(fake_apify.requests, [])

    def test_harvest_json_request_rejects_synthetic_fixture_payload_even_in_local_live_runtime(self) -> None:
        endpoint = "https://api.apify.com/v2/acts/harvestapi%2Flinkedin-profile-scraper/runs?token=secret"
        with (
            patch.dict(
                os.environ,
                {
                    "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                },
                clear=True,
            ),
            patch("sourcing_agent.harvest_connectors.request.urlopen") as urlopen_mock,
        ):
            with self.assertRaises(LiveProviderAccessError):
                _harvest_json_request(
                    endpoint,
                    payload={"urls": ["https://www.linkedin.com/in/openai-agent-current-0189/"]},
                )

        urlopen_mock.assert_not_called()

    def test_submit_harvest_actor_run_can_disable_default_webhook_url(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="actor-api-token",
            actor_id="harvestapi/linkedin-profile-scraper",
            default_mode="full",
            timeout_seconds=900,
            max_total_charge_usd=1.25,
        )
        observed_endpoints: list[str] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            observed_endpoints.append(endpoint)
            return {"data": {"id": "run-no-webhook", "status": "RUNNING"}}

        with (
            patch.dict(
                os.environ,
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_RUNTIME_ENVIRONMENT": "production",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED": "0",
                },
                clear=False,
            ),
            patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request),
        ):
            _submit_harvest_actor_run(settings, {"urls": ["https://www.linkedin.com/in/example/"]})

        query = urlparse.parse_qs(urlparse.urlparse(observed_endpoints[0]).query)
        self.assertNotIn("webhooks", query)

    def test_harvest_run_status_poll_uses_short_control_plane_timeout(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="token",
            actor_id="actor",
            default_mode="full",
            timeout_seconds=900,
        )
        observed_timeouts: list[int] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            observed_timeouts.append(int(timeout))
            return {"data": {"id": "run-1", "status": "SUCCEEDED", "defaultDatasetId": "dataset-1"}}

        with (
            patch.dict(os.environ, {"HARVEST_RUN_STATUS_TIMEOUT_SECONDS": ""}),
            patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request),
        ):
            payload = _get_harvest_actor_run(settings, "run-1")

        self.assertEqual(payload["data"]["id"], "run-1")
        self.assertEqual(observed_timeouts, [30])

    def test_harvest_run_status_poll_can_long_poll_wait_for_finish(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="token",
            actor_id="actor",
            default_mode="full",
            timeout_seconds=900,
        )
        observed_queries: list[dict[str, list[str]]] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            observed_queries.append(urlparse.parse_qs(urlparse.urlparse(endpoint).query))
            self.assertEqual(int(timeout), 30)
            return {"data": {"id": "run-long-poll", "status": "SUCCEEDED", "defaultDatasetId": "dataset-long-poll"}}

        with (
            patch.dict(os.environ, {"HARVEST_RUN_STATUS_WAIT_FOR_FINISH_SECONDS": "12"}),
            patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request),
        ):
            payload = _get_harvest_actor_run(settings, "run-long-poll")

        self.assertEqual(payload["data"]["id"], "run-long-poll")
        self.assertEqual(observed_queries[0].get("waitForFinish"), ["12"])

    def test_harvest_run_status_poll_request_context_can_disable_long_poll(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="token",
            actor_id="actor",
            default_mode="full",
            timeout_seconds=900,
        )
        observed_queries: list[dict[str, list[str]]] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            observed_queries.append(urlparse.parse_qs(urlparse.urlparse(endpoint).query))
            return {"data": {"id": "run-no-wait", "status": "RUNNING", "defaultDatasetId": "dataset-no-wait"}}

        with (
            patch.dict(os.environ, {"HARVEST_RUN_STATUS_WAIT_FOR_FINISH_SECONDS": "12"}),
            patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request),
        ):
            payload = _get_harvest_actor_run(
                settings,
                "run-no-wait",
                request_context={"harvest_run_status_wait_for_finish_seconds": 0},
            )

        self.assertEqual(payload["data"]["id"], "run-no-wait")
        self.assertNotIn("waitForFinish", observed_queries[0])

    def test_harvest_run_status_poll_accepts_request_scoped_timeout_override(self) -> None:
        settings = HarvestActorSettings(
            enabled=True,
            api_token="token",
            actor_id="actor",
            default_mode="full",
            timeout_seconds=900,
        )
        observed_timeouts: list[int] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            observed_timeouts.append(int(timeout))
            return {"data": {"id": "run-override", "status": "SUCCEEDED", "defaultDatasetId": "dataset-override"}}

        with patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request):
            payload = _get_harvest_actor_run(
                settings,
                "run-override",
                request_context={"harvest_run_status_timeout_seconds": 18},
            )

        self.assertEqual(payload["data"]["id"], "run-override")
        self.assertEqual(observed_timeouts, [18])

    def test_get_harvest_dataset_items_retries_retryable_page_failure_before_succeeding(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        attempt_counter = {"count": 0}

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            attempt_counter["count"] += 1
            if attempt_counter["count"] < 3:
                raise RuntimeError("Harvest API request failed: IncompleteRead(2048 bytes read)")
            return [{"idx": 1}]

        with patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request):
            items = _get_harvest_dataset_items(
                settings,
                "dataset-retryable",
                logical_name="harvest_profile_scraper_batch",
                run_id="run-retryable",
                request_context={"requested_url_count": 50},
            )

        self.assertEqual(items, [{"idx": 1}])
        self.assertEqual(attempt_counter["count"], 3)

    def test_get_harvest_dataset_items_retries_code_22_queue_backpressure(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        attempt_counter = {"count": 0}

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            attempt_counter["count"] += 1
            if attempt_counter["count"] == 1:
                raise RuntimeError("Harvest API HTTP 400: Too many queued requests (code_22)")
            return [{"idx": 22}]

        with patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request), patch(
            "sourcing_agent.harvest_connectors.time.sleep",
            return_value=None,
        ):
            items = _get_harvest_dataset_items(
                settings,
                "dataset-code-22",
                logical_name="harvest_profile_scraper_batch",
                run_id="run-code-22",
                request_context={"requested_url_count": 73},
            )

        self.assertEqual(items, [{"idx": 22}])
        self.assertEqual(attempt_counter["count"], 2)

    def test_get_harvest_dataset_items_request_scoped_fast_smoke_shortens_retry_backoff(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        attempt_counter = {"count": 0}

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            attempt_counter["count"] += 1
            if attempt_counter["count"] < 3:
                raise RuntimeError("Harvest API request failed: IncompleteRead(2048 bytes read)")
            return [{"idx": 1}]

        with patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request), patch(
            "sourcing_agent.harvest_connectors.time.sleep",
            return_value=None,
        ) as sleep_mock:
            items = _get_harvest_dataset_items(
                settings,
                "dataset-retryable-fast-smoke",
                logical_name="harvest_profile_scraper_batch",
                run_id="run-retryable-fast-smoke",
                request_context={
                    "requested_url_count": 50,
                    "runtime_tuning_profile": "fast_smoke",
                    "harvest_dataset_fetch_max_attempts": 3,
                },
            )

        self.assertEqual(items, [{"idx": 1}])
        self.assertEqual(attempt_counter["count"], 3)
        self.assertEqual(sleep_mock.call_args_list, [call(0.25), call(0.25)])

    def test_get_harvest_dataset_items_request_scoped_timeout_and_attempt_override(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        attempt_counter = {"count": 0}
        observed_timeouts: list[int] = []

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
            attempt_counter["count"] += 1
            observed_timeouts.append(int(timeout))
            raise RuntimeError("Harvest API request failed: IncompleteRead(2048 bytes read)")

        with patch("sourcing_agent.harvest_connectors._harvest_json_request", side_effect=_fake_request):
            with self.assertRaises(HarvestRetryableRequestError):
                _get_harvest_dataset_items(
                    settings,
                    "dataset-retryable-tight",
                    logical_name="harvest_profile_scraper_batch",
                    run_id="run-retryable-tight",
                    request_context={
                        "requested_url_count": 73,
                        "harvest_dataset_page_timeout_seconds": 20,
                        "harvest_dataset_fetch_max_attempts": 1,
                    },
                )

        self.assertEqual(attempt_counter["count"], 1)
        self.assertEqual(observed_timeouts, [20])

    def test_harvest_profile_batch_execute_with_checkpoint_preserves_run_on_retryable_dataset_download_failure(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
        connector = HarvestProfileConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "xai" / "snap-async-retry"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch(
                "sourcing_agent.harvest_connectors._load_cached_harvest_payload",
                return_value=(None, None, None),
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                side_effect=AssertionError("existing run should be reused instead of resubmitted"),
            ), patch(
                "sourcing_agent.harvest_connectors._get_harvest_actor_run",
                return_value={"data": {"id": "run-keep", "defaultDatasetId": "dataset-keep", "status": "SUCCEEDED"}},
            ), patch(
                "sourcing_agent.harvest_connectors._harvest_json_request",
                side_effect=RuntimeError("Harvest API request failed: IncompleteRead(1787319 bytes read)"),
            ):
                result = connector.execute_batch_with_checkpoint(
                    ["https://www.linkedin.com/in/jane-doe/"],
                    snapshot_dir,
                    checkpoint={"run_id": "run-keep", "dataset_id": "dataset-keep", "status": "running"},
                )

        self.assertTrue(result.pending)
        self.assertEqual(result.checkpoint["run_id"], "run-keep")
        self.assertEqual(result.checkpoint["dataset_id"], "dataset-keep")
        self.assertEqual(result.checkpoint["status"], "dataset_download_retryable")
        self.assertEqual(result.checkpoint["dataset_fetch_retry_count"], 1)
        self.assertIn("will be retried", result.message)
        self.assertTrue(any(artifact.label == "dataset_items_retryable_error" for artifact in result.artifacts))

    def test_run_harvest_actor_prefers_async_for_large_requests(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full", max_paid_items=2500)
        payload = {
            "companies": ["https://www.linkedin.com/company/google/"],
            "takePages": 100,
            "maxItems": 2500,
        }
        with patch(
            "sourcing_agent.harvest_connectors._run_harvest_actor_sync_request",
            side_effect=AssertionError("large request should skip sync path"),
        ), patch(
            "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
            return_value={"data": {"id": "run-async", "defaultDatasetId": "dataset-async", "status": "SUCCEEDED"}},
        ), patch(
            "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
            return_value=[{"idx": 1}],
        ) as dataset_mock:
            from sourcing_agent.harvest_connectors import _run_harvest_actor

            body = _run_harvest_actor(settings, payload)

        self.assertEqual(body, [{"idx": 1}])
        self.assertEqual(dataset_mock.call_count, 1)

    def test_run_harvest_actor_falls_back_to_async_when_sync_returns_none(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full", max_paid_items=25)
        payload = {
            "urls": ["https://www.linkedin.com/in/jane-doe/"],
            "profileScraperMode": "Profile details no email ($4 per 1k)",
        }
        with patch(
            "sourcing_agent.harvest_connectors._run_harvest_actor_sync_request",
            return_value=None,
        ), patch(
            "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
            return_value={"data": {"id": "run-fallback", "defaultDatasetId": "dataset-fallback", "status": "SUCCEEDED"}},
        ), patch(
            "sourcing_agent.harvest_connectors._get_harvest_dataset_items",
            return_value=[{"idx": 1}],
        ) as dataset_mock:
            from sourcing_agent.harvest_connectors import _run_harvest_actor

            body = _run_harvest_actor(settings, payload)

        self.assertEqual(body, [{"idx": 1}])
        self.assertEqual(dataset_mock.call_count, 1)

    def test_scripted_sync_harvest_actor_waits_for_remote_wait_terminal_body(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short", max_paid_items=25)
        payload = {
            "profileScraperMode": "Short",
            "maxItems": 25,
            "startPage": 1,
            "takePages": 1,
            "searchQuery": "Agent",
            "currentCompanies": ["https://www.linkedin.com/company/openai/"],
        }
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            scenario_path = Path(tempdir) / "scripted_harvest.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "remote_wait_profile_search",
                                    "match": {
                                        "logical_name": "harvest_profile_search",
                                        "payload_contains": ["currentCompanies", "openai", "Agent"],
                                    },
                                    "execute_sleep_position": "remote_wait",
                                    "scripted_remote_wait_seconds": 0.01,
                                    "body": [
                                        {
                                            "fullName": "OpenAI Agent 1",
                                            "linkedinUrl": "https://www.linkedin.com/in/openai-agent-1/",
                                        }
                                    ],
                                }
                            ]
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with patch.dict(
                "os.environ",
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                    "SOURCING_SCRIPTED_SYNC_HARVEST_MAX_ROUNDS": "1",
                    "SOURCING_SCRIPTED_SYNC_REMOTE_WAIT_TIMEOUT_SECONDS": "2",
                },
                clear=False,
            ):
                from sourcing_agent.harvest_connectors import _run_harvest_actor

                body = _run_harvest_actor(settings, payload)

        self.assertEqual(body, [{"fullName": "OpenAI Agent 1", "linkedinUrl": "https://www.linkedin.com/in/openai-agent-1/"}])

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
                cache_files = list(
                    (
                        Path(tempdir)
                        / "runtime"
                        / "provider_cache"
                        / "local_dev"
                        / "live"
                        / "harvest_company_employees"
                    ).glob("*.json")
                )
        self.assertFalse(result.pending)
        self.assertEqual(result.checkpoint["status"], "completed")
        self.assertEqual(len(result.body), 1)
        self.assertTrue(any(path.name.endswith(".request.json") for path in cache_files))
        self.assertTrue(any(path.name.endswith(".json") and not path.name.endswith(".request.json") for path in cache_files))

    def test_harvest_company_execute_with_checkpoint_can_bypass_shared_cache(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Humans&",
            canonical_name="Humans&",
            company_key="humansand",
            linkedin_slug="humansand",
            linkedin_company_url="https://www.linkedin.com/company/humansand/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "humansand" / "snap-fresh"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            with patch(
                "sourcing_agent.harvest_connectors._load_cached_harvest_payload",
                return_value=([{"cached": True}], "shared_cache", snapshot_dir / "provider_cache.json"),
            ), patch(
                "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                return_value={"data": {"id": "run-fresh-1", "defaultDatasetId": "dataset-fresh-1", "status": "RUNNING"}},
            ) as submit_mock:
                result = connector.execute_with_checkpoint(
                    identity,
                    snapshot_dir,
                    max_pages=1,
                    page_limit=25,
                    allow_shared_provider_cache=False,
                )
        self.assertTrue(result.pending)
        self.assertEqual(result.checkpoint["run_id"], "run-fresh-1")
        self.assertEqual(result.checkpoint["status"], "submitted")
        self.assertEqual(submit_mock.call_count, 1)

    def test_harvest_company_employees_reuses_completed_queue_dataset_without_rerunning_actor(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="short")
        connector = HarvestCompanyEmployeesConnector(settings)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "runtime" / "company_assets" / "anthropic" / "snap-queue"
            harvest_dir = snapshot_dir / "harvest_company_employees"
            harvest_dir.mkdir(parents=True, exist_ok=True)
            dataset_items_path = harvest_dir / "harvest_company_employees_queue_dataset_items.json"
            dataset_items_path.write_text(
                json.dumps(
                    [
                        {
                            "firstName": "Dario",
                            "lastName": "Amodei",
                            "linkedinUrl": "https://www.linkedin.com/in/dario-amodei/",
                            "publicIdentifier": "dario-amodei",
                            "_meta": {"pagination": {"pageNumber": 1}},
                        }
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (harvest_dir / "harvest_company_employees_queue_summary.json").write_text(
                json.dumps(
                    {
                        "status": "completed",
                        "artifact_paths": {
                            "dataset_items": str(dataset_items_path),
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            with patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                side_effect=AssertionError("live actor should not rerun when completed queue dataset exists"),
            ):
                snapshot = connector.fetch_company_roster(identity, snapshot_dir, max_pages=10, page_limit=50)

            manifest = json.loads((harvest_dir / "harvest_company_employees_raw.request.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["request_context"]["cache_status"], "completed_queue_dataset")
            self.assertEqual(len(snapshot.visible_entries), 1)
            self.assertEqual(snapshot.visible_entries[0]["full_name"], "Dario Amodei")

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
                    "source_shard_filters": {"function_ids": ["24"]},
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
        self.assertEqual(candidates[0].metadata.get("function_ids"), [])
        self.assertEqual(evidence[0].metadata.get("profile_url"), "https://www.linkedin.com/in/john-smith/")
