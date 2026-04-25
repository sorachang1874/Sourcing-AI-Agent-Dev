import json
import os
import tempfile
import unittest
from hashlib import sha1
from pathlib import Path
from typing import Any
from unittest.mock import call, patch
from urllib import parse as urlparse

from sourcing_agent.settings import HarvestActorSettings
from sourcing_agent.connectors import (
    CompanyIdentity,
    CompanyRosterSnapshot,
    _canonical_company_key_for_identity,
    build_candidates_from_roster,
)
from sourcing_agent.domain import Candidate
from sourcing_agent.enrichment import _classify_profile_membership, _merge_profile_into_candidate, _names_match, _profile_matches_candidate
from sourcing_agent.harvest_connectors import (
    HarvestCompanyEmployeesConnector,
    HarvestProfileConnector,
    HarvestProfileSearchConnector,
    _get_harvest_dataset_items,
    _apply_harvest_search_filters,
    _load_cached_harvest_payload,
    _persist_shared_harvest_payload,
    _profile_scraper_mode,
    _recommended_harvest_profile_charge_cap_usd,
    _recommended_harvest_profile_timeout_seconds,
    _recommended_harvest_company_timeout_seconds,
    _runtime_dir_from_path,
    parse_harvest_company_employee_run_log,
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
    def test_runtime_dir_from_path_prefers_configured_nested_test_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_runtime = Path(tempdir) / "runtime"
            isolated_runtime = repo_runtime / "test_env" / "scripted_case"
            snapshot_dir = isolated_runtime / "company_assets" / "google" / "snap-1"
            snapshot_dir.mkdir(parents=True, exist_ok=True)

            with patch.dict("os.environ", {"SOURCING_RUNTIME_DIR": str(isolated_runtime)}):
                self.assertEqual(_runtime_dir_from_path(snapshot_dir), isolated_runtime.resolve())

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
""".strip()

        summary = parse_harvest_company_employee_run_log(log_text)

        self.assertEqual(summary["estimated_total_count"], 4845)
        self.assertTrue(summary["provider_result_limited"])
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

        self.assertTrue(first.pending)
        self.assertEqual(first.checkpoint["provider_mode"], "scripted")
        self.assertEqual(first.checkpoint["status"], "submitted")
        self.assertFalse(second.pending)
        self.assertEqual(second.checkpoint["provider_mode"], "scripted")
        self.assertEqual(second.checkpoint["status"], "completed")
        self.assertEqual(len(second.body), 1)
        self.assertEqual(second.body[0]["publicIdentifier"], "jane-doe")

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

        def _fake_request(endpoint: str, *, payload=None, timeout=180):
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
                },
            )

        self.assertEqual(items, [{"idx": 1}])
        self.assertEqual(attempt_counter["count"], 3)
        self.assertEqual(sleep_mock.call_args_list, [call(0.25), call(0.25)])

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
