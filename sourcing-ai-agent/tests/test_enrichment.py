import json
import os
import tempfile
import threading
import time
import unittest
from hashlib import sha1
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from sourcing_agent.agent_runtime import AgentRuntimeCoordinator
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.connectors import CompanyIdentity, LinkedInCompanyRosterConnector, RapidApiAccount
from sourcing_agent.domain import Candidate, EvidenceRecord, JobRequest
from sourcing_agent.durable_runtime import (
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
    legacy_job_operation_id,
    legacy_job_workflow_run_id,
    linkedin_profile_refill_submit_idempotency_key,
    linkedin_profile_url_terminal_record_idempotency_key,
)
from sourcing_agent.enrichment import (
    CompanyPublicationConnector,
    LinkedInProfileDetailConnector,
    LinkedInSearchSlugResolver,
    MultiSourceEnricher,
    PublicationRecord,
    _build_profile_prefetch_batch_envelope,
    _build_profile_prefetch_batch_plan,
    _build_profile_prefetch_queue_items,
    _candidate_key,
    _coalesce_tiny_profile_dispatch_chunks,
    _extract_page_authors,
    _extract_publications_from_rss,
    _extract_publications_from_surface_index,
    _harvest_execution_remote_identifiers,
    _invoke_local_provider_event_callback_serialized,
    _local_provider_event_watcher_lease_is_active,
    _parse_author_text,
    _prioritize_candidates,
    _prioritize_scholar_coauthor_prospects,
    _profile_prefetch_oldest_deferred_coalescing_age_ms,
    _recommended_harvest_profile_prefetch_dispatch_window,
    _record_profile_prefetch_batch_plan_items,
    build_people_search_url,
    extract_search_people_rows,
    parse_basic_linkedin_profile_payload,
)
from sourcing_agent.harvest_connectors import HarvestExecutionArtifact, HarvestExecutionResult, HarvestProfileConnector
from sourcing_agent.linkedin_url_normalization import normalize_linkedin_profile_url_key
from sourcing_agent.runtime_environment import LiveProviderAccessError
from sourcing_agent.settings import HarvestActorSettings
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import pg_durable_runtime_env
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class EnrichmentHelpersTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def _seed_harvest_profile_registry(
        self,
        store: ControlPlaneStore,
        snapshot_dir: Path,
        profile_url: str,
        *,
        full_name: str,
        source_jobs: list[str] | None = None,
    ) -> Path:
        raw_dir = snapshot_dir / "harvest_profiles"
        raw_dir.mkdir(parents=True, exist_ok=True)
        slug = profile_url.rstrip("/").rsplit("/", 1)[-1]
        raw_path = raw_dir / f"{slug}.json"
        raw_path.write_text(
            json.dumps(
                {
                    "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                    "fullName": full_name,
                    "profileUrl": profile_url,
                    "publicIdentifier": slug,
                    "headline": "Research Engineer at Thinking Machines Lab",
                    "currentCompany": "Thinking Machines Lab",
                    "experience": [
                        {"companyName": "Thinking Machines Lab", "title": "Research Engineer", "isCurrent": True}
                    ],
                    "education": [],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        store.repos.linkedin_profile_registry.mark_fetched(
            profile_url,
            raw_path=str(raw_path),
            source_shards=["test_cached_harvest_profile"],
            source_jobs=list(source_jobs or []),
            snapshot_dir=str(snapshot_dir),
        )
        return raw_path

    def test_extract_search_people_rows(self) -> None:
        payload = {
            "status": "SUCCESS",
            "data": {
                "data": [
                    {
                        "urn": "urn:li:fsd_profile:ABC",
                        "fullName": "Neal Bayya",
                        "headline": "Infrastructure @ xAI",
                        "location": "Palo Alto, CA",
                    }
                ]
            },
        }
        rows = extract_search_people_rows(payload)
        self.assertEqual(
            rows,
            [
                {
                    "urn": "urn:li:fsd_profile:ABC",
                    "full_name": "Neal Bayya",
                    "headline": "Infrastructure @ xAI",
                    "location": "Palo Alto, CA",
                }
            ],
        )

    def test_parse_basic_linkedin_profile_payload(self) -> None:
        payload = {
            "data": {
                "username": "nealbayya",
                "firstName": "Neal",
                "lastName": "Bayya",
                "headline": "Infrastructure @ xAI",
                "location": {"locationName": "Palo Alto, California, United States"},
            }
        }
        parsed = parse_basic_linkedin_profile_payload(payload)
        self.assertEqual(parsed["full_name"], "Neal Bayya")
        self.assertEqual(parsed["username"], "nealbayya")
        self.assertEqual(parsed["headline"], "Infrastructure @ xAI")
        self.assertEqual(parsed["profile_url"], "https://www.linkedin.com/in/nealbayya/")

    def test_harvest_execution_remote_identifiers_fall_back_to_run_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            run_get_path = Path(tempdir) / "batch.queue_run_get.json"
            run_get_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "run-from-artifact",
                            "defaultDatasetId": "dataset-from-artifact",
                            "status": "SUCCEEDED",
                        }
                    }
                ),
                encoding="utf-8",
            )

            run_id, dataset_id = _harvest_execution_remote_identifiers(
                {"status": "completed", "cache_source": "shared_cache"},
                {"run_get": str(run_get_path)},
            )

        self.assertEqual(run_id, "run-from-artifact")
        self.assertEqual(dataset_id, "dataset-from-artifact")

    def test_prioritize_scholar_coauthor_prospects_prefers_seed_and_paper_coverage(self) -> None:
        low = Candidate(
            candidate_id="low",
            name_en="Aaron Low",
            display_name="Aaron Low",
            metadata={
                "scholar_coauthor_seed_names": ["Seed A"],
                "scholar_coauthor_papers": [{"title": "Paper A"}],
                "publication_title": "Paper A",
            },
        )
        high = Candidate(
            candidate_id="high",
            name_en="Zed High",
            display_name="Zed High",
            metadata={
                "scholar_coauthor_seed_names": ["Seed A", "Seed B", "Seed C"],
                "scholar_coauthor_papers": [{"title": "Paper A"}, {"title": "Paper B"}, {"title": "Paper C"}],
                "publication_title": "Paper A",
            },
        )
        ordered = _prioritize_scholar_coauthor_prospects([low, high])
        self.assertEqual([item.candidate_id for item in ordered], ["high", "low"])

    def test_build_people_search_url(self) -> None:
        account = RapidApiAccount(
            account_id="account_014",
            source="test",
            provider="zscraper",
            host="z-real-time-linkedin-scraper-api1.p.rapidapi.com",
            base_url="https://z-real-time-linkedin-scraper-api1.p.rapidapi.com",
            api_key="test",
        )
        url = build_people_search_url(account, "Neal Bayya xAI", limit=5)
        self.assertIn("/api/search/people?", url)
        self.assertIn("keywords=Neal+Bayya+xAI", url)
        self.assertIn("limit=5", url)

    def test_rapidapi_slug_search_rejects_isolated_runtime_without_confirm(self) -> None:
        account = RapidApiAccount(
            account_id="account_search",
            source="test",
            provider="zscraper",
            host="z-real-time-linkedin-scraper-api1.p.rapidapi.com",
            base_url="https://z-real-time-linkedin-scraper-api1.p.rapidapi.com",
            api_key="test",
            endpoint_search="/api/search/people",
        )
        with tempfile.TemporaryDirectory() as tempdir, mock.patch.dict(
            os.environ,
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                "SOURCING_RUNTIME_ENVIRONMENT": "test",
                "SOURCING_RUNTIME_DIR": str(Path(tempdir) / "runtime" / "test_env" / "scripted_case"),
            },
            clear=True,
        ), mock.patch("sourcing_agent.enrichment.request.urlopen") as urlopen_mock:
            resolver = LinkedInSearchSlugResolver([account])
            with self.assertRaises(LiveProviderAccessError):
                resolver._search_people("OpenAI Agent employees")

        urlopen_mock.assert_not_called()

    def test_rapidapi_company_roster_rejects_isolated_runtime_without_confirm(self) -> None:
        account = RapidApiAccount(
            account_id="account_company",
            source="test",
            provider="zscraper",
            host="linkedin-company-data.p.rapidapi.com",
            base_url="https://linkedin-company-data.p.rapidapi.com",
            api_key="test",
            endpoint_company="/api/company/people",
        )
        with tempfile.TemporaryDirectory() as tempdir, mock.patch.dict(
            os.environ,
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                "SOURCING_RUNTIME_ENVIRONMENT": "test",
                "SOURCING_RUNTIME_DIR": str(Path(tempdir) / "runtime" / "test_env" / "scripted_case"),
            },
            clear=True,
        ), mock.patch("sourcing_agent.connectors.request.urlopen") as urlopen_mock:
            connector = LinkedInCompanyRosterConnector([account])
            with self.assertRaises(LiveProviderAccessError):
                connector._fetch_page("openai", 1, 25)

        urlopen_mock.assert_not_called()

    def test_rapidapi_profile_detail_rejects_isolated_runtime_without_confirm(self) -> None:
        account = RapidApiAccount(
            account_id="account_profile",
            source="test",
            provider="zscraper",
            host="linkedin-profile-data.p.rapidapi.com",
            base_url="https://linkedin-profile-data.p.rapidapi.com",
            api_key="test",
            endpoint_profile="/api/people/profile",
        )
        with tempfile.TemporaryDirectory() as tempdir, mock.patch.dict(
            os.environ,
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                "SOURCING_RUNTIME_ENVIRONMENT": "test",
                "SOURCING_RUNTIME_DIR": str(Path(tempdir) / "runtime" / "test_env" / "scripted_case"),
            },
            clear=True,
        ), mock.patch("sourcing_agent.enrichment.request.urlopen") as urlopen_mock:
            snapshot_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_case" / "company_assets" / "openai" / "snap-1"
            connector = LinkedInProfileDetailConnector([account])
            with self.assertRaises(LiveProviderAccessError):
                connector.fetch_profile("openai-agent-current-0189", snapshot_dir)

        urlopen_mock.assert_not_called()

    def test_rapidapi_profile_detail_is_cache_only_in_non_live_provider_mode(self) -> None:
        account = RapidApiAccount(
            account_id="account_profile",
            source="test",
            provider="zscraper",
            host="linkedin-profile-data.p.rapidapi.com",
            base_url="https://linkedin-profile-data.p.rapidapi.com",
            api_key="test",
            endpoint_profile="/api/people/profile",
        )
        with tempfile.TemporaryDirectory() as tempdir, mock.patch.dict(
            os.environ,
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                "SOURCING_RUNTIME_ENVIRONMENT": "scripted",
                "SOURCING_RUNTIME_DIR": str(Path(tempdir) / "runtime" / "test_env" / "scripted_case"),
                "SOURCING_LIVE_PROVIDER_ACCESS_DISABLED": "1",
            },
            clear=True,
        ), mock.patch("sourcing_agent.enrichment.request.urlopen") as urlopen_mock:
            snapshot_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_case" / "company_assets" / "openai" / "snap-1"
            connector = LinkedInProfileDetailConnector([account])
            self.assertIsNone(connector.fetch_profile("openai-agent-current-0189", snapshot_dir))

        urlopen_mock.assert_not_called()

    def test_load_cached_search_payload_from_sibling_snapshot(self) -> None:
        resolver = LinkedInSearchSlugResolver([])
        with tempfile.TemporaryDirectory() as tempdir:
            company_dir = Path(tempdir)
            old_snapshot = company_dir / "20260405T214403" / "slug_search"
            current_snapshot = company_dir / "20260405T220122"
            old_snapshot.mkdir(parents=True, exist_ok=True)
            current_snapshot.mkdir(parents=True, exist_ok=True)
            cached_path = old_snapshot / "candidate_q01.json"
            cached_path.write_text(json.dumps({"data": {"data": [{"fullName": "Jake Palmer"}]}}))

            payload, source_path = resolver._load_cached_search_payload(
                company_dir=company_dir,
                snapshot_dir=current_snapshot,
                candidate_id="candidate",
                query_index=1,
            )
            self.assertIsNotNone(payload)
            self.assertEqual(payload["data"]["data"][0]["fullName"], "Jake Palmer")
            self.assertEqual(source_path, cached_path)

    def test_prioritize_candidates_elevates_technical_leadership(self) -> None:
        candidates = [
            Candidate(candidate_id="ops", name_en="Ops Lead", display_name="Ops Lead", category="employee", employment_status="current", role="Member of Operations Staff at Thinking Machines Lab"),
            Candidate(candidate_id="cto", name_en="Soumith Chintala", display_name="Soumith Chintala", category="employee", employment_status="current", role="Chief Technology Officer at Thinking Machines Lab"),
            Candidate(candidate_id="mts", name_en="Andy Hwang", display_name="Andy Hwang", category="employee", employment_status="current", role="Member of Technical Staff at Thinking Machines Lab"),
            Candidate(candidate_id="founder", name_en="Lilian Weng", display_name="Lilian Weng", category="employee", employment_status="current", role="Co-Founder at Thinking Machines Lab"),
        ]

        prioritized = _prioritize_candidates(candidates)
        leading = [item.display_name for item in prioritized[:3]]
        self.assertIn("Soumith Chintala", leading)
        self.assertIn("Andy Hwang", leading)
        self.assertIn("Lilian Weng", leading)
        self.assertEqual(prioritized[-1].display_name, "Ops Lead")

    def test_parse_author_text_strips_collaboration_suffix(self) -> None:
        self.assertEqual(
            _parse_author_text("Kevin Lu in collaboration with others at Thinking Machines"),
            ["Kevin Lu"],
        )
        self.assertEqual(_parse_author_text("Thinking Machines Lab"), [])

    def test_extract_publications_from_surface_index_reads_title_and_author(self) -> None:
        html = """
        <li>
          <a class="post-item-link" href="/blog/on-policy-distillation/">
            <div class="post-title">On-Policy Distillation</div>
            <div class="author-date">Kevin Lu in collaboration with others at Thinking Machines</div>
          </a>
        </li>
        """
        records = _extract_publications_from_surface_index(
            html,
            "https://thinkingmachines.ai/blog/",
            "/tmp/blog.html",
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].title, "On-Policy Distillation")
        self.assertEqual(records[0].url, "https://thinkingmachines.ai/blog/on-policy-distillation/")
        self.assertEqual(records[0].authors, ["Kevin Lu"])

    def test_extract_publications_from_rss_and_page_authors(self) -> None:
        rss = """<?xml version="1.0" encoding="utf-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>LoRA Without Regret</title>
              <link>https://thinkingmachines.ai/blog/lora/</link>
              <pubDate>Mon, 29 Sep 2025 00:00:00 +0000</pubDate>
            </item>
          </channel>
        </rss>
        """
        records = _extract_publications_from_rss(
            rss,
            "https://thinkingmachines.ai/blog/index.xml",
            "/tmp/blog.xml",
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].title, "LoRA Without Regret")
        self.assertEqual(records[0].year, 2025)
        self.assertEqual(records[0].authors, [])

        article_html = """
        <html><head><meta name="author" content="Thinking Machines Lab"></head>
        <body><span class="author"><a href="https://example.com">John Schulman</a> in collaboration with others at Thinking Machines</span></body></html>
        """
        self.assertEqual(_extract_page_authors(article_html), ["John Schulman"])

    def test_publication_connector_skips_remote_collection_in_simulate_mode_and_fast_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            connector = CompanyPublicationConnector(catalog)
            identity = CompanyIdentity(
                requested_name="Google",
                canonical_name="Google",
                company_key="google",
                linkedin_slug="google",
                linkedin_company_url="https://www.linkedin.com/company/google/",
                domain="google.com",
            )
            publications_dir = root / "publications"
            logger = AssetLogger(root)
            plan_payload = {
                "publication_coverage": {
                    "source_families": [
                        {"family": "official_research"},
                        {"family": "publication_platforms"},
                    ]
                }
            }
            with (
                mock.patch.object(
                    connector,
                    "_collect_official_surface_publications",
                    side_effect=AssertionError("official surface fetch should be skipped"),
                ),
                mock.patch.object(
                    connector,
                    "_search_arxiv_publications",
                    side_effect=AssertionError("arxiv fetch should be skipped"),
                ),
            ):
                with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}, clear=False):
                    self.assertEqual(
                        connector._collect_publications(
                            identity,
                            publications_dir,
                            5,
                            asset_logger=logger,
                            request_payload={},
                            plan_payload=plan_payload,
                        ),
                        [],
                    )
                self.assertEqual(
                    connector._collect_publications(
                        identity,
                        publications_dir,
                        5,
                        asset_logger=logger,
                        request_payload={"execution_preferences": {"runtime_tuning_profile": "fast_smoke"}},
                        plan_payload=plan_payload,
                    ),
                    [],
                )

    def test_publication_connector_adds_coauthor_evidence_without_creating_new_leads(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            connector = CompanyPublicationConnector(catalog)
            connector._collect_publications = lambda *args, **kwargs: [
                PublicationRecord(
                    publication_id="pub_001",
                    source="company_blog",
                    source_dataset="thinkingmachineslab_publications",
                    source_path=str(root / "publications.json"),
                    title="Scaling Laws in Practice",
                    url="https://thinkingmachines.ai/blog/scaling-laws/",
                    year=2026,
                    authors=["Alice Zhang", "Bob Li"],
                    acknowledgement_names=[],
                )
            ]
            connector._search_roster_anchored_scholar_publications = lambda *args, **kwargs: []
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            candidates = [
                Candidate(
                    candidate_id="alice",
                    name_en="Alice Zhang",
                    display_name="Alice Zhang",
                    category="employee",
                    target_company="Thinking Machines Lab",
                    organization="Thinking Machines Lab",
                ),
                Candidate(
                    candidate_id="bob",
                    name_en="Bob Li",
                    display_name="Bob Li",
                    category="employee",
                    target_company="Thinking Machines Lab",
                    organization="Thinking Machines Lab",
                ),
            ]

            result = connector.enrich(
                identity,
                root,
                candidates,
                asset_logger=AssetLogger(root),
                max_publications=5,
                max_leads=5,
            )

            self.assertEqual(result["lead_candidates"], [])
            self.assertEqual(result["coauthor_edges"], [{"source": "Alice Zhang", "target": "Bob Li"}])
            coauthor_evidence = [item for item in result["evidence"] if item.source_type == "publication_coauthor"]
            self.assertEqual(len(coauthor_evidence), 2)
            self.assertEqual({item.candidate_id for item in coauthor_evidence}, {"alice", "bob"})
            self.assertEqual(
                {tuple(item.metadata["coauthors"]) for item in coauthor_evidence},
                {("Alice Zhang",), ("Bob Li",)},
            )
            self.assertTrue((root / "publications" / "coauthor_graph.json").exists())

    def test_publication_connector_collects_scholar_coauthor_prospects(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            connector = CompanyPublicationConnector(catalog)
            connector._collect_publications = lambda *args, **kwargs: []
            connector._search_roster_anchored_scholar_publications = lambda seed_candidate, *args, **kwargs: (
                [
                    PublicationRecord(
                        publication_id="paper_001",
                        source="arxiv_author_seed_search",
                        source_dataset="thinkingmachineslab_roster_anchored_scholar",
                        source_path=str(root / "papers.xml"),
                        title="Scaling Laws in Practice",
                        url="https://arxiv.org/abs/1234.5678",
                        year=2026,
                        authors=["Alice Zhang", "Bob Li", "Carol Wu"],
                        acknowledgement_names=[],
                    )
                ]
                if seed_candidate.candidate_id == "alice"
                else []
            )
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            candidates = [
                Candidate(
                    candidate_id="alice",
                    name_en="Alice Zhang",
                    display_name="Alice Zhang",
                    category="employee",
                    target_company="Thinking Machines Lab",
                    organization="Thinking Machines Lab",
                ),
                Candidate(
                    candidate_id="bob",
                    name_en="Bob Li",
                    display_name="Bob Li",
                    category="employee",
                    target_company="Thinking Machines Lab",
                    organization="Thinking Machines Lab",
                ),
            ]

            result = connector.enrich(
                identity,
                root,
                candidates,
                asset_logger=AssetLogger(root),
                max_publications=5,
                max_leads=5,
            )

            scholar_evidence = [item for item in result["evidence"] if item.source_type == "scholar_coauthor"]
            self.assertEqual(len(scholar_evidence), 1)
            self.assertEqual(scholar_evidence[0].candidate_id, "bob")
            self.assertEqual(len(result["scholar_coauthor_prospects"]), 1)
            self.assertEqual(result["scholar_coauthor_prospects"][0].display_name, "Carol Wu")
            self.assertEqual(
                result["scholar_coauthor_prospects"][0].metadata["lead_discovery_method"],
                "roster_anchored_scholar_coauthor_expansion",
            )
            graph_path = root / "publications" / "roster_anchored_scholar_coauthors" / "scholar_coauthor_graph.json"
            seed_roster_path = root / "publications" / "roster_anchored_scholar_coauthors" / "seed_roster.json"
            seed_publications_path = root / "publications" / "roster_anchored_scholar_coauthors" / "seed_publications.json"
            self.assertTrue(graph_path.exists())
            self.assertTrue(seed_roster_path.exists())
            self.assertTrue(seed_publications_path.exists())
            self.assertTrue((root / "publications" / "roster_anchored_scholar_coauthors" / "scholar_coauthor_prospects.json").exists())
            graph_payload = json.loads(graph_path.read_text())
            self.assertEqual(graph_payload[0]["paper_count"], 1)
            self.assertEqual(graph_payload[0]["papers"][0]["publication_id"], "paper_001")

    def test_publication_lead_targeted_harvest_resolution_upgrades_candidate(self) -> None:
        class _FakeSearchSettings:
            enabled = True
            max_paid_items = 10

        class _FakeSearchConnector:
            settings = _FakeSearchSettings()

            def search_profiles(self, **kwargs):
                discovery_dir = kwargs["discovery_dir"]
                raw_path = discovery_dir / "fake_search.json"
                raw_path.write_text("[]")
                return {
                    "raw_path": raw_path,
                    "rows": [
                        {
                            "full_name": "Kevin Lu",
                            "headline": "Research Engineer at Thinking Machines Lab",
                            "profile_url": "https://www.linkedin.com/in/kevin-lu/",
                            "username": "kevin-lu",
                            "current_company": "Thinking Machines Lab",
                        }
                    ],
                }

        class _FakeProfileConnector:
            def __init__(self) -> None:
                self.batch_calls = []

            def fetch_profiles_by_urls(
                self,
                profile_urls,
                snapshot_dir,
                asset_logger=None,
                use_cache=True,
                allow_shared_provider_cache=True,
            ):
                self.batch_calls.append(list(profile_urls))
                raw_path = snapshot_dir / "harvest_profiles" / "kevin-lu.json"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text("{}")
                return {
                    profile_url: {
                        "raw_path": raw_path,
                        "account_id": "harvest_profile_scraper",
                        "parsed": {
                            "full_name": "Kevin Lu",
                            "headline": "Research Engineer at Thinking Machines Lab",
                            "profile_url": profile_url,
                            "public_identifier": "kevin-lu",
                            "summary": "Works on post-training systems.",
                            "location": "San Francisco Bay Area",
                            "current_company": "Thinking Machines Lab",
                            "experience": [{"company": "Thinking Machines Lab", "title": "Research Engineer", "is_current": True}],
                            "education": [],
                            "publications": [],
                            "more_profiles": [],
                        },
                    }
                    for profile_url in profile_urls
                }

            def fetch_profile_by_url(self, profile_url, snapshot_dir, asset_logger=None):
                raise AssertionError("publication lead targeted harvest should batch known profile URLs")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            fake_profile_connector = _FakeProfileConnector()
            store = self.make_pg_store(root / "control_plane.db")
            self._seed_harvest_profile_registry(
                store,
                root,
                "https://www.linkedin.com/in/kevin-lu/",
                full_name="Kevin Lu",
                source_jobs=["job_publication_lead"],
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=fake_profile_connector,
                harvest_profile_search_connector=_FakeSearchConnector(),
                store=store,
            )
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            lead = Candidate(
                candidate_id="lead1",
                name_en="Kevin Lu",
                display_name="Kevin Lu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                source_dataset="publication_lead",
                source_path=str(root / "publication_leads.json"),
            )
            candidate_map = {"kevinlu": lead}
            resolved_profiles = []
            unresolved = [
                {
                    "candidate_id": "lead1",
                    "display_name": "Kevin Lu",
                    "attempted_slugs": [],
                    "query_summaries": [],
                }
            ]
            evidence = []
            used_budget, summary_path = enricher._resolve_publication_leads_with_harvest_search(
                lead_candidates=[lead],
                identity=identity,
                snapshot_dir=root,
                remaining_profile_budget=2,
                candidate_map=candidate_map,
                resolved_profiles=resolved_profiles,
                unresolved_candidates=unresolved,
                evidence=evidence,
                asset_logger=AssetLogger(root),
                source_job_id="job_publication_lead",
            )
            self.assertEqual(used_budget, 1)
            self.assertTrue(summary_path.exists())
            self.assertEqual(candidate_map["kevinlu"].category, "employee")
            self.assertEqual(candidate_map["kevinlu"].employment_status, "current")
            self.assertEqual(len(resolved_profiles), 1)
            self.assertEqual(unresolved, [])
            self.assertEqual(len(evidence), 1)
            self.assertEqual(fake_profile_connector.batch_calls, [])

    def test_publication_lead_targeted_harvest_batches_current_phase_urls_across_candidates(self) -> None:
        class _FakeSearchSettings:
            enabled = True
            max_paid_items = 10

        class _FakeSearchConnector:
            settings = _FakeSearchSettings()

            def search_profiles(self, **kwargs):
                query_text = kwargs["query_text"]
                discovery_dir = kwargs["discovery_dir"]
                raw_path = discovery_dir / f"{query_text.replace(' ', '_')}.json"
                raw_path.write_text("[]")
                profile_url = {
                    "Kevin Lu": "https://www.linkedin.com/in/kevin-lu/",
                    "Alice Wu": "https://www.linkedin.com/in/alice-wu/",
                }[query_text]
                return {
                    "raw_path": raw_path,
                    "rows": [
                        {
                            "full_name": query_text,
                            "headline": "Research Engineer at Thinking Machines Lab",
                            "profile_url": profile_url,
                            "username": profile_url.rstrip("/").split("/")[-1],
                            "current_company": "Thinking Machines Lab",
                        }
                    ],
                }

        class _FakeProfileConnector:
            def __init__(self) -> None:
                self.batch_calls = []

            def fetch_profiles_by_urls(
                self,
                profile_urls,
                snapshot_dir,
                asset_logger=None,
                use_cache=True,
                allow_shared_provider_cache=True,
            ):
                self.batch_calls.append(list(profile_urls))
                payloads = {}
                for profile_url in profile_urls:
                    slug = profile_url.rstrip("/").split("/")[-1]
                    raw_path = snapshot_dir / "harvest_profiles" / f"{slug}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text("{}")
                    full_name = "Kevin Lu" if slug == "kevin-lu" else "Alice Wu"
                    payloads[profile_url] = {
                        "raw_path": raw_path,
                        "account_id": "harvest_profile_scraper",
                        "parsed": {
                            "full_name": full_name,
                            "headline": "Research Engineer at Thinking Machines Lab",
                            "profile_url": profile_url,
                            "public_identifier": slug,
                            "summary": "Works on training systems.",
                            "location": "San Francisco Bay Area",
                            "current_company": "Thinking Machines Lab",
                            "experience": [{"company": "Thinking Machines Lab", "title": "Research Engineer", "is_current": True}],
                            "education": [],
                            "publications": [],
                            "more_profiles": [],
                        },
                    }
                return payloads

            def fetch_profile_by_url(self, profile_url, snapshot_dir, asset_logger=None):
                raise AssertionError("publication lead targeted harvest should stay on batch profile fetching")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            fake_profile_connector = _FakeProfileConnector()
            store = self.make_pg_store(root / "control_plane.db")
            self._seed_harvest_profile_registry(
                store,
                root,
                "https://www.linkedin.com/in/kevin-lu/",
                full_name="Kevin Lu",
                source_jobs=["job_publication_lead_batch"],
            )
            self._seed_harvest_profile_registry(
                store,
                root,
                "https://www.linkedin.com/in/alice-wu/",
                full_name="Alice Wu",
                source_jobs=["job_publication_lead_batch"],
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=fake_profile_connector,
                harvest_profile_search_connector=_FakeSearchConnector(),
                store=store,
            )
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            lead_one = Candidate(
                candidate_id="lead1",
                name_en="Kevin Lu",
                display_name="Kevin Lu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
            )
            lead_two = Candidate(
                candidate_id="lead2",
                name_en="Alice Wu",
                display_name="Alice Wu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
            )
            candidate_map = {"kevinlu": lead_one, "alicewu": lead_two}
            resolved_profiles = []
            unresolved = []
            evidence = []

            used_budget, summary_path = enricher._resolve_publication_leads_with_harvest_search(
                lead_candidates=[lead_one, lead_two],
                identity=identity,
                snapshot_dir=root,
                remaining_profile_budget=4,
                candidate_map=candidate_map,
                resolved_profiles=resolved_profiles,
                unresolved_candidates=unresolved,
                evidence=evidence,
                asset_logger=AssetLogger(root),
                source_job_id="job_publication_lead_batch",
            )

            self.assertEqual(used_budget, 2)
            self.assertTrue(summary_path.exists())
            self.assertEqual(len(resolved_profiles), 2)
            self.assertEqual(len(evidence), 2)
            self.assertEqual(fake_profile_connector.batch_calls, [])

    def test_resolve_candidate_with_known_refs_batches_known_profile_urls(self) -> None:
        class _BatchOnlyHarvestProfileConnector:
            def __init__(self) -> None:
                self.batch_calls = []

            def fetch_profiles_by_urls(
                self,
                profile_urls,
                snapshot_dir,
                asset_logger=None,
                use_cache=True,
                allow_shared_provider_cache=True,
            ):
                self.batch_calls.append(list(profile_urls))
                results = {}
                for profile_url in profile_urls:
                    slug = profile_url.rstrip("/").split("/")[-1]
                    raw_path = snapshot_dir / "harvest_profiles" / f"{slug}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text("{}")
                    full_name = "Wrong Person" if slug == "mismatch" else "Kevin Lu"
                    results[profile_url] = {
                        "raw_path": raw_path,
                        "account_id": "harvest_profile_scraper",
                        "parsed": {
                            "full_name": full_name,
                            "headline": "Research Engineer at Thinking Machines Lab",
                            "profile_url": profile_url,
                            "public_identifier": slug,
                            "summary": "Works on training systems.",
                            "location": "San Francisco Bay Area",
                            "current_company": "Thinking Machines Lab",
                            "experience": [{"company": "Thinking Machines Lab", "title": "Research Engineer", "is_current": True}],
                            "education": [],
                            "publications": [],
                            "more_profiles": [],
                        },
                    }
                return results

            def fetch_profile_by_url(self, profile_url, snapshot_dir, asset_logger=None):
                raise AssertionError("known profile URL resolution should not fall back to single Harvest fetches")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            harvest_profile_connector = _BatchOnlyHarvestProfileConnector()
            store = self.make_pg_store(root / "control_plane.db")
            self._seed_harvest_profile_registry(
                store,
                root,
                "https://www.linkedin.com/in/kevin-lu/",
                full_name="Kevin Lu",
                source_jobs=["job_known_refs"],
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=harvest_profile_connector,
                store=store,
            )
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            candidate = Candidate(
                candidate_id="lead1",
                name_en="Kevin Lu",
                display_name="Kevin Lu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                linkedin_url="https://www.linkedin.com/in/mismatch/",
                metadata={
                    "profile_url": "https://www.linkedin.com/in/kevin-lu/",
                },
            )
            candidate_map = {"kevinlu": candidate}
            resolved_profiles = []
            evidence = []

            resolved, fetch_count = enricher._resolve_candidate_with_known_refs(
                candidate,
                identity,
                root,
                0,
                5,
                candidate_map,
                resolved_profiles,
                evidence,
                asset_logger=AssetLogger(root),
                source_job_id="job_known_refs",
            )

            self.assertTrue(resolved)
            self.assertEqual(fetch_count, 1)
            self.assertEqual(harvest_profile_connector.batch_calls, [])
            self.assertEqual(candidate_map["kevinlu"].category, "employee")
            self.assertEqual(len(resolved_profiles), 1)
            self.assertEqual(len(evidence), 1)

    def test_fetch_harvest_profiles_for_urls_does_not_use_direct_live_batches(self) -> None:
        class _FakeProfileConnector:
            def __init__(self) -> None:
                self.batch_sizes: list[int] = []

            def fetch_profiles_by_urls(
                self,
                profile_urls,
                snapshot_dir,
                asset_logger=None,
                use_cache=True,
                allow_shared_provider_cache=True,
            ):
                self.batch_sizes.append(len(profile_urls))
                payloads = {}
                for profile_url in profile_urls:
                    slug = profile_url.rstrip("/").split("/")[-1]
                    raw_path = snapshot_dir / "harvest_profiles" / f"{slug}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text("{}")
                    payloads[profile_url] = {
                        "raw_path": raw_path,
                        "account_id": "harvest_profile_scraper",
                        "parsed": {
                            "full_name": slug,
                            "profile_url": profile_url,
                        },
                    }
                return payloads

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            fake_profile_connector = _FakeProfileConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=fake_profile_connector,
            )
            profile_urls = [f"https://www.linkedin.com/in/micro-batch-{idx}/" for idx in range(205)]
            fetched = enricher._fetch_harvest_profiles_for_urls(
                profile_urls,
                root,
                asset_logger=AssetLogger(root),
            )

            self.assertEqual(fetched, {})
            self.assertEqual(fake_profile_connector.batch_sizes, [])

    def test_fetch_harvest_profiles_for_urls_does_not_use_bounded_direct_live_batches(self) -> None:
        class _FakeProfileConnector:
            def __init__(self) -> None:
                self.batch_sizes: list[int] = []
                self._lock = threading.Lock()
                self.inflight = 0
                self.max_inflight = 0

            def fetch_profiles_by_urls(
                self,
                profile_urls,
                snapshot_dir,
                asset_logger=None,
                use_cache=True,
                allow_shared_provider_cache=True,
            ):
                with self._lock:
                    self.batch_sizes.append(len(profile_urls))
                    self.inflight += 1
                    self.max_inflight = max(self.max_inflight, self.inflight)
                time.sleep(0.05)
                payloads = {}
                for profile_url in profile_urls:
                    slug = profile_url.rstrip("/").split("/")[-1]
                    raw_path = snapshot_dir / "harvest_profiles" / f"{slug}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text("{}")
                    payloads[profile_url] = {
                        "raw_path": raw_path,
                        "account_id": "harvest_profile_scraper",
                        "parsed": {
                            "full_name": slug,
                            "profile_url": profile_url,
                        },
                    }
                with self._lock:
                    self.inflight -= 1
                return payloads

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            fake_profile_connector = _FakeProfileConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=fake_profile_connector,
            )
            profile_urls = [f"https://www.linkedin.com/in/parallel-live-{idx}/" for idx in range(205)]
            with mock.patch("sourcing_agent.enrichment._external_provider_mode", return_value="live"):
                fetched = enricher._fetch_harvest_profiles_for_urls(
                    profile_urls,
                    root,
                    asset_logger=AssetLogger(root),
                )

            self.assertEqual(fetched, {})
            self.assertEqual(fake_profile_connector.batch_sizes, [])
            self.assertEqual(fake_profile_connector.max_inflight, 0)

    def test_fetch_harvest_profiles_for_urls_does_not_open_direct_global_inflight_slot(self) -> None:
        class _FakeProfileConnector:
            def __init__(self) -> None:
                self._lock = threading.Lock()
                self.inflight = 0
                self.max_inflight = 0

            def fetch_profiles_by_urls(
                self,
                profile_urls,
                snapshot_dir,
                asset_logger=None,
                use_cache=True,
                allow_shared_provider_cache=True,
            ):
                with self._lock:
                    self.inflight += 1
                    self.max_inflight = max(self.max_inflight, self.inflight)
                time.sleep(0.02)
                payloads = {}
                for profile_url in profile_urls:
                    slug = profile_url.rstrip("/").split("/")[-1]
                    raw_path = snapshot_dir / "harvest_profiles" / f"{slug}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text("{}")
                    payloads[profile_url] = {
                        "raw_path": raw_path,
                        "parsed": {"full_name": slug, "profile_url": profile_url},
                    }
                with self._lock:
                    self.inflight -= 1
                return payloads

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            fake_profile_connector = _FakeProfileConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=fake_profile_connector,
            )
            profile_urls = [f"https://www.linkedin.com/in/global-budget-{idx}/" for idx in range(160)]
            with mock.patch("sourcing_agent.enrichment._external_provider_mode", return_value="live"), mock.patch.dict(
                "os.environ",
                {"SOURCING_HARVEST_PROFILE_SCRAPE_GLOBAL_INFLIGHT": "1"},
                clear=False,
            ):
                fetched = enricher._fetch_harvest_profiles_for_urls(
                    profile_urls,
                    root,
                    asset_logger=AssetLogger(root),
                )

            self.assertEqual(fetched, {})
            self.assertEqual(fake_profile_connector.max_inflight, 0)

    def test_fetch_harvest_profiles_for_urls_does_not_direct_fetch_roster_heavy_batches(self) -> None:
        class _FakeProfileConnector:
            def __init__(self) -> None:
                self.batch_sizes: list[int] = []
                self._lock = threading.Lock()
                self.inflight = 0
                self.max_inflight = 0

            def fetch_profiles_by_urls(
                self,
                profile_urls,
                snapshot_dir,
                asset_logger=None,
                use_cache=True,
                allow_shared_provider_cache=True,
            ):
                with self._lock:
                    self.batch_sizes.append(len(profile_urls))
                    self.inflight += 1
                    self.max_inflight = max(self.max_inflight, self.inflight)
                time.sleep(0.05)
                payloads = {}
                for profile_url in profile_urls:
                    slug = profile_url.rstrip("/").split("/")[-1]
                    raw_path = snapshot_dir / "harvest_profiles" / f"{slug}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text("{}")
                    payloads[profile_url] = {
                        "raw_path": raw_path,
                        "account_id": "harvest_profile_scraper",
                        "parsed": {
                            "full_name": slug,
                            "profile_url": profile_url,
                        },
                    }
                with self._lock:
                    self.inflight -= 1
                return payloads

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            fake_profile_connector = _FakeProfileConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=fake_profile_connector,
            )
            profile_urls = [f"https://www.linkedin.com/in/roster-heavy-{idx}/" for idx in range(240)]
            source_shards_by_url = {
                profile_url: ["harvest_company_employees_visible"]
                for profile_url in profile_urls
            }
            with mock.patch("sourcing_agent.enrichment._external_provider_mode", return_value="live"):
                fetched = enricher._fetch_harvest_profiles_for_urls(
                    profile_urls,
                    root,
                    asset_logger=AssetLogger(root),
                    source_shards_by_url=source_shards_by_url,
                )

            self.assertEqual(fetched, {})
            self.assertEqual(fake_profile_connector.batch_sizes, [])
            self.assertEqual(fake_profile_connector.max_inflight, 0)

    def test_fetch_harvest_profiles_for_urls_does_not_head_of_line_block_on_contended_registry_urls(self) -> None:
        class _FakeProfileConnector:
            def __init__(self, events: list[str]) -> None:
                self.events = events

            def fetch_profiles_by_urls(
                self,
                profile_urls,
                snapshot_dir,
                asset_logger=None,
                use_cache=True,
                allow_shared_provider_cache=True,
            ):
                self.events.append("live_fetch")
                payloads = {}
                for profile_url in profile_urls:
                    slug = profile_url.rstrip("/").split("/")[-1]
                    raw_path = snapshot_dir / "harvest_profiles" / f"{slug}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text("{}", encoding="utf-8")
                    payloads[profile_url] = {
                        "raw_path": raw_path,
                        "account_id": "harvest_profile_scraper",
                        "parsed": {
                            "full_name": slug,
                            "profile_url": profile_url,
                        },
                    }
                return payloads

        class _TrackingStore:
            def __init__(self, queued_url: str, events: list[str]) -> None:
                self.queued_url = normalize_linkedin_profile_url_key(queued_url)
                self.events = events
                self.lookup_count = 0
                self.repos = SimpleNamespace(linkedin_profile_registry=self)

            def get_bulk(self, profile_urls):
                return {
                    self.queued_url: {"status": "queued"},
                }

            def get(self, profile_url):
                if normalize_linkedin_profile_url_key(profile_url) == self.queued_url:
                    self.lookup_count += 1
                    self.events.append(f"queued_lookup_{self.lookup_count}")
                    return {"status": "queued"}
                return {}

            def list_agent_workers(self, job_id="", lane_id="", limit=0):
                return []

            def acquire_lease(self, profile_url, lease_owner="", lease_seconds=0):
                return {"acquired": True, "lease_owner": lease_owner, "lease_token": "token"}

            def release_lease(self, profile_url, lease_owner="", lease_token=""):
                return {"released": True}

            def record_event(self, profile_url, **kwargs):
                self.events.append(str(kwargs.get("event_type") or "registry_event"))
                return {"profile_url": profile_url, **kwargs}

            def mark_queued(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def mark_fetched(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def mark_failed(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def upsert_sources(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            events: list[str] = []
            queued_url = "https://www.linkedin.com/in/queued-profile/"
            pending_url = "https://www.linkedin.com/in/pending-profile/"
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_FakeProfileConnector(events),
                store=_TrackingStore(queued_url, events),
            )
            monotonic_values = iter([0.0, 0.0, 0.1, 18.1, 18.1, 18.1])
            with mock.patch("sourcing_agent.enrichment.time.monotonic", side_effect=lambda: next(monotonic_values)), mock.patch(
                "sourcing_agent.enrichment.time.sleep",
                return_value=None,
            ):
                fetched = enricher._fetch_harvest_profiles_for_urls(
                    [queued_url, pending_url],
                    root,
                    asset_logger=AssetLogger(root),
                    source_jobs=["job_enrichment_scheduler"],
                )

            self.assertNotIn("live_fetch", events)
            self.assertIn("stale_queued_registry_reclaimed", events)
            self.assertIn("cache_miss_scheduler_required", events)
            self.assertEqual(fetched, {})
            self.assertNotIn(queued_url, fetched)

    def test_fetch_harvest_profiles_reuses_queued_registry_raw_without_live_fetch(self) -> None:
        class _FailIfLiveFetchConnector:
            def fetch_profiles_by_urls(
                self,
                profile_urls,
                snapshot_dir,
                asset_logger=None,
                use_cache=True,
                allow_shared_provider_cache=True,
            ):
                raise AssertionError("live fetch should not run when queued registry entry already has valid raw payload")

        class _QueuedRawStore:
            def __init__(self, profile_url: str, raw_path: Path) -> None:
                self.profile_url = normalize_linkedin_profile_url_key(profile_url)
                self.raw_path = raw_path
                self.fetched_marks: list[str] = []
                self.repos = SimpleNamespace(linkedin_profile_registry=self)

            def get_bulk(self, profile_urls):
                return {
                    self.profile_url: {
                        "status": "queued",
                        "last_raw_path": str(self.raw_path),
                    }
                }

            def get(self, profile_url):
                if normalize_linkedin_profile_url_key(profile_url) == self.profile_url:
                    return {
                        "status": "queued",
                        "last_raw_path": str(self.raw_path),
                    }
                return {}

            def acquire_lease(self, profile_url, lease_owner="", lease_seconds=0):
                return {"acquired": True, "lease_owner": lease_owner, "lease_token": "token"}

            def release_lease(self, profile_url, lease_owner="", lease_token=""):
                return {"released": True}

            def record_event(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def mark_queued(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def mark_fetched(self, profile_url, **kwargs):
                self.fetched_marks.append(str(profile_url or "").strip())
                return {"profile_url": profile_url, **kwargs}

            def mark_failed(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def upsert_sources(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            raw_dir = root / "existing_snapshot" / "harvest_profiles"
            raw_dir.mkdir(parents=True, exist_ok=True)
            profile_url = "https://www.linkedin.com/in/cached-queued-profile/"
            raw_path = raw_dir / "cached.json"
            raw_path.write_text(
                json.dumps(
                    {
                        "_harvest_request": {"profile_url": profile_url},
                        "item": {
                            "firstName": "Cached",
                            "lastName": "Queued",
                            "linkedinUrl": profile_url,
                            "headline": "Infra Engineer",
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = _QueuedRawStore(profile_url, raw_path)
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_FailIfLiveFetchConnector(),
                store=store,
            )

            fetched = enricher._fetch_harvest_profiles_for_urls(
                [profile_url],
                root / "active_snapshot",
                asset_logger=AssetLogger(root / "active_snapshot"),
            )

            self.assertIn(profile_url, fetched)
            self.assertEqual(fetched[profile_url]["parsed"]["full_name"], "Cached Queued")
            self.assertEqual(store.fetched_marks, [profile_url])

    def test_publication_lead_public_web_gate_requires_candidate_confirmation_for_paid_search(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[])
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            lead = Candidate(
                candidate_id="lead1",
                name_en="Kevin Lu",
                display_name="Kevin Lu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                source_dataset="publication_lead",
                source_path=str(root / "publication_leads.json"),
                metadata={
                    "publication_title": "Scaling Laws in Practice",
                    "exploration_affiliation_signals": [
                        {
                            "organization": "Thinking Machines Lab",
                            "relation": "explicit_current_affiliation",
                            "evidence": "Kevin Lu is a research engineer at Thinking Machines Lab.",
                        }
                    ],
                },
            )
            candidate_map = {"kevinlu": lead}
            unresolved: list[dict[str, object]] = []

            gated, summary_path = enricher._gate_publication_leads_after_exploration(
                lead_candidates=[lead],
                identity=identity,
                snapshot_dir=root,
                candidate_map=candidate_map,
                unresolved_candidates=unresolved,
                asset_logger=AssetLogger(root),
                allow_targeted_name_search=True,
            )

            self.assertEqual(gated, [])
            self.assertTrue(summary_path.exists())
            updated = candidate_map["kevinlu"]
            self.assertEqual(updated.metadata["publication_lead_resolution_state"], "confirmed_public_web_missing_linkedin")
            self.assertFalse(updated.metadata["publication_lead_targeted_name_search_eligible"])
            self.assertEqual(len(unresolved), 1)
            self.assertEqual(unresolved[0]["resolution_source"], "publication_lead_public_web_verification")
            self.assertEqual(unresolved[0]["next_step"], "await_user_confirmation_before_paid_search")

    def test_scholar_coauthor_follow_up_fetches_profile_only_after_public_web_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[])
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            prospect = Candidate(
                candidate_id="carol",
                name_en="Carol Wu",
                display_name="Carol Wu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                source_dataset="roster_anchored_scholar_coauthor_prospect",
                source_path=str(root / "prospects.json"),
                metadata={
                    "lead_discovery_method": "roster_anchored_scholar_coauthor_expansion",
                    "scholar_coauthor_seed_names": ["Alice Zhang"],
                    "scholar_coauthor_papers": [{"title": "Scaling Laws in Practice", "url": "https://arxiv.org/abs/1234.5678"}],
                    "publication_title": "Scaling Laws in Practice",
                },
            )
            candidate_map: dict[str, Candidate] = {}
            resolved_profiles: list[dict[str, object]] = []
            evidence = []

            enricher.exploratory_enricher._explore_candidate = lambda **kwargs: {
                "candidate": Candidate(
                    **{
                        **prospect.to_record(),
                        "linkedin_url": "https://www.linkedin.com/in/carol-wu/",
                        "metadata": {
                            **prospect.metadata,
                            "exploration_links": {"linkedin": ["https://www.linkedin.com/in/carol-wu/"]},
                            "exploration_affiliation_signals": [
                                {
                                    "organization": "Thinking Machines Lab",
                                    "relation": "explicit_current_affiliation",
                                    "evidence": "Carol Wu is a member of technical staff at Thinking Machines Lab.",
                                }
                            ],
                        },
                    }
                ),
                "evidence": [],
                "summary": {"candidate_id": "carol"},
                "errors": [],
            }

            def _resolve_candidate(*args, **kwargs):
                candidate = args[0]
                candidate_map["carolwu"] = Candidate(
                    candidate_id=candidate.candidate_id,
                    name_en=candidate.name_en,
                    display_name=candidate.display_name,
                    category="employee",
                    employment_status="current",
                    target_company=candidate.target_company,
                    organization=candidate.organization,
                    linkedin_url=candidate.linkedin_url,
                    source_dataset=candidate.source_dataset,
                    source_path=candidate.source_path,
                    metadata=dict(candidate.metadata),
                )
                resolved_profiles.append({"candidate_id": candidate.candidate_id})
                return True, 1

            enricher._resolve_candidate_with_known_refs = _resolve_candidate
            profile_fetch_count, summary_path, errors, queued_count = enricher._follow_up_roster_anchored_scholar_coauthor_prospects(
                prospects=[prospect],
                identity=identity,
                snapshot_dir=root,
                candidate_map=candidate_map,
                resolved_profiles=resolved_profiles,
                evidence=evidence,
                profile_fetch_count=0,
                profile_detail_limit=2,
                exploration_limit=1,
                asset_logger=AssetLogger(root),
                job_id="",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(profile_fetch_count, 1)
            self.assertEqual(queued_count, 0)
            self.assertTrue(summary_path.exists())
            self.assertEqual(errors, [])
            self.assertEqual(candidate_map["carolwu"].category, "employee")
            summary = json.loads(summary_path.read_text())
            self.assertTrue(summary["decisions"][0]["profile_verified"])

    def test_scholar_coauthor_follow_up_holds_unconfirmed_linkedin_without_profile_fetch(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[])
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            prospect = Candidate(
                candidate_id="carol",
                name_en="Carol Wu",
                display_name="Carol Wu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                source_dataset="roster_anchored_scholar_coauthor_prospect",
                source_path=str(root / "prospects.json"),
                metadata={
                    "lead_discovery_method": "roster_anchored_scholar_coauthor_expansion",
                    "scholar_coauthor_seed_names": ["Alice Zhang"],
                    "scholar_coauthor_papers": [{"title": "Scaling Laws in Practice", "url": "https://arxiv.org/abs/1234.5678"}],
                    "publication_title": "Scaling Laws in Practice",
                },
            )

            enricher.exploratory_enricher._explore_candidate = lambda **kwargs: {
                "candidate": Candidate(
                    **{
                        **prospect.to_record(),
                        "linkedin_url": "https://www.linkedin.com/in/carol-wu/",
                        "metadata": {
                            **prospect.metadata,
                            "exploration_links": {"linkedin": ["https://www.linkedin.com/in/carol-wu/"]},
                        },
                    }
                ),
                "evidence": [],
                "summary": {"candidate_id": "carol"},
                "errors": [],
            }

            def _fail_if_called(*args, **kwargs):
                raise AssertionError("unconfirmed scholar coauthor prospects should not auto-fetch LinkedIn profile")

            enricher._resolve_candidate_with_known_refs = _fail_if_called
            profile_fetch_count, summary_path, errors, queued_count = enricher._follow_up_roster_anchored_scholar_coauthor_prospects(
                prospects=[prospect],
                identity=identity,
                snapshot_dir=root,
                candidate_map={},
                resolved_profiles=[],
                evidence=[],
                profile_fetch_count=0,
                profile_detail_limit=2,
                exploration_limit=1,
                asset_logger=AssetLogger(root),
                job_id="",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(profile_fetch_count, 0)
            self.assertEqual(queued_count, 0)
            self.assertTrue(summary_path.exists())
            self.assertEqual(errors, [])
            summary = json.loads(summary_path.read_text())
            self.assertEqual(summary["decisions"][0]["state"], "linkedin_discovered_membership_unconfirmed")
            self.assertFalse(summary["decisions"][0]["profile_verified"])

    def test_scholar_coauthor_follow_up_persists_progress_and_resumes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[])
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            prospects = [
                Candidate(
                    candidate_id="carol",
                    name_en="Carol Wu",
                    display_name="Carol Wu",
                    category="lead",
                    target_company="Thinking Machines Lab",
                    organization="Thinking Machines Lab",
                    source_dataset="roster_anchored_scholar_coauthor_prospect",
                    source_path=str(root / "prospects.json"),
                    metadata={
                        "lead_discovery_method": "roster_anchored_scholar_coauthor_expansion",
                        "scholar_coauthor_seed_names": ["Alice Zhang"],
                        "scholar_coauthor_papers": [{"title": "Scaling Laws in Practice", "url": "https://arxiv.org/abs/1234.5678"}],
                        "publication_title": "Scaling Laws in Practice",
                    },
                ),
                Candidate(
                    candidate_id="dana",
                    name_en="Dana Li",
                    display_name="Dana Li",
                    category="lead",
                    target_company="Thinking Machines Lab",
                    organization="Thinking Machines Lab",
                    source_dataset="roster_anchored_scholar_coauthor_prospect",
                    source_path=str(root / "prospects.json"),
                    metadata={
                        "lead_discovery_method": "roster_anchored_scholar_coauthor_expansion",
                        "scholar_coauthor_seed_names": ["Bob Li"],
                        "scholar_coauthor_papers": [{"title": "Inference Systems", "url": "https://arxiv.org/abs/9999.9999"}],
                        "publication_title": "Inference Systems",
                    },
                ),
            ]
            explore_calls: list[str] = []

            def _explore_candidate(**kwargs):
                candidate = kwargs["candidate"]
                explore_calls.append(candidate.candidate_id)
                return {
                    "candidate": Candidate(
                        **{
                            **candidate.to_record(),
                            "linkedin_url": f"https://www.linkedin.com/in/{candidate.name_en.lower().replace(' ', '-')}/",
                            "metadata": {
                                **candidate.metadata,
                                "exploration_links": {"linkedin": [f"https://www.linkedin.com/in/{candidate.name_en.lower().replace(' ', '-')}/"]},
                                "exploration_affiliation_signals": [
                                    {
                                        "organization": "Thinking Machines Lab",
                                        "relation": "explicit_current_affiliation",
                                        "evidence": f"{candidate.display_name} works at Thinking Machines Lab.",
                                    }
                                ],
                            },
                        }
                    ),
                    "evidence": [
                        EvidenceRecord(
                            evidence_id=f"explore-{candidate.candidate_id}",
                            candidate_id=candidate.candidate_id,
                            source_type="exploration_summary",
                            title=f"{candidate.display_name} exploration",
                            url="https://example.com/exploration",
                            summary=f"Exploration confirmed {candidate.display_name} has public-web affiliation evidence.",
                            source_dataset="exploration_summary",
                            source_path=str(root / f"{candidate.candidate_id}.json"),
                        )
                    ],
                    "summary": {"candidate_id": candidate.candidate_id},
                    "errors": [],
                }

            def _resolve_candidate(*args, **kwargs):
                candidate = args[0]
                current_count = args[3]
                candidate_map = args[5]
                resolved_profiles = args[6]
                evidence = args[7]
                updated_candidate = Candidate(
                    candidate_id=candidate.candidate_id,
                    name_en=candidate.name_en,
                    display_name=candidate.display_name,
                    category="employee",
                    employment_status="current",
                    target_company=candidate.target_company,
                    organization=candidate.organization,
                    linkedin_url=candidate.linkedin_url,
                    source_dataset=candidate.source_dataset,
                    source_path=candidate.source_path,
                    metadata=dict(candidate.metadata),
                )
                candidate_map[candidate.name_en.lower().replace(" ", "")] = updated_candidate
                resolved_profiles.append(
                    {
                        "candidate_id": candidate.candidate_id,
                        "profile_url": candidate.linkedin_url,
                        "raw_path": str(root / f"{candidate.candidate_id}_profile.json"),
                        "resolution_source": "known_profile_url_harvest",
                    }
                )
                evidence.append(
                    EvidenceRecord(
                        evidence_id=f"profile-{candidate.candidate_id}",
                        candidate_id=candidate.candidate_id,
                        source_type="linkedin_profile_detail",
                        title=f"{candidate.display_name} profile",
                        url=candidate.linkedin_url,
                        summary=f"Resolved {candidate.display_name} via profile detail.",
                        source_dataset="linkedin_profile_detail",
                        source_path=str(root / f"{candidate.candidate_id}_profile.json"),
                    )
                )
                return True, current_count + 1

            enricher.exploratory_enricher._explore_candidate = _explore_candidate
            enricher._resolve_candidate_with_known_refs = _resolve_candidate

            candidate_map_1: dict[str, Candidate] = {}
            resolved_profiles_1: list[dict[str, object]] = []
            evidence_1: list[EvidenceRecord] = []
            profile_fetch_count, summary_path, errors, queued_count = enricher._follow_up_roster_anchored_scholar_coauthor_prospects(
                prospects=prospects,
                identity=identity,
                snapshot_dir=root,
                candidate_map=candidate_map_1,
                resolved_profiles=resolved_profiles_1,
                evidence=evidence_1,
                profile_fetch_count=0,
                profile_detail_limit=5,
                exploration_limit=1,
                asset_logger=AssetLogger(root),
                job_id="",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(profile_fetch_count, 1)
            self.assertEqual(queued_count, 0)
            self.assertEqual(errors, [])
            self.assertEqual(explore_calls, ["carol"])
            progress_path = summary_path.parent / "follow_up_progress.json"
            patch_path = summary_path.parent / "follow_up_candidate_patch.json"
            self.assertTrue(progress_path.exists())
            self.assertTrue(patch_path.exists())
            partial_summary = json.loads(summary_path.read_text())
            self.assertEqual(partial_summary["status"], "partial")
            self.assertEqual(partial_summary["candidate_count"], 1)

            candidate_map_2: dict[str, Candidate] = {}
            resolved_profiles_2: list[dict[str, object]] = []
            evidence_2: list[EvidenceRecord] = []
            profile_fetch_count, summary_path, errors, queued_count = enricher._follow_up_roster_anchored_scholar_coauthor_prospects(
                prospects=prospects,
                identity=identity,
                snapshot_dir=root,
                candidate_map=candidate_map_2,
                resolved_profiles=resolved_profiles_2,
                evidence=evidence_2,
                profile_fetch_count=0,
                profile_detail_limit=5,
                exploration_limit=2,
                asset_logger=AssetLogger(root),
                job_id="",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(profile_fetch_count, 1)
            self.assertEqual(queued_count, 0)
            self.assertEqual(errors, [])
            self.assertEqual(explore_calls, ["carol", "dana"])
            self.assertEqual({item.display_name for item in candidate_map_2.values()}, {"Carol Wu", "Dana Li"})
            self.assertEqual({item["candidate_id"] for item in resolved_profiles_2}, {"carol", "dana"})
            self.assertEqual({item.candidate_id for item in evidence_2}, {"carol", "dana"})
            resumed_summary = json.loads(summary_path.read_text())
            self.assertEqual(resumed_summary["status"], "completed")
            self.assertEqual(resumed_summary["candidate_count"], 2)
            self.assertEqual(resumed_summary["remaining_candidate_count"], 0)

    def test_scholar_coauthor_follow_up_keeps_queued_candidates_recoverable(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[])
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            prospect = Candidate(
                candidate_id="queued",
                name_en="Queued Prospect",
                display_name="Queued Prospect",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                metadata={"scholar_coauthor_seed_names": ["Seed A"]},
            )
            call_count = {"value": 0}

            def _explore_candidate(*args, **kwargs):
                call_count["value"] += 1
                if call_count["value"] == 1:
                    return {
                        "candidate": prospect,
                        "evidence": [],
                        "summary": {"candidate_id": "queued", "status": "queued"},
                        "errors": [],
                        "worker_status": "queued",
                    }
                return {
                    "candidate": Candidate(
                        **{
                            **prospect.to_record(),
                            "linkedin_url": "https://www.linkedin.com/in/queued-prospect/",
                            "metadata": {
                                **prospect.metadata,
                                "exploration_affiliation_signals": [
                                    {
                                        "organization": "Thinking Machines Lab",
                                        "role": "Researcher",
                                        "evidence": "Queued Prospect is affiliated with Thinking Machines Lab.",
                                    }
                                ],
                                "exploration_validated_summaries": [
                                    "Queued Prospect is affiliated with Thinking Machines Lab."
                                ],
                                "exploration_links": {
                                    "linkedin": ["https://www.linkedin.com/in/queued-prospect/"],
                                },
                            },
                        }
                    ),
                    "evidence": [],
                    "summary": {"candidate_id": "queued", "status": "completed"},
                    "errors": [],
                    "worker_status": "completed",
                }

            enricher.exploratory_enricher._explore_candidate = _explore_candidate

            def _resolve_candidate(*args, **kwargs):
                candidate = args[0]
                profile_fetch_count = args[3]
                candidate_map = args[5]
                resolved_profiles = args[6]
                candidate_map[_candidate_key(candidate)] = candidate
                resolved_profiles.append({"candidate_id": candidate.candidate_id})
                return True, profile_fetch_count + 1

            enricher._resolve_candidate_with_known_refs = _resolve_candidate

            candidate_map_1: dict[str, Candidate] = {}
            resolved_profiles_1: list[dict[str, object]] = []
            evidence_1: list[EvidenceRecord] = []
            profile_fetch_count, summary_path, errors, queued_count = enricher._follow_up_roster_anchored_scholar_coauthor_prospects(
                prospects=[prospect],
                identity=identity,
                snapshot_dir=root,
                candidate_map=candidate_map_1,
                resolved_profiles=resolved_profiles_1,
                evidence=evidence_1,
                profile_fetch_count=0,
                profile_detail_limit=5,
                exploration_limit=1,
                asset_logger=AssetLogger(root),
                job_id="job_1",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(profile_fetch_count, 0)
            self.assertEqual(queued_count, 1)
            self.assertEqual(errors, [])
            first_summary = json.loads(summary_path.read_text())
            self.assertEqual(first_summary["status"], "partial")
            self.assertEqual(first_summary["remaining_candidate_count"], 1)
            self.assertEqual(first_summary["decisions"][0]["state"], "queued_background_exploration")

            candidate_map_2: dict[str, Candidate] = {}
            resolved_profiles_2: list[dict[str, object]] = []
            evidence_2: list[EvidenceRecord] = []
            profile_fetch_count, summary_path, errors, queued_count = enricher._follow_up_roster_anchored_scholar_coauthor_prospects(
                prospects=[prospect],
                identity=identity,
                snapshot_dir=root,
                candidate_map=candidate_map_2,
                resolved_profiles=resolved_profiles_2,
                evidence=evidence_2,
                profile_fetch_count=0,
                profile_detail_limit=5,
                exploration_limit=1,
                asset_logger=AssetLogger(root),
                job_id="job_1",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(profile_fetch_count, 1)
            self.assertEqual(queued_count, 0)
            self.assertEqual(errors, [])
            final_summary = json.loads(summary_path.read_text())
            self.assertEqual(final_summary["status"], "completed")
            self.assertEqual(final_summary["remaining_candidate_count"], 0)

    def test_enrich_skips_targeted_publication_harvest_without_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[])
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            lead = Candidate(
                candidate_id="lead1",
                name_en="Kevin Lu",
                display_name="Kevin Lu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                source_dataset="publication_lead",
                source_path=str(root / "publication_leads.json"),
            )
            enricher.slug_resolver.resolve = lambda *args, **kwargs: {"results": [], "summary_path": None, "errors": []}
            enricher.exploratory_enricher.enrich = lambda *args, **kwargs: type(
                "_Result",
                (),
                {"artifact_paths": {}, "errors": [], "evidence": [], "candidates": []},
            )()
            enricher.publication_connector.enrich = lambda **kwargs: {
                "matched_candidates": [],
                "lead_candidates": [lead],
                "artifact_paths": {},
                "errors": [],
                "evidence": [],
                "publication_matches": [],
                "coauthor_edges": [],
            }

            def _fail_if_called(**kwargs):
                raise AssertionError("targeted Harvest name search should be gated off by default")

            enricher._resolve_publication_leads_with_harvest_search = _fail_if_called
            result = enricher.enrich(
                identity,
                root,
                [],
                JobRequest(
                    raw_user_request="Find Thinking Machines Lab publication leads",
                    target_company="Thinking Machines Lab",
                    publication_lead_limit=1,
                    profile_detail_limit=2,
                    exploration_limit=0,
                ),
            )
            self.assertEqual(len(result.unresolved_candidates), 1)
            self.assertEqual(result.unresolved_candidates[0]["resolution_source"], "publication_lead_public_web_verification")
            self.assertEqual(
                result.unresolved_candidates[0]["publication_lead_resolution_state"],
                "publication_source_only_unconfirmed",
            )

    def test_enrich_does_not_auto_fetch_linkedin_for_unconfirmed_publication_lead(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[])
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            lead = Candidate(
                candidate_id="lead1",
                name_en="Kevin Lu",
                display_name="Kevin Lu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                source_dataset="publication_lead",
                source_path=str(root / "publication_leads.json"),
                metadata={
                    "publication_title": "Scaling Laws in Practice",
                    "lead_discovery_method": "publication_author_acknowledgement_scan",
                },
            )
            explored = Candidate(
                candidate_id="lead1",
                name_en="Kevin Lu",
                display_name="Kevin Lu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                linkedin_url="https://www.linkedin.com/in/kevin-lu/",
                source_dataset="publication_lead",
                source_path=str(root / "publication_leads.json"),
                metadata={
                    "publication_title": "Scaling Laws in Practice",
                    "lead_discovery_method": "publication_author_acknowledgement_scan",
                    "exploration_links": {"linkedin": ["https://www.linkedin.com/in/kevin-lu/"]},
                },
            )
            enricher.slug_resolver.resolve = lambda *args, **kwargs: {"results": [], "summary_path": None, "errors": []}
            enricher.exploratory_enricher.enrich = lambda *args, **kwargs: type(
                "_Result",
                (),
                {"artifact_paths": {}, "errors": [], "evidence": [], "candidates": [explored]},
            )()
            enricher.publication_connector.enrich = lambda **kwargs: {
                "matched_candidates": [],
                "lead_candidates": [lead],
                "artifact_paths": {},
                "errors": [],
                "evidence": [],
                "publication_matches": [],
                "coauthor_edges": [],
            }

            def _fail_if_called(*args, **kwargs):
                raise AssertionError("publication leads without public-web affiliation confirmation should not auto-fetch LinkedIn")

            enricher._resolve_candidate_with_known_refs = _fail_if_called
            result = enricher.enrich(
                identity,
                root,
                [],
                JobRequest(
                    raw_user_request="Find Thinking Machines Lab publication leads",
                    target_company="Thinking Machines Lab",
                    publication_lead_limit=1,
                    profile_detail_limit=2,
                    exploration_limit=1,
                ),
            )
            self.assertEqual(len(result.unresolved_candidates), 1)
            self.assertEqual(
                result.unresolved_candidates[0]["publication_lead_resolution_state"],
                "linkedin_discovered_membership_unconfirmed",
            )

    def test_enrich_does_not_auto_run_scholar_coauthor_follow_up_without_explicit_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[])
            identity = CompanyIdentity(
                requested_name="Thinking Machines Lab",
                canonical_name="Thinking Machines Lab",
                company_key="thinkingmachineslab",
                linkedin_slug="thinkingmachinesai",
                linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            )
            prospect = Candidate(
                candidate_id="prospect1",
                name_en="Carol Wu",
                display_name="Carol Wu",
                category="lead",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                source_dataset="roster_anchored_scholar_coauthor_prospect",
                source_path=str(root / "prospects.json"),
                metadata={
                    "lead_discovery_method": "roster_anchored_scholar_coauthor_expansion",
                    "scholar_coauthor_seed_names": ["Alice Zhang"],
                    "scholar_coauthor_papers": [{"title": "Scaling Laws in Practice"}],
                },
            )
            enricher.slug_resolver.resolve = lambda *args, **kwargs: {"results": [], "summary_path": None, "errors": []}
            enricher.exploratory_enricher.enrich = lambda *args, **kwargs: type(
                "_Result",
                (),
                {"artifact_paths": {}, "errors": [], "evidence": [], "candidates": []},
            )()
            enricher.publication_connector.enrich = lambda **kwargs: {
                "matched_candidates": [],
                "lead_candidates": [],
                "artifact_paths": {},
                "errors": [],
                "evidence": [],
                "publication_matches": [],
                "coauthor_edges": [],
                "scholar_coauthor_prospects": [prospect],
            }

            def _fail_if_called(**kwargs):
                raise AssertionError("scholar coauthor follow-up should require explicit scholar_coauthor_follow_up_limit")

            enricher._follow_up_roster_anchored_scholar_coauthor_prospects = _fail_if_called
            result = enricher.enrich(
                identity,
                root,
                [],
                JobRequest(
                    raw_user_request="Find Thinking Machines Lab coauthors",
                    target_company="Thinking Machines Lab",
                    exploration_limit=3,
                    scholar_coauthor_follow_up_limit=0,
                ),
            )
            self.assertEqual(result.errors, [])

    def test_queue_background_profile_prefetch_reuses_registry_queued_urls_without_provider_submit(self) -> None:
        class _FailIfSubmittedConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("already queued registry URLs should not submit a new Harvest actor run")

        class _QueuedStore:
            def __init__(self, profile_url: str) -> None:
                self.profile_key = normalize_linkedin_profile_url_key(profile_url)
                self.source_updates: list[dict[str, object]] = []
                self.repos = SimpleNamespace(linkedin_profile_registry=self)

            def get_bulk(self, profile_urls):
                return {self.profile_key: {"status": "queued"}}

            def upsert_sources(self, profile_url, **kwargs):
                self.source_updates.append({"profile_url": profile_url, **kwargs})
                return {"profile_url": profile_url, **kwargs}

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            profile_url = "https://www.linkedin.com/in/already-queued/"
            store = _QueuedStore(profile_url)
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_FailIfSubmittedConnector(),
                store=store,
            )
            enricher.worker_runtime = object()
            result = enricher.queue_background_profile_prefetch(
                candidates=[
                    Candidate(
                        candidate_id="queued",
                        name_en="Already Queued",
                        display_name="Already Queued",
                        linkedin_url=profile_url,
                    )
                ],
                snapshot_dir=root,
                job_id="job_queued_registry",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["reason"], "reused_active_harvest_profile_queue")
        self.assertEqual(result["dispatched_url_count"], 0)
        self.assertEqual(result["already_queued_url_count"], 1)
        self.assertEqual(store.source_updates[0]["source_jobs"], ["job_queued_registry"])

    def test_queue_background_profile_prefetch_reclaims_stale_queued_registry_completed_worker(self) -> None:
        class _CompletingConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                self.calls.append(
                    {
                        "profile_urls": list(profile_urls),
                        "checkpoint": dict(checkpoint or {}),
                    }
                )
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-new",
                        "dataset_id": "dataset-new",
                        "status": "succeeded",
                    },
                    body={
                        "items": [
                            {
                                "profileUrl": list(profile_urls)[0],
                                "firstName": "Recovered",
                                "lastName": "Profile",
                                "headline": "Agent engineer at OpenAI",
                            }
                        ]
                    },
                    pending=False,
                    message="Harvest batch completed.",
                )

            def persist_profiles_from_batch_body(self, profile_urls, body, snapshot_dir, asset_logger=None):
                profiles = {}
                for profile_url in profile_urls:
                    slug = profile_url.rstrip("/").rsplit("/", 1)[-1]
                    raw_path = snapshot_dir / "harvest_profiles" / f"{slug}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text(
                        json.dumps(
                            {
                                "_harvest_request": {"profile_url": profile_url},
                                "item": {
                                    "linkedinUrl": profile_url,
                                    "firstName": "Recovered",
                                    "lastName": "Profile",
                                    "headline": "Agent engineer at OpenAI",
                                },
                            },
                            ensure_ascii=False,
                        ),
                        encoding="utf-8",
                    )
                    profiles[profile_url] = {"raw_path": str(raw_path)}
                return {"profiles": profiles, "unresolved_urls": []}

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            profile_url = "https://www.linkedin.com/in/stale-queued-profile/"
            request = JobRequest(raw_user_request="Find stale queued profile", target_company="OpenAI")
            payload_hash = sha1(
                json.dumps(sorted([profile_url]), ensure_ascii=False).encode("utf-8")
            ).hexdigest()[:16]
            old_handle = worker_runtime.begin_worker(
                job_id="job_reclaim_stale_queued_registry",
                request=request,
                plan_payload={},
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::{payload_hash}",
                stage="enriching",
                span_name=f"harvest_profile_batch:{payload_hash}",
                budget_payload={"requested_url_count": 1},
                input_payload={"profile_urls": [profile_url]},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(root),
                    "profile_urls": [profile_url],
                    "request_payload": request.to_record(),
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                    "allow_shared_provider_cache": True,
                },
                handoff_from_lane="acquisition_specialist",
            )
            worker_runtime.complete_worker(
                old_handle,
                status="completed",
                checkpoint_payload={
                    "stage": "completed",
                    "run_id": "run-old",
                    "dataset_id": "dataset-old",
                },
                output_payload={
                    "summary": {
                        "status": "completed",
                        "requested_urls": [profile_url],
                        "persisted_profile_count": 0,
                    }
                },
            )
            store.repos.linkedin_profile_registry.mark_queued(
                profile_url,
                source_shards=["enrichment_background_prefetch"],
                source_jobs=["job_reclaim_stale_queued_registry"],
                run_id="run-old",
                dataset_id="dataset-old",
                snapshot_dir=str(root),
            )
            connector = _CompletingConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime

            result = enricher.queue_background_profile_prefetch(
                candidates=[
                    Candidate(
                        candidate_id="stale_queued",
                        name_en="Stale Queued",
                        display_name="Stale Queued",
                        linkedin_url=profile_url,
                    )
                ],
                snapshot_dir=root,
                job_id="job_reclaim_stale_queued_registry",
                request_payload=request.to_record(),
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )
            registry = store.repos.linkedin_profile_registry.get(profile_url) or {}
            worker = store.get_agent_worker(worker_id=old_handle.worker_id) or {}

        self.assertEqual(result["status"], "completed")
        self.assertEqual(len(connector.calls), 1)
        self.assertEqual(connector.calls[0]["profile_urls"], [profile_url])
        self.assertNotEqual(dict(connector.calls[0]["checkpoint"]).get("run_id"), "run-old")
        self.assertEqual(registry.get("status"), "fetched")
        self.assertEqual(worker.get("status"), "completed")

    def test_harvest_profile_batch_worker_resumes_remote_run_even_when_registry_urls_are_queued(self) -> None:
        class _ResumeConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                self.calls.append(
                    {
                        "profile_urls": list(profile_urls),
                        "checkpoint": dict(checkpoint or {}),
                        "runtime_timing_overrides": dict(kwargs.get("runtime_timing_overrides") or {}),
                    }
                )
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-existing",
                        "dataset_id": "dataset-existing",
                        "status": "succeeded",
                    },
                    body={"items": [{"profileUrl": list(profile_urls)[0]}]},
                    pending=False,
                    message="Harvest batch completed.",
                )

            def persist_profiles_from_batch_body(self, profile_urls, body, snapshot_dir, asset_logger=None):
                raw_path = snapshot_dir / "harvest_profiles" / "resume-profile.json"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(json.dumps(body or {}, ensure_ascii=False), encoding="utf-8")
                return {
                    "profiles": {
                        list(profile_urls)[0]: {
                            "raw_path": str(raw_path),
                        }
                    },
                    "unresolved_urls": [],
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            profile_url = "https://www.linkedin.com/in/queued-resume/"
            request = JobRequest(raw_user_request="Find queued resume", target_company="Queued Resume")
            payload_hash = sha1(
                json.dumps(sorted([profile_url]), ensure_ascii=False).encode("utf-8")
            ).hexdigest()[:16]
            handle = worker_runtime.begin_worker(
                job_id="job_resume_queued_registry",
                request=request,
                plan_payload={},
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::{payload_hash}",
                stage="enriching",
                span_name=f"harvest_profile_batch:{payload_hash}",
                budget_payload={"requested_url_count": 1},
                input_payload={"profile_urls": [profile_url]},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(root),
                    "profile_urls": [profile_url],
                    "request_payload": request.to_record(),
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                    "allow_shared_provider_cache": True,
                },
                handoff_from_lane="acquisition_specialist",
            )
            store.repos.linkedin_profile_registry.mark_queued(
                profile_url,
                source_shards=["enrichment_background_prefetch"],
                source_jobs=["job_resume_queued_registry"],
                run_id="run-existing",
                dataset_id="dataset-existing",
                snapshot_dir=str(root),
            )
            worker_runtime.complete_worker(
                handle,
                status="queued",
                checkpoint_payload={
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-existing",
                    "dataset_id": "dataset-existing",
                    "provider_limiter_lease": {
                        "db_limiter_enabled": False,
                        "limiter_key": "harvest_profile_scraper_actor",
                    },
                },
                output_payload={
                    "summary": {
                        "status": "queued",
                        "requested_urls": [profile_url],
                    }
                },
            )
            connector = _ResumeConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=[profile_url],
                snapshot_dir=root,
                job_id="job_resume_queued_registry",
                request_payload=request.to_record(),
                plan_payload={},
                runtime_mode="daemon_recovery",
                allow_shared_provider_cache=True,
            )

            registry = store.repos.linkedin_profile_registry.get_bulk([profile_url])
            worker = store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(result["worker_status"], "completed")
        self.assertEqual(len(connector.calls), 1)
        self.assertEqual(connector.calls[0]["profile_urls"], [profile_url])
        self.assertEqual(dict(connector.calls[0]["checkpoint"]).get("run_id"), "run-existing")
        self.assertEqual(
            connector.calls[0]["runtime_timing_overrides"],
            {
                "harvest_run_status_timeout_seconds": 15,
                "harvest_run_status_wait_for_finish_seconds": 10,
                "harvest_dataset_page_timeout_seconds": 15,
                "harvest_dataset_fetch_max_attempts": 1,
            },
        )
        self.assertEqual(dict(registry[normalize_linkedin_profile_url_key(profile_url)]).get("status"), "fetched")
        self.assertEqual(dict(worker or {}).get("status"), "completed")

    def test_harvest_profile_batch_worker_replays_completed_summary_without_provider_submit(self) -> None:
        class _ReplayConnector:
            settings = HarvestActorSettings(enabled=True)

            def __init__(self) -> None:
                self.execute_calls: list[dict[str, object]] = []
                self.persist_calls: list[dict[str, object]] = []
                self.before_persist_return = None

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                self.execute_calls.append(
                    {
                        "profile_urls": list(profile_urls),
                        "checkpoint": dict(checkpoint or {}),
                    }
                )
                raise AssertionError("completed queue summary should replay locally without provider submit")

            def persist_profiles_from_batch_body(self, profile_urls, body, snapshot_dir, asset_logger=None):
                self.persist_calls.append(
                    {
                        "profile_urls": list(profile_urls),
                        "body": body,
                    }
                )
                profiles = {}
                for index, profile_url in enumerate(list(profile_urls or [])):
                    raw_path = Path(snapshot_dir) / "harvest_profiles" / f"replayed-{index}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text(
                        json.dumps(
                            {
                                "_harvest_request": {
                                    "kind": "url",
                                    "value": profile_url,
                                    "profile_url": profile_url,
                                },
                                "item": {
                                    "linkedinUrl": profile_url,
                                    "publicIdentifier": f"replayed-{index}",
                                    "headline": "Replay Engineer",
                                },
                            },
                            ensure_ascii=False,
                        ),
                        encoding="utf-8",
                    )
                    profiles[profile_url] = {"raw_path": str(raw_path)}
                if callable(self.before_persist_return):
                    self.before_persist_return()
                return {"profiles": profiles, "unresolved_urls": []}

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            request = JobRequest(raw_user_request="Find replayed profiles", target_company="Replay Co")
            job_id = "job_completed_summary_replay"
            store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="running",
                stage="acquiring",
                request_payload=request.to_record(),
                plan_payload={},
                artifact_path=str(root),
            )
            profile_urls = [
                "https://www.linkedin.com/in/replay-summary-a/",
                "https://www.linkedin.com/in/replay-summary-b/",
            ]
            payload_hash = sha1(
                json.dumps(sorted(profile_urls), ensure_ascii=False).encode("utf-8")
            ).hexdigest()[:16]
            harvest_dir = root / "harvest_profiles"
            harvest_dir.mkdir(parents=True, exist_ok=True)
            dataset_path = harvest_dir / f"harvest_profile_batch_{payload_hash}.queue_dataset_items.json"
            dataset_path.write_text(
                json.dumps(
                    [
                        {
                            "linkedinUrl": profile_url,
                            "publicIdentifier": f"replay-summary-{index}",
                            "headline": "Replay Engineer",
                        }
                        for index, profile_url in enumerate(profile_urls)
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            summary_path = harvest_dir / f"harvest_profile_batch_{payload_hash}.queue_summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "logical_name": "harvest_profile_scraper_batch",
                        "requested_url_count": len(profile_urls),
                        "requested_urls": profile_urls,
                        "queued_urls": profile_urls,
                        "dispatched_url_count": len(profile_urls),
                        "status": "completed",
                        "message": "Scripted Harvest response returned.",
                        "worker_id": 1,
                        "run_id": "run-replayed",
                        "dataset_id": "dataset-replayed",
                        "payload_hash": payload_hash,
                        "artifact_paths": {"dataset_items": str(dataset_path)},
                        "provider_limiter": {"limiter_key": "harvest_profile_scraper_actor", "budget": 4},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            handle = worker_runtime.begin_worker(
                job_id=job_id,
                request=request,
                plan_payload={},
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::{payload_hash}",
                stage="enriching",
                span_name=f"harvest_profile_batch:{payload_hash}",
                budget_payload={"requested_url_count": len(profile_urls)},
                input_payload={"profile_urls": profile_urls},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(root),
                    "profile_urls": profile_urls,
                    "request_payload": request.to_record(),
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                    "allow_shared_provider_cache": True,
                },
                handoff_from_lane="acquisition_specialist",
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=profile_urls,
                source_jobs=[job_id],
                snapshot_dir=str(root),
                trigger_kind="profile_prefetch_provider_submit",
                plan_reason="remote_provider_submitted",
                active_queue_state="planned_dispatch",
                active_owner_worker_id=handle.worker_id,
                active_owner_run_id="run-replayed",
                active_owner_dataset_id="dataset-replayed",
                active_owner_payload_hash=payload_hash,
            )
            worker_runtime.checkpoint_worker(
                handle,
                status="running",
                checkpoint_payload={
                    "stage": "submitting_remote_harvest",
                    "provider_limiter_lease": {
                        "db_limiter_enabled": False,
                        "limiter_key": "harvest_profile_scraper_actor",
                    },
                },
                output_payload={"summary": {"status": "queued", "requested_urls": profile_urls}},
            )
            connector = _ReplayConnector()

            def _write_concurrent_ingest_marker() -> None:
                latest_worker = store.get_agent_worker(worker_id=handle.worker_id) or {}
                checkpoint = dict(latest_worker.get("checkpoint") or {})
                output = dict(latest_worker.get("output") or {})
                output["inline_incremental_ingest"] = {
                    "worker_kind": "harvest_prefetch",
                    "snapshot_id": root.name,
                    "applied_at": "2026-05-08 00:00:00",
                    "candidate_ids": ["existing-marker"],
                    "applied_worker_ids": [handle.worker_id],
                }
                store.checkpoint_agent_worker(
                    handle.worker_id,
                    checkpoint_payload=checkpoint,
                    output_payload=output,
                    status="completed",
                )

            connector.before_persist_return = _write_concurrent_ingest_marker
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=profile_urls,
                snapshot_dir=root,
                job_id=job_id,
                request_payload=request.to_record(),
                plan_payload={},
                runtime_mode="daemon_recovery",
                allow_shared_provider_cache=True,
            )

            worker = store.get_agent_worker(worker_id=handle.worker_id) or {}
            registry = store.repos.linkedin_profile_registry.get_bulk(profile_urls)

        self.assertEqual(result["worker_status"], "completed")
        self.assertEqual(result["summary"]["replay_reason"], "completed_queue_summary_recovered")
        self.assertEqual(connector.execute_calls, [])
        self.assertEqual(len(connector.persist_calls), 1)
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(worker["checkpoint"]["stage"], "completed")
        self.assertEqual(worker["checkpoint"]["run_id"], "run-replayed")
        self.assertEqual(worker["output"]["summary"]["persisted_profile_count"], 2)
        self.assertEqual(worker["output"]["inline_incremental_ingest"]["candidate_ids"], ["existing-marker"])
        for profile_url in profile_urls:
            row = registry[normalize_linkedin_profile_url_key(profile_url)]
            self.assertEqual(row["status"], "fetched")
            self.assertEqual(row["refill_queue_state"], "")
            self.assertEqual(row["refill_terminal_status"], "completed")

    def test_harvest_profile_batch_terminal_persistence_is_bounded_without_resubmitting_provider(self) -> None:
        class _ChunkingConnector:
            settings = HarvestActorSettings(enabled=True)

            def __init__(self) -> None:
                self.execute_calls: list[list[str]] = []
                self.persist_calls: list[list[str]] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                urls = list(profile_urls or [])
                self.execute_calls.append(urls)
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-terminal-chunked",
                        "dataset_id": "dataset-terminal-chunked",
                        "status": "succeeded",
                    },
                    body=[
                        {
                            "linkedinUrl": profile_url,
                            "publicIdentifier": profile_url.rstrip("/").rsplit("/", 1)[-1],
                            "headline": "Chunked terminal persistence",
                        }
                        for profile_url in urls
                    ],
                    pending=False,
                    message="Harvest batch completed.",
                )

            def persist_profiles_from_batch_body(self, profile_urls, body, snapshot_dir, asset_logger=None):
                urls = list(profile_urls or [])
                self.persist_calls.append(urls)
                profiles = {}
                for profile_url in urls:
                    slug = profile_url.rstrip("/").rsplit("/", 1)[-1]
                    raw_path = Path(snapshot_dir) / "harvest_profiles" / f"{slug}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text(
                        json.dumps(
                            {
                                "_harvest_request": {"profile_url": profile_url},
                                "item": {
                                    "linkedinUrl": profile_url,
                                    "publicIdentifier": slug,
                                    "headline": "Chunked terminal persistence",
                                },
                            },
                            ensure_ascii=False,
                        ),
                        encoding="utf-8",
                    )
                    profiles[profile_url] = {"raw_path": str(raw_path)}
                return {"profiles": profiles, "unresolved_urls": []}

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            request = JobRequest(raw_user_request="Find chunked terminal profiles", target_company="Chunk Co")
            job_id = "job_terminal_persistence_chunked"
            store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="running",
                stage="acquiring",
                request_payload=request.to_record(),
                plan_payload={},
                artifact_path=str(root),
            )
            profile_urls = [
                f"https://www.linkedin.com/in/terminal-chunked-{index}/"
                for index in range(5)
            ]
            connector = _ChunkingConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime

            with mock.patch.dict(
                os.environ,
                {
                    "HARVEST_PROFILE_TERMINAL_PERSIST_URL_LIMIT": "2",
                    "HARVEST_PROFILE_TERMINAL_PERSIST_CHUNK_URLS": "2",
                    "HARVEST_PROFILE_TERMINAL_PERSIST_BUDGET_MS": "0",
                },
                clear=False,
            ):
                first = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=profile_urls,
                    snapshot_dir=root,
                    job_id=job_id,
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                )
                second = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=profile_urls,
                    snapshot_dir=root,
                    job_id=job_id,
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="daemon_recovery",
                    allow_shared_provider_cache=True,
                )
                third = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=profile_urls,
                    snapshot_dir=root,
                    job_id=job_id,
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="daemon_recovery",
                    allow_shared_provider_cache=True,
                )

            payload_hash = sha1(
                json.dumps(sorted(profile_urls), ensure_ascii=False).encode("utf-8")
            ).hexdigest()[:16]
            worker = store.get_agent_worker(
                job_id=job_id,
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::{payload_hash}",
            )
            registry = store.repos.linkedin_profile_registry.get_bulk(profile_urls)

        self.assertEqual(connector.execute_calls, [profile_urls])
        self.assertEqual(connector.persist_calls, [profile_urls[:2], profile_urls[2:4], profile_urls[4:]])
        self.assertEqual(first["worker_status"], "running")
        self.assertEqual(second["worker_status"], "running")
        self.assertEqual(third["worker_status"], "completed")
        self.assertEqual(first["summary"]["terminal_persist_progress"]["processed_url_count"], 2)
        self.assertEqual(second["summary"]["terminal_persist_progress"]["processed_url_count"], 4)
        self.assertEqual(third["summary"]["persisted_profile_count"], 5)
        self.assertEqual(third["summary"]["profile_url_terminal_record_command_count"], 3)
        self.assertEqual(third["summary"]["profile_url_terminal_recorded_count"], 5)
        self.assertEqual(dict(worker or {}).get("status"), "completed")
        self.assertEqual(dict(dict(worker or {}).get("checkpoint") or {}).get("stage"), "completed")
        commands = store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id(job_id),
            limit=0,
        )
        self.assertEqual(len(commands), 3)
        self.assertTrue(
            all(command["command_type"] == LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE for command in commands)
        )
        self.assertTrue(all(command["status"] == "succeeded" for command in commands))
        for profile_url in profile_urls:
            row = registry[normalize_linkedin_profile_url_key(profile_url)]
            self.assertEqual(row["status"], "fetched")
            self.assertEqual(row["refill_queue_state"], "")
            self.assertEqual(row["refill_terminal_status"], "completed")

    def test_harvest_profile_batch_partial_success_retries_only_unresolved_urls(self) -> None:
        class _PartialConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-partial",
                        "dataset_id": "dataset-partial",
                        "status": "succeeded",
                    },
                    body={
                        "items": [
                            {
                                "profileUrl": list(profile_urls)[0],
                                "firstName": "Fetched",
                                "lastName": "Profile",
                                "headline": "Research Engineer at OpenAI",
                            }
                        ]
                    },
                    pending=False,
                    message="Harvest batch completed with one unresolved URL.",
                )

            def persist_profiles_from_batch_body(self, profile_urls, body, snapshot_dir, asset_logger=None):
                fetched_url, unresolved_url = list(profile_urls)
                raw_path = snapshot_dir / "harvest_profiles" / "partial-fetched.json"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(
                    json.dumps(
                        {
                            "_harvest_request": {"profile_url": fetched_url},
                            "item": {
                                "linkedinUrl": fetched_url,
                                "firstName": "Fetched",
                                "lastName": "Profile",
                            },
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                return {
                    "profiles": {fetched_url: {"raw_path": str(raw_path)}},
                    "unresolved_urls": [unresolved_url],
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            fetched_url = "https://www.linkedin.com/in/partial-success-fetched/"
            unresolved_url = "https://www.linkedin.com/in/partial-success-unresolved/"
            request = JobRequest(raw_user_request="Find partial success profiles", target_company="OpenAI")
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_PartialConnector(),
                store=store,
            )
            enricher.worker_runtime = worker_runtime

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=[fetched_url, unresolved_url],
                snapshot_dir=root,
                job_id="job_partial_success_retry",
                request_payload=request.to_record(),
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

            fetched_entry = store.repos.linkedin_profile_registry.get(fetched_url) or {}
            unresolved_entry = store.repos.linkedin_profile_registry.get(unresolved_url) or {}
            retry_items = store.repos.linkedin_profile_registry.list_refill_queue_items(
                states=["retry_wait"],
                source_job="job_partial_success_retry",
                snapshot_dir=str(root),
                limit=10,
                ready_only=False,
            )

        self.assertEqual(result["worker_status"], "completed")
        summary = dict(result.get("summary") or {})
        self.assertEqual(summary["persisted_profile_count"], 1)
        self.assertEqual(summary["unresolved_url_count"], 1)
        self.assertEqual(fetched_entry["status"], "fetched")
        self.assertEqual(fetched_entry["refill_queue_state"], "")
        self.assertEqual(unresolved_entry["status"], "failed_retryable")
        self.assertEqual(unresolved_entry["refill_queue_state"], "retry_wait")
        self.assertEqual(unresolved_entry["last_refill_plan_reason"], "profile_retry_wait")
        self.assertEqual(unresolved_entry["retry_count"], 1)
        self.assertEqual([item["profile_url"] for item in retry_items], [unresolved_url])

    def test_harvest_profile_batch_worker_scripted_resume_polls_own_queued_registry_urls(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            scenario_path = root / "scripted_harvest.json"
            profile_url = "https://www.linkedin.com/in/scripted-queued-resume/"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "scripted_profile_batch_resume",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "execute_pending_rounds": 1,
                                    "run_id": "scripted-run-profile",
                                    "dataset_id": "scripted-dataset-profile",
                                    "body": [
                                        {
                                            "linkedinUrl": profile_url,
                                            "publicIdentifier": "scripted-queued-resume",
                                            "firstName": "Scripted",
                                            "lastName": "Resume",
                                            "headline": "Research Engineer at OpenAI",
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
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = HarvestProfileConnector(
                HarvestActorSettings(enabled=True, api_token="token", actor_id="actor", default_mode="full")
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            request = JobRequest(raw_user_request="Find scripted resume", target_company="OpenAI")

            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                },
                clear=False,
            ):
                first = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=[profile_url],
                    snapshot_dir=root,
                    job_id="job_scripted_resume_queued_registry",
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                )
                second = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=[profile_url],
                    snapshot_dir=root,
                    job_id="job_scripted_resume_queued_registry",
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="daemon_recovery",
                    allow_shared_provider_cache=True,
                )
            registry = store.repos.linkedin_profile_registry.get_bulk([profile_url])
            workers = worker_runtime.list_workers(job_id="job_scripted_resume_queued_registry")

        self.assertEqual(first["worker_status"], "queued")
        self.assertEqual(second["worker_status"], "completed")
        self.assertEqual(dict(registry[normalize_linkedin_profile_url_key(profile_url)]).get("status"), "fetched")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["status"], "completed")

    def test_harvest_profile_batch_worker_recovers_remote_identity_from_queued_summary(self) -> None:
        class _ResumeConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.checkpoints: list[dict[str, object]] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                checkpoint_payload = dict(checkpoint or {})
                self.checkpoints.append(checkpoint_payload)
                if checkpoint_payload.get("run_id") != "run-summary-recovered":
                    raise AssertionError("remote run id must be recovered from queue summary before retry")
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **checkpoint_payload,
                        "run_id": "run-summary-recovered",
                        "dataset_id": "dataset-summary-recovered",
                        "status": "completed",
                    },
                    body=[
                        {
                            "linkedinUrl": profile_urls[0],
                            "profileUrl": profile_urls[0],
                            "publicIdentifier": "summary-recovered",
                            "firstName": "Summary",
                            "lastName": "Recovered",
                            "headline": "Research Engineer at OpenAI",
                        }
                    ],
                    message="completed from recovered summary identity",
                )

            def persist_profiles_from_batch_body(self, profile_urls, body, snapshot_dir, asset_logger=None):
                harvest_dir = Path(snapshot_dir) / "harvest_profiles"
                harvest_dir.mkdir(parents=True, exist_ok=True)
                raw_path = harvest_dir / "summary-recovered-profile.json"
                payload = dict(list(body or [{}])[0] or {})
                raw_path.write_text(json.dumps(payload), encoding="utf-8")
                return {
                    "profiles": {
                        profile_urls[0]: {
                            **payload,
                            "raw_path": str(raw_path),
                        }
                    },
                    "unresolved_urls": [],
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root / "anthropic",
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _ResumeConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            profile_url = "https://www.linkedin.com/in/summary-recovered/"
            payload_hash = sha1(
                json.dumps(sorted([profile_url]), ensure_ascii=False).encode("utf-8")
            ).hexdigest()[:16]
            request = JobRequest(raw_user_request="Find summary recovered", target_company="OpenAI")
            handle = worker_runtime.begin_worker(
                job_id="job_summary_recovered",
                request=request,
                plan_payload={},
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::{payload_hash}",
                stage="enriching",
                span_name=f"harvest_profile_batch:{payload_hash}",
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(root),
                    "profile_urls": [profile_url],
                    "request_payload": request.to_record(),
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                    "allow_shared_provider_cache": True,
                },
            )
            worker_runtime.checkpoint_worker(
                handle,
                status="running",
                checkpoint_payload={"stage": "submitting_remote_harvest"},
                output_payload={},
            )
            harvest_dir = root / "harvest_profiles"
            harvest_dir.mkdir(parents=True, exist_ok=True)
            pending_path = harvest_dir / f"harvest_profile_batch_{payload_hash}.queue_scripted_harvest_pending.json"
            pending_path.write_text(
                json.dumps(
                    {
                        "scripted_remote_wait_after_submit": True,
                        "scripted_remote_ready_epoch_ms": 1,
                    }
                ),
                encoding="utf-8",
            )
            summary_path = harvest_dir / f"harvest_profile_batch_{payload_hash}.queue_summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "logical_name": "harvest_profile_scraper_batch",
                        "status": "queued",
                        "run_id": "run-summary-recovered",
                        "dataset_id": "dataset-summary-recovered",
                        "payload_hash": payload_hash,
                        "requested_urls": [profile_url],
                        "queued_urls": [profile_url],
                        "artifact_paths": {"scripted_harvest_pending": str(pending_path)},
                    }
                ),
                encoding="utf-8",
            )

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=[profile_url],
                snapshot_dir=root,
                job_id="job_summary_recovered",
                request_payload=request.to_record(),
                plan_payload={},
                runtime_mode="daemon_recovery",
                allow_shared_provider_cache=True,
            )
            registry = store.repos.linkedin_profile_registry.get(profile_url) or {}
            worker = store.get_agent_worker(worker_id=handle.worker_id) or {}

        self.assertEqual(result["worker_status"], "completed")
        self.assertEqual(connector.checkpoints[0]["run_id"], "run-summary-recovered")
        self.assertEqual(registry["status"], "fetched")
        self.assertEqual(registry["refill_queue_state"], "")
        self.assertEqual(worker["status"], "completed")

    def test_harvest_profile_batch_worker_schedules_local_event_watcher_without_webhook(self) -> None:
        class _PendingConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.status_calls = 0

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-local-watch",
                        "dataset_id": "dataset-local-watch",
                        "status": "submitted",
                    },
                    pending=True,
                    message="Submitted test run.",
                )

            def get_actor_run_status(self, run_id, *, runtime_timing_overrides=None):
                self.status_calls += 1
                return {
                    "run_id": run_id,
                    "dataset_id": "dataset-local-watch",
                    "status": "SUCCEEDED",
                    "is_terminal": True,
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _PendingConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            callback_payloads: list[dict[str, object]] = []
            enricher.remote_provider_event_callback = lambda payload: callback_payloads.append(dict(payload or {}))
            request = JobRequest(raw_user_request="Find local watcher", target_company="OpenAI")

            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS": "5",
                },
                clear=False,
            ):
                result = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=["https://www.linkedin.com/in/local-watch/"],
                    snapshot_dir=root,
                    job_id="job_local_provider_watch",
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                )

            deadline = time.time() + 2
            while not callback_payloads and time.time() < deadline:
                time.sleep(0.01)

        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(result["local_provider_event_watcher"]["status"], "scheduled")
        self.assertGreaterEqual(connector.status_calls, 1)
        self.assertEqual(callback_payloads[0]["eventType"], "ACTOR.RUN.SUCCEEDED")
        self.assertEqual(callback_payloads[0]["eventData"]["actorRunId"], "run-local-watch")
        self.assertEqual(callback_payloads[0]["eventData"]["defaultDatasetId"], "dataset-local-watch")

    def test_pending_harvest_worker_watcher_uses_checkpoint_remote_ready_timing(self) -> None:
        class _PendingConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.status_contexts: list[dict[str, object]] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-scripted-ready",
                        "dataset_id": "dataset-scripted-ready",
                        "status": "submitted",
                        "scripted_remote_wait_after_submit": True,
                        "scripted_remote_wait_seconds": 10,
                        "scripted_remote_ready_epoch_ms": 1_010_000,
                    },
                    pending=True,
                    message="Submitted scripted test run.",
                )

            def get_actor_run_status(self, run_id, *, runtime_timing_overrides=None):
                self.status_contexts.append(dict(runtime_timing_overrides or {}))
                return {
                    "run_id": run_id,
                    "dataset_id": "dataset-scripted-ready",
                    "status": "SUCCEEDED",
                    "is_terminal": True,
                    "started_at": "1970-01-01T00:16:40.000+00:00",
                    "finished_at": "1970-01-01T00:16:50.000+00:00",
                    "remote_completed_at": "1970-01-01T00:16:50.000+00:00",
                    "raw": {
                        "startedAt": "1970-01-01T00:16:40.000+00:00",
                        "finishedAt": "1970-01-01T00:16:50.000+00:00",
                        "eventCreatedAt": "1970-01-01T00:16:50.000+00:00",
                    },
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _PendingConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            callback_payloads: list[dict[str, object]] = []
            enricher.remote_provider_event_callback = lambda payload: callback_payloads.append(dict(payload or {}))
            request = JobRequest(raw_user_request="Find local watcher", target_company="OpenAI")

            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_APIFY_WEBHOOK_URL": "",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS": "5",
                },
                clear=False,
            ):
                result = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=["https://www.linkedin.com/in/scripted-ready/"],
                    snapshot_dir=root,
                    job_id="job_scripted_provider_ready_watch",
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                )

            deadline = time.time() + 2
            while not callback_payloads and time.time() < deadline:
                time.sleep(0.01)

        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(result["local_provider_event_watcher"]["status"], "scheduled")
        self.assertTrue(connector.status_contexts)
        self.assertEqual(connector.status_contexts[0]["scripted_remote_ready_epoch_ms"], 1_010_000)
        self.assertEqual(connector.status_contexts[0]["scripted_remote_wait_seconds"], 10)
        event_data = dict(callback_payloads[0]["eventData"])
        self.assertEqual(event_data["startedAt"], "1970-01-01T00:16:40.000+00:00")
        self.assertEqual(event_data["finishedAt"], "1970-01-01T00:16:50.000+00:00")
        self.assertEqual(event_data["eventCreatedAt"], "1970-01-01T00:16:50.000+00:00")

    def test_pending_harvest_worker_reuses_active_local_provider_event_watcher(self) -> None:
        class _PendingConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.execute_calls = 0
                self.status_calls = 0

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                self.execute_calls += 1
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-scripted-watch-once",
                        "dataset_id": "dataset-scripted-watch-once",
                        "status": "submitted",
                        "scripted_remote_wait_after_submit": True,
                        "scripted_remote_wait_seconds": 600,
                        "scripted_remote_ready_epoch_ms": int((time.time() + 600) * 1000),
                    },
                    pending=True,
                    message="Submitted scripted test run.",
                )

            def get_actor_run_status(self, run_id, *, runtime_timing_overrides=None):
                self.status_calls += 1
                return {
                    "run_id": run_id,
                    "dataset_id": "dataset-scripted-watch-once",
                    "status": "RUNNING",
                    "is_terminal": False,
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _PendingConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            enricher.remote_provider_event_callback = lambda payload: None
            request = JobRequest(raw_user_request="Find local watcher", target_company="OpenAI")
            profile_url = "https://www.linkedin.com/in/scripted-watch-once/"

            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_APIFY_WEBHOOK_URL": "",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS": "30",
                },
                clear=False,
            ):
                first = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=[profile_url],
                    snapshot_dir=root,
                    job_id="job_scripted_provider_watch_once",
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                )
                second = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=[profile_url],
                    snapshot_dir=root,
                    job_id="job_scripted_provider_watch_once",
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                )

        self.assertEqual(first["worker_status"], "queued")
        self.assertEqual(first["local_provider_event_watcher"]["status"], "scheduled")
        self.assertEqual(second["worker_status"], "queued")
        self.assertEqual(second["local_provider_event_watcher"]["status"], "already_scheduled")
        watcher_lease = dict(second["local_provider_event_watcher"].get("watcher_lease") or {})
        self.assertTrue(
            _local_provider_event_watcher_lease_is_active(
                watcher_lease,
                run_id="run-scripted-watch-once",
                dataset_id="dataset-scripted-watch-once",
                worker_id=int(first["local_provider_event_watcher"]["worker_id"]),
            )
        )
        self.assertEqual(connector.execute_calls, 2)

    def test_local_provider_event_watcher_skips_callback_after_terminal_marker_seen(self) -> None:
        class _StatusConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def get_actor_run_status(self, run_id, *, runtime_timing_overrides=None):
                return {
                    "run_id": run_id,
                    "dataset_id": "dataset-terminal-before-callback",
                    "status": "SUCCEEDED",
                    "is_terminal": True,
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            worker = worker_runtime.begin_worker(
                job_id="job_terminal_before_callback",
                request=JobRequest(raw_user_request="Find local watcher", target_company="OpenAI"),
                plan_payload={},
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key="harvest_profile_batch::terminal-before-callback",
                stage="enriching",
                span_name="harvest_profile_batch:terminal-before-callback",
                budget_payload={},
                input_payload={},
                metadata={"recovery_kind": "harvest_profile_batch"},
            )
            store.checkpoint_agent_worker(
                worker.worker_id,
                status="queued",
                checkpoint_payload={
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-terminal-before-callback",
                    "dataset_id": "dataset-terminal-before-callback",
                    "remote_provider_terminal_event_seen_at": "2026-05-12T00:00:00+00:00",
                    "remote_provider_terminal_event": {
                        "run_id": "run-terminal-before-callback",
                        "dataset_id": "dataset-terminal-before-callback",
                        "is_terminal": True,
                    },
                },
                output_payload={},
            )
            connector = _StatusConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            callback_payloads: list[dict[str, object]] = []
            enricher.remote_provider_event_callback = lambda payload: callback_payloads.append(dict(payload or {}))
            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS": "5",
                },
                clear=False,
            ):
                watcher = enricher._schedule_local_provider_event_watcher(
                    run_id="run-terminal-before-callback",
                    dataset_id="dataset-terminal-before-callback",
                    worker_id=int(worker.worker_id),
                    job_id="job_terminal_before_callback",
                    payload_hash="terminal-before-callback",
                    snapshot_dir=root,
                    runtime_timing_overrides={},
                )
            time.sleep(0.15)

        self.assertEqual(watcher["status"], "scheduled")
        self.assertEqual(callback_payloads, [])

    def test_local_provider_event_callback_guard_runs_after_serial_wait(self) -> None:
        callback_payloads: list[dict[str, object]] = []
        guard_calls = 0
        allow_callback = False

        def _callback(payload: dict[str, object]) -> None:
            callback_payloads.append(dict(payload or {}))

        def _guard() -> bool:
            nonlocal guard_calls
            guard_calls += 1
            return allow_callback

        result = _invoke_local_provider_event_callback_serialized(
            _callback,
            {"source": "local_provider_event_watcher"},
            before_invoke=_guard,
        )

        self.assertEqual(callback_payloads, [])
        self.assertEqual(guard_calls, 1)
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "local_provider_event_callback_guard_rejected")

    def test_scripted_local_provider_event_watcher_uses_artifact_remote_identifiers(self) -> None:
        class _ArtifactOnlyPendingConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.status_run_ids: list[str] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "status": "submitted",
                    },
                    pending=True,
                    message="Submitted scripted test run.",
                    artifacts=[
                        HarvestExecutionArtifact(
                            label="run_get",
                            payload={
                                "data": {
                                    "id": "run-artifact-watch",
                                    "defaultDatasetId": "dataset-artifact-watch",
                                    "status": "RUNNING",
                                }
                            },
                        )
                    ],
                )

            def get_actor_run_status(self, run_id, *, runtime_timing_overrides=None):
                self.status_run_ids.append(str(run_id))
                return {
                    "run_id": run_id,
                    "dataset_id": "dataset-artifact-watch",
                    "status": "SUCCEEDED",
                    "is_terminal": True,
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _ArtifactOnlyPendingConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            callback_payloads: list[dict[str, object]] = []
            enricher.remote_provider_event_callback = lambda payload: callback_payloads.append(dict(payload or {}))
            request = JobRequest(raw_user_request="Find scripted watcher", target_company="OpenAI")

            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_APIFY_WEBHOOK_URL": "",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS": "5",
                },
                clear=False,
            ):
                result = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=["https://www.linkedin.com/in/artifact-watch/"],
                    snapshot_dir=root,
                    job_id="job_scripted_provider_watch",
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                )

            deadline = time.time() + 2
            while not callback_payloads and time.time() < deadline:
                time.sleep(0.01)

        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(result["local_provider_event_watcher"]["status"], "scheduled")
        self.assertEqual(connector.status_run_ids[:1], ["run-artifact-watch"])
        self.assertEqual(callback_payloads[0]["eventData"]["actorRunId"], "run-artifact-watch")
        self.assertEqual(callback_payloads[0]["eventData"]["defaultDatasetId"], "dataset-artifact-watch")

    def test_local_provider_event_watcher_runs_by_default_when_webhook_configured(self) -> None:
        class _StatusConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.status_calls = 0

            def get_actor_run_status(self, run_id, *, runtime_timing_overrides=None):
                self.status_calls += 1
                return {
                    "run_id": run_id,
                    "dataset_id": "dataset-webhook-default",
                    "status": "SUCCEEDED",
                    "is_terminal": True,
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            connector = _StatusConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
            )
            callback_payloads: list[dict[str, object]] = []
            enricher.remote_provider_event_callback = lambda payload: callback_payloads.append(dict(payload or {}))
            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "https://example.test/api/providers/apify/webhook",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS": "5",
                },
                clear=False,
            ):
                watcher = enricher._schedule_local_provider_event_watcher(
                    run_id="run-webhook-default",
                    dataset_id="dataset-webhook-default",
                    worker_id=123,
                    job_id="job-webhook-default",
                    payload_hash="payload",
                    snapshot_dir=root,
                    runtime_timing_overrides={},
                )
            deadline = time.time() + 2
            while not callback_payloads and time.time() < deadline:
                time.sleep(0.01)

        self.assertEqual(watcher["status"], "scheduled")
        self.assertTrue(watcher["provider_webhook_configured"])
        self.assertGreaterEqual(connector.status_calls, 1)
        self.assertEqual(callback_payloads[0]["eventType"], "ACTOR.RUN.SUCCEEDED")

    def test_local_provider_event_watcher_serializes_terminal_callbacks(self) -> None:
        class _StatusConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def get_actor_run_status(self, run_id, *, runtime_timing_overrides=None):
                return {
                    "run_id": run_id,
                    "dataset_id": f"dataset-{run_id}",
                    "status": "SUCCEEDED",
                    "is_terminal": True,
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_StatusConnector(),
            )
            callback_payloads: list[dict[str, object]] = []
            active_callbacks = 0
            max_active_callbacks = 0
            lock = threading.Lock()

            def _callback(payload: dict[str, object]) -> None:
                nonlocal active_callbacks, max_active_callbacks
                with lock:
                    active_callbacks += 1
                    max_active_callbacks = max(max_active_callbacks, active_callbacks)
                try:
                    time.sleep(0.05)
                    callback_payloads.append(dict(payload or {}))
                finally:
                    with lock:
                        active_callbacks -= 1

            enricher.remote_provider_event_callback = _callback
            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS": "5",
                    "SOURCING_LOCAL_PROVIDER_EVENT_CALLBACK_CONCURRENCY": "1",
                },
                clear=False,
            ):
                watchers = [
                    enricher._schedule_local_provider_event_watcher(
                        run_id=f"run-serialized-{index}",
                        dataset_id=f"dataset-run-serialized-{index}",
                        worker_id=index,
                        job_id="job-serialized-callbacks",
                        payload_hash=f"payload-{index}",
                        snapshot_dir=root,
                        runtime_timing_overrides={},
                    )
                    for index in (1, 2)
                ]
            deadline = time.time() + 2
            while len(callback_payloads) < 2 and time.time() < deadline:
                time.sleep(0.01)

        self.assertTrue(all(watcher["status"] == "scheduled" for watcher in watchers))
        self.assertEqual(len(callback_payloads), 2)
        self.assertEqual(max_active_callbacks, 1)
        serializations = [dict(payload.get("callback_serialization") or {}) for payload in callback_payloads]
        self.assertTrue(all(item.get("mode") == "bounded_local_provider_event_callback" for item in serializations))
        self.assertTrue(all(item.get("concurrency") == 1 for item in serializations))

    def test_local_provider_event_watcher_skips_by_default_in_scripted_mode(self) -> None:
        class _StatusConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.status_calls = 0

            def get_actor_run_status(self, run_id, *, runtime_timing_overrides=None):
                self.status_calls += 1
                return {
                    "run_id": run_id,
                    "dataset_id": "dataset-scripted-watch",
                    "status": "SUCCEEDED",
                    "is_terminal": True,
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            connector = _StatusConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
            )
            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                },
                clear=False,
            ):
                watcher = enricher._schedule_local_provider_event_watcher(
                    run_id="run-scripted-watch",
                    dataset_id="dataset-scripted-watch",
                    worker_id=123,
                    job_id="job-scripted-watch",
                    payload_hash="payload",
                    snapshot_dir=root,
                    runtime_timing_overrides={},
                )

        self.assertEqual(watcher["status"], "skipped")
        self.assertEqual(watcher["reason"], "scripted_local_provider_event_watch_disabled")
        self.assertEqual(connector.status_calls, 0)

    def test_local_provider_event_watcher_can_disable_webhook_fallback(self) -> None:
        class _StatusConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.status_calls = 0

            def get_actor_run_status(self, run_id, *, runtime_timing_overrides=None):
                self.status_calls += 1
                return {
                    "run_id": run_id,
                    "dataset_id": "dataset-webhook-disabled",
                    "status": "SUCCEEDED",
                    "is_terminal": True,
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            connector = _StatusConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
            )
            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "https://example.test/api/providers/apify/webhook",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED": "0",
                },
                clear=False,
            ):
                watcher = enricher._schedule_local_provider_event_watcher(
                    run_id="run-webhook-disabled",
                    dataset_id="dataset-webhook-disabled",
                    worker_id=123,
                    job_id="job-webhook-disabled",
                    payload_hash="payload",
                    snapshot_dir=root,
                    runtime_timing_overrides={},
                )

        self.assertEqual(watcher["status"], "skipped")
        self.assertEqual(watcher["reason"], "provider_webhook_configured")
        self.assertEqual(connector.status_calls, 0)

    def test_local_provider_event_watcher_can_opt_into_webhook_fallback(self) -> None:
        class _StatusConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.status_calls = 0

            def get_actor_run_status(self, run_id, *, runtime_timing_overrides=None):
                self.status_calls += 1
                return {
                    "run_id": run_id,
                    "dataset_id": "dataset-webhook-fallback",
                    "status": "SUCCEEDED",
                    "is_terminal": True,
                }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            connector = _StatusConnector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
            )
            callback_payloads: list[dict[str, object]] = []
            enricher.remote_provider_event_callback = lambda payload: callback_payloads.append(dict(payload or {}))
            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_APIFY_WEBHOOK_URL": "https://example.test/api/providers/apify/webhook",
                    "APIFY_WEBHOOK_URL": "",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED": "1",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED": "1",
                    "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS": "5",
                },
                clear=False,
            ):
                watcher = enricher._schedule_local_provider_event_watcher(
                    run_id="run-webhook-fallback",
                    dataset_id="dataset-webhook-fallback",
                    worker_id=123,
                    job_id="job-webhook-fallback",
                    payload_hash="payload",
                    snapshot_dir=root,
                    runtime_timing_overrides={},
                )

            deadline = time.time() + 2
            while not callback_payloads and time.time() < deadline:
                time.sleep(0.01)

        self.assertEqual(watcher["status"], "scheduled")
        self.assertTrue(watcher["provider_webhook_configured"])
        self.assertGreaterEqual(connector.status_calls, 1)
        self.assertEqual(callback_payloads[0]["eventType"], "ACTOR.RUN.SUCCEEDED")

    def test_queue_background_profile_prefetch_defers_when_active_batch_worker_exhausts_budget(self) -> None:
        class _FailIfSubmittedConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("backpressure should defer without submitting a provider actor")

        class _WorkerRuntime:
            def __init__(self, snapshot_dir: Path) -> None:
                self.snapshot_dir = snapshot_dir

            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return [
                    {
                        "job_id": job_id,
                        "lane_id": lane_id,
                        "status": "queued",
                        "metadata": {
                            "recovery_kind": "harvest_profile_batch",
                            "snapshot_dir": str(self.snapshot_dir),
                        },
                        "checkpoint": {"stage": "waiting_remote_harvest"},
                    }
                ]

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_FailIfSubmittedConnector(),
            )
            enricher.worker_runtime = _WorkerRuntime(root)
            candidates = [
                Candidate(
                    candidate_id=f"cand_{index}",
                    name_en=f"Candidate {index}",
                    display_name=f"Candidate {index}",
                    linkedin_url=f"https://www.linkedin.com/in/deferred-{index}/",
                )
                for index in range(2)
            ]
            result = enricher.queue_background_profile_prefetch(
                candidates=candidates,
                snapshot_dir=root,
                job_id="job_backpressure",
                request_payload={"harvest_profile_batch_submit_global_inflight": 1},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["reason"], "harvest_profile_prefetch_backpressure")
        self.assertEqual(result["active_worker_count"], 1)
        self.assertEqual(result["submit_budget"], 1)
        self.assertEqual(result["dispatched_url_count"], 0)
        self.assertEqual(result["deferred_url_count"], 2)

    def test_queue_background_profile_prefetch_defaults_submit_budget_to_actor_budget(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _WorkerRuntime:
            def __init__(self, snapshot_dir: Path) -> None:
                self.snapshot_dir = snapshot_dir

            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return [
                    {
                        "job_id": job_id,
                        "lane_id": lane_id,
                        "status": "running",
                        "metadata": {
                            "recovery_kind": "harvest_profile_batch",
                            "snapshot_dir": str(self.snapshot_dir),
                        },
                        "checkpoint": {
                            "stage": "waiting_remote_harvest",
                            "requested_urls": ["https://www.linkedin.com/in/already-active/"],
                        },
                    }
                ]

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = _WorkerRuntime(root)
            dispatched_urls: list[str] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_urls.extend(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / "tail.queue_summary.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            result = enricher.queue_background_profile_prefetch(
                candidates=[
                    Candidate(
                        candidate_id="tail_candidate",
                        name_en="Tail Candidate",
                        display_name="Tail Candidate",
                        linkedin_url="https://www.linkedin.com/in/new-tail-candidate/",
                    )
                ],
                snapshot_dir=root,
                job_id="job_tail_overlap",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["active_worker_count"], 1)
        self.assertEqual(result["actor_budget"], 4)
        self.assertEqual(result["submit_budget"], 4)
        self.assertEqual(result["queued_worker_count"], 1)
        self.assertEqual(dispatched_urls, ["https://www.linkedin.com/in/new-tail-candidate/"])

    def test_active_harvest_profile_budget_ignores_pre_submit_worker_without_remote_slot(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _WorkerRuntime:
            def __init__(self, snapshot_dir: Path) -> None:
                self.snapshot_dir = snapshot_dir

            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return [
                    {
                        "job_id": job_id,
                        "lane_id": lane_id,
                        "status": "running",
                        "metadata": {
                            "recovery_kind": "harvest_profile_batch",
                            "snapshot_dir": str(self.snapshot_dir),
                        },
                        "checkpoint": {},
                        "output": {},
                    }
                ]

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector())
            enricher.worker_runtime = _WorkerRuntime(root)

            budget = enricher._harvest_profile_prefetch_new_worker_budget(
                job_id="job_pre_submit_worker",
                snapshot_dir=root,
                runtime_tuning_context={"harvest_profile_actor_global_inflight": 1},
                default_submit_budget=1,
            )

        self.assertEqual(budget["active_worker_count"], 0)
        self.assertEqual(budget["available_new_worker_count"], 1)

    def test_active_harvest_profile_budget_ignores_failed_remote_wait_worker(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _WorkerRuntime:
            def __init__(self, snapshot_dir: Path) -> None:
                self.snapshot_dir = snapshot_dir

            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return [
                    {
                        "job_id": job_id,
                        "lane_id": lane_id,
                        "status": "failed",
                        "metadata": {
                            "recovery_kind": "harvest_profile_batch",
                            "snapshot_dir": str(self.snapshot_dir),
                        },
                        "checkpoint": {
                            "stage": "waiting_remote_harvest",
                            "run_id": "run-failed-local-recovery",
                            "provider_limiter_lease": {"lease_token": "lease-failed"},
                        },
                        "output": {"daemon_error": "local recovery callback failed"},
                    }
                ]

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector())
            enricher.worker_runtime = _WorkerRuntime(root)

            budget = enricher._harvest_profile_prefetch_new_worker_budget(
                job_id="job_failed_remote_wait_worker",
                snapshot_dir=root,
                runtime_tuning_context={"harvest_profile_actor_global_inflight": 1},
                default_submit_budget=1,
            )

        self.assertEqual(budget["active_worker_count"], 0)
        self.assertEqual(budget["available_new_worker_count"], 1)

    def test_active_harvest_profile_budget_ignores_remote_terminal_event_worker(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _WorkerRuntime:
            def __init__(self, snapshot_dir: Path) -> None:
                self.snapshot_dir = snapshot_dir

            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return [
                    {
                        "job_id": job_id,
                        "lane_id": lane_id,
                        "status": "queued",
                        "metadata": {
                            "recovery_kind": "harvest_profile_batch",
                            "snapshot_dir": str(self.snapshot_dir),
                        },
                        "checkpoint": {
                            "stage": "waiting_remote_harvest",
                            "run_id": "run-terminal-seen",
                            "provider_limiter_lease": {"lease_token": "lease-terminal-seen"},
                            "remote_provider_terminal_event_seen_at": "2026-05-08T00:00:00+00:00",
                            "remote_provider_terminal_event": {"is_terminal": True},
                        },
                        "output": {},
                    }
                ]

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector())
            enricher.worker_runtime = _WorkerRuntime(root)

            budget = enricher._harvest_profile_prefetch_new_worker_budget(
                job_id="job_terminal_event_slot",
                snapshot_dir=root,
                runtime_tuning_context={"harvest_profile_actor_global_inflight": 1},
                default_submit_budget=1,
            )

        self.assertEqual(budget["active_worker_count"], 0)
        self.assertEqual(budget["available_new_worker_count"], 1)

    def test_harvest_profile_batch_resume_preserves_prefetch_context(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.checkpoints: list[dict] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                self.checkpoints.append(dict(checkpoint or {}))
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-existing",
                        "dataset_id": "dataset-existing",
                    },
                    pending=True,
                    message="existing remote run still pending",
                    artifacts=[],
                )

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _Connector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                worker_runtime=worker_runtime,
                store=store,
            )
            profile_urls = [
                f"https://www.linkedin.com/in/resume-context-{index:02d}/"
                for index in range(50)
            ]
            payload_hash = sha1(
                json.dumps(sorted(profile_urls), ensure_ascii=False).encode("utf-8")
            ).hexdigest()[:16]
            request = JobRequest(raw_user_request="Find OpenAI agents", target_company="OpenAI")
            handle = worker_runtime.begin_worker(
                job_id="job_resume_context",
                request=request,
                plan_payload={},
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::{payload_hash}",
                stage="enriching",
                span_name=f"harvest_profile_batch:{payload_hash}",
                budget_payload={"requested_url_count": len(profile_urls)},
                input_payload={"profile_urls": profile_urls},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(root),
                    "profile_urls": profile_urls,
                    "prefetch_batch_context": {"requested_url_count": 50},
                },
                handoff_from_lane="acquisition_specialist",
            )
            worker_runtime.checkpoint_worker(
                handle,
                status="queued",
                checkpoint_payload={
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-existing",
                    "dataset_id": "dataset-existing",
                    "provider_limiter_lease": {
                        "db_limiter_enabled": False,
                        "lease_token": "lease-existing",
                        "limiter_key": "harvest_profile_scraper_actor",
                    },
                    "prefetch_batch_context": {
                        "requested_url_count": 50,
                        "candidate_count": 50,
                        "planned_deferred_url_count": 0,
                        "planned_dispatch_worker_count": 1,
                        "nonblocking_submit": True,
                    },
                },
                output_payload={"summary": {"queued_urls": profile_urls}},
            )

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=profile_urls,
                snapshot_dir=root,
                job_id="job_resume_context",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                prefetch_batch_context={},
            )
            refreshed = store.get_agent_worker(worker_id=handle.worker_id) or {}

        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(connector.checkpoints[0]["prefetch_batch_context"]["candidate_count"], 50)
        self.assertTrue(connector.checkpoints[0]["prefetch_batch_context"]["nonblocking_submit"])
        self.assertEqual(
            dict(refreshed.get("checkpoint") or {}).get("prefetch_batch_context", {}).get("candidate_count"),
            50,
        )

    def test_queue_background_profile_prefetch_fills_available_actor_slots(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _WorkerRuntime:
            def __init__(self, snapshot_dir: Path) -> None:
                self.snapshot_dir = snapshot_dir

            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return [
                    {
                        "job_id": job_id,
                        "lane_id": lane_id,
                        "status": "queued",
                        "metadata": {
                            "recovery_kind": "harvest_profile_batch",
                            "snapshot_dir": str(self.snapshot_dir),
                        },
                        "checkpoint": {
                            "stage": "waiting_remote_harvest",
                            "run_id": f"active-run-{index}",
                        },
                        "output": {},
                    }
                    for index in range(2)
                ]

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = _WorkerRuntime(root)
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / f"chunk-{len(dispatched_chunks)}.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            candidates = [
                Candidate(
                    candidate_id=f"candidate_{index}",
                    name_en=f"Candidate {index}",
                    display_name=f"Candidate {index}",
                    linkedin_url=f"https://www.linkedin.com/in/fill-slot-{index}/",
                )
                for index in range(600)
            ]
            result = enricher.queue_background_profile_prefetch(
                candidates=candidates,
                snapshot_dir=root,
                job_id="job_fill_available_slots",
                request_payload={
                    "harvest_profile_actor_global_inflight": 4,
                    "harvest_profile_batch_submit_global_inflight": 4,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["active_worker_count"], 2)
        self.assertEqual(result["actor_budget"], 4)
        self.assertEqual(result["submit_budget"], 4)
        self.assertEqual(result["queued_worker_count"], 2)
        self.assertEqual(len(dispatched_chunks), 2)
        self.assertGreater(int(result.get("deferred_url_count") or 0), 0)

    def test_queue_background_profile_prefetch_records_typed_refill_submit_command(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _WorkerRuntime:
            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return []

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = _WorkerRuntime()
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "worker_id": 101,
                        "run_id": "run-typed-command",
                        "dataset_id": "dataset-typed-command",
                        "payload_hash": "payload-typed-command",
                        "summary_path": str(root / "typed-command-summary.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            candidates = [
                Candidate(
                    candidate_id=f"typed_{index}",
                    name_en=f"Typed {index}",
                    display_name=f"Typed {index}",
                    linkedin_url=f"https://www.linkedin.com/in/typed-refill-{index}/",
                )
                for index in range(60)
            ]

            result = enricher.queue_background_profile_prefetch(
                candidates=candidates,
                snapshot_dir=root,
                job_id="job_typed_refill_submit",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                load_cached_profile_payloads=False,
                dispatch_worker_limit=1,
            )

            workflow_run_id = legacy_job_workflow_run_id("job_typed_refill_submit")
            commands = store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=0)
            events = store.repos.workflow_runtime.list_workflow_events(workflow_run_id, limit=0)

        self.assertEqual(len(dispatched_chunks), 1)
        self.assertEqual(result["queued_worker_count"], 1)
        self.assertEqual(result["workflow_command_count"], 1)
        self.assertEqual(result["workflow_command_status_counts"], {"succeeded": 1})
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["command_type"], LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE)
        self.assertEqual(commands[0]["owner"], "linkedin_profile_owner")
        self.assertEqual(commands[0]["status"], "succeeded")
        self.assertEqual(commands[0]["result"]["owner_run_id"], "run-typed-command")
        self.assertEqual(
            [event["event_type"] for event in events],
            ["WorkflowStarted", "CommandPlanRequested"],
        )

    def test_queue_background_profile_prefetch_does_not_resubmit_when_typed_command_is_owned(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _WorkerRuntime:
            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return []

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = _WorkerRuntime()
            calls: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                calls.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "worker_id": 102,
                        "run_id": f"run-owned-{len(calls)}",
                        "dataset_id": f"dataset-owned-{len(calls)}",
                        "payload_hash": f"payload-owned-{len(calls)}",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            candidates = [
                Candidate(
                    candidate_id=f"owned_{index}",
                    name_en=f"Owned {index}",
                    display_name=f"Owned {index}",
                    linkedin_url=f"https://www.linkedin.com/in/owned-refill-{index}/",
                )
                for index in range(60)
            ]

            expected_chunk_urls = [
                f"https://www.linkedin.com/in/owned-refill-{index}/"
                for index in range(60)
            ]
            workflow_run_id = legacy_job_workflow_run_id("job_typed_refill_owned")
            command = store.upsert_workflow_command(
                workflow_run_id=workflow_run_id,
                operation_id=legacy_job_operation_id("job_typed_refill_owned"),
                command_type=LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
                owner="linkedin_profile_owner",
                idempotency_key=linkedin_profile_refill_submit_idempotency_key(
                    job_id="job_typed_refill_owned",
                    snapshot_dir=str(root),
                    profile_urls=expected_chunk_urls,
                    submit_scope="ready_to_dispatch",
                ),
                payload={
                    "job_id": "job_typed_refill_owned",
                    "snapshot_dir": str(root),
                    "chunk_index": 0,
                    "profile_url_count": len(expected_chunk_urls),
                    "profile_urls": expected_chunk_urls,
                    "requested_url_count": len(expected_chunk_urls),
                    "candidate_count": len(expected_chunk_urls),
                    "batch_plan_reason": "ready_to_dispatch",
                    "submit_scope": "ready_to_dispatch",
                    "request_payload": {
                        "harvest_profile_actor_global_inflight": 1,
                        "harvest_profile_batch_submit_global_inflight": 1,
                    },
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                    "allow_shared_provider_cache": True,
                    "load_cached_profile_payloads": False,
                },
            )
            claimed = store.claim_workflow_command(
                command["command_id"],
                lease_owner="linkedin_profile_owner:test-running-command",
                lease_seconds=7200,
            )
            store.mark_workflow_command_running(
                command["command_id"],
                lease_owner=str(claimed.get("lease_owner") or "linkedin_profile_owner:test-running-command"),
            )
            result = enricher.queue_background_profile_prefetch(
                candidates=candidates,
                snapshot_dir=root,
                job_id="job_typed_refill_owned",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                load_cached_profile_payloads=False,
                dispatch_worker_limit=1,
            )
            commands = store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=0)

        self.assertEqual(len(calls), 0)
        self.assertEqual(result["queued_worker_count"], 1)
        self.assertEqual(result["dispatched_url_count"], 0)
        self.assertEqual(result["workflow_command_status_counts"], {"running": 1})
        self.assertTrue(dict(result["workflow_commands"][0]).get("runtime_command_contention"))
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["status"], "running")

    def test_linkedin_profile_owner_can_execute_ready_submit_command_directly(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _WorkerRuntime:
            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return []

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = _WorkerRuntime()
            profile_urls = [
                f"https://www.linkedin.com/in/direct-owner-{index}/"
                for index in range(3)
            ]
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls_payload = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls_payload)
                self.assertEqual(kwargs.get("runtime_mode"), "daemon_refill")
                self.assertFalse(kwargs.get("load_cached_profile_payloads"))
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls_payload,
                        "worker_id": 203,
                        "run_id": "run-direct-owner",
                        "dataset_id": "dataset-direct-owner",
                        "payload_hash": "payload-direct-owner",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            workflow_run_id = legacy_job_workflow_run_id("job_direct_owner")
            command = store.upsert_workflow_command(
                workflow_run_id=workflow_run_id,
                operation_id=legacy_job_operation_id("job_direct_owner"),
                command_type=LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
                owner="linkedin_profile_owner",
                idempotency_key=linkedin_profile_refill_submit_idempotency_key(
                    job_id="job_direct_owner",
                    snapshot_dir=str(root),
                    profile_urls=profile_urls,
                    submit_scope="ready_to_dispatch",
                ),
                payload={
                    "job_id": "job_direct_owner",
                    "snapshot_dir": str(root),
                    "chunk_index": 0,
                    "profile_urls": profile_urls,
                    "requested_url_count": len(profile_urls),
                    "candidate_count": len(profile_urls),
                    "batch_plan_reason": "ready_to_dispatch",
                    "request_payload": {
                        "harvest_profile_actor_global_inflight": 1,
                        "harvest_profile_batch_submit_global_inflight": 1,
                    },
                    "plan_payload": {},
                    "runtime_mode": "daemon_refill",
                    "allow_shared_provider_cache": True,
                    "load_cached_profile_payloads": False,
                },
            )

            result = enricher.run_linkedin_profile_refill_submit_command_once(command)
            refreshed_command = store.get_workflow_command(command["command_id"])

        self.assertEqual(dispatched_chunks, [profile_urls])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["workflow_command"]["status"], "succeeded")
        self.assertEqual(refreshed_command["status"], "succeeded")
        self.assertEqual(refreshed_command["result"]["owner_run_id"], "run-direct-owner")

    def test_linkedin_profile_owner_records_url_terminal_state_from_typed_command(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            fetched_url = "https://www.linkedin.com/in/terminal-owner-fetched/"
            failed_url = "https://www.linkedin.com/in/terminal-owner-failed/"
            raw_path = root / "harvest_profiles" / "terminal-owner-fetched.json"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(
                json.dumps({"item": {"linkedinUrl": fetched_url}}, ensure_ascii=False),
                encoding="utf-8",
            )
            entries = [
                {
                    "profile_url": fetched_url,
                    "status": "fetched",
                    "raw_path": str(raw_path),
                    "source_shards": ["enrichment_background_prefetch"],
                    "source_jobs": ["job_terminal_owner"],
                    "run_id": "run-terminal-owner",
                    "dataset_id": "dataset-terminal-owner",
                    "snapshot_dir": str(root),
                },
                {
                    "profile_url": failed_url,
                    "status": "failed_retryable",
                    "error": "background_prefetch_unresolved",
                    "retryable": True,
                    "source_shards": ["enrichment_background_prefetch"],
                    "source_jobs": ["job_terminal_owner"],
                    "run_id": "run-terminal-owner",
                    "dataset_id": "dataset-terminal-owner",
                    "snapshot_dir": str(root),
                },
            ]
            workflow_run_id = legacy_job_workflow_run_id("job_terminal_owner")
            command = store.upsert_workflow_command(
                workflow_run_id=workflow_run_id,
                operation_id=legacy_job_operation_id("job_terminal_owner"),
                command_type=LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
                owner="linkedin_profile_owner",
                idempotency_key=linkedin_profile_url_terminal_record_idempotency_key(
                    job_id="job_terminal_owner",
                    snapshot_dir=str(root),
                    entries=entries,
                    terminal_scope="unit-test",
                ),
                payload={
                    "job_id": "job_terminal_owner",
                    "snapshot_dir": str(root),
                    "terminal_scope": "unit-test",
                    "entries": entries,
                },
            )

            result = enricher.run_linkedin_profile_url_terminal_record_command_once(command)
            replay = enricher.run_linkedin_profile_url_terminal_record_command_once(command)
            fetched_entry = store.repos.linkedin_profile_registry.get(fetched_url) or {}
            failed_entry = store.repos.linkedin_profile_registry.get(failed_url) or {}
            refreshed_command = store.get_workflow_command(command["command_id"])

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["recorded_count"], 2)
        self.assertEqual(replay["status"], "completed")
        self.assertEqual(replay["reason"], "typed_command_already_succeeded")
        self.assertEqual(refreshed_command["status"], "succeeded")
        self.assertEqual(fetched_entry["status"], "fetched")
        self.assertEqual(fetched_entry["last_raw_path"], str(raw_path))
        self.assertEqual(failed_entry["status"], "failed_retryable")
        self.assertEqual(failed_entry["refill_queue_state"], "retry_wait")

    def test_queue_background_profile_prefetch_records_batch_envelopes_and_underuse(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _WorkerRuntime:
            def __init__(self, snapshot_dir: Path) -> None:
                self.snapshot_dir = snapshot_dir

            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return []

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = _WorkerRuntime(root)
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / f"chunk-{len(dispatched_chunks)}.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            candidates = [
                Candidate(
                    candidate_id=f"infra_{index}",
                    name_en=f"Infra {index}",
                    display_name=f"Infra {index}",
                    linkedin_url=f"https://www.linkedin.com/in/openai-infra-{index}/",
                    source_dataset="openai_search_seed_candidates",
                    metadata={"seed_source_type": "harvest_profile_search"},
                )
                for index in range(180)
            ]

            with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "live"}, clear=False):
                result = enricher.queue_background_profile_prefetch(
                    candidates=candidates,
                    snapshot_dir=root,
                    job_id="job_openai_infra_underuse_shape",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 4,
                        "harvest_profile_batch_submit_global_inflight": 2,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                )

        # R1.b ("fewer larger envelopes"): the 180-url profile-search set now packs into a
        # single durable-unit envelope dispatched by one worker, instead of the old 2-way
        # 50-slot fan-out. The underuse semantics this test pins are re-derived against the
        # new envelope count rather than deleted: provider-slot underuse is "idle actor
        # slots WITH a deferred backlog", and a single complete envelope with zero deferral
        # is correctly NOT flagged as underuse (no false positive), so the underuse count
        # is 0 and no envelope carries an underuse reason.
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["queued_worker_count"], 1)
        self.assertEqual(int(result.get("deferred_url_count") or 0), 0)
        envelopes = list(result.get("batch_envelopes") or [])
        self.assertEqual(len(envelopes), 1)
        self.assertEqual(result["recommended_batch_size"], 180)
        self.assertEqual(result["provider_slot_underuse_with_backlog_count"], 0)
        self.assertFalse(any(dict(item).get("provider_slot_underuse_with_backlog") for item in envelopes))
        self.assertTrue(all(str(dict(item).get("underuse_reason") or "") == "" for item in envelopes))

    def test_queue_background_profile_prefetch_reports_profile_queue_snapshot(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        # D2 (test-infra only): the new durable scheduler needs the full typed-command /
        # refill-item-ownership surface to actually dispatch. The hand-written stub only
        # modelled the two registry-shape rows this test cares about (one fetched/cached,
        # one already-queued), so it never satisfied the durable-command writes and the
        # plan never dispatched. We keep the bespoke registry-row injection but delegate
        # every other method (workflow commands, activity runs, durable events, refill-item
        # ownership) to a real PG store so the scheduler can form and own a wave.
        class _Store:
            def __init__(self, cached_url: str, queued_url: str, raw_path: Path, backing) -> None:
                self.cached_url = cached_url
                self.queued_url = queued_url
                self.raw_path = raw_path
                self._backing = backing
                self.repos = SimpleNamespace(
                    linkedin_profile_registry=self,
                    workflow_runtime=backing.repos.workflow_runtime,
                )

            def get_bulk(self, profile_urls):
                rows = {}
                for profile_url in list(profile_urls or []):
                    key = normalize_linkedin_profile_url_key(profile_url)
                    if profile_url == self.cached_url:
                        rows[key] = {
                            "profile_url": profile_url,
                            "status": "fetched",
                            "last_raw_path": str(self.raw_path),
                        }
                    elif profile_url == self.queued_url:
                        rows[key] = {
                            "profile_url": profile_url,
                            "status": "queued",
                            "last_run_id": "run-already-queued",
                            "last_queued_at": "2026-05-02 00:00:00",
                        }
                return rows

            def upsert_sources(self, *args, **kwargs):
                return {}

            def __getattr__(self, name):
                # Delegate the durable typed-command / refill-item surface (and anything
                # else the scheduler reaches for) to the backing PG store, EXCEPT the
                # worker-state scan methods. The original hand-written stub exposed no
                # worker-listing surface, so the partition logic took its conservative
                # "cannot inspect worker state -> treat the queued row as already-active"
                # path. Keep that surface absent so the already-queued row is still
                # detected as already queued rather than reclaimed for a duplicate submit.
                if name in {
                    "list_agent_workers",
                    "list_agent_workers_by_remote_provider_identifiers",
                }:
                    raise AttributeError(name)
                backing = self.__dict__["_backing"]
                try:
                    return getattr(backing, name)
                except AttributeError:
                    return getattr(backing.repos.linkedin_profile_registry, name)

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            raw_path = root / "cached-profile.json"
            raw_path.write_text("{}", encoding="utf-8")
            cached_url = "https://www.linkedin.com/in/queue-cached/"
            queued_url = "https://www.linkedin.com/in/queue-already-active/"
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            backing_store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=_Store(cached_url, queued_url, raw_path, backing_store),
            )
            enricher.worker_runtime = object()
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / f"queue-{len(dispatched_chunks)}.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            candidates = [
                Candidate(
                    candidate_id=f"queue_{index}",
                    name_en=f"Queue {index}",
                    display_name=f"Queue {index}",
                    linkedin_url=(
                        cached_url
                        if index == 0
                        else queued_url
                        if index == 1
                        else f"https://www.linkedin.com/in/queue-ready-{index}/"
                    ),
                )
                for index in range(60)
            ]

            with mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window:
                window.return_value = {
                    "batch_size": 10,
                    "max_workers": 1,
                    "batch_count": 6,
                    "source_mix": {"other": 58},
                    "strategy": "test_queue_snapshot",
                    "priority": False,
                }
                result = enricher.queue_background_profile_prefetch(
                    candidates=candidates,
                    snapshot_dir=root,
                    job_id="job_profile_queue_snapshot",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 1,
                        "harvest_profile_batch_submit_global_inflight": 1,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    load_cached_profile_payloads=False,
                )

        self.assertEqual(result["status"], "queued")
        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [50])
        queue = dict(result.get("profile_prefetch_queue") or {})
        self.assertEqual(queue["kind"], "linkedin_profile_prefetch_queue")
        self.assertEqual(queue["item_store"], "linkedin_profile_registry")
        self.assertEqual(queue["requested_url_count"], 60)
        self.assertEqual(queue["cached_url_count"], 1)
        self.assertEqual(queue["already_queued_url_count"], 1)
        self.assertEqual(queue["ready_url_count"], 58)
        self.assertEqual(queue["newly_queued_url_count"], 50)
        self.assertEqual(queue["queued_url_count"], 51)
        self.assertEqual(queue["deferred_url_count"], 8)
        self.assertEqual(queue["pending_url_count"], 59)
        self.assertFalse(queue["local_queue_quiescent"])
        self.assertFalse(queue["queue_quiescent"])
        self.assertEqual(queue["registry_status_counts"], {"fetched": 1, "queued": 1})
        self.assertEqual(queue["batch_envelope_count"], 1)

    def test_provider_owned_planned_dispatch_is_not_reclaimed_for_duplicate_submit(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            profile_url = "https://www.linkedin.com/in/provider-owned-planned-dispatch/"
            job_id = "job_provider_owned_planned_dispatch"
            store.repos.linkedin_profile_registry.mark_queued(
                profile_url,
                source_jobs=[job_id],
                snapshot_dir=str(root),
                run_id="run-provider-owned",
                dataset_id="dataset-provider-owned",
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=[profile_url],
                source_jobs=[job_id],
                snapshot_dir=str(root),
                trigger_kind="profile_prefetch_provider_submit",
                plan_reason="remote_provider_submitted",
                active_queue_state="planned_dispatch",
                active_owner_worker_id=42,
                active_owner_run_id="run-provider-owned",
                active_owner_dataset_id="dataset-provider-owned",
                active_owner_payload_hash="payload-provider-owned",
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = object()

            dispatch_urls, already_queued_urls = enricher._partition_already_queued_profile_urls(
                [profile_url],
                snapshot_dir=root,
                source_jobs=[job_id],
            )

        self.assertEqual(dispatch_urls, [])
        self.assertEqual(already_queued_urls, [profile_url])

    def test_harvest_profile_batch_worker_keeps_provider_owned_race_queued(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("provider-owned URLs must not submit a duplicate Harvest actor run")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            profile_urls = [
                f"https://www.linkedin.com/in/provider-owned-race-{index:03d}/"
                for index in range(20)
            ]
            job_id = "job_provider_owned_race"
            original_get_bulk = store.repos.linkedin_profile_registry.get_bulk
            original_get_lease = store.repos.linkedin_profile_registry.get_lease

            def _get_bulk_with_provider_owner_race(profile_urls_arg):
                urls = [str(profile_url or "").strip() for profile_url in list(profile_urls_arg or [])]
                if urls and all(
                    str(dict(original_get_lease(profile_url) or {}).get("lease_owner") or "").startswith(
                        "background_prefetch:"
                    )
                    for profile_url in urls
                ):
                    return {
                        normalize_linkedin_profile_url_key(profile_url): {
                            "status": "queued",
                            "source_jobs": [job_id],
                            "last_snapshot_dir": str(root),
                            "refill_queue_state": "planned_dispatch",
                            "refill_owner_worker_id": 77,
                            "refill_owner_run_id": "run-provider-owned-race",
                            "refill_owner_dataset_id": "dataset-provider-owned-race",
                            "refill_owner_payload_hash": "payload-provider-owned-race",
                        }
                        for profile_url in urls
                    }
                return original_get_bulk(profile_urls_arg)

            store.repos.linkedin_profile_registry.get_bulk = _get_bulk_with_provider_owner_race
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = object()

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=profile_urls,
                snapshot_dir=root,
                job_id=job_id,
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                load_cached_profile_payloads=False,
                prefetch_batch_context={
                    "requested_url_count": 25,
                    "candidate_count": 25,
                    "planned_deferred_url_count": 0,
                    "planned_dispatch_worker_count": 1,
                    "allow_under_target_final_tail_dispatch": True,
                },
            )

        summary = dict(result.get("summary") or {})
        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(summary["message"], "profile_urls_already_queued_or_claimed")
        self.assertEqual(summary["queued_urls"], profile_urls)
        self.assertEqual(summary["deferred_urls"], [])
        self.assertEqual(summary["already_queued_url_count"], 20)

    def test_harvest_profile_batch_worker_keeps_provider_owned_lease_contention_queued(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("provider-owned lease contention must not submit a duplicate actor run")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            profile_urls = [
                f"https://www.linkedin.com/in/provider-owned-contended-{index:03d}/"
                for index in range(20)
            ]
            job_id = "job_provider_owned_contended"
            original_get_bulk = store.repos.linkedin_profile_registry.get_bulk
            batch_acquire_started = False

            def _get_bulk_with_provider_owner_after_contention(profile_urls_arg):
                if not batch_acquire_started:
                    return original_get_bulk(profile_urls_arg)
                return {
                    normalize_linkedin_profile_url_key(profile_url): {
                        "status": "queued",
                        "source_jobs": [job_id],
                        "last_snapshot_dir": str(root),
                        "refill_queue_state": "planned_dispatch",
                        "refill_owner_worker_id": 88,
                        "refill_owner_run_id": "run-provider-owned-contended",
                        "refill_owner_dataset_id": "dataset-provider-owned-contended",
                        "refill_owner_payload_hash": "payload-provider-owned-contended",
                    }
                    for profile_url in list(profile_urls_arg or [])
                }

            def _contended_batch_acquire(profile_urls_arg, **kwargs):
                nonlocal batch_acquire_started
                batch_acquire_started = True
                urls = [str(profile_url or "").strip() for profile_url in list(profile_urls_arg or [])]
                return {
                    "acquired_urls": [],
                    "contended_urls": urls,
                    "leases_by_url": {
                        profile_url: {
                            "acquired": False,
                            "lease_owner": "provider-owned-submit-in-flight",
                        }
                        for profile_url in urls
                    },
                }

            store.repos.linkedin_profile_registry.get_bulk = _get_bulk_with_provider_owner_after_contention
            store.repos.linkedin_profile_registry.acquire_leases = _contended_batch_acquire  # type: ignore[method-assign]
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = object()

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=profile_urls,
                snapshot_dir=root,
                job_id=job_id,
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                load_cached_profile_payloads=False,
                prefetch_batch_context={
                    "requested_url_count": 25,
                    "candidate_count": 25,
                    "planned_deferred_url_count": 0,
                    "planned_dispatch_worker_count": 1,
                    "allow_under_target_final_tail_dispatch": True,
                },
            )

        summary = dict(result.get("summary") or {})
        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(summary["message"], "profile_urls_already_queued_or_claimed")
        self.assertEqual(summary["queued_urls"], profile_urls)
        self.assertEqual(summary["deferred_urls"], [])
        self.assertEqual(summary["already_queued_url_count"], 20)

    def test_harvest_profile_batch_worker_defers_real_registry_lease_contention(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("lease-contended URLs should wait for a later refill tick")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            profile_urls = [
                f"https://www.linkedin.com/in/lease-contention-{index:03d}/"
                for index in range(3)
            ]
            job_id = "job_registry_lease_contention"
            store.repos.linkedin_profile_registry.acquire_leases(
                profile_urls,
                lease_owner="competing-local-submit",
                lease_seconds=120,
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = object()

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=profile_urls,
                snapshot_dir=root,
                job_id=job_id,
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                load_cached_profile_payloads=False,
                prefetch_batch_context={
                    "requested_url_count": len(profile_urls),
                    "candidate_count": len(profile_urls),
                    "planned_deferred_url_count": 0,
                    "planned_dispatch_worker_count": 1,
                    "allow_under_target_final_tail_dispatch": True,
                },
            )
            entries = store.repos.linkedin_profile_registry.get_bulk(profile_urls)

        summary = dict(result.get("summary") or {})
        self.assertEqual(result["worker_status"], "backpressure")
        self.assertEqual(summary["message"], "profile_registry_lease_contention")
        self.assertEqual(summary["queued_urls"], [])
        self.assertEqual(summary["deferred_urls"], profile_urls)
        self.assertTrue(
            all(str(entry.get("refill_queue_state") or "") == "deferred_budget" for entry in entries.values())
        )

    def test_queue_background_profile_prefetch_returns_completed_when_registry_closes_during_submit(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = object()
            job_id = "job_profile_registry_closes_during_submit"
            profile_urls = [
                f"https://www.linkedin.com/in/terminal-race-{index:04d}/"
                for index in range(50)
            ]
            candidates = [
                Candidate(
                    candidate_id=f"terminal_race_{index}",
                    name_en=f"Terminal Race {index}",
                    display_name=f"Terminal Race {index}",
                    linkedin_url=profile_url,
                )
                for index, profile_url in enumerate(profile_urls)
            ]

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                submitted_urls = [
                    str(profile_url or "").strip()
                    for profile_url in list(kwargs.get("profile_urls") or [])
                    if str(profile_url or "").strip()
                ]
                for profile_url in submitted_urls:
                    raw_path = root / "harvest_profiles" / f"{normalize_linkedin_profile_url_key(profile_url)}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text(json.dumps({"profileUrl": profile_url}), encoding="utf-8")
                    store.repos.linkedin_profile_registry.mark_fetched(
                        profile_url,
                        raw_path=str(raw_path),
                        source_shards=["enrichment_background_prefetch"],
                        source_jobs=[job_id],
                        snapshot_dir=str(root),
                        run_id="run-terminal-race",
                        dataset_id="dataset-terminal-race",
                    )
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": submitted_urls,
                        "requested_urls": submitted_urls,
                        "dispatched_url_count": len(submitted_urls),
                        "worker_id": 123,
                        "run_id": "run-terminal-race",
                        "dataset_id": "dataset-terminal-race",
                        "payload_hash": "terminal-race",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

            result = enricher.queue_background_profile_prefetch(
                candidates=candidates,
                snapshot_dir=root,
                job_id=job_id,
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                load_cached_profile_payloads=False,
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["reason"], "registry_all_requested_profiles_fetched")
        self.assertTrue(result["terminal_proof_only"])
        self.assertFalse(result["submit_anchor"])
        self.assertEqual(result["dispatched_url_count"], 0)
        self.assertEqual(result["queued_worker_count"], 0)
        self.assertEqual(result["active_worker_count"], 0)
        self.assertEqual(result["summary_paths"], [])
        self.assertEqual(result["batch_envelopes"], [])
        self.assertEqual(result["batch_plans"], [])
        self.assertEqual(result["batch_plan"], {})
        queue = dict(result.get("profile_prefetch_queue") or {})
        self.assertTrue(queue["registry_all_requested_terminal"])
        self.assertEqual(queue["registry_fetched_url_count"], 50)
        self.assertEqual(queue["queued_url_count"], 0)
        self.assertEqual(queue["pending_url_count"], 0)
        self.assertTrue(queue["queue_quiescent"])

    def test_profile_batch_backpressure_envelope_is_not_counted_as_idle_slot_underuse(self) -> None:
        envelope = _build_profile_prefetch_batch_envelope(
            chunk_index=1,
            profile_url_chunk=[f"https://www.linkedin.com/in/backpressure-{index}/" for index in range(21)],
            requested_url_count=297,
            candidate_count=297,
            active_worker_count=2,
            actor_budget=4,
            submit_budget=4,
            recommended_batch_size=25,
            recommended_batch_count=3,
            recommended_max_workers=4,
            dispatch_strategy="configured_prefetch_batch_size",
            deferred_url_count=23,
            queued_worker_count=0,
            dispatched_url_count=0,
            status="backpressure",
        )

        self.assertTrue(envelope["backpressure_exempt_from_underuse"])
        self.assertFalse(envelope["is_tiny_batch"])

    def test_profile_batch_tail_coalescing_does_not_count_as_slot_underuse(self) -> None:
        envelope = _build_profile_prefetch_batch_envelope(
            chunk_index=1,
            profile_url_chunk=[f"https://www.linkedin.com/in/tail-coalescing-{index}/" for index in range(50)],
            requested_url_count=74,
            candidate_count=74,
            active_worker_count=0,
            actor_budget=4,
            submit_budget=4,
            recommended_batch_size=50,
            recommended_batch_count=2,
            recommended_max_workers=2,
            dispatch_strategy="actor_slot_item_packing_scripted_prefetch_window",
            deferred_url_count=24,
            tail_coalescing_url_count=24,
            queued_worker_count=1,
            dispatched_url_count=50,
            status="queued",
        )

        self.assertEqual(envelope["worker_budget_deferred_url_count"], 0)
        self.assertFalse(envelope["provider_slot_underuse_with_backlog"])
        self.assertEqual(envelope["underuse_reason"], "")

    def test_profile_batch_envelope_with_zero_dispatch_is_not_counted_as_tiny_batch(self) -> None:
        envelope = _build_profile_prefetch_batch_envelope(
            chunk_index=1,
            profile_url_chunk=["https://www.linkedin.com/in/already-cached/"],
            requested_url_count=77,
            candidate_count=154,
            active_worker_count=0,
            actor_budget=4,
            submit_budget=4,
            recommended_batch_size=25,
            recommended_batch_count=1,
            recommended_max_workers=4,
            dispatch_strategy="configured_prefetch_batch_size",
            deferred_url_count=0,
            queued_worker_count=0,
            dispatched_url_count=0,
            status="completed",
        )

        self.assertEqual(envelope["dispatched_url_count"], 0)
        self.assertFalse(envelope["is_tiny_batch"])
        self.assertTrue(envelope["tiny_batch_allowed"])
        self.assertEqual(envelope["small_batch_reason"], "")
        self.assertFalse(envelope["provider_slot_underuse_with_backlog"])
        self.assertEqual(envelope["underuse_reason"], "")

    def test_coalesce_tiny_profile_dispatch_chunks_merges_non_tail_tiny_batches(self) -> None:
        chunks = [
            [f"https://www.linkedin.com/in/tiny-{index}/" for index in range(start, start + 3)]
            for start in (0, 3, 6, 9)
        ]

        coalesced = _coalesce_tiny_profile_dispatch_chunks(
            chunks,
            requested_url_count=24,
            candidate_count=24,
            min_non_tail_batch_size=10,
            tiny_batch_max_size=5,
        )

        self.assertEqual([len(chunk) for chunk in coalesced], [12])

    def test_profile_prefetch_batch_plan_normalizes_tiny_window_before_budget_split(self) -> None:
        profile_urls = [f"https://www.linkedin.com/in/plan-tiny-{index}/" for index in range(50)]

        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=profile_urls,
                requested_url_count=50,
                candidate_count=50,
                priority=True,
                source_shards_by_url={},
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 1,
                    "submit_budget": 1,
                    "available_new_worker_count": 1,
                },
                dispatch_window={
                    "batch_size": 3,
                    "max_workers": 1,
                    "batch_count": 17,
                    "source_mix": {"other": 50},
                    "strategy": "test_unexplained_tiny_batch",
                    "priority": True,
                },
            )

        self.assertEqual(plan.plan_reason, "ready_to_dispatch")
        self.assertEqual(plan.original_dispatch_chunk_count, 1)
        self.assertEqual(plan.coalesced_dispatch_chunk_count, 1)
        self.assertEqual(plan.tiny_batch_coalesced_count, 0)
        self.assertEqual([len(chunk) for _, chunk in plan.dispatch_specs], [50])
        self.assertEqual(plan.deferred_urls, [])

    def test_profile_prefetch_batch_plan_preserves_low_volume_batches(self) -> None:
        profile_urls = [f"https://www.linkedin.com/in/low-volume-{index}/" for index in range(4)]

        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=profile_urls,
                requested_url_count=4,
                candidate_count=4,
                priority=False,
                source_shards_by_url={},
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 2,
                    "submit_budget": 2,
                    "available_new_worker_count": 2,
                },
                dispatch_window={
                    "batch_size": 2,
                    "max_workers": 2,
                    "batch_count": 2,
                    "source_mix": {"other": 4},
                    "strategy": "test_low_volume",
                    "priority": False,
                },
            )

        self.assertEqual(plan.plan_reason, "ready_to_dispatch")
        self.assertEqual(plan.original_dispatch_chunk_count, 1)
        self.assertEqual(plan.coalesced_dispatch_chunk_count, 1)
        self.assertEqual(plan.tiny_batch_coalesced_count, 0)
        self.assertEqual([len(chunk) for _, chunk in plan.dispatch_specs], [4])
        self.assertEqual(plan.deferred_urls, [])

    def test_profile_prefetch_batch_plan_preserves_queue_item_metadata_across_budget_split(self) -> None:
        profile_urls = [f"https://www.linkedin.com/in/item-plan-{index}/" for index in range(105)]
        queue_items = _build_profile_prefetch_queue_items(
            profile_urls,
            source_shards_by_url={
                profile_urls[0]: ["openai::agent", "openai::infra"],
                profile_urls[75]: ["openai::health"],
            },
            source_jobs=["job_item_plan"],
            priority=True,
            registry_entries={
                normalize_linkedin_profile_url_key(profile_urls[0]): {"status": "failed_retryable"},
            },
        )

        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=profile_urls,
                requested_url_count=105,
                candidate_count=105,
                priority=True,
                source_shards_by_url={},
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 2,
                    "submit_budget": 2,
                    "available_new_worker_count": 1,
                },
                dispatch_window={
                    "batch_size": 2,
                    "max_workers": 2,
                    "batch_count": 3,
                    "source_mix": {"other": 105},
                    "strategy": "test_item_plan",
                    "priority": True,
                },
                queue_items=queue_items,
            )

        # The invariant this test pins is that per-item queue metadata (source shards,
        # source jobs, priority, registry status) survives the budget split intact — on
        # both the dispatched and the deferred side. Under the new contract a tiny/mocked
        # window (batch_size=2) over a 105-url set floors to a single actor-slot envelope
        # (R1.a) and a single available worker dispatches one 50-slot envelope while the
        # remaining 55 defer as a worker-budget backlog (R5), instead of the old
        # single-105-chunk. We re-derive the split counts but keep every metadata check.
        self.assertEqual(plan.queue_item_count, 105)
        self.assertEqual(plan.planned_dispatch_item_count, 50)
        self.assertEqual(plan.planned_deferred_item_count, 55)
        self.assertEqual(plan.dispatch_specs, [(1, profile_urls[:50])])
        self.assertEqual(plan.deferred_urls, profile_urls[50:])
        # Dispatched-side metadata preserved (item 0 carries shards + retryable status).
        self.assertEqual(plan.dispatch_item_specs[0][1][0].source_shards, ["openai::agent", "openai::infra"])
        self.assertEqual(plan.dispatch_item_specs[0][1][0].source_jobs, ["job_item_plan"])
        self.assertTrue(plan.dispatch_item_specs[0][1][0].priority)
        self.assertEqual(plan.dispatch_item_specs[0][1][0].registry_status, "failed_retryable")
        # Deferred-side metadata preserved (item 75, now in the deferred wave, keeps shards).
        deferred_item_75 = next(item for item in plan.deferred_items if item.profile_url == profile_urls[75])
        self.assertEqual(deferred_item_75.source_shards, ["openai::health"])
        self.assertEqual(deferred_item_75.source_jobs, ["job_item_plan"])
        self.assertTrue(deferred_item_75.priority)
        self.assertEqual(plan.to_record()["item_store"], "linkedin_profile_registry")
        self.assertEqual(plan.to_record()["planned_deferred_item_count"], 55)
        self.assertEqual(plan.to_record()["planned_tail_coalescing_item_count"], 0)
        self.assertEqual(plan.to_record()["planned_worker_budget_deferred_item_count"], 55)
        self.assertEqual(plan.to_record()["refill_policy"], "continuous_ready_item_refill")
        self.assertEqual(plan.to_record()["available_slot_count"], 1)
        self.assertEqual(plan.to_record()["planned_new_worker_count"], 1)
        self.assertEqual(plan.to_record()["unfilled_available_slot_count"], 0)
        self.assertEqual(plan.to_record()["refill_saturation"], "worker_budget_saturated")

    def test_profile_prefetch_queue_items_preserve_registry_refill_state_for_replan(self) -> None:
        profile_urls = [f"https://www.linkedin.com/in/refill-state-{index}/" for index in range(24)]
        registry_entries = {
            normalize_linkedin_profile_url_key(profile_url): {
                "status": "queued",
                "refill_queue_state": "deferred_budget",
            }
            for profile_url in profile_urls
        }
        queue_items = _build_profile_prefetch_queue_items(
            profile_urls,
            source_jobs=["job_refill_state"],
            queue_state="ready",
            registry_entries=registry_entries,
        )

        plan = _build_profile_prefetch_batch_plan(
            dispatch_urls=profile_urls,
            requested_url_count=len(profile_urls),
            candidate_count=len(profile_urls),
            priority=True,
            source_shards_by_url={},
            worker_budget={
                "active_worker_count": 3,
                "actor_budget": 4,
                "submit_budget": 4,
                "available_new_worker_count": 1,
            },
            dispatch_window={
                "batch_size": 50,
                "max_workers": 1,
                "batch_count": 1,
                "source_mix": {"other": 24},
                "strategy": "actor_slot_item_packing_scripted_prefetch_window",
                "base_strategy": "adaptive_scripted_prefetch_window",
                "batch_size_contract": "profile_actor_slot_ready_item_packing",
                "batch_size_reason": "actor_slot_item_packing",
            },
            queue_items=queue_items,
        )

        self.assertTrue(all(item.queue_state == "deferred_budget" for item in queue_items))
        self.assertEqual(plan.plan_reason, "deferred_coalescing_sub_50_tail")
        self.assertEqual(plan.planned_dispatch_item_count, 0)
        self.assertEqual(plan.planned_deferred_item_count, 24)

    def test_profile_prefetch_actor_slot_contract_caps_large_ready_sets_to_provider_envelopes(self) -> None:
        cases = [
            (50, 50, 1, "actor_slot_item_packing"),
            (120, 120, 1, "single_durable_unit_ready_set"),
            (297, 297, 1, "single_durable_unit_ready_set"),
            (401, 201, 2, "large_ready_set_provider_envelope_target"),
            (529, 265, 2, "large_ready_set_provider_envelope_target"),
            (600, 300, 2, "large_ready_set_provider_envelope_target"),
            (2384, 298, 8, "large_ready_set_provider_envelope_target"),
        ]
        for count, expected_size, expected_count, expected_reason in cases:
            with self.subTest(count=count):
                source_shards_by_url = {
                    f"https://www.linkedin.com/in/actor-slot-{count}-{index}/": [
                        "harvest_profile_search:openai:agent"
                    ]
                    for index in range(count)
                }
                with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
                    window = _recommended_harvest_profile_prefetch_dispatch_window(
                        count,
                        priority=True,
                        source_shards_by_url=source_shards_by_url,
                    )

                self.assertEqual(window["batch_size"], expected_size)
                self.assertEqual(window["batch_count"], expected_count)
                self.assertEqual(window["batch_size_contract"], "profile_actor_slot_ready_item_packing")
                self.assertEqual(window["batch_size_reason"], expected_reason)
                self.assertEqual(window["actor_slot_url_target"], 50)
                self.assertEqual(window["durable_unit_max_urls"], 200)
                self.assertEqual(window["provider_envelope_max_urls"], 300)

    def test_profile_prefetch_provider_envelope_cap_is_decoupled_from_durable_unit_cap(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                "HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS": "160",
                "HARVEST_PROFILE_PREFETCH_PROVIDER_ENVELOPE_MAX_URLS": "480",
            },
            clear=False,
        ):
            window = _recommended_harvest_profile_prefetch_dispatch_window(
                960,
                priority=True,
                source_shards_by_url={
                    f"https://www.linkedin.com/in/decoupled-{index}/": [
                        "harvest_profile_search:google:vision-language"
                    ]
                    for index in range(960)
                },
            )

        self.assertEqual(window["batch_size"], 480)
        self.assertEqual(window["batch_count"], 2)
        self.assertEqual(window["durable_unit_max_urls"], 160)
        self.assertEqual(window["provider_envelope_max_urls"], 480)
        self.assertEqual(window["batch_size_reason"], "large_ready_set_provider_envelope_target")

    def test_profile_prefetch_batch_plan_dispatches_small_complete_shard_as_single_durable_unit(self) -> None:
        profile_urls = [
            f"https://www.linkedin.com/in/google-gemini-small-former-{index:03d}/"
            for index in range(120)
        ]

        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            window = _recommended_harvest_profile_prefetch_dispatch_window(
                len(profile_urls),
                priority=True,
                source_shards_by_url={
                    profile_url: ["harvest_profile_search:google:gemini:former"]
                    for profile_url in profile_urls
                },
            )
            plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=profile_urls,
                requested_url_count=len(profile_urls),
                candidate_count=len(profile_urls),
                priority=True,
                source_shards_by_url={},
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 4,
                    "submit_budget": 4,
                    "available_new_worker_count": 4,
                },
                dispatch_window=window,
            )

        self.assertEqual(window["batch_size"], 120)
        self.assertEqual(window["batch_count"], 1)
        self.assertEqual(window["batch_size_reason"], "single_durable_unit_ready_set")
        self.assertEqual(plan.plan_reason, "ready_to_dispatch")
        self.assertEqual([len(chunk) for _, chunk in plan.dispatch_specs], [120])
        self.assertEqual(plan.deferred_urls, [])

    def test_profile_prefetch_batch_plan_splits_roster_wave_across_available_actor_slots(self) -> None:
        profile_urls = [
            f"https://www.linkedin.com/in/lovable-roster-{index:03d}/"
            for index in range(120)
        ]
        source_shards_by_url = {
            profile_url: ["harvest_company_employees:lovable:current"]
            for profile_url in profile_urls
        }

        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            window = _recommended_harvest_profile_prefetch_dispatch_window(
                len(profile_urls),
                priority=True,
                source_shards_by_url=source_shards_by_url,
            )
            plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=profile_urls,
                requested_url_count=len(profile_urls),
                candidate_count=len(profile_urls),
                priority=True,
                source_shards_by_url=source_shards_by_url,
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 4,
                    "submit_budget": 4,
                    "available_new_worker_count": 4,
                },
                dispatch_window=window,
            )

        self.assertEqual(window["batch_size"], 120)
        self.assertEqual(window["batch_size_reason"], "single_durable_unit_ready_set")
        self.assertEqual(plan.dispatch_window["batch_size"], 50)
        self.assertEqual(plan.dispatch_window["batch_count"], 3)
        self.assertEqual(plan.dispatch_window["batch_size_reason"], "roster_actor_slot_fill")
        self.assertEqual([len(chunk) for _, chunk in plan.dispatch_specs], [50, 50])
        self.assertEqual(len(plan.tail_coalescing_items), 20)
        self.assertEqual(len(plan.deferred_urls), 20)
        plan_record = plan.to_record()
        self.assertEqual(plan_record["planned_new_worker_count"], 2)
        self.assertEqual(plan_record["recommended_max_workers"], 3)
        self.assertEqual(plan_record["planned_tail_coalescing_item_count"], 20)

    def test_profile_prefetch_batch_plan_classifies_dataset_company_people_as_roster(self) -> None:
        profile_urls = [
            f"https://www.linkedin.com/in/lovable-company-people-{index:03d}/"
            for index in range(120)
        ]
        source_shards_by_url = {
            profile_url: ["dataset:lovable_linkedin_company_people"]
            for profile_url in profile_urls
        }

        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            window = _recommended_harvest_profile_prefetch_dispatch_window(
                len(profile_urls),
                priority=True,
                source_shards_by_url=source_shards_by_url,
            )
            plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=profile_urls,
                requested_url_count=len(profile_urls),
                candidate_count=len(profile_urls),
                priority=True,
                source_shards_by_url=source_shards_by_url,
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 4,
                    "submit_budget": 4,
                    "available_new_worker_count": 4,
                },
                dispatch_window=window,
            )

        self.assertEqual(window["source_mix"]["company_roster"], 120)
        self.assertEqual(plan.dispatch_window["batch_size"], 50)
        self.assertEqual(plan.dispatch_window["batch_size_reason"], "roster_actor_slot_fill")
        self.assertEqual([len(chunk) for _, chunk in plan.dispatch_specs], [50, 50])
        self.assertEqual(len(plan.tail_coalescing_items), 20)

    def test_profile_prefetch_batch_plan_defers_sub_50_normal_tail(self) -> None:
        profile_urls = [f"https://www.linkedin.com/in/probe-tail-{index}/" for index in range(25)]

        plan = _build_profile_prefetch_batch_plan(
            dispatch_urls=profile_urls,
            requested_url_count=297,
            candidate_count=297,
            priority=True,
            source_shards_by_url={},
            worker_budget={
                "active_worker_count": 0,
                "actor_budget": 4,
                "submit_budget": 4,
                "available_new_worker_count": 4,
            },
            dispatch_window={
                "batch_size": 50,
                "max_workers": 4,
                "batch_count": 1,
                "source_mix": {"other": 25},
                "strategy": "test_sub_50_tail",
                "priority": True,
            },
        )

        self.assertEqual(plan.plan_reason, "deferred_coalescing_sub_50_tail")
        self.assertEqual(plan.dispatch_specs, [])
        self.assertEqual(plan.deferred_urls, profile_urls)
        self.assertEqual([item.profile_url for item in plan.tail_coalescing_items], profile_urls)
        self.assertEqual(plan.to_record()["planned_deferred_item_count"], 25)
        self.assertEqual(plan.to_record()["planned_tail_coalescing_item_count"], 25)

    def test_profile_prefetch_batch_plan_releases_sub_50_tail_for_daemon_flush(self) -> None:
        profile_urls = [f"https://www.linkedin.com/in/final-tail-{index}/" for index in range(47)]

        plan = _build_profile_prefetch_batch_plan(
            dispatch_urls=profile_urls,
            requested_url_count=297,
            candidate_count=297,
            priority=True,
            source_shards_by_url={},
            worker_budget={
                "active_worker_count": 0,
                "actor_budget": 4,
                "submit_budget": 4,
                "available_new_worker_count": 4,
            },
            dispatch_window={
                "batch_size": 50,
                "max_workers": 4,
                "batch_count": 1,
                "source_mix": {"other": 47},
                "strategy": "test_final_tail_flush",
                "priority": True,
            },
            allow_under_target_final_tail_dispatch=True,
        )

        self.assertEqual(plan.plan_reason, "queue_quiescent_final_tail")
        self.assertEqual(plan.dispatch_specs, [(1, profile_urls)])
        self.assertEqual(plan.deferred_urls, [])
        self.assertEqual(plan.to_record()["planned_dispatch_item_count"], 47)
        self.assertEqual(plan.to_record()["planned_deferred_item_count"], 0)

    def test_profile_prefetch_batch_plan_defers_final_tail_by_budget_not_coalescing_when_terminal(self) -> None:
        profile_urls = [f"https://www.linkedin.com/in/terminal-wave-{index}/" for index in range(140)]

        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=profile_urls,
                requested_url_count=140,
                candidate_count=140,
                priority=True,
                source_shards_by_url={},
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 2,
                    "submit_budget": 2,
                    "available_new_worker_count": 2,
                },
                dispatch_window={
                    "batch_size": 50,
                    "max_workers": 2,
                    "batch_count": 3,
                    "source_mix": {"other": 140},
                    "strategy": "test_terminal_wave_tail",
                    "priority": True,
                },
                allow_under_target_final_tail_dispatch=True,
            )

        self.assertEqual(plan.plan_reason, "ready_to_dispatch")
        self.assertEqual([len(chunk) for _, chunk in plan.dispatch_specs], [140])
        self.assertEqual(plan.deferred_urls, [])
        self.assertEqual(plan.tail_coalescing_items, [])
        self.assertEqual(plan.to_record()["planned_tail_coalescing_item_count"], 0)

    def test_profile_prefetch_sub_50_replan_records_durable_coalescing_state(self) -> None:
        profile_urls = [f"https://www.linkedin.com/in/durable-tail-{index}/" for index in range(25)]
        plan = _build_profile_prefetch_batch_plan(
            dispatch_urls=profile_urls,
            requested_url_count=297,
            candidate_count=297,
            priority=True,
            source_shards_by_url={},
            worker_budget={
                "active_worker_count": 0,
                "actor_budget": 4,
                "submit_budget": 4,
                "available_new_worker_count": 4,
            },
            dispatch_window={
                "batch_size": 50,
                "max_workers": 4,
                "batch_count": 1,
                "source_mix": {"other": 25},
                "strategy": "test_sub_50_tail",
                "priority": True,
            },
        )

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = self.make_pg_store(str(root / "control_plane.db"))
            record = _record_profile_prefetch_batch_plan_items(
                store,
                plan,
                source_jobs=["job_tail_record"],
                snapshot_dir=root,
                trigger_kind="profile_prefetch_replan",
                record_active_items=False,
            )
            entries = store.repos.linkedin_profile_registry.get_bulk(profile_urls)
            oldest_age_ms = _profile_prefetch_oldest_deferred_coalescing_age_ms(
                profile_urls,
                entries,
            )

        self.assertEqual(record["status"], "recorded")
        self.assertEqual(record["deferred_queue_state"], "deferred_coalescing")
        self.assertEqual(record["plan_reason"], "deferred_coalescing_sub_50_tail")
        self.assertTrue(record["refill_not_before_at"])
        self.assertTrue(all(str(entry.get("refill_queue_state") or "") == "deferred_coalescing" for entry in entries.values()))
        self.assertIsNotNone(oldest_age_ms)

    def test_profile_prefetch_refill_daemon_releases_ready_sub_50_tail(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            profile_urls = [
                f"https://www.linkedin.com/in/ready-final-tail-{index}/"
                for index in range(47)
            ]
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                deferred_profile_urls=profile_urls,
                source_jobs=["job_ready_tail"],
                snapshot_dir=str(root),
                trigger_kind="profile_tiny_tail_coalescing",
                plan_reason="tiny_tail_coalescing_wait",
                deferred_reason="final_tail_unproven",
                deferred_queue_state="deferred_coalescing",
                refill_not_before_at="2000-01-01 00:00:00",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                chunk = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(chunk)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": chunk,
                        "summary_path": str(root / "ready-final-tail.json"),
                        "worker_id": 7,
                        "run_id": "run-ready-final-tail",
                        "dataset_id": "dataset-ready-final-tail",
                        "payload_hash": "payload-ready-final-tail",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

            result = enricher.queue_background_profile_prefetch(
                candidates=[],
                extra_profile_urls=[],
                snapshot_dir=root,
                job_id="job_ready_tail",
                request_payload={
                    "harvest_profile_actor_global_inflight": 4,
                    "harvest_profile_batch_submit_global_inflight": 4,
                },
                plan_payload={},
                runtime_mode="daemon_refill",
                allow_shared_provider_cache=True,
                priority=True,
                load_cached_profile_payloads=False,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [47])
        self.assertEqual(result["batch_plan_reason"], "queue_quiescent_final_tail")
        self.assertEqual(result["dispatched_url_count"], 47)
        self.assertEqual(result["deferred_url_count"], 0)

    def test_profile_prefetch_refill_daemon_final_tail_does_not_reenter_worker_tiny_gate(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.submitted_urls: list[list[str]] = []

            def execute_batch_with_checkpoint(self, profile_urls, *args, **kwargs):
                self.submitted_urls.append(list(profile_urls))
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={"run_id": "run-final-tail", "dataset_id": "dataset-final-tail"},
                    pending=True,
                    message="queued",
                )

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _Connector()
            profile_urls = [
                f"https://www.linkedin.com/in/daemon-final-tail-{index}/"
                for index in range(40)
            ]
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                deferred_profile_urls=profile_urls,
                source_jobs=["job_daemon_final_tail"],
                snapshot_dir=str(root),
                trigger_kind="profile_tiny_tail_coalescing",
                plan_reason="tiny_tail_coalescing_wait",
                deferred_reason="final_tail_unproven",
                deferred_queue_state="deferred_coalescing",
                refill_not_before_at="2000-01-01 00:00:00",
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime

            with mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window:
                window.return_value = {
                    "batch_size": 50,
                    "max_workers": 2,
                    "batch_count": 1,
                    "source_mix": {"other": 40},
                    "strategy": "test_final_tail_daemon_refill",
                    "priority": True,
                }
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=[],
                    snapshot_dir=root,
                    job_id="job_daemon_final_tail",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 2,
                        "harvest_profile_batch_submit_global_inflight": 2,
                    },
                    plan_payload={},
                    runtime_mode="daemon_refill",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )
            registry_entries = store.repos.linkedin_profile_registry.get_bulk(profile_urls)

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["batch_plan_reason"], "queue_quiescent_final_tail")
        self.assertEqual(result["dispatched_url_count"], 40)
        self.assertEqual(result["queued_worker_count"], 1)
        self.assertEqual(result["deferred_url_count"], 0)
        self.assertEqual(connector.submitted_urls, [profile_urls])
        envelope = dict(list(result.get("batch_envelopes") or [])[0])
        self.assertEqual(envelope["status"], "queued")
        self.assertEqual(envelope["small_batch_reason"], "queue_quiescent_final_tail")
        self.assertTrue(envelope["tiny_batch_allowed"])
        self.assertTrue(
            all(
                str(entry.get("refill_queue_state") or "") == "planned_dispatch"
                for entry in registry_entries.values()
            )
        )

    def test_profile_prefetch_append_trigger_coalesces_25_plus_25_before_timer(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            current_urls = [
                f"https://www.linkedin.com/in/current-probe-{index}/"
                for index in range(25)
            ]
            former_urls = [
                f"https://www.linkedin.com/in/former-probe-{index}/"
                for index in range(25)
            ]
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                deferred_profile_urls=current_urls,
                source_jobs=["job_probe_coalescing"],
                snapshot_dir=str(root),
                trigger_kind="profile_prefetch_replan",
                plan_reason="deferred_coalescing_sub_50_tail",
                deferred_reason="sub_50_tail_waiting_for_more_discovery",
                deferred_queue_state="deferred_coalescing",
                refill_not_before_at="2999-01-01 00:00:00",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                chunk = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(chunk)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": chunk,
                        "summary_path": str(root / "probe-coalesced.json"),
                        "worker_id": 1,
                        "run_id": "run-probe-coalesced",
                        "dataset_id": "dataset-probe-coalesced",
                        "payload_hash": "payload-probe-coalesced",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

            with mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window:
                window.return_value = {
                    "batch_size": 50,
                    "max_workers": 1,
                    "batch_count": 1,
                    "source_mix": {"other": 50},
                    "strategy": "test_probe_coalescing",
                    "priority": True,
                }
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=former_urls,
                    snapshot_dir=root,
                    job_id="job_probe_coalescing",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 1,
                        "harvest_profile_batch_submit_global_inflight": 1,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )

        self.assertEqual(result["status"], "queued")
        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [50])
        self.assertCountEqual(dispatched_chunks[0], [*former_urls, *current_urls])
        self.assertEqual(result["refill_queue_item_count"], 25)
        self.assertEqual(result["deferred_url_count"], 0)

    def test_queue_background_profile_prefetch_does_not_let_tail_chunk_overtake_backpressured_head(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            attempted_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                attempted_chunks.append(profile_urls)
                return {
                    "worker_status": "backpressure",
                    "summary": {
                        "requested_urls": profile_urls,
                        "deferred_urls": profile_urls,
                        "message": "provider_limiter_backpressure",
                    },
                    "cached_profiles": {},
                    "deferred_urls": profile_urls,
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            former_urls = [
                f"https://www.linkedin.com/in/former-wave-{index:04d}/"
                for index in range(74)
            ]

            with mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window:
                window.return_value = {
                    "batch_size": 50,
                    "max_workers": 2,
                    "batch_count": 2,
                    "source_mix": {"other": 74},
                    "strategy": "test_ordinal_submit_gate",
                    "priority": True,
                }
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=former_urls,
                    snapshot_dir=root,
                    job_id="job_ordinal_submit_gate",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 1,
                        "harvest_profile_batch_submit_global_inflight": 1,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )

            head_entries = store.repos.linkedin_profile_registry.get_bulk(former_urls[:50])
            tail_entries = store.repos.linkedin_profile_registry.get_bulk(former_urls[50:])

        self.assertEqual([len(chunk) for chunk in attempted_chunks], [50])
        self.assertEqual(result["status"], "queued")
        self.assertTrue(result["ordinal_gate_triggered"])
        self.assertEqual(result["ordinal_submit_gate"], "same_plan_chunk_order")
        self.assertEqual(result["ordinal_gate_deferred_url_count"], 0)
        self.assertEqual(result["dispatched_url_count"], 0)
        self.assertEqual(result["deferred_url_count"], 74)
        self.assertEqual(result["queued_worker_count"], 0)
        envelopes = [dict(item) for item in list(result.get("batch_envelopes") or [])]
        self.assertEqual([item["status"] for item in envelopes], ["backpressure"])
        self.assertTrue(all(str(entry.get("refill_queue_state") or "") == "deferred_budget" for entry in head_entries.values()))
        self.assertTrue(all(str(entry.get("refill_queue_state") or "") == "deferred_coalescing" for entry in tail_entries.values()))

    def test_queue_background_profile_prefetch_reserves_same_wave_urls_before_submit(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            profile_urls = [
                f"https://www.linkedin.com/in/lovable-wave-{index:04d}/"
                for index in range(115)
            ]
            dispatched_chunks: list[list[str]] = []
            nested_results: list[dict[str, object]] = []
            inside_nested = False

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                nonlocal inside_nested
                profile_url_chunk = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_url_chunk)
                if not inside_nested:
                    inside_nested = True
                    nested_results.append(
                        enricher.queue_background_profile_prefetch(
                            candidates=[],
                            extra_profile_urls=profile_urls,
                            snapshot_dir=root,
                            job_id="job_same_wave_reservation",
                            request_payload={
                                "harvest_profile_actor_global_inflight": 4,
                                "harvest_profile_batch_submit_global_inflight": 4,
                            },
                            plan_payload={},
                            runtime_mode="workflow",
                            allow_shared_provider_cache=True,
                            priority=True,
                            load_cached_profile_payloads=False,
                        )
                    )
                    inside_nested = False
                return {
                    "worker_status": "queued",
                    "summary": {
                        "requested_urls": profile_url_chunk,
                        "queued_urls": profile_url_chunk,
                        "worker_id": len(dispatched_chunks),
                        "run_id": f"scripted_run_{len(dispatched_chunks)}",
                        "dataset_id": f"scripted_dataset_{len(dispatched_chunks)}",
                        "payload_hash": f"payload_{len(dispatched_chunks)}",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

            with mock.patch.dict(os.environ, {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=profile_urls,
                    snapshot_dir=root,
                    job_id="job_same_wave_reservation",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 4,
                        "harvest_profile_batch_submit_global_inflight": 4,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )

            nested_result = dict(nested_results[0])
            all_entries = store.repos.linkedin_profile_registry.get_bulk(profile_urls)

        # R1.b ("fewer larger envelopes"): the 115-url wave now reserves into a single
        # durable-unit envelope instead of the old 50+50 fan-out with a 15-item
        # deferred_coalescing tail. The invariant this test pins — same-wave URLs are
        # reserved before submit so a concurrent replan cannot re-dispatch them — is
        # preserved: the whole wave reaches planned_dispatch, and the nested replan over
        # the identical URL set sees all 115 already reserved and dispatches nothing.
        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [115])
        self.assertEqual(result["queued_worker_count"], 1)
        self.assertEqual(result["deferred_url_count"], 0)
        self.assertEqual(nested_result["dispatched_url_count"], 0)
        nested_queue = dict(nested_result.get("profile_prefetch_queue") or {})
        self.assertEqual(nested_queue["already_queued_url_count"], 115)
        self.assertEqual(nested_result["queued_worker_count"], 0)
        self.assertTrue(
            all(str(entry.get("refill_queue_state") or "") == "planned_dispatch" for entry in all_entries.values())
        )

    def test_profile_prefetch_reserved_url_requires_matching_payload_hash_to_dispatch(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            profile_url = "https://www.linkedin.com/in/reserved-owner-hash/"
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=[profile_url],
                source_jobs=["job_reserved_owner_hash"],
                snapshot_dir=str(root),
                active_queue_state="dispatch_reserved",
                active_owner_payload_hash="owner-hash-a",
                active_refill_not_before_at="2000-01-01 00:00:00",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)

            generic_dispatch, generic_queued = enricher._partition_already_queued_profile_urls(
                [profile_url],
                snapshot_dir=root,
                source_jobs=["job_reserved_owner_hash"],
                refill_dispatch_profile_urls={profile_url},
            )
            mismatched_dispatch, mismatched_queued = enricher._partition_already_queued_profile_urls(
                [profile_url],
                snapshot_dir=root,
                source_jobs=["job_reserved_owner_hash"],
                refill_dispatch_profile_urls={profile_url},
                expected_owner_payload_hash="owner-hash-b",
            )
            matched_dispatch, matched_queued = enricher._partition_already_queued_profile_urls(
                [profile_url],
                snapshot_dir=root,
                source_jobs=["job_reserved_owner_hash"],
                refill_dispatch_profile_urls={profile_url},
                expected_owner_payload_hash="owner-hash-a",
            )

        self.assertEqual(generic_dispatch, [])
        self.assertEqual(generic_queued, [profile_url])
        self.assertEqual(mismatched_dispatch, [])
        self.assertEqual(mismatched_queued, [profile_url])
        self.assertEqual(matched_dispatch, [profile_url])
        self.assertEqual(matched_queued, [])

    def test_profile_prefetch_budget_counts_reserved_registry_batches(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            first_batch = [
                f"https://www.linkedin.com/in/reserved-budget-a-{index:03d}/"
                for index in range(50)
            ]
            second_batch = [
                f"https://www.linkedin.com/in/reserved-budget-b-{index:03d}/"
                for index in range(50)
            ]
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=first_batch,
                source_jobs=["job_reserved_budget"],
                snapshot_dir=str(root),
                active_queue_state="dispatch_reserved",
                active_owner_payload_hash="reserved-a",
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=second_batch,
                source_jobs=["job_reserved_budget"],
                snapshot_dir=str(root),
                active_queue_state="dispatch_reserved",
                active_owner_payload_hash="reserved-b",
            )

            budget = enricher._harvest_profile_prefetch_new_worker_budget(
                job_id="job_reserved_budget",
                snapshot_dir=root,
                runtime_tuning_context={
                    "harvest_profile_actor_global_inflight": 4,
                    "harvest_profile_batch_submit_global_inflight": 4,
                },
                default_submit_budget=4,
            )

        self.assertEqual(budget["active_worker_count"], 0)
        self.assertEqual(budget["scheduler_reserved_worker_count"], 2)
        self.assertEqual(budget["effective_active_worker_count"], 2)
        self.assertEqual(budget["available_new_worker_count"], 2)

    def test_profile_prefetch_budget_releases_planned_dispatch_when_remote_terminal_seen(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _WorkerRuntime:
            def __init__(self, snapshot_dir: Path) -> None:
                self.snapshot_dir = snapshot_dir

            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return [
                    {
                        "worker_id": 101,
                        "job_id": job_id,
                        "lane_id": lane_id,
                        "status": "queued",
                        "metadata": {
                            "recovery_kind": "harvest_profile_batch",
                            "snapshot_dir": str(self.snapshot_dir),
                        },
                        "checkpoint": {
                            "stage": "waiting_remote_harvest",
                            "run_id": "run-terminal-owner",
                            "remote_provider_terminal_event_seen_at": "2026-05-08T00:00:00+00:00",
                            "remote_provider_terminal_event": {"is_terminal": True},
                        },
                        "output": {"summary": {"payload_hash": "terminal-owner"}},
                    },
                    {
                        "worker_id": 102,
                        "job_id": job_id,
                        "lane_id": lane_id,
                        "status": "queued",
                        "metadata": {
                            "recovery_kind": "harvest_profile_batch",
                            "snapshot_dir": str(self.snapshot_dir),
                        },
                        "checkpoint": {
                            "stage": "waiting_remote_harvest",
                            "run_id": "run-active-owner",
                        },
                        "output": {"summary": {"payload_hash": "active-owner"}},
                    },
                ]

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            terminal_batch = [
                f"https://www.linkedin.com/in/terminal-owner-{index:03d}/"
                for index in range(50)
            ]
            active_batch = [
                f"https://www.linkedin.com/in/active-owner-{index:03d}/"
                for index in range(50)
            ]
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=terminal_batch,
                source_jobs=["job_terminal_slot_release"],
                snapshot_dir=str(root),
                active_queue_state="planned_dispatch",
                active_owner_worker_id=101,
                active_owner_payload_hash="terminal-owner",
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=active_batch,
                source_jobs=["job_terminal_slot_release"],
                snapshot_dir=str(root),
                active_queue_state="planned_dispatch",
                active_owner_worker_id=102,
                active_owner_payload_hash="active-owner",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = _WorkerRuntime(root)

            budget = enricher._harvest_profile_prefetch_new_worker_budget(
                job_id="job_terminal_slot_release",
                snapshot_dir=root,
                runtime_tuning_context={
                    "harvest_profile_actor_global_inflight": 4,
                    "harvest_profile_batch_submit_global_inflight": 4,
                },
                default_submit_budget=4,
            )

        self.assertEqual(budget["active_worker_count"], 1)
        self.assertEqual(budget["scheduler_reserved_worker_count"], 1)
        self.assertEqual(budget["effective_active_worker_count"], 1)
        self.assertEqual(budget["available_new_worker_count"], 3)

    def test_concurrent_profile_prefetch_replan_respects_reserved_actor_budget(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            current_urls = [
                f"https://www.linkedin.com/in/openai-agent-current-budget-{index:04d}/"
                for index in range(223)
            ]
            former_urls = [
                f"https://www.linkedin.com/in/openai-agent-former-budget-{index:04d}/"
                for index in range(74)
            ]
            dispatched_chunks: list[list[str]] = []
            nested_results: list[dict[str, object]] = []
            inside_nested = False

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                nonlocal inside_nested
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                if not inside_nested:
                    inside_nested = True
                    nested_results.append(
                        enricher.queue_background_profile_prefetch(
                            candidates=[],
                            extra_profile_urls=former_urls,
                            snapshot_dir=root,
                            job_id="job_concurrent_budget",
                            request_payload={
                                "harvest_profile_actor_global_inflight": 4,
                                "harvest_profile_batch_submit_global_inflight": 4,
                            },
                            plan_payload={},
                            runtime_mode="workflow",
                            allow_shared_provider_cache=True,
                            priority=True,
                            load_cached_profile_payloads=False,
                        )
                    )
                    inside_nested = False
                return {
                    "worker_status": "queued",
                    "summary": {
                        "requested_urls": profile_urls,
                        "queued_urls": profile_urls,
                        "worker_id": len(dispatched_chunks),
                        "run_id": f"run-concurrent-budget-{len(dispatched_chunks)}",
                        "dataset_id": f"dataset-concurrent-budget-{len(dispatched_chunks)}",
                        "payload_hash": f"payload-concurrent-budget-{len(dispatched_chunks)}",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

            with mock.patch.dict(os.environ, {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=current_urls,
                    snapshot_dir=root,
                    job_id="job_concurrent_budget",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 4,
                        "harvest_profile_batch_submit_global_inflight": 4,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )

            nested_result = dict(nested_results[0])
            nested_queue = dict(nested_result.get("profile_prefetch_queue") or {})

        # R1.b ("fewer larger envelopes"): each wave now packs into a single
        # provider/durable envelope (223 current, then 74 former) instead of the old
        # per-50-slot fan-out. The reserved-actor-budget invariant is unchanged and is
        # what this test pins: the nested replan must see the slot the outer wave already
        # holds in flight (scheduler_reserved_worker_count=1) and dispatch only against the
        # remaining slots — it must never re-consume the reserved slot.
        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [223, 74])
        self.assertEqual(result["queued_worker_count"], 1)
        self.assertEqual(result["deferred_url_count"], 0)
        self.assertEqual(nested_result["dispatched_url_count"], 74)
        self.assertEqual(nested_result["queued_worker_count"], 1)
        self.assertEqual(nested_queue["scheduler_reserved_worker_count"], 1)
        self.assertEqual(nested_queue["available_new_worker_count"], 3)

    def test_queue_background_profile_prefetch_revalidates_owned_urls_inside_scheduler_lock(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _Lock:
            def __init__(self, on_enter):
                self._on_enter = on_enter

            def __enter__(self):
                self._on_enter()
                return {
                    "required": False,
                    "kind": "in_process_test_lock",
                    "distributed": False,
                    "source": "test",
                }

            def __exit__(self, exc_type, exc, tb):
                return False

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            profile_urls = [
                f"https://www.linkedin.com/in/revalidate-owned-{index:03d}/"
                for index in range(100)
            ]
            externally_owned_urls = profile_urls[:50]
            dispatched_chunks: list[list[str]] = []
            lock_enter_count = 0

            def _mark_external_owner_once() -> None:
                nonlocal lock_enter_count
                lock_enter_count += 1
                if lock_enter_count != 1:
                    return
                store.repos.linkedin_profile_registry.record_refill_plan_items(
                    active_profile_urls=externally_owned_urls,
                    source_jobs=["job_revalidate_owned"],
                    snapshot_dir=str(root),
                    trigger_kind="test_external_provider_submit",
                    plan_reason="remote_provider_submitted",
                    active_queue_state="planned_dispatch",
                    active_owner_worker_id=99,
                    active_owner_run_id="run-owned",
                    active_owner_dataset_id="dataset-owned",
                )

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_url_chunk = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_url_chunk)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "requested_urls": profile_url_chunk,
                        "queued_urls": profile_url_chunk,
                        "worker_id": 100 + len(dispatched_chunks),
                        "run_id": f"run-new-{len(dispatched_chunks)}",
                        "dataset_id": f"dataset-new-{len(dispatched_chunks)}",
                        "payload_hash": f"payload-new-{len(dispatched_chunks)}",
                    },
                    "cached_profiles": {},
                }

            enricher._profile_prefetch_scheduler_lock = lambda **kwargs: _Lock(_mark_external_owner_once)
            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

            with mock.patch.dict(os.environ, {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=profile_urls,
                    snapshot_dir=root,
                    job_id="job_revalidate_owned",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 4,
                        "harvest_profile_batch_submit_global_inflight": 4,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )

        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [50])
        self.assertEqual(dispatched_chunks[0], profile_urls[50:])
        self.assertEqual(result["dispatched_url_count"], 50)
        queue = dict(result.get("profile_prefetch_queue") or {})
        self.assertEqual(queue["already_queued_url_count"], 50)
        self.assertEqual(queue["newly_queued_url_count"], 50)

    def test_profile_submit_claim_does_not_reenter_scheduler_lock_after_provider_slot(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                self.calls.append(
                    {
                        "profile_urls": list(profile_urls),
                        "checkpoint": dict(checkpoint or {}),
                    }
                )
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-submit-no-second-lock",
                        "dataset_id": "dataset-submit-no-second-lock",
                    },
                    pending=True,
                    message="Harvest batch queued.",
                )

        class _Lock:
            def __init__(self, on_enter):
                self._on_enter = on_enter

            def __enter__(self):
                self._on_enter()
                return {
                    "required": True,
                    "kind": "pg_advisory_xact_lock",
                    "distributed": True,
                    "source": "test",
                }

            def __exit__(self, exc_type, exc, tb):
                return False

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _Connector()
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=connector, store=store)
            enricher.worker_runtime = worker_runtime
            profile_urls = [
                f"https://www.linkedin.com/in/submit-no-second-lock-{index:03d}/"
                for index in range(50)
            ]
            lock_enter_count = 0

            def _fail_on_second_lock() -> None:
                nonlocal lock_enter_count
                lock_enter_count += 1
                if lock_enter_count > 1:
                    raise AssertionError("submit hot path must not re-enter scheduler lock")

            enricher._profile_prefetch_scheduler_lock = lambda **kwargs: _Lock(_fail_on_second_lock)

            with mock.patch.dict(os.environ, {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=profile_urls,
                    snapshot_dir=root,
                    job_id="job_submit_no_second_lock",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 1,
                        "harvest_profile_batch_submit_global_inflight": 1,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )
            worker_key = (
                "harvest_profile_batch::"
                + sha1(json.dumps(sorted(profile_urls), ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
            )
            worker = store.get_agent_worker(
                job_id="job_submit_no_second_lock",
                lane_id="enrichment_specialist",
                worker_key=worker_key,
            ) or {}

        self.assertEqual(lock_enter_count, 1)
        self.assertEqual(len(connector.calls), 1)
        self.assertEqual(connector.calls[0]["profile_urls"], profile_urls)
        self.assertEqual(result["queued_worker_count"], 1)
        worker_summary = dict(dict(worker.get("output") or {}).get("summary") or {})
        self.assertEqual(
            worker_summary.get("dispatch_claim_lock_policy"),
            "url_lease_plus_provider_slot_no_scheduler_lock",
        )
        self.assertEqual(dict(worker.get("checkpoint") or {}).get("stage"), "waiting_remote_harvest")

    def test_queue_background_profile_prefetch_yields_when_scheduler_lock_busy(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        class _BusyLock:
            def __enter__(self):
                return {
                    "required": True,
                    "kind": "pg_try_advisory_xact_lock",
                    "lock_kind": "pg_try_advisory_xact_lock",
                    "distributed": True,
                    "acquired": False,
                    "busy": True,
                    "source": "test",
                    "reason": "profile_prefetch_scheduler_lock_busy",
                }

            def __exit__(self, exc_type, exc, tb):
                return False

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            enricher._profile_prefetch_scheduler_lock = lambda **kwargs: _BusyLock()
            enricher._profile_prefetch_scheduler_lock_required = lambda: True
            enricher._execute_harvest_profile_batch_worker = mock.Mock(
                side_effect=AssertionError("provider submit must not run when scheduler lock is busy")
            )
            profile_urls = [
                f"https://www.linkedin.com/in/lock-busy-{index:03d}/"
                for index in range(50)
            ]

            with mock.patch.dict(os.environ, {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=profile_urls,
                    snapshot_dir=root,
                    job_id="job_lock_busy",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 4,
                        "harvest_profile_batch_submit_global_inflight": 4,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["reason"], "profile_prefetch_scheduler_lock_busy")
        self.assertEqual(result["dispatched_url_count"], 0)
        self.assertEqual(result["queued_worker_count"], 0)
        scheduler_lock = dict(result.get("scheduler_lock") or {})
        self.assertTrue(scheduler_lock["busy"])
        self.assertFalse(scheduler_lock["acquired"])

    def test_profile_prefetch_batch_plan_replans_large_late_shard_without_historical_budget(self) -> None:
        profile_urls = [f"https://www.linkedin.com/in/late-current-{index}/" for index in range(1600)]

        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            window = _recommended_harvest_profile_prefetch_dispatch_window(
                len(profile_urls),
                priority=True,
                source_shards_by_url={profile_urls[0]: ["harvest_profile_search:openai:agent"]},
            )
            plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=profile_urls,
                requested_url_count=1600,
                candidate_count=1600,
                priority=True,
                source_shards_by_url={},
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 4,
                    "submit_budget": 4,
                    "available_new_worker_count": 4,
                },
                dispatch_window=window,
            )

        # R1.c (balanced provider envelopes, decoupled from the 200 durable-unit cap):
        # ceil(1600/300)=6 balanced envelopes of max(50,min(300,ceil(1600/6)))=267 each,
        # replacing the old 200-cap 8-way split. available_new_worker_count=4 dispatches
        # the first four 267 envelopes; the remaining two (1600-4*267=532) defer.
        self.assertEqual(window["batch_size"], 267)
        self.assertEqual(window["batch_count"], 6)
        self.assertEqual(window["batch_size_reason"], "large_ready_set_provider_envelope_target")
        self.assertEqual([len(chunk) for _, chunk in plan.dispatch_specs], [267, 267, 267, 267])
        self.assertEqual(len(plan.deferred_urls), 1600 - 4 * 267)

    def test_profile_prefetch_batch_envelope_completed_zero_dispatch_is_not_slot_underuse(self) -> None:
        envelope = _build_profile_prefetch_batch_envelope(
            chunk_index=1,
            profile_url_chunk=["https://www.linkedin.com/in/cached-tail/"],
            requested_url_count=297,
            candidate_count=297,
            active_worker_count=0,
            actor_budget=4,
            submit_budget=4,
            recommended_batch_size=3,
            recommended_batch_count=58,
            recommended_max_workers=4,
            dispatch_strategy="adaptive_scripted_prefetch_window",
            deferred_url_count=124,
            queued_worker_count=0,
            dispatched_url_count=0,
            status="completed",
            flush_reason="adaptive_prefetch_window",
        )

        self.assertFalse(envelope["is_tiny_batch"])
        self.assertFalse(envelope["provider_slot_underuse_with_backlog"])
        self.assertEqual(envelope["underuse_reason"], "")

    def test_queue_background_profile_prefetch_records_refill_item_state_for_active_and_deferred_urls(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / "refill-items.json"),
                        "worker_id": len(dispatched_chunks),
                        "run_id": f"run-refill-items-{len(dispatched_chunks)}",
                        "dataset_id": f"dataset-refill-items-{len(dispatched_chunks)}",
                        "payload_hash": f"payload-refill-items-{len(dispatched_chunks)}",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            candidates = [
                Candidate(
                    candidate_id=f"refill_item_{index}",
                    name_en=f"Refill Item {index}",
                    display_name=f"Refill Item {index}",
                    linkedin_url=f"https://www.linkedin.com/in/refill-item-{index}/",
                )
                for index in range(105)
            ]

            with mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window:
                window.return_value = {
                    "batch_size": 2,
                    "max_workers": 2,
                    "batch_count": 3,
                    "source_mix": {"other": 105},
                    "strategy": "test_refill_item_state",
                    "priority": True,
                }
                result = enricher.queue_background_profile_prefetch(
                    candidates=candidates,
                    snapshot_dir=root,
                    job_id="job_refill_item_state",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 1,
                        "harvest_profile_batch_submit_global_inflight": 1,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                )

            active_url = "https://www.linkedin.com/in/refill-item-0/"
            deferred_url = "https://www.linkedin.com/in/refill-item-104/"
            active_entry = store.repos.linkedin_profile_registry.get(active_url) or {}
            deferred_entry = store.repos.linkedin_profile_registry.get(deferred_url) or {}

        self.assertEqual(result["status"], "queued")
        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [50])
        refill_items = dict(result.get("refill_plan_items") or {})
        self.assertEqual(refill_items["status"], "recorded")
        self.assertEqual(refill_items["active_item_count"], 50)
        self.assertEqual(refill_items["deferred_item_count"], 55)
        self.assertEqual(active_entry["refill_queue_state"], "planned_dispatch")
        self.assertGreater(active_entry["refill_owner_worker_id"], 0)
        self.assertTrue(active_entry["refill_owner_run_id"])
        self.assertEqual(active_entry["last_refill_attempt_count"], 1)
        self.assertEqual(active_entry["last_refill_plan_reason"], "remote_provider_submitted")
        self.assertEqual(deferred_entry["refill_queue_state"], "deferred_budget")
        self.assertEqual(deferred_entry["last_refill_attempt_count"], 0)
        self.assertEqual(deferred_entry["last_refill_deferred_reason"], "worker_budget_deferred")
        self.assertEqual(deferred_entry["refill_plan_batch_size"], 50)
        self.assertEqual(deferred_entry["refill_plan_batch_count"], 3)
        self.assertEqual(deferred_entry["refill_plan_window_url_count"], 105)
        queue = dict(result.get("profile_prefetch_queue") or {})
        self.assertEqual(queue["refill_queue_state_counts"], {"planned_dispatch": 50, "deferred_budget": 55})

    def test_queue_background_profile_prefetch_refill_preserves_large_wave_batch_size(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            initial_urls = [
                f"https://www.linkedin.com/in/google-vl-wave-{index:04d}/"
                for index in range(1856)
            ]
            initial_plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=initial_urls,
                requested_url_count=len(initial_urls),
                candidate_count=len(initial_urls),
                priority=True,
                source_shards_by_url={},
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 4,
                    "submit_budget": 4,
                    "available_new_worker_count": 4,
                },
                dispatch_window={
                    "batch_size": 232,
                    "max_workers": 4,
                    "batch_count": 8,
                    "source_mix": {"other": len(initial_urls)},
                    "strategy": "test_large_wave_initial_window",
                    "priority": True,
                },
            )
            _record_profile_prefetch_batch_plan_items(
                store,
                initial_plan,
                source_jobs=["job_large_wave_refill"],
                snapshot_dir=root,
                trigger_kind="profile_prefetch_replan",
                record_active_items=False,
            )
            # Simulate four provider batches from the same wave already submitted
            # and closed before the daemon refills the remaining deferred items.
            submitted_urls = initial_urls[:928]
            for profile_url in submitted_urls:
                store.repos.linkedin_profile_registry.mark_fetched(
                    profile_url,
                    raw_path=str(root / "harvest_profiles" / f"{normalize_linkedin_profile_url_key(profile_url)}.json"),
                    source_jobs=["job_large_wave_refill"],
                    snapshot_dir=str(root),
                )

            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / f"large-wave-refill-{len(dispatched_chunks)}.json"),
                        "worker_id": len(dispatched_chunks),
                        "run_id": f"run-large-wave-refill-{len(dispatched_chunks)}",
                        "dataset_id": f"dataset-large-wave-refill-{len(dispatched_chunks)}",
                        "payload_hash": f"payload-large-wave-refill-{len(dispatched_chunks)}",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

            with mock.patch.dict(os.environ, {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=[],
                    snapshot_dir=root,
                    job_id="job_large_wave_refill",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 4,
                        "harvest_profile_batch_submit_global_inflight": 4,
                    },
                    plan_payload={},
                    runtime_mode="daemon_refill",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )

            batch_plan = dict(result.get("batch_plan") or {})

        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [232, 232, 232, 232])
        self.assertEqual(result["dispatched_url_count"], 928)
        self.assertEqual(result["deferred_url_count"], 0)
        self.assertEqual(batch_plan["recommended_batch_size"], 232)
        self.assertEqual(batch_plan["batch_size_reason"], "durable_refill_wave_batch_size")
        self.assertEqual(batch_plan["batch_size_contract"], "profile_actor_slot_durable_wave_item_packing")

    def test_queue_background_profile_prefetch_refills_deferred_budget_items_from_registry(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            deferred_urls = [
                f"https://www.linkedin.com/in/refill-loop-deferred-{index}/"
                for index in range(50)
            ]
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                deferred_profile_urls=deferred_urls,
                source_jobs=["job_refill_loop"],
                snapshot_dir=str(root),
                plan_reason="ready_to_dispatch",
                deferred_reason="worker_budget_deferred",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / "refill-loop.json"),
                        "worker_id": len(dispatched_chunks),
                        "run_id": f"run-refill-loop-{len(dispatched_chunks)}",
                        "dataset_id": f"dataset-refill-loop-{len(dispatched_chunks)}",
                        "payload_hash": f"payload-refill-loop-{len(dispatched_chunks)}",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

            with mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window:
                window.return_value = {
                    "batch_size": 50,
                    "max_workers": 1,
                    "batch_count": 1,
                    "source_mix": {"other": 50},
                    "strategy": "test_refill_loop_selector",
                    "priority": False,
                }
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=[],
                    snapshot_dir=root,
                    job_id="job_refill_loop",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 1,
                        "harvest_profile_batch_submit_global_inflight": 1,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                )

            deferred_entry = store.repos.linkedin_profile_registry.get(deferred_urls[0]) or {}

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["refill_queue_item_count"], 50)
        self.assertEqual(dispatched_chunks, [deferred_urls])
        self.assertEqual(deferred_entry["refill_queue_state"], "planned_dispatch")
        self.assertEqual(deferred_entry["refill_owner_worker_id"], 1)
        self.assertEqual(deferred_entry["refill_owner_run_id"], "run-refill-loop-1")
        self.assertEqual(deferred_entry["last_refill_attempt_count"], 1)

    def test_queue_background_profile_prefetch_isolates_retry_wait_until_normal_queue_empty(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            pg_context = pg_durable_runtime_env(
                runtime_dir=root,
                schema_label="enrichment_retry_isolation_normal_first",
            )
            pg_context.__enter__()
            try:
                self._assert_queue_background_profile_prefetch_isolates_retry_wait_until_normal_queue_empty(
                    root=root,
                    connector_cls=_Connector,
                )
            finally:
                pg_context.__exit__(None, None, None)

    def _assert_queue_background_profile_prefetch_isolates_retry_wait_until_normal_queue_empty(
        self,
        *,
        root: Path,
        connector_cls: type,
    ) -> None:
        catalog = AssetCatalog(
            project_root=root,
            dev_root=root,
            anthropic_root=root,
            anthropic_workbook=root / "anthropic.xlsx",
            anthropic_readme=root / "README.md",
            anthropic_progress=root / "PROGRESS.md",
            legacy_api_accounts=root / "api_accounts.json",
            legacy_company_ids=root / "company_ids.json",
            anthropic_publications=root / "publications.json",
            scholar_scan_results=root / "scholar.json",
            investor_members_json=root / "investor.json",
            employee_scan_skill=root / "employee_skill.md",
            investor_scan_skill=root / "investor_skill.md",
            onepager_skill=root / "onepager_skill.md",
        )
        store = self.make_pg_store(str(root / "control_plane.db"))
        self.assertTrue(store.control_plane_postgres_is_postgres_only())
        normal_urls = [
            f"https://www.linkedin.com/in/refill-normal-first-{index}/"
            for index in range(50)
        ]
        retry_url = "https://www.linkedin.com/in/refill-retry-second/"
        store.repos.linkedin_profile_registry.record_refill_plan_items(
            deferred_profile_urls=normal_urls,
            source_jobs=["job_refill_retry_isolation"],
            snapshot_dir=str(root),
            plan_reason="ready_to_dispatch",
            deferred_reason="worker_budget_deferred",
        )
        store.repos.linkedin_profile_registry.mark_failed(
            retry_url,
            error="temporary provider timeout",
            retryable=True,
            retry_delay_seconds=1,
            source_jobs=["job_refill_retry_isolation"],
            snapshot_dir=str(root),
        )
        store.repos.linkedin_profile_registry.record_refill_plan_items(
            deferred_profile_urls=[retry_url],
            source_jobs=["job_refill_retry_isolation"],
            snapshot_dir=str(root),
            trigger_kind="profile_retry",
            plan_reason="profile_retry_wait",
            deferred_reason="temporary provider timeout",
            deferred_queue_state="retry_wait",
            refill_not_before_at="2000-01-01 00:00:00",
        )
        enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=connector_cls(), store=store)
        enricher.worker_runtime = object()
        dispatched_chunks: list[list[str]] = []

        def _fake_execute_harvest_profile_batch_worker(**kwargs):
            profile_urls = list(kwargs.get("profile_urls") or [])
            dispatched_chunks.append(profile_urls)
            return {
                "worker_status": "queued",
                "summary": {
                    "queued_urls": profile_urls,
                    "summary_path": str(root / "refill-retry-isolation.json"),
                    "worker_id": len(dispatched_chunks),
                    "run_id": f"run-refill-retry-isolation-{len(dispatched_chunks)}",
                    "dataset_id": f"dataset-refill-retry-isolation-{len(dispatched_chunks)}",
                    "payload_hash": f"payload-refill-retry-isolation-{len(dispatched_chunks)}",
                },
                "cached_profiles": {},
            }

        enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

        with mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window:
            window.return_value = {
                "batch_size": 50,
                "max_workers": 1,
                "batch_count": 1,
                "source_mix": {"other": 50},
                "strategy": "test_refill_retry_isolation",
                "priority": False,
            }
            normal_result = enricher.queue_background_profile_prefetch(
                candidates=[],
                extra_profile_urls=[],
                snapshot_dir=root,
                job_id="job_refill_retry_isolation",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        retry_entry = store.repos.linkedin_profile_registry.get(retry_url) or {}

        self.assertEqual(normal_result["status"], "queued")
        self.assertEqual(dispatched_chunks, [normal_urls])
        self.assertFalse(normal_result["retry_isolated_refill"])
        self.assertEqual(retry_entry["refill_queue_state"], "retry_wait")

    def test_queue_background_profile_prefetch_dispatches_retry_wait_as_isolated_batch(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            pg_context = pg_durable_runtime_env(
                runtime_dir=root,
                schema_label="enrichment_retry_isolated_batch",
            )
            pg_context.__enter__()
            try:
                self._assert_queue_background_profile_prefetch_dispatches_retry_wait_as_isolated_batch(
                    root=root,
                    connector_cls=_Connector,
                )
            finally:
                pg_context.__exit__(None, None, None)

    def _assert_queue_background_profile_prefetch_dispatches_retry_wait_as_isolated_batch(
        self,
        *,
        root: Path,
        connector_cls: type,
    ) -> None:
        catalog = AssetCatalog(
            project_root=root,
            dev_root=root,
            anthropic_root=root,
            anthropic_workbook=root / "anthropic.xlsx",
            anthropic_readme=root / "README.md",
            anthropic_progress=root / "PROGRESS.md",
            legacy_api_accounts=root / "api_accounts.json",
            legacy_company_ids=root / "company_ids.json",
            anthropic_publications=root / "publications.json",
            scholar_scan_results=root / "scholar.json",
            investor_members_json=root / "investor.json",
            employee_scan_skill=root / "employee_skill.md",
            investor_scan_skill=root / "investor_skill.md",
            onepager_skill=root / "onepager_skill.md",
        )
        store = self.make_pg_store(str(root / "control_plane.db"))
        self.assertTrue(store.control_plane_postgres_is_postgres_only())
        retry_url = "https://www.linkedin.com/in/refill-retry-only/"
        store.repos.linkedin_profile_registry.mark_failed(
            retry_url,
            error="temporary provider timeout",
            retryable=True,
            retry_delay_seconds=1,
            source_jobs=["job_refill_retry_only"],
            snapshot_dir=str(root),
        )
        store.repos.linkedin_profile_registry.record_refill_plan_items(
            deferred_profile_urls=[retry_url],
            source_jobs=["job_refill_retry_only"],
            snapshot_dir=str(root),
            trigger_kind="profile_retry",
            plan_reason="profile_retry_wait",
            deferred_reason="temporary provider timeout",
            deferred_queue_state="retry_wait",
            refill_not_before_at="2000-01-01 00:00:00",
        )
        enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=connector_cls(), store=store)
        enricher.worker_runtime = object()
        dispatched_chunks: list[list[str]] = []

        def _fake_execute_harvest_profile_batch_worker(**kwargs):
            profile_urls = list(kwargs.get("profile_urls") or [])
            dispatched_chunks.append(profile_urls)
            return {
                "worker_status": "queued",
                "summary": {
                    "queued_urls": profile_urls,
                    "summary_path": str(root / "refill-retry-only.json"),
                    "worker_id": len(dispatched_chunks),
                    "run_id": f"run-refill-retry-only-{len(dispatched_chunks)}",
                    "dataset_id": f"dataset-refill-retry-only-{len(dispatched_chunks)}",
                    "payload_hash": f"payload-refill-retry-only-{len(dispatched_chunks)}",
                },
                "cached_profiles": {},
            }

        enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

        with mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window:
            window.return_value = {
                "batch_size": 10,
                "max_workers": 1,
                "batch_count": 1,
                "source_mix": {"other": 1},
                "strategy": "test_refill_retry_only",
                "priority": False,
            }
            retry_result = enricher.queue_background_profile_prefetch(
                candidates=[],
                extra_profile_urls=[],
                snapshot_dir=root,
                job_id="job_refill_retry_only",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        self.assertEqual(retry_result["status"], "queued")
        self.assertEqual(dispatched_chunks, [[retry_url]])
        self.assertTrue(retry_result["retry_isolated_refill"])
        batch_plan = dict(retry_result.get("batch_plan") or {})
        self.assertEqual(batch_plan["plan_reason"], "retry_wait_isolated_dispatch")
        self.assertTrue(batch_plan["retry_isolation"])
        self.assertEqual(batch_plan["retry_wait_item_count"], 1)
        envelope = dict(list(retry_result.get("batch_envelopes") or [])[0])
        self.assertEqual(envelope["flush_reason"], "retry_isolation")
        self.assertEqual(envelope["small_batch_reason"], "retry_isolation")
        self.assertTrue(envelope["tiny_batch_allowed"])

    def test_queue_background_profile_prefetch_can_defer_provider_submit_to_refill_daemon(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("callback-safe refill must not submit provider work inline")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            urls = [
                f"https://www.linkedin.com/in/deferred-submit-{index}/"
                for index in range(1, 6)
            ]
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()

            result = enricher.queue_background_profile_prefetch(
                candidates=[],
                extra_profile_urls=urls,
                snapshot_dir=root,
                job_id="job_callback_safe_refill",
                request_payload={
                    "harvest_profile_actor_global_inflight": 4,
                    "harvest_profile_batch_submit_global_inflight": 4,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                submit_provider=False,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["reason"], "provider_submit_deferred_to_refill_daemon")
        self.assertEqual(result["dispatched_url_count"], 0)
        self.assertEqual(result["callback_deferred_submit_url_count"], 5)
        self.assertEqual(result["deferred_url_count"], 5)
        batch_plan = dict(result.get("batch_plan") or {})
        self.assertEqual(batch_plan["plan_reason"], "ready_to_dispatch")
        self.assertEqual(batch_plan["planned_dispatch_item_count"], 5)
        self.assertEqual(batch_plan["planned_deferred_item_count"], 0)
        self.assertEqual(batch_plan["available_slot_count"], 4)
        registry_entries = store.repos.linkedin_profile_registry.get_bulk(urls)
        self.assertTrue(all(str(entry.get("refill_queue_state") or "") == "deferred_budget" for entry in registry_entries.values()))

    def test_queue_background_profile_prefetch_blocks_retry_until_normal_owned_worker_terminal(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("retry_wait must not submit while normal provider-owned work is active")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            normal_url = "https://www.linkedin.com/in/refill-normal-owned/"
            retry_url = "https://www.linkedin.com/in/refill-retry-held/"
            request = JobRequest(raw_user_request="Find retry gate", target_company="OpenAI")
            handle = worker_runtime.begin_worker(
                job_id="job_refill_retry_gate",
                request=request,
                plan_payload={},
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key="harvest_profile_batch::normal-owned",
                stage="enriching",
                span_name="harvest_profile_batch:normal-owned",
                budget_payload={"requested_url_count": 1},
                input_payload={"profile_urls": [normal_url]},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(root),
                    "profile_urls": [normal_url],
                },
                handoff_from_lane="acquisition_specialist",
            )
            worker_runtime.checkpoint_worker(
                handle,
                status="queued",
                checkpoint_payload={
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-normal-owned",
                    "dataset_id": "dataset-normal-owned",
                },
                output_payload={
                    "summary": {
                        "queued_urls": [normal_url],
                        "run_id": "run-normal-owned",
                        "dataset_id": "dataset-normal-owned",
                    }
                },
            )
            store.repos.linkedin_profile_registry.mark_queued(
                normal_url,
                source_jobs=["job_refill_retry_gate"],
                run_id="run-normal-owned",
                dataset_id="dataset-normal-owned",
                snapshot_dir=str(root),
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=[normal_url],
                source_jobs=["job_refill_retry_gate"],
                snapshot_dir=str(root),
                trigger_kind="profile_prefetch_provider_submit",
                plan_reason="remote_provider_submitted",
                active_queue_state="planned_dispatch",
                active_owner_worker_id=handle.worker_id,
                active_owner_run_id="run-normal-owned",
                active_owner_dataset_id="dataset-normal-owned",
                active_owner_payload_hash="payload-normal-owned",
            )
            store.repos.linkedin_profile_registry.mark_failed(
                retry_url,
                error="temporary provider timeout",
                retryable=True,
                retry_delay_seconds=1,
                source_jobs=["job_refill_retry_gate"],
                snapshot_dir=str(root),
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                deferred_profile_urls=[retry_url],
                source_jobs=["job_refill_retry_gate"],
                snapshot_dir=str(root),
                trigger_kind="profile_retry",
                plan_reason="profile_retry_wait",
                deferred_reason="temporary provider timeout",
                deferred_queue_state="retry_wait",
                refill_not_before_at="2000-01-01 00:00:00",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = worker_runtime

            result = enricher.queue_background_profile_prefetch(
                candidates=[],
                extra_profile_urls=[],
                snapshot_dir=root,
                job_id="job_refill_retry_gate",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["reason"], "retry_wait_blocked_by_normal_profile_wave")
        gate = dict(result.get("retry_wait_gate") or {})
        self.assertFalse(gate["retry_allowed"])
        self.assertEqual(gate["reason"], "normal_provider_owned_items_pending")
        self.assertEqual(gate["normal_owned_inflight_item_count"], 1)
        self.assertEqual(gate["normal_owned_inflight_urls"], [normal_url])

    def test_queue_background_profile_prefetch_blocks_retry_until_normal_dispatch_claim_closes(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("retry_wait must not submit while normal dispatch claim is still open")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            normal_url = "https://www.linkedin.com/in/refill-normal-claim-open/"
            retry_url = "https://www.linkedin.com/in/refill-retry-held-by-claim/"
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=[normal_url],
                source_jobs=["job_refill_retry_claim_gate"],
                snapshot_dir=str(root),
                trigger_kind="profile_prefetch_refill",
                plan_reason="ready_to_dispatch",
                active_queue_state="dispatch_claimed",
                active_reason="provider_submit_claimed",
                active_refill_not_before_at="2099-01-01 00:00:00",
            )
            store.repos.linkedin_profile_registry.mark_failed(
                retry_url,
                error="temporary provider timeout",
                retryable=True,
                retry_delay_seconds=1,
                source_jobs=["job_refill_retry_claim_gate"],
                snapshot_dir=str(root),
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                deferred_profile_urls=[retry_url],
                source_jobs=["job_refill_retry_claim_gate"],
                snapshot_dir=str(root),
                trigger_kind="profile_retry",
                plan_reason="profile_retry_wait",
                deferred_reason="temporary provider timeout",
                deferred_queue_state="retry_wait",
                refill_not_before_at="2000-01-01 00:00:00",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()

            result = enricher.queue_background_profile_prefetch(
                candidates=[],
                extra_profile_urls=[],
                snapshot_dir=root,
                job_id="job_refill_retry_claim_gate",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["reason"], "retry_wait_blocked_by_normal_profile_wave")
        gate = dict(result.get("retry_wait_gate") or {})
        self.assertFalse(gate["retry_allowed"])
        self.assertEqual(gate["reason"], "normal_wave_open_items_pending")
        self.assertEqual(gate["normal_open_state_counts"], {"dispatch_claimed": 1})
        self.assertEqual(gate["normal_open_urls"], [normal_url])

    def test_queue_background_profile_prefetch_blocks_retry_until_retry_counted_first_attempt_owned_item_closes(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("retry_wait must not submit while first-attempt owned item is unresolved")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            normal_url = "https://www.linkedin.com/in/refill-normal-owned-retry-counted/"
            retry_url = "https://www.linkedin.com/in/refill-retry-held-by-owned/"
            store.repos.linkedin_profile_registry.mark_queued(
                normal_url,
                source_jobs=["job_refill_retry_counted_owned_gate"],
                snapshot_dir=str(root),
            )
            store.repos.linkedin_profile_registry.mark_failed(
                normal_url,
                error="partial batch still recording terminal URL states",
                retryable=True,
                retry_delay_seconds=1,
                source_jobs=["job_refill_retry_counted_owned_gate"],
                snapshot_dir=str(root),
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=[normal_url],
                source_jobs=["job_refill_retry_counted_owned_gate"],
                snapshot_dir=str(root),
                trigger_kind="profile_prefetch_provider_submit",
                plan_reason="remote_provider_submitted",
                active_queue_state="planned_dispatch",
                active_owner_worker_id=321,
                active_owner_run_id="run-normal-owned-retry-counted",
                active_owner_dataset_id="dataset-normal-owned-retry-counted",
                active_owner_payload_hash="payload-normal-owned-retry-counted",
            )
            store.repos.linkedin_profile_registry.mark_failed(
                retry_url,
                error="temporary provider timeout",
                retryable=True,
                retry_delay_seconds=1,
                source_jobs=["job_refill_retry_counted_owned_gate"],
                snapshot_dir=str(root),
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                deferred_profile_urls=[retry_url],
                source_jobs=["job_refill_retry_counted_owned_gate"],
                snapshot_dir=str(root),
                trigger_kind="profile_retry",
                plan_reason="profile_retry_wait",
                deferred_reason="temporary provider timeout",
                deferred_queue_state="retry_wait",
                refill_not_before_at="2000-01-01 00:00:00",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()

            result = enricher.queue_background_profile_prefetch(
                candidates=[],
                extra_profile_urls=[],
                snapshot_dir=root,
                job_id="job_refill_retry_counted_owned_gate",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["reason"], "retry_wait_blocked_by_normal_profile_wave")
        gate = dict(result.get("retry_wait_gate") or {})
        self.assertFalse(gate["retry_allowed"])
        self.assertEqual(gate["reason"], "normal_provider_owned_items_unresolved")
        self.assertEqual(gate["normal_owned_stale_item_count"], 1)
        self.assertEqual(gate["inspected_owned_item_count"], 1)

    def test_queue_background_profile_prefetch_coalesces_unexplained_tiny_live_batches(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / f"tiny-{len(dispatched_chunks)}.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            candidates = [
                Candidate(
                    candidate_id=f"tiny_{index}",
                    name_en=f"Tiny {index}",
                    display_name=f"Tiny {index}",
                    linkedin_url=f"https://www.linkedin.com/in/tiny-live-{index}/",
                )
                for index in range(50)
            ]

            with (
                mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "live"}, clear=False),
                mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window,
            ):
                window.return_value = {
                    "batch_size": 3,
                    "max_workers": 1,
                    "batch_count": 17,
                    "source_mix": {"other": 50},
                    "strategy": "test_unexplained_tiny_batch",
                    "priority": True,
                }
                result = enricher.queue_background_profile_prefetch(
                    candidates=candidates,
                    snapshot_dir=root,
                    job_id="job_unexplained_tiny_batch",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 1,
                        "harvest_profile_batch_submit_global_inflight": 1,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                )

        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [50])
        self.assertEqual(result["tiny_batch_coalesced_count"], 0)
        batch_plan = dict(result.get("batch_plan") or {})
        self.assertEqual(batch_plan["kind"], "profile_prefetch_batch_plan")
        self.assertEqual(batch_plan["item_store"], "linkedin_profile_registry")
        self.assertEqual(batch_plan["queue_item_count"], 50)
        self.assertEqual(batch_plan["planned_dispatch_item_count"], 50)
        self.assertEqual(batch_plan["planned_deferred_item_count"], 0)
        self.assertEqual(batch_plan["available_slot_count"], 1)
        self.assertEqual(batch_plan["planned_new_worker_count"], 1)
        self.assertEqual(batch_plan["unfilled_available_slot_count"], 0)
        self.assertEqual(batch_plan["refill_saturation"], "filled_available_slots")
        self.assertEqual(result["tiny_batch_count"], 0)
        self.assertEqual(result["unexplained_tiny_batch_count"], 0)
        envelope = dict(list(result.get("batch_envelopes") or [])[0])
        self.assertFalse(envelope["is_tiny_batch"])
        self.assertTrue(envelope["tiny_batch_allowed"])
        self.assertEqual(envelope["small_batch_reason"], "")

    def test_harvest_profile_batch_worker_defers_tiny_after_cache_filter_when_backlog_exists(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("tiny filtered dispatch should defer before provider submit")

        class _Store:
            def __init__(self, raw_paths_by_url):
                self.raw_paths_by_url = dict(raw_paths_by_url)
                self.repos = SimpleNamespace(linkedin_profile_registry=self)

            def get_agent_worker(self, *args, **kwargs):
                return None

            def get_bulk(self, profile_urls):
                rows = {}
                for profile_url in list(profile_urls or []):
                    raw_path = self.raw_paths_by_url.get(profile_url)
                    if raw_path:
                        rows[normalize_linkedin_profile_url_key(profile_url)] = {
                            "profile_url": profile_url,
                            "status": "fetched",
                            "last_raw_path": str(raw_path),
                        }
                return rows

            def acquire_lease(self, profile_url, **kwargs):
                return {"acquired": True, "lease_owner": "test", "lease_token": f"lease-{profile_url}"}

            def release_lease(self, *args, **kwargs):
                return True

            def mark_fetched(self, *args, **kwargs):
                return True

        class _WorkerRuntime:
            pass

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=None,
            )
            profile_urls = [
                f"https://www.linkedin.com/in/filter-to-tiny-{index}/" for index in range(22)
            ]
            raw_paths_by_url = {}
            for index, profile_url in enumerate(profile_urls[:17]):
                raw_path = root / f"cached-{index}.json"
                raw_path.write_text(
                    json.dumps(
                        {
                            "profileUrl": profile_url,
                            "firstName": "Cached",
                            "lastName": str(index),
                            "headline": "Cached profile",
                        }
                    ),
                    encoding="utf-8",
                )
                raw_paths_by_url[profile_url] = raw_path
            enricher.store = _Store(raw_paths_by_url)
            enricher.worker_runtime = _WorkerRuntime()
            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=profile_urls,
                snapshot_dir=root,
                job_id="job_filter_to_tiny",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                prefetch_batch_context={
                    "requested_url_count": 297,
                    "candidate_count": 297,
                    "planned_deferred_url_count": 43,
                    "planned_dispatch_worker_count": 3,
                },
            )

        self.assertEqual(result["worker_status"], "backpressure")
        summary = dict(result.get("summary") or {})
        self.assertEqual(summary["message"], "harvest_profile_tiny_batch_deferred_for_coalescing")
        self.assertEqual(summary["small_batch_reason"], "normal_batch_under_target_with_deferred_backlog")
        self.assertEqual(len(summary["deferred_urls"]), 5)

    def test_harvest_profile_batch_worker_allows_cache_filtered_full_actor_slot(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.submitted_urls: list[list[str]] = []

            def execute_batch_with_checkpoint(self, profile_urls, *args, **kwargs):
                self.submitted_urls.append(list(profile_urls))
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={"run_id": "run-cache-filtered", "dataset_id": "dataset-cache-filtered"},
                    pending=True,
                    message="queued",
                )

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _Connector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            profile_urls = [
                f"https://www.linkedin.com/in/cache-filtered-slot-{index:02d}/"
                for index in range(50)
            ]
            for index, profile_url in enumerate(profile_urls[:22]):
                raw_path = root / f"cached-full-slot-{index}.json"
                raw_path.write_text(
                    json.dumps(
                        {
                            "profileUrl": profile_url,
                            "firstName": "Cached",
                            "lastName": str(index),
                            "headline": "Cached profile",
                        }
                    ),
                    encoding="utf-8",
                )
                store.repos.linkedin_profile_registry.mark_fetched(
                    profile_url,
                    raw_path=str(raw_path),
                    source_shards=["enrichment_background_prefetch"],
                    source_jobs=["job_cache_filtered_slot"],
                    snapshot_dir=str(root),
                )

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=profile_urls,
                snapshot_dir=root,
                job_id="job_cache_filtered_slot",
                request_payload={
                    "harvest_profile_actor_global_inflight": 4,
                    "harvest_profile_batch_submit_global_inflight": 4,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                prefetch_batch_context={
                    "requested_url_count": 140,
                    "candidate_count": 140,
                    "planned_deferred_url_count": 50,
                    "planned_dispatch_worker_count": 3,
                },
            )

        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(len(connector.submitted_urls), 1)
        self.assertEqual(connector.submitted_urls[0], profile_urls[22:])
        summary = dict(result.get("summary") or {})
        self.assertEqual(summary["small_batch_reason"], "cache_filtered_actor_slot")
        self.assertEqual(summary["dispatched_url_count"], 28)

    def test_harvest_profile_batch_worker_defers_tiny_tail_when_sibling_worker_active(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("unproven tiny final tail should defer before provider submit")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=None,
            )
            enricher.worker_runtime = object()
            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=["https://www.linkedin.com/in/tail-one/"],
                snapshot_dir=root,
                job_id="job_unproven_tail",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                prefetch_batch_context={
                    "requested_url_count": 297,
                    "candidate_count": 297,
                    "planned_deferred_url_count": 0,
                    "planned_dispatch_worker_count": 2,
                },
            )

        self.assertEqual(result["worker_status"], "backpressure")
        summary = dict(result.get("summary") or {})
        self.assertEqual(summary["message"], "harvest_profile_tiny_batch_deferred_for_coalescing")
        self.assertEqual(summary["small_batch_reason"], "final_tail_unproven")
        self.assertEqual(summary["deferred_urls"], ["https://www.linkedin.com/in/tail-one/"])

    def test_harvest_profile_batch_worker_defers_subtarget_tail_when_sibling_worker_active(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("sub-target normal tail must wait for coalescing/quiescence")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=None,
            )
            enricher.worker_runtime = object()
            profile_urls = [
                f"https://www.linkedin.com/in/subtarget-tail-{index:02d}/"
                for index in range(24)
            ]
            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=profile_urls,
                snapshot_dir=root,
                job_id="job_subtarget_tail",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                prefetch_batch_context={
                    "requested_url_count": 74,
                    "candidate_count": 74,
                    "planned_deferred_url_count": 0,
                    "planned_dispatch_worker_count": 2,
                },
            )

        self.assertEqual(result["worker_status"], "backpressure")
        summary = dict(result.get("summary") or {})
        self.assertEqual(summary["message"], "harvest_profile_tiny_batch_deferred_for_coalescing")
        self.assertEqual(summary["small_batch_reason"], "final_tail_unproven")
        self.assertEqual(summary["deferred_urls"], profile_urls)

    def test_harvest_profile_batch_worker_defers_fresh_tiny_tail_until_coalescing_window(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("fresh tiny final tail should wait for coalescing window")

        class _Store:
            def __init__(self) -> None:
                self.deferred_urls: list[str] = []
                self.released_urls: list[str] = []
                self.repos = SimpleNamespace(linkedin_profile_registry=self)

            def get_agent_worker(self, *args, **kwargs):
                return None

            def get_bulk(self, profile_urls):
                return {}

            def acquire_lease(self, profile_url, **kwargs):
                return {"acquired": True, "lease_owner": "test", "lease_token": f"lease-{profile_url}"}

            def release_lease(self, profile_url, **kwargs):
                self.released_urls.append(profile_url)
                return True

            def mark_deferred_for_coalescing(self, profile_url, **kwargs):
                self.deferred_urls.append(profile_url)
                return {"profile_url": profile_url, "status": "deferred_coalescing"}

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = _Store()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = object()
            profile_url = "https://www.linkedin.com/in/fresh-tiny-tail/"
            with mock.patch("sourcing_agent.enrichment.HARVEST_PROFILE_TINY_TAIL_COALESCING_MIN_AGE_MS", 10_000):
                result = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=[profile_url],
                    snapshot_dir=root,
                    job_id="job_fresh_tail",
                    request_payload={},
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    prefetch_batch_context={
                        "requested_url_count": 100,
                        "candidate_count": 100,
                        "planned_deferred_url_count": 0,
                        "planned_dispatch_worker_count": 1,
                    },
                )

        self.assertEqual(result["worker_status"], "backpressure")
        summary = dict(result.get("summary") or {})
        self.assertEqual(summary["small_batch_reason"], "final_tail_unproven")
        self.assertEqual(summary["tiny_tail_coalescing_min_age_ms"], 10_000)
        self.assertIsNone(summary["oldest_deferred_coalescing_age_ms"])
        self.assertEqual(store.deferred_urls, [profile_url])
        self.assertEqual(store.released_urls, [])

    def test_harvest_profile_batch_worker_schedules_tiny_tail_in_registry_without_worker(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("fresh tiny final tail should not submit before registry timer is ready")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            profile_url = "https://www.linkedin.com/in/durable-tiny-tail/"
            request = JobRequest(raw_user_request="Find OpenAI infra people", target_company="OpenAI")

            with mock.patch("sourcing_agent.enrichment.HARVEST_PROFILE_TINY_TAIL_COALESCING_MIN_AGE_MS", 60_000):
                result = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=[profile_url],
                    snapshot_dir=root,
                    job_id="job_durable_tail",
                    request_payload=request.to_record(),
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    prefetch_batch_context={
                        "requested_url_count": 100,
                        "candidate_count": 100,
                        "planned_deferred_url_count": 0,
                        "planned_dispatch_worker_count": 1,
                    },
                )
            summary = dict(result.get("summary") or {})
            workers = store.list_agent_workers(job_id="job_durable_tail", lane_id="enrichment_specialist")
            registry = store.repos.linkedin_profile_registry.get(profile_url)
            future_ready_items = store.repos.linkedin_profile_registry.list_refill_queue_items(
                states=["deferred_coalescing"],
                source_job="job_durable_tail",
                snapshot_dir=str(root),
                limit=10,
            )

        self.assertEqual(result["worker_status"], "backpressure")
        self.assertEqual(summary["coalescing_scheduler"], "linkedin_profile_registry")
        self.assertEqual(summary["coalescing_refill_items"]["status"], "recorded")
        self.assertEqual(summary["coalescing_refill_items"]["deferred_queue_state"], "deferred_coalescing")
        self.assertEqual(summary["coalescing_refill_items"]["refill_not_before_at"], summary["coalescing_not_before_at"])
        self.assertEqual(workers, [])
        self.assertEqual(future_ready_items, [])
        assert registry is not None
        self.assertEqual(registry["status"], "deferred_coalescing")
        self.assertEqual(registry["refill_queue_state"], "deferred_coalescing")
        self.assertEqual(registry["last_refill_trigger_kind"], "profile_tiny_tail_coalescing")
        self.assertEqual(registry["last_refill_plan_reason"], "tiny_tail_coalescing_wait")
        self.assertEqual(registry["last_refill_deferred_reason"], "final_tail_unproven")
        self.assertEqual(registry["last_refill_attempt_count"], 0)
        self.assertEqual(registry["refill_not_before_at"], summary["coalescing_not_before_at"])

    def test_harvest_profile_batch_worker_allows_tiny_tail_after_coalescing_window(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.submitted_urls: list[list[str]] = []

            def execute_batch_with_checkpoint(self, profile_urls, *args, **kwargs):
                self.submitted_urls.append(list(profile_urls))
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={"run_id": "run-tail", "dataset_id": "dataset-tail"},
                    pending=True,
                    message="queued",
                )

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _Connector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            profile_url = "https://www.linkedin.com/in/aged-tiny-tail/"
            store.repos.linkedin_profile_registry.mark_deferred_for_coalescing(
                profile_url,
                reason="final_tail_unproven",
                source_shards=["enrichment_background_prefetch"],
                source_jobs=["job_aged_tail"],
                snapshot_dir=str(root),
            )
            with (
                mock.patch("sourcing_agent.enrichment.HARVEST_PROFILE_TINY_TAIL_COALESCING_MIN_AGE_MS", 10_000),
                mock.patch(
                    "sourcing_agent.enrichment._profile_prefetch_oldest_deferred_coalescing_age_ms",
                    return_value=20_000,
                ),
            ):
                result = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=[profile_url],
                    snapshot_dir=root,
                    job_id="job_aged_tail",
                    request_payload={},
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    prefetch_batch_context={
                        "requested_url_count": 100,
                        "candidate_count": 100,
                        "planned_deferred_url_count": 0,
                        "planned_dispatch_worker_count": 1,
                    },
                )
            registry_entry = store.repos.linkedin_profile_registry.get(profile_url) or {}

        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(connector.submitted_urls, [[profile_url]])
        summary = dict(result.get("summary") or {})
        self.assertEqual(summary["queued_urls"], [profile_url])
        self.assertEqual(summary["run_id"], "run-tail")
        self.assertEqual(registry_entry["refill_queue_state"], "planned_dispatch")
        self.assertEqual(registry_entry["last_refill_plan_reason"], "remote_provider_submitted")
        self.assertEqual(registry_entry["last_refill_attempt_count"], 1)

    def test_harvest_profile_batch_worker_bulk_marks_queued_after_submit(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.submitted_urls: list[list[str]] = []

            def execute_batch_with_checkpoint(self, profile_urls, *args, **kwargs):
                self.submitted_urls.append(list(profile_urls))
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={"run_id": "run-bulk-submit", "dataset_id": "dataset-bulk-submit"},
                    pending=True,
                    message="queued",
                )

        class _SpyStore(ControlPlaneStore):
            def __init__(self, db_path: str) -> None:
                super().__init__(db_path)
                self.queued_many_calls: list[list[str]] = []
                self.queued_single_calls: list[str] = []
                registry_repo = self.repos.linkedin_profile_registry
                original_mark_queued_many = registry_repo.mark_queued_many
                original_mark_queued = registry_repo.mark_queued

                def _spy_mark_queued_many(profile_urls, **kwargs):
                    self.queued_many_calls.append(list(profile_urls))
                    return original_mark_queued_many(profile_urls, **kwargs)

                def _spy_mark_queued(profile_url, **kwargs):
                    self.queued_single_calls.append(str(profile_url))
                    return original_mark_queued(profile_url, **kwargs)

                registry_repo.mark_queued_many = _spy_mark_queued_many
                registry_repo.mark_queued = _spy_mark_queued

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = _SpyStore(str(root / "control_plane.db"))
            self.addCleanup(store.close)
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _Connector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            profile_urls = [
                f"https://www.linkedin.com/in/bulk-submit-{index:04d}/" for index in range(50)
            ]
            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=profile_urls,
                snapshot_dir=root,
                job_id="job_bulk_submit",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                prefetch_batch_context={
                    "requested_url_count": 100,
                    "candidate_count": 100,
                    "planned_deferred_url_count": 0,
                    "planned_dispatch_worker_count": 1,
                },
            )

        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(connector.submitted_urls, [profile_urls])
        self.assertEqual(store.queued_many_calls, [profile_urls])
        self.assertEqual(store.queued_single_calls, [])

    def test_harvest_profile_batch_provider_failure_completes_envelope_and_leaves_url_retry(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, profile_urls, *args, **kwargs):
                raise RuntimeError("scripted profile fixture missing requested profile URLs")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            profile_url = "https://www.linkedin.com/in/provider-failure-terminal/"

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=[profile_url],
                snapshot_dir=root,
                job_id="job_provider_failure_terminal",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                prefetch_batch_context={
                    "requested_url_count": 1,
                    "candidate_count": 1,
                    "planned_deferred_url_count": 0,
                    "planned_dispatch_worker_count": 1,
                },
            )
            workers = worker_runtime.list_workers(job_id="job_provider_failure_terminal")
            registry_entry = store.repos.linkedin_profile_registry.get(profile_url) or {}

        self.assertEqual(result["worker_status"], "completed")
        self.assertEqual(result["terminal_envelope_outcome"], "provider_failed_url_retry_recorded")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["status"], "completed")
        summary = dict(dict(workers[0]["output"]).get("summary") or {})
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["unresolved_url_count"], 1)
        self.assertEqual(summary["persisted_profile_count"], 0)
        self.assertEqual(registry_entry["status"], "failed_retryable")
        self.assertEqual(registry_entry["refill_queue_state"], "retry_wait")
        self.assertEqual(registry_entry["retry_count"], 1)

    def test_queue_background_profile_prefetch_does_not_create_worker_when_provider_slot_full(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("provider submit should not run when the provider slot is full")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            store.acquire_runtime_provider_limiter_slot(
                "harvest_profile_scraper_actor",
                lease_owner="existing-remote-actor",
                budget=1,
                lease_seconds=120,
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            result = enricher.queue_background_profile_prefetch(
                candidates=[
                    Candidate(
                        candidate_id="provider_full",
                        name_en="Provider Full",
                        display_name="Provider Full",
                        linkedin_url="https://www.linkedin.com/in/provider-full/",
                    )
                ],
                snapshot_dir=root,
                job_id="job_provider_slot_full",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )
            workers = worker_runtime.list_workers(job_id="job_provider_slot_full")
            registry_entry = store.repos.linkedin_profile_registry.get("https://www.linkedin.com/in/provider-full/") or {}
            ready_items = store.repos.linkedin_profile_registry.list_refill_queue_items(
                states=["deferred_budget"],
                source_job="job_provider_slot_full",
                snapshot_dir=str(root),
                limit=10,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["queued_worker_count"], 0)
        self.assertEqual(result["dispatched_url_count"], 0)
        self.assertEqual(result["deferred_url_count"], 1)
        self.assertEqual(workers, [])
        self.assertEqual(registry_entry["refill_queue_state"], "deferred_budget")
        self.assertEqual(registry_entry["last_refill_plan_reason"], "provider_limiter_backpressure")
        self.assertEqual(registry_entry["last_refill_deferred_reason"], "provider_limiter_backpressure")
        self.assertEqual(registry_entry["last_refill_attempt_count"], 0)
        self.assertEqual([item["profile_url"] for item in ready_items], ["https://www.linkedin.com/in/provider-full/"])

    def test_harvest_profile_batch_resume_without_limiter_lease_does_not_block_on_new_slot(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.checkpoints: list[dict[str, object]] = []

            def execute_batch_with_checkpoint(self, profile_urls, *args, checkpoint=None, **kwargs):
                self.checkpoints.append(dict(checkpoint or {}))
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-resume-without-lease",
                        "dataset_id": "dataset-resume-without-lease",
                    },
                    pending=True,
                    message="still running",
                )

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            worker_runtime = AgentRuntimeCoordinator(store)
            connector = _Connector()
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=connector,
                store=store,
            )
            enricher.worker_runtime = worker_runtime
            profile_urls = ["https://www.linkedin.com/in/resume-without-limiter-lease/"]
            payload_hash = sha1(json.dumps(sorted(profile_urls), ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
            worker_handle = worker_runtime.begin_worker(
                job_id="job_resume_without_limiter_lease",
                request=JobRequest(raw_user_request="Find OpenAI infra people", target_company="OpenAI"),
                plan_payload={},
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::{payload_hash}",
                stage="enriching",
                span_name=f"harvest_profile_batch:{payload_hash}",
                budget_payload={"requested_url_count": len(profile_urls)},
                input_payload={"profile_urls": profile_urls},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(root),
                    "profile_urls": profile_urls,
                    "prefetch_batch_context": {"nonblocking_submit": True},
                },
                handoff_from_lane="acquisition_specialist",
            )
            worker_runtime.checkpoint_worker(
                worker_handle,
                status="queued",
                checkpoint_payload={
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-resume-without-lease",
                    "dataset_id": "dataset-resume-without-lease",
                    "payload_hash": payload_hash,
                    "prefetch_batch_context": {"nonblocking_submit": True},
                },
                output_payload={"summary": {"status": "queued", "queued_urls": profile_urls}},
            )

            with mock.patch(
                "sourcing_agent.enrichment.acquire_runtime_provider_limiter_slot",
                side_effect=AssertionError("resuming an existing remote run must not block on a new provider slot"),
            ) as acquire_mock:
                result = enricher._execute_harvest_profile_batch_worker(
                    profile_urls=profile_urls,
                    snapshot_dir=root,
                    job_id="job_resume_without_limiter_lease",
                    request_payload={
                        "raw_user_request": "Find OpenAI infra people",
                        "target_company": "OpenAI",
                        "harvest_profile_actor_global_inflight": 1,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    prefetch_batch_context={"nonblocking_submit": True},
                )

        acquire_mock.assert_not_called()
        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(connector.checkpoints[0]["run_id"], "run-resume-without-lease")

    def test_queue_background_profile_prefetch_dispatch_claim_is_daemon_recoverable_after_ttl(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            profile_url = "https://www.linkedin.com/in/dispatch-claim-recoverable/"

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                return {
                    "worker_status": "backpressure",
                    "summary": {
                        "deferred_urls": list(kwargs.get("profile_urls") or []),
                        "message": "provider_submit_in_progress_window",
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

            with (
                mock.patch("sourcing_agent.enrichment.HARVEST_PROFILE_DISPATCH_CLAIM_TTL_SECONDS", 60),
                mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window,
            ):
                window.return_value = {
                    "batch_size": 10,
                    "max_workers": 1,
                    "batch_count": 1,
                    "source_mix": {"other": 1},
                    "strategy": "test_dispatch_claim",
                    "priority": True,
                }
                result = enricher.queue_background_profile_prefetch(
                    candidates=[
                        Candidate(
                            candidate_id="dispatch_claim",
                            name_en="Dispatch Claim",
                            display_name="Dispatch Claim",
                            linkedin_url=profile_url,
                        )
                    ],
                    snapshot_dir=root,
                    job_id="job_dispatch_claim_recoverable",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 1,
                        "harvest_profile_batch_submit_global_inflight": 1,
                    },
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                )
            registry_entry = store.repos.linkedin_profile_registry.get(profile_url) or {}
            immediate_ready_items = store.repos.linkedin_profile_registry.list_refill_queue_items(
                states=["dispatch_claimed"],
                source_job="job_dispatch_claim_recoverable",
                snapshot_dir=str(root),
                limit=10,
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=[profile_url],
                source_jobs=["job_dispatch_claim_recoverable"],
                snapshot_dir=str(root),
                trigger_kind="profile_prefetch_refill",
                plan_reason="ready_to_dispatch",
                active_queue_state="dispatch_claimed",
                active_reason="provider_submit_claimed",
                active_refill_not_before_at="2000-01-01 00:00:00",
            )
            expired_ready_items = store.repos.linkedin_profile_registry.list_refill_queue_items(
                states=["dispatch_claimed"],
                source_job="job_dispatch_claim_recoverable",
                snapshot_dir=str(root),
                limit=10,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["queued_worker_count"], 0)
        self.assertEqual(result["deferred_url_count"], 1)
        self.assertEqual(registry_entry["refill_queue_state"], "deferred_budget")
        self.assertEqual(immediate_ready_items, [])
        self.assertEqual([item["profile_url"] for item in expired_ready_items], [profile_url])

    def test_queue_background_profile_prefetch_can_use_registry_only_cache_markers(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def execute_batch_with_checkpoint(self, *args, **kwargs):
                raise AssertionError("registry-only cache hit should not submit provider actor")

        class _Store:
            def __init__(self, raw_path: Path) -> None:
                self.raw_path = raw_path
                self.repos = SimpleNamespace(linkedin_profile_registry=self)

            def get_bulk(self, profile_urls):
                return {
                    normalize_linkedin_profile_url_key(profile_url): {
                        "profile_url": profile_url,
                        "status": "fetched",
                        "last_raw_path": str(self.raw_path),
                    }
                    for profile_url in list(profile_urls or [])
                }

        class _WorkerRuntime:
            def list_workers(self, *, job_id="", session_id=0, lane_id=""):
                return []

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            raw_path = root / "already_fetched_profile.json"
            raw_path.write_text("{not valid json and should not be read", encoding="utf-8")
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=_Store(raw_path),
            )
            enricher.worker_runtime = _WorkerRuntime()
            result = enricher.queue_background_profile_prefetch(
                candidates=[
                    Candidate(
                        candidate_id="cached_candidate",
                        name_en="Cached Candidate",
                        display_name="Cached Candidate",
                        linkedin_url="https://www.linkedin.com/in/cached-candidate/",
                    )
                ],
                snapshot_dir=root,
                job_id="job_registry_only_prefetch",
                request_payload={},
                plan_payload={},
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
                load_cached_profile_payloads=False,
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["reason"], "reused_local_raw_cache")
        self.assertEqual(result["cached_profile_count"], 1)
        self.assertEqual(result["dispatched_url_count"], 0)
        self.assertEqual(result["registry_cache_marker_count"], 1)
        self.assertEqual(result["metrics"]["registry_cache_marker_count"], 1)
        self.assertEqual(result["metrics"]["cached_profile_payload_count"], 0)
        self.assertFalse(result["metrics"]["load_cached_profile_payloads"])
        self.assertIn("prefetch_elapsed_ms", result["metrics"])

    def test_profile_prefetch_refill_dispatch_uses_durable_item_ownership_without_worker_scan(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            profile_urls = [
                f"https://www.linkedin.com/in/refill-dispatch-hot-path-{index}/"
                for index in range(4)
            ]
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                deferred_profile_urls=profile_urls,
                source_jobs=["job_refill_hot_path"],
                snapshot_dir=str(root),
                plan_reason="worker_budget_deferred",
                deferred_reason="worker_budget_deferred",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=store)
            enricher.worker_runtime = object()
            dispatched_chunks: list[list[str]] = []

            def _fail_if_worker_state_scanned(*args, **kwargs):
                raise AssertionError("durable refill ownership should avoid worker-state scan")

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                chunk = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(chunk)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "worker_id": len(dispatched_chunks),
                        "run_id": f"run-{len(dispatched_chunks)}",
                        "dataset_id": f"dataset-{len(dispatched_chunks)}",
                        "queued_urls": chunk,
                        "summary_path": str(root / f"chunk-{len(dispatched_chunks)}.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._queued_profile_registry_entry_has_active_worker = _fail_if_worker_state_scanned
            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            with mock.patch("sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window") as window:
                window.return_value = {
                    "batch_size": 2,
                    "max_workers": 2,
                    "batch_count": 2,
                    "source_mix": {"other": 4},
                    "strategy": "test_refill_hot_path",
                    "priority": True,
                }
                result = enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=[],
                    snapshot_dir=root,
                    job_id="job_refill_hot_path",
                    request_payload={
                        "harvest_profile_actor_global_inflight": 2,
                        "harvest_profile_batch_submit_global_inflight": 2,
                    },
                    plan_payload={},
                    runtime_mode="daemon_refill",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["queued_worker_count"], 1)
        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [4])
        self.assertEqual(result["dispatched_url_count"], 4)

    def test_harvest_profile_batch_worker_uses_dispatch_claim_without_worker_scan(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.submitted_urls: list[list[str]] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                self.submitted_urls.append(list(profile_urls))
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-claimed",
                        "dataset_id": "dataset-claimed",
                        "status": "submitted",
                    },
                    pending=True,
                    message="queued",
                    artifacts=[],
                )

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            profile_url = "https://www.linkedin.com/in/dispatch-claimed-hot-path/"
            owner_payload_hash = sha1(
                json.dumps([profile_url], ensure_ascii=False).encode("utf-8")
            ).hexdigest()[:16]
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=[profile_url],
                source_jobs=["job_dispatch_claim_hot_path"],
                snapshot_dir=str(root),
                trigger_kind="profile_prefetch_refill",
                plan_reason="ready_to_dispatch",
                active_queue_state="dispatch_claimed",
                active_reason="provider_submit_claimed",
                active_refill_not_before_at="2099-01-01 00:00:00",
                active_owner_payload_hash=owner_payload_hash,
            )
            connector = _Connector()
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=connector, store=store)
            enricher.worker_runtime = AgentRuntimeCoordinator(store)

            def _fail_if_worker_state_scanned(*args, **kwargs):
                raise AssertionError("dispatch-claimed item should bypass active-worker scan for its own submit")

            enricher._queued_profile_registry_entry_has_active_worker = _fail_if_worker_state_scanned
            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=[profile_url],
                snapshot_dir=root,
                job_id="job_dispatch_claim_hot_path",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="daemon_refill",
                allow_shared_provider_cache=True,
            )
            registry_entry = store.repos.linkedin_profile_registry.get(profile_url) or {}

        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(connector.submitted_urls, [[profile_url]])
        self.assertEqual(registry_entry["refill_queue_state"], "planned_dispatch")
        self.assertEqual(registry_entry["refill_owner_run_id"], "run-claimed")

    def test_profile_prefetch_refill_handoff_does_not_hydrate_cached_payloads(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.submitted_urls: list[list[str]] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                self.submitted_urls.append(list(profile_urls))
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-refill-no-hydrate",
                        "dataset_id": "dataset-refill-no-hydrate",
                        "status": "submitted",
                    },
                    pending=True,
                    message="queued",
                    artifacts=[],
                )

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            cached_marker_url = "https://www.linkedin.com/in/refill-marker-only/"
            dispatch_url = "https://www.linkedin.com/in/refill-submit-no-hydrate/"
            invalid_raw_path = root / "invalid_cached_profile.json"
            invalid_raw_path.write_text("{not valid json and must not be read", encoding="utf-8")
            store.repos.linkedin_profile_registry.mark_fetched(
                cached_marker_url,
                raw_path=str(invalid_raw_path),
                source_jobs=["job_refill_no_hydrate"],
                snapshot_dir=str(root),
            )
            store.repos.linkedin_profile_registry.record_refill_plan_items(
                deferred_profile_urls=[dispatch_url],
                source_jobs=["job_refill_no_hydrate"],
                snapshot_dir=str(root),
                plan_reason="worker_budget_deferred",
                deferred_reason="worker_budget_deferred",
            )
            connector = _Connector()
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=connector, store=store)
            enricher.worker_runtime = AgentRuntimeCoordinator(store)

            result = enricher.queue_background_profile_prefetch(
                candidates=[],
                extra_profile_urls=[cached_marker_url],
                snapshot_dir=root,
                job_id="job_refill_no_hydrate",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="daemon_refill",
                allow_shared_provider_cache=True,
                priority=True,
                load_cached_profile_payloads=False,
            )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["cached_profile_count"], 1)
        self.assertEqual(result["registry_cache_marker_count"], 1)
        self.assertEqual(result["metrics"]["cached_profile_payload_count"], 0)
        self.assertEqual(result["dispatched_url_count"], 1)
        self.assertEqual(connector.submitted_urls, [[dispatch_url]])

    def test_harvest_profile_batch_worker_uses_batch_registry_lease_for_submit_handoff(self) -> None:
        class _Connector:
            settings = type("_Settings", (), {"enabled": True})()

            def __init__(self) -> None:
                self.submitted_urls: list[list[str]] = []

            def execute_batch_with_checkpoint(self, profile_urls, snapshot_dir, checkpoint=None, **kwargs):
                self.submitted_urls.append(list(profile_urls))
                return HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={
                        **dict(checkpoint or {}),
                        "run_id": "run-batch-lease",
                        "dataset_id": "dataset-batch-lease",
                        "status": "submitted",
                    },
                    pending=True,
                    message="queued",
                    artifacts=[],
                )

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            store = self.make_pg_store(str(root / "control_plane.db"))
            batch_acquire_calls: list[list[str]] = []
            single_acquire_calls: list[str] = []
            batch_release_calls: list[list[str]] = []
            original_batch_acquire = store.repos.linkedin_profile_registry.acquire_leases
            original_single_acquire = store.repos.linkedin_profile_registry.acquire_lease
            original_batch_release = store.repos.linkedin_profile_registry.release_leases

            def _record_batch_acquire(profile_urls, **kwargs):
                batch_acquire_calls.append(list(profile_urls or []))
                return original_batch_acquire(profile_urls, **kwargs)

            def _record_single_acquire(profile_url, **kwargs):
                single_acquire_calls.append(str(profile_url or ""))
                return original_single_acquire(profile_url, **kwargs)

            def _record_batch_release(profile_urls, **kwargs):
                batch_release_calls.append(list(profile_urls or []))
                return original_batch_release(profile_urls, **kwargs)

            store.repos.linkedin_profile_registry.acquire_leases = _record_batch_acquire  # type: ignore[method-assign]
            store.repos.linkedin_profile_registry.acquire_lease = _record_single_acquire  # type: ignore[method-assign]
            store.repos.linkedin_profile_registry.release_leases = _record_batch_release  # type: ignore[method-assign]
            connector = _Connector()
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=connector, store=store)
            enricher.worker_runtime = AgentRuntimeCoordinator(store)
            profile_urls = [
                f"https://www.linkedin.com/in/batch-lease-{index:02d}/"
                for index in range(50)
            ]

            result = enricher._execute_harvest_profile_batch_worker(
                profile_urls=profile_urls,
                snapshot_dir=root,
                job_id="job_batch_lease_submit",
                request_payload={
                    "harvest_profile_actor_global_inflight": 1,
                    "harvest_profile_batch_submit_global_inflight": 1,
                },
                plan_payload={},
                runtime_mode="daemon_refill",
                allow_shared_provider_cache=True,
                prefetch_batch_context={
                    "requested_url_count": 50,
                    "candidate_count": 50,
                    "planned_deferred_url_count": 0,
                    "planned_dispatch_worker_count": 1,
                },
            )

        self.assertEqual(result["worker_status"], "queued")
        self.assertEqual(connector.submitted_urls, [profile_urls])
        self.assertEqual(batch_acquire_calls, [profile_urls])
        self.assertEqual(single_acquire_calls, [])
        self.assertEqual(batch_release_calls, [profile_urls])

    def test_profile_search_prefetch_defaults_to_parallel_actor_submission_for_medium_batches(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], store=self.make_pg_store(str(root / "control_plane.db")))
            enricher.worker_runtime = object()
            enricher.harvest_profile_connector = mock.Mock(settings=type("_Settings", (), {"enabled": True})())
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / f"chunk_{len(dispatched_chunks)}.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            candidates = [
                Candidate(
                    candidate_id=f"former_{index}",
                    name_en=f"Former {index}",
                    display_name=f"Former {index}",
                    category="former_employee",
                    target_company="Lovable",
                    organization="Lovable",
                    employment_status="former",
                    role="Former Lovable member",
                    linkedin_url=f"https://www.linkedin.com/in/lovable-former-{index}/",
                    source_dataset="lovable_search_seed_candidates",
                    source_path=str(root / "search_seed_discovery" / "harvest_profile_search" / "summary.json"),
                    metadata={
                        "seed_source_type": "harvest_profile_search",
                        "seed_query": "Lovable former members",
                    },
                )
                for index in range(173)
            ]

            with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "live"}, clear=False):
                result = enricher.queue_background_profile_prefetch(
                    candidates=candidates,
                    snapshot_dir=root,
                    job_id="job_lovable_former_prefetch",
                    request_payload={},
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["queued_worker_count"], 1)
        self.assertEqual(result["deferred_url_count"], 0)
        self.assertGreaterEqual(result["submit_budget"], 4)
        self.assertEqual(result["recommended_batch_size"], 173)
        self.assertEqual(result["recommended_max_workers"], 1)
        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [173])

    def test_profile_search_prefetch_uses_adaptive_live_chunks_before_global_submit_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], store=self.make_pg_store(str(root / "control_plane.db")))
            enricher.worker_runtime = object()
            enricher.harvest_profile_connector = mock.Mock(settings=type("_Settings", (), {"enabled": True})())
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / f"chunk_{len(dispatched_chunks)}.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            candidates = [
                Candidate(
                    candidate_id=f"former_tail_{index}",
                    name_en=f"Former Tail {index}",
                    display_name=f"Former Tail {index}",
                    category="former_employee",
                    target_company="Lovable",
                    organization="Lovable",
                    employment_status="former",
                    role="Former Lovable member",
                    linkedin_url=f"https://www.linkedin.com/in/lovable-former-tail-{index}/",
                    source_dataset="lovable_search_seed_candidates",
                    metadata={"seed_source_type": "harvest_profile_search"},
                )
                for index in range(365)
            ]

            with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "live"}, clear=False):
                result = enricher.queue_background_profile_prefetch(
                    candidates=candidates,
                    snapshot_dir=root,
                    job_id="job_lovable_former_tail_prefetch",
                    request_payload={},
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                )

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["queued_worker_count"], 2)
        self.assertEqual(result["deferred_url_count"], 0)
        self.assertEqual(result["recommended_batch_size"], 183)
        self.assertEqual(result["recommended_batch_count"], 2)
        self.assertEqual(result["recommended_max_workers"], 2)
        self.assertEqual(str(result["dispatch_strategy"]), "actor_slot_item_packing_live_prefetch_window")
        self.assertEqual([len(chunk) for chunk in dispatched_chunks], [183, 182])
        self.assertNotIn(73, [len(chunk) for chunk in dispatched_chunks])

    def test_scripted_prefetch_uses_live_like_window_for_profile_search_tail(self) -> None:
        source_shards_by_url = {
            f"https://www.linkedin.com/in/openai-agent-tail-{index}/": [
                "harvest_profile_search:openai:agent"
            ]
            for index in range(247)
        }

        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            window = _recommended_harvest_profile_prefetch_dispatch_window(
                247,
                priority=False,
                source_shards_by_url=source_shards_by_url,
            )

        self.assertEqual(window["strategy"], "actor_slot_item_packing_scripted_prefetch_window")
        # R1.b ("fewer larger envelopes"): a 51..provider-cap profile-search tail packs into
        # a single durable-unit envelope rather than the old 124x2 half-split.
        self.assertEqual(window["batch_size"], 247)
        self.assertEqual(window["batch_count"], 1)
        self.assertEqual(window["max_workers"], 1)
        self.assertEqual(window["batch_size_reason"], "single_durable_unit_ready_set")

    def test_enrich_full_roster_prefetch_uses_canonical_scheduler_without_tiny_parallel_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], store=self.make_pg_store(str(root / "control_plane.db")))
            enricher.worker_runtime = object()
            enricher.harvest_profile_connector = mock.Mock(
                settings=type("_Settings", (), {"enabled": True})()
            )
            identity = CompanyIdentity(
                requested_name="OpenAI",
                canonical_name="OpenAI",
                company_key="openai",
                linkedin_slug="openai",
                linkedin_company_url="https://www.linkedin.com/company/openai/",
            )
            candidates = [
                Candidate(
                    candidate_id=f"cand_{index}",
                    name_en=f"Candidate {index}",
                    display_name=f"Candidate {index}",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url=f"https://www.linkedin.com/in/openai-prefetch-{index}/",
                )
                for index in range(4)
            ]
            batch_contexts: list[dict] = []
            dispatched_chunks: list[list[str]] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                batch_contexts.append(dict(kwargs.get("prefetch_batch_context") or {}))
                profile_key = profile_urls[0].rstrip("/").split("/")[-1] if profile_urls else "empty"
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / f"{profile_key}.queue_summary.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker

            with mock.patch(
                "sourcing_agent.enrichment._recommended_harvest_profile_prefetch_batch_size",
                return_value=1,
            ):
                result = enricher.enrich(
                    identity,
                    root,
                    candidates,
                    JobRequest(
                        raw_user_request="Find OpenAI multimodal people",
                        target_company="OpenAI",
                        categories=["employee"],
                        employment_statuses=["current", "former"],
                        profile_detail_limit=4,
                        slug_resolution_limit=1,
                        execution_preferences={
                            "harvest_prefetch_submit_workers": 2,
                            "harvest_profile_batch_submit_global_inflight": 4,
                        },
                    ),
                    job_id="job_prefetch_parallel_submit",
                    full_roster_profile_prefetch=True,
                )

            self.assertEqual(result.stop_reason, "queued_background_harvest")
            self.assertEqual(result.queued_harvest_worker_count, 1)
            self.assertEqual([len(chunk) for chunk in dispatched_chunks], [4])
            self.assertEqual(len(batch_contexts), 1)
            self.assertEqual(batch_contexts[0]["requested_url_count"], 4)
            self.assertEqual(batch_contexts[0]["candidate_count"], 4)
            self.assertEqual(batch_contexts[0]["planned_dispatch_worker_count"], 1)
            self.assertTrue(batch_contexts[0]["nonblocking_submit"])
            profile_prefetch = dict(result.profile_prefetch or {})
            self.assertEqual(profile_prefetch["queued_worker_count"], 1)
            self.assertFalse(profile_prefetch["metrics"]["load_cached_profile_payloads"])
            self.assertEqual(profile_prefetch["ordinal_submit_gate"], "same_plan_chunk_order")
            self.assertEqual(dict(profile_prefetch["batch_plan"])["item_store"], "linkedin_profile_registry")
            self.assertEqual(dict(profile_prefetch["batch_plan"])["planned_dispatch_worker_count"], 1)

    def test_enrich_worker_prefetch_completed_hydrates_cache_without_direct_profile_fetch(self) -> None:
        class _NoDirectFetchConnector:
            settings = type("_Settings", (), {"enabled": True})()

            def fetch_profiles_by_urls(self, *args, **kwargs):  # noqa: ANN002, ANN003
                raise AssertionError("worker-backed prefetch completion must not call direct profile fetch")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[])
            enricher.worker_runtime = object()
            enricher.harvest_profile_connector = _NoDirectFetchConnector()
            identity = CompanyIdentity(
                requested_name="OpenAI",
                canonical_name="OpenAI",
                company_key="openai",
                linkedin_slug="openai",
                linkedin_company_url="https://www.linkedin.com/company/openai/",
            )
            candidate = Candidate(
                candidate_id="cached_prefetch",
                name_en="Cached Prefetch",
                display_name="Cached Prefetch",
                category="employee",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="current",
                role="Research Engineer",
                linkedin_url="https://www.linkedin.com/in/cached-prefetch/",
            )
            prefetch_calls: list[dict] = []
            resolver_payloads: list[dict] = []

            def _fake_queue_background_profile_prefetch(**kwargs):  # noqa: ANN003
                prefetch_calls.append(dict(kwargs))
                return {
                    "status": "completed",
                    "reason": "reused_local_raw_cache",
                    "requested_url_count": 1,
                    "dispatched_url_count": 0,
                    "cached_profile_count": 1,
                    "queued_worker_count": 0,
                    "summary_paths": [],
                }

            def _fake_hydrate_cached_prefetch_profiles(profile_urls, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
                return {
                    str(profile_urls[0]): {
                        "parsed": {
                            "full_name": "Cached Prefetch",
                            "headline": "Research Engineer at OpenAI",
                            "profile_url": str(profile_urls[0]),
                        },
                        "raw_path": str(root / "cached-prefetch.raw.json"),
                        "account_id": "harvest",
                    }
                }

            def _fake_resolve_candidate_with_known_refs(*args, **kwargs):  # noqa: ANN002, ANN003
                resolver_payloads.append(dict(kwargs))
                return True, 1

            enricher.queue_background_profile_prefetch = _fake_queue_background_profile_prefetch
            enricher._hydrate_cached_prefetch_profiles = _fake_hydrate_cached_prefetch_profiles
            enricher._resolve_candidate_with_known_refs = _fake_resolve_candidate_with_known_refs

            result = enricher.enrich(
                identity,
                root,
                [candidate],
                JobRequest(
                    raw_user_request="Find OpenAI cached profile",
                    target_company="OpenAI",
                    categories=["employee"],
                    employment_statuses=["current"],
                    profile_detail_limit=1,
                    slug_resolution_limit=1,
                ),
                job_id="job_cached_prefetch_completed",
            )

        self.assertEqual(result.stop_reason, "")
        self.assertEqual(len(prefetch_calls), 1)
        self.assertEqual(prefetch_calls[0]["candidates"], [candidate])
        self.assertEqual(prefetch_calls[0]["priority"], True)
        self.assertEqual(result.profile_prefetch["reason"], "reused_local_raw_cache")
        self.assertEqual(len(resolver_payloads), 1)
        self.assertIn(candidate.linkedin_url, resolver_payloads[0]["prefetched_harvest_profiles"])

    def test_enrich_full_roster_prefetch_uses_batch_plan_for_tiny_coalescing(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = AssetCatalog(
                project_root=root,
                dev_root=root,
                anthropic_root=root,
                anthropic_workbook=root / "anthropic.xlsx",
                anthropic_readme=root / "README.md",
                anthropic_progress=root / "PROGRESS.md",
                legacy_api_accounts=root / "api_accounts.json",
                legacy_company_ids=root / "company_ids.json",
                anthropic_publications=root / "publications.json",
                scholar_scan_results=root / "scholar.json",
                investor_members_json=root / "investor.json",
                employee_scan_skill=root / "employee_skill.md",
                investor_scan_skill=root / "investor_skill.md",
                onepager_skill=root / "onepager_skill.md",
            )
            enricher = MultiSourceEnricher(catalog, accounts=[], store=self.make_pg_store(str(root / "control_plane.db")))
            enricher.worker_runtime = object()
            enricher.harvest_profile_connector = mock.Mock(
                settings=type("_Settings", (), {"enabled": True})()
            )
            identity = CompanyIdentity(
                requested_name="OpenAI",
                canonical_name="OpenAI",
                company_key="openai",
                linkedin_slug="openai",
                linkedin_company_url="https://www.linkedin.com/company/openai/",
            )
            candidates = [
                Candidate(
                    candidate_id=f"tiny_full_roster_{index}",
                    name_en=f"Tiny Full Roster {index}",
                    display_name=f"Tiny Full Roster {index}",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url=f"https://www.linkedin.com/in/openai-full-roster-tiny-{index}/",
                )
                for index in range(50)
            ]
            dispatched_chunks: list[list[str]] = []
            batch_contexts: list[dict] = []

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                dispatched_chunks.append(profile_urls)
                batch_contexts.append(dict(kwargs.get("prefetch_batch_context") or {}))
                return {
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "summary_path": str(root / "tiny-full-roster.queue_summary.json"),
                    },
                    "cached_profiles": {},
                }

            enricher._execute_harvest_profile_batch_worker = _fake_execute_harvest_profile_batch_worker
            with mock.patch(
                "sourcing_agent.enrichment._recommended_harvest_profile_prefetch_dispatch_window",
                return_value={
                    "batch_size": 3,
                    "max_workers": 1,
                    "batch_count": 17,
                    "source_mix": {"other": 50},
                    "strategy": "test_full_roster_tiny_coalescing",
                    "priority": False,
                },
            ):
                result = enricher.enrich(
                    identity,
                    root,
                    candidates,
                    JobRequest(
                        raw_user_request="Find OpenAI infra people",
                        target_company="OpenAI",
                        categories=["employee"],
                        employment_statuses=["current", "former"],
                        profile_detail_limit=50,
                        slug_resolution_limit=1,
                        execution_preferences={
                            "harvest_profile_batch_submit_global_inflight": 1,
                        },
                    ),
                    job_id="job_full_roster_tiny_plan",
                    full_roster_profile_prefetch=True,
                )

            self.assertEqual(result.stop_reason, "queued_background_harvest")
            self.assertEqual(result.queued_harvest_worker_count, 1)
            self.assertEqual([len(chunk) for chunk in dispatched_chunks], [50])
            self.assertEqual(len(batch_contexts), 1)
            self.assertEqual(batch_contexts[0]["requested_url_count"], 50)
            self.assertEqual(batch_contexts[0]["candidate_count"], 50)
            self.assertEqual(batch_contexts[0]["planned_deferred_url_count"], 0)
            self.assertEqual(batch_contexts[0]["planned_dispatch_worker_count"], 1)
            self.assertTrue(batch_contexts[0]["nonblocking_submit"])


if __name__ == "__main__":
    unittest.main()
