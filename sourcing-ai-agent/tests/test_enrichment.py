import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.connectors import CompanyIdentity, RapidApiAccount
from sourcing_agent.domain import Candidate, EvidenceRecord, JobRequest
from sourcing_agent.enrichment import (
    CompanyPublicationConnector,
    LinkedInSearchSlugResolver,
    MultiSourceEnricher,
    PublicationRecord,
    _candidate_key,
    _extract_page_authors,
    _extract_publications_from_rss,
    _extract_publications_from_surface_index,
    _parse_author_text,
    _prioritize_candidates,
    _prioritize_scholar_coauthor_prospects,
    build_people_search_url,
    extract_search_people_rows,
    parse_basic_linkedin_profile_payload,
)
from sourcing_agent.linkedin_url_normalization import normalize_linkedin_profile_url_key


class EnrichmentHelpersTest(unittest.TestCase):
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
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=fake_profile_connector,
                harvest_profile_search_connector=_FakeSearchConnector(),
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
            )
            self.assertEqual(used_budget, 1)
            self.assertTrue(summary_path.exists())
            self.assertEqual(candidate_map["kevinlu"].category, "employee")
            self.assertEqual(candidate_map["kevinlu"].employment_status, "current")
            self.assertEqual(len(resolved_profiles), 1)
            self.assertEqual(unresolved, [])
            self.assertEqual(len(evidence), 1)
            self.assertEqual(fake_profile_connector.batch_calls, [["https://www.linkedin.com/in/kevin-lu/"]])

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
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=fake_profile_connector,
                harvest_profile_search_connector=_FakeSearchConnector(),
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
            )

            self.assertEqual(used_budget, 2)
            self.assertTrue(summary_path.exists())
            self.assertEqual(len(resolved_profiles), 2)
            self.assertEqual(len(evidence), 2)
            self.assertEqual(
                fake_profile_connector.batch_calls,
                [[
                    "https://www.linkedin.com/in/kevin-lu/",
                    "https://www.linkedin.com/in/alice-wu/",
                ]],
            )

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
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=harvest_profile_connector,
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
            )

            self.assertTrue(resolved)
            self.assertEqual(fetch_count, 1)
            self.assertEqual(
                harvest_profile_connector.batch_calls,
                [[
                    "https://www.linkedin.com/in/mismatch/",
                    "https://www.linkedin.com/in/kevin-lu/",
                ]],
            )
            self.assertEqual(candidate_map["kevinlu"].category, "employee")
            self.assertEqual(len(resolved_profiles), 1)
            self.assertEqual(len(evidence), 1)

    def test_fetch_harvest_profiles_for_urls_uses_balanced_live_batches(self) -> None:
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

            self.assertEqual(len(fetched), 205)
            self.assertEqual(sorted(fake_profile_connector.batch_sizes), [51, 51, 51, 52])

    def test_fetch_harvest_profiles_for_urls_uses_bounded_parallel_live_batches(self) -> None:
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

            self.assertEqual(len(fetched), 205)
            self.assertEqual(sorted(fake_profile_connector.batch_sizes), [51, 51, 51, 52])
            self.assertEqual(fake_profile_connector.max_inflight, 2)

    def test_fetch_harvest_profiles_for_urls_respects_global_inflight_budget(self) -> None:
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

            self.assertEqual(len(fetched), 160)
            self.assertEqual(fake_profile_connector.max_inflight, 1)

    def test_fetch_harvest_profiles_for_urls_uses_higher_parallelism_for_roster_heavy_live_batches(self) -> None:
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

            self.assertEqual(len(fetched), 240)
            self.assertEqual(sorted(fake_profile_connector.batch_sizes), [60, 60, 60, 60])
            self.assertEqual(fake_profile_connector.max_inflight, 3)

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

            def get_linkedin_profile_registry_bulk(self, profile_urls):
                return {
                    self.queued_url: {"status": "queued"},
                }

            def get_linkedin_profile_registry(self, profile_url):
                if normalize_linkedin_profile_url_key(profile_url) == self.queued_url:
                    self.lookup_count += 1
                    self.events.append(f"queued_lookup_{self.lookup_count}")
                    if self.lookup_count >= 2 and "live_fetch" not in self.events:
                        raise AssertionError("contended registry wait happened before uncached live fetch")
                    return {"status": "queued"}
                return {}

            def acquire_linkedin_profile_registry_lease(self, profile_url, lease_owner="", lease_seconds=0):
                return {"acquired": True, "lease_owner": lease_owner, "lease_token": "token"}

            def release_linkedin_profile_registry_lease(self, profile_url, lease_owner="", lease_token=""):
                return {"released": True}

            def record_linkedin_profile_registry_event(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def mark_linkedin_profile_registry_queued(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def mark_linkedin_profile_registry_fetched(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def mark_linkedin_profile_registry_failed(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def upsert_linkedin_profile_registry_sources(self, profile_url, **kwargs):
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
                )

            self.assertIn("live_fetch", events)
            self.assertIn(pending_url, fetched)
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

            def get_linkedin_profile_registry_bulk(self, profile_urls):
                return {
                    self.profile_url: {
                        "status": "queued",
                        "last_raw_path": str(self.raw_path),
                    }
                }

            def get_linkedin_profile_registry(self, profile_url):
                if normalize_linkedin_profile_url_key(profile_url) == self.profile_url:
                    return {
                        "status": "queued",
                        "last_raw_path": str(self.raw_path),
                    }
                return {}

            def acquire_linkedin_profile_registry_lease(self, profile_url, lease_owner="", lease_seconds=0):
                return {"acquired": True, "lease_owner": lease_owner, "lease_token": "token"}

            def release_linkedin_profile_registry_lease(self, profile_url, lease_owner="", lease_token=""):
                return {"released": True}

            def record_linkedin_profile_registry_event(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def mark_linkedin_profile_registry_queued(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def mark_linkedin_profile_registry_fetched(self, profile_url, **kwargs):
                self.fetched_marks.append(str(profile_url or "").strip())
                return {"profile_url": profile_url, **kwargs}

            def mark_linkedin_profile_registry_failed(self, profile_url, **kwargs):
                return {"profile_url": profile_url, **kwargs}

            def upsert_linkedin_profile_registry_sources(self, profile_url, **kwargs):
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

    def test_enrich_background_prefetch_dispatch_uses_bounded_parallel_submit_workers(self) -> None:
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
            counters = {"active": 0, "max_active": 0}
            lock = threading.Lock()
            overlap_seen = threading.Event()

            def _fake_execute_harvest_profile_batch_worker(**kwargs):
                profile_urls = list(kwargs.get("profile_urls") or [])
                with lock:
                    counters["active"] += 1
                    counters["max_active"] = max(counters["max_active"], counters["active"])
                    if counters["active"] >= 2:
                        overlap_seen.set()
                overlap_seen.wait(0.25)
                time.sleep(0.05)
                with lock:
                    counters["active"] -= 1
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
                        execution_preferences={"harvest_prefetch_submit_workers": 2},
                    ),
                    job_id="job_prefetch_parallel_submit",
                    full_roster_profile_prefetch=True,
                )

            self.assertEqual(result.stop_reason, "queued_background_harvest")
            self.assertEqual(result.queued_harvest_worker_count, 4)
            self.assertGreaterEqual(counters["max_active"], 2)


if __name__ == "__main__":
    unittest.main()
