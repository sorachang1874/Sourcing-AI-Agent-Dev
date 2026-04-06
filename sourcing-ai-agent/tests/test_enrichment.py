import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.connectors import RapidApiAccount
from sourcing_agent.enrichment import (
    MultiSourceEnricher,
    LinkedInSearchSlugResolver,
    _extract_page_authors,
    _extract_publications_from_rss,
    _extract_publications_from_surface_index,
    _parse_author_text,
    _prioritize_candidates,
    build_people_search_url,
    extract_search_people_rows,
    parse_basic_linkedin_profile_payload,
)


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
            def fetch_profile_by_url(self, profile_url, snapshot_dir, asset_logger=None):
                raw_path = snapshot_dir / "harvest_profiles" / "kevin-lu.json"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text("{}")
                return {
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
                harvest_profile_connector=_FakeProfileConnector(),
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
            self.assertEqual(result.unresolved_candidates, [])


if __name__ == "__main__":
    unittest.main()
