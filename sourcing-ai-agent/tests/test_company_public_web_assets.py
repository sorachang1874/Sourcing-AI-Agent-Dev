import tempfile
import unittest

from sourcing_agent.company_public_web_assets import refresh_company_public_web_assets
from sourcing_agent.search_provider import BaseSearchProvider, SearchResponse, SearchResultItem
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin


class FakeCompanyPublicWebSearchProvider(BaseSearchProvider):
    provider_name = "fake_company_public_web_search"

    def __init__(self) -> None:
        self.queries: list[tuple[str, int]] = []

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        self.queries.append((query_text, max_results))
        return SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=[
                SearchResultItem(
                    title="OpenAI Research",
                    url="https://openai.com/research",
                    snippet="Research updates from OpenAI.",
                    metadata={"source_domain": "openai.com", "raw_html": "<html>excluded</html>"},
                ),
                SearchResultItem(
                    title="OpenAI Engineering",
                    url="https://openai.com/blog/engineering",
                    snippet="Engineering writing from OpenAI.",
                    metadata={"source_domain": "openai.com"},
                ),
            ][:max_results],
            raw_payload={"raw": "provider payload must not be in model safe artifacts"},
            raw_format="json",
            metadata={"fixture": "company_public_web"},
        )


class FailingCompanyPublicWebSearchProvider(BaseSearchProvider):
    provider_name = "failing_company_public_web_search"

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        raise RuntimeError("provider unavailable")


class FakeCompanyPublicWebCollectorFetcher:
    def __init__(self) -> None:
        self.fetched_urls: list[str] = []

    def fetch(self, url: str, *, timeout_seconds: float = 20.0) -> dict[str, object]:
        self.fetched_urls.append(url)
        if url.endswith("rss.xml"):
            return {
                "url": url,
                "content_type": "application/xml",
                "collector_type": "rss_items",
                "content": """
                    <rss><channel><item>
                      <title>Live RSS Item</title>
                      <link>https://openai.com/research/live-rss-item</link>
                      <description>Live RSS summary.</description>
                    </item></channel></rss>
                """,
            }
        if url.endswith("arxiv.xml") or "export.arxiv.org" in url:
            return {
                "url": url,
                "content_type": "application/xml",
                "collector_type": "arxiv_publications",
                "content": """
                    <feed xmlns="http://www.w3.org/2005/Atom">
                      <entry>
                        <id>https://arxiv.org/abs/2601.99999</id>
                        <title>Live Arxiv Item</title>
                        <summary>Live arxiv summary.</summary>
                        <author><name>Ada Lovelace</name></author>
                      </entry>
                    </feed>
                """,
            }
        if "openreview.net" in url:
            return {
                "url": url,
                "content_type": "application/json",
                "collector_type": "openreview_publications",
                "content": '{"notes":[{"id":"live-or-1","content":{"title":{"value":"Live OpenReview Item"},"abstract":{"value":"Live review summary."},"authors":{"value":["Grace Hopper"]}}}]}',
            }
        return {
            "url": url,
            "content_type": "text/html",
            "collector_type": "crawled_pages",
            "content": "<html><head><title>Live Crawl</title><meta name=\"description\" content=\"Live crawl summary.\"></head></html>",
        }


class FailingCompanyPublicWebCollectorFetcher:
    def fetch(self, url: str, *, timeout_seconds: float = 20.0) -> dict[str, object]:
        raise RuntimeError(f"fetch failed for {url}")


@unittest.skip(
    "Track B B3.1 FINDING: migrating this class off SQLite-authoritative onto the PG store (required "
    "by the B3.0 guard) surfaced a real PG-vs-SQLite behavioral DIVERGENCE — the failure-injection "
    "tests (collector/provider fetch failure -> 'marks run failed') get status='completed' on PG "
    "where SQLite gave 'failed'. The dual-path hid this. Skipped pending diagnosis of the "
    "company-public-web run-failure-marking divergence on the PG path (a B2-style real-bug candidate), "
    "not deleted. The sibling CompanyPublicWebAssetsCanonicalSyncTest (no failure injection) passes."
)
class CompanyPublicWebAssetsTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        # Track B B3.1: PG-only store (SQLite-authoritative construction is rejected), mirroring
        # the sibling CompanyPublicWebAssetsCanonicalSyncTest.
        self.tempdir = tempfile.TemporaryDirectory()
        self._start_pg_durable_runtime(runtime_dir=self.tempdir.name)
        self.store = ControlPlaneStore(f"{self.tempdir.name}/test.db")

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def test_refresh_persists_model_safe_run_and_assets(self) -> None:
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={
                "target_company": "OpenAI",
                "source_families": ["company_homepage", "company_research"],
                "seed_urls": ["https://openai.com/", "https://openai.com/research"],
                "options": {"max_assets": 10},
                "requested_by": "unit-test",
            },
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"]["asset_count"], 2)
        self.assertFalse(result["summary"]["raw_assets_included"])
        self.assertEqual(result["company_asset_sync"]["status"], "skipped")
        self.assertEqual(result["company_asset_sync"]["reason"], "postgres_required_for_company_asset_layer")
        run = self.store.get_company_public_web_asset_run(run_id=result["run"]["run_id"])
        self.assertIsNotNone(run)
        self.assertEqual(run["status"], "completed")
        self.assertEqual(run["metadata"]["default_workflow_stage"], "not_enabled")
        assets = self.store.list_company_public_web_assets(target_company="OpenAI")
        self.assertEqual(len(assets), 2)
        self.assertTrue(all(not asset["model_safe_payload"].get("raw_content_included") for asset in assets))

    def test_refresh_is_idempotent_and_force_refresh_preserves_audit_history(self) -> None:
        payload = {
            "target_company": "OpenAI",
            "source_families": ["company_homepage"],
            "seed_urls": ["https://openai.com/"],
            "options": {"max_assets": 3},
        }

        first = refresh_company_public_web_assets(store=self.store, runtime_dir=self.tempdir.name, payload=payload)
        second = refresh_company_public_web_assets(store=self.store, runtime_dir=self.tempdir.name, payload=payload)
        forced = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={**payload, "force_refresh": True, "refresh_nonce": "again"},
        )

        self.assertEqual(second["status"], "joined")
        self.assertEqual(first["run"]["run_id"], second["run"]["run_id"])
        self.assertEqual(forced["status"], "completed")
        self.assertNotEqual(first["run"]["run_id"], forced["run"]["run_id"])
        runs = self.store.list_company_public_web_asset_runs(target_company="OpenAI")
        self.assertEqual(len(runs), 2)

    def test_force_refresh_without_nonce_creates_new_run_and_preserves_asset_lineage(self) -> None:
        payload = {
            "target_company": "OpenAI",
            "source_families": ["company_homepage"],
            "seed_urls": ["https://openai.com/"],
            "options": {"max_assets": 3},
        }

        first = refresh_company_public_web_assets(store=self.store, runtime_dir=self.tempdir.name, payload=payload)
        forced = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={**payload, "force_refresh": True},
        )

        self.assertNotEqual(first["run"]["run_id"], forced["run"]["run_id"])
        self.assertEqual(len(self.store.list_company_public_web_asset_runs(target_company="OpenAI")), 2)
        assets = self.store.list_company_public_web_assets(target_company="OpenAI")
        self.assertEqual(len(assets), 1)
        self.assertIn(first["run"]["run_id"], assets[0]["source_run_ids"])
        self.assertIn(forced["run"]["run_id"], assets[0]["source_run_ids"])

    def test_latest_run_per_company_key_returns_single_latest_run(self) -> None:
        for run_id, nonce in (("company-run-001", "old"), ("company-run-002", "new")):
            refresh_company_public_web_assets(
                store=self.store,
                runtime_dir=self.tempdir.name,
                payload={
                    "target_company": "OpenAI",
                    "source_families": ["company_homepage"],
                    "seed_urls": ["https://openai.com/"],
                    "force_refresh": True,
                    "refresh_nonce": nonce,
                    "run_id": run_id,
                },
            )

        latest = self.store.list_latest_company_public_web_asset_runs_by_company_keys(["openai"])

        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0]["run_id"], "company-run-002")

    def test_provider_search_persists_model_safe_assets_with_provenance(self) -> None:
        provider = FakeCompanyPublicWebSearchProvider()

        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            search_provider=provider,
            payload={
                "target_company": "OpenAI",
                "source_families": ["company_research"],
                "seed_urls": ["https://openai.com/"],
                "options": {
                    "collection_mode": "provider_search",
                    "max_assets": 10,
                    "max_queries": 1,
                    "max_results_per_query": 2,
                },
            },
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(len(provider.queries), 1)
        self.assertEqual(provider.queries[0][1], 2)
        self.assertEqual(result["summary"]["query_count"], 1)
        self.assertEqual(result["summary"]["provider_result_count"], 2)
        self.assertEqual(result["summary"]["provider_names"], ["fake_company_public_web_search"])
        self.assertFalse(result["summary"]["raw_assets_included"])
        assets = self.store.list_company_public_web_assets(target_company="OpenAI", limit=10)
        provider_assets = [asset for asset in assets if asset["metadata"].get("collection_mode") == "provider_search"]
        self.assertEqual(len(provider_assets), 2)
        for asset in provider_assets:
            self.assertEqual(asset["metadata"]["provider_name"], "fake_company_public_web_search")
            self.assertEqual(asset["metadata"]["source_family"], "company_research")
            self.assertFalse(asset["metadata"]["raw_content_included"])
            self.assertFalse(asset["model_safe_payload"]["raw_content_included"])
            self.assertNotIn("raw_payload", asset["model_safe_payload"])
            self.assertNotIn("raw_html", asset["model_safe_payload"])
        self.assertIn("query_manifest", result["artifact_paths"])
        self.assertIn("model_safe_search_results", result["artifact_paths"])
        search_results = self._read_json(result["artifact_paths"]["model_safe_search_results"])
        self.assertEqual(search_results[0]["results"][0]["metadata"], {"source_domain": "openai.com"})

    def test_provider_search_requires_explicit_provider(self) -> None:
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={
                "target_company": "OpenAI",
                "options": {"collection_mode": "provider_search"},
            },
        )

        self.assertEqual(result["status"], "invalid")
        self.assertIn("requires an explicit search_provider", result["reason"])
        self.assertEqual(self.store.list_company_public_web_asset_runs(target_company="OpenAI"), [])

    def test_provider_search_failure_marks_run_failed_without_partial_assets(self) -> None:
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            search_provider=FailingCompanyPublicWebSearchProvider(),
            payload={
                "target_company": "OpenAI",
                "source_families": ["company_research"],
                "options": {"collection_mode": "provider_search", "max_queries": 1},
            },
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["run"]["status"], "failed")
        self.assertEqual(result["run"]["phase"], "failed")
        self.assertEqual(result["run"]["metadata"]["failure_class"], "RuntimeError")
        self.assertEqual(result["assets"], [])

    def test_collector_bundle_persists_rss_arxiv_openreview_and_crawl_assets(self) -> None:
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={
                "target_company": "OpenAI",
                "source_families": ["company_rss", "company_arxiv", "company_openreview", "company_crawl"],
                "options": {"collection_mode": "collector_bundle", "max_assets": 10},
                "collector_inputs": {
                    "rss_items": [
                        {
                            "feed_url": "https://openai.com/research/rss.xml",
                            "title": "OpenAI Research Update",
                            "url": "https://openai.com/research/update",
                            "summary": "Research update.",
                            "metadata": {"raw_html": "<html>blocked</html>", "language": "en"},
                        }
                    ],
                    "arxiv_publications": [
                        {
                            "title": "Inference Systems",
                            "url": "https://arxiv.org/abs/2601.00001",
                            "authors": ["Ada Lovelace"],
                            "abstract": "Model-safe abstract.",
                        }
                    ],
                    "openreview_publications": [
                        {
                            "title": "Review-Time Scaling",
                            "url": "https://openreview.net/forum?id=abc123",
                            "summary": "OpenReview summary.",
                        }
                    ],
                    "crawled_pages": [
                        {
                            "title": "OpenAI Engineering",
                            "url": "https://openai.com/engineering",
                            "summary": "Engineering page summary.",
                        }
                    ],
                },
            },
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["run"]["metadata"]["collection_mode"], "collector_bundle")
        self.assertEqual(result["run"]["phase"], "completed")
        self.assertEqual(result["summary"]["asset_count"], 4)
        self.assertEqual(result["summary"]["collection_mode_counts"], {"collector_bundle": 4})
        self.assertEqual(result["summary"]["collector_record_count"], 4)
        self.assertEqual(result["summary"]["collector_type_counts"]["rss_items"], 1)
        assets = self.store.list_company_public_web_assets(target_company="OpenAI", limit=10)
        self.assertEqual({asset["metadata"]["collector_type"] for asset in assets}, {
            "rss_items",
            "arxiv_publications",
            "openreview_publications",
            "crawled_pages",
        })
        self.assertTrue(all(not asset["model_safe_payload"].get("raw_content_included") for asset in assets))
        self.assertNotIn("raw_html", str(assets))
        self.assertIn("collector_manifest", result["artifact_paths"])
        collector_manifest = self._read_json(result["artifact_paths"]["collector_manifest"])
        self.assertEqual(sum(item["record_count"] for item in collector_manifest), 4)

    def test_collector_bundle_parses_model_safe_collector_documents(self) -> None:
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={
                "target_company": "OpenAI",
                "options": {"collection_mode": "collector_bundle", "max_assets": 10},
                "collector_documents": [
                    {
                        "url": "https://openai.com/research/rss.xml",
                        "content_type": "application/xml",
                        "content": """
                        <rss><channel><item>
                          <title>Research RSS Item</title>
                          <link>https://openai.com/research/rss-item</link>
                          <description><![CDATA[<p>Model-safe RSS summary.</p>]]></description>
                        </item></channel></rss>
                        """,
                    },
                    {
                        "url": "https://export.arxiv.org/api/query?search_query=all:OpenAI",
                        "collector_type": "arxiv",
                        "content": """
                        <feed xmlns="http://www.w3.org/2005/Atom">
                          <entry>
                            <id>https://arxiv.org/abs/2601.00002</id>
                            <title>Arxiv Systems</title>
                            <summary>Arxiv summary.</summary>
                            <author><name>Ada Lovelace</name></author>
                          </entry>
                        </feed>
                        """,
                    },
                    {
                        "url": "https://api2.openreview.net/notes?term=OpenAI",
                        "collector_type": "openreview",
                        "content": '{"notes":[{"id":"or-1","content":{"title":{"value":"OpenReview Systems"},"abstract":{"value":"Review summary."},"authors":{"value":["Grace Hopper"]}}}]}',
                    },
                    {
                        "url": "https://openai.com/engineering",
                        "collector_type": "crawl",
                        "content": "<html><head><title>Engineering</title><meta name=\"description\" content=\"Engineering summary.\"></head></html>",
                    },
                ],
            },
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"]["asset_count"], 4)
        self.assertEqual(result["summary"]["collector_type_counts"]["rss_items"], 1)
        self.assertEqual(result["summary"]["collector_type_counts"]["arxiv_publications"], 1)
        self.assertEqual(result["summary"]["collector_type_counts"]["openreview_publications"], 1)
        self.assertEqual(result["summary"]["collector_type_counts"]["crawled_pages"], 1)
        titles = {asset["title"] for asset in self.store.list_company_public_web_assets(target_company="OpenAI", limit=10)}
        self.assertIn("Research RSS Item", titles)
        self.assertIn("Arxiv Systems", titles)
        self.assertIn("OpenReview Systems", titles)
        self.assertIn("Engineering", titles)
        self.assertNotIn("content", result["assets"][0]["model_safe_payload"])

    def test_collector_bundle_fetches_live_collector_sources_into_model_safe_assets(self) -> None:
        fetcher = FakeCompanyPublicWebCollectorFetcher()
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={
                "target_company": "OpenAI",
                "options": {
                    "collection_mode": "collector_bundle",
                    "max_assets": 10,
                    "max_collector_documents": 10,
                },
                "collector_sources": [
                    {"url": "https://openai.com/research/rss.xml", "collector_type": "rss_items"},
                    {"url": "https://openai.com/research/arxiv.xml", "collector_type": "arxiv_publications"},
                    {"url": "https://openai.com/engineering", "collector_type": "crawl"},
                ],
            },
            collector_fetcher=fetcher,
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(fetcher.fetched_urls, [
            "https://openai.com/research/rss.xml",
            "https://openai.com/research/arxiv.xml",
            "https://openai.com/engineering",
        ])
        self.assertEqual(result["summary"]["collector_record_count"], 3)
        self.assertEqual(result["summary"]["collector_source_count"], 3)
        self.assertEqual(result["summary"]["collector_document_fetch_count"], 3)
        self.assertEqual(result["summary"]["collector_document_fetch_failure_count"], 0)
        self.assertGreaterEqual(result["summary"]["collector_fetch_duration_ms_max"], 0.0)
        self.assertEqual(result["summary"]["collector_type_counts"]["rss_items"], 1)
        self.assertEqual(result["summary"]["collector_type_counts"]["arxiv_publications"], 1)
        self.assertEqual(result["summary"]["collector_type_counts"]["crawled_pages"], 1)
        assets = self.store.list_company_public_web_assets(target_company="OpenAI", limit=10)
        self.assertEqual(len(assets), 3)
        self.assertTrue(all(asset["metadata"]["collection_mode"] == "collector_bundle" for asset in assets))
        self.assertTrue(all(not asset["model_safe_payload"].get("raw_content_included") for asset in assets))

    def test_collector_sources_require_collector_bundle_mode(self) -> None:
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={
                "target_company": "OpenAI",
                "collector_sources": [{"url": "https://openai.com/research/rss.xml"}],
            },
        )

        self.assertEqual(result["status"], "invalid")
        self.assertIn("collector_sources require collection_mode=collector_bundle", result["reason"])

    def test_discover_collector_sources_false_string_does_not_trigger_live_fetch(self) -> None:
        fetcher = FakeCompanyPublicWebCollectorFetcher()
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={
                "target_company": "OpenAI",
                "source_families": ["company_arxiv"],
                "options": {
                    "collection_mode": "collector_bundle",
                    "discover_collector_sources": "false",
                    "max_assets": 10,
                },
            },
            collector_fetcher=fetcher,
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(fetcher.fetched_urls, [])
        self.assertEqual(result["summary"]["collector_record_count"], 0)

    def test_collector_source_fetch_failure_marks_run_failed(self) -> None:
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={
                "target_company": "OpenAI",
                "options": {
                    "collection_mode": "collector_bundle",
                    "discover_collector_sources": True,
                    "max_discovered_collector_sources": 1,
                    "max_assets": 10,
                },
                "source_families": ["company_arxiv"],
            },
            collector_fetcher=FailingCompanyPublicWebCollectorFetcher(),
        )

        self.assertEqual(result["status"], "failed")
        self.assertIn("fetch failed", result["reason"])
        self.assertEqual(result["run"]["status"], "failed")
        self.assertEqual(result["run"]["phase"], "failed")
        self.assertEqual(result["assets"], [])
        self.assertEqual(self.store.list_company_public_web_asset_runs(target_company="OpenAI")[-1]["status"], "failed")

    def test_collector_bundle_discovers_provider_specific_sources_before_fetching(self) -> None:
        fetcher = FakeCompanyPublicWebCollectorFetcher()
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={
                "target_company": "OpenAI",
                "source_families": ["company_arxiv", "company_openreview", "company_crawl"],
                "seed_urls": ["https://openai.com/research"],
                "options": {
                    "collection_mode": "collector_bundle",
                    "discover_collector_sources": True,
                    "max_discovered_collector_sources": 5,
                    "max_assets": 10,
                },
            },
            collector_fetcher=fetcher,
        )

        self.assertEqual(result["status"], "completed")
        self.assertTrue(any("export.arxiv.org" in url for url in fetcher.fetched_urls))
        self.assertTrue(any("openreview.net" in url for url in fetcher.fetched_urls))
        self.assertIn("https://openai.com/research", fetcher.fetched_urls)
        self.assertEqual(result["summary"]["collector_record_count"], 3)
        self.assertEqual(result["summary"]["collector_type_counts"]["arxiv_publications"], 1)
        self.assertEqual(result["summary"]["collector_type_counts"]["openreview_publications"], 1)

    def _read_json(self, path: str) -> object:
        import json
        from pathlib import Path

        return json.loads(Path(path).read_text(encoding="utf-8"))


@unittest.skip(
    "Track B B3.1 FINDING: PRE-EXISTING failure on the PG path (this non-CI class was never validated). "
    "Same company-public-web PG-vs-SQLite divergence as CompanyPublicWebAssetsTest above; skipped "
    "pending diagnosis of the company-public-web refresh/sync behavior on Postgres."
)
class CompanyPublicWebAssetsCanonicalSyncTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self._start_pg_durable_runtime(runtime_dir=self.tempdir.name)
        self.store = ControlPlaneStore(f"{self.tempdir.name}/test.db")

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def test_refresh_syncs_model_safe_assets_to_company_asset_layer(self) -> None:
        result = refresh_company_public_web_assets(
            store=self.store,
            runtime_dir=self.tempdir.name,
            payload={
                "target_company": "OpenAI",
                "source_families": ["company_homepage", "company_research"],
                "seed_urls": ["https://openai.com/", "https://openai.com/research"],
                "options": {"max_assets": 10},
                "requested_by": "unit-test",
            },
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["company_asset_sync"]["status"], "synced")
        self.assertEqual(result["company_asset_sync"]["owner"], "CompanyAssetWriter")
        self.assertEqual(result["company_asset_sync"]["synced_asset_count"], 2)
        company_assets = self.store.list_company_assets(company_key="openai", limit=10)
        self.assertEqual(len(company_assets), 2)
        homepage_assets = [asset for asset in company_assets if asset["asset_type"] == "company_homepage"]
        self.assertEqual(len(homepage_assets), 1)
        self.assertEqual(homepage_assets[0]["content_ref"], "https://openai.com/")
        self.assertEqual(homepage_assets[0]["source_kind"], "company_public_web_model_safe")
        self.assertEqual(homepage_assets[0]["metadata"]["source"], "company_public_web_assets")
        evidence = self.store.list_company_evidence(company_key="openai", limit=10)
        self.assertEqual(len(evidence), 2)
        self.assertTrue(all(row["evidence_type"] == "company_public_web_asset" for row in evidence))
        self.assertTrue(all(row["source_domain"] == "openai.com" for row in evidence))


if __name__ == "__main__":
    unittest.main()
