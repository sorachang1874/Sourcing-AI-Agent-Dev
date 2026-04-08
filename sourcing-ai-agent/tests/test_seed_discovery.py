import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.connectors import CompanyIdentity, RapidApiAccount
from sourcing_agent.search_provider import SearchExecutionArtifact, SearchExecutionResult
from sourcing_agent.seed_discovery import (
    SearchSeedAcquirer,
    _normalize_harvest_company_filters,
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
                self.filters = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                self.queries.append(kwargs.get("query_text", ""))
                self.filters.append(dict(kwargs.get("filter_hints") or {}))
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
            self.assertEqual(
                fake.filters[0].get("past_companies"),
                ["https://www.linkedin.com/company/thinkingmachinesai/"],
            )

    def test_normalize_harvest_company_filters_prefers_exact_company_url(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            aliases=["thinking machines ai", "tml"],
        )
        normalized = _normalize_harvest_company_filters(
            identity,
            {
                "past_companies": ["Thinking Machines Lab"],
                "exclude_current_companies": ["thinkingmachinesai"],
                "keywords": ["systems"],
            },
        )
        self.assertEqual(
            normalized["past_companies"],
            ["https://www.linkedin.com/company/thinkingmachinesai/"],
        )
        self.assertEqual(
            normalized["exclude_current_companies"],
            ["https://www.linkedin.com/company/thinkingmachinesai/"],
        )
        self.assertEqual(normalized["keywords"], ["systems"])

    def test_execute_query_spec_queues_worker_when_dataforseo_task_not_ready(self) -> None:
        class _PendingSearchProvider:
            provider_name = "dataforseo_google_organic"

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    pending=True,
                    message="waiting for queue",
                    checkpoint={"provider_name": self.provider_name, "task_id": "task_123", "status": "submitted"},
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post",
                            payload={"tasks": [{"id": "task_123"}]},
                            metadata={"task_id": "task_123"},
                        )
                    ],
                )

        class _WorkerHandle:
            worker_id = 1

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self.completed = []

            def begin_worker(self, **kwargs):
                return _WorkerHandle()

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                self.completed.append(kwargs)
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            discovery_dir = Path(tempdir)
            runtime = _FakeWorkerRuntime()
            acquirer = SearchSeedAcquirer([], search_provider=_PendingSearchProvider())
            result = acquirer._execute_query_spec(
                index=1,
                query_spec={
                    "query": '"Kevin Lu" "Thinking Machines Lab" site:linkedin.com/in',
                    "bundle_id": "targeted_people_search",
                    "source_family": "targeted_people_search",
                    "execution_mode": "web_search",
                },
                identity=identity,
                discovery_dir=discovery_dir,
                logger=AssetLogger(discovery_dir.parent),
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                result_limit=10,
            )
            self.assertEqual(result["worker_status"], "queued")
            self.assertEqual(result["summary"]["status"], "queued")
            self.assertEqual(runtime.completed[0]["status"], "queued")
            self.assertTrue((discovery_dir / "web_query_01_task_post.json").exists())

    def test_discover_blocks_paid_fallback_while_low_cost_queue_is_pending(self) -> None:
        class _PendingSearchProvider:
            provider_name = "dataforseo_google_organic"

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    pending=True,
                    message="waiting for queue",
                    checkpoint={"provider_name": self.provider_name, "task_id": "task_queued", "status": "submitted"},
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post",
                            payload={"tasks": [{"id": "task_queued"}]},
                            metadata={"task_id": "task_queued"},
                        )
                    ],
                )

        class _FakeHarvestSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeHarvestSettings()

            def __init__(self) -> None:
                self.calls = 0

            def search_profiles(self, **kwargs):
                self.calls += 1
                return {
                    "raw_path": Path(kwargs["discovery_dir"]) / "harvest.json",
                    "rows": [
                        {
                            "full_name": "Queued Fallback Candidate",
                            "headline": "Former Thinking Machines Lab",
                            "location": "San Francisco",
                            "profile_url": "https://www.linkedin.com/in/queued-fallback/",
                            "username": "queued-fallback",
                        }
                    ],
                }

        class _WorkerHandle:
            worker_id = 1

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self.completed = []

            def begin_worker(self, **kwargs):
                return _WorkerHandle()

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                self.completed.append(kwargs)
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        fake_account = RapidApiAccount(
            account_id="search_1",
            source="rapidapi",
            provider="fake",
            host="search/people",
            base_url="https://example.com",
            api_key="token",
            endpoint_search="/search/people",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "thinkingmachineslab" / "snapshot-01"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            harvest_connector = _FakeHarvestConnector()
            runtime = _FakeWorkerRuntime()
            acquirer = SearchSeedAcquirer(
                [fake_account],
                harvest_search_connector=harvest_connector,
                search_provider=_PendingSearchProvider(),
            )
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=["Thinking Machines Lab former employee"],
                query_bundles=[],
                filter_hints={"past_companies": ["Thinking Machines Lab"]},
                cost_policy={
                    "provider_people_search_mode": "fallback_only",
                    "provider_people_search_min_expected_results": 1,
                    "parallel_search_workers": 1,
                    "public_media_results_per_query": 10,
                },
                employment_status="former",
                worker_runtime=runtime,
                job_id="job_queued",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(snapshot.stop_reason, "queued_background_search")
            self.assertEqual(snapshot.entries, [])
            self.assertEqual(harvest_connector.calls, 0)
            self.assertEqual(runtime.completed[0]["status"], "queued")
