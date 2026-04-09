import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.connectors import CompanyIdentity, RapidApiAccount, resolve_company_identity
from sourcing_agent.search_provider import (
    SearchBatchFetchResult,
    SearchBatchFetchTask,
    SearchBatchReadyResult,
    SearchBatchReadyTask,
    SearchResponse,
    SearchResultItem,
    SearchBatchSubmissionResult,
    SearchBatchSubmissionTask,
    SearchExecutionArtifact,
    SearchExecutionResult,
    search_response_to_record,
)
from sourcing_agent.seed_discovery import (
    SearchSeedAcquirer,
    _lead_entries_from_public_result,
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

    def test_public_media_lead_guard_rejects_non_person_titles(self) -> None:
        identity = CompanyIdentity(
            requested_name="Humans&",
            canonical_name="Humans&",
            company_key="humans",
            linkedin_slug="humans",
            aliases=["humansand"],
        )
        entries = _lead_entries_from_public_result(
            {
                "title": "Nature Biomedical",
                "url": "https://example.com/article",
            },
            identity,
            "humansand contributor",
            "current",
            analysis={"target_company_relation": "explicit", "confidence_label": "high"},
            source_family="publication_and_blog",
        )
        self.assertEqual(entries, [])

    def test_resolve_company_identity_prefers_builtin_mapping_over_model_assisted_observed_candidates(self) -> None:
        class _FakeModelClient:
            def judge_company_equivalence(self, payload):
                self.payload = payload
                return {
                    "decision": "same_company",
                    "matched_label": "Humans And AI",
                    "confidence_label": "high",
                    "rationale": "Observed LinkedIn company candidate matches the requested organization naming.",
                }

        client = _FakeModelClient()
        identity = resolve_company_identity(
            "Humans&",
            model_client=client,
            observed_companies=[
                {
                    "label": "Humans And AI",
                    "linkedin_slug": "humansand",
                    "linkedin_company_url": "https://www.linkedin.com/company/humansand/",
                }
            ],
        )
        self.assertEqual(identity.linkedin_slug, "humansand")
        self.assertEqual(identity.linkedin_company_url, "https://www.linkedin.com/company/humansand/")
        self.assertEqual(identity.company_key, "humansand")
        self.assertEqual(identity.resolver, "builtin")
        self.assertEqual(identity.confidence, "high")
        self.assertFalse(hasattr(client, "payload"))

    def test_resolve_company_identity_prefers_builtin_mapping_over_observed_exact_match_without_model_client(self) -> None:
        identity = resolve_company_identity(
            "Humans&",
            observed_companies=[
                {
                    "label": "Humans&",
                    "linkedin_slug": "humansand",
                    "linkedin_company_url": "https://www.linkedin.com/company/humansand/",
                }
            ],
        )
        self.assertEqual(identity.linkedin_slug, "humansand")
        self.assertEqual(identity.linkedin_company_url, "https://www.linkedin.com/company/humansand/")
        self.assertEqual(identity.company_key, "humansand")
        self.assertEqual(identity.resolver, "builtin")
        self.assertEqual(identity.confidence, "high")

    def test_resolve_company_identity_prefers_alias_mapped_linkedin_slug_for_company_key(self) -> None:
        identity = resolve_company_identity(
            "Thinking Machines Lab",
            observed_companies=[
                {
                    "label": "Thinking Machines Lab",
                    "linkedin_slug": "thinkingmachinesai",
                    "linkedin_company_url": "https://www.linkedin.com/company/thinkingmachinesai/",
                }
            ],
        )
        self.assertEqual(identity.linkedin_slug, "thinkingmachinesai")
        self.assertEqual(identity.company_key, "thinkingmachineslab")

    def test_resolve_company_identity_skips_model_for_irrelevant_observed_candidates(self) -> None:
        class _FailIfCalledModelClient:
            def judge_company_equivalence(self, payload):  # noqa: ARG002
                raise AssertionError("judge_company_equivalence should not be called for irrelevant observed candidates")

        identity = resolve_company_identity(
            "Humans&",
            model_client=_FailIfCalledModelClient(),
            observed_companies=[
                {
                    "label": "Completely Different Organization",
                    "linkedin_slug": "differentorg",
                    "linkedin_company_url": "https://www.linkedin.com/company/differentorg/",
                }
            ],
        )

        self.assertEqual(identity.resolver, "builtin")
        self.assertEqual(identity.confidence, "high")

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

    def test_discover_allows_harvest_former_fallback_without_rapidapi_accounts(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 100

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def search_profiles(self, **kwargs):
                return {
                    "raw_path": Path(kwargs["discovery_dir"]) / "fake_harvest.json",
                    "rows": [
                        {
                            "full_name": "Former Example",
                            "headline": "Research Engineer at NewCo",
                            "location": "San Francisco",
                            "profile_url": "https://www.linkedin.com/in/former-example/",
                            "username": "former-example",
                            "current_company": "NewCo",
                        }
                    ],
                    "payload": {},
                }

        class _NoopSearchProvider:
            def search(self, query, max_results=10):  # noqa: ARG002
                return SearchResponse(provider_name="noop", results=[])

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "thinkingmachineslab" / "snapshot-01"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            acquirer = SearchSeedAcquirer(
                [],
                harvest_search_connector=_FakeHarvestConnector(),
                search_provider=_NoopSearchProvider(),
            )
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[],
                query_bundles=[],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                cost_policy={
                    "provider_people_search_mode": "fallback_only",
                    "provider_people_search_min_expected_results": 1,
                    "provider_people_search_pages": 1,
                },
                employment_status="former",
                worker_runtime=None,
                job_id="",
                request_payload={},
                plan_payload={},
                runtime_mode="maintenance",
            )
            self.assertEqual(snapshot.stop_reason, "provider_people_search_fallback")
            self.assertEqual(len(snapshot.entries), 1)
            self.assertEqual(snapshot.entries[0]["full_name"], "Former Example")

    def test_former_paid_fallback_probes_then_expands_to_provider_total(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                self.calls.append(
                    {
                        "query_text": kwargs.get("query_text", ""),
                        "limit": kwargs.get("limit"),
                        "pages": kwargs.get("pages"),
                    }
                )
                if len(self.calls) == 1:
                    return {
                        "raw_path": self.tempdir / "probe.json",
                        "rows": [
                            {
                                "full_name": "Probe Example",
                                "headline": "Former Research Engineer",
                                "location": "San Francisco",
                                "profile_url": "https://www.linkedin.com/in/probe-example/",
                                "username": "probe-example",
                                "current_company": "NewCo",
                            }
                        ],
                        "pagination": {
                            "returned_count": 1,
                            "total_elements": 559,
                            "total_pages": 23,
                            "page_number": 1,
                            "page_size": 25,
                        },
                    }
                return {
                    "raw_path": self.tempdir / "full.json",
                    "rows": [
                        {
                            "full_name": "Former Example",
                            "headline": "Research Engineer at NewCo",
                            "location": "San Francisco",
                            "profile_url": "https://www.linkedin.com/in/former-example/",
                            "username": "former-example",
                            "current_company": "NewCo",
                        }
                    ],
                    "pagination": {
                        "returned_count": 1,
                        "total_elements": 559,
                        "total_pages": 23,
                        "page_number": 1,
                        "page_size": 25,
                    },
                }

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
                search_seed_queries=[],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                employment_status="former",
                limit=50,
                cost_policy={"provider_people_search_pages": 2},
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertEqual(len(entries), 1)
        self.assertEqual(len(fake.calls), 2)
        self.assertEqual(fake.calls[0], {"query_text": "", "limit": 25, "pages": 1})
        self.assertEqual(fake.calls[1], {"query_text": "", "limit": 559, "pages": 23})
        self.assertEqual(summaries[0]["effective_limit"], 559)
        self.assertEqual(summaries[0]["effective_pages"], 23)
        self.assertEqual(summaries[0]["probe"]["provider_total_count"], 559)

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

    def test_discover_batch_prefetches_dataforseo_tasks(self) -> None:
        class _BatchSearchProvider:
            provider_name = "dataforseo_google_organic"

            def __init__(self) -> None:
                self.batch_calls = []
                self.ready_calls = []
                self.fetch_calls = []
                self.execute_calls = []

            def submit_batch_queries(self, query_specs):
                self.batch_calls.append(list(query_specs))
                return SearchBatchSubmissionResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchSubmissionTask(
                            task_key=str(item["task_key"]),
                            query_text=str(item["query_text"]),
                            checkpoint={
                                "provider_name": self.provider_name,
                                "task_id": f"task_{index}",
                                "status": "submitted",
                            },
                            metadata={"artifact_label": "task_post_batch_01"},
                        )
                        for index, item in enumerate(query_specs, start=1)
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post_batch_01",
                            payload={"tasks": [{"id": "task_1"}, {"id": "task_2"}]},
                        )
                    ],
                )

            def poll_ready_batch(self, query_specs):
                self.ready_calls.append(list(query_specs))
                return SearchBatchReadyResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchReadyTask(
                            task_key="seed_queries::01",
                            task_id="task_1",
                            query_text=query_specs[0]["query_text"],
                            checkpoint={
                                **dict(query_specs[0].get("checkpoint") or {}),
                                "status": "ready_cached",
                            },
                        ),
                        SearchBatchReadyTask(
                            task_key="seed_queries::02",
                            task_id="task_2",
                            query_text=query_specs[1]["query_text"],
                            checkpoint={
                                **dict(query_specs[1].get("checkpoint") or {}),
                                "status": "waiting_for_ready_cached",
                            },
                        ),
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="tasks_ready_batch",
                            payload={"tasks": [{"result": []}]},
                        )
                    ],
                )

            def fetch_ready_batch(self, query_specs):
                self.fetch_calls.append(list(query_specs))
                return SearchBatchFetchResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchFetchTask(
                            task_key="seed_queries::01",
                            task_id="task_1",
                            query_text=query_specs[0]["query_text"],
                            response=SearchResponse(
                                provider_name=self.provider_name,
                                query_text=query_specs[0]["query_text"],
                                results=[
                                    SearchResultItem(
                                        title="Jane Doe - LinkedIn",
                                        url="https://www.linkedin.com/in/jane-doe/",
                                        snippet="Thinking Machines Lab",
                                    )
                                ],
                                raw_payload={"tasks": [{"id": "task_1"}]},
                                raw_format="json",
                            ),
                            checkpoint={
                                "provider_name": self.provider_name,
                                "task_id": "task_1",
                                "status": "fetched_cached",
                            },
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_get_batch_01",
                            payload={"tasks": [{"id": "task_1"}]},
                        )
                    ],
                )

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                self.execute_calls.append({"query_text": query_text, "checkpoint": dict(checkpoint or {})})
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    pending=True,
                    message="waiting for queue",
                    checkpoint=dict(checkpoint or {}),
                )

        class _WorkerHandle:
            def __init__(self, worker_id: int) -> None:
                self.worker_id = worker_id

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self.completed = []
                self.checkpoints = []
                self._next_worker_id = 0

            def begin_worker(self, **kwargs):
                self._next_worker_id += 1
                return _WorkerHandle(self._next_worker_id)

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
                self.checkpoints.append(kwargs)
                return kwargs

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            runtime = _FakeWorkerRuntime()
            provider = _BatchSearchProvider()
            acquirer = SearchSeedAcquirer([], search_provider=provider)
            result = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[
                    '"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in',
                    '"John Smith" "Thinking Machines Lab" site:linkedin.com/in',
                ],
                filter_hints={},
                cost_policy={"parallel_search_workers": 2, "public_media_results_per_query": 10},
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(len(provider.batch_calls), 1)
            self.assertEqual(len(provider.ready_calls), 1)
            self.assertEqual(len(provider.fetch_calls), 1)
            self.assertEqual(len(provider.execute_calls), 1)
            self.assertEqual(provider.execute_calls[0]["checkpoint"]["task_id"], "task_2")
            self.assertEqual(provider.execute_calls[0]["checkpoint"]["status"], "waiting_for_ready_cached")
            self.assertEqual(result.stop_reason, "queued_background_search")
            self.assertEqual(len(result.entries), 1)
            self.assertTrue((snapshot_dir / "search_seed_discovery" / "web_query_01.json").exists())
            self.assertTrue((snapshot_dir / "search_seed_discovery" / "web_search_batch_manifest.json").exists())
            acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[
                    '"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in',
                    '"John Smith" "Thinking Machines Lab" site:linkedin.com/in',
                ],
                filter_hints={},
                cost_policy={"parallel_search_workers": 2, "public_media_results_per_query": 10},
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(len(provider.ready_calls), 1)
            self.assertEqual(len(provider.fetch_calls), 1)

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

    def test_discover_does_not_repoll_ready_cached_entries(self) -> None:
        class _BatchSearchProvider:
            provider_name = "dataforseo_google_organic"

            def __init__(self) -> None:
                self.batch_calls = []
                self.ready_calls = []
                self.fetch_calls = []
                self.execute_calls = []

            def submit_batch_queries(self, query_specs):
                self.batch_calls.append(list(query_specs))
                return SearchBatchSubmissionResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchSubmissionTask(
                            task_key=str(item["task_key"]),
                            query_text=str(item["query_text"]),
                            checkpoint={
                                "provider_name": self.provider_name,
                                "task_id": f"task_{index}",
                                "status": "submitted",
                            },
                            metadata={"artifact_label": "task_post_batch_01"},
                        )
                        for index, item in enumerate(query_specs, start=1)
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post_batch_01",
                            payload={"tasks": [{"id": "task_1"}, {"id": "task_2"}]},
                        )
                    ],
                )

            def poll_ready_batch(self, query_specs):
                self.ready_calls.append([str(item["task_key"]) for item in query_specs])
                if len(self.ready_calls) == 1:
                    return SearchBatchReadyResult(
                        provider_name=self.provider_name,
                        tasks=[
                            SearchBatchReadyTask(
                                task_key="seed_queries::01",
                                task_id="task_1",
                                query_text=query_specs[0]["query_text"],
                                checkpoint={
                                    **dict(query_specs[0].get("checkpoint") or {}),
                                    "status": "ready_cached",
                                },
                            ),
                            SearchBatchReadyTask(
                                task_key="seed_queries::02",
                                task_id="task_2",
                                query_text=query_specs[1]["query_text"],
                                checkpoint={
                                    **dict(query_specs[1].get("checkpoint") or {}),
                                    "status": "waiting_for_ready_cached",
                                },
                            ),
                        ],
                        artifacts=[
                            SearchExecutionArtifact(
                                label="tasks_ready_batch",
                                payload={"tasks": [{"result": []}]},
                            )
                        ],
                    )
                self.assertEqual(query_specs[0]["task_key"], "seed_queries::02")
                self.assertEqual(len(query_specs), 1)
                return SearchBatchReadyResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchReadyTask(
                            task_key="seed_queries::02",
                            task_id="task_2",
                            query_text=query_specs[0]["query_text"],
                            checkpoint={
                                **dict(query_specs[0].get("checkpoint") or {}),
                                "status": "waiting_for_ready_cached",
                            },
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="tasks_ready_batch",
                            payload={"tasks": [{"result": []}]},
                        )
                    ],
                )

            def fetch_ready_batch(self, query_specs):
                self.fetch_calls.append([str(item["task_key"]) for item in query_specs])
                return SearchBatchFetchResult(
                    provider_name=self.provider_name,
                    tasks=[],
                    artifacts=[],
                    message="no fetched tasks yet",
                )

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                self.execute_calls.append({"query_text": query_text, "checkpoint": dict(checkpoint or {})})
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    pending=True,
                    message="waiting for queue",
                    checkpoint=dict(checkpoint or {}),
                )

        class _WorkerHandle:
            def __init__(self, worker_id: int) -> None:
                self.worker_id = worker_id

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self._next_worker_id = 0

            def begin_worker(self, **kwargs):
                self._next_worker_id += 1
                return _WorkerHandle(self._next_worker_id)

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
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
            snapshot_dir = Path(tempdir)
            runtime = _FakeWorkerRuntime()
            provider = _BatchSearchProvider()
            acquirer = SearchSeedAcquirer([], search_provider=provider)
            acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[
                    '"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in',
                    '"John Smith" "Thinking Machines Lab" site:linkedin.com/in',
                ],
                filter_hints={},
                cost_policy={"parallel_search_workers": 2, "public_media_results_per_query": 10},
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            manifest_path = snapshot_dir / "search_seed_discovery" / "web_search_batch_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            for item in manifest["entries"]:
                search_state = dict(item.get("search_state") or {})
                search_state["ready_attempted_at"] = "2026-01-01T00:00:00+00:00"
                search_state["fetch_attempted_at"] = "2026-01-01T00:00:00+00:00"
                item["search_state"] = search_state
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

            acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[
                    '"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in',
                    '"John Smith" "Thinking Machines Lab" site:linkedin.com/in',
                ],
                filter_hints={},
                cost_policy={"parallel_search_workers": 2, "public_media_results_per_query": 10},
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(provider.ready_calls[0], ["seed_queries::01", "seed_queries::02"])
            self.assertEqual(provider.ready_calls[1], ["seed_queries::02"])

    def test_discover_worker_direct_fetch_updates_batch_manifest(self) -> None:
        class _BatchSearchProvider:
            provider_name = "dataforseo_google_organic"

            def __init__(self) -> None:
                self.ready_calls = []
                self.fetch_calls = 0
                self.execute_calls = []

            def submit_batch_queries(self, query_specs):
                return SearchBatchSubmissionResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchSubmissionTask(
                            task_key=str(query_specs[0]["task_key"]),
                            query_text=str(query_specs[0]["query_text"]),
                            checkpoint={
                                "provider_name": self.provider_name,
                                "task_id": "task_1",
                                "status": "submitted",
                            },
                            metadata={"artifact_label": "task_post_batch_01"},
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post_batch_01",
                            payload={"tasks": [{"id": "task_1"}]},
                        )
                    ],
                )

            def poll_ready_batch(self, query_specs):
                self.ready_calls.append([str(item["task_key"]) for item in query_specs])
                return SearchBatchReadyResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchReadyTask(
                            task_key=str(query_specs[0]["task_key"]),
                            task_id="task_1",
                            query_text=str(query_specs[0]["query_text"]),
                            checkpoint={
                                **dict(query_specs[0].get("checkpoint") or {}),
                                "status": "ready_cached",
                            },
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="tasks_ready_batch",
                            payload={"tasks": [{"result": []}]},
                        )
                    ],
                )

            def fetch_ready_batch(self, query_specs):
                self.fetch_calls += 1
                return None

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                self.execute_calls.append({"query_text": query_text, "checkpoint": dict(checkpoint or {})})
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    response=SearchResponse(
                        provider_name=self.provider_name,
                        query_text=query_text,
                        results=[
                            SearchResultItem(
                                title="Jane Doe - LinkedIn",
                                url="https://www.linkedin.com/in/jane-doe/",
                                snippet="Thinking Machines Lab",
                            )
                        ],
                        raw_payload={"tasks": [{"id": "task_1"}]},
                        raw_format="json",
                    ),
                    checkpoint={
                        "provider_name": self.provider_name,
                        "task_id": "task_1",
                        "status": "completed",
                    },
                )

        class _WorkerHandle:
            def __init__(self, worker_id: int) -> None:
                self.worker_id = worker_id

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self._next_worker_id = 0

            def begin_worker(self, **kwargs):
                self._next_worker_id += 1
                return _WorkerHandle(self._next_worker_id)

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
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
            snapshot_dir = Path(tempdir)
            provider = _BatchSearchProvider()
            runtime = _FakeWorkerRuntime()
            acquirer = SearchSeedAcquirer([], search_provider=provider)
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=['"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in'],
                filter_hints={},
                cost_policy={"parallel_search_workers": 1, "public_media_results_per_query": 10},
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_direct_fetch",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            manifest = json.loads(
                (snapshot_dir / "search_seed_discovery" / "web_search_batch_manifest.json").read_text(encoding="utf-8")
            )
            entry = manifest["entries"][0]
            self.assertEqual(provider.execute_calls[0]["checkpoint"]["status"], "ready_cached")
            self.assertEqual(entry["search_state"]["status"], "fetched_cached")
            self.assertTrue(str(entry["search_state"]["fetch_token"]).startswith("worker_direct_"))
            self.assertTrue(str(entry["raw_path"]).endswith("web_query_01.json"))
            self.assertEqual(len(snapshot.entries), 1)

    def test_discover_cached_raw_path_updates_summary_path(self) -> None:
        class _UnusedSearchProvider:
            provider_name = "dataforseo_google_organic"

            def __init__(self) -> None:
                self.execute_calls = 0

            def submit_batch_queries(self, query_specs):
                return SearchBatchSubmissionResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchSubmissionTask(
                            task_key=str(query_specs[0]["task_key"]),
                            query_text=str(query_specs[0]["query_text"]),
                            checkpoint={
                                "provider_name": self.provider_name,
                                "task_id": "task_cached",
                                "status": "submitted",
                            },
                            metadata={"artifact_label": "task_post_batch_01"},
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post_batch_01",
                            payload={"tasks": [{"id": "task_cached"}]},
                        )
                    ],
                )

            def poll_ready_batch(self, query_specs):
                return SearchBatchReadyResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchReadyTask(
                            task_key=str(query_specs[0]["task_key"]),
                            task_id="task_cached",
                            query_text=str(query_specs[0]["query_text"]),
                            checkpoint={
                                **dict(query_specs[0].get("checkpoint") or {}),
                                "status": "ready_cached",
                            },
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="tasks_ready_batch",
                            payload={"tasks": [{"result": []}]},
                        )
                    ],
                )

            def fetch_ready_batch(self, query_specs):
                return None

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                self.execute_calls += 1
                raise AssertionError("execute_with_checkpoint should not be called when cached raw_path exists")

        class _WorkerHandle:
            def __init__(self, worker_id: int) -> None:
                self.worker_id = worker_id

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self._next_worker_id = 0

            def begin_worker(self, **kwargs):
                self._next_worker_id += 1
                return _WorkerHandle(self._next_worker_id)

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
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
            snapshot_dir = Path(tempdir)
            provider = _UnusedSearchProvider()
            runtime = _FakeWorkerRuntime()
            discovery_dir = snapshot_dir / "search_seed_discovery"
            discovery_dir.mkdir(parents=True, exist_ok=True)
            logger = AssetLogger(snapshot_dir)
            cached_raw_path = discovery_dir / "web_query_01.json"
            logger.write_json(
                cached_raw_path,
                search_response_to_record(
                    SearchResponse(
                        provider_name=provider.provider_name,
                        query_text='"Jane Doe" "Thinking Machines Lab"',
                        results=[
                            SearchResultItem(
                                title="Jane Doe - LinkedIn",
                                url="https://www.linkedin.com/in/jane-doe/",
                                snippet="Thinking Machines Lab",
                            )
                        ],
                        raw_payload={"tasks": [{"id": "task_cached"}]},
                        raw_format="json",
                    )
                ),
                asset_type="web_search_payload",
                source_kind="search_seed_discovery",
                is_raw_asset=True,
                model_safe=False,
            )
            manifest_path = discovery_dir / "web_search_batch_manifest.json"
            logger.write_json(
                manifest_path,
                {
                    "provider_name": provider.provider_name,
                    "submitted_query_count": 1,
                    "artifact_paths": {},
                    "entries": [
                        {
                            "task_key": "seed_queries::01",
                            "query": '"Jane Doe" "Thinking Machines Lab"',
                            "search_state": {
                                "provider_name": provider.provider_name,
                                "task_id": "task_cached",
                                "status": "ready_cached",
                            },
                            "artifact_paths": {},
                            "raw_path": str(cached_raw_path),
                            "metadata": {},
                        }
                    ],
                    "message": "",
                },
                asset_type="web_search_batch_manifest",
                source_kind="search_seed_discovery",
                is_raw_asset=False,
                model_safe=False,
            )
            acquirer = SearchSeedAcquirer([], search_provider=provider)
            result = acquirer._execute_query_spec(
                index=1,
                query_spec={
                    "bundle_id": "seed_queries",
                    "source_family": "public_web_search",
                    "execution_mode": "low_cost_web_search",
                    "query": '"Jane Doe" "Thinking Machines Lab"',
                },
                identity=identity,
                discovery_dir=discovery_dir,
                logger=logger,
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_cached",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                result_limit=10,
                prefetched_search_state={
                    "provider_name": provider.provider_name,
                    "task_id": "task_cached",
                    "status": "ready_cached",
                },
                prefetched_search_artifact_paths={},
                prefetched_search_raw_path=str(cached_raw_path),
                prefetched_search_manifest_path=str(manifest_path),
                prefetched_search_manifest_key="seed_queries::01",
            )
            self.assertEqual(provider.execute_calls, 0)
            self.assertEqual(result["worker_status"], "completed")
            self.assertEqual(result["summary"]["raw_path"], str(cached_raw_path))
