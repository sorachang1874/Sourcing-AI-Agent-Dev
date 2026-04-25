import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import sourcing_agent.exploratory_enrichment as exploratory_enrichment_module
from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.asset_policy import DEFAULT_MODEL_CONTEXT_CHAR_LIMIT, RAW_ASSET_POLICY_SUMMARY
from sourcing_agent.domain import Candidate
from sourcing_agent.exploratory_enrichment import ExploratoryWebEnricher, _build_exploration_queries, build_page_analysis_input, extract_page_signals
from sourcing_agent.search_provider import (
    SearchBatchFetchResult,
    SearchBatchFetchTask,
    SearchBatchReadyResult,
    SearchBatchReadyTask,
    SearchBatchSubmissionResult,
    SearchBatchSubmissionTask,
    SearchExecutionArtifact,
    SearchExecutionResult,
    SearchResponse,
    SearchResultItem,
)


class ExploratoryEnrichmentTest(unittest.TestCase):
    def test_runtime_exploration_poll_intervals_follow_env_without_reload(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS": "0",
                "WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS": "0",
            },
            clear=False,
        ):
            self.assertEqual(exploratory_enrichment_module._lane_ready_poll_min_interval_seconds(), 0)
            self.assertEqual(exploratory_enrichment_module._lane_fetch_min_interval_seconds(), 0)

        with mock.patch.dict(
            os.environ,
            {
                "WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS": "7",
                "WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS": "8",
                "EXPLORATION_READY_POLL_MIN_INTERVAL_SECONDS": "1",
                "EXPLORATION_FETCH_MIN_INTERVAL_SECONDS": "2",
            },
            clear=False,
        ):
            self.assertEqual(exploratory_enrichment_module._lane_ready_poll_min_interval_seconds(), 1)
            self.assertEqual(exploratory_enrichment_module._lane_fetch_min_interval_seconds(), 2)

    def test_extract_page_signals(self) -> None:
        html = """
        <html>
          <head>
            <title>Jane Doe</title>
            <meta name="description" content="Research engineer at xAI. Ex OpenAI.">
          </head>
          <body>
            <a href="https://www.linkedin.com/in/jane-doe/">LinkedIn</a>
            <a href="https://x.com/janedoe">X</a>
            <a href="https://github.com/janedoe">GitHub</a>
            <a href="https://janedoe.dev">Homepage</a>
            <a href="https://janedoe.dev/Jane_Doe_CV.pdf">CV</a>
          </body>
        </html>
        """
        signals = extract_page_signals(html, "https://janedoe.dev/about")
        self.assertEqual(signals["linkedin_urls"][0], "https://www.linkedin.com/in/jane-doe/")
        self.assertEqual(signals["x_urls"][0], "https://x.com/janedoe")
        self.assertEqual(signals["github_urls"][0], "https://github.com/janedoe")
        self.assertIn("https://janedoe.dev", signals["personal_urls"][0])
        self.assertEqual(signals["resume_urls"][0], "https://janedoe.dev/Jane_Doe_CV.pdf")
        self.assertIn("Research engineer at xAI.", signals["descriptions"][1])
        self.assertEqual(signals["education_signals"], [])

    def test_build_page_analysis_input_truncates_excerpt(self) -> None:
        candidate = Candidate(candidate_id="c1", name_en="Jane Doe", display_name="Jane Doe")
        html = "<html><head><title>Jane Doe</title><meta name=\"description\" content=\"Research engineer\"></head><body>" + ("signal " * 1000) + "</body></html>"
        payload = build_page_analysis_input(
            candidate=candidate,
            target_company="xAI",
            source_url="https://janedoe.dev",
            html_text=html,
            extracted_links={"linkedin_urls": [], "x_urls": [], "github_urls": [], "personal_urls": [], "resume_urls": []},
        )
        self.assertLessEqual(payload["excerpt_char_count"], DEFAULT_MODEL_CONTEXT_CHAR_LIMIT)
        self.assertEqual(payload["raw_asset_policy"], RAW_ASSET_POLICY_SUMMARY)
        self.assertIn("document_type", payload)
        self.assertIn("text_blocks", payload)

    def test_build_exploration_queries_adds_publication_context(self) -> None:
        candidate = Candidate(
            candidate_id="c1",
            name_en="Jane Doe",
            display_name="Jane Doe",
            metadata={"publication_title": "Scaling Laws in Practice"},
        )
        queries = _build_exploration_queries(candidate, "Thinking Machines Lab")
        self.assertIn('"Jane Doe" "Thinking Machines Lab"', queries)
        self.assertIn('"Jane Doe" "Scaling Laws in Practice"', queries)
        self.assertIn('"Jane Doe" "Scaling Laws in Practice" "Thinking Machines Lab"', queries)

    def test_explore_candidate_queues_worker_when_dataforseo_task_not_ready(self) -> None:
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

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                self.completed.append(kwargs)
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

        candidate = Candidate(
            candidate_id="c1",
            name_en="Jane Doe",
            display_name="Jane Doe",
            target_company="Thinking Machines Lab",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            enricher = ExploratoryWebEnricher(
                worker_runtime=_FakeWorkerRuntime(),
                search_provider=_PendingSearchProvider(),
            )
            result = enricher._explore_candidate(
                snapshot_dir=snapshot_dir,
                candidate=candidate,
                target_company="Thinking Machines Lab",
                logger=AssetLogger(snapshot_dir),
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(result["worker_status"], "queued")
            self.assertEqual(result["summary"]["status"], "queued")
            self.assertTrue((snapshot_dir / "exploration" / "c1" / "query_01_task_post.json").exists())

    def test_enrich_reports_queued_background_exploration(self) -> None:
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
            def begin_worker(self, **kwargs):
                return _WorkerHandle()

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

            def list_workers(self, **kwargs):
                return []

        candidate = Candidate(
            candidate_id="c1",
            name_en="Jane Doe",
            display_name="Jane Doe",
            target_company="Thinking Machines Lab",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            enricher = ExploratoryWebEnricher(
                worker_runtime=_FakeWorkerRuntime(),
                search_provider=_PendingSearchProvider(),
            )
            result = enricher.enrich(
                snapshot_dir,
                [candidate],
                target_company="Thinking Machines Lab",
                max_candidates=1,
                asset_logger=AssetLogger(snapshot_dir),
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                parallel_workers=1,
            )
            summary_path = snapshot_dir / "exploration" / "summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(result.queued_candidate_count, 1)
            self.assertEqual(result.stop_reason, "queued_background_exploration")
            self.assertEqual(summary["queued_candidate_count"], 1)
            self.assertEqual(summary["stop_reason"], "queued_background_exploration")

    def test_enrich_batch_prefetches_exploration_queries(self) -> None:
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
                            payload={"tasks": [{"id": "task_1"}]},
                        )
                    ],
                )

            def poll_ready_batch(self, query_specs):
                self.ready_calls.append(list(query_specs))
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
                    ]
                    + [
                        SearchBatchReadyTask(
                            task_key=str(item["task_key"]),
                            task_id=str(dict(item.get("checkpoint") or {}).get("task_id") or ""),
                            query_text=str(item["query_text"]),
                            checkpoint={
                                **dict(item.get("checkpoint") or {}),
                                "status": "waiting_for_ready_cached",
                            },
                        )
                        for item in query_specs[1:]
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
                            task_key=str(query_specs[0]["task_key"]),
                            task_id="task_1",
                            query_text=str(query_specs[0]["query_text"]),
                            response=SearchResponse(
                                provider_name=self.provider_name,
                                query_text=str(query_specs[0]["query_text"]),
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
                self._next_worker_id = 0

            def begin_worker(self, **kwargs):
                self._next_worker_id += 1
                return _WorkerHandle(self._next_worker_id)

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

            def list_workers(self, **kwargs):
                return []

        candidate = Candidate(
            candidate_id="c1",
            name_en="Jane Doe",
            display_name="Jane Doe",
            target_company="Thinking Machines Lab",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            provider = _BatchSearchProvider()
            enricher = ExploratoryWebEnricher(
                worker_runtime=_FakeWorkerRuntime(),
                search_provider=provider,
            )
            result = enricher.enrich(
                snapshot_dir,
                [candidate],
                target_company="Thinking Machines Lab",
                max_candidates=1,
                asset_logger=AssetLogger(snapshot_dir),
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                parallel_workers=1,
            )
            self.assertEqual(len(provider.batch_calls), 1)
            self.assertEqual(len(provider.ready_calls), 1)
            self.assertEqual(len(provider.fetch_calls), 1)
            self.assertGreaterEqual(len(provider.batch_calls[0]), 7)
            self.assertEqual(provider.execute_calls[0]["checkpoint"]["task_id"], "task_2")
            self.assertEqual(provider.execute_calls[0]["checkpoint"]["status"], "waiting_for_ready_cached")
            self.assertEqual(result.queued_candidate_count, 1)
            self.assertTrue((snapshot_dir / "exploration" / "c1" / "query_01_prefetched.json").exists())
            self.assertTrue((snapshot_dir / "exploration" / "search_batch_manifest.json").exists())
            enricher.enrich(
                snapshot_dir,
                [candidate],
                target_company="Thinking Machines Lab",
                max_candidates=1,
                asset_logger=AssetLogger(snapshot_dir),
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                parallel_workers=1,
            )
            self.assertEqual(len(provider.ready_calls), 1)
            self.assertEqual(len(provider.fetch_calls), 1)

    def test_prepare_batched_exploration_queries_reuses_cached_query_list_per_candidate(self) -> None:
        class _BatchSearchProvider:
            provider_name = "dataforseo_google_organic"

            def submit_batch_queries(self, query_specs):
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
                            metadata={},
                        )
                        for index, item in enumerate(query_specs, start=1)
                    ],
                    artifacts=[],
                )

            def poll_ready_batch(self, query_specs):
                return SearchBatchReadyResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchReadyTask(
                            task_key=str(item["task_key"]),
                            task_id=str(dict(item.get("checkpoint") or {}).get("task_id") or ""),
                            query_text=str(item["query_text"]),
                            checkpoint={
                                **dict(item.get("checkpoint") or {}),
                                "status": "waiting_for_ready_cached",
                            },
                        )
                        for item in query_specs
                    ],
                    artifacts=[],
                )

            def fetch_ready_batch(self, query_specs):
                raise AssertionError("fetch_ready_batch should not run when nothing is ready")

        candidate = Candidate(
            candidate_id="c1",
            name_en="Jane Doe",
            display_name="Jane Doe",
            target_company="Thinking Machines Lab",
        )
        pending_specs = [{"candidate": candidate, "runtime_timing_overrides": {}}]
        with tempfile.TemporaryDirectory() as tempdir, mock.patch.object(
            exploratory_enrichment_module,
            "_build_exploration_queries",
            return_value=["q1", "q2"],
        ) as build_queries_mock:
            errors = exploratory_enrichment_module._prepare_batched_exploration_queries(
                search_provider=_BatchSearchProvider(),
                logger=AssetLogger(Path(tempdir)),
                exploration_dir=Path(tempdir) / "exploration",
                pending_specs=pending_specs,
                target_company="Thinking Machines Lab",
            )

        self.assertEqual(errors, [])
        self.assertEqual(build_queries_mock.call_count, 1)
        self.assertEqual(pending_specs[0]["exploration_queries"], ["q1", "q2"])

    def test_enrich_does_not_repoll_ready_cached_queries(self) -> None:
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
                            payload={"tasks": [{"id": "task_1"}]},
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
                                task_key=str(query_specs[0]["task_key"]),
                                task_id="task_1",
                                query_text=str(query_specs[0]["query_text"]),
                                checkpoint={
                                    **dict(query_specs[0].get("checkpoint") or {}),
                                    "status": "ready_cached",
                                },
                            )
                        ]
                        + [
                            SearchBatchReadyTask(
                                task_key=str(item["task_key"]),
                                task_id=str(dict(item.get("checkpoint") or {}).get("task_id") or ""),
                                query_text=str(item["query_text"]),
                                checkpoint={
                                    **dict(item.get("checkpoint") or {}),
                                    "status": "waiting_for_ready_cached",
                                },
                            )
                            for item in query_specs[1:]
                        ],
                        artifacts=[
                            SearchExecutionArtifact(
                                label="tasks_ready_batch",
                                payload={"tasks": [{"result": []}]},
                            )
                        ],
                    )
                self.assertTrue(all(not item["task_key"].endswith("::01") for item in query_specs))
                return SearchBatchReadyResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchReadyTask(
                            task_key=str(item["task_key"]),
                            task_id=str(dict(item.get("checkpoint") or {}).get("task_id") or ""),
                            query_text=str(item["query_text"]),
                            checkpoint={
                                **dict(item.get("checkpoint") or {}),
                                "status": "waiting_for_ready_cached",
                            },
                        )
                        for item in query_specs
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

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

            def list_workers(self, **kwargs):
                return []

        candidate = Candidate(
            candidate_id="c1",
            name_en="Jane Doe",
            display_name="Jane Doe",
            target_company="Thinking Machines Lab",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            provider = _BatchSearchProvider()
            enricher = ExploratoryWebEnricher(
                worker_runtime=_FakeWorkerRuntime(),
                search_provider=provider,
            )
            enricher.enrich(
                snapshot_dir,
                [candidate],
                target_company="Thinking Machines Lab",
                max_candidates=1,
                asset_logger=AssetLogger(snapshot_dir),
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                parallel_workers=1,
            )
            manifest_path = snapshot_dir / "exploration" / "search_batch_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            for item in manifest["entries"]:
                search_state = dict(item.get("search_state") or {})
                search_state["ready_attempted_at"] = "2026-01-01T00:00:00+00:00"
                search_state["fetch_attempted_at"] = "2026-01-01T00:00:00+00:00"
                item["search_state"] = search_state
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

            enricher.enrich(
                snapshot_dir,
                [candidate],
                target_company="Thinking Machines Lab",
                max_candidates=1,
                asset_logger=AssetLogger(snapshot_dir),
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                parallel_workers=1,
            )
            self.assertTrue(provider.ready_calls[0][0].endswith("::01"))
            self.assertTrue(all(not key.endswith("::01") for key in provider.ready_calls[1]))

    def test_enrich_worker_direct_fetch_updates_batch_manifest(self) -> None:
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
                    ]
                    + [
                        SearchBatchReadyTask(
                            task_key=str(item["task_key"]),
                            task_id=str(dict(item.get("checkpoint") or {}).get("task_id") or ""),
                            query_text=str(item["query_text"]),
                            checkpoint={
                                **dict(item.get("checkpoint") or {}),
                                "status": "waiting_for_ready_cached",
                            },
                        )
                        for item in query_specs[1:]
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
                if dict(checkpoint or {}).get("status") == "ready_cached":
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

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

            def list_workers(self, **kwargs):
                return []

        candidate = Candidate(
            candidate_id="c1",
            name_en="Jane Doe",
            display_name="Jane Doe",
            target_company="Thinking Machines Lab",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            provider = _BatchSearchProvider()
            enricher = ExploratoryWebEnricher(
                worker_runtime=_FakeWorkerRuntime(),
                search_provider=provider,
            )
            result = enricher.enrich(
                snapshot_dir,
                [candidate],
                target_company="Thinking Machines Lab",
                max_candidates=1,
                asset_logger=AssetLogger(snapshot_dir),
                job_id="job_direct_fetch",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                parallel_workers=1,
            )
            manifest = json.loads(
                (snapshot_dir / "exploration" / "search_batch_manifest.json").read_text(encoding="utf-8")
            )
            first_entry = next(item for item in manifest["entries"] if item["task_key"].endswith("::01"))
            self.assertEqual(provider.execute_calls[0]["checkpoint"]["status"], "ready_cached")
            self.assertEqual(first_entry["search_state"]["status"], "fetched_cached")
            self.assertTrue(str(first_entry["search_state"]["fetch_token"]).startswith("worker_direct_"))
            self.assertTrue(str(first_entry["raw_path"]).endswith("query_01.json"))
            self.assertEqual(result.queued_candidate_count, 1)

    def test_explore_candidate_reuses_active_prefetched_raw_without_execute(self) -> None:
        class _NoExecuteSearchProvider:
            provider_name = "dataforseo_google_organic"

            def __init__(self) -> None:
                self.execute_calls = 0

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                self.execute_calls += 1
                raise AssertionError("active prefetched raw should bypass execute_with_checkpoint")

        class _WorkerHandle:
            worker_id = 1

        class _FakeWorkerRuntime:
            def __init__(self, checkpoint: dict) -> None:
                self._checkpoint = dict(checkpoint)
                self.completed = []

            def begin_worker(self, **kwargs):
                return _WorkerHandle()

            def get_worker(self, worker_id):
                return {"checkpoint": dict(self._checkpoint), "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                self.completed.append(dict(kwargs))
                self._checkpoint = dict(kwargs.get("checkpoint_payload") or self._checkpoint)
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                self._checkpoint = dict(kwargs.get("checkpoint_payload") or self._checkpoint)
                return kwargs

        candidate = Candidate(
            candidate_id="c1",
            name_en="Jane Doe",
            display_name="Jane Doe",
            target_company="Thinking Machines Lab",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            query_text = _build_exploration_queries(candidate, "Thinking Machines Lab")[0]
            prefetched_path = snapshot_dir / "exploration" / "c1" / "query_01_prefetched.json"
            prefetched_path.parent.mkdir(parents=True, exist_ok=True)
            prefetched_path.write_text(
                json.dumps(
                    {
                        "provider_name": "dataforseo_google_organic",
                        "query_text": query_text,
                        "results": [],
                        "raw_payload": {"tasks": []},
                        "raw_format": "json",
                        "final_url": "",
                        "content_type": "application/json",
                        "metadata": {},
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            ready_artifact_path = snapshot_dir / "exploration" / "search_tasks_ready_batch_20260410T101252Z_tasks_ready_batch.json"
            ready_artifact_path.write_text("{}", encoding="utf-8")
            initial_checkpoint = {
                "completed_queries": ["2", "3", "4", "5", "6", "7"],
                "active_query_index": 1,
                "active_query_text": query_text,
                "active_search_state": {
                    "provider_name": "dataforseo_google_organic",
                    "task_id": "task_1",
                    "status": "waiting_for_ready",
                },
                "prefetched_queries": {
                    "1": {
                        "task_key": "c1::01",
                        "query": query_text,
                        "search_state": {
                            "provider_name": "dataforseo_google_organic",
                            "task_id": "task_1",
                            "status": "waiting_for_ready",
                        },
                        "artifact_paths": {
                            "tasks_ready_batch_20260410T101252Z": str(ready_artifact_path),
                        },
                        "raw_path": "",
                    }
                },
            }
            prefetched_search_queries = {
                "1": {
                    "task_key": "c1::01",
                    "query": query_text,
                    "search_state": {
                        "provider_name": "dataforseo_google_organic",
                        "task_id": "task_1",
                        "status": "fetched_cached",
                        "ready_poll_source": "lane_batch",
                    },
                    "artifact_paths": {
                        "tasks_ready_batch_20260410T101252Z": str(ready_artifact_path),
                    },
                    "raw_path": str(prefetched_path),
                }
            }
            runtime = _FakeWorkerRuntime(initial_checkpoint)
            provider = _NoExecuteSearchProvider()
            enricher = ExploratoryWebEnricher(
                worker_runtime=runtime,
                search_provider=provider,
            )
            result = enricher._explore_candidate(
                snapshot_dir=snapshot_dir,
                candidate=candidate,
                target_company="Thinking Machines Lab",
                logger=AssetLogger(snapshot_dir),
                job_id="job_prefetched_active",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                prefetched_search_queries=prefetched_search_queries,
            )
            self.assertEqual(provider.execute_calls, 0)
            self.assertEqual(result["worker_status"], "completed")
            self.assertEqual(runtime.completed[-1]["status"], "completed")
