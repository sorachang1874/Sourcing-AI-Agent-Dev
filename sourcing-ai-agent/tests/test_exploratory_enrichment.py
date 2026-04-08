import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.asset_policy import DEFAULT_MODEL_CONTEXT_CHAR_LIMIT, RAW_ASSET_POLICY_SUMMARY
from sourcing_agent.domain import Candidate
from sourcing_agent.exploratory_enrichment import ExploratoryWebEnricher, _build_exploration_queries, build_page_analysis_input, extract_page_signals
from sourcing_agent.search_provider import SearchExecutionArtifact, SearchExecutionResult


class ExploratoryEnrichmentTest(unittest.TestCase):
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
