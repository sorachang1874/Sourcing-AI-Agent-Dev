import tempfile
import unittest

from sourcing_agent.search_provider import (
    BaseSearchProvider,
    SearchBatchFetchResult,
    SearchBatchFetchTask,
    SearchBatchReadyResult,
    SearchBatchReadyTask,
    SearchBatchSubmissionResult,
    SearchBatchSubmissionTask,
    SearchResponse,
    SearchResultItem,
)
from sourcing_agent.storage import ControlPlaneStore
from sourcing_agent.target_candidate_public_web import (
    build_person_public_web_signal_rows,
    execute_target_candidate_public_web_run_once,
    start_target_candidate_public_web_batch,
)


class _DeferredBatchSearchProvider(BaseSearchProvider):
    provider_name = "deferred_batch_search"

    def __init__(self) -> None:
        self.submit_calls = 0
        self.poll_calls = 0
        self.fetch_calls = 0

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        raise AssertionError("productized Public Web run should use batch search in this test")

    def submit_batch_queries(self, query_specs: list[dict]) -> SearchBatchSubmissionResult | None:
        self.submit_calls += 1
        tasks = [
            SearchBatchSubmissionTask(
                task_key=str(spec.get("task_key") or ""),
                query_text=str(spec.get("query_text") or ""),
                checkpoint={
                    "provider_name": self.provider_name,
                    "task_id": f"task-{index}",
                    "status": "submitted",
                },
                metadata={"task_id": f"task-{index}"},
            )
            for index, spec in enumerate(query_specs, start=1)
        ]
        return SearchBatchSubmissionResult(provider_name=self.provider_name, tasks=tasks)

    def poll_ready_batch(self, query_specs: list[dict]) -> SearchBatchReadyResult | None:
        self.poll_calls += 1
        ready = self.poll_calls >= 2
        tasks = [
            SearchBatchReadyTask(
                task_key=str(spec.get("task_key") or ""),
                task_id=str(dict(spec.get("checkpoint") or {}).get("task_id") or spec.get("task_id") or ""),
                query_text=str(spec.get("query_text") or ""),
                checkpoint={
                    **dict(spec.get("checkpoint") or {}),
                    "status": "ready_cached" if ready else "waiting_for_ready_cached",
                },
                metadata={"ready": ready},
            )
            for spec in query_specs
        ]
        return SearchBatchReadyResult(provider_name=self.provider_name, tasks=tasks)

    def fetch_ready_batch(self, query_specs: list[dict]) -> SearchBatchFetchResult | None:
        self.fetch_calls += 1
        tasks = []
        for spec in query_specs:
            query_text = str(spec.get("query_text") or "")
            tasks.append(
                SearchBatchFetchTask(
                    task_key=str(spec.get("task_key") or ""),
                    task_id=str(dict(spec.get("checkpoint") or {}).get("task_id") or spec.get("task_id") or ""),
                    query_text=query_text,
                    response=SearchResponse(
                        provider_name=self.provider_name,
                        query_text=query_text,
                        results=[
                            SearchResultItem(
                                title="Ada Lovelace homepage",
                                url="https://ada.example.edu/",
                                snippet="Ada Lovelace researcher at Example AI. Publications and contact.",
                            )
                        ],
                        raw_payload={"query": query_text},
                        raw_format="json",
                    ),
                    checkpoint={
                        **dict(spec.get("checkpoint") or {}),
                        "status": "fetched_cached",
                    },
                    metadata={"fetched": True},
                )
            )
        return SearchBatchFetchResult(provider_name=self.provider_name, tasks=tasks)


class TargetCandidatePublicWebTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = ControlPlaneStore(f"{self.tempdir.name}/test.db")
        self.store.upsert_target_candidate(
            {
                "record_id": "target-ada",
                "candidate_id": "cand-ada",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
                "headline": "Research Engineer",
                "linkedin_url": "https://www.linkedin.com/in/ada-lovelace/",
            }
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_batch_trigger_is_idempotent_and_creates_per_candidate_run(self) -> None:
        payload = {
            "record_ids": ["target-ada"],
            "options": {
                "max_queries_per_candidate": 4,
                "fetch_content": False,
                "ai_extraction": "off",
            },
        }

        first = start_target_candidate_public_web_batch(
            store=self.store,
            target_candidates=[self.store.get_target_candidate("target-ada")],
            runtime_dir=self.tempdir.name,
            payload=payload,
        )
        second = start_target_candidate_public_web_batch(
            store=self.store,
            target_candidates=[self.store.get_target_candidate("target-ada")],
            runtime_dir=self.tempdir.name,
            payload=payload,
        )

        self.assertEqual(first["status"], "queued")
        self.assertEqual(second["status"], "joined")
        self.assertEqual(first["batch"]["batch_id"], second["batch"]["batch_id"])
        runs = self.store.list_target_candidate_public_web_runs(batch_id=first["batch"]["batch_id"])
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["record_id"], "target-ada")
        self.assertTrue(runs[0]["person_identity_key"].startswith("linkedin:"))

    def test_run_resumes_dataforseo_style_batch_checkpoint_before_analysis(self) -> None:
        trigger = start_target_candidate_public_web_batch(
            store=self.store,
            target_candidates=[self.store.get_target_candidate("target-ada")],
            runtime_dir=self.tempdir.name,
            payload={
                "record_ids": ["target-ada"],
                "options": {
                    "max_queries_per_candidate": 1,
                    "fetch_content": False,
                    "ai_extraction": "off",
                },
            },
        )
        run_id = trigger["runs"][0]["run_id"]
        provider = _DeferredBatchSearchProvider()

        first = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )
        self.assertEqual(first["worker_status"], "running")
        after_first = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(after_first["status"], "searching")
        self.assertEqual(after_first["search_checkpoint"]["stage"], "waiting_remote_search")
        self.assertEqual(provider.submit_calls, 1)
        self.assertEqual(provider.poll_calls, 1)

        second = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )

        self.assertEqual(second["worker_status"], "completed")
        completed = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(completed["status"], "completed")
        self.assertGreater(completed["summary"]["entry_link_count"], 0)
        self.assertEqual(provider.submit_calls, 1)
        self.assertEqual(provider.poll_calls, 2)
        self.assertEqual(provider.fetch_calls, 1)
        asset = self.store.get_person_public_web_asset(person_identity_key=completed["person_identity_key"])
        self.assertIsNotNone(asset)
        self.assertEqual(asset["latest_run_id"], run_id)
        signals = self.store.list_person_public_web_signals(run_id=run_id)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["signal_kind"], "profile_link")
        self.assertTrue(signals[0]["signal_type"])
        self.assertEqual(signals[0]["source_url"], "https://ada.example.edu/")

    def test_signal_rows_preserve_email_identity_and_model_safe_artifact_refs(self) -> None:
        run = {
            "run_id": "run-signals-1",
            "record_id": "target-ada",
            "candidate_id": "cand-ada",
            "candidate_name": "Ada Lovelace",
            "current_company": "Example AI",
            "linkedin_url_key": "ada-lovelace",
            "person_identity_key": "linkedin:ada-lovelace",
            "artifact_root": f"{self.tempdir.name}/public_web/run-signals-1",
        }
        signals = {
            "email_candidates": [
                {
                    "value": "ada@example.edu",
                    "normalized_value": "ada@example.edu",
                    "email_type": "academic",
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "publishable": True,
                    "promotion_status": "promotion_recommended",
                    "source_url": "https://ada.example.edu/",
                    "source_domain": "ada.example.edu",
                    "source_family": "profile_web_presence",
                    "source_title": "Ada Lovelace",
                    "evidence_excerpt": "Contact Ada at ada@example.edu",
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.9,
                }
            ],
            "entry_links": [
                {
                    "url": "https://github.com/ada",
                    "normalized_url": "https://github.com/ada",
                    "title": "Ada on GitHub",
                    "snippet": "Ada Lovelace research code",
                    "source_domain": "github.com",
                    "entry_type": "github_url",
                    "source_family": "technical_presence",
                    "score": 0.82,
                    "identity_match_label": "ambiguous_identity",
                    "identity_match_score": 0.41,
                    "confidence_label": "medium",
                },
                {
                    "url": "https://x.com/ada_ai/status/123",
                    "normalized_url": "https://x.com/ada_ai/status/123",
                    "title": "Ada AI on X",
                    "snippet": "Ada Lovelace post about Example AI",
                    "source_domain": "x.com",
                    "entry_type": "x_url",
                    "source_family": "social_presence",
                    "score": 0.8,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.77,
                    "confidence_label": "medium",
                },
            ],
            "fetched_documents": [
                {
                    "source_url": "https://ada.example.edu/",
                    "final_url": "https://ada.example.edu/",
                    "raw_path": "/tmp/raw.html",
                    "analysis_input_path": "/tmp/input.txt",
                    "analysis_path": "/tmp/analysis.json",
                    "evidence_slice_path": "/tmp/evidence.json",
                }
            ],
            "ai_adjudication": {"status": "completed", "provider": "deterministic"},
        }

        rows = build_person_public_web_signal_rows(run=run, signals=signals)
        self.assertEqual(len(rows), 3)
        email = next(row for row in rows if row["signal_kind"] == "email_candidate")
        link = next(row for row in rows if row["signal_kind"] == "profile_link" and row["source_domain"] == "github.com")
        x_post = next(row for row in rows if row["signal_kind"] == "profile_link" and row["source_domain"] == "x.com")
        self.assertEqual(email["promotion_status"], "promotion_recommended")
        self.assertEqual(email["identity_match_label"], "likely_same_person")
        self.assertEqual(email["artifact_refs"]["analysis_path"], "/tmp/analysis.json")
        self.assertNotIn("raw_path", email["artifact_refs"])
        self.assertFalse(link["publishable"])
        self.assertEqual(link["identity_match_label"], "ambiguous_identity")
        self.assertFalse(x_post["publishable"])
        self.assertEqual(x_post["suppression_reason"], "non_profile_link_shape")
        self.assertIn("x_link_not_profile", x_post["metadata"]["link_shape_warnings"])
        self.assertFalse(x_post["metadata"]["clean_profile_link"])

    def test_promotion_rows_preserve_signal_lineage_without_raw_assets(self) -> None:
        promotion = self.store.upsert_target_candidate_public_web_promotion(
            {
                "promotion_id": "promotion-ada-email-1",
                "signal_id": "signal-ada-email-1",
                "run_id": "run-ada-1",
                "asset_id": "asset-ada-1",
                "person_identity_key": "linkedin:ada-lovelace",
                "record_id": "target-ada",
                "candidate_id": "cand-ada",
                "candidate_name": "Ada Lovelace",
                "signal_kind": "email_candidate",
                "signal_type": "academic",
                "email_type": "academic",
                "value": "ada@example.edu",
                "normalized_value": "ada@example.edu",
                "source_url": "https://ada.example.edu/",
                "source_domain": "ada.example.edu",
                "source_family": "profile_web_presence",
                "confidence_label": "high",
                "confidence_score": 0.93,
                "identity_match_label": "likely_same_person",
                "identity_match_score": 0.91,
                "publishable": True,
                "action": "promote",
                "promoted_field": "primary_email",
                "previous_value": "",
                "new_value": "ada@example.edu",
                "operator": "unit-test",
                "metadata": {
                    "raw_assets_included": False,
                    "raw_path": "/tmp/raw.html",
                },
            }
        )

        loaded = self.store.list_target_candidate_public_web_promotions(record_id="target-ada")
        self.assertEqual(len(loaded), 1)
        self.assertEqual(promotion["promotion_status"], "manually_promoted")
        self.assertEqual(loaded[0]["signal_id"], "signal-ada-email-1")
        self.assertEqual(loaded[0]["source_url"], "https://ada.example.edu/")
        self.assertEqual(loaded[0]["previous_value"], "")
        self.assertEqual(loaded[0]["new_value"], "ada@example.edu")
        self.assertFalse(loaded[0]["metadata"]["raw_assets_included"])


if __name__ == "__main__":
    unittest.main()
