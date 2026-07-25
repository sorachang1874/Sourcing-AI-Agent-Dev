"""Migration-only coverage for retired target-candidate Public Web runtime.

Normal product behavior is covered through CRM Public Web tests. W7e removed
the old target-candidate execution override; remaining legacy tests either
assert fail-closed behavior or cover pure signal/promotion row helpers.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.legacy_public_web_storage import (
    seed_legacy_target_public_web_batch,
    seed_legacy_target_public_web_promotion,
    seed_legacy_target_public_web_run,
    update_legacy_target_public_web_run,
)
from sourcing_agent.legacy_target_candidate_public_web_runtime import (
    cancel_target_candidate_public_web_run,
    execute_target_candidate_public_web_run_once,
    execute_target_candidate_public_web_run_to_local_idle,
    start_target_candidate_public_web_batch,
    sync_public_web_batch_summary,
)
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.public_web_runtime_core import (
    build_person_public_web_signal_rows,
    sync_public_web_batch_to_crm_owner,
)
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
    build_search_provider,
)
from sourcing_agent.settings import SearchProviderSettings
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


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


class _StaggeredBatchSearchProvider(BaseSearchProvider):
    provider_name = "staggered_batch_search"

    def __init__(self, *, ready_after_by_record: dict[str, int], fail_fetch_record_ids: set[str] | None = None) -> None:
        self.ready_after_by_record = dict(ready_after_by_record)
        self.fail_fetch_record_ids = set(fail_fetch_record_ids or set())
        self.submit_calls = 0
        self.poll_calls = 0
        self.fetch_calls = 0
        self.poll_counts_by_task: dict[str, int] = {}
        self.submitted_record_ids: list[str] = []
        self.fetched_record_ids: list[str] = []

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        raise AssertionError("productized Public Web run should use batch search in this test")

    def submit_batch_queries(self, query_specs: list[dict]) -> SearchBatchSubmissionResult | None:
        self.submit_calls += 1
        tasks = []
        for index, spec in enumerate(query_specs, start=1):
            record_id = str(dict(spec.get("metadata") or {}).get("record_id") or "")
            self.submitted_record_ids.append(record_id)
            tasks.append(
                SearchBatchSubmissionTask(
                    task_key=str(spec.get("task_key") or ""),
                    query_text=str(spec.get("query_text") or ""),
                    checkpoint={
                        "provider_name": self.provider_name,
                        "task_id": f"staggered-task-{record_id}-{index}",
                        "status": "submitted",
                    },
                    metadata={"task_id": f"staggered-task-{record_id}-{index}", "record_id": record_id},
                )
            )
        return SearchBatchSubmissionResult(provider_name=self.provider_name, tasks=tasks)

    def poll_ready_batch(self, query_specs: list[dict]) -> SearchBatchReadyResult | None:
        self.poll_calls += 1
        tasks = []
        for spec in query_specs:
            task_key = str(spec.get("task_key") or "")
            record_id = str(dict(spec.get("metadata") or {}).get("record_id") or "")
            poll_count = int(self.poll_counts_by_task.get(task_key) or 0) + 1
            self.poll_counts_by_task[task_key] = poll_count
            ready_after = max(1, int(self.ready_after_by_record.get(record_id, 1)))
            ready = poll_count >= ready_after
            tasks.append(
                SearchBatchReadyTask(
                    task_key=task_key,
                    task_id=str(dict(spec.get("checkpoint") or {}).get("task_id") or spec.get("task_id") or ""),
                    query_text=str(spec.get("query_text") or ""),
                    checkpoint={
                        **dict(spec.get("checkpoint") or {}),
                        "status": "ready_cached" if ready else "waiting_for_ready_cached",
                    },
                    metadata={"ready": ready, "record_id": record_id},
                )
            )
        return SearchBatchReadyResult(provider_name=self.provider_name, tasks=tasks)

    def fetch_ready_batch(self, query_specs: list[dict]) -> SearchBatchFetchResult | None:
        self.fetch_calls += 1
        tasks = []
        for spec in query_specs:
            record_id = str(dict(spec.get("metadata") or {}).get("record_id") or "")
            self.fetched_record_ids.append(record_id)
            if record_id in self.fail_fetch_record_ids:
                raise RuntimeError(f"scripted_fetch_failed:{record_id}")
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
                                title=f"{record_id} homepage",
                                url=f"https://{record_id}.example.edu/",
                                snippet=f"{record_id} researcher at Example AI. Publications and contact.",
                            )
                        ],
                        raw_payload={"record_id": record_id},
                        raw_format="json",
                    ),
                    checkpoint={**dict(spec.get("checkpoint") or {}), "status": "fetched_cached"},
                    metadata={"fetched": True, "record_id": record_id},
                )
            )
        return SearchBatchFetchResult(provider_name=self.provider_name, tasks=tasks)


class TargetCandidatePublicWebTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.env_patch_active = False
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = self.make_pg_store(f"{self.tempdir.name}/test.db")
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
        self.store.upsert_target_candidate(
            {
                "record_id": "target-grace",
                "candidate_id": "cand-grace",
                "candidate_name": "Grace Hopper",
                "current_company": "Example AI",
                "headline": "Systems Engineer",
                "linkedin_url": "https://www.linkedin.com/in/grace-hopper/",
            }
        )

    def tearDown(self) -> None:
        if self.env_patch_active:
            self.env_patch.stop()
            self.env_patch_active = False
        self.tempdir.cleanup()

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
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

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
    def test_target_batch_summary_does_not_sync_to_crm_owner_by_default(self) -> None:
        crm_record = self.store.upsert_crm_record(
            {
                "crm_record_id": "crm-public-web-legacy-sync-disabled",
                "workspace_id": "default",
                "person_identity_key": "linkedin:crm-public-web-legacy-sync-disabled",
                "candidate_identity_key": "linkedin:crm-public-web-legacy-sync-disabled",
                "display_name": "CRM Legacy Sync Disabled",
                "current_company": "Example AI",
            }
        )
        batch = seed_legacy_target_public_web_batch(
            self.store,
            {
                "batch_id": "legacy-sync-disabled-batch",
                "idempotency_key": "legacy-sync-disabled-batch",
                "requested_record_ids": [crm_record["crm_record_id"]],
                "status": "queued",
            },
        )
        run = seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "legacy-sync-disabled-run",
                "batch_id": batch["batch_id"],
                "record_id": crm_record["crm_record_id"],
                "candidate_id": "crm-public-web-legacy-sync-disabled",
                "candidate_name": "CRM Legacy Sync Disabled",
                "current_company": "Example AI",
                "status": "completed",
                "phase": "completed",
            },
        )

        sync = sync_public_web_batch_to_crm_owner(self.store, batch, runs=[run])
        summary = sync_public_web_batch_summary(self.store, batch["batch_id"])

        self.assertEqual(sync["status"], "skipped")
        self.assertEqual(sync["reason"], "legacy_target_public_web_to_crm_owner_sync_disabled")
        self.assertEqual(sync["execution_backend"], "crm_public_web_v1")
        self.assertEqual(summary["status"], "updated")
        self.assertNotIn("crm_owner_sync", summary["summary"])
        self.assertIsNone(self.store.get_crm_public_web_run(run_id=run["run_id"]))
        self.assertEqual(self.store.list_crm_public_web_batches(workspace_id="default"), [])

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
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
        first_metrics = after_first["summary"]["phase_metrics"]
        self.assertEqual(first_metrics["stage"], "waiting_remote_search")
        self.assertEqual(first_metrics["submitted_task_count"], 1)
        self.assertEqual(first_metrics["pending_task_count"], 1)
        self.assertEqual(first_metrics["fetched_task_count"], 0)
        self.assertEqual(first_metrics["ready_poll_count"], 1)
        self.assertIn("search_submit_duration_ms", first_metrics)
        self.assertIn("search_poll_duration_ms", first_metrics)
        self.assertEqual(provider.submit_calls, 1)
        self.assertEqual(provider.poll_calls, 1)

        second = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )

        self.assertEqual(second["worker_status"], "running")
        self.assertEqual(second["run_status"], "entry_links_ready")
        ready = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(ready["status"], "entry_links_ready")
        self.assertEqual(ready["summary"]["phase_metrics"]["pending_task_count"], 0)
        self.assertEqual(ready["summary"]["phase_metrics"]["fetched_task_count"], 1)
        self.assertEqual(provider.submit_calls, 1)
        self.assertEqual(provider.poll_calls, 2)
        self.assertEqual(provider.fetch_calls, 1)

        third = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )

        self.assertEqual(third["worker_status"], "running")
        self.assertEqual(third["run_status"], "documents_fetched")
        fetched_documents = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(fetched_documents["status"], "documents_fetched")
        self.assertIn("document_fetch_payload_path", fetched_documents["analysis_checkpoint"])

        fourth = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )

        self.assertEqual(fourth["worker_status"], "running")
        self.assertEqual(fourth["run_status"], "adjudication_completed")
        adjudicated = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(adjudicated["status"], "adjudication_completed")
        self.assertIn("adjudication_payload_path", adjudicated["analysis_checkpoint"])
        self.assertTrue(Path(adjudicated["analysis_checkpoint"]["adjudication_payload_path"]).exists())
        self.assertFalse((Path(adjudicated["artifact_root"]) / "signals.json").exists())
        self.assertIn("adjudication_duration_ms", adjudicated["summary"]["phase_metrics"])

        fifth = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )

        self.assertEqual(fifth["worker_status"], "running")
        self.assertEqual(fifth["run_status"], "analysis_completed")
        analyzed = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(analyzed["status"], "analysis_completed")
        self.assertEqual(analyzed["summary"]["phase_metrics"]["signal_materialization_required_count"], 1)
        self.assertEqual(analyzed["summary"]["phase_metrics"]["signal_materialized_count"], 0)

        sixth = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )

        self.assertEqual(sixth["worker_status"], "completed")
        self.assertEqual(sixth["summary"]["phase_metrics"]["signal_materialized_count"], 1)
        completed = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(completed["status"], "completed")
        self.assertGreater(completed["summary"]["entry_link_count"], 0)
        completed_metrics = completed["summary"]["phase_metrics"]
        self.assertEqual(completed_metrics["stage"], "analysis")
        self.assertEqual(completed_metrics["status"], "search_completed")
        self.assertEqual(completed_metrics["pending_task_count"], 0)
        self.assertEqual(completed_metrics["fetched_task_count"], 1)
        self.assertEqual(completed_metrics["ready_poll_count"], 2)
        self.assertEqual(completed_metrics["signal_materialized_count"], 1)
        self.assertGreaterEqual(completed_metrics["analysis_duration_ms"], 0)
        self.assertEqual(completed["analysis_checkpoint"]["signal_materialized_count"], 1)
        self.assertEqual(completed["analysis_checkpoint"]["phase_metrics"]["signal_materialized_count"], 1)
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

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
    def test_run_to_local_idle_does_not_wait_for_daemon_ticks_after_remote_search_ready(self) -> None:
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

        first = execute_target_candidate_public_web_run_to_local_idle(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )
        self.assertEqual(first["run_status"], "searching")
        self.assertEqual(first["local_step_count"], 1)

        second = execute_target_candidate_public_web_run_to_local_idle(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )

        self.assertEqual(second["worker_status"], "completed")
        self.assertEqual(second["run_status"], "completed")
        self.assertGreaterEqual(second["local_step_count"], 5)
        completed = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["summary"]["phase_metrics"]["pending_task_count"], 0)
        self.assertEqual(completed["summary"]["phase_metrics"]["signal_materialized_count"], 1)

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
    def test_scripted_fixture_materializes_publishable_profile_links(self) -> None:
        scenario_path = (
            Path(__file__).resolve().parents[1] / "configs" / "scripted" / "target_candidate_public_web_search.json"
        )
        with patch.dict(
            "os.environ",
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
            },
        ):
            search_provider = build_search_provider(
                SearchProviderSettings(provider_order=("dataforseo_google_organic",))
            )
        trigger = start_target_candidate_public_web_batch(
            store=self.store,
            target_candidates=[self.store.get_target_candidate("target-ada")],
            runtime_dir=self.tempdir.name,
            payload={
                "record_ids": ["target-ada"],
                "options": {
                    "max_queries_per_candidate": 2,
                    "fetch_content": False,
                    "ai_extraction": "on",
                },
            },
        )
        run_id = trigger["runs"][0]["run_id"]
        model_client = DeterministicModelClient()
        observed_statuses: list[str] = []

        for _ in range(8):
            result = execute_target_candidate_public_web_run_once(
                store=self.store,
                search_provider=search_provider,
                model_client=model_client,
                runtime_dir=self.tempdir.name,
                run_id=run_id,
            )
            observed_statuses.append(str(result.get("run_status") or ""))
            if result.get("worker_status") == "completed":
                break

        self.assertEqual(
            observed_statuses,
            [
                "searching",
                "entry_links_ready",
                "documents_fetched",
                "adjudication_completed",
                "analysis_completed",
                "completed",
            ],
        )
        completed = self.store.get_target_candidate_public_web_run(run_id=run_id)
        summary = dict(completed["summary"])
        self.assertGreaterEqual(summary["entry_link_count"], 1)
        self.assertTrue(summary["primary_links"])
        self.assertIn("personal_homepage", summary["primary_links"])
        signals = self.store.list_person_public_web_signals(run_id=run_id)
        self.assertGreaterEqual(len(signals), 1)
        self.assertTrue(any(signal["publishable"] for signal in signals))
        self.assertIn("ada-lovelace.example.dev", json.dumps(summary, ensure_ascii=False))

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
    def test_cancelled_run_is_terminal_and_does_not_resume_provider_work(self) -> None:
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
        self.assertEqual(first["run_status"], "searching")

        cancelled = cancel_target_candidate_public_web_run(
            store=self.store,
            run_id=run_id,
            reason="operator_changed_scope",
            operator="unit-test",
        )
        resumed = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )

        self.assertEqual(cancelled["status"], "cancelled")
        self.assertEqual(resumed["worker_status"], "completed")
        self.assertEqual(resumed["run_status"], "cancelled")
        self.assertEqual(provider.submit_calls, 1)
        self.assertEqual(provider.poll_calls, 1)
        self.assertEqual(provider.fetch_calls, 0)
        run = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(run["status"], "cancelled")
        self.assertEqual(run["search_checkpoint"]["status"], "cancelled")
        batch = self.store.get_target_candidate_public_web_batch(batch_id=trigger["batch"]["batch_id"])
        self.assertEqual(batch["summary"]["cancelled_count"], 1)
        self.assertEqual(batch["summary"]["running_count"], 0)

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
    def test_documents_fetched_requires_durable_document_payload(self) -> None:
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
        run = self.store.get_target_candidate_public_web_run(run_id=run_id)
        missing_path = Path(self.tempdir.name) / "missing-document-fetch-payload.json"
        update_legacy_target_public_web_run(
            self.store,
            run_id,
            {
                "status": "documents_fetched",
                "phase": "documents_fetched",
                "artifact_root": str(Path(run["artifact_root"])),
                "search_checkpoint": {
                    "stage": "search_completed",
                    "status": "search_completed",
                    "query_results": [],
                    "raw_links": [],
                    "errors": [],
                },
                "analysis_checkpoint": {
                    "stage": "documents_fetched",
                    "status": "documents_fetched",
                    "document_fetch_payload_path": str(missing_path),
                },
            },
        )

        result = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=_DeferredBatchSearchProvider(),
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )

        self.assertEqual(result["worker_status"], "completed")
        self.assertEqual(result["run_status"], "failed")
        failed = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(failed["status"], "failed")
        self.assertIn("document_fetch_payload_missing", failed["last_error"])

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
    def test_adjudication_completed_requires_durable_adjudication_payload(self) -> None:
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
        run = self.store.get_target_candidate_public_web_run(run_id=run_id)
        missing_path = Path(self.tempdir.name) / "missing-adjudication-payload.json"
        update_legacy_target_public_web_run(
            self.store,
            run_id,
            {
                "status": "adjudication_completed",
                "phase": "adjudication_completed",
                "artifact_root": str(Path(run["artifact_root"])),
                "search_checkpoint": {
                    "stage": "search_completed",
                    "status": "search_completed",
                    "query_results": [],
                    "raw_links": [],
                    "errors": [],
                },
                "analysis_checkpoint": {
                    "stage": "adjudication_completed",
                    "status": "adjudication_completed",
                    "adjudication_payload_path": str(missing_path),
                },
            },
        )

        result = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=_DeferredBatchSearchProvider(),
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=run_id,
        )

        self.assertEqual(result["worker_status"], "completed")
        self.assertEqual(result["run_status"], "failed")
        failed = self.store.get_target_candidate_public_web_run(run_id=run_id)
        self.assertEqual(failed["status"], "failed")
        self.assertIn("adjudication_payload_missing", failed["last_error"])

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
    def test_batch_summary_aggregates_phase_metrics_across_runs(self) -> None:
        batch = seed_legacy_target_public_web_batch(
            self.store,
            {
                "batch_id": "batch-phase-metrics",
                "idempotency_key": "unit:batch-phase-metrics",
                "status": "searching",
                "requested_record_ids": ["target-ada", "target-grace"],
                "run_ids": ["run-ada-phase", "run-grace-phase"],
            },
        )
        seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "run-ada-phase",
                "batch_id": batch["batch_id"],
                "record_id": "target-ada",
                "candidate_id": "cand-ada",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
                "status": "searching",
                "phase": "searching",
                "summary": {
                    "phase_metrics": {
                        "stage": "waiting_remote_search",
                        "status": "searching",
                        "submitted_task_count": 2,
                        "pending_task_count": 2,
                        "fetched_task_count": 0,
                        "failed_task_count": 1,
                        "fetch_error_count_last_poll": 1,
                        "ready_poll_count": 1,
                        "search_poll_duration_ms": 25.5,
                    }
                },
                "analysis_checkpoint": {
                    "phase_metrics": {
                        "submitted_task_count": 2,
                        "pending_task_count": 2,
                        "ready_poll_count": 1,
                    }
                },
            },
        )
        seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "run-grace-phase",
                "batch_id": batch["batch_id"],
                "record_id": "target-grace",
                "candidate_id": "cand-grace",
                "candidate_name": "Grace Hopper",
                "current_company": "Example AI",
                "status": "completed",
                "phase": "completed",
                "summary": {
                    "phase_metrics": {
                        "stage": "analysis",
                        "status": "search_completed",
                        "submitted_task_count": 1,
                        "pending_task_count": 0,
                        "fetched_task_count": 1,
                        "ready_poll_count": 2,
                        "analysis_duration_ms": 77.0,
                        "adjudication_duration_ms": 35.0,
                        "signal_materialization_duration_ms": 12.0,
                        "signal_materialized_count": 3,
                        "email_signal_count": 2,
                        "profile_link_signal_count": 1,
                    }
                },
                "analysis_checkpoint": {
                    "phase_metrics": {
                        "submitted_task_count": 1,
                        "fetched_task_count": 1,
                        "signal_materialized_count": 3,
                    }
                },
            },
        )

        synced = sync_public_web_batch_summary(self.store, batch["batch_id"])
        batch_summary = synced["summary"]
        metrics = batch_summary["phase_metrics"]

        self.assertEqual(batch_summary["run_count"], 2)
        self.assertEqual(metrics["metric_run_count"], 2)
        self.assertEqual(metrics["submitted_task_count"], 3)
        self.assertEqual(metrics["pending_task_count"], 2)
        self.assertEqual(metrics["fetched_task_count"], 1)
        self.assertEqual(metrics["failed_task_count"], 1)
        self.assertEqual(metrics["fetch_error_count_last_poll"], 1)
        self.assertEqual(metrics["signal_materialized_count"], 3)
        self.assertEqual(metrics["email_signal_count"], 2)
        self.assertEqual(metrics["profile_link_signal_count"], 1)
        self.assertEqual(metrics["phase_counts"], {"completed": 1, "searching": 1})
        self.assertEqual(metrics["status_counts"], {"completed": 1, "searching": 1})
        self.assertEqual(metrics["slowest_phase"], "analysis")
        self.assertEqual(metrics["duration_by_phase_ms"]["search_poll"]["count"], 1)
        self.assertEqual(metrics["duration_by_phase_ms"]["search_poll"]["max"], 25.5)
        self.assertEqual(metrics["duration_by_phase_ms"]["analysis"]["avg"], 77.0)
        self.assertEqual(metrics["provider_or_fetch_failure_count"], 2)
        self.assertEqual(metrics["local_processing_error_count"], 0)
        self.assertTrue(metrics["has_pending_remote_search"])
        self.assertFalse(metrics["has_unmaterialized_signals"])
        self.assertEqual(metrics["remote_search_pending_run_count"], 1)
        self.assertEqual(metrics["unmaterialized_signal_gap_count"], 0)
        self.assertEqual(metrics["terminal_with_errors_count"], 0)
        self.assertEqual(metrics["partial_failure_count"], 0)
        self.assertEqual(metrics["runs_with_errors_count"], 1)
        self.assertEqual(metrics["missing_phase_metric_count"], 0)
        self.assertEqual(metrics["phase_lag_risk_reasons"], ["remote_search_pending", "provider_or_fetch_errors"])
        self.assertFalse(metrics["service_guardrail_violation_detected"])

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
    def test_batch_summary_flags_terminal_materialization_and_metric_gaps(self) -> None:
        batch = seed_legacy_target_public_web_batch(
            self.store,
            {
                "batch_id": "batch-phase-guardrail",
                "idempotency_key": "unit:batch-phase-guardrail",
                "status": "completed",
                "requested_record_ids": ["target-ada", "target-grace"],
                "run_ids": ["run-ada-gap", "run-grace-missing-metrics"],
            },
        )
        seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "run-ada-gap",
                "batch_id": batch["batch_id"],
                "record_id": "target-ada",
                "candidate_id": "cand-ada",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
                "status": "completed",
                "phase": "completed",
                "summary": {
                    "phase_metrics": {
                        "signal_materialization_required_count": 2,
                        "signal_materialized_count": 0,
                    }
                },
            },
        )
        seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "run-grace-missing-metrics",
                "batch_id": batch["batch_id"],
                "record_id": "target-grace",
                "candidate_id": "cand-grace",
                "candidate_name": "Grace Hopper",
                "current_company": "Example AI",
                "status": "completed",
                "phase": "completed",
                "summary": {},
                "analysis_checkpoint": {},
            },
        )

        synced = sync_public_web_batch_summary(self.store, batch["batch_id"])
        metrics = synced["summary"]["phase_metrics"]

        self.assertEqual(metrics["metric_run_count"], 1)
        self.assertEqual(metrics["missing_phase_metric_count"], 1)
        self.assertEqual(metrics["unmaterialized_signal_gap_count"], 1)
        self.assertEqual(metrics["completed_without_materialized_signals_count"], 1)
        self.assertTrue(metrics["service_guardrail_violation_detected"])
        self.assertIn("missing_phase_metrics", metrics["phase_lag_risk_reasons"])
        self.assertIn("terminal_signal_materialization_gap", metrics["phase_lag_risk_reasons"])

    @unittest.skip(
        "legacy target-candidate Public Web execution is permanently retired; CRM Public Web owns e2e coverage"
    )
    def test_multi_candidate_batch_aggregates_staggered_progress_and_partial_failure(self) -> None:
        trigger = start_target_candidate_public_web_batch(
            store=self.store,
            target_candidates=[
                self.store.get_target_candidate("target-ada"),
                self.store.get_target_candidate("target-grace"),
            ],
            runtime_dir=self.tempdir.name,
            payload={
                "record_ids": ["target-ada", "target-grace"],
                "options": {
                    "max_queries_per_candidate": 1,
                    "fetch_content": False,
                    "ai_extraction": "off",
                },
            },
        )
        batch_id = trigger["batch"]["batch_id"]
        runs_by_record = {run["record_id"]: run["run_id"] for run in trigger["runs"]}
        provider = _StaggeredBatchSearchProvider(
            ready_after_by_record={"target-ada": 2, "target-grace": 3},
            fail_fetch_record_ids={"target-grace"},
        )

        ada_first = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-ada"],
        )
        grace_first = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-grace"],
        )
        self.assertEqual(ada_first["run_status"], "searching")
        self.assertEqual(grace_first["run_status"], "searching")

        ada_second = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-ada"],
        )
        grace_second = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-grace"],
        )
        self.assertEqual(ada_second["run_status"], "entry_links_ready")
        self.assertEqual(grace_second["run_status"], "searching")

        ada_third = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-ada"],
        )
        self.assertEqual(ada_third["run_status"], "documents_fetched")
        ada_fourth = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-ada"],
        )
        self.assertEqual(ada_fourth["run_status"], "adjudication_completed")
        ada_fifth = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-ada"],
        )
        self.assertEqual(ada_fifth["run_status"], "analysis_completed")
        ada_sixth = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-ada"],
        )
        self.assertEqual(ada_sixth["run_status"], "completed")

        grace_third = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-grace"],
        )
        self.assertEqual(grace_third["run_status"], "entry_links_ready")
        grace_fourth = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-grace"],
        )
        self.assertEqual(grace_fourth["run_status"], "documents_fetched")
        grace_fifth = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-grace"],
        )
        self.assertEqual(grace_fifth["run_status"], "adjudication_completed")
        grace_sixth = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-grace"],
        )
        self.assertEqual(grace_sixth["run_status"], "analysis_completed")
        grace_seventh = execute_target_candidate_public_web_run_once(
            store=self.store,
            search_provider=provider,
            model_client=None,
            runtime_dir=self.tempdir.name,
            run_id=runs_by_record["target-grace"],
        )
        self.assertEqual(grace_seventh["run_status"], "completed_with_errors")

        batch_summary = self.store.get_target_candidate_public_web_batch(batch_id=batch_id)["summary"]
        self.assertEqual(batch_summary["run_count"], 2)
        self.assertEqual(batch_summary["completed_count"], 1)
        self.assertEqual(batch_summary["completed_with_errors_count"], 1)
        self.assertEqual(batch_summary["running_count"], 0)
        self.assertEqual(batch_summary["failed_count"], 0)
        self.assertEqual(batch_summary["phase_metrics"]["metric_run_count"], 2)
        self.assertEqual(batch_summary["phase_metrics"]["signal_materialized_count"], 1)
        self.assertEqual(batch_summary["phase_metrics"]["remote_search_pending_run_count"], 0)
        self.assertEqual(batch_summary["phase_metrics"]["terminal_with_errors_count"], 1)
        self.assertEqual(batch_summary["phase_metrics"]["partial_failure_count"], 1)
        self.assertEqual(batch_summary["phase_metrics"]["runs_with_errors_count"], 1)
        self.assertEqual(batch_summary["phase_metrics"]["missing_phase_metric_count"], 0)
        self.assertIn("terminal_run_errors", batch_summary["phase_metrics"]["phase_lag_risk_reasons"])
        self.assertFalse(batch_summary["phase_metrics"]["service_guardrail_violation_detected"])
        self.assertGreaterEqual(batch_summary["phase_metrics"]["analysis_duration_ms_max"], 0.0)
        self.assertGreaterEqual(batch_summary["phase_metrics"]["signal_materialization_duration_ms_max"], 0.0)
        self.assertEqual(provider.submit_calls, 2)
        self.assertGreaterEqual(provider.poll_calls, 5)
        self.assertGreaterEqual(provider.fetch_calls, 2)

        ada_run = self.store.get_target_candidate_public_web_run(run_id=runs_by_record["target-ada"])
        grace_run = self.store.get_target_candidate_public_web_run(run_id=runs_by_record["target-grace"])
        self.assertEqual(ada_run["status"], "completed")
        self.assertEqual(grace_run["status"], "completed_with_errors")
        self.assertTrue((Path(ada_run["analysis_checkpoint"]["adjudication_payload_path"]).exists()))
        self.assertTrue((Path(ada_run["analysis_checkpoint"]["document_fetch_payload_path"]).exists()))
        self.assertTrue((Path(grace_run["analysis_checkpoint"]["adjudication_payload_path"]).exists()))
        self.assertGreaterEqual(len(self.store.list_person_public_web_signals(run_id=runs_by_record["target-ada"])), 1)
        self.assertGreaterEqual(
            len(self.store.list_person_public_web_signals(run_id=runs_by_record["target-grace"])), 0
        )

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
                    "adjudication": {
                        "identity_match_label": "ambiguous_identity",
                        "identity_match_score": 0.41,
                        "user_visible_signal": True,
                        "review_queue_reason": "Clean GitHub profile with candidate name evidence; useful for manual review.",
                        "rationale": "Clean GitHub profile with candidate-name evidence, but ownership is not confirmed.",
                        "signal_type": "github_url",
                    },
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
        self.assertEqual(len(rows), 2)
        email = next(row for row in rows if row["signal_kind"] == "email_candidate")
        link = next(
            row for row in rows if row["signal_kind"] == "profile_link" and row["source_domain"] == "github.com"
        )
        self.assertEqual(email["promotion_status"], "promotion_recommended")
        self.assertEqual(email["identity_match_label"], "likely_same_person")
        self.assertEqual(email["artifact_refs"]["analysis_path"], "/tmp/analysis.json")
        self.assertNotIn("raw_path", email["artifact_refs"])
        self.assertFalse(link["publishable"])
        self.assertEqual(link["identity_match_label"], "ambiguous_identity")
        self.assertFalse(any(row["source_domain"] == "x.com" for row in rows))

    def test_unassessed_profile_links_stay_in_artifacts_not_user_visible_signals(self) -> None:
        run = {
            "run_id": "public-web-noise-run-1",
            "record_id": "target-jackie",
            "candidate_id": "cand-jackie",
            "candidate_name": "Jackie Bow",
            "current_company": "Anthropic",
            "linkedin_url_key": "jackie-bow",
            "person_identity_key": "linkedin:jackie-bow",
        }
        signals = {
            "entry_links": [
                {
                    "url": "https://x.com/staceywueste",
                    "normalized_url": "https://x.com/staceywueste",
                    "title": "Stacey Wueste (@staceywueste) / Posts / X",
                    "snippet": "Jackie Bow (Anthropic) conference mention.",
                    "source_domain": "x.com",
                    "entry_type": "x_url",
                    "source_family": "social_presence",
                    "score": 104.0,
                    "identity_match_label": "needs_review",
                    "identity_match_score": 0.0,
                    "confidence_label": "high",
                    "adjudication": {
                        "reason": "model_no_assessment",
                        "clean_profile_link": True,
                        "link_shape_warnings": [],
                    },
                },
                {
                    "url": "https://www.researchgate.net/publication/123",
                    "normalized_url": "https://www.researchgate.net/publication/123",
                    "title": "Unrelated publication mentioning Jackie Bow",
                    "snippet": "Third-party publication result.",
                    "source_domain": "researchgate.net",
                    "entry_type": "publication_url",
                    "source_family": "candidate_publication_presence",
                    "score": 96.0,
                    "identity_match_label": "needs_review",
                    "identity_match_score": 0.0,
                    "confidence_label": "high",
                    "adjudication": {
                        "reason": "model_no_assessment",
                        "clean_profile_link": False,
                        "link_shape_warnings": ["publication_evidence_only_not_profile"],
                    },
                },
                {
                    "url": "https://x.com/jbowocky",
                    "normalized_url": "https://x.com/jbowocky",
                    "title": "Jackie Bow (@jbowocky) / Posts / X",
                    "snippet": "Jackie Bow profile.",
                    "source_domain": "x.com",
                    "entry_type": "x_url",
                    "source_family": "social_presence",
                    "score": 97.0,
                    "identity_match_label": "needs_review",
                    "identity_match_score": 0.45,
                    "confidence_label": "low",
                    "adjudication": {
                        "identity_match_label": "needs_review",
                        "identity_match_score": 0.45,
                        "user_visible_signal": True,
                        "review_queue_reason": "Plausible owned X profile; weak but useful enough for human review.",
                        "rationale": "Same name and plausible handle, but no corroborating employer evidence.",
                        "signal_type": "x_url",
                    },
                },
            ],
            "email_candidates": [
                {
                    "value": "wrong-person@example.edu",
                    "normalized_value": "wrong-person@example.edu",
                    "email_type": "academic",
                    "promotion_status": "suppressed",
                    "publishable": False,
                    "identity_match_label": "not_same_person",
                    "identity_match_score": 0.0,
                    "suppression_reason": "belongs to a different person",
                    "source_url": "https://example.edu/wrong-person",
                }
            ],
            "fetched_documents": [],
        }

        rows = build_person_public_web_signal_rows(run=run, signals=signals)

        self.assertEqual([row["normalized_value"] for row in rows], ["https://x.com/jbowocky"])
        self.assertEqual(rows[0]["identity_match_label"], "needs_review")

    def test_ai_rejected_review_links_stay_in_artifacts_not_user_visible_signals(self) -> None:
        run = {
            "run_id": "public-web-margaret-noise-run-1",
            "record_id": "target-margaret",
            "candidate_id": "cand-margaret",
            "candidate_name": "Margaret V.",
            "current_company": "Anthropic",
            "linkedin_url_key": "margaret-v",
            "person_identity_key": "linkedin:margaret-v",
        }
        signals = {
            "entry_links": [
                {
                    "url": "https://scholar.google.com/citations?user=QrdlROsAAAAJ&hl=en",
                    "normalized_url": "https://scholar.google.com/citations?user=QrdlROsAAAAJ&hl=en",
                    "title": "Margaret V. Becker",
                    "snippet": "University of Texas Medical Branch. Verified email at utmb.edu.",
                    "source_domain": "scholar.google.com",
                    "entry_type": "scholar_url",
                    "source_family": "scholar_profile_discovery",
                    "score": 99.0,
                    "identity_match_label": "needs_review",
                    "identity_match_score": 0.7,
                    "confidence_label": "medium",
                    "adjudication": {
                        "identity_match_label": "needs_review",
                        "identity_match_score": 0.7,
                        "user_visible_signal": False,
                        "review_queue_reason": (
                            "Same first name and initial, but Scholar affiliation conflicts with the known profile "
                            "and there is no employer or education corroboration."
                        ),
                        "rationale": "Potential same-name collision; keep as raw evidence only.",
                        "signal_type": "scholar_url",
                    },
                },
                {
                    "url": "https://scholar.google.com/citations?user=radAAAAAJ&hl=en",
                    "normalized_url": "https://scholar.google.com/citations?user=radAAAAAJ&hl=en",
                    "title": "RADHIKA S",
                    "snippet": "Google Scholar profile for a different person.",
                    "source_domain": "scholar.google.com",
                    "entry_type": "scholar_url",
                    "source_family": "scholar_profile_discovery",
                    "score": 97.0,
                    "identity_match_label": "needs_review",
                    "identity_match_score": 0.0,
                    "confidence_label": "low",
                    "adjudication": {
                        "identity_match_label": "not_same_person",
                        "identity_match_score": 0.0,
                        "user_visible_signal": False,
                        "review_queue_reason": "Different person name.",
                        "rationale": "Different person name.",
                        "signal_type": "scholar_url",
                    },
                },
                {
                    "url": "https://github.com/zooniverse/Serengeti/blob/master/app/views/authors_page.eco",
                    "normalized_url": "https://github.com/zooniverse/Serengeti/blob/master/app/views/authors_page.eco",
                    "title": "Serengeti/app/views/authors_page.eco at master",
                    "snippet": "Large author list that mentions Margaret V.",
                    "source_domain": "github.com",
                    "entry_type": "github_url",
                    "source_family": "technical_presence",
                    "score": 95.0,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.8,
                    "confidence_label": "medium",
                    "adjudication": {
                        "identity_match_label": "likely_same_person",
                        "identity_match_score": 0.8,
                        "user_visible_signal": False,
                        "review_queue_reason": "GitHub repository blob is not a person profile.",
                        "rationale": "Repository blob evidence only.",
                        "signal_type": "github_url",
                    },
                },
            ],
            "email_candidates": [],
            "fetched_documents": [],
        }

        rows = build_person_public_web_signal_rows(run=run, signals=signals)

        self.assertEqual(rows, [])


@unittest.skip(
    "Track B B3.1: SQLite-authoritative ControlPlaneStore construction is no longer supported, so "
    "this class (which deliberately read retired target_candidate_public_web_* rows from a direct "
    "SQLite store) cannot construct its store. The legacy fail-closed + legacy-row read-back coverage "
    "needs re-homing onto the PG legacy_target_public_web_migration_table_context (B3.1 follow-up); "
    "skipped rather than deleted to preserve the retirement guards until then."
)
class TargetCandidatePublicWebSqliteLegacyTableTest(unittest.TestCase):
    """Direct-SQLite coverage for retired ``target_candidate_public_web_*`` tables.

    SKIPPED under the PG-only contract (Track B B3.1): these seed retired legacy rows and read
    them back through regular ControlPlaneStore methods on a direct SQLite-backed store, which is
    no longer constructable. Re-home onto the PG legacy migration context as a follow-up.
    """

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
        self.store.upsert_target_candidate(
            {
                "record_id": "target-grace",
                "candidate_id": "cand-grace",
                "candidate_name": "Grace Hopper",
                "current_company": "Example AI",
                "headline": "Systems Engineer",
                "linkedin_url": "https://www.linkedin.com/in/grace-hopper/",
            }
        )

    def tearDown(self) -> None:
        self.store.close()
        self.tempdir.cleanup()

    def test_legacy_target_execution_helpers_fail_closed_even_with_removed_override(self) -> None:
        with patch.dict(os.environ, {"SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS": "1"}, clear=False):
            start_result = start_target_candidate_public_web_batch(
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
        self.assertEqual(start_result["status"], "retired")
        self.assertEqual(start_result["reason"], "legacy_target_public_web_execution_disabled")
        self.assertFalse(start_result["normal_path"])
        self.assertEqual(start_result["migration_override_env"], "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS")
        self.assertEqual(start_result["migration_override_status"], "removed")
        self.assertEqual(self.store.list_target_candidate_public_web_batches(), [])

        batch = seed_legacy_target_public_web_batch(
            self.store,
            {
                "batch_id": "legacy-direct-helper-disabled-batch",
                "idempotency_key": "legacy-direct-helper-disabled-batch",
                "requested_record_ids": ["target-ada"],
                "status": "queued",
            },
        )
        run = seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "legacy-direct-helper-disabled-run",
                "batch_id": batch["batch_id"],
                "record_id": "target-ada",
                "candidate_id": "cand-ada",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
                "status": "queued",
                "phase": "queued",
                "options": {
                    "max_queries_per_candidate": 1,
                    "fetch_content": False,
                    "ai_extraction": "off",
                },
            },
        )

        with patch.dict(os.environ, {"SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS": "1"}, clear=False):
            cancel_result = cancel_target_candidate_public_web_run(
                store=self.store,
                run_id=run["run_id"],
                reason="unit_test",
            )
            execute_result = execute_target_candidate_public_web_run_once(
                store=self.store,
                search_provider=_DeferredBatchSearchProvider(),
                model_client=None,
                runtime_dir=self.tempdir.name,
                run_id=run["run_id"],
            )
            sync_result = sync_public_web_batch_summary(self.store, batch["batch_id"])

        self.assertEqual(cancel_result["status"], "retired")
        self.assertEqual(execute_result["run_status"], "retired")
        self.assertEqual(execute_result["reason"], "legacy_target_public_web_execution_disabled")
        self.assertEqual(sync_result["status"], "retired")
        unchanged_run = self.store.get_target_candidate_public_web_run(run_id=run["run_id"])
        self.assertEqual(unchanged_run["status"], "queued")

    def test_latest_public_web_runs_by_record_ids_returns_one_latest_run_per_record(self) -> None:
        seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "run-ada-aaa-old",
                "idempotency_key": "unit:run-ada-aaa-old",
                "batch_id": "batch-old",
                "record_id": "target-ada",
                "candidate_id": "cand-ada",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
                "status": "failed",
                "phase": "failed",
                "updated_at": "2026-05-03 10:00:00",
            },
        )
        seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "run-grace-latest",
                "idempotency_key": "unit:run-grace-latest",
                "batch_id": "batch-grace",
                "record_id": "target-grace",
                "candidate_id": "cand-grace",
                "candidate_name": "Grace Hopper",
                "current_company": "Example AI",
                "status": "completed",
                "phase": "completed",
                "updated_at": "2026-05-03 10:01:00",
            },
        )
        seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "run-ada-zzz-latest",
                "idempotency_key": "unit:run-ada-zzz-latest",
                "batch_id": "batch-new",
                "record_id": "target-ada",
                "candidate_id": "cand-ada",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
                "status": "queued",
                "phase": "queued",
                "updated_at": "2026-05-03 10:02:00",
            },
        )

        runs = self.store.list_latest_target_candidate_public_web_runs_by_record_ids(
            ["target-ada", "target-grace"],
            limit=2,
        )

        self.assertEqual({run["record_id"] for run in runs}, {"target-ada", "target-grace"})
        self.assertEqual(
            {run["record_id"]: run["run_id"] for run in runs},
            {"target-ada": "run-ada-zzz-latest", "target-grace": "run-grace-latest"},
        )

    def test_promotion_rows_preserve_signal_lineage_without_raw_assets(self) -> None:
        promotion = seed_legacy_target_public_web_promotion(
            self.store,
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
            },
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
