import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import JobRequest
from sourcing_agent.durable_runtime import (
    COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES,
    CRM_PUBLIC_WEB_PHASE_OWNER,
    CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
    DEFAULT_COMMAND_OWNER_REGISTRY,
    LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
    PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
    PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
    SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
    CommandOwnerRegistry,
    DurableRuntimeWriter,
    collection_authoritative_merge_idempotency_key,
    command_causality_for,
    command_id_for,
    crm_public_web_queue_batch_idempotency_key,
    crm_public_web_run_phase_idempotency_key,
    legacy_job_workflow_run_id,
    linkedin_discovery_query_run_idempotency_key,
    linkedin_local_profile_delta_apply_idempotency_key,
    linkedin_profile_refill_submit_idempotency_key,
    linkedin_profile_url_terminal_record_idempotency_key,
    projection_board_visible_patch_publish_idempotency_key,
    projection_facet_layering_build_idempotency_key,
    projection_person_search_index_build_idempotency_key,
    projection_run_scope_finalize_idempotency_key,
    reduce_workflow_events,
    snapshot_compaction_run_idempotency_key,
)
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin

try:
    import psycopg
except Exception:  # pragma: no cover - PG fixture skips before this path matters.
    psycopg = None


class DurableRuntimeStorageTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def test_workflow_events_are_append_only_and_idempotent_per_run(self) -> None:
        first = self.store.repos.workflow_runtime.append_workflow_event(
            workflow_run_id="wf_openai_1",
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key="wf_openai_1:start",
            payload={"stage_key": "stage1_candidate_set"},
        )
        duplicate = self.store.repos.workflow_runtime.append_workflow_event(
            workflow_run_id="wf_openai_1",
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key="wf_openai_1:start",
            payload={"stage_key": "should_not_replace"},
        )
        second = self.store.repos.workflow_runtime.append_workflow_event(
            workflow_run_id="wf_openai_1",
            event_family="domain_event",
            event_type="CompletionProofRecorded",
            idempotency_key="wf_openai_1:stage1_terminal",
            payload={"proof_key": "stage1_candidate_set_terminal", "status": "proved"},
        )

        events = self.store.repos.workflow_runtime.list_workflow_events("wf_openai_1")

        self.assertEqual(first["event_id"], duplicate["event_id"])
        self.assertEqual(first["payload"]["stage_key"], "stage1_candidate_set")
        self.assertEqual([event["sequence_number"] for event in events], [1, 2])
        self.assertEqual(second["sequence_number"], 2)

    def test_workflow_event_repository_rejects_immutable_identity_collision(self) -> None:
        repository = self.store.repos.workflow_runtime
        event = repository.append_workflow_event(
            workflow_run_id="wf_event_identity_collision",
            operation_id="op_event_identity_collision",
            command_id="cmd_event_identity_collision",
            activity_attempt_id="attempt_event_identity_collision",
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key="wf_event_identity_collision:start",
            payload={"stage_key": "profile_fetch"},
        )
        replay = repository.append_workflow_event(
            workflow_run_id="wf_event_identity_collision",
            operation_id="op_event_identity_collision",
            command_id="cmd_event_identity_collision",
            activity_attempt_id="attempt_event_identity_collision",
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key="wf_event_identity_collision:start",
            payload={"stage_key": "must_not_replace"},
        )

        self.assertEqual(replay["event_id"], event["event_id"])
        self.assertEqual(replay["payload"], {"stage_key": "profile_fetch"})
        with self.assertRaisesRegex(RuntimeError, "workflow_events.*append_workflow_event"):
            repository.append_workflow_event(
                workflow_run_id="wf_event_identity_collision",
                operation_id="op_event_identity_collision",
                command_id="cmd_event_identity_collision",
                activity_attempt_id="attempt_event_identity_collision",
                event_family="workflow_event",
                event_type="WorkflowCompleted",
                idempotency_key="wf_event_identity_collision:start",
            )

    def test_workflow_event_repository_rejects_explicit_late_sequence(self) -> None:
        repository = self.store.repos.workflow_runtime
        for index in range(2):
            repository.append_workflow_event(
                workflow_run_id="wf_event_late_sequence",
                event_family="workflow_event",
                event_type="ProgressRecorded",
                idempotency_key=f"wf_event_late_sequence:progress:{index}",
                payload={"index": index},
            )

        with self.assertRaisesRegex(RuntimeError, "workflow_events.*append_workflow_event"):
            repository.append_workflow_event(
                workflow_run_id="wf_event_late_sequence",
                event_family="workflow_event",
                event_type="LateProgressRecorded",
                idempotency_key="wf_event_late_sequence:late",
                sequence_number=1,
            )

        events = repository.list_workflow_events("wf_event_late_sequence")
        self.assertEqual([event["sequence_number"] for event in events], [1, 2])
        self.assertEqual(
            [event["idempotency_key"] for event in events],
            ["wf_event_late_sequence:progress:0", "wf_event_late_sequence:progress:1"],
        )

    def test_workflow_event_repository_allocates_contiguous_sequences_concurrently(self) -> None:
        import threading

        repository = self.store.repos.workflow_runtime
        thread_count = 8
        barrier = threading.Barrier(thread_count)
        results: list[dict[str, object]] = []
        errors: list[Exception] = []

        def append(index: int) -> None:
            try:
                barrier.wait(timeout=10)
                results.append(
                    repository.append_workflow_event(
                        workflow_run_id="wf_event_concurrent_sequence",
                        event_family="workflow_event",
                        event_type="ProgressRecorded",
                        idempotency_key=f"wf_event_concurrent_sequence:progress:{index}",
                        payload={"index": index},
                    )
                )
            except Exception as exc:  # pragma: no cover - asserted below.
                errors.append(exc)

        threads = [threading.Thread(target=append, args=(index,)) for index in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertEqual(sorted(int(result["sequence_number"]) for result in results), list(range(1, 9)))
        events = repository.list_workflow_events("wf_event_concurrent_sequence")
        self.assertEqual([event["sequence_number"] for event in events], list(range(1, 9)))
        self.assertEqual(len({event["event_id"] for event in events}), thread_count)

    def test_sqlite_durable_runtime_normal_path_fails_closed(self) -> None:
        self._stop_pg_durable_runtime()
        with mock.patch.dict(
            os.environ,
            {
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "",
                "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "",
                "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "disabled",
                "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "",
                "SOURCING_LOCAL_POSTGRES_ENV_FILE": str(self.runtime_dir / "missing-postgres.env"),
            },
            clear=False,
        ):
            # Track B B3: SQLite-authoritative control-plane storage is no longer supported —
            # the disabled-mode store is rejected at construction (stronger than the former
            # per-durable-op fail-closed); durable runtime state can only live in Postgres.
            with self.assertRaisesRegex(RuntimeError, "no longer supported"):
                ControlPlaneStore(self.runtime_dir / "blocked.db")
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "sourcing_agent.db")

    def test_workflow_current_state_is_materialized_not_event_truth(self) -> None:
        state = self.store.repos.workflow_runtime.upsert_workflow_current_state(
            workflow_run_id="wf_google_1",
            operation_id="op_google_1",
            workflow_type="linkedin_acquisition",
            status="running",
            current_stage_key="profile_fetch",
            completion_proofs={"stage1_candidate_set_terminal": {"status": "proved"}},
            last_processed_sequence_number=7,
            reducer_version="test_reducer_v1",
            metadata={"job_id": "legacy_job_google_1"},
        )
        stale_update = self.store.repos.workflow_runtime.upsert_workflow_current_state(
            workflow_run_id="wf_google_1",
            status="running",
            last_processed_sequence_number=3,
        )

        self.assertEqual(state["workflow_run_id"], "wf_google_1")
        self.assertEqual(stale_update["last_processed_sequence_number"], 7)
        self.assertEqual(stale_update["metadata"]["job_id"], "legacy_job_google_1")

    def test_workflow_current_state_repository_lower_sequence_cannot_regress_snapshot(self) -> None:
        repository = self.store.repos.workflow_runtime
        committed = repository.upsert_workflow_current_state(
            workflow_run_id="wf_state_sequence_fence",
            operation_id="op_state_sequence_fence",
            workflow_type="linkedin_acquisition",
            status="completed",
            current_stage_key="serving_finalized",
            completion_proofs={"workflow_terminal": {"status": "proved"}},
            last_processed_sequence_number=9,
            reducer_version="reducer_v9",
            metadata={"snapshot_id": "snapshot-new", "writer": "new"},
        )
        stale = repository.upsert_workflow_current_state(
            workflow_run_id="wf_state_sequence_fence",
            status="running",
            current_stage_key="profile_fetch",
            completion_proofs={"profile_fetch": {"status": "pending"}},
            last_processed_sequence_number=3,
            reducer_version="reducer_v3",
            metadata={"snapshot_id": "snapshot-old", "writer": "stale"},
        )

        self.assertEqual(stale["last_processed_sequence_number"], 9)
        self.assertEqual(stale["status"], "completed")
        self.assertEqual(stale["current_stage_key"], "serving_finalized")
        self.assertEqual(stale["completion_proofs"], committed["completion_proofs"])
        self.assertEqual(stale["metadata"], committed["metadata"])
        self.assertEqual(stale["reducer_version"], "reducer_v9")
        self.assertEqual(repository.get_workflow_current_state("wf_state_sequence_fence"), stale)

    def test_workflow_current_state_repository_allows_equal_sequence_command_count_refresh(self) -> None:
        repository = self.store.repos.workflow_runtime
        repository.upsert_workflow_current_state(
            workflow_run_id="wf_state_equal_sequence_refresh",
            operation_id="op_state_equal_sequence_refresh",
            workflow_type="linkedin_acquisition",
            status="running",
            current_stage_key="profile_fetch",
            completion_proofs={"candidate_set": {"status": "proved"}},
            active_command_counts={"profile_owner": {"profile.fetch": 2}},
            terminal_command_counts={},
            last_processed_sequence_number=7,
            reducer_version="reducer_v7",
            metadata={"snapshot_id": "snapshot-equal-sequence"},
        )

        refreshed = repository.upsert_workflow_current_state(
            workflow_run_id="wf_state_equal_sequence_refresh",
            active_command_counts={"profile_owner": {"profile.fetch": 1}},
            terminal_command_counts={"profile_owner": {"profile.fetch": 1}},
            last_processed_sequence_number=7,
        )

        self.assertEqual(refreshed["last_processed_sequence_number"], 7)
        self.assertEqual(refreshed["active_command_counts"], {"profile_owner": {"profile.fetch": 1}})
        self.assertEqual(refreshed["terminal_command_counts"], {"profile_owner": {"profile.fetch": 1}})
        self.assertEqual(refreshed["status"], "running")
        self.assertEqual(refreshed["current_stage_key"], "profile_fetch")
        self.assertEqual(refreshed["completion_proofs"], {"candidate_set": {"status": "proved"}})
        self.assertEqual(refreshed["metadata"], {"snapshot_id": "snapshot-equal-sequence"})

    def test_workflow_current_state_repository_rejects_equal_sequence_reducer_collision(self) -> None:
        repository = self.store.repos.workflow_runtime
        committed = repository.upsert_workflow_current_state(
            workflow_run_id="wf_state_equal_sequence_collision",
            status="running",
            current_stage_key="profile_fetch",
            completion_proofs={"candidate_set": {"status": "proved"}},
            last_processed_sequence_number=5,
            reducer_version="reducer_v5",
            metadata={"snapshot_id": "snapshot-v5"},
        )

        with self.assertRaisesRegex(RuntimeError, "same-sequence reducer collision"):
            repository.upsert_workflow_current_state(
                workflow_run_id="wf_state_equal_sequence_collision",
                status="completed",
                current_stage_key="serving_finalized",
                last_processed_sequence_number=5,
                reducer_version="reducer_v5-conflict",
                metadata={"snapshot_id": "snapshot-conflict"},
            )

        self.assertEqual(
            repository.get_workflow_current_state("wf_state_equal_sequence_collision"),
            committed,
        )

    def test_workflow_current_state_repository_omitted_status_preserves_existing_status(self) -> None:
        repository = self.store.repos.workflow_runtime
        repository.upsert_workflow_current_state(
            workflow_run_id="wf_state_omitted_status",
            status="running",
            current_stage_key="profile_fetch",
            last_processed_sequence_number=1,
        )

        refreshed = repository.upsert_workflow_current_state(
            workflow_run_id="wf_state_omitted_status",
            active_command_counts={"profile_owner": {"profile.fetch": 1}},
            last_processed_sequence_number=2,
        )

        self.assertEqual(refreshed["status"], "running")
        self.assertEqual(refreshed["current_stage_key"], "profile_fetch")
        self.assertEqual(refreshed["last_processed_sequence_number"], 2)
        self.assertEqual(refreshed["active_command_counts"], {"profile_owner": {"profile.fetch": 1}})

    def test_workflow_current_state_repository_existing_patch_requires_checkpoint(self) -> None:
        repository = self.store.repos.workflow_runtime
        committed = repository.upsert_workflow_current_state(
            workflow_run_id="wf_state_checkpoint_required",
            status="running",
            active_command_counts={"profile_owner": {"profile.fetch": 1}},
            last_processed_sequence_number=3,
        )

        with self.assertRaisesRegex(RuntimeError, "updates require last_processed_sequence_number"):
            repository.upsert_workflow_current_state(
                workflow_run_id="wf_state_checkpoint_required",
                active_command_counts={"profile_owner": {"profile.fetch": 0}},
            )

        self.assertEqual(
            repository.get_workflow_current_state("wf_state_checkpoint_required"),
            committed,
        )

    def test_workflow_current_state_repository_new_row_defaults_checkpoint_to_zero(self) -> None:
        created = self.store.repos.workflow_runtime.upsert_workflow_current_state(
            workflow_run_id="wf_state_checkpoint_default_insert",
            status="running",
        )

        self.assertEqual(created["status"], "running")
        self.assertEqual(created["last_processed_sequence_number"], 0)

    def test_workflow_commands_are_idempotent_claimable_and_terminal(self) -> None:
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf_lovable_1",
            operation_id="op_lovable_1",
            command_type="linkedin.profile_refill.submit_batch",
            owner="linkedin_profile_owner",
            idempotency_key="wf_lovable_1:profile_batch:1",
            payload={"profile_url_count": 50},
            max_attempts=2,
        )
        duplicate = self.store.upsert_workflow_command(
            workflow_run_id="wf_lovable_1",
            command_type="linkedin.profile_refill.submit_batch",
            owner="wrong_owner_should_not_replace",
            idempotency_key="wf_lovable_1:profile_batch:1",
        )
        claimed = self.store.claim_workflow_command(command["command_id"], lease_owner="worker-a")
        running = self.store.mark_workflow_command_running(command["command_id"], lease_owner="worker-a")
        succeeded = self.store.mark_workflow_command_succeeded(
            command["command_id"],
            result={"provider_run_id": "run_1"},
        )
        failed_after_terminal = self.store.mark_workflow_command_failed(
            command["command_id"],
            error_text="late failure should not change terminal command",
        )

        self.assertEqual(command["command_id"], command_id_for("wf_lovable_1", "wf_lovable_1:profile_batch:1"))
        self.assertEqual(duplicate["owner"], "linkedin_profile_owner")
        self.assertEqual(claimed["status"], "claimed")
        self.assertEqual(claimed["attempt"], 1)
        self.assertEqual(running["status"], "running")
        self.assertEqual(succeeded["status"], "succeeded")
        self.assertEqual(succeeded["result"]["provider_run_id"], "run_1")
        self.assertEqual(failed_after_terminal, {})

    def test_workflow_commands_reclaim_expired_running_lease(self) -> None:
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf_public_web_lease_reclaim",
            operation_id="op_public_web_lease_reclaim",
            command_type="crm.public_web.documents.fetch",
            owner="crm_public_web_owner",
            idempotency_key="wf_public_web_lease_reclaim:documents_fetch",
            max_attempts=3,
        )
        self.store.claim_workflow_command(command["command_id"], lease_owner="worker-a", lease_seconds=300)
        running = self.store.mark_workflow_command_running(command["command_id"], lease_owner="worker-a")
        self.assertEqual(running["status"], "running")

        if psycopg is None:
            self.skipTest("psycopg is required for PG durable runtime lease mutation")
        fixture = self._pg_durable_runtime_fixture
        assert fixture is not None
        with psycopg.connect(fixture.dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'SET search_path TO "{fixture.schema}"')
                cursor.execute(
                    """
                    UPDATE workflow_commands
                    SET lease_expires_at = '2099-01-01 00:00:00'
                    WHERE command_id = %s
                    """,
                    (command["command_id"],),
                )

        not_ready = self.store.list_ready_workflow_commands(
            owner="crm_public_web_owner",
            command_type="crm.public_web.documents.fetch",
            limit=10,
        )
        self.assertEqual([item["command_id"] for item in not_ready], [])

        with psycopg.connect(fixture.dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'SET search_path TO "{fixture.schema}"')
                cursor.execute(
                    """
                    UPDATE workflow_commands
                    SET lease_expires_at = '2000-01-01 00:00:00'
                    WHERE command_id = %s
                    """,
                    (command["command_id"],),
                )

        ready = self.store.list_ready_workflow_commands(
            owner="crm_public_web_owner",
            command_type="crm.public_web.documents.fetch",
            limit=10,
        )
        self.assertEqual([item["command_id"] for item in ready], [command["command_id"]])
        reclaimed = self.store.claim_workflow_command(command["command_id"], lease_owner="worker-b", lease_seconds=300)
        self.assertEqual(reclaimed["status"], "claimed")
        self.assertEqual(reclaimed["lease_owner"], "worker-b")
        self.assertEqual(reclaimed["attempt"], 2)

    def test_workflow_command_control_is_safe_and_status_bounded(self) -> None:
        cancelable = self.store.upsert_workflow_command(
            workflow_run_id="wf_command_control",
            operation_id="",
            command_type="crm.public_web.search.submit",
            owner="crm_public_web_owner",
            idempotency_key="wf_command_control:cancelable",
        )
        cancelled = self.store.cancel_workflow_command(
            cancelable["command_id"],
            reason="unit-test-cancel",
            actor="unit-test",
            result={"test_marker": "cancel"},
        )
        retried_from_cancel = self.store.retry_workflow_command(
            cancelable["command_id"],
            reason="unit-test-retry",
            actor="unit-test",
        )

        running_command = self.store.upsert_workflow_command(
            workflow_run_id="wf_command_control",
            command_type="crm.public_web.documents.fetch",
            owner="crm_public_web_owner",
            idempotency_key="wf_command_control:running",
        )
        self.store.claim_workflow_command(running_command["command_id"], lease_owner="worker-a")
        self.store.mark_workflow_command_running(running_command["command_id"], lease_owner="worker-a")
        cancelled_running = self.store.cancel_workflow_command(
            running_command["command_id"],
            reason="should-not-cancel-running",
            actor="unit-test",
        )

        retry_wait_command = self.store.upsert_workflow_command(
            workflow_run_id="wf_command_control",
            command_type="crm.public_web.evidence.adjudicate",
            owner="crm_public_web_owner",
            idempotency_key="wf_command_control:retry_wait",
            max_attempts=2,
        )
        self.store.claim_workflow_command(retry_wait_command["command_id"], lease_owner="worker-b")
        retry_waiting = self.store.mark_workflow_command_failed(
            retry_wait_command["command_id"],
            error_text="temporary provider error",
            retryable=True,
            retry_delay_seconds=60,
        )
        resumed = self.store.resume_workflow_command(
            retry_wait_command["command_id"],
            reason="unit-test-resume",
            actor="unit-test",
        )

        failed_command = self.store.upsert_workflow_command(
            workflow_run_id="wf_command_control",
            command_type="crm.public_web.model_safe.finalize",
            owner="crm_public_web_owner",
            idempotency_key="wf_command_control:failed_terminal",
            max_attempts=1,
        )
        self.store.claim_workflow_command(failed_command["command_id"], lease_owner="worker-c")
        failed_terminal = self.store.mark_workflow_command_failed(
            failed_command["command_id"],
            error_text="terminal failure",
            retryable=True,
        )
        retried_from_terminal = self.store.retry_workflow_command(
            failed_command["command_id"],
            reason="unit-test-retry-terminal",
            actor="unit-test",
        )

        self.assertEqual(cancelled["status"], "cancelled")
        self.assertEqual(cancelled["result"]["control_action"], "cancel")
        self.assertEqual(cancelled["result"]["test_marker"], "cancel")
        self.assertEqual(retried_from_cancel["status"], "queued")
        self.assertEqual(retried_from_cancel["attempt"], 0)
        self.assertEqual(retried_from_cancel["result"]["control_action"], "retry")
        self.assertEqual(cancelled_running, {})
        self.assertEqual(self.store.get_workflow_command(running_command["command_id"])["status"], "running")
        self.assertEqual(retry_waiting["status"], "retry_wait")
        self.assertNotEqual(retry_waiting["not_before_at"], "")
        self.assertEqual(resumed["status"], "queued")
        self.assertEqual(resumed["attempt"], 0)
        self.assertEqual(resumed["result"]["control_action"], "resume")
        self.assertEqual(failed_terminal["status"], "failed_terminal")
        self.assertEqual(retried_from_terminal["status"], "queued")

    def test_profile_refill_submit_idempotency_key_includes_submit_scope(self) -> None:
        normal = linkedin_profile_refill_submit_idempotency_key(
            job_id="job_scope",
            snapshot_dir="/tmp/snapshot",
            profile_urls=["https://www.linkedin.com/in/a/", "https://www.linkedin.com/in/b/"],
            submit_scope="filled_available_slots",
        )
        retry = linkedin_profile_refill_submit_idempotency_key(
            job_id="job_scope",
            snapshot_dir="/tmp/snapshot",
            profile_urls=["https://www.linkedin.com/in/b/", "https://www.linkedin.com/in/a/"],
            submit_scope="retry_wait",
        )

        self.assertNotEqual(normal, retry)
        self.assertTrue(normal.startswith("linkedin.profile_refill.submit_batch:"))
        self.assertTrue(retry.startswith("linkedin.profile_refill.submit_batch:"))

    def test_profile_url_terminal_record_idempotency_key_normalizes_entry_order(self) -> None:
        first = linkedin_profile_url_terminal_record_idempotency_key(
            job_id="job_terminal",
            snapshot_dir="/tmp/snapshot",
            terminal_scope="worker:1",
            entries=[
                {
                    "profile_url": "https://www.linkedin.com/in/b/",
                    "status": "failed_retryable",
                    "error": "unresolved",
                    "retryable": True,
                },
                {
                    "profile_url": "https://www.linkedin.com/in/a/",
                    "status": "fetched",
                    "raw_path": "/tmp/a.json",
                },
            ],
        )
        second = linkedin_profile_url_terminal_record_idempotency_key(
            job_id="job_terminal",
            snapshot_dir="/tmp/snapshot",
            terminal_scope="worker:1",
            entries=[
                {
                    "profile_url": "https://www.linkedin.com/in/a/",
                    "status": "fetched",
                    "raw_path": "/tmp/a.json",
                },
                {
                    "profile_url": "https://www.linkedin.com/in/b/",
                    "status": "failed_retryable",
                    "error": "unresolved",
                    "retryable": True,
                },
            ],
        )
        different_scope = linkedin_profile_url_terminal_record_idempotency_key(
            job_id="job_terminal",
            snapshot_dir="/tmp/snapshot",
            terminal_scope="worker:2",
            entries=[
                {
                    "profile_url": "https://www.linkedin.com/in/a/",
                    "status": "fetched",
                    "raw_path": "/tmp/a.json",
                }
            ],
        )

        self.assertEqual(first, second)
        self.assertNotEqual(first, different_scope)
        self.assertTrue(first.startswith("linkedin.profile_url_terminal.record:"))

    def test_local_profile_delta_apply_idempotency_key_includes_item_scope(self) -> None:
        first = linkedin_local_profile_delta_apply_idempotency_key(
            job_id="job_delta",
            snapshot_id="snapshot_delta",
            item_id="item_a",
            worker_kind="harvest_prefetch",
            source_worker_ids=[2, 1, 2],
            apply_scope="local_apply_closure_item",
        )
        reordered = linkedin_local_profile_delta_apply_idempotency_key(
            job_id="job_delta",
            snapshot_id="snapshot_delta",
            item_id="item_a",
            worker_kind="harvest_prefetch",
            source_worker_ids=[1, 2],
            apply_scope="local_apply_closure_item",
        )
        different_item = linkedin_local_profile_delta_apply_idempotency_key(
            job_id="job_delta",
            snapshot_id="snapshot_delta",
            item_id="item_b",
            worker_kind="harvest_prefetch",
            source_worker_ids=[1, 2],
            apply_scope="local_apply_closure_item",
        )

        self.assertEqual(first, reordered)
        self.assertNotEqual(first, different_item)
        self.assertTrue(first.startswith("linkedin.local_profile_delta.apply:"))

    def test_board_visible_patch_publish_idempotency_key_includes_item_scope(self) -> None:
        first = projection_board_visible_patch_publish_idempotency_key(
            job_id="job_board",
            snapshot_id="snapshot_board",
            item_id="item_a",
            candidate_ids=["candidate-b", "candidate-a"],
            publish_scope="board_visible_delta_apply_item",
        )
        reordered = projection_board_visible_patch_publish_idempotency_key(
            job_id="job_board",
            snapshot_id="snapshot_board",
            item_id="item_a",
            candidate_ids=["candidate-a", "candidate-b"],
            publish_scope="board_visible_delta_apply_item",
        )
        different_item = projection_board_visible_patch_publish_idempotency_key(
            job_id="job_board",
            snapshot_id="snapshot_board",
            item_id="item_b",
            candidate_ids=["candidate-a", "candidate-b"],
            publish_scope="board_visible_delta_apply_item",
        )

        self.assertEqual(first, reordered)
        self.assertNotEqual(first, different_item)
        self.assertTrue(first.startswith("projection.board_visible_patch.publish:"))

    def test_projection_person_search_index_build_idempotency_key_includes_item_scope(self) -> None:
        first = projection_person_search_index_build_idempotency_key(
            projection_id="proj_index",
            item_id="item_a",
            projection_index_input_version="input_v1",
            build_scope="projection_person_search_index_build_item",
        )
        repeated = projection_person_search_index_build_idempotency_key(
            projection_id="proj_index",
            item_id="item_a",
            projection_index_input_version="input_v1",
            build_scope="projection_person_search_index_build_item",
        )
        different_item = projection_person_search_index_build_idempotency_key(
            projection_id="proj_index",
            item_id="item_b",
            projection_index_input_version="input_v1",
            build_scope="projection_person_search_index_build_item",
        )
        different_input = projection_person_search_index_build_idempotency_key(
            projection_id="proj_index",
            item_id="item_a",
            projection_index_input_version="input_v2",
            build_scope="projection_person_search_index_build_item",
        )

        self.assertEqual(first, repeated)
        self.assertNotEqual(first, different_item)
        self.assertNotEqual(first, different_input)
        self.assertTrue(first.startswith("projection.person_search_index.build:"))

    def test_collection_authoritative_merge_idempotency_key_includes_item_scope(self) -> None:
        first = collection_authoritative_merge_idempotency_key(
            collection_id="company:openai",
            source_projection_id="proj_run",
            item_id="item_a",
            publication_fingerprint="fp1",
            merge_scope="collection_authoritative_merge_item",
        )
        repeated = collection_authoritative_merge_idempotency_key(
            collection_id="company:openai",
            source_projection_id="proj_run",
            item_id="item_a",
            publication_fingerprint="fp1",
            merge_scope="collection_authoritative_merge_item",
        )
        different_projection = collection_authoritative_merge_idempotency_key(
            collection_id="company:openai",
            source_projection_id="proj_run_2",
            item_id="item_a",
            publication_fingerprint="fp1",
            merge_scope="collection_authoritative_merge_item",
        )

        self.assertEqual(first, repeated)
        self.assertNotEqual(first, different_projection)
        self.assertTrue(first.startswith(f"{COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE}:"))

    def test_run_scope_projection_finalize_idempotency_key_includes_result_view_scope(self) -> None:
        first = projection_run_scope_finalize_idempotency_key(
            job_id="job_finalize",
            view_id="view_a",
            snapshot_id="snapshot_a",
            source_path="/tmp/snapshot-a/normalized_artifacts/materialized_candidate_documents.json",
            finalize_scope="job_result_view_asset_population",
        )
        repeated = projection_run_scope_finalize_idempotency_key(
            job_id="job_finalize",
            view_id="view_a",
            snapshot_id="snapshot_a",
            source_path="/tmp/snapshot-a/normalized_artifacts/materialized_candidate_documents.json",
            finalize_scope="job_result_view_asset_population",
        )
        different_view = projection_run_scope_finalize_idempotency_key(
            job_id="job_finalize",
            view_id="view_b",
            snapshot_id="snapshot_a",
            source_path="/tmp/snapshot-a/normalized_artifacts/materialized_candidate_documents.json",
            finalize_scope="job_result_view_asset_population",
        )
        different_source_path = projection_run_scope_finalize_idempotency_key(
            job_id="job_finalize",
            view_id="view_a",
            snapshot_id="snapshot_a",
            source_path="/tmp/snapshot-b/normalized_artifacts/materialized_candidate_documents.json",
            finalize_scope="job_result_view_asset_population",
        )

        self.assertEqual(first, repeated)
        self.assertNotEqual(first, different_view)
        self.assertNotEqual(first, different_source_path)
        self.assertTrue(first.startswith(f"{PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE}:"))

    def test_search_seed_projection_facet_and_snapshot_command_keys_include_item_scope(self) -> None:
        discovery = linkedin_discovery_query_run_idempotency_key(
            job_id="job_cmd_scope",
            snapshot_id="snapshot-a",
            item_id="search-item-a",
            query="OpenAI Agent site:linkedin.com/in",
            employment_status="current",
            run_scope="search_seed_discovery_query_item",
        )
        discovery_other_query = linkedin_discovery_query_run_idempotency_key(
            job_id="job_cmd_scope",
            snapshot_id="snapshot-a",
            item_id="search-item-a",
            query="OpenAI Health site:linkedin.com/in",
            employment_status="current",
            run_scope="search_seed_discovery_query_item",
        )
        facet = projection_facet_layering_build_idempotency_key(
            job_id="job_cmd_scope",
            snapshot_id="snapshot-a",
            item_id="facet-item-a",
            input_fingerprint="fingerprint-a",
            build_scope="projection_facet_layering_build_item",
        )
        facet_other_input = projection_facet_layering_build_idempotency_key(
            job_id="job_cmd_scope",
            snapshot_id="snapshot-a",
            item_id="facet-item-a",
            input_fingerprint="fingerprint-b",
            build_scope="projection_facet_layering_build_item",
        )
        snapshot = snapshot_compaction_run_idempotency_key(
            job_id="job_cmd_scope",
            snapshot_id="snapshot-a",
            item_id="snapshot-item-a",
            compaction_scope="snapshot_full_materialization_item",
        )
        snapshot_other_item = snapshot_compaction_run_idempotency_key(
            job_id="job_cmd_scope",
            snapshot_id="snapshot-a",
            item_id="snapshot-item-b",
            compaction_scope="snapshot_full_materialization_item",
        )

        self.assertNotEqual(discovery, discovery_other_query)
        self.assertTrue(discovery.startswith(f"{LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE}:"))
        self.assertNotEqual(facet, facet_other_input)
        self.assertTrue(facet.startswith(f"{PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE}:"))
        self.assertNotEqual(snapshot, snapshot_other_item)
        self.assertTrue(snapshot.startswith(f"{SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE}:"))

    def test_runtime_outbox_is_idempotent_and_dispatchable(self) -> None:
        queued = self.store.repos.workflow_runtime.enqueue_runtime_outbox(
            workflow_run_id="wf_openai_2",
            outbox_type="stream.workflow_event",
            idempotency_key="wf_openai_2:stream:started",
            payload={"event_type": "WorkflowStarted"},
        )
        duplicate = self.store.repos.workflow_runtime.enqueue_runtime_outbox(
            workflow_run_id="wf_openai_2",
            outbox_type="stream.workflow_event",
            idempotency_key="wf_openai_2:stream:started",
            payload={"event_type": "Different"},
        )
        dispatched = self.store.repos.workflow_runtime.mark_runtime_outbox_dispatched(queued["outbox_id"])

        self.assertEqual(queued["outbox_id"], duplicate["outbox_id"])
        self.assertEqual(duplicate["payload"]["event_type"], "WorkflowStarted")
        self.assertEqual(dispatched["status"], "dispatched")
        self.assertTrue(dispatched["dispatched_at"])

    def test_runtime_outbox_repository_rejects_cross_run_and_type_idempotency_collisions(self) -> None:
        repository = self.store.repos.workflow_runtime
        repository.enqueue_runtime_outbox(
            workflow_run_id="wf_outbox_identity_a",
            outbox_type="stream.workflow_event",
            idempotency_key="outbox:cross-run",
            payload={"event_type": "WorkflowStarted"},
        )
        with self.assertRaisesRegex(RuntimeError, "runtime_outbox.*enqueue_runtime_outbox"):
            repository.enqueue_runtime_outbox(
                workflow_run_id="wf_outbox_identity_b",
                outbox_type="stream.workflow_event",
                idempotency_key="outbox:cross-run",
            )

        repository.enqueue_runtime_outbox(
            workflow_run_id="wf_outbox_identity_type",
            outbox_type="stream.workflow_event",
            idempotency_key="outbox:cross-type",
        )
        with self.assertRaisesRegex(RuntimeError, "runtime_outbox.*enqueue_runtime_outbox"):
            repository.enqueue_runtime_outbox(
                workflow_run_id="wf_outbox_identity_type",
                outbox_type="workflow.completed",
                idempotency_key="outbox:cross-type",
            )

    def test_runtime_outbox_repository_repeated_dispatch_preserves_first_timestamp(self) -> None:
        repository = self.store.repos.workflow_runtime
        queued = repository.enqueue_runtime_outbox(
            workflow_run_id="wf_outbox_dispatch_replay",
            outbox_type="stream.workflow_event",
            idempotency_key="wf_outbox_dispatch_replay:stream:started",
        )

        first = repository.mark_runtime_outbox_dispatched(queued["outbox_id"])
        repeated = repository.mark_runtime_outbox_dispatched(queued["outbox_id"])

        self.assertEqual(first["status"], "dispatched")
        self.assertTrue(first["dispatched_at"])
        self.assertEqual(repeated["outbox_id"], queued["outbox_id"])
        self.assertEqual(repeated["status"], "dispatched")
        self.assertEqual(repeated["dispatched_at"], first["dispatched_at"])

    def test_runtime_outbox_repository_claimed_dispatch_requires_lease_owner(self) -> None:
        repository = self.store.repos.workflow_runtime
        queued = repository.enqueue_runtime_outbox(
            workflow_run_id="wf_outbox_dispatch_lease",
            outbox_type="stream.workflow_event",
            idempotency_key="wf_outbox_dispatch_lease:stream:started",
        )
        adapter = self.store._control_plane_postgres
        with adapter._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE runtime_outbox SET status = 'claimed', lease_owner = %s WHERE outbox_id = %s",
                    ("outbox-worker-a", queued["outbox_id"]),
                )
            connection.commit()

        with self.assertRaisesRegex(RuntimeError, "dispatch lease-owner mismatch"):
            repository.mark_runtime_outbox_dispatched(queued["outbox_id"])
        with self.assertRaisesRegex(RuntimeError, "dispatch lease-owner mismatch"):
            repository.mark_runtime_outbox_dispatched(
                queued["outbox_id"],
                lease_owner="outbox-worker-b",
            )

        dispatched = repository.mark_runtime_outbox_dispatched(
            queued["outbox_id"],
            lease_owner="outbox-worker-a",
        )
        self.assertEqual(dispatched["status"], "dispatched")
        self.assertEqual(dispatched["lease_owner"], "")

    def test_runtime_writer_applies_reducer_output_idempotently(self) -> None:
        registry = CommandOwnerRegistry(
            {"linkedin.profile_refill.submit_batch": "linkedin_profile_owner"}
        )
        writer = DurableRuntimeWriter(self.store, owner_registry=registry)

        started = writer.append_event_and_reduce(
            workflow_run_id="wf_writer_1",
            operation_id="op_writer_1",
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key="wf_writer_1:start",
            payload={"stage_key": "profile_fetch", "workflow_type": "linkedin_acquisition"},
        )
        planned = writer.append_event_and_reduce(
            workflow_run_id="wf_writer_1",
            operation_id="op_writer_1",
            event_family="workflow_event",
            event_type="CommandPlanRequested",
            idempotency_key="wf_writer_1:plan_profile_batch",
            payload={
                "command_type": "linkedin.profile_refill.submit_batch",
                "idempotency_key": "wf_writer_1:profile_batch:1",
                "stage_id": "profile_fetch",
                "causal_group_id": "profile_fetch:batch:1",
                "produced_entity_counts": {"profile_url": 50},
                "payload": {"profile_url_count": 50},
            },
        )
        duplicate_reduce = writer.reduce_and_persist(workflow_run_id="wf_writer_1")
        completed = writer.append_event_and_reduce(
            workflow_run_id="wf_writer_1",
            operation_id="op_writer_1",
            event_family="workflow_event",
            event_type="WorkflowCompleted",
            idempotency_key="wf_writer_1:completed",
            payload={"stage_key": "serving_finalized"},
        )

        commands = self.store.list_workflow_commands(workflow_run_id="wf_writer_1", limit=0)

        self.assertEqual(started.state["status"], "running")
        self.assertEqual(planned.applied_event_count, 1)
        self.assertEqual(len(planned.commands), 1)
        self.assertEqual(duplicate_reduce.applied_event_count, 0)
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["owner"], "linkedin_profile_owner")
        self.assertEqual(commands[0]["stage_id"], "profile_fetch")
        self.assertEqual(commands[0]["causal_group_id"], "profile_fetch:batch:1")
        self.assertEqual(commands[0]["source_event_id"], planned.event["event_id"])
        self.assertEqual(commands[0]["source_event_type"], "CommandPlanRequested")
        self.assertEqual(commands[0]["produced_entity_counts"], {"profile_url": 50})
        self.assertEqual(commands[0]["payload"]["causality"]["causal_group_id"], "profile_fetch:batch:1")
        self.assertEqual(commands[0]["payload"]["causality"]["source_event_id"], planned.event["event_id"])
        self.assertEqual(commands[0]["payload"]["causality"]["produced_entity_counts"], {"profile_url": 50})
        self.assertEqual(completed.state["status"], "completed")
        self.assertEqual(completed.state["last_processed_sequence_number"], 3)
        self.assertEqual(len(completed.outbox), 1)
        self.assertEqual(completed.outbox[0]["outbox_type"], "workflow.completed")

    def test_runtime_writer_refreshes_command_projection_without_new_events(self) -> None:
        registry = CommandOwnerRegistry(
            {"linkedin.profile_refill.submit_batch": "linkedin_profile_owner"}
        )
        writer = DurableRuntimeWriter(self.store, owner_registry=registry)

        writer.append_event_and_reduce(
            workflow_run_id="wf_writer_command_projection",
            operation_id="op_writer_command_projection",
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key="wf_writer_command_projection:start",
            payload={"stage_key": "profile_fetch", "workflow_type": "linkedin_acquisition"},
        )
        planned = writer.append_event_and_reduce(
            workflow_run_id="wf_writer_command_projection",
            operation_id="op_writer_command_projection",
            event_family="workflow_event",
            event_type="CommandPlanRequested",
            idempotency_key="wf_writer_command_projection:plan_profile_batch",
            payload={
                "command_type": "linkedin.profile_refill.submit_batch",
                "idempotency_key": "wf_writer_command_projection:profile_batch:1",
                "payload": {"profile_url_count": 50},
            },
        )
        command = self.store.claim_workflow_command(
            planned.commands[0]["command_id"],
            lease_owner="profile-owner",
        )
        self.store.mark_workflow_command_running(command["command_id"], lease_owner="profile-owner")
        self.store.mark_workflow_command_succeeded(
            command["command_id"],
            result={"provider_run_id": "run_1"},
        )

        refreshed = writer.reduce_and_persist(workflow_run_id="wf_writer_command_projection")

        self.assertEqual(refreshed.applied_event_count, 0)
        self.assertEqual(refreshed.state["active_command_counts"], {})
        self.assertEqual(
            refreshed.state["terminal_command_counts"],
            {"linkedin_profile_owner": {"linkedin.profile_refill.submit_batch": 1}},
        )

    def test_legacy_job_materialization_writes_are_report_visible_and_strict_gateable(self) -> None:
        item = self.store.upsert_job_materialization_item(
            item_id="legacy_item_normal",
            job_id="job_legacy_normal",
            item_kind="board_visible_delta_apply",
            source="existing_runtime",
        )

        self.assertTrue(item["metadata"]["legacy_materialization_write_contract"]["normal_path"])
        self.assertEqual(
            item["metadata"]["legacy_materialization_write_contract"]["target_runtime_table"],
            "workflow_commands",
        )
        with mock.patch.dict(
            "os.environ",
            {"SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES": "1"},
            clear=False,
        ):
            with self.assertRaises(RuntimeError):
                self.store.upsert_job_materialization_item(
                    item_id="legacy_item_blocked",
                    job_id="job_legacy_blocked",
                    item_kind="board_visible_delta_apply",
                    source="existing_runtime",
                )
            migration_item = self.store.upsert_job_materialization_item(
                item_id="legacy_item_migration",
                job_id="job_legacy_migration",
                item_kind="board_visible_delta_apply",
                source="durable_runtime_migration_adapter",
                metadata={"migration_adapter": True},
            )

        self.assertTrue(
            migration_item["metadata"]["legacy_materialization_write_contract"]["migration_adapter"]
        )

    def test_waiting_prerequisite_command_can_be_marked_from_queued_without_legacy_item(self) -> None:
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf_waiting_prereq",
            operation_id="op_waiting_prereq",
            command_type=PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
            owner="board_visible_projection_owner",
            idempotency_key="wf_waiting_prereq:board_visible:1",
            payload={"job_id": "job_waiting_prereq", "snapshot_id": "snapshot-a"},
        )

        waiting = self.store.mark_workflow_command_waiting_prerequisite(
            command["command_id"],
            retry_delay_seconds=8,
            result={"status": "waiting_prerequisite", "reason": "candidate_delta_candidates_missing"},
            from_statuses=["queued", "claimed", "running", "retry_wait"],
        )

        self.assertEqual(waiting["status"], "retry_wait")
        self.assertEqual(waiting["attempt"], 0)
        self.assertEqual(waiting["result"]["status"], "waiting_prerequisite")
        self.assertEqual(waiting["last_error"], "")


class LegacyMaterializationAdapterTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "runtime.db")
        self.catalog = AssetCatalog.discover()
        self.settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "providers.local.json",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            db_path=self.runtime_dir / "runtime.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.model_client = DeterministicModelClient()
        self.acquisition_engine = AcquisitionEngine(
            self.catalog,
            self.settings,
            self.store,
            self.model_client,
        )
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=self.settings.jobs_dir,
            model_client=self.model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=self.acquisition_engine,
        )
        self.request = JobRequest.from_payload(
            {
                "raw_user_request": "Find OpenAI agent people",
                "target_company": "OpenAI",
                "keywords": ["agent"],
                "employment_statuses": ["current"],
            }
        )
        self.store.save_job(
            job_id="job_adapter",
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=self.request.to_record(),
            plan_payload={},
            summary_payload={},
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def test_adapter_converts_local_apply_and_board_visible_items_idempotently(self) -> None:
        local_item = self.store.upsert_job_materialization_item(
            item_id="legacy_local_apply_1",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="local_apply_closure",
            source="legacy_fixture",
            status="queued",
            source_worker_ids=[11],
            candidate_ids=["candidate-a"],
            metadata={
                "migration_adapter": True,
                "worker_kind": "harvest_prefetch",
                "request_payload": self.request.to_record(),
            },
        )
        board_item = self.store.upsert_job_materialization_item(
            item_id="legacy_board_visible_1",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="board_visible_delta_apply",
            source="legacy_fixture",
            status="queued",
            candidate_ids=["candidate-a", "candidate-b"],
            metadata={"migration_adapter": True, "request_payload": self.request.to_record()},
        )

        first = self.orchestrator.convert_legacy_job_materialization_items_to_workflow_commands(
            {"job_id": "job_adapter", "limit": 10}
        )
        second = self.orchestrator.convert_legacy_job_materialization_items_to_workflow_commands(
            {"job_id": "job_adapter", "limit": 10}
        )

        commands = self.store.list_workflow_commands(limit=0)
        command_types = {str(command.get("command_type") or "") for command in commands}

        self.assertEqual(local_item["item_id"], "legacy_local_apply_1")
        self.assertEqual(board_item["item_id"], "legacy_board_visible_1")
        self.assertEqual(first["converted_count"], 2)
        self.assertEqual(first["open_unconverted_count"], 0)
        self.assertEqual(second["converted_count"], 0)
        self.assertEqual(second["already_converted_count"], 2)
        self.assertEqual(len(commands), 2)
        self.assertIn(LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE, command_types)
        self.assertIn(PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE, command_types)
        for command in commands:
            legacy_ref = dict(dict(command.get("payload") or {}).get("legacy_materialization_item") or {})
            self.assertEqual(legacy_ref.get("migration_adapter"), True)
            self.assertEqual(legacy_ref.get("retirement_phase"), "Phase W6")

    def test_adapter_converts_projection_index_and_collection_merge_items(self) -> None:
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_adapter",
                "projection_type": "run_scope_projection",
                "collection_id": "company:openai",
                "source_run_id": "job_adapter",
                "state": "serving",
                "counts": {"visible_member_count": 1},
            }
        )
        self.store.upsert_job_materialization_item(
            item_id="legacy_projection_index_1",
            job_id="job_adapter",
            target_company="OpenAI",
            item_kind="projection_person_search_index_build",
            source="legacy_fixture",
            status="queued",
            serving_projection_id="proj_adapter",
            metadata={
                "migration_adapter": True,
                "projection_id": "proj_adapter",
                "projection_index_input_version": "input-v1",
                "member_count": 1,
            },
        )
        self.store.upsert_job_materialization_item(
            item_id="legacy_collection_merge_1",
            job_id="job_adapter",
            target_company="OpenAI",
            item_kind="collection_authoritative_merge",
            source="legacy_fixture",
            status="queued",
            metadata={
                "migration_adapter": True,
                "collection_id": "company:openai",
                "source_projection_id": "proj_adapter",
                "source_run_id": "job_adapter",
                "publication_fingerprint": "fingerprint-v1",
            },
        )

        result = self.orchestrator.convert_legacy_job_materialization_items_to_workflow_commands(
            {"job_id": "job_adapter", "limit": 10}
        )

        commands = self.store.list_workflow_commands(limit=0)
        command_types = {str(command.get("command_type") or "") for command in commands}

        self.assertEqual(result["converted_count"], 2)
        self.assertEqual(result["open_unconverted_count"], 0)
        self.assertIn(PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE, command_types)
        self.assertIn(COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE, command_types)

    def test_adapter_reports_unsupported_and_invalid_scope_without_silent_success(self) -> None:
        self.store.upsert_job_materialization_item(
            item_id="legacy_unsupported_1",
            job_id="job_adapter",
            target_company="OpenAI",
            item_kind="unsupported_legacy_kind",
            source="legacy_fixture",
            status="queued",
            metadata={"migration_adapter": True},
        )
        self.store.upsert_job_materialization_item(
            item_id="legacy_invalid_board_1",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="board_visible_delta_apply",
            source="legacy_fixture",
            status="queued",
            candidate_ids=[],
            metadata={"migration_adapter": True},
        )

        result = self.orchestrator.convert_legacy_job_materialization_items_to_workflow_commands(
            {
                "job_id": "job_adapter",
                "item_kinds": ["unsupported_legacy_kind", "board_visible_delta_apply"],
                "limit": 10,
            }
        )

        self.assertEqual(result["converted_count"], 0)
        self.assertEqual(result["unsupported_count"], 1)
        self.assertEqual(result["invalid_scope_count"], 1)
        self.assertEqual(result["open_unconverted_count"], 1)
        self.assertIn("unsupported_legacy_kind", result["by_kind"])
        self.assertEqual(
            result["by_kind"]["unsupported_legacy_kind"]["unsupported_count"],
            1,
        )

    def test_adapter_converts_search_seed_projection_facet_and_snapshot_items(self) -> None:
        self.store.upsert_job_materialization_item(
            item_id="legacy_search_seed_1",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="search_seed_discovery_query",
            source="legacy_fixture",
            status="queued",
            metadata={
                "migration_adapter": True,
                "query": "OpenAI Agent site:linkedin.com/in",
                "employment_status": "current",
                "provider_name": "harvest_profile_search",
            },
        )
        self.store.upsert_job_materialization_item(
            item_id="legacy_projection_facet_1",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="projection_facet_layering_build",
            source="legacy_fixture",
            status="queued",
            metadata={
                "migration_adapter": True,
                "input_fingerprint": "facet-input-v1",
                "request_payload": self.request.to_record(),
            },
        )
        self.store.upsert_job_materialization_item(
            item_id="legacy_snapshot_full_1",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="snapshot_full_materialization",
            source="legacy_fixture",
            status="queued",
            metadata={"migration_adapter": True, "request_payload": self.request.to_record()},
        )

        result = self.orchestrator.convert_legacy_job_materialization_items_to_workflow_commands(
            {
                "job_id": "job_adapter",
                "item_kinds": [
                    "search_seed_discovery_query",
                    "projection_facet_layering_build",
                    "snapshot_full_materialization",
                ],
                "limit": 10,
            }
        )

        commands = self.store.list_workflow_commands(limit=0)
        command_types = {str(command.get("command_type") or "") for command in commands}

        self.assertEqual(result["converted_count"], 3)
        self.assertEqual(result["open_unconverted_count"], 0)
        self.assertIn(LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE, command_types)
        self.assertIn(PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE, command_types)
        self.assertIn(SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE, command_types)
        for command in commands:
            payload = dict(command.get("payload") or {})
            self.assertEqual(payload["legacy_materialization_item"]["migration_adapter"], True)

    def test_recovery_tick_does_not_run_legacy_adapter_without_explicit_opt_in(self) -> None:
        self.store.upsert_job_materialization_item(
            item_id="legacy_local_apply_recovery_1",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="local_apply_closure",
            source="legacy_fixture",
            status="queued",
            source_worker_ids=[22],
            metadata={
                "migration_adapter": True,
                "worker_kind": "harvest_prefetch",
                "request_payload": self.request.to_record(),
            },
        )

        result = self.orchestrator.run_worker_recovery_once(
            {
                "job_id": "job_adapter",
                "search_seed_discovery_enabled": False,
                "profile_prefetch_refill_enabled": False,
                "stage1_preview_recovery_enabled": False,
                "post_completion_reconcile_enabled": False,
                "snapshot_full_materialization_enabled": False,
                "projection_facet_layering_enabled": False,
                "projection_person_search_index_enabled": False,
                "collection_authoritative_merge_enabled": False,
                "excel_intake_recovery_enabled": False,
                "post_recovery_housekeeping_enabled": False,
                "workflow_resume_requires_daemon_quiescent": False,
            }
        )

        adapter = dict(result.get("legacy_materialization_adapter") or {})
        phase = dict(dict(result.get("recovery_phase_metrics") or {}).get("legacy_materialization_adapter") or {})
        commands = self.store.list_workflow_commands(limit=0)

        self.assertEqual(adapter["status"], "skipped")
        self.assertEqual(adapter["reason"], "legacy_materialization_adapter_disabled_after_w6_signoff")
        self.assertEqual(dict(phase.get("counts") or {}).get("converted_count"), None)
        self.assertEqual(commands, [])

    def test_recovery_tick_exposes_adapter_counts_only_with_explicit_migration_opt_in(self) -> None:
        self.store.upsert_job_materialization_item(
            item_id="legacy_local_apply_recovery_1",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="local_apply_closure",
            source="legacy_fixture",
            status="queued",
            source_worker_ids=[22],
            metadata={
                "migration_adapter": True,
                "worker_kind": "harvest_prefetch",
                "request_payload": self.request.to_record(),
            },
        )

        result = self.orchestrator.run_worker_recovery_once(
            {
                "job_id": "job_adapter",
                "search_seed_discovery_enabled": False,
                "profile_prefetch_refill_enabled": False,
                "stage1_preview_recovery_enabled": False,
                "post_completion_reconcile_enabled": False,
                "snapshot_full_materialization_enabled": False,
                "projection_facet_layering_enabled": False,
                "projection_person_search_index_enabled": False,
                "collection_authoritative_merge_enabled": False,
                "excel_intake_recovery_enabled": False,
                "post_recovery_housekeeping_enabled": False,
                "workflow_resume_requires_daemon_quiescent": False,
                "legacy_materialization_adapter_enabled": True,
            }
        )

        adapter = dict(result.get("legacy_materialization_adapter") or {})
        phase = dict(dict(result.get("recovery_phase_metrics") or {}).get("legacy_materialization_adapter") or {})
        commands = self.store.list_workflow_commands(limit=0)

        self.assertEqual(adapter["converted_count"], 1)
        self.assertEqual(dict(phase.get("counts") or {}).get("converted_count"), 1)
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["status"], "queued")

    def test_disabled_command_owner_does_not_implicitly_execute_legacy_local_apply_bridge(self) -> None:
        item = self.store.upsert_job_materialization_item(
            item_id="legacy_local_apply_disabled_owner",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="local_apply_closure",
            source="legacy_fixture",
            status="queued",
            source_worker_ids=[22],
            metadata={"migration_adapter": True},
        )

        with mock.patch.dict(
            "os.environ",
            {"LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_OWNER_ENABLED": "0"},
            clear=False,
        ):
            result = self.orchestrator._drain_local_profile_delta_apply_commands(  # noqa: SLF001
                {"job_id": "job_adapter"}
            )

        refreshed = self.store.get_job_materialization_item(str(item["item_id"]))
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "legacy_job_materialization_recovery_bridge_disabled")
        self.assertTrue(result["legacy_bridge_blocked"])
        self.assertFalse(result["legacy_bridge_used"])
        self.assertEqual(refreshed["status"], "queued")
        self.assertEqual(refreshed["lease_owner"], "")

    def test_explicit_migration_allow_no_longer_executes_legacy_local_apply_bridge(self) -> None:
        self.store.upsert_job_materialization_item(
            item_id="legacy_local_apply_allowed_owner",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="local_apply_closure",
            source="legacy_fixture",
            status="queued",
            source_worker_ids=[22],
            metadata={"migration_adapter": True},
        )

        with mock.patch.dict(
            "os.environ",
            {"LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_OWNER_ENABLED": "0"},
            clear=False,
        ), mock.patch.object(
            self.orchestrator,
            "_run_local_apply_closure_item_queue_once",
            side_effect=AssertionError("legacy bridge execution must be physically retired"),
        ) as legacy_queue:
            result = self.orchestrator._drain_local_profile_delta_apply_commands(  # noqa: SLF001
                {
                    "job_id": "job_adapter",
                    "allow_legacy_job_materialization_recovery_bridge": True,
                }
            )

        legacy_queue.assert_not_called()
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "legacy_job_materialization_recovery_bridge_disabled")
        self.assertTrue(result["legacy_bridge_blocked"])
        self.assertFalse(result["legacy_bridge_used"])
        self.assertEqual(result["migration_phase"], "W2c_local_profile_delta_apply")

    def test_w6_normal_path_enqueues_local_apply_command_without_legacy_item_row(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES": "1"},
            clear=False,
        ):
            item = self.orchestrator._enqueue_local_apply_closure_item(  # noqa: SLF001
                job={"job_id": "job_adapter", "target_company": "OpenAI"},
                request=self.request,
                snapshot_id="snapshot-a",
                worker_kind="harvest_prefetch",
                worker_ids=[101],
                reason="unit_w6_normal_path",
                metadata={"candidate_ids": ["candidate-a"]},
            )

        commands = self.store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id("job_adapter"),
            owner="profile_local_apply_owner",
            limit=0,
        )
        payload = dict(commands[-1]["payload"] or {})

        self.assertEqual(item["status"], "queued")
        self.assertEqual(commands[-1]["command_type"], LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE)
        self.assertEqual(payload["item_id"], item["item_id"])
        self.assertNotIn("legacy_materialization_item", payload)
        self.assertEqual(self.store.list_job_materialization_items(job_id="job_adapter"), [])

    def test_w6_normal_path_enqueues_board_visible_command_without_legacy_item_row(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "OpenAI" / "snapshots" / "snapshot-a"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        with mock.patch.dict(
            "os.environ",
            {"SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES": "1"},
            clear=False,
        ):
            item = self.orchestrator._enqueue_board_visible_delta_apply_item(  # noqa: SLF001
                job={"job_id": "job_adapter", "target_company": "OpenAI"},
                request=self.request,
                snapshot_dir=snapshot_dir,
                delta_candidate_ids=["candidate-a"],
                reason="unit_w6_normal_path",
            )

        commands = self.store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id("job_adapter"),
            owner="board_visible_projection_owner",
            limit=0,
        )
        payload = dict(commands[-1]["payload"] or {})

        self.assertEqual(item["status"], "queued")
        self.assertEqual(commands[-1]["command_type"], PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE)
        self.assertEqual(payload["item_id"], item["item_id"])
        self.assertNotIn("legacy_materialization_item", payload)
        self.assertEqual(self.store.list_job_materialization_items(job_id="job_adapter"), [])

    def test_w6_normal_path_enqueues_projection_index_command_without_legacy_item_row(self) -> None:
        self.orchestrator.serving_projection_writer.publish_run_scope_projection(
            run_id="job_adapter",
            collection_id="company:openai",
            projection_id="proj_w6_index",
            members=[
                {
                    "candidate_identity_key": "linkedin:w6-index",
                    "person_identity_key": "linkedin:w6-index",
                    "public_summary": {"display_name": "W6 Index"},
                }
            ],
            replace_members=True,
        )
        with mock.patch.dict(
            "os.environ",
            {"SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES": "1"},
            clear=False,
        ):
            item = self.orchestrator._enqueue_projection_person_search_index_build_item(  # noqa: SLF001
                projection_id="proj_w6_index",
                job_id="job_adapter",
                reason="unit_w6_normal_path",
            )

        commands = self.store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id("job_adapter"),
            owner="projection_index_owner",
            limit=0,
        )
        payload = dict(commands[-1]["payload"] or {})

        self.assertEqual(item["status"], "queued")
        self.assertEqual(commands[-1]["command_type"], PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE)
        self.assertEqual(payload["item_id"], item["item_id"])
        self.assertNotIn("legacy_materialization_item", payload)
        self.assertEqual(self.store.list_job_materialization_items(job_id="job_adapter"), [])

    def test_w6_normal_path_enqueues_collection_merge_command_without_legacy_item_row(self) -> None:
        publication = self.orchestrator.serving_projection_writer.publish_run_scope_projection(
            run_id="job_adapter",
            collection_id="company:openai",
            projection_id="proj_w6_merge",
            members=[
                {
                    "candidate_identity_key": "linkedin:w6-merge",
                    "person_identity_key": "linkedin:w6-merge",
                    "public_summary": {"display_name": "W6 Merge"},
                }
            ],
            replace_members=True,
        )
        with mock.patch.dict(
            "os.environ",
            {"SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES": "1"},
            clear=False,
        ):
            item = self.orchestrator._enqueue_collection_authoritative_merge_item(  # noqa: SLF001
                job_id="job_adapter",
                request=self.request,
                projection_publication=publication,
                reason="unit_w6_normal_path",
            )

        commands = self.store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id("job_adapter"),
            owner="collection_writer_owner",
            limit=0,
        )
        payload = dict(commands[-1]["payload"] or {})

        self.assertEqual(item["status"], "queued")
        self.assertEqual(commands[-1]["command_type"], COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE)
        self.assertEqual(payload["item_id"], item["item_id"])
        self.assertNotIn("legacy_materialization_item", payload)
        self.assertEqual(self.store.list_job_materialization_items(job_id="job_adapter"), [])

    def test_w6_normal_path_enqueues_search_seed_command_without_legacy_item_row(self) -> None:
        item = {
            "item_id": "search_seed_w6",
            "job_id": "job_adapter",
            "target_company": "OpenAI",
            "snapshot_id": "snapshot-a",
            "item_kind": "search_seed_discovery_query",
            "source": "unit_w6_normal_path",
            "status": "queued",
            "metadata": {
                "query": "OpenAI Agent site:linkedin.com/in",
                "employment_status": "current",
                "provider_name": "harvest_profile_search",
            },
        }

        with mock.patch.dict(
            "os.environ",
            {"SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES": "1"},
            clear=False,
        ):
            command = self.orchestrator._plan_search_seed_discovery_query_run_command_for_item(  # noqa: SLF001
                item=item,
                source="unit_w6_normal_path",
                reason="unit_w6_normal_path",
            )

        commands = self.store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id("job_adapter"),
            owner="linkedin_acquisition_owner",
            limit=0,
        )
        payload = dict(commands[-1]["payload"] or {})

        self.assertEqual(command["command_type"], LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE)
        self.assertEqual(payload["item_id"], "search_seed_w6")
        self.assertNotIn("legacy_materialization_item", payload)
        self.assertEqual(self.store.list_job_materialization_items(job_id="job_adapter"), [])

    def test_w6_normal_path_enqueues_projection_facet_command_without_legacy_item_row(self) -> None:
        self.orchestrator.serving_projection_writer.publish_run_scope_projection(
            run_id="job_adapter",
            collection_id="company:openai",
            projection_id="proj_w6_facet",
            members=[
                {
                    "candidate_identity_key": "linkedin:w6-facet",
                    "person_identity_key": "linkedin:w6-facet",
                    "rank_index": 1,
                    "public_summary": {
                        "candidate_id": "candidate-a",
                        "display_name": "W6 Facet",
                        "linkedin_url": "https://www.linkedin.com/in/w6-facet/",
                    },
                }
            ],
            replace_members=True,
            counts={"candidate_count": 1, "result_count": 1, "count_scope": "exact_projection"},
            readiness={"row": "complete"},
            provenance={"source_run_id": "job_adapter", "snapshot_id": "snapshot-a"},
        )

        with mock.patch.dict(
            "os.environ",
            {"SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES": "1"},
            clear=False,
        ):
            item = self.orchestrator._enqueue_projection_facet_layering_build_item(  # noqa: SLF001
                job_id="job_adapter",
                request=self.request,
                overlay_info={"serving_projection_id": "proj_w6_facet", "snapshot_id": "snapshot-a"},
                reason="unit_w6_normal_path",
            )

        commands = self.store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id("job_adapter"),
            owner="projection_facet_layering_owner",
            limit=0,
        )
        payload = dict(commands[-1]["payload"] or {})

        self.assertEqual(item["status"], "queued")
        self.assertEqual(item["metadata"]["source_kind"], "serving_projection_members")
        self.assertEqual(commands[-1]["command_type"], PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE)
        self.assertEqual(payload["item_id"], item["item_id"])
        self.assertNotIn("legacy_materialization_item", payload)
        self.assertEqual(self.store.list_job_materialization_items(job_id="job_adapter"), [])

    def test_w6_normal_path_enqueues_snapshot_command_without_legacy_item_row(self) -> None:
        self.store.save_job(
            job_id="job_snapshot_w6",
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "Find OpenAI agent people",
                "target_company": "OpenAI",
                "keywords": ["agent"],
            },
            plan_payload={},
            summary_payload={
                "background_snapshot_materialization": {
                    "status": "deferred",
                    "snapshot_id": "snapshot-a",
                    "reason": "unit_w6_normal_path",
                }
            },
        )

        with mock.patch.dict(
            "os.environ",
            {"SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES": "1"},
            clear=False,
        ):
            item = self.orchestrator._enqueue_snapshot_full_materialization_item(  # noqa: SLF001
                job_id="job_snapshot_w6",
                source="unit_w6_normal_path",
            )

        commands = self.store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id("job_snapshot_w6"),
            owner="snapshot_materialization_owner",
            limit=0,
        )
        payload = dict(commands[-1]["payload"] or {})

        self.assertEqual(item["status"], "queued")
        self.assertEqual(commands[-1]["command_type"], SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE)
        self.assertEqual(payload["item_id"], item["item_id"])
        self.assertNotIn("legacy_materialization_item", payload)
        self.assertEqual(self.store.list_job_materialization_items(job_id="job_snapshot_w6"), [])

    def test_disabled_command_owner_does_not_implicitly_execute_board_visible_bridge(self) -> None:
        item = self.store.upsert_job_materialization_item(
            item_id="legacy_board_visible_disabled_owner",
            job_id="job_adapter",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="board_visible_delta_apply",
            source="legacy_fixture",
            status="queued",
            candidate_ids=["candidate-a"],
            metadata={"migration_adapter": True},
        )

        with mock.patch.dict(
            "os.environ",
            {"PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_OWNER_ENABLED": "0"},
            clear=False,
        ):
            result = self.orchestrator._run_board_visible_apply_queue_once(  # noqa: SLF001
                {"job_id": "job_adapter"}
            )

        refreshed = self.store.get_job_materialization_item(str(item["item_id"]))
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "legacy_job_materialization_recovery_bridge_disabled")
        self.assertTrue(result["legacy_bridge_blocked"])
        self.assertFalse(result["legacy_bridge_used"])
        self.assertEqual(refreshed["status"], "queued")
        self.assertEqual(refreshed["lease_owner"], "")


class DurableRuntimeReducerTest(unittest.TestCase):
    def test_command_owner_registry_rejects_ambiguous_owners(self) -> None:
        registry = CommandOwnerRegistry({"projection.person_search_index.build": "projection_index_owner"})

        with self.assertRaises(ValueError):
            registry.register("projection.person_search_index.build", "other_owner")

        self.assertEqual(registry.owner_for("projection.person_search_index.build"), "projection_index_owner")
        with self.assertRaises(KeyError):
            registry.owner_for("missing.command")

    def test_crm_public_web_queue_batch_command_is_registered_and_idempotent(self) -> None:
        first = crm_public_web_queue_batch_idempotency_key(
            workspace_id="default",
            batch_id="crm-public-web-batch-1",
            run_ids=["run-b", "run-a", "run-a"],
            queue_scope="crm_public_web_start",
        )
        reordered = crm_public_web_queue_batch_idempotency_key(
            workspace_id="default",
            batch_id="crm-public-web-batch-1",
            run_ids=["run-a", "run-b"],
            queue_scope="crm_public_web_start",
        )
        different_batch = crm_public_web_queue_batch_idempotency_key(
            workspace_id="default",
            batch_id="crm-public-web-batch-2",
            run_ids=["run-a", "run-b"],
            queue_scope="crm_public_web_start",
        )

        self.assertEqual(first, reordered)
        self.assertNotEqual(first, different_batch)
        self.assertTrue(first.startswith(f"{CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE}:"))
        self.assertEqual(
            DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE),
            CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
        )

        causality = command_causality_for(
            workflow_run_id="wf_crm_public_web",
            operation_id="op_crm_public_web",
            command_type=CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
            owner=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
            idempotency_key=first,
            source_event={"event_id": "evt_crm_public_web", "event_type": "CommandPlanRequested"},
            command_payload={"run_ids": ["run-a", "run-b"], "record_ids": ["crm-a", "crm-b"]},
        )

        self.assertEqual(causality.stage_id, "crm_public_web_queue_batch")
        self.assertEqual(causality.readiness_effect, "crm_public_web_workers_queued")
        self.assertEqual(
            causality.produced_entity_counts,
            {"crm_public_web_run": 2, "crm_record": 2},
        )

    def test_crm_public_web_phase_commands_are_registered_and_idempotent(self) -> None:
        first = crm_public_web_run_phase_idempotency_key(
            workspace_id="default",
            batch_id="crm-public-web-batch-1",
            run_id="crm-public-web-run-1",
            phase_command_type=CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
        )
        second = crm_public_web_run_phase_idempotency_key(
            workspace_id="default",
            batch_id="crm-public-web-batch-1",
            run_id="crm-public-web-run-1",
            phase_command_type=CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
        )
        other_phase = crm_public_web_run_phase_idempotency_key(
            workspace_id="default",
            batch_id="crm-public-web-batch-1",
            run_id="crm-public-web-run-1",
            phase_command_type="crm.public_web.search.poll_fetch",
        )

        self.assertEqual(first, second)
        self.assertNotEqual(first, other_phase)
        self.assertTrue(first.startswith(f"{CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE}:"))
        for command_type in CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES:
            self.assertEqual(DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(command_type), CRM_PUBLIC_WEB_PHASE_OWNER)

        causality = command_causality_for(
            workflow_run_id="wf_crm_public_web",
            operation_id="op_crm_public_web",
            command_type=CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
            owner=CRM_PUBLIC_WEB_PHASE_OWNER,
            idempotency_key=first,
            source_event={
                "event_id": "evt_crm_public_web_phase",
                "event_type": "CommandPlanRequested",
                "command_id": "cmd_parent_queue_batch",
                "payload": {"causal_group_id": "crm_public_web:batch:1"},
            },
            command_payload={
                "run_id": "crm-public-web-run-1",
                "run_ids": ["crm-public-web-run-1"],
                "crm_record_id": "crmrec-1",
                "crm_record_ids": ["crmrec-1"],
                "run_count": 1,
                "record_count": 1,
            },
        )

        self.assertEqual(causality.parent_command_id, "cmd_parent_queue_batch")
        self.assertEqual(causality.causal_group_id, "crm_public_web:batch:1")
        self.assertEqual(causality.stage_id, "crm_public_web_search_submit")
        self.assertEqual(causality.readiness_effect, "crm_public_web_search_submitted")
        self.assertEqual(causality.produced_entity_counts, {"crm_public_web_run": 1, "crm_record": 1})

    def test_reducer_generates_typed_commands_and_completion_outbox(self) -> None:
        registry = CommandOwnerRegistry(
            {"linkedin.profile_refill.submit_batch": "linkedin_profile_owner"}
        )
        events = [
            {
                "event_id": "evt_1",
                "workflow_run_id": "wf_reducer_1",
                "sequence_number": 1,
                "event_type": "WorkflowStarted",
                "payload": {"stage_key": "profile_fetch"},
            },
            {
                "event_id": "evt_2",
                "workflow_run_id": "wf_reducer_1",
                "sequence_number": 2,
                "event_type": "CommandPlanRequested",
                "payload": {
                    "command_type": "linkedin.profile_refill.submit_batch",
                    "idempotency_key": "wf_reducer_1:profile_batch:1",
                    "stage_id": "profile_fetch",
                    "causal_group_id": "profile_fetch:batch:1",
                    "payload": {"profile_url_count": 50},
                },
            },
            {
                "event_id": "evt_3",
                "workflow_run_id": "wf_reducer_1",
                "sequence_number": 3,
                "event_type": "WorkflowCompleted",
                "payload": {"stage_key": "serving_finalized"},
            },
        ]

        result = reduce_workflow_events(
            current_state={},
            new_events=events,
            existing_commands=[],
            owner_registry=registry,
        )
        duplicate_guard = reduce_workflow_events(
            current_state={"status": "running"},
            new_events=[events[1]],
            existing_commands=[{"idempotency_key": "wf_reducer_1:profile_batch:1"}],
            owner_registry=registry,
        )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.current_stage_key, "serving_finalized")
        self.assertEqual(len(result.commands), 1)
        self.assertEqual(result.commands[0].owner, "linkedin_profile_owner")
        self.assertEqual(result.commands[0].payload["profile_url_count"], 50)
        self.assertEqual(result.commands[0].payload["causality"]["source_event_id"], "evt_2")
        self.assertEqual(result.commands[0].payload["causality"]["causal_group_id"], "profile_fetch:batch:1")
        self.assertEqual(result.commands[0].payload["causality"]["stage_id"], "profile_fetch")
        self.assertEqual(
            result.commands[0].payload["causality"]["readiness_effect"],
            "profile_refill_submitted",
        )
        self.assertEqual(result.commands[0].payload["causality"]["produced_entity_counts"], {"profile_url": 50})
        self.assertEqual(len(result.outbox), 1)
        self.assertEqual(result.outbox[0].outbox_type, "workflow.completed")
        self.assertEqual(duplicate_guard.commands, ())

    def test_command_causality_infers_counts_for_typed_build_and_apply_commands(self) -> None:
        cases = [
            (
                LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
                "linkedin_profile_owner",
                {"entry_count": 2, "entries": [{"profile_url": "a"}, {"profile_url": "b"}]},
                {"terminal_entry": 2},
            ),
            (
                LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
                "profile_local_apply_owner",
                {"candidate_ids": ["candidate-a", "candidate-b"], "source_worker_ids": [1]},
                {"candidate": 2, "source_worker": 1},
            ),
            (
                COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
                "collection_writer_owner",
                {"materialization_metadata": {"member_count": 12}},
                {"member": 12},
            ),
            (
                PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
                "projection_facet_layering_owner",
                {"candidate_ids": ["candidate-a"]},
                {"candidate": 1},
            ),
            (
                PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
                "projection_index_owner",
                {"materialization_metadata": {"member_count": 9}},
                {"indexed_person": 9},
            ),
            (
                PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
                "serving_projection_owner",
                {"candidate_source": {"candidate_count": 7}},
                {"candidate": 7},
            ),
            (
                SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
                "snapshot_materialization_owner",
                {"source_worker_ids": [1, 2, 3]},
                {"source_worker": 3},
            ),
        ]

        for command_type, owner, payload, expected_counts in cases:
            with self.subTest(command_type=command_type):
                causality = command_causality_for(
                    workflow_run_id="wf_counts",
                    operation_id="op_counts",
                    command_type=command_type,
                    owner=owner,
                    idempotency_key=f"{command_type}:counts",
                    source_event={
                        "event_id": f"evt_{command_type}",
                        "event_type": "CommandPlanRequested",
                    },
                    command_payload=payload,
                )

                self.assertEqual(causality.produced_entity_counts, expected_counts)
                self.assertEqual(causality.no_op_reason, "")

    def test_command_causality_records_typed_build_product_when_payload_has_no_member_count(self) -> None:
        cases = [
            (PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE, "serving_projection_owner", {"projection": 1}),
            (COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE, "collection_writer_owner", {"collection_projection": 1}),
            (PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE, "projection_facet_layering_owner", {"facet_layering": 1}),
            (PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE, "projection_index_owner", {"person_search_index": 1}),
            (SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE, "snapshot_materialization_owner", {"snapshot": 1}),
        ]

        for command_type, owner, expected_counts in cases:
            with self.subTest(command_type=command_type):
                causality = command_causality_for(
                    workflow_run_id="wf_build_product",
                    operation_id="op_build_product",
                    command_type=command_type,
                    owner=owner,
                    idempotency_key=f"{command_type}:build_product",
                    source_event={
                        "event_id": f"evt_{command_type}",
                        "event_type": "CommandPlanRequested",
                    },
                    command_payload={"item_id": "item-a"},
                )

                self.assertEqual(causality.produced_entity_counts, expected_counts)
                self.assertEqual(causality.no_op_reason, "")

    def test_command_causality_records_no_op_reason_for_explicit_empty_input(self) -> None:
        causality = command_causality_for(
            workflow_run_id="wf_empty",
            operation_id="op_empty",
            command_type=LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
            owner="profile_local_apply_owner",
            idempotency_key="linkedin.local_profile_delta.apply:empty",
            source_event={"event_id": "evt_empty", "event_type": "CommandPlanRequested"},
            command_payload={"candidate_count": 0, "candidate_ids": []},
        )

        self.assertEqual(causality.produced_entity_counts, {})
        self.assertEqual(
            causality.no_op_reason,
            "linkedin.local_profile_delta.apply:empty_input",
        )

    def test_reducer_keeps_completed_workflow_terminal_but_plans_post_result_commands(self) -> None:
        registry = CommandOwnerRegistry(
            {PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE: "board_visible_projection_owner"}
        )
        post_result_event = {
            "event_id": "evt_post_result",
            "workflow_run_id": "wf_reducer_completed",
            "sequence_number": 4,
            "event_type": "CommandPlanRequested",
            "payload": {
                "command_type": PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
                "idempotency_key": "projection.board_visible_patch.publish:completed:1",
                "payload": {"candidate_ids": ["candidate-a"]},
            },
        }

        result = reduce_workflow_events(
            current_state={
                "status": "completed",
                "current_stage_key": "serving_finalized",
            },
            new_events=[post_result_event],
            existing_commands=[],
            owner_registry=registry,
        )
        duplicate_guard = reduce_workflow_events(
            current_state={
                "status": "completed",
                "current_stage_key": "serving_finalized",
            },
            new_events=[post_result_event],
            existing_commands=[
                {"idempotency_key": "projection.board_visible_patch.publish:completed:1"}
            ],
            owner_registry=registry,
        )
        reopened_guard = reduce_workflow_events(
            current_state={
                "status": "completed",
                "current_stage_key": "serving_finalized",
            },
            new_events=[
                {
                    "event_id": "evt_late_start",
                    "workflow_run_id": "wf_reducer_completed",
                    "sequence_number": 5,
                    "event_type": "WorkflowStarted",
                    "payload": {"stage_key": "profile_fetch"},
                }
            ],
            existing_commands=[],
            owner_registry=registry,
        )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.current_stage_key, "serving_finalized")
        self.assertEqual(len(result.commands), 1)
        self.assertEqual(result.commands[0].owner, "board_visible_projection_owner")
        self.assertEqual(result.commands[0].payload["candidate_ids"], ["candidate-a"])
        self.assertEqual(duplicate_guard.commands, ())
        self.assertEqual(reopened_guard.status, "completed")
        self.assertEqual(reopened_guard.current_stage_key, "serving_finalized")


if __name__ == "__main__":
    unittest.main()
