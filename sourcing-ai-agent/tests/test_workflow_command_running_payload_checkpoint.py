from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin

try:
    import psycopg
except Exception:  # pragma: no cover - PG fixture skips before this path matters.
    psycopg = None


class WorkflowCommandRunningPayloadCheckpointTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _create_command(self, suffix: str, *, payload: dict[str, object]) -> dict[str, object]:
        return self.store.upsert_workflow_command(
            workflow_run_id=f"wf_checkpoint_{suffix}",
            operation_id=f"op_checkpoint_{suffix}",
            command_type="crm.public_web.queue_batch",
            owner="crm_public_web_owner",
            idempotency_key=f"wf_checkpoint_{suffix}:queue_batch",
            payload=payload,
            artifact_refs=["artifact://original"],
            max_attempts=3,
        )

    def _expire_lease(self, command_id: str) -> None:
        if psycopg is None:
            self.skipTest("psycopg is required for PG lease mutation")
        fixture = self._pg_durable_runtime_fixture
        assert fixture is not None
        with psycopg.connect(
            fixture.dsn,
            autocommit=True,
            connect_timeout=5,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'SET search_path TO "{fixture.schema}"')
                cursor.execute(
                    """
                    UPDATE workflow_commands
                    SET lease_expires_at = '2000-01-01 00:00:00'
                    WHERE command_id = %s
                    """,
                    (command_id,),
                )

    def test_checkpoint_updates_running_payload_and_typed_causality_under_matching_lease(self) -> None:
        command = self._create_command("success", payload={"phase": "before"})
        command_id = str(command["command_id"])
        self.store.claim_workflow_command(command_id, lease_owner="worker-a", lease_seconds=300)
        self.store.mark_workflow_command_running(command_id, lease_owner="worker-a")

        generic_update = self.store.update_workflow_command_payload(
            command_id,
            payload={"phase": "generic-update-must-not-write"},
        )
        self.assertEqual(generic_update, {})
        self.assertEqual(self.store.get_workflow_command(command_id)["payload"], {"phase": "before"})

        payload = {
            "phase": "batch_started",
            "batch_id": "batch-1",
            "causality": {
                "stage_id": "crm_public_web",
                "causal_group_id": "group-1",
                "parent_command_id": "parent-1",
                "source_event_id": "event-1",
                "source_event_type": "crm.public_web.requested",
                "input_artifact_refs": ["artifact://input"],
                "output_artifact_refs": ["artifact://output"],
                "produced_entity_counts": {"batch": 1},
                "no_op_reason": "",
                "readiness_effect": "public_web_queued",
                "downstream_command_ids": ["command-next"],
                "schema_version": "command_causality_v1",
            },
        }
        checkpointed = self.store.checkpoint_running_workflow_command_payload(
            command_id,
            lease_owner="worker-a",
            payload=payload,
        )

        self.assertEqual(checkpointed["status"], "running")
        self.assertEqual(checkpointed["lease_owner"], "worker-a")
        self.assertEqual(checkpointed["payload"], payload)
        self.assertEqual(checkpointed["stage_id"], "crm_public_web")
        self.assertEqual(checkpointed["causal_group_id"], "group-1")
        self.assertEqual(checkpointed["parent_command_id"], "parent-1")
        self.assertEqual(checkpointed["source_event_id"], "event-1")
        self.assertEqual(checkpointed["source_event_type"], "crm.public_web.requested")
        self.assertEqual(checkpointed["input_artifact_refs"], ["artifact://input"])
        self.assertEqual(checkpointed["output_artifact_refs"], ["artifact://output"])
        self.assertEqual(checkpointed["produced_entity_counts"], {"batch": 1})
        self.assertEqual(checkpointed["readiness_effect"], "public_web_queued")
        self.assertEqual(checkpointed["downstream_command_ids"], ["command-next"])
        self.assertEqual(checkpointed["artifact_refs"], ["artifact://original"])
        self.assertEqual(self.store.get_workflow_command(command_id), checkpointed)

    def test_checkpoint_rejects_wrong_owner_and_expired_lease_without_writing(self) -> None:
        initial_payload = {"phase": "before"}
        command = self._create_command("fenced", payload=initial_payload)
        command_id = str(command["command_id"])
        self.store.claim_workflow_command(command_id, lease_owner="worker-a", lease_seconds=300)
        self.store.mark_workflow_command_running(command_id, lease_owner="worker-a")

        wrong_owner = self.store.checkpoint_running_workflow_command_payload(
            command_id,
            lease_owner="worker-b",
            payload={"phase": "wrong-owner"},
        )
        self.assertEqual(wrong_owner, {})
        self.assertEqual(self.store.get_workflow_command(command_id)["payload"], initial_payload)

        self._expire_lease(command_id)
        expired = self.store.checkpoint_running_workflow_command_payload(
            command_id,
            lease_owner="worker-a",
            payload={"phase": "expired"},
        )
        self.assertEqual(expired, {})
        self.assertEqual(self.store.get_workflow_command(command_id)["payload"], initial_payload)

    def test_checkpoint_rejects_non_running_command_without_writing(self) -> None:
        initial_payload = {"phase": "queued"}
        command = self._create_command("queued", payload=initial_payload)
        command_id = str(command["command_id"])

        checkpointed = self.store.checkpoint_running_workflow_command_payload(
            command_id,
            lease_owner="worker-a",
            payload={"phase": "must-not-write"},
        )

        self.assertEqual(checkpointed, {})
        self.assertEqual(self.store.get_workflow_command(command_id)["payload"], initial_payload)

    def test_checkpoint_surfaces_native_postgres_failures_instead_of_reporting_cas_conflict(self) -> None:
        postgres = self.store._control_plane_postgres  # noqa: SLF001
        original_checkpoint = postgres.checkpoint_running_workflow_command_payload

        def fail_checkpoint(*args: object, **kwargs: object) -> None:
            del args, kwargs
            raise ValueError("injected checkpoint writer failure")

        postgres.checkpoint_running_workflow_command_payload = fail_checkpoint  # type: ignore[method-assign]
        try:
            with self.assertRaisesRegex(
                RuntimeError,
                "Postgres authoritative write failed for workflow_commands via "
                "checkpoint_running_workflow_command_payload",
            ):
                self.store.checkpoint_running_workflow_command_payload(
                    "command-native-failure",
                    lease_owner="worker-a",
                    payload={"phase": "must-surface"},
                )
        finally:
            postgres.checkpoint_running_workflow_command_payload = original_checkpoint  # type: ignore[method-assign]


if __name__ == "__main__":
    unittest.main()
