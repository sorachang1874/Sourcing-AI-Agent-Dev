import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.storage import ControlPlaneStore
from tests.testcontainers_helpers import (
    libpq_dsn_from_container,
    psycopg,
    require_testcontainers_pg,
    start_postgres_container,
)


class TestcontainersPostgresContractTest(unittest.TestCase):
    def test_pg_only_store_bootstraps_serving_projection_and_durable_runtime_tables(self) -> None:
        require_testcontainers_pg()
        assert psycopg is not None

        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            postgres = start_postgres_container()
            try:
                dsn = libpq_dsn_from_container(postgres)
                env = {
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": dsn,
                    "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "projection_contract",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
                    "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
                }
                with mock.patch.dict(os.environ, env, clear=False):
                    store = ControlPlaneStore(runtime_dir / "sourcing_agent.db")
                    # PG-only reads fail closed on missing tables, so the contract harness must run
                    # the versioned migration before repository read-before-write paths execute.
                    store._control_plane_postgres.ensure_bootstrapped()  # noqa: SLF001
                    store.repos.serving_projection.upsert(
                        {
                            "projection_id": "proj_container_pg",
                            "projection_type": "run_scope_projection",
                            "collection_id": "company:openai",
                            "source_run_id": "job-container-pg",
                            "state": "serving",
                            "counts": {"candidate_count": 1},
                        }
                    )
                    store.repos.serving_projection.upsert_members(
                        "proj_container_pg",
                        [
                            {
                                "candidate_identity_key": "linkedin:container",
                                "person_identity_key": "linkedin:container",
                                "rank_index": 0,
                                "public_summary": {"name": "Container Candidate"},
                            }
                        ],
                    )
                    store.repos.serving_projection.upsert_run_link(
                        {
                            "run_id": "job-container-pg",
                            "projection_id": "proj_container_pg",
                            "collection_id": "company:openai",
                        }
                    )
                    store.repos.serving_projection.upsert_authoritative_pointer(
                        {
                            "collection_id": "company:openai",
                            "active_projection_id": "proj_container_pg",
                            "active_collection_version": "v1",
                        }
                    )
                    event = store.repos.workflow_runtime.append_workflow_event(
                        workflow_run_id="wf_container_pg",
                        operation_id="op_container_pg",
                        event_family="workflow_event",
                        event_type="WorkflowStarted",
                        idempotency_key="wf_container_pg:start",
                        payload={"stage_key": "stage1_candidate_set"},
                    )
                    command = store.upsert_workflow_command(
                        workflow_run_id="wf_container_pg",
                        operation_id="op_container_pg",
                        command_type="linkedin.profile_refill.submit_batch",
                        owner="linkedin_profile_owner",
                        idempotency_key="wf_container_pg:profile_batch:1",
                        payload={
                            "profile_url_count": 1,
                            "causality": {
                                "workflow_run_id": "wf_container_pg",
                                "operation_id": "op_container_pg",
                                "stage_id": "profile_fetch",
                                "command_type": "linkedin.profile_refill.submit_batch",
                                "owner": "linkedin_profile_owner",
                                "causal_group_id": "cg_container_profile_batch_1",
                                "source_event_id": event["event_id"],
                                "source_event_type": "WorkflowStarted",
                                "idempotency_key": "wf_container_pg:profile_batch:1",
                                "produced_entity_counts": {"profile_url": 1},
                                "readiness_effect": "profile_refill_submitted",
                                "schema_version": "command_causality_v1",
                            },
                        },
                    )
                    claimed_command = store.claim_workflow_command(command["command_id"], lease_owner="pg-worker")
                    store.mark_workflow_command_running(command["command_id"], lease_owner="pg-worker")
                    store.mark_workflow_command_succeeded(
                        command["command_id"],
                        result={"provider_run_id": "pg-provider-run"},
                    )
                    store.repos.workflow_runtime.upsert_workflow_current_state(
                        workflow_run_id="wf_container_pg",
                        operation_id="op_container_pg",
                        workflow_type="linkedin_acquisition",
                        status="running",
                        current_stage_key="profile_fetch",
                        last_processed_sequence_number=event["sequence_number"],
                    )
                    outbox = store.repos.workflow_runtime.enqueue_runtime_outbox(
                        workflow_run_id="wf_container_pg",
                        operation_id="op_container_pg",
                        command_id=command["command_id"],
                        outbox_type="stream.workflow_event",
                        idempotency_key="wf_container_pg:stream:start",
                        payload={"event_id": event["event_id"]},
                    )
                    store.repos.workflow_runtime.mark_runtime_outbox_dispatched(outbox["outbox_id"])
                    store.close()

                with psycopg.connect(dsn) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SET search_path TO projection_contract")
                        cursor.execute(
                            """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = 'projection_contract'
                              AND table_name IN (
                                'serving_projections',
                                'serving_projection_members',
                                'projection_manifest_shards',
                                'run_projection_links',
                                'collection_authoritative_pointers',
                                'workflow_events',
                                'workflow_current_state',
                                'workflow_commands',
                                'runtime_outbox'
                              )
                            ORDER BY table_name
                            """
                        )
                        table_names = [str(row[0]) for row in cursor.fetchall()]
                        cursor.execute("SELECT COUNT(*) FROM serving_projection_members WHERE projection_id = %s", ("proj_container_pg",))
                        member_count = int(cursor.fetchone()[0])
                        cursor.execute("SELECT COUNT(*) FROM run_projection_links WHERE run_id = %s", ("job-container-pg",))
                        link_count = int(cursor.fetchone()[0])
                        cursor.execute(
                            "SELECT active_projection_id FROM collection_authoritative_pointers WHERE collection_id = %s",
                            ("company:openai",),
                        )
                        active_projection_id = str(cursor.fetchone()[0])
                        cursor.execute("SELECT COUNT(*) FROM workflow_events WHERE workflow_run_id = %s", ("wf_container_pg",))
                        workflow_event_count = int(cursor.fetchone()[0])
                        cursor.execute(
                            """
                            SELECT status,
                                   attempt,
                                   result_json,
                                   causal_group_id,
                                   source_event_id,
                                   source_event_type,
                                   produced_entity_counts_json,
                                   readiness_effect,
                                   causality_schema_version
                            FROM workflow_commands
                            WHERE workflow_run_id = %s
                            """,
                            ("wf_container_pg",),
                        )
                        (
                            workflow_command_status,
                            workflow_command_attempt,
                            workflow_command_result_json,
                            workflow_command_causal_group_id,
                            workflow_command_source_event_id,
                            workflow_command_source_event_type,
                            workflow_command_produced_counts_json,
                            workflow_command_readiness_effect,
                            workflow_command_causality_schema_version,
                        ) = cursor.fetchone()
                        cursor.execute("SELECT COUNT(*) FROM workflow_commands WHERE workflow_run_id = %s", ("wf_container_pg",))
                        workflow_command_count = int(cursor.fetchone()[0])
                        cursor.execute(
                            "SELECT status, last_processed_sequence_number FROM workflow_current_state WHERE workflow_run_id = %s",
                            ("wf_container_pg",),
                        )
                        workflow_state_status, workflow_state_sequence = cursor.fetchone()
                        cursor.execute("SELECT status FROM runtime_outbox WHERE workflow_run_id = %s", ("wf_container_pg",))
                        outbox_status = str(cursor.fetchone()[0])
            finally:
                postgres.stop()

        self.assertEqual(
            table_names,
            [
                "collection_authoritative_pointers",
                "projection_manifest_shards",
                "run_projection_links",
                "runtime_outbox",
                "serving_projection_members",
                "serving_projections",
                "workflow_commands",
                "workflow_current_state",
                "workflow_events",
            ],
        )
        self.assertEqual(member_count, 1)
        self.assertEqual(link_count, 1)
        self.assertEqual(active_projection_id, "proj_container_pg")
        self.assertEqual(workflow_event_count, 1)
        self.assertEqual(workflow_command_count, 1)
        self.assertEqual(claimed_command["status"], "claimed")
        self.assertEqual(str(workflow_command_status), "succeeded")
        self.assertEqual(int(workflow_command_attempt), 1)
        self.assertIn("pg-provider-run", str(workflow_command_result_json))
        self.assertEqual(str(workflow_command_causal_group_id), "cg_container_profile_batch_1")
        self.assertEqual(str(workflow_command_source_event_id), event["event_id"])
        self.assertEqual(str(workflow_command_source_event_type), "WorkflowStarted")
        self.assertIn("profile_url", str(workflow_command_produced_counts_json))
        self.assertEqual(str(workflow_command_readiness_effect), "profile_refill_submitted")
        self.assertEqual(str(workflow_command_causality_schema_version), "command_causality_v1")
        self.assertEqual(str(workflow_state_status), "running")
        self.assertEqual(int(workflow_state_sequence), 1)
        self.assertEqual(outbox_status, "dispatched")


if __name__ == "__main__":
    unittest.main()
