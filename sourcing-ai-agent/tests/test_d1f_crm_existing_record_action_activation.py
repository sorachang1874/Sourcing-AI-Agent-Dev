from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.operation_runtime import (
    ACTION_ADD_CRM_NOTE,
    ACTION_CREATE_CRM_TASK,
    ACTION_EXTERNAL_INTAKE,
    ACTION_FETCH_PROFILE_SAMPLE,
    ACTION_SET_CRM_STAGE,
    CRM_EXISTING_RECORD_ACTION_TYPES,
    CRM_RESOURCE_BOUND_ACTION_TYPES,
    DEFAULT_ACTION_REGISTRY,
    OperationSubmissionResult,
)
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
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin, psycopg

CRM_ACTION_INPUTS: dict[str, dict[str, Any]] = {
    ACTION_SET_CRM_STAGE: {"stage": "researching", "quality_score": 91.0, "comment": "qualified"},
    ACTION_ADD_CRM_NOTE: {"note": "Follow up after the conference."},
    ACTION_CREATE_CRM_TASK: {
        "title": "Prepare introduction",
        "description": "Draft a concise introduction.",
        "due_at": "2026-07-20T09:00:00Z",
    },
}

CRM_DOMAIN_TABLES = (
    "crm_records",
    "crm_engagements",
    "crm_tasks",
    "crm_events",
)
FULL_ZERO_WRITE_TABLES = (
    "agent_actions",
    "operation_runs",
    "operation_events",
    "workflow_events",
    "workflow_current_state",
    "workflow_commands",
    "runtime_outbox",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    *CRM_DOMAIN_TABLES,
)


def test_production_registry_reflects_current_schema_partition_and_still_serves_zero_tools() -> None:
    records = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    schema_defined = {
        action_type for action_type in records if DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema
    }

    assert len(records) == 15
    assert schema_defined == set(CRM_RESOURCE_BOUND_ACTION_TYPES)
    assert len(schema_defined) == 4
    assert sum(not DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema for action_type in records) == 11
    assert sum(record.get("agent_tool_enabled") is True for record in records.values()) == 0
    assert all("served_tool_status" not in record for record in records.values())


class D1fCRMExistingRecordActionActivationPGTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir, schema_label="d1f_crm_action")
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "d1f-crm-action.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        self.orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, model_client),
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _seed_record(
        self,
        crm_record_id: str,
        *,
        workspace_id: str = "user-alice",
        owner_user_id: str = "alice",
    ) -> dict[str, Any]:
        return self.store.upsert_crm_record(
            {
                "crm_record_id": crm_record_id,
                "workspace_id": workspace_id,
                "owner_user_id": owner_user_id,
                "person_identity_key": f"person::{crm_record_id}",
                "display_name_cache": crm_record_id,
            }
        )

    def _submit_authenticated(
        self,
        *,
        action_type: str,
        crm_record_id: str,
        idempotency_key: str,
        input_payload: dict[str, Any] | None = None,
        target_ref: dict[str, Any] | None = None,
        workspace_id: str = "user-alice",
        owner_user_id: str = "alice",
    ) -> dict[str, Any]:
        return self.orchestrator.submit_operation_action(
            {
                "action_type": action_type,
                "workspace_id": workspace_id,
                "target_ref": target_ref if target_ref is not None else {"crm_record_id": crm_record_id},
                "input": dict(input_payload if input_payload is not None else CRM_ACTION_INPUTS[action_type]),
                "idempotency_key": idempotency_key,
                "actor": owner_user_id,
            },
            expected_workspace_id=workspace_id,
            expected_owner_user_id=owner_user_id,
        )

    def _submit_open(
        self,
        *,
        action_type: str,
        crm_record_id: str,
        idempotency_key: str,
        workspace_id: str,
        input_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.orchestrator.submit_operation_action(
            {
                "action_type": action_type,
                "workspace_id": workspace_id,
                "target_ref": {"crm_record_id": crm_record_id},
                "input": dict(input_payload if input_payload is not None else CRM_ACTION_INPUTS[action_type]),
                "idempotency_key": idempotency_key,
                "actor": "operator",
            }
        )

    def _table_state(self, table_names: tuple[str, ...]) -> dict[str, tuple[str, ...]]:
        fixture = self._pg_durable_runtime_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        state: dict[str, tuple[str, ...]] = {}
        with psycopg.connect(
            fixture.dsn,
            autocommit=True,
            connect_timeout=5,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                for table_name in table_names:
                    quoted_table = quote_control_plane_postgres_identifier(table_name)
                    cursor.execute(f"SELECT row_to_json(t)::text FROM {quoted_schema}.{quoted_table} AS t")
                    state[table_name] = tuple(sorted(str(row[0]) for row in cursor.fetchall()))
        return state

    def _delete_operation_run(self, operation_run_id: str) -> None:
        fixture = self._pg_durable_runtime_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        with psycopg.connect(
            fixture.dsn,
            autocommit=True,
            connect_timeout=5,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM {quoted_schema}.operation_events WHERE operation_run_id = %s",
                    (operation_run_id,),
                )
                cursor.execute(
                    f"DELETE FROM {quoted_schema}.operation_runs WHERE operation_run_id = %s",
                    (operation_run_id,),
                )

    def _delete_action(self, action_id: str) -> None:
        fixture = self._pg_durable_runtime_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        with psycopg.connect(
            fixture.dsn,
            autocommit=True,
            connect_timeout=5,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM {quoted_schema}.operation_events WHERE action_id = %s",
                    (action_id,),
                )
                cursor.execute(
                    f"DELETE FROM {quoted_schema}.agent_actions WHERE action_id = %s",
                    (action_id,),
                )

    def _assert_not_found(self, result: dict[str, Any]) -> None:
        self.assertEqual(result.get("status"), "not_found", result)
        self.assertEqual(result.get("reason"), "crm_record_not_found", result)
        self.assertFalse(result.get("module_state_mutated", False), result)

    def test_authenticated_missing_foreign_and_forged_requests_are_pre_submit_zero_write(self) -> None:
        self._seed_record("owned")
        self._seed_record("foreign-workspace", workspace_id="user-bob", owner_user_id="bob")
        self._seed_record("foreign-adjunct", owner_user_id="bob")
        baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

        for action_type in CRM_EXISTING_RECORD_ACTION_TYPES:
            for record_id in ("missing", "foreign-workspace", "foreign-adjunct"):
                with self.subTest(action_type=action_type, record_id=record_id):
                    result = self._submit_authenticated(
                        action_type=action_type,
                        crm_record_id=record_id,
                        idempotency_key=f"denied:{action_type}:{record_id}",
                    )
                    self._assert_not_found(result)
                    self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

        forged_requests = (
            {
                "target_ref": {"record_id": "owned"},
                "input_payload": CRM_ACTION_INPUTS[ACTION_ADD_CRM_NOTE],
            },
            {
                "target_ref": {"crm_record_id": "owned", "workspace_id": "user-alice"},
                "input_payload": CRM_ACTION_INPUTS[ACTION_ADD_CRM_NOTE],
            },
            {
                "target_ref": {"crm_record_id": "owned"},
                "input_payload": {**CRM_ACTION_INPUTS[ACTION_ADD_CRM_NOTE], "crm_record_id": "owned"},
            },
            {
                "target_ref": {"crm_record_id": "owned"},
                "input_payload": {**CRM_ACTION_INPUTS[ACTION_ADD_CRM_NOTE], "record_id": "owned"},
            },
        )
        with mock.patch.object(self.store, "get_crm_record", wraps=self.store.get_crm_record) as record_lookup:
            for index, forged in enumerate(forged_requests):
                with self.subTest(forged_index=index):
                    result = self._submit_authenticated(
                        action_type=ACTION_ADD_CRM_NOTE,
                        crm_record_id="owned",
                        idempotency_key=f"forged:{index}",
                        input_payload=dict(forged["input_payload"]),
                        target_ref=dict(forged["target_ref"]),
                    )
                    self.assertEqual(result.get("status"), "invalid", result)
                    self.assertFalse(result.get("module_state_mutated", False), result)
                    self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)
            self.assertEqual(record_lookup.call_count, 0)

    def test_same_owner_blank_adjunct_open_mode_replay_and_collision_matrix(self) -> None:
        owned = self._seed_record("owned")
        blank = self._seed_record("blank-adjunct", owner_user_id="")
        operator = self._seed_record(
            "operator-record",
            workspace_id="operator-a",
            owner_user_id="legacy-user",
        )
        crm_before = self._table_state(CRM_DOMAIN_TABLES)

        same_owner_results: dict[str, dict[str, Any]] = {}
        for action_type in CRM_EXISTING_RECORD_ACTION_TYPES:
            result = self._submit_authenticated(
                action_type=action_type,
                crm_record_id="owned",
                idempotency_key=f"same-owner:{action_type}",
            )
            with self.subTest(action_type=action_type):
                self.assertEqual(result.get("status"), "queued", result)
                self.assertEqual(result["action"]["target_ref"]["crm_record_id"], "owned")
                self.assertEqual(result["action"]["target_ref"]["workspace_id"], "user-alice")
                self.assertEqual(result["action"]["target_ref"]["owner_user_id"], "alice")
                self.assertEqual(result["action"]["target_ref"]["crm_version"], owned["crm_version"])
                self.assertTrue(result["action"]["request_schema_version"])
                self.assertEqual(len(result["action"]["request_schema_digest"]), 64)
            same_owner_results[action_type] = result

        blank_result = self._submit_authenticated(
            action_type=ACTION_ADD_CRM_NOTE,
            crm_record_id="blank-adjunct",
            idempotency_key="blank-adjunct:note",
        )
        self.assertEqual(blank_result.get("status"), "queued", blank_result)
        self.assertEqual(blank_result["action"]["target_ref"]["owner_user_id"], "")
        self.assertEqual(blank_result["action"]["target_ref"]["crm_version"], blank["crm_version"])

        open_result = self._submit_open(
            action_type=ACTION_CREATE_CRM_TASK,
            crm_record_id="operator-record",
            idempotency_key="open:task",
            workspace_id="operator-a",
        )
        self.assertEqual(open_result.get("status"), "queued", open_result)
        self.assertEqual(open_result["action"]["target_ref"]["workspace_id"], "operator-a")
        self.assertEqual(open_result["action"]["target_ref"]["owner_user_id"], "legacy-user")
        self.assertEqual(open_result["action"]["target_ref"]["crm_version"], operator["crm_version"])
        self.assertEqual(self._table_state(CRM_DOMAIN_TABLES), crm_before)

        before_open_mismatch = self._table_state(FULL_ZERO_WRITE_TABLES)
        open_mismatch = self._submit_open(
            action_type=ACTION_CREATE_CRM_TASK,
            crm_record_id="operator-record",
            idempotency_key="open:mismatch",
            workspace_id="operator-b",
        )
        self._assert_not_found(open_mismatch)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_open_mismatch)

        original_note = same_owner_results[ACTION_ADD_CRM_NOTE]
        before_replay = self._table_state(FULL_ZERO_WRITE_TABLES)
        replay = self._submit_authenticated(
            action_type=ACTION_ADD_CRM_NOTE,
            crm_record_id="owned",
            idempotency_key="same-owner:add_crm_note",
        )
        self.assertEqual(replay.get("status"), "queued", replay)
        self.assertEqual(replay["action"]["action_id"], original_note["action"]["action_id"])
        self.assertEqual(
            replay["operation_run"]["operation_run_id"],
            original_note["operation_run"]["operation_run_id"],
        )
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_replay)

        collision = self._submit_authenticated(
            action_type=ACTION_ADD_CRM_NOTE,
            crm_record_id="owned",
            idempotency_key="same-owner:add_crm_note",
            input_payload={"note": "A different request under the same idempotency key."},
        )
        self.assertEqual(collision.get("status"), "conflict", collision)
        self.assertEqual(collision.get("reason"), "operation_action_idempotency_payload_conflict", collision)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_replay)

    def test_crm_input_envelopes_use_presence_and_fail_closed_before_runtime_writes(self) -> None:
        self._seed_record("input-envelope")
        valid_input = dict(CRM_ACTION_INPUTS[ACTION_ADD_CRM_NOTE])

        def submit(envelopes: dict[str, Any], *, suffix: str) -> dict[str, Any]:
            return self.orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_ADD_CRM_NOTE,
                    "workspace_id": "user-alice",
                    "target_ref": {"crm_record_id": "input-envelope"},
                    "idempotency_key": f"input-envelope:{suffix}",
                    "actor": "alice",
                    **envelopes,
                },
                expected_workspace_id="user-alice",
                expected_owner_user_id="alice",
            )

        baseline = self._table_state(FULL_ZERO_WRITE_TABLES)
        malformed_cases = (
            {"input": [], "input_payload": valid_input},
            {"input": "", "input_payload": valid_input},
            {"input": False, "input_payload": valid_input},
            {"input": 0, "input_payload": valid_input},
            {"input": None, "input_payload": valid_input},
            {"input_payload": []},
            {"input_payload": ""},
            {"input_payload": False},
            {"input_payload": 0},
            {"input_payload": None},
            {"input": valid_input, "input_payload": []},
            {"input": valid_input, "input_payload": ""},
            {"input": valid_input, "input_payload": False},
            {"input": valid_input, "input_payload": 0},
            {"input": valid_input, "input_payload": None},
        )
        for index, envelopes in enumerate(malformed_cases):
            with self.subTest(case="malformed", index=index, envelopes=envelopes):
                result = submit(envelopes, suffix=f"malformed:{index}")
                self.assertEqual(result.get("status"), "invalid", result)
                self.assertEqual(result.get("reason"), "action_request_input_payload_must_be_object", result)
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

        ambiguous_cases = (
            {"input": valid_input, "input_payload": valid_input},
            {"input": valid_input, "input_payload": {"note": "different"}},
            {"input": {}, "input_payload": valid_input},
            {"input": valid_input, "input_payload": {}},
            {"input": {}, "input_payload": {}},
        )
        for index, envelopes in enumerate(ambiguous_cases):
            with self.subTest(case="ambiguous", index=index, envelopes=envelopes):
                result = submit(envelopes, suffix=f"ambiguous:{index}")
                self.assertEqual(result.get("status"), "invalid", result)
                self.assertEqual(result.get("reason"), "action_request_input_alias_ambiguous", result)
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

        for input_field in ("input", "input_payload"):
            with self.subTest(case="single-empty", input_field=input_field):
                result = submit({input_field: {}}, suffix=f"single-empty:{input_field}")
                self.assertEqual(result.get("status"), "invalid", result)
                self.assertTrue(
                    str(result.get("reason") or "").startswith("action_request_schema_validation_failed:"),
                    result,
                )
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

        alias_only = submit({"input_payload": valid_input}, suffix="alias-only")
        self.assertEqual(alias_only.get("status"), "queued", alias_only)
        self.assertFalse(alias_only.get("idempotent_replay"), alias_only)
        self.assertEqual(alias_only["action"]["input"], valid_input)

    def test_post_effect_replay_uses_stable_request_identity_with_and_without_explicit_key(self) -> None:
        self._seed_record("post-effect")

        def execute(submission: dict[str, Any]) -> None:
            dispatched = self.orchestrator.dispatch_operation_run_api(
                submission["operation_run"]["operation_run_id"],
                {"actor": "alice"},
            )
            self.assertEqual(dispatched.get("status"), "planned", dispatched)
            drained = self.orchestrator._drain_crm_writer_commands(  # noqa: SLF001
                {
                    "workflow_run_id": dispatched["workflow_command"]["workflow_run_id"],
                    "command_limit": 1,
                }
            )
            self.assertEqual(drained.get("completed_count"), 1, drained)

        explicit = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="post-effect",
            idempotency_key="post-effect:explicit",
        )
        self.assertFalse(explicit.get("idempotent_replay"), explicit)
        first_version = explicit["action"]["target_ref"]["crm_version"]
        execute(explicit)
        self.assertGreater(self.store.get_crm_record("post-effect")["crm_version"], first_version)
        before_explicit_replay = self._table_state(FULL_ZERO_WRITE_TABLES)
        explicit_replay = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="post-effect",
            idempotency_key="post-effect:explicit",
        )
        self.assertEqual(explicit_replay.get("status"), "completed", explicit_replay)
        self.assertTrue(explicit_replay.get("idempotent_replay"), explicit_replay)
        self.assertEqual(explicit_replay["action"]["status"], "completed")
        self.assertEqual(explicit_replay["operation_run"]["status"], "completed")
        self.assertEqual(explicit_replay["action"]["action_id"], explicit["action"]["action_id"])
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_explicit_replay)

        implicit = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="post-effect",
            idempotency_key="",
        )
        self.assertFalse(implicit.get("idempotent_replay"), implicit)
        execute(implicit)
        before_implicit_replay = self._table_state(FULL_ZERO_WRITE_TABLES)
        implicit_replay = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="post-effect",
            idempotency_key="",
        )
        self.assertEqual(implicit_replay.get("status"), "completed", implicit_replay)
        self.assertTrue(implicit_replay.get("idempotent_replay"), implicit_replay)
        self.assertEqual(implicit_replay["action"]["status"], "completed")
        self.assertEqual(implicit_replay["operation_run"]["status"], "completed")
        self.assertEqual(implicit_replay["action"]["action_id"], implicit["action"]["action_id"])
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_implicit_replay)

    def test_replay_with_unknown_persisted_lifecycle_status_fails_closed_without_writes(self) -> None:
        for corrupted_record in ("action", "operation_run"):
            with self.subTest(corrupted_record=corrupted_record):
                record_id = f"unknown-status-{corrupted_record}"
                idempotency_key = f"unknown-status:{corrupted_record}"
                self._seed_record(record_id)
                submitted = self._submit_authenticated(
                    action_type=ACTION_ADD_CRM_NOTE,
                    crm_record_id=record_id,
                    idempotency_key=idempotency_key,
                )
                if corrupted_record == "action":
                    corrupted = self.store.repos.workflow_runtime.update_action_state(
                        submitted["action"]["action_id"],
                        status="future_action_status",
                    )
                else:
                    corrupted = self.store.repos.workflow_runtime.update_operation_state(
                        submitted["operation_run"]["operation_run_id"],
                        status="future_operation_status",
                    )
                self.assertTrue(str(corrupted.get("status") or "").startswith("future_"), corrupted)

                baseline = self._table_state(FULL_ZERO_WRITE_TABLES)
                replay = self._submit_authenticated(
                    action_type=ACTION_ADD_CRM_NOTE,
                    crm_record_id=record_id,
                    idempotency_key=idempotency_key,
                )
                self.assertEqual(replay.get("status"), "conflict", replay)
                self.assertEqual(replay.get("reason"), "operation_submission_current_status_invalid", replay)
                if corrupted_record == "operation_run":
                    self.assertNotIn("action", replay)
                    self.assertEqual(
                        replay["operation_run"].get("operation_run_id"),
                        submitted["operation_run"].get("operation_run_id"),
                    )
                    self.assertEqual(replay["operation_run"].get("status"), "future_operation_status")
                else:
                    self.assertNotIn("operation_run", replay)
                    self.assertEqual(replay["action"].get("action_id"), submitted["action"].get("action_id"))
                    self.assertEqual(replay["action"].get("status"), "future_action_status")
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

    def test_replay_with_incoherent_stable_action_state_fails_closed_without_writes(self) -> None:
        missing_run_statuses = ("planned", "running", "completed", "failed", "cancelled")
        for action_status in missing_run_statuses:
            with self.subTest(case="missing-run", action_status=action_status):
                record_id = f"incoherent-missing-{action_status}"
                idempotency_key = f"incoherent:missing:{action_status}"
                self._seed_record(record_id)
                submitted = self._submit_authenticated(
                    action_type=ACTION_ADD_CRM_NOTE,
                    crm_record_id=record_id,
                    idempotency_key=idempotency_key,
                )
                action = self.store.repos.workflow_runtime.update_action_state(
                    submitted["action"]["action_id"],
                    status=action_status,
                )
                self.assertEqual(action.get("status"), action_status, action)
                self._delete_operation_run(submitted["operation_run"]["operation_run_id"])
                baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

                replay = self._submit_authenticated(
                    action_type=ACTION_ADD_CRM_NOTE,
                    crm_record_id=record_id,
                    idempotency_key=idempotency_key,
                )

                self.assertEqual(replay.get("status"), "conflict", replay)
                self.assertEqual(replay.get("reason"), "operation_submission_state_incoherent", replay)
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

        for action_status in ("completed", "failed", "cancelled", "rejected"):
            with self.subTest(case="terminal-run-mismatch", action_status=action_status):
                record_id = f"incoherent-mismatch-{action_status}"
                idempotency_key = f"incoherent:mismatch:{action_status}"
                self._seed_record(record_id)
                submitted = self._submit_authenticated(
                    action_type=ACTION_ADD_CRM_NOTE,
                    crm_record_id=record_id,
                    idempotency_key=idempotency_key,
                )
                action = self.store.repos.workflow_runtime.update_action_state(
                    submitted["action"]["action_id"],
                    status=action_status,
                    approval_status="rejected" if action_status == "rejected" else "",
                )
                self.assertEqual(action.get("status"), action_status, action)
                self.assertEqual(submitted["operation_run"].get("status"), "queued", submitted)
                baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

                replay = self._submit_authenticated(
                    action_type=ACTION_ADD_CRM_NOTE,
                    crm_record_id=record_id,
                    idempotency_key=idempotency_key,
                )

                self.assertEqual(replay.get("status"), "conflict", replay)
                self.assertEqual(replay.get("reason"), "operation_submission_state_incoherent", replay)
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

        for action_status in ("cancelled", "rejected"):
            with self.subTest(case="action-only-rejection", action_status=action_status):
                record_id = f"stable-rejection-{action_status}"
                idempotency_key = f"stable:rejection:{action_status}"
                self._seed_record(record_id)
                submitted = self._submit_authenticated(
                    action_type=ACTION_ADD_CRM_NOTE,
                    crm_record_id=record_id,
                    idempotency_key=idempotency_key,
                )
                action = self.store.repos.workflow_runtime.update_action_state(
                    submitted["action"]["action_id"],
                    status=action_status,
                    approval_status="rejected",
                )
                self.assertEqual(action.get("status"), action_status, action)
                self._delete_operation_run(submitted["operation_run"]["operation_run_id"])
                baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

                replay = self._submit_authenticated(
                    action_type=ACTION_ADD_CRM_NOTE,
                    crm_record_id=record_id,
                    idempotency_key=idempotency_key,
                )

                self.assertEqual(replay.get("status"), action_status, replay)
                self.assertTrue(replay.get("idempotent_replay"), replay)
                self.assertFalse(replay.get("operation_run"), replay)
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

    def test_non_fresh_committed_status_marks_replay_when_insert_race_outcome_is_unavailable(self) -> None:
        payload = {
            "action_type": ACTION_EXTERNAL_INTAKE,
            "workspace_id": "user-alice",
            "target_ref": {},
            "input": {},
            "idempotency_key": "submission-race:terminal",
            "actor": "alice",
        }
        submitted = self.orchestrator.submit_operation_action(payload)
        action = self.store.repos.workflow_runtime.update_action_state(
            submitted["action"]["action_id"],
            status="completed",
        )
        operation_run = self.store.repos.workflow_runtime.update_operation_state(
            submitted["operation_run"]["operation_run_id"],
            status="completed",
        )
        self.assertEqual(action.get("status"), "completed", action)
        self.assertEqual(operation_run.get("status"), "completed", operation_run)

        with mock.patch.object(
            self.orchestrator.operation_runtime_writer,
            "submit_action",
            return_value=OperationSubmissionResult(
                action=action,
                operation_run=operation_run,
                replayed=False,
            ),
        ):
            terminal_race = self.orchestrator.submit_operation_action(payload)

        self.assertEqual(terminal_race.get("status"), "completed", terminal_race)
        self.assertTrue(terminal_race.get("idempotent_replay"), terminal_race)

    def test_queued_nonapproval_partial_submission_repairs_only_the_deterministic_run(self) -> None:
        self._seed_record("queued-repair")
        submitted = self._submit_authenticated(
            action_type=ACTION_ADD_CRM_NOTE,
            crm_record_id="queued-repair",
            idempotency_key="queued-repair:deterministic",
        )
        action_id = submitted["action"]["action_id"]
        operation_run_id = submitted["operation_run"]["operation_run_id"]
        self._delete_operation_run(operation_run_id)
        domain_baseline = self._table_state(CRM_DOMAIN_TABLES)

        repaired = self._submit_authenticated(
            action_type=ACTION_ADD_CRM_NOTE,
            crm_record_id="queued-repair",
            idempotency_key="queued-repair:deterministic",
        )

        self.assertEqual(repaired.get("status"), "queued", repaired)
        self.assertTrue(repaired.get("idempotent_replay"), repaired)
        self.assertEqual(repaired["action"].get("action_id"), action_id)
        self.assertEqual(repaired["operation_run"].get("operation_run_id"), operation_run_id)
        self.assertEqual(repaired["operation_run"].get("status"), "queued")
        operation_rows = self._table_state(("operation_runs",))["operation_runs"]
        self.assertEqual(len(operation_rows), 1, operation_rows)
        self.assertIn(operation_run_id, operation_rows[0])
        self.assertEqual(self._table_state(CRM_DOMAIN_TABLES), domain_baseline)

    def test_approved_action_replay_reads_existing_run_and_rejects_unknown_run_status_pre_write(self) -> None:
        payload = {
            "action_type": ACTION_FETCH_PROFILE_SAMPLE,
            "workspace_id": "user-alice",
            "target_ref": {},
            "input": {},
            "budget": {"max_cost_usd": 1.0},
            "idempotency_key": "approval-replay:existing-run",
            "actor": "alice",
        }
        submitted = self.orchestrator.submit_operation_action(payload)
        self.assertEqual(submitted.get("status"), "approval_required", submitted)
        self.assertFalse(submitted.get("operation_run"), submitted)
        pending_replay = self.orchestrator.submit_operation_action(payload)
        self.assertEqual(pending_replay.get("status"), "approval_required", pending_replay)
        self.assertTrue(pending_replay.get("idempotent_replay"), pending_replay)
        self.assertFalse(pending_replay.get("operation_run"), pending_replay)
        after_compatibility_observation = self._table_state(FULL_ZERO_WRITE_TABLES)
        pending_replay_again = self.orchestrator.submit_operation_action(payload)
        self.assertEqual(pending_replay_again.get("status"), "approval_required", pending_replay_again)
        self.assertTrue(pending_replay_again.get("idempotent_replay"), pending_replay_again)
        self.assertFalse(pending_replay_again.get("operation_run"), pending_replay_again)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), after_compatibility_observation)

        approved = self.orchestrator.approve_operation_action_api(
            submitted["action"]["action_id"],
            {"actor": "alice"},
        )
        self.assertEqual(approved.get("status"), "queued", approved)
        operation_run_id = approved["operation_run"]["operation_run_id"]
        running = self.store.repos.workflow_runtime.update_operation_state(
            operation_run_id,
            status="running",
        )
        self.assertEqual(running.get("status"), "running", running)

        running_replay = self.orchestrator.submit_operation_action(payload)
        self.assertEqual(running_replay.get("status"), "running", running_replay)
        self.assertTrue(running_replay.get("idempotent_replay"), running_replay)
        self.assertEqual(running_replay["operation_run"]["operation_run_id"], operation_run_id)
        self.assertEqual(running_replay["operation_run"]["status"], "running")

        corrupted = self.store.repos.workflow_runtime.update_operation_state(
            operation_run_id,
            status="future_operation_status",
        )
        self.assertEqual(corrupted.get("status"), "future_operation_status", corrupted)
        baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

        rejected_replay = self.orchestrator.submit_operation_action(payload)
        self.assertEqual(rejected_replay.get("status"), "conflict", rejected_replay)
        self.assertEqual(
            rejected_replay.get("reason"),
            "operation_submission_current_status_invalid",
            rejected_replay,
        )
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

        restored = self.store.repos.workflow_runtime.update_operation_state(
            operation_run_id,
            status="running",
        )
        self.assertEqual(restored.get("status"), "running", restored)
        self._delete_operation_run(operation_run_id)
        missing_run_baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

        missing_run_replay = self.orchestrator.submit_operation_action(payload)

        self.assertEqual(missing_run_replay.get("status"), "conflict", missing_run_replay)
        self.assertEqual(
            missing_run_replay.get("reason"),
            "operation_submission_state_incoherent",
            missing_run_replay,
        )
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), missing_run_baseline)

    def test_approval_submission_rejects_orphan_and_unapproved_runs_before_writes(self) -> None:
        def approval_payload(suffix: str) -> dict[str, Any]:
            return {
                "action_type": ACTION_FETCH_PROFILE_SAMPLE,
                "workspace_id": "user-alice",
                "target_ref": {},
                "input": {},
                "budget": {"max_cost_usd": 1.0},
                "idempotency_key": f"approval-coherence:{suffix}",
                "actor": "alice",
            }

        orphan_payload = approval_payload("orphan-run")
        orphan_submission = self.orchestrator.submit_operation_action(orphan_payload)
        orphan_approval = self.orchestrator.approve_operation_action_api(
            orphan_submission["action"]["action_id"],
            {"actor": "alice"},
        )
        orphan_run_id = orphan_approval["operation_run"]["operation_run_id"]
        self._delete_action(orphan_submission["action"]["action_id"])
        orphan_baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

        orphan_replay = self.orchestrator.submit_operation_action(orphan_payload)

        self.assertEqual(orphan_replay.get("status"), "conflict", orphan_replay)
        self.assertEqual(orphan_replay.get("reason"), "operation_submission_state_incoherent", orphan_replay)
        self.assertNotIn("action", orphan_replay)
        self.assertEqual(orphan_replay["operation_run"].get("operation_run_id"), orphan_run_id)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), orphan_baseline)

        for operation_status in ("queued", "completed"):
            with self.subTest(case="unapproved-run", operation_status=operation_status):
                payload = approval_payload(f"unapproved-run:{operation_status}")
                submitted = self.orchestrator.submit_operation_action(payload)
                approved = self.orchestrator.approve_operation_action_api(
                    submitted["action"]["action_id"],
                    {"actor": "alice"},
                )
                action = self.store.repos.workflow_runtime.update_action_state(
                    submitted["action"]["action_id"],
                    status="approval_required",
                    approval_status="required",
                )
                self.assertEqual(action.get("status"), "approval_required", action)
                self.assertEqual(action.get("approval_status"), "required", action)
                if operation_status != "queued":
                    operation = self.store.repos.workflow_runtime.update_operation_state(
                        approved["operation_run"]["operation_run_id"],
                        status=operation_status,
                    )
                    self.assertEqual(operation.get("status"), operation_status, operation)
                baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

                replay = self.orchestrator.submit_operation_action(payload)

                self.assertEqual(replay.get("status"), "conflict", replay)
                self.assertEqual(replay.get("reason"), "operation_submission_state_incoherent", replay)
                self.assertEqual(replay["action"].get("action_id"), submitted["action"]["action_id"])
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

    def test_dynamic_approval_writer_preserves_cancel_and_retry_replay_states(self) -> None:
        self._seed_record("dynamic-approval")
        submitted = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="dynamic-approval",
            idempotency_key="dynamic-approval:closed-states",
            input_payload={"stage": "archived"},
        )
        action_id = submitted["action"]["action_id"]
        operation_run_id = submitted["operation_run"]["operation_run_id"]
        pending_dispatch = self.orchestrator.dispatch_operation_run_api(
            operation_run_id,
            {"actor": "alice"},
        )
        self.assertEqual(pending_dispatch.get("status"), "approval_required", pending_dispatch)
        pending = pending_dispatch["action"]
        self.assertEqual(pending.get("status"), "approval_required", pending)
        self.assertEqual(pending.get("approval_status"), "required", pending)
        self.assertEqual(pending_dispatch["operation_run"].get("status"), "queued")
        pending_baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

        pending_replay = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="dynamic-approval",
            idempotency_key="dynamic-approval:closed-states",
            input_payload={"stage": "archived"},
        )

        self.assertEqual(pending_replay.get("status"), "queued", pending_replay)
        self.assertTrue(pending_replay.get("idempotent_replay"), pending_replay)
        self.assertEqual(pending_replay["action"].get("status"), "approval_required")
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), pending_baseline)

        cancelled = self.orchestrator.cancel_operation_run_api(
            operation_run_id,
            {"actor": "alice", "reason": "dynamic approval cancelled"},
        )
        self.assertEqual(cancelled.get("status"), "cancelled", cancelled)
        self.assertEqual(cancelled["operation_run"].get("status"), "cancelled")
        cancelled_action = self.store.repos.workflow_runtime.get_action(action_id)
        self.assertEqual(cancelled_action.get("status"), "cancelled", cancelled_action)
        self.assertEqual(cancelled_action.get("approval_status"), "required", cancelled_action)
        cancelled_baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

        cancelled_replay = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="dynamic-approval",
            idempotency_key="dynamic-approval:closed-states",
            input_payload={"stage": "archived"},
        )

        self.assertEqual(cancelled_replay.get("status"), "cancelled", cancelled_replay)
        self.assertTrue(cancelled_replay.get("idempotent_replay"), cancelled_replay)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), cancelled_baseline)

        retried = self.orchestrator.retry_operation_run_api(
            operation_run_id,
            {"actor": "alice", "reason": "dynamic approval retry"},
        )
        self.assertEqual(retried.get("status"), "queued", retried)
        self.assertEqual(retried["operation_run"].get("status"), "queued")
        retry_baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

        retry_replay = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="dynamic-approval",
            idempotency_key="dynamic-approval:closed-states",
            input_payload={"stage": "archived"},
        )

        self.assertEqual(retry_replay.get("status"), "cancelled", retry_replay)
        self.assertTrue(retry_replay.get("idempotent_replay"), retry_replay)
        self.assertEqual(retry_replay["action"].get("status"), "queued")
        self.assertEqual(retry_replay["action"].get("approval_status"), "required")
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), retry_baseline)

    def test_command_fence_uses_canonical_linked_action_not_mutable_payload_label(self) -> None:
        self._seed_record("command-discriminator")
        submitted = self._submit_authenticated(
            action_type=ACTION_ADD_CRM_NOTE,
            crm_record_id="command-discriminator",
            idempotency_key="command:discriminator",
        )
        dispatched = self.orchestrator.dispatch_operation_run_api(
            submitted["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        self.assertEqual(dispatched.get("status"), "planned", dispatched)
        command = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        self.assertTrue(command)
        preflight_baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

        physical_only = {**command, "payload": dict(command["payload"])}
        physical_only["payload"].pop("operation_run_id", None)
        payload_only = {**command, "operation_id": "", "payload": dict(command["payload"])}
        both_absent = {**command, "operation_id": "", "payload": dict(command["payload"])}
        both_absent["payload"].pop("operation_run_id", None)
        for carrier, variant in (
            ("physical", physical_only),
            ("payload", payload_only),
            ("both", command),
            ("legacy_absent", both_absent),
        ):
            with self.subTest(valid_carrier=carrier):
                preflight = self.orchestrator._revalidate_crm_existing_record_command_target(  # noqa: SLF001
                    command=variant
                )
                self.assertEqual(preflight.get("status"), "ready", preflight)
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), preflight_baseline)

        variants: list[tuple[dict[str, Any], str]] = []
        missing_label = {**command, "payload": dict(command["payload"])}
        missing_label["payload"].pop("action_type", None)
        variants.append((missing_label, "crm_record_command_action_mismatch"))
        wrong_label = {**command, "payload": {**dict(command["payload"]), "action_type": "add_to_crm"}}
        variants.append((wrong_label, "crm_record_command_action_mismatch"))
        wrong_action = {**command, "payload": {**dict(command["payload"]), "action_id": "foreign-action"}}
        variants.append((wrong_action, "crm_record_command_action_mismatch"))
        wrong_target = {**command, "payload": {**dict(command["payload"])}}
        wrong_target["payload"]["crm_record_target"] = {
            **dict(wrong_target["payload"]["crm_record_target"]),
            "crm_record_id": "foreign-record",
        }
        wrong_target["payload"]["crm_record_ids"] = ["foreign-record"]
        variants.append((wrong_target, "crm_record_bound_target_mismatch"))
        non_target_action = self.orchestrator.submit_operation_action(
            {
                "action_type": ACTION_EXTERNAL_INTAKE,
                "workspace_id": "user-alice",
                "target_ref": {},
                "input": {},
                "idempotency_key": "command:non-target-action",
                "actor": "alice",
            }
        )
        wrong_canonical_action = {
            **command,
            "operation_id": non_target_action["operation_run"]["operation_run_id"],
            "payload": {
                **dict(command["payload"]),
                "operation_run_id": non_target_action["operation_run"]["operation_run_id"],
                "action_id": non_target_action["action"]["action_id"],
                "action_type": ACTION_EXTERNAL_INTAKE,
            },
        }
        variants.append((wrong_canonical_action, "crm_record_command_action_mismatch"))

        dangling_labels = (
            ("exact", str(command["payload"].get("action_type") or "")),
            ("missing", None),
            ("wrong", "add_to_crm"),
        )
        for carrier in ("physical", "payload", "both"):
            for _label_name, action_type in dangling_labels:
                payload = dict(command["payload"])
                if action_type is None:
                    payload.pop("action_type", None)
                else:
                    payload["action_type"] = action_type
                if carrier == "physical":
                    payload.pop("operation_run_id", None)
                else:
                    payload["operation_run_id"] = "missing-operation"
                variants.append(
                    (
                        {
                            **command,
                            "operation_id": "missing-operation" if carrier != "payload" else "",
                            "payload": payload,
                        },
                        "crm_record_command_operation_missing",
                    )
                )

        mismatch_payload = dict(command["payload"])
        mismatch_payload["operation_run_id"] = "missing-operation-b"
        variants.append(
            (
                {
                    **command,
                    "operation_id": "missing-operation-a",
                    "payload": mismatch_payload,
                },
                "crm_record_command_operation_mismatch",
            )
        )
        valid_operation_id = str(command["operation_id"])
        for physical_id, payload_id in (
            (valid_operation_id, "missing-operation"),
            ("missing-operation", valid_operation_id),
        ):
            unequal_payload = dict(command["payload"])
            unequal_payload["operation_run_id"] = payload_id
            variants.append(
                (
                    {
                        **command,
                        "operation_id": physical_id,
                        "payload": unequal_payload,
                    },
                    "crm_record_command_operation_mismatch",
                )
            )

        zero_write_baseline = self._table_state(FULL_ZERO_WRITE_TABLES)
        for index, (variant, expected_reason) in enumerate(variants):
            with self.subTest(variant=index):
                result = self.orchestrator._execute_crm_writer_command_payload(variant)  # noqa: SLF001
                self.assertEqual(result.get("status"), "invalid", result)
                self.assertEqual(result.get("reason"), expected_reason, result)
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), zero_write_baseline)

    def test_dispatch_owner_loss_and_version_staleness_are_full_zero_write(self) -> None:
        owner_record = self._seed_record("dispatch-owner-loss")
        stale_record = self._seed_record("dispatch-stale")
        owner_submission = self._submit_authenticated(
            action_type=ACTION_ADD_CRM_NOTE,
            crm_record_id="dispatch-owner-loss",
            idempotency_key="dispatch:owner-loss",
        )
        stale_submission = self._submit_authenticated(
            action_type=ACTION_ADD_CRM_NOTE,
            crm_record_id="dispatch-stale",
            idempotency_key="dispatch:stale",
        )

        self.store.upsert_crm_record({**owner_record, "owner_user_id": "bob"})
        self.store.upsert_crm_record(stale_record)
        before_dispatch = self._table_state(FULL_ZERO_WRITE_TABLES)

        owner_loss = self.orchestrator.dispatch_operation_run_api(
            owner_submission["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        self._assert_not_found(owner_loss)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_dispatch)

        stale = self.orchestrator.dispatch_operation_run_api(
            stale_submission["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        self.assertEqual(stale.get("status"), "conflict", stale)
        self.assertEqual(stale.get("reason"), "crm_record_target_stale", stale)
        self.assertFalse(stale.get("module_state_mutated", False), stale)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_dispatch)

    def test_command_owner_rejects_post_plan_owner_and_version_drift_without_domain_or_delta_writes(self) -> None:
        planned: list[tuple[str, str, dict[str, Any], str]] = []
        for action_type in CRM_EXISTING_RECORD_ACTION_TYPES:
            for drift_kind in ("owner", "version"):
                crm_record_id = f"command-{action_type}-{drift_kind}"
                seeded = self._seed_record(crm_record_id)
                submitted = self._submit_authenticated(
                    action_type=action_type,
                    crm_record_id=crm_record_id,
                    idempotency_key=f"command:{action_type}:{drift_kind}",
                )
                dispatched = self.orchestrator.dispatch_operation_run_api(
                    submitted["operation_run"]["operation_run_id"],
                    {"actor": "alice"},
                )
                self.assertEqual(dispatched.get("status"), "planned", dispatched)
                self.assertEqual(dispatched["workflow_command"]["owner"], "crm_writer")
                planned.append((action_type, drift_kind, dispatched["workflow_command"], crm_record_id))
                if drift_kind == "owner":
                    self.store.upsert_crm_record({**seeded, "owner_user_id": "bob"})
                else:
                    self.store.upsert_crm_record(seeded)

        guarded_tables = (*CRM_DOMAIN_TABLES, "workflow_entity_deltas")
        before_owner_execution = self._table_state(guarded_tables)
        for action_type, drift_kind, command, crm_record_id in planned:
            with self.subTest(action_type=action_type, drift_kind=drift_kind):
                drain = self.orchestrator._drain_crm_writer_commands(  # noqa: SLF001
                    {"workflow_run_id": command["workflow_run_id"], "command_limit": 1}
                )
                self.assertEqual(drain.get("completed_count"), 0, drain)
                self.assertEqual(drain.get("failed_count"), 1, drain)
                self.assertEqual(len(drain.get("items") or []), 1, drain)
                expected_reason = "crm_record_not_found" if drift_kind == "owner" else "crm_record_target_stale"
                self.assertEqual(drain["items"][0].get("reason"), expected_reason, drain)
                self.assertEqual(self._table_state(guarded_tables), before_owner_execution)
                self.assertEqual(
                    self.store.get_crm_record(crm_record_id)["owner_user_id"],
                    "bob" if drift_kind == "owner" else "alice",
                )


if __name__ == "__main__":
    unittest.main()
