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
    ACTION_SET_CRM_STAGE,
    CRM_EXISTING_RECORD_ACTION_TYPES,
    DEFAULT_ACTION_REGISTRY,
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


def test_production_registry_activates_exactly_three_schemas_and_still_serves_zero_tools() -> None:
    records = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    schema_defined = {
        action_type for action_type in records if DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema
    }

    assert len(records) == 15
    assert schema_defined == set(CRM_EXISTING_RECORD_ACTION_TYPES)
    assert len(schema_defined) == 3
    assert sum(not DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema for action_type in records) == 12
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
        first_version = explicit["action"]["target_ref"]["crm_version"]
        execute(explicit)
        self.assertGreater(self.store.get_crm_record("post-effect")["crm_version"], first_version)
        before_explicit_replay = self._table_state(FULL_ZERO_WRITE_TABLES)
        explicit_replay = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="post-effect",
            idempotency_key="post-effect:explicit",
        )
        self.assertEqual(explicit_replay.get("status"), "queued", explicit_replay)
        self.assertEqual(explicit_replay["action"]["action_id"], explicit["action"]["action_id"])
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_explicit_replay)

        implicit = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="post-effect",
            idempotency_key="",
        )
        execute(implicit)
        before_implicit_replay = self._table_state(FULL_ZERO_WRITE_TABLES)
        implicit_replay = self._submit_authenticated(
            action_type=ACTION_SET_CRM_STAGE,
            crm_record_id="post-effect",
            idempotency_key="",
        )
        self.assertEqual(implicit_replay.get("status"), "queued", implicit_replay)
        self.assertEqual(implicit_replay["action"]["action_id"], implicit["action"]["action_id"])
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_implicit_replay)

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
        baseline = self._table_state((*CRM_DOMAIN_TABLES, "workflow_entity_deltas"))

        variants = []
        missing_label = {**command, "payload": dict(command["payload"])}
        missing_label["payload"].pop("action_type", None)
        variants.append(missing_label)
        wrong_label = {**command, "payload": {**dict(command["payload"]), "action_type": "add_to_crm"}}
        variants.append(wrong_label)
        wrong_action = {**command, "payload": {**dict(command["payload"]), "action_id": "foreign-action"}}
        variants.append(wrong_action)
        wrong_target = {**command, "payload": {**dict(command["payload"])}}
        wrong_target["payload"]["crm_record_target"] = {
            **dict(wrong_target["payload"]["crm_record_target"]),
            "crm_record_id": "foreign-record",
        }
        wrong_target["payload"]["crm_record_ids"] = ["foreign-record"]
        variants.append(wrong_target)
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
        variants.append(wrong_canonical_action)

        for index, variant in enumerate(variants):
            with self.subTest(variant=index):
                result = self.orchestrator._execute_crm_writer_command_payload(variant)  # noqa: SLF001
                self.assertEqual(result.get("status"), "invalid", result)
                self.assertEqual(self._table_state((*CRM_DOMAIN_TABLES, "workflow_entity_deltas")), baseline)

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
