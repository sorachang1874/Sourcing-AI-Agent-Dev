from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from threading import Barrier
from typing import Any, Callable

from sourcing_agent import acquisition_start_v2_create_postgres as create_pg
from sourcing_agent import acquisition_start_v2_postgres as submit_pg
from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.acquisition_start_v2 import (
    ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    AcquisitionStartV2BindContext,
    AcquisitionStartV2OwnerBinder,
    AcquisitionStartV2ToolPins,
)
from sourcing_agent.agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence, AgentToolTerminalResult
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.operation_runtime import operation_run_id_for
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin, psycopg
from tests.test_d1n_acquisition_plan_preview_uow import _uow_kwargs

_APPROVAL = {
    "approval_actor_id": "human_1",
    "approval_actor_kind": "authenticated_user",
    "approval_policy_revision": "acquisition_confirmation_policy_v1",
}
_RESULT_ACCEPTANCE_HOLD_UNTIL = "9999-12-31 23:59:59"
_EFFECT_TABLES = (
    "agent_actions",
    "operation_runs",
    "acquisition_plan_previews",
    "operation_events",
    "agent_tool_result_slots",
    "workflow_events",
    "workflow_commands",
    "workflow_current_state",
    "runtime_outbox",
    "agent_tool_result_attempts",
    "agent_tool_result_journal",
    "acquisition_runs",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "acquisition_discovery_lanes",
    "plan_review_sessions",
    "crm_tasks",
    "company_assets",
    "company_evidence",
    "company_assertions",
)
_CREATE_FAULT_POINTS = (
    "after_receipt_event_write",
    "after_action_update",
    "after_operation_run_write",
    "after_workflow_started_event_write",
    "after_command_plan_requested_event_write",
    "after_command_write",
    "after_current_state_write",
    "after_planned_event_write",
)


class _RepositoryPreviewReader:
    def __init__(self, repository: Any) -> None:
        self._repository = repository

    def get_acquisition_plan_preview(self, preview_id: str, **owner: Any) -> dict[str, Any] | None:
        row = self._repository.get_acquisition_plan_preview(preview_id, **owner)
        preview = row.get("preview") if isinstance(row, dict) else None
        return dict(preview) if isinstance(preview, dict) else None


def _decode_physical_row(row: dict[str, Any]) -> dict[str, Any]:
    decoded = dict(row)
    for field, value in tuple(decoded.items()):
        if field.endswith("_json") and isinstance(value, str):
            decoded[field] = json.loads(value)
    return decoded


class D1nStartAcquisitionV2CreatePGMatrixTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1n_start_create_matrix"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(Path(self.tempdir.name) / "start-create-matrix.db")
        self.repository = self.store.repos.workflow_runtime
        self.adapter = self.store._control_plane_postgres  # noqa: SLF001

    def _schema_connection(self) -> tuple[Any, str]:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        return fixture, quote_control_plane_postgres_identifier(fixture.schema)

    def _table_counts(self) -> dict[str, int]:
        fixture, quoted_schema = self._schema_connection()
        assert psycopg is not None
        result: dict[str, int] = {}
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                for table_name in _EFFECT_TABLES:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {quoted_schema}.{quote_control_plane_postgres_identifier(table_name)}"
                    )
                    result[table_name] = int(cursor.fetchone()[0])
        return result

    def _table_snapshot(self) -> dict[str, tuple[str, ...]]:
        fixture, quoted_schema = self._schema_connection()
        assert psycopg is not None
        result: dict[str, tuple[str, ...]] = {}
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                for table_name in _EFFECT_TABLES:
                    quoted_table = quote_control_plane_postgres_identifier(table_name)
                    cursor.execute(
                        f"SELECT row_to_json(row_value)::text "
                        f"FROM {quoted_schema}.{quoted_table} AS row_value ORDER BY 1"
                    )
                    result[table_name] = tuple(str(row[0]) for row in cursor.fetchall())
        return result

    def _rows(self, table_name: str, *, where: str = "", params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        fixture, quoted_schema = self._schema_connection()
        assert psycopg is not None
        quoted_table = quote_control_plane_postgres_identifier(table_name)
        suffix = f" WHERE {where}" if where else ""
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT row_to_json(row_value) FROM {quoted_schema}.{quoted_table} AS row_value{suffix}",
                    params,
                )
                return [_decode_physical_row(dict(row[0])) for row in cursor.fetchall()]

    def _execute(self, sql: str, params: tuple[Any, ...]) -> None:
        fixture, quoted_schema = self._schema_connection()
        assert psycopg is not None
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.replace("{schema}", quoted_schema), params)
            connection.commit()

    def _execute_with_preview_mutation_trigger_disabled(self, sql: str, params: tuple[Any, ...]) -> None:
        fixture, quoted_schema = self._schema_connection()
        assert psycopg is not None
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                preview_table = f"{quoted_schema}.{quote_control_plane_postgres_identifier('acquisition_plan_previews')}"
                cursor.execute(f"ALTER TABLE {preview_table} DISABLE TRIGGER USER")
                try:
                    cursor.execute(sql.replace("{schema}", quoted_schema), params)
                finally:
                    cursor.execute(f"ALTER TABLE {preview_table} ENABLE TRIGGER USER")
            connection.commit()

    def _arrange_pending(self, *, suffix: str) -> AgentToolOccurrence:
        kwargs = _uow_kwargs(suffix=f"create_matrix_{suffix}")
        kwargs["start_request_schema_version"] = ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        kwargs["start_request_schema_digest"] = ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
        preview_bundle = self.repository.create_acquisition_plan_preview_uow(**kwargs)
        preview = dict(preview_bundle["preview"])
        reference = {
            "preview_id": preview["preview_id"],
            "preview_revision": preview["preview_revision"],
            "preview_digest": preview["preview_digest"],
        }
        bound = AcquisitionStartV2OwnerBinder(_RepositoryPreviewReader(self.repository)).bind(
            input_payload=reference,
            context=AcquisitionStartV2BindContext(
                workspace_id=kwargs["workspace_id"],
                requester_id=kwargs["requester_id"],
            ),
            tool_pins=AcquisitionStartV2ToolPins(
                tool_spec_version=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version,
                tool_spec_digest=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_digest,
            ),
            now=datetime.now(timezone.utc),
        )
        occurrence = AgentToolOccurrence.from_tool_spec(
            result_slot_id=f"slot_start_create_matrix_{suffix}",
            slot_generation=1,
            workspace_id=kwargs["workspace_id"],
            actor_id=kwargs["requester_id"],
            runtime_namespace="isolated_local_canary",
            provider_mode="simulate",
            turn_id=f"turn_start_create_matrix_{suffix}",
            step_id=f"step_start_create_matrix_{suffix}",
            tool_spec=START_ACQUISITION_RUN_TOOL_SPEC,
            canonical_args=bound.to_record(),
            occurrence_ordinal=1,
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        submitted = submit_pg.submit_acquisition_start_v2_action_uow(self.adapter, occurrence=occurrence)
        self.assertEqual(submitted["outcome"], "submitted")
        return occurrence

    def _create(self, occurrence: AgentToolOccurrence, **overrides: Any) -> dict[str, Any]:
        options = {**_APPROVAL, **overrides}
        return create_pg.create_acquisition_start_v2_uow(self.adapter, occurrence=occurrence, **options)

    def _prepare_terminal(
        self,
        occurrence: AgentToolOccurrence,
        *,
        suffix: str,
    ) -> Any:
        return self.repository.prepare_start_acquisition_tool_result(
            occurrence=occurrence,
            result_attempt_id=f"attempt_start_create_matrix_{suffix}",
            provider_call_id=f"provider_start_create_matrix_{suffix}",
            tool_call_id=f"tool_start_create_matrix_{suffix}",
        )

    def _orchestrator(self) -> SourcingOrchestrator:
        runtime_dir = Path(self.tempdir.name) / "runtime"
        settings = AppSettings(
            project_root=runtime_dir,
            runtime_dir=runtime_dir,
            secrets_file=runtime_dir / "secrets.toml",
            db_path=runtime_dir / "control-plane.db",
            jobs_dir=runtime_dir / "jobs",
            company_assets_dir=runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        return SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, model_client),
        )

    def test_authoritative_pg_create_persists_one_exact_complete_bundle(self) -> None:
        occurrence = self._arrange_pending(suffix="complete")
        for table_name in (
            "operation_runs",
            "agent_actions",
            "operation_events",
            "agent_tool_result_slots",
            "acquisition_plan_previews",
            "workflow_events",
            "workflow_commands",
            "workflow_current_state",
        ):
            self.assertTrue(self.adapter.should_prefer_read(table_name), table_name)
            self.assertTrue(self.adapter.is_authoritative(table_name), table_name)

        bundle = self._create(occurrence)
        self.assertEqual(bundle["outcome"], "created")
        self.assertFalse(bundle["replayed"])

        binding = submit_pg.revalidate_acquisition_start_v2_occurrence(occurrence)
        action_id = binding.action_id
        operation_run_id = operation_run_id_for(
            action_id=action_id,
            operation_type="acquisition_run",
            idempotency_key=binding.start_idempotency,
        )
        action = self._rows("agent_actions", where="action_id = %s", params=(action_id,))[0]
        operation = self._rows("operation_runs", where="operation_run_id = %s", params=(operation_run_id,))[0]
        action_events = self._rows("operation_events", where="event_stream_id = %s", params=(action_id,))
        operation_events = self._rows("operation_events", where="event_stream_id = %s", params=(operation_run_id,))
        self.assertEqual(len(action_events), 2)
        self.assertEqual(len(operation_events), 1)
        receipt_event = next(event for event in action_events if event["sequence_number"] == 2)
        planned_event = operation_events[0]
        receipt = receipt_event["payload_json"]
        owner_result_ref = planned_event["payload_json"]["owner_result_ref"]
        owner_result_digest = planned_event["payload_json"]["owner_result_digest"]
        workflow_run_id = owner_result_ref["workflow_run_id"]
        command_id = owner_result_ref["workflow_command_id"]
        workflow_events = sorted(
            self._rows("workflow_events", where="workflow_run_id = %s", params=(workflow_run_id,)),
            key=lambda event: event["sequence_number"],
        )
        command = self._rows("workflow_commands", where="command_id = %s", params=(command_id,))[0]
        current_state = self._rows("workflow_current_state", where="workflow_run_id = %s", params=(workflow_run_id,))[0]

        self.assertEqual(action["status"], "queued")
        self.assertEqual(action["approval_status"], "approved")
        self.assertEqual(action["result_ref_json"], owner_result_ref)
        self.assertEqual(operation["action_id"], action_id)
        self.assertEqual(operation["status"], "queued")
        self.assertEqual(operation["result_ref_json"], owner_result_ref)
        self.assertEqual(action["budget_json"], receipt["budget"])
        self.assertEqual(operation["cost_budget_json"], receipt["budget"])

        self.assertEqual(receipt_event["event_type"], "ActionApproved")
        self.assertEqual(receipt_event["schema_version"], "acquisition_confirmation_receipt.v1")
        self.assertEqual(receipt["receipt_id"], receipt_event["event_id"])
        self.assertEqual(receipt["approval_actor_id"], _APPROVAL["approval_actor_id"])
        self.assertEqual(receipt["approval_actor_kind"], _APPROVAL["approval_actor_kind"])
        self.assertEqual(receipt["approval_policy_revision"], _APPROVAL["approval_policy_revision"])

        self.assertEqual(
            [event["event_type"] for event in workflow_events], ["WorkflowStarted", "CommandPlanRequested"]
        )
        self.assertEqual([event["sequence_number"] for event in workflow_events], [1, 2])
        self.assertTrue(all(event["actor"] == "operation_workflow_command_planner" for event in workflow_events))
        self.assertTrue(all(event["source"] == "operation_run_dispatch" for event in workflow_events))
        self.assertTrue(all(event["schema_version"] == "workflow_event_v1" for event in workflow_events))
        self.assertTrue(all(event["artifact_refs_json"] == [] for event in workflow_events))
        self.assertEqual(workflow_events[0]["payload_json"]["stage_key"], "acquisition_run_create")
        source_event = workflow_events[1]
        self.assertEqual(source_event["event_id"], owner_result_ref["command_source_event_id"])
        self.assertEqual(source_event["payload_json"]["command_type"], "acquisition.run.create")
        self.assertEqual(source_event["payload_json"]["max_attempts"], 5)
        self.assertEqual(
            source_event["payload_json"]["retry_policy"],
            {"kind": "operation_acquisition_run_create", "retry_delay_seconds": 30},
        )
        self.assertEqual(source_event["payload_json"]["not_before_at"], _RESULT_ACCEPTANCE_HOLD_UNTIL)

        self.assertEqual(command["status"], "queued")
        self.assertEqual(command["command_type"], "acquisition.run.create")
        self.assertEqual(command["owner"], "acquisition_run_writer")
        self.assertEqual(command["source_event_id"], source_event["event_id"])
        self.assertEqual(command["source_event_type"], "CommandPlanRequested")
        self.assertEqual(command["payload_json"], source_event["payload_json"]["payload"])
        self.assertEqual(command["not_before_at"], _RESULT_ACCEPTANCE_HOLD_UNTIL)
        self.assertEqual(command["max_attempts"], 5)
        self.assertEqual(command["retry_policy_json"], source_event["payload_json"]["retry_policy"])

        self.assertEqual(current_state["schema_version"], "workflow_current_state_v1")
        self.assertEqual(current_state["operation_id"], operation_run_id)
        self.assertEqual(current_state["workflow_type"], "agent_callable_workflow_command")
        self.assertEqual(current_state["status"], "running")
        self.assertEqual(current_state["current_stage_key"], "acquisition_run_create")
        self.assertEqual(current_state["completion_proofs_json"], {})
        self.assertEqual(
            current_state["active_command_counts_json"],
            {"acquisition_run_writer": {"acquisition.run.create": 1}},
        )
        self.assertEqual(current_state["terminal_command_counts_json"], {})
        self.assertEqual(current_state["read_model_pointers_json"], {})
        self.assertEqual(current_state["migration_status_json"], {})
        self.assertEqual(current_state["last_processed_sequence_number"], 2)
        self.assertEqual(current_state["reducer_version"], "durable_runtime_reducer_v1")
        self.assertEqual(current_state["metadata_json"], {})

        self.assertEqual(planned_event["event_type"], "OperationCommandPlanned")
        self.assertEqual(planned_event["sequence_number"], 1)
        self.assertEqual(planned_event["schema_version"], "acquisition_start_command_acceptance.v1")
        self.assertEqual(planned_event["event_id"], owner_result_ref["terminal_winner_id"])
        self.assertEqual(owner_result_ref["terminal_winner_sequence_number"], 1)
        self.assertEqual(owner_result_ref["command_source_event_sequence_number"], 2)
        self.assertEqual(
            owner_result_ref["confirmation_receipt_ref"],
            {
                "receipt_id": receipt["receipt_id"],
                "receipt_digest": receipt["receipt_digest"],
            },
        )
        self.assertEqual(bundle["confirmation_receipt"], receipt)
        self.assertEqual(bundle["owner_result_ref"], owner_result_ref)
        self.assertEqual(bundle["owner_result_digest"], owner_result_digest)

        create_timestamp = receipt["approved_at"]
        for actual in (
            action["updated_at"],
            operation["created_at"],
            operation["updated_at"],
            receipt_event["occurred_at"],
            receipt_event["recorded_at"],
            receipt_event["created_at"],
            workflow_events[0]["occurred_at"],
            workflow_events[0]["recorded_at"],
            workflow_events[0]["created_at"],
            workflow_events[1]["occurred_at"],
            workflow_events[1]["recorded_at"],
            workflow_events[1]["created_at"],
            command["created_at"],
            command["updated_at"],
            current_state["created_at"],
            current_state["updated_at"],
            planned_event["occurred_at"],
            planned_event["recorded_at"],
            planned_event["created_at"],
        ):
            self.assertEqual(actual, create_timestamp)

        counts = self._table_counts()
        self.assertEqual(counts["agent_actions"], 2)
        self.assertEqual(counts["operation_runs"], 2)
        self.assertEqual(counts["operation_events"], 4)
        self.assertEqual(counts["workflow_events"], 2)
        self.assertEqual(counts["workflow_commands"], 1)
        self.assertEqual(counts["workflow_current_state"], 1)
        for table_name in (
            "runtime_outbox",
            "agent_tool_result_attempts",
            "agent_tool_result_journal",
            "acquisition_runs",
            "workflow_activity_runs",
            "workflow_activity_attempts",
            "workflow_entity_deltas",
            "acquisition_discovery_lanes",
            "plan_review_sessions",
            "crm_tasks",
            "company_assets",
            "company_evidence",
            "company_assertions",
        ):
            self.assertEqual(counts[table_name], 0, table_name)

    def test_every_precommit_fault_rolls_back_the_complete_effect_surface(self) -> None:
        occurrence = self._arrange_pending(suffix="rollback")
        baseline_counts = self._table_counts()
        baseline_snapshot = self._table_snapshot()

        for fault_point in _CREATE_FAULT_POINTS:
            with self.subTest(fault_point=fault_point):
                with self.assertRaisesRegex(RuntimeError, "injected acquisition start create fault"):
                    self._create(occurrence, fault_injection_point=fault_point)
                self.assertEqual(self._table_counts(), baseline_counts)
                self.assertEqual(self._table_snapshot(), baseline_snapshot)

    def test_prepare_start_result_rebuilds_exact_terminal_without_writes(self) -> None:
        occurrence = self._arrange_pending(suffix="prepare_result")
        bundle = self._create(occurrence)
        baseline_counts = self._table_counts()
        terminal = self._prepare_terminal(occurrence, suffix="prepare_result")

        self.assertEqual(self._table_counts(), baseline_counts)
        self.assertFalse(terminal.is_error)
        self.assertEqual(terminal.action_id, bundle["action"]["action_id"])
        self.assertEqual(terminal.operation_run_id, bundle["operation_run"]["operation_run_id"])
        self.assertEqual(terminal.workflow_command_id, bundle["workflow_command"]["command_id"])
        self.assertEqual(terminal.owner_target_kind, "acquisition_start_command_acceptance_v1")
        self.assertEqual(terminal.owner_target_id, bundle["workflow_command"]["command_id"])
        self.assertEqual(terminal.owner_target_revision, 1)
        self.assertEqual(terminal.owner_target_generation, 0)
        self.assertEqual(terminal.owner_target_revision_token, "")
        self.assertEqual(terminal.terminal_winner_id, bundle["planned_event"]["event_id"])
        self.assertEqual(terminal.owner_result_ref, bundle["owner_result_ref"])
        self.assertEqual(terminal.owner_result_digest, bundle["owner_result_digest"])
        self.assertEqual(
            terminal.serialized_result,
            {
                "variant": "success",
                "status": "accepted",
                "action_id": bundle["action"]["action_id"],
                "operation_run_id": bundle["operation_run"]["operation_run_id"],
                "workflow_command_id": bundle["workflow_command"]["command_id"],
                "preview_id": bundle["confirmation_receipt"]["preview_ref"]["preview_id"],
                "preview_revision": bundle["confirmation_receipt"]["preview_ref"]["preview_revision"],
                "preview_digest": bundle["confirmation_receipt"]["preview_ref"]["preview_digest"],
                "confirmation_receipt_id": bundle["confirmation_receipt"]["receipt_id"],
                "confirmation_receipt_digest": bundle["confirmation_receipt"]["receipt_digest"],
            },
        )

    def test_accept_start_result_journals_and_releases_dormant_command_without_outbox(self) -> None:
        occurrence = self._arrange_pending(suffix="accept_result")
        bundle = self._create(occurrence)
        terminal = self._prepare_terminal(occurrence, suffix="accept_result")
        command_id = bundle["workflow_command"]["command_id"]
        before = self._rows("workflow_commands", where="command_id = %s", params=(command_id,))[0]
        self.assertEqual(before["not_before_at"], _RESULT_ACCEPTANCE_HOLD_UNTIL)

        accepted = self.repository.accept_start_acquisition_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=occurrence.slot_generation,
        )

        self.assertEqual(accepted["outcome"], "accepted")
        self.assertFalse(accepted["replayed"])
        self.assertEqual(accepted["slot"]["status"], "accepted")
        self.assertEqual(accepted["attempt"]["disposition"], "accepted")
        self.assertEqual(accepted["journal"]["result_attempt_id"], terminal.result_attempt_id)
        self.assertEqual(accepted["released_workflow_command"]["command_id"], command_id)
        self.assertEqual(accepted["released_workflow_command"]["not_before_at"], "")
        self.assertIn("recovery_wakeup", accepted)
        after = self._rows("workflow_commands", where="command_id = %s", params=(command_id,))[0]
        self.assertEqual(after["not_before_at"], "")
        counts = self._table_counts()
        self.assertEqual(counts["agent_tool_result_attempts"], 1)
        self.assertEqual(counts["agent_tool_result_journal"], 1)
        self.assertEqual(counts["runtime_outbox"], 0)
        self.assertEqual(counts["acquisition_runs"], 0)

    def test_direct_accept_start_result_requires_authoritative_pg_before_writes(self) -> None:
        occurrence = self._arrange_pending(suffix="accept_non_authoritative")
        bundle = self._create(occurrence)
        terminal = self._prepare_terminal(occurrence, suffix="accept_non_authoritative")
        command_id = bundle["workflow_command"]["command_id"]
        baseline = self._table_snapshot()
        original_mode = self.adapter.mode

        try:
            self.adapter.mode = "prefer_postgres"
            self.assertTrue(self.adapter.should_prefer_read("agent_tool_result_slots"))
            self.assertFalse(self.adapter.is_authoritative("agent_tool_result_slots"))
            accepted = self.adapter.accept_start_acquisition_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=occurrence.slot_generation,
            )
        finally:
            self.adapter.mode = original_mode

        self.assertIsNone(accepted)
        self.assertEqual(self._table_snapshot(), baseline)
        after = self._rows("workflow_commands", where="command_id = %s", params=(command_id,))[0]
        self.assertEqual(after["not_before_at"], _RESULT_ACCEPTANCE_HOLD_UNTIL)

    def test_accept_start_result_rejects_forged_terminal_bytes_without_writes(self) -> None:
        occurrence = self._arrange_pending(suffix="forged_result")
        self._create(occurrence)
        terminal = self._prepare_terminal(occurrence, suffix="forged_result")
        forged = AgentToolTerminalResult.from_serialized_result(
            result_attempt_id=terminal.result_attempt_id,
            provider_call_id=terminal.provider_call_id,
            tool_call_id=terminal.tool_call_id,
            action_id=terminal.action_id,
            operation_run_id=terminal.operation_run_id,
            workflow_command_id=terminal.workflow_command_id,
            owner_target_kind=terminal.owner_target_kind,
            owner_target_id=terminal.owner_target_id,
            owner_target_revision=terminal.owner_target_revision,
            owner_target_generation=terminal.owner_target_generation,
            terminal_winner_id=terminal.terminal_winner_id,
            owner_result_ref=terminal.owner_result_ref,
            owner_result_digest=terminal.owner_result_digest,
            serialized_result={
                **terminal.serialized_result,
                "status": "accepted",
                "forged": True,
            },
            is_error=False,
        )
        baseline = self._table_snapshot()

        with self.assertRaisesRegex(RuntimeError, "serializer output mismatch"):
            self.repository.accept_start_acquisition_tool_result_uow(
                occurrence=occurrence,
                terminal=forged,
                attempted_slot_generation=occurrence.slot_generation,
            )

        self.assertEqual(self._table_snapshot(), baseline)

    def test_accept_start_result_revalidates_canonical_preview_owner_without_writes(self) -> None:
        mutations: tuple[tuple[str, str], ...] = (
            (
                "nested_company_identity",
                """
                UPDATE {schema}.acquisition_plan_previews
                SET canonical_company_id = %s,
                    preview_json = jsonb_set(
                        preview_json,
                        '{company_target,canonical_company_id}',
                        to_jsonb(%s::text),
                        false
                    )
                WHERE preview_id = %s
                """,
            ),
            (
                "expired_row_and_payload",
                """
                UPDATE {schema}.acquisition_plan_previews
                SET created_at = %s::timestamptz,
                    expires_at = %s::timestamptz,
                    preview_json = jsonb_set(
                        jsonb_set(preview_json, '{created_at}', to_jsonb(%s::text), false),
                        '{expires_at}',
                        to_jsonb(%s::text),
                        false
                    )
                WHERE preview_id = %s
                """,
            ),
            (
                "start_schema_pin",
                """
                UPDATE {schema}.acquisition_plan_previews
                SET start_request_schema_version = %s,
                    preview_json = jsonb_set(
                        preview_json,
                        '{schema_pins,intended_start_request_schema_version}',
                        to_jsonb(%s::text),
                        false
                    )
                WHERE preview_id = %s
                """,
            ),
        )

        for ordinal, (label, sql) in enumerate(mutations, start=1):
            with self.subTest(owner=label):
                occurrence = self._arrange_pending(suffix=f"accept_preview_owner_{ordinal}")
                bundle = self._create(occurrence)
                terminal = self._prepare_terminal(occurrence, suffix=f"accept_preview_owner_{ordinal}")
                preview_id = bundle["confirmation_receipt"]["preview_ref"]["preview_id"]
                if label == "expired_row_and_payload":
                    params = (
                        "2020-01-01T00:00:00Z",
                        "2020-01-01T01:00:00Z",
                        "2020-01-01T00:00:00Z",
                        "2020-01-01T01:00:00Z",
                        preview_id,
                    )
                else:
                    params = ("forged_owner", "forged_owner", preview_id)
                self._execute_with_preview_mutation_trigger_disabled(sql, params)
                baseline = self._table_snapshot()

                with self.assertRaisesRegex(RuntimeError, "acquisition_start_preview_not_found_or_conflict"):
                    self.repository.accept_start_acquisition_tool_result_uow(
                        occurrence=occurrence,
                        terminal=terminal,
                        attempted_slot_generation=occurrence.slot_generation,
                    )

                self.assertEqual(self._table_snapshot(), baseline)

    def test_accept_start_result_rejects_error_terminal_without_writes(self) -> None:
        occurrence = self._arrange_pending(suffix="forged_error")
        self._create(occurrence)
        terminal = self._prepare_terminal(occurrence, suffix="forged_error")
        forged = replace(terminal, is_error=True)
        baseline = self._table_snapshot()

        with self.assertRaisesRegex(RuntimeError, "serializer output mismatch"):
            self.repository.accept_start_acquisition_tool_result_uow(
                occurrence=occurrence,
                terminal=forged,
                attempted_slot_generation=occurrence.slot_generation,
            )

        self.assertEqual(self._table_snapshot(), baseline)

    def test_accept_start_result_exact_replay_and_late_attempt_quarantine_do_not_rewrite_owner(self) -> None:
        occurrence = self._arrange_pending(suffix="replay_result")
        self._create(occurrence)
        terminal = self._prepare_terminal(occurrence, suffix="replay_result")
        accepted = self.repository.accept_start_acquisition_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=occurrence.slot_generation,
        )
        self.assertEqual(accepted["outcome"], "accepted")
        baseline_snapshot = self._table_snapshot()

        replay = self.repository.accept_start_acquisition_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=occurrence.slot_generation,
        )
        self.assertEqual(replay["outcome"], "replayed")
        self.assertEqual(self._table_snapshot(), baseline_snapshot)

        late_terminal = self._prepare_terminal(occurrence, suffix="late_result")
        quarantined = self.repository.accept_start_acquisition_tool_result_uow(
            occurrence=occurrence,
            terminal=late_terminal,
            attempted_slot_generation=occurrence.slot_generation,
        )
        self.assertEqual(quarantined["outcome"], "quarantined")
        self.assertEqual(quarantined["attempt"]["disposition"], "quarantined")
        self.assertEqual(quarantined["attempt"]["quarantine_reason"], "terminal_winner_already_accepted")
        counts = self._table_counts()
        self.assertEqual(counts["agent_tool_result_attempts"], 2)
        self.assertEqual(counts["agent_tool_result_journal"], 1)
        self.assertEqual(counts["runtime_outbox"], 0)

    def test_accept_start_result_fault_rolls_back_journal_and_command_release(self) -> None:
        occurrence = self._arrange_pending(suffix="accept_fault")
        bundle = self._create(occurrence)
        terminal = self._prepare_terminal(occurrence, suffix="accept_fault")
        command_id = bundle["workflow_command"]["command_id"]
        baseline_snapshot = self._table_snapshot()

        with self.assertRaisesRegex(RuntimeError, "injected agent tool result fault after journal write"):
            self.adapter.accept_start_acquisition_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=occurrence.slot_generation,
                fault_injection_point="after_journal_write",
            )

        self.assertEqual(self._table_snapshot(), baseline_snapshot)
        command = self._rows("workflow_commands", where="command_id = %s", params=(command_id,))[0]
        self.assertEqual(command["not_before_at"], _RESULT_ACCEPTANCE_HOLD_UNTIL)

    def test_released_start_v2_root_command_drains_into_intent_command_without_live_provider(self) -> None:
        occurrence = self._arrange_pending(suffix="root_owner")
        bundle = self._create(occurrence)
        terminal = self._prepare_terminal(occurrence, suffix="root_owner")
        accepted = self.repository.accept_start_acquisition_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=occurrence.slot_generation,
        )
        self.assertEqual(accepted["released_workflow_command"]["not_before_at"], "")
        late_terminal = self._prepare_terminal(occurrence, suffix="late_after_root_owner")

        owner_result = self._orchestrator()._drain_acquisition_run_create_commands(  # noqa: SLF001
            {"acquisition_run_create_command_limit": 5}
        )

        self.assertEqual(owner_result["status"], "completed")
        self.assertEqual(owner_result["completed_count"], 1)
        command_id = bundle["workflow_command"]["command_id"]
        root_command = self.store.get_workflow_command(command_id)
        self.assertEqual(root_command["status"], "succeeded")
        self.assertTrue(root_command["result"]["operation_completion_deferred"])
        self.assertFalse(root_command["result"]["queue_workflow_called"])
        self.assertFalse(root_command["result"]["legacy_job_shell_created"])
        self.assertEqual(root_command["result"]["next_phase"], "W11b_acquisition_intent_plan_commands")
        self.assertEqual(root_command["result"]["downstream_command_count"], 1)
        child_command_id = root_command["result"]["downstream_command_ids"][0]
        child_command = self.store.get_workflow_command(child_command_id)
        self.assertEqual(child_command["command_type"], "acquisition.intent.resolve")
        self.assertEqual(child_command["owner"], "acquisition_planner")
        self.assertEqual(child_command["parent_command_id"], command_id)
        self.assertEqual(child_command["status"], "queued")
        self.assertEqual(
            child_command["payload"]["workflow_payload"]["schema_version"],
            "acquisition_start_v2_root_owner_compat_payload.v1",
        )
        self.assertEqual(child_command["payload"]["workflow_payload"]["target_company"], "Thinking Machines Lab")
        accepted_downstream_snapshot = self._table_snapshot()

        replay = self.repository.accept_start_acquisition_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=occurrence.slot_generation,
        )
        self.assertEqual(replay["outcome"], "replayed")
        self.assertEqual(self._table_snapshot(), accepted_downstream_snapshot)

        late_before_counts = self._table_counts()
        quarantined = self.repository.accept_start_acquisition_tool_result_uow(
            occurrence=occurrence,
            terminal=late_terminal,
            attempted_slot_generation=occurrence.slot_generation,
        )
        self.assertEqual(quarantined["outcome"], "quarantined")
        late_after_counts = self._table_counts()
        self.assertEqual(late_after_counts["agent_tool_result_attempts"], late_before_counts["agent_tool_result_attempts"] + 1)
        for table_name in (
            "runtime_outbox",
            "acquisition_runs",
            "workflow_activity_runs",
            "workflow_activity_attempts",
            "workflow_entity_deltas",
            "acquisition_discovery_lanes",
            "plan_review_sessions",
            "crm_tasks",
            "company_assets",
            "company_evidence",
            "company_assertions",
        ):
            self.assertEqual(late_after_counts[table_name], late_before_counts[table_name])
        self.assertEqual(
            child_command["payload"]["workflow_payload"]["cohort_selection"]["role_bucket_ids"],
            ["research", "engineering"],
        )
        counts = self._table_counts()
        self.assertEqual(counts["runtime_outbox"], 0)
        self.assertEqual(counts["acquisition_runs"], 0)

    def test_postcommit_lost_ack_exact_replay_preserves_the_committed_bundle(self) -> None:
        occurrence = self._arrange_pending(suffix="lost_ack")

        with self.assertRaisesRegex(RuntimeError, "injected acquisition start create fault after commit"):
            self._create(occurrence, fault_injection_point="after_commit")

        committed_snapshot = self._table_snapshot()
        replay = self._create(occurrence)
        self.assertEqual(replay["outcome"], "replayed")
        self.assertTrue(replay["replayed"])
        self.assertEqual(self._table_snapshot(), committed_snapshot)

    def test_result_acceptance_hold_keeps_root_dormant_and_exact_replayable(self) -> None:
        occurrence = self._arrange_pending(suffix="result_acceptance_hold")
        created = self._create(occurrence)
        command_id = created["owner_result_ref"]["workflow_command_id"]

        ready_ids = {
            str(command.get("command_id") or "")
            for command in self.store.list_ready_workflow_commands(
                owner="acquisition_run_writer",
                command_type="acquisition.run.create",
                limit=0,
            )
        }
        self.assertNotIn(command_id, ready_ids)

        committed_snapshot = self._table_snapshot()
        replay = self._create(occurrence)
        self.assertEqual(replay["outcome"], "replayed")
        self.assertTrue(replay["replayed"])
        self.assertEqual(self._table_snapshot(), committed_snapshot)

    def test_generic_operation_controls_reject_v2_create_without_writes(self) -> None:
        occurrence = self._arrange_pending(suffix="generic_control_closed")
        created = self._create(occurrence)
        owner_ref = created["owner_result_ref"]
        orchestrator = self._orchestrator()

        controls: tuple[tuple[str, Callable[[], dict[str, Any]]], ...] = (
            (
                "approve",
                lambda: orchestrator.approve_operation_action_api(
                    owner_ref["action_id"],
                    {"actor": "generic-control-test"},
                    expected_workspace_id=occurrence.workspace_id,
                ),
            ),
            (
                "reject",
                lambda: orchestrator.reject_operation_action_api(
                    owner_ref["action_id"],
                    {"actor": "generic-control-test", "reason": "not allowed"},
                    expected_workspace_id=occurrence.workspace_id,
                ),
            ),
            (
                "cancel",
                lambda: orchestrator.cancel_operation_run_api(
                    owner_ref["operation_run_id"],
                    {"actor": "generic-control-test", "reason": "not allowed"},
                    expected_workspace_id=occurrence.workspace_id,
                ),
            ),
            (
                "retry",
                lambda: orchestrator.retry_operation_run_api(
                    owner_ref["operation_run_id"],
                    {"actor": "generic-control-test", "idempotency_key": "retry-not-allowed"},
                    expected_workspace_id=occurrence.workspace_id,
                ),
            ),
            (
                "resume",
                lambda: orchestrator.resume_operation_run_api(
                    owner_ref["operation_run_id"],
                    {"actor": "generic-control-test"},
                    expected_workspace_id=occurrence.workspace_id,
                ),
            ),
            (
                "dispatch",
                lambda: orchestrator.dispatch_operation_run_api(
                    owner_ref["operation_run_id"],
                    {"actor": "generic-control-test"},
                    expected_workspace_id=occurrence.workspace_id,
                ),
            ),
        )

        detail = orchestrator.get_operation_run_api(
            owner_ref["operation_run_id"],
            expected_workspace_id=occurrence.workspace_id,
        )
        self.assertEqual(detail["status"], "ok")
        control_state = detail["operation_run"]["control_state"]
        self.assertFalse(control_state["can_dispatch"])
        self.assertFalse(control_state["can_resume"])
        self.assertFalse(control_state["can_retry"])
        self.assertFalse(control_state["can_cancel"])
        self.assertEqual(control_state["allowed_actions"], [])
        self.assertEqual(
            control_state["disabled_reasons"],
            {
                "dispatch": "acquisition_start_v2_generic_operation_control_not_enabled",
                "resume": "acquisition_start_v2_generic_operation_control_not_enabled",
                "retry": "acquisition_start_v2_generic_operation_control_not_enabled",
                "cancel": "acquisition_start_v2_generic_operation_control_not_enabled",
            },
        )

        for label, control in controls:
            with self.subTest(control=label):
                baseline = self._table_snapshot()
                response = control()
                self.assertEqual(response["status"], "unsupported")
                self.assertEqual(
                    response["reason"],
                    "acquisition_start_v2_generic_operation_control_not_enabled",
                )
                self.assertFalse(response["module_state_mutated"])
                if "control_state" in dict(response.get("operation_run") or {}):
                    self.assertEqual(response["operation_run"]["control_state"], control_state)
                self.assertEqual(self._table_snapshot(), baseline)

    def test_generic_operation_controls_fail_closed_on_start_v2_action_drift_without_writes(self) -> None:
        cases = (
            (
                "action_type_drift",
                "UPDATE {schema}.agent_actions SET action_type = %s WHERE action_id = %s",
                ("legacy_start_acquisition_run",),
            ),
            (
                "empty_schema_pair",
                "UPDATE {schema}.agent_actions SET request_schema_version = %s, request_schema_digest = %s "
                "WHERE action_id = %s",
                ("", ""),
            ),
            (
                "alternate_schema_pair",
                "UPDATE {schema}.agent_actions SET request_schema_version = %s, request_schema_digest = %s "
                "WHERE action_id = %s",
                ("start_acquisition_run_request.v999", "0" * 64),
            ),
        )
        for label, sql, params in cases:
            with self.subTest(case=label):
                occurrence = self._arrange_pending(suffix=f"generic_control_drift_{label}")
                created = self._create(occurrence)
                owner_ref = created["owner_result_ref"]
                self._execute(sql, (*params, owner_ref["action_id"]))
                orchestrator = self._orchestrator()
                baseline = self._table_snapshot()

                response = orchestrator.cancel_operation_run_api(
                    owner_ref["operation_run_id"],
                    {"actor": "generic-control-test", "reason": "not allowed"},
                    expected_workspace_id=occurrence.workspace_id,
                )

                self.assertEqual(response["status"], "invalid")
                self.assertEqual(
                    response["reason"],
                    "acquisition_start_v2_generic_operation_control_identity_mismatch",
                )
                self.assertFalse(response["module_state_mutated"])
                self.assertEqual(self._table_snapshot(), baseline)

    def test_generic_action_controls_fail_closed_on_partial_start_v2_pending_action_without_writes(self) -> None:
        cases = (
            (
                "schema_pair_only",
                "UPDATE {schema}.agent_actions SET input_json = %s WHERE action_id = %s",
                ('{"legacy_intent":"start"}',),
            ),
            (
                "preview_id_only",
                "UPDATE {schema}.agent_actions SET request_schema_version = %s, request_schema_digest = %s, "
                "input_json = %s WHERE action_id = %s",
                ("", "", '{"preview_id":"preview_partial"}'),
            ),
            (
                "preview_pair_without_digest",
                "UPDATE {schema}.agent_actions SET request_schema_version = %s, request_schema_digest = %s, "
                "input_json = %s WHERE action_id = %s",
                ("", "", '{"preview_id":"preview_partial","preview_revision":1}'),
            ),
            (
                "version_match_digest_mismatch",
                "UPDATE {schema}.agent_actions SET request_schema_version = %s, request_schema_digest = %s, "
                "input_json = %s WHERE action_id = %s",
                (ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION, "0" * 64, "{}"),
            ),
            (
                "digest_match_version_mismatch",
                "UPDATE {schema}.agent_actions SET request_schema_version = %s, request_schema_digest = %s, "
                "input_json = %s WHERE action_id = %s",
                ("acquisition_root_request_legacy", ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST, "{}"),
            ),
        )
        for ordinal, (label, sql, params) in enumerate(cases, start=1):
            with self.subTest(case=label):
                occurrence = self._arrange_pending(suffix=f"generic_partial_v2_{ordinal}")
                action_id = submit_pg.revalidate_acquisition_start_v2_occurrence(occurrence).action_id
                self._execute(sql, (*params, action_id))
                orchestrator = self._orchestrator()
                controls: tuple[tuple[str, Callable[[], dict[str, Any]]], ...] = (
                    (
                        "approve",
                        lambda: orchestrator.approve_operation_action_api(
                            action_id,
                            {"actor": "generic-control-test"},
                            expected_workspace_id=occurrence.workspace_id,
                        ),
                    ),
                    (
                        "reject",
                        lambda: orchestrator.reject_operation_action_api(
                            action_id,
                            {"actor": "generic-control-test", "reason": "not allowed"},
                            expected_workspace_id=occurrence.workspace_id,
                        ),
                    ),
                )

                for control_label, control in controls:
                    with self.subTest(control=control_label):
                        baseline = self._table_snapshot()
                        response = control()
                        self.assertEqual(response["status"], "invalid")
                        self.assertEqual(
                            response["reason"],
                            "acquisition_start_v2_generic_operation_control_identity_mismatch",
                        )
                        self.assertFalse(response["module_state_mutated"])
                        self.assertEqual(self._table_snapshot(), baseline)

    def test_dormant_start_v2_root_command_blocks_generic_command_control_claim_and_ready_list(self) -> None:
        occurrence = self._arrange_pending(suffix="command_control_closed")
        created = self._create(occurrence)
        owner_ref = created["owner_result_ref"]
        command_id = owner_ref["workflow_command_id"]
        orchestrator = self._orchestrator()

        ready_commands = self.store.list_ready_workflow_commands(
            owner="acquisition_run_writer",
            command_type="acquisition.run.create",
        )
        self.assertNotIn(command_id, {str(command.get("command_id") or "") for command in ready_commands})
        baseline = self._table_snapshot()
        self.assertEqual(self.store.claim_workflow_command(command_id, lease_owner="generic-worker"), {})
        self.assertEqual(self._table_snapshot(), baseline)

        controls: tuple[tuple[str, Callable[[], dict[str, Any]]], ...] = (
            (
                "cancel",
                lambda: orchestrator.cancel_workflow_command_api(
                    command_id,
                    {"actor": "generic-command-control-test", "reason": "not allowed"},
                ),
            ),
            (
                "retry",
                lambda: orchestrator.retry_workflow_command_api(
                    command_id,
                    {"actor": "generic-command-control-test", "reason": "not allowed"},
                ),
            ),
            (
                "resume",
                lambda: orchestrator.resume_workflow_command_api(
                    command_id,
                    {"actor": "generic-command-control-test", "reason": "not allowed"},
                ),
            ),
        )
        for label, control in controls:
            with self.subTest(control=label):
                response = control()
                self.assertEqual(response["status"], "unsupported")
                self.assertEqual(response["reason"], "acquisition_start_v2_root_command_waiting_for_result_acceptance")
                self.assertFalse(response["module_state_mutated"])
                self.assertEqual(self._table_snapshot(), baseline)

    def test_dormant_start_v2_root_command_blocks_native_command_mutators_without_writes(self) -> None:
        cases: tuple[tuple[str, str, Callable[[str], dict[str, Any] | None]], ...] = (
            (
                "cancel",
                "queued",
                lambda command_id: self.store.cancel_workflow_command(
                    command_id,
                    reason="native cancel must not clear hold",
                    actor="native-control-test",
                    result={"control_source": "native_test"},
                ),
            ),
            (
                "retry",
                "cancelled",
                lambda command_id: self.store.retry_workflow_command(
                    command_id,
                    reason="native retry must not clear hold",
                    actor="native-control-test",
                    result={"control_source": "native_test"},
                ),
            ),
            (
                "resume",
                "retry_wait",
                lambda command_id: self.store.resume_workflow_command(
                    command_id,
                    reason="native resume must not clear hold",
                    actor="native-control-test",
                    result={"control_source": "native_test"},
                ),
            ),
        )

        for ordinal, (label, status, mutator) in enumerate(cases, start=1):
            with self.subTest(control=label):
                occurrence = self._arrange_pending(suffix=f"native_command_control_closed_{ordinal}")
                created = self._create(occurrence)
                command_id = created["owner_result_ref"]["workflow_command_id"]
                if status != "queued":
                    self._execute(
                        "UPDATE {schema}.workflow_commands SET status = %s WHERE command_id = %s",
                        (status, command_id),
                    )
                baseline = self._table_snapshot()

                self.assertEqual(mutator(command_id), {})

                self.assertEqual(self._table_snapshot(), baseline)

    def test_create_replay_rejects_noncanonical_string_identity_without_writes(self) -> None:
        occurrence = self._arrange_pending(suffix="noncanonical_text")
        created = self._create(occurrence)
        command_id = created["owner_result_ref"]["workflow_command_id"]
        self._execute(
            "UPDATE {schema}.workflow_commands SET owner = %s WHERE command_id = %s",
            (" acquisition_run_writer ", command_id),
        )
        corrupted = self._table_snapshot()

        with self.assertRaisesRegex(ValueError, "WorkflowCommand immutable identity collision: owner"):
            self._create(occurrence)
        self.assertEqual(self._table_snapshot(), corrupted)

    def test_create_replay_rejects_blank_json_carrier_without_writes(self) -> None:
        occurrence = self._arrange_pending(suffix="blank_json")
        created = self._create(occurrence)
        command_id = created["owner_result_ref"]["workflow_command_id"]
        self._execute(
            "UPDATE {schema}.workflow_commands SET result_json = %s WHERE command_id = %s",
            ("", command_id),
        )
        corrupted = self._table_snapshot()

        with self.assertRaisesRegex(ValueError, "WorkflowCommand.result_json"):
            self._create(occurrence)
        self.assertEqual(self._table_snapshot(), corrupted)

    def test_create_replay_rejects_bool_int_json_alias_without_writes(self) -> None:
        occurrence = self._arrange_pending(suffix="bool_int_json")
        created = self._create(occurrence)
        command_id = created["owner_result_ref"]["workflow_command_id"]
        self._execute(
            "UPDATE {schema}.workflow_commands SET retry_policy_json = %s WHERE command_id = %s",
            (json.dumps({"kind": "operation_acquisition_run_create", "retry_delay_seconds": True}), command_id),
        )
        corrupted = self._table_snapshot()

        with self.assertRaisesRegex(ValueError, "WorkflowCommand.retry_policy_json"):
            self._create(occurrence)
        self.assertEqual(self._table_snapshot(), corrupted)

    def test_eight_concurrent_identical_calls_commit_one_bundle(self) -> None:
        occurrence = self._arrange_pending(suffix="concurrent_exact")
        barrier = Barrier(8)

        def run(_: int) -> dict[str, Any]:
            barrier.wait(timeout=5.0)
            return self._create(occurrence)

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(run, range(8)))

        self.assertEqual(sum(result["outcome"] == "created" for result in results), 1)
        self.assertEqual(sum(result["outcome"] == "replayed" for result in results), 7)
        self.assertEqual(len({result["owner_result_digest"] for result in results}), 1)
        self.assertEqual(len({result["confirmation_receipt"]["approved_at"] for result in results}), 1)

    def test_two_concurrent_nonidentical_approvers_commit_one_winner(self) -> None:
        occurrence = self._arrange_pending(suffix="concurrent_approvers")
        barrier = Barrier(2)

        def run(actor_id: str) -> tuple[str, dict[str, Any] | Exception]:
            barrier.wait(timeout=5.0)
            try:
                return "ok", self._create(occurrence, approval_actor_id=actor_id)
            except Exception as exc:  # exact loser type is asserted below
                return "error", exc

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(run, ("human_a", "human_b")))

        winners = [value for status, value in results if status == "ok"]
        losers = [value for status, value in results if status == "error"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        self.assertEqual(winners[0]["outcome"], "created")  # type: ignore[index]
        self.assertIsInstance(losers[0], ValueError)
        self.assertRegex(str(losers[0]), "collision|mismatch|conflict")

    def test_alternate_operation_identity_collision_writes_nothing(self) -> None:
        occurrence = self._arrange_pending(suffix="alternate_operation")
        binding = submit_pg.revalidate_acquisition_start_v2_occurrence(occurrence)
        operation_run_id = operation_run_id_for(
            action_id=binding.action_id,
            operation_type="acquisition_run",
            idempotency_key=binding.start_idempotency,
        )
        self._execute(
            """
            INSERT INTO {schema}.operation_runs (
                operation_run_id, workspace_id, action_id, owner_module, operation_type,
                status, progress_json, workflow_ref_json, cost_budget_json, idempotency_key,
                result_ref_json, metadata_json, started_at, completed_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, 'collision', 'acquisition_run',
                'queued', '{}', '{}', '{}', %s,
                '{}', '{}', '', '', '2026-07-17T00:00:00Z', '2026-07-17T00:00:00Z'
            )
            """,
            (
                f"{operation_run_id}_collision",
                occurrence.workspace_id,
                binding.action_id,
                binding.start_idempotency,
            ),
        )
        baseline = self._table_snapshot()

        with self.assertRaisesRegex(ValueError, "collision|mismatch|conflict"):
            self._create(occurrence)

        self.assertEqual(self._table_snapshot(), baseline)

    def test_foreign_operation_membership_collision_writes_nothing(self) -> None:
        occurrence = self._arrange_pending(suffix="foreign_operation")
        binding = submit_pg.revalidate_acquisition_start_v2_occurrence(occurrence)
        operation_run_id = operation_run_id_for(
            action_id=binding.action_id,
            operation_type="acquisition_run",
            idempotency_key=binding.start_idempotency,
        )
        self._execute(
            """
            INSERT INTO {schema}.operation_runs (
                operation_run_id, workspace_id, action_id, owner_module, operation_type,
                status, progress_json, workflow_ref_json, cost_budget_json, idempotency_key,
                result_ref_json, metadata_json, started_at, completed_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, 'foreign_collision', 'acquisition_run',
                'queued', '{}', '{}', '{}', 'foreign_idempotency',
                '{}', '{}', '', '', '2026-07-17T00:00:00Z', '2026-07-17T00:00:00Z'
            )
            """,
            (
                f"{operation_run_id}_foreign",
                occurrence.workspace_id,
                binding.action_id,
            ),
        )
        baseline = self._table_snapshot()

        with self.assertRaisesRegex(ValueError, "membership collision"):
            self._create(occurrence)

        self.assertEqual(self._table_snapshot(), baseline)

    def test_corrupted_pending_action_or_slot_json_rejects_before_create_writes(self) -> None:
        mutations: tuple[tuple[str, Callable[[AgentToolOccurrence], tuple[str, tuple[Any, ...]]]], ...] = (
            (
                "blank_action_result_ref",
                lambda occurrence: (
                    "UPDATE {schema}.agent_actions SET result_ref_json = '' WHERE action_id = %s",
                    (submit_pg.revalidate_acquisition_start_v2_occurrence(occurrence).action_id,),
                ),
            ),
            (
                "malformed_action_metadata",
                lambda occurrence: (
                    "UPDATE {schema}.agent_actions SET metadata_json = '{bad' WHERE action_id = %s",
                    (submit_pg.revalidate_acquisition_start_v2_occurrence(occurrence).action_id,),
                ),
            ),
            (
                "duplicate_action_budget_key",
                lambda occurrence: (
                    'UPDATE {schema}.agent_actions SET budget_json = \'{"a":1,"a":2}\' WHERE action_id = %s',
                    (submit_pg.revalidate_acquisition_start_v2_occurrence(occurrence).action_id,),
                ),
            ),
            (
                "wrong_container_action_result_ref",
                lambda occurrence: (
                    "UPDATE {schema}.agent_actions SET result_ref_json = '[]' WHERE action_id = %s",
                    (submit_pg.revalidate_acquisition_start_v2_occurrence(occurrence).action_id,),
                ),
            ),
        )

        for ordinal, (label, mutation) in enumerate(mutations, start=1):
            with self.subTest(owner=label):
                occurrence = self._arrange_pending(suffix=f"pending_json_{ordinal}")
                sql, params = mutation(occurrence)
                self._execute(sql, params)
                baseline = self._table_snapshot()

                with self.assertRaisesRegex(ValueError, "immutable identity collision"):
                    self._create(occurrence)

                self.assertEqual(self._table_snapshot(), baseline)

    def test_corrupted_committed_owner_rows_are_zero_write_collisions(self) -> None:
        mutations: tuple[tuple[str, Callable[[dict[str, Any]], tuple[str, tuple[Any, ...]]]], ...] = (
            (
                "action",
                lambda ids: (
                    "UPDATE {schema}.agent_actions SET result_ref_json = '{}' WHERE action_id = %s",
                    (ids["action_id"],),
                ),
            ),
            (
                "operation",
                lambda ids: (
                    "UPDATE {schema}.operation_runs SET result_ref_json = '{}' WHERE operation_run_id = %s",
                    (ids["operation_run_id"],),
                ),
            ),
            (
                "receipt",
                lambda ids: (
                    "UPDATE {schema}.operation_events SET payload_json = '{}' WHERE event_id = %s",
                    (ids["receipt_event_id"],),
                ),
            ),
            (
                "source_event",
                lambda ids: (
                    "UPDATE {schema}.workflow_events SET payload_json = '{}' WHERE event_id = %s",
                    (ids["source_event_id"],),
                ),
            ),
            (
                "command",
                lambda ids: (
                    "UPDATE {schema}.workflow_commands SET payload_json = '{}' WHERE command_id = %s",
                    (ids["command_id"],),
                ),
            ),
            (
                "current_state",
                lambda ids: (
                    "UPDATE {schema}.workflow_current_state SET status = 'corrupt' WHERE workflow_run_id = %s",
                    (ids["workflow_run_id"],),
                ),
            ),
            (
                "winner",
                lambda ids: (
                    "UPDATE {schema}.operation_events SET payload_json = '{}' WHERE event_id = %s",
                    (ids["planned_event_id"],),
                ),
            ),
        )

        for ordinal, (label, mutation) in enumerate(mutations, start=1):
            with self.subTest(owner=label):
                occurrence = self._arrange_pending(suffix=f"corrupt_{ordinal}")
                created = self._create(occurrence)
                owner_ref = created["owner_result_ref"]
                ids = {
                    "action_id": owner_ref["action_id"],
                    "operation_run_id": owner_ref["operation_run_id"],
                    "receipt_event_id": owner_ref["confirmation_receipt_ref"]["receipt_id"],
                    "source_event_id": owner_ref["command_source_event_id"],
                    "command_id": owner_ref["workflow_command_id"],
                    "workflow_run_id": owner_ref["workflow_run_id"],
                    "planned_event_id": owner_ref["terminal_winner_id"],
                }
                sql, params = mutation(ids)
                self._execute(sql, params)
                baseline = self._table_snapshot()

                with self.assertRaises(ValueError):
                    self._create(occurrence)

                self.assertEqual(self._table_snapshot(), baseline)

    def test_committed_aggregate_suffix_membership_replay_collision_writes_nothing(self) -> None:
        mutations: tuple[tuple[str, Callable[[dict[str, Any]], tuple[str, tuple[Any, ...]]]], ...] = (
            (
                "action_event_suffix",
                lambda ids: (
                    """
                    INSERT INTO {schema}.operation_events (
                        event_id, workspace_id, event_stream_id, operation_run_id, action_id, event_family,
                        event_type, sequence_number, idempotency_key, actor, source, payload_json,
                        schema_version, created_at
                    )
                    SELECT
                        %s, workspace_id, event_stream_id, operation_run_id, action_id, event_family,
                        event_type, 3, %s, actor, source, payload_json, schema_version, created_at
                    FROM {schema}.operation_events WHERE event_id = %s
                    """,
                    (
                        f"{ids['receipt_event_id']}_suffix",
                        f"{ids['receipt_event_id']}:suffix",
                        ids["receipt_event_id"],
                    ),
                ),
            ),
            (
                "workflow_event_suffix",
                lambda ids: (
                    """
                    INSERT INTO {schema}.workflow_events (
                        event_id, workflow_run_id, workspace_id, operation_id, command_id,
                        activity_attempt_id, event_family, event_type, sequence_number,
                        idempotency_key, actor, source, payload_json, schema_version, created_at
                    )
                    SELECT
                        %s, workflow_run_id, workspace_id, operation_id, command_id,
                        activity_attempt_id, event_family, event_type, 3,
                        %s, actor, source, payload_json, schema_version, created_at
                    FROM {schema}.workflow_events WHERE event_id = %s
                    """,
                    (
                        f"{ids['source_event_id']}_suffix",
                        f"{ids['source_event_id']}:suffix",
                        ids["source_event_id"],
                    ),
                ),
            ),
        )

        for ordinal, (label, mutation) in enumerate(mutations, start=1):
            with self.subTest(owner=label):
                occurrence = self._arrange_pending(suffix=f"membership_suffix_{ordinal}")
                created = self._create(occurrence)
                owner_ref = created["owner_result_ref"]
                ids = {
                    "receipt_event_id": owner_ref["confirmation_receipt_ref"]["receipt_id"],
                    "source_event_id": owner_ref["command_source_event_id"],
                    "command_id": owner_ref["workflow_command_id"],
                }
                sql, params = mutation(ids)
                self._execute(sql, params)
                baseline = self._table_snapshot()

                with self.assertRaisesRegex(ValueError, "collision"):
                    self._create(occurrence)

                self.assertEqual(self._table_snapshot(), baseline)


if __name__ == "__main__":
    unittest.main()
