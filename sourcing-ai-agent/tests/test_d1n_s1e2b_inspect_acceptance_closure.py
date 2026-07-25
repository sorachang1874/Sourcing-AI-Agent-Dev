from __future__ import annotations

import hashlib
import json
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
from sourcing_agent.agent_canary_registry import INSPECT_OPERATION_TOOL_SPEC, START_ACQUISITION_RUN_TOOL_SPEC
from sourcing_agent.agent_operation_query_postgres import _operation_command_planned_event_identity
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.control_plane_live_postgres import ControlPlaneAdvisoryLockBusy
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
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
from sourcing_agent.workflow_progressed_child_contract import (
    build_acquisition_root_intent_plan,
    canonical_progressed_child_identity,
    expected_progressed_child_row,
    progressed_child_completion_contract,
    progressed_child_plan_event_id,
    progressed_child_plan_event_violation,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin, psycopg
from tests.test_d1n_acquisition_plan_preview_uow import _uow_kwargs

_APPROVAL = {
    "approval_actor_id": "human_1",
    "approval_actor_kind": "authenticated_user",
    "approval_policy_revision": "acquisition_confirmation_policy_v1",
}


class _RepositoryPreviewReader:
    def __init__(self, repository: Any) -> None:
        self._repository = repository

    def get_acquisition_plan_preview(self, preview_id: str, **owner: Any) -> dict[str, Any] | None:
        row = self._repository.get_acquisition_plan_preview(preview_id, **owner)
        preview = row.get("preview") if isinstance(row, dict) else None
        return dict(preview) if isinstance(preview, dict) else None


def _digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class D1nS1e2bInspectAcceptanceClosureTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1n_s1e2b_inspect_acceptance_closure"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(Path(self.tempdir.name) / "inspect-acceptance-closure.db")
        self.repository = self.store.repos.workflow_runtime
        self.adapter = self.store._control_plane_postgres  # noqa: SLF001

    def _schema_connection(self) -> tuple[Any, str]:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        return fixture, quote_control_plane_postgres_identifier(fixture.schema)

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
                return [dict(row[0]) for row in cursor.fetchall()]

    def _execute(self, sql: str, params: tuple[Any, ...]) -> None:
        fixture, quoted_schema = self._schema_connection()
        assert psycopg is not None
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.replace("{schema}", quoted_schema), params)
            connection.commit()

    def _execute_with_result_slot_trigger_disabled(self, sql: str, params: tuple[Any, ...]) -> None:
        fixture, quoted_schema = self._schema_connection()
        assert psycopg is not None
        table = f"{quoted_schema}.{quote_control_plane_postgres_identifier('agent_tool_result_slots')}"
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"ALTER TABLE {table} DISABLE TRIGGER USER")
                try:
                    cursor.execute(sql.replace("{schema}", quoted_schema), params)
                finally:
                    cursor.execute(f"ALTER TABLE {table} ENABLE TRIGGER USER")
            connection.commit()

    def _execute_with_workflow_command_trigger_disabled(self, sql: str, params: tuple[Any, ...]) -> None:
        fixture, quoted_schema = self._schema_connection()
        assert psycopg is not None
        table = f"{quoted_schema}.{quote_control_plane_postgres_identifier('workflow_commands')}"
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"ALTER TABLE {table} DISABLE TRIGGER USER")
                try:
                    cursor.execute(sql.replace("{schema}", quoted_schema), params)
                finally:
                    cursor.execute(f"ALTER TABLE {table} ENABLE TRIGGER USER")
            connection.commit()

    def _effect_counts(self) -> dict[str, int]:
        return {
            table: len(self._rows(table))
            for table in (
                "agent_tool_result_attempts",
                "agent_tool_result_journal",
                "acquisition_runs",
                "workflow_activity_runs",
                "workflow_activity_attempts",
                "workflow_entity_deltas",
                "acquisition_discovery_lanes",
                "plan_review_sessions",
                "crm_tasks",
            )
        }

    def _acceptance_snapshot(self) -> dict[str, tuple[str, ...]]:
        return {
            table: tuple(
                sorted(json.dumps(row, sort_keys=True, default=str, separators=(",", ":")) for row in self._rows(table))
            )
            for table in (
                "agent_actions",
                "operation_runs",
                "operation_events",
                "workflow_commands",
                "workflow_events",
                "agent_tool_result_slots",
                "agent_tool_result_attempts",
                "agent_tool_result_journal",
            )
        }

    def _arrange_created(self, *, suffix: str) -> tuple[AgentToolOccurrence, dict[str, Any]]:
        kwargs = _uow_kwargs(suffix=f"s1e2b_inspect_closure_{suffix}")
        kwargs["start_request_schema_version"] = ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        kwargs["start_request_schema_digest"] = ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
        preview_bundle = self.repository.create_acquisition_plan_preview_uow(**kwargs)
        preview = dict(preview_bundle["preview"])
        bound = AcquisitionStartV2OwnerBinder(_RepositoryPreviewReader(self.repository)).bind(
            input_payload={
                "preview_id": preview["preview_id"],
                "preview_revision": preview["preview_revision"],
                "preview_digest": preview["preview_digest"],
            },
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
            result_slot_id=f"slot_s1e2b_inspect_closure_{suffix}",
            slot_generation=1,
            workspace_id=kwargs["workspace_id"],
            actor_id=kwargs["requester_id"],
            runtime_namespace="isolated_local_canary",
            provider_mode="simulate",
            turn_id=f"turn_s1e2b_inspect_closure_{suffix}",
            step_id=f"step_s1e2b_inspect_closure_{suffix}",
            tool_spec=START_ACQUISITION_RUN_TOOL_SPEC,
            canonical_args=bound.to_record(),
            occurrence_ordinal=1,
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        submitted = submit_pg.submit_acquisition_start_v2_action_uow(self.adapter, occurrence=occurrence)
        self.assertEqual(submitted["outcome"], "submitted")
        created = create_pg.create_acquisition_start_v2_uow(self.adapter, occurrence=occurrence, **_APPROVAL)
        self.assertEqual(created["outcome"], "created")
        return occurrence, dict(created["owner_result_ref"])

    def _inspect_occurrence(
        self,
        *,
        start_occurrence: AgentToolOccurrence,
        owner_ref: dict[str, Any],
        suffix: str,
    ) -> AgentToolOccurrence:
        occurrence = AgentToolOccurrence.from_tool_spec(
            result_slot_id=f"slot_s1e2b_inspect_query_{suffix}",
            slot_generation=1,
            workspace_id=start_occurrence.workspace_id,
            actor_id=start_occurrence.actor_id,
            runtime_namespace=start_occurrence.runtime_namespace,
            provider_mode="simulate",
            turn_id=f"turn_s1e2b_inspect_query_{suffix}",
            step_id=f"step_s1e2b_inspect_query_{suffix}",
            tool_spec=INSPECT_OPERATION_TOOL_SPEC,
            canonical_args={"operation_run_id": str(owner_ref["operation_run_id"])},
            occurrence_ordinal=1,
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        return occurrence

    def _prepare_inspect(
        self,
        *,
        occurrence: AgentToolOccurrence,
        owner_ref: dict[str, Any],
        suffix: str,
    ) -> Any:
        return self.repository.prepare_inspect_operation_tool_result(
            occurrence=occurrence,
            result_attempt_id=f"attempt_s1e2b_inspect_{suffix}",
            provider_call_id=f"provider_s1e2b_inspect_{suffix}",
            tool_call_id=f"tool_s1e2b_inspect_{suffix}",
            action_id=str(owner_ref["action_id"]),
            operation_run_id=str(owner_ref["operation_run_id"]),
        )

    def _assert_inspect_rejects_without_effects(
        self,
        *,
        start_occurrence: AgentToolOccurrence,
        owner_ref: dict[str, Any],
        suffix: str,
    ) -> None:
        inspect_occurrence = self._inspect_occurrence(
            start_occurrence=start_occurrence,
            owner_ref=owner_ref,
            suffix=suffix,
        )
        baseline = self._effect_counts()
        with self.assertRaisesRegex(ValueError, "workflow command link mismatch"):
            self._prepare_inspect(occurrence=inspect_occurrence, owner_ref=owner_ref, suffix=suffix)
        self.assertEqual(self._effect_counts(), baseline)

    def test_exact_and_partial_start_v2_cannot_downgrade_to_valid_legacy_plan_event(self) -> None:
        for label, partial in (("exact", False), ("partial", True)):
            with self.subTest(classification=label):
                occurrence, owner_ref = self._arrange_created(suffix=f"legacy_pair_{label}")
                legacy_payload = {
                    "workflow_run_id": owner_ref["workflow_run_id"],
                    "command_id": owner_ref["workflow_command_id"],
                    "command_type": "acquisition.run.create",
                    "owner": "acquisition_run_writer",
                    "module_state_mutated": False,
                }
                self._execute(
                    "UPDATE {schema}.operation_events SET schema_version = %s, payload_json = %s WHERE event_id = %s",
                    ("operation_event_v1", json.dumps(legacy_payload), owner_ref["terminal_winner_id"]),
                )
                if partial:
                    self._execute(
                        "UPDATE {schema}.agent_actions SET action_type = %s WHERE action_id = %s",
                        ("fetch_profile_sample", owner_ref["action_id"]),
                    )
                self._assert_inspect_rejects_without_effects(
                    start_occurrence=occurrence,
                    owner_ref=owner_ref,
                    suffix=f"legacy_pair_{label}",
                )

    def test_partial_registered_action_still_validates_physical_acceptance(self) -> None:
        occurrence, owner_ref = self._arrange_created(suffix="registered_foreign_physical")
        self._execute(
            "UPDATE {schema}.agent_actions SET action_type = %s WHERE action_id = %s",
            ("fetch_profile_sample", owner_ref["action_id"]),
        )
        positive_occurrence = self._inspect_occurrence(
            start_occurrence=occurrence,
            owner_ref=owner_ref,
            suffix="registered_foreign_positive",
        )
        terminal = self._prepare_inspect(
            occurrence=positive_occurrence,
            owner_ref=owner_ref,
            suffix="registered_foreign_positive",
        )
        self.assertEqual(terminal.serialized_result["variant"], "success")
        self.assertEqual(terminal.serialized_result["display_contract"]["action_type"], "start_acquisition_run")
        self.assertEqual(
            set(terminal.serialized_result["control_state"]["disabled_reasons"].values()),
            {"acquisition_start_v2_generic_operation_control_identity_mismatch"},
        )
        self._execute(
            "UPDATE {schema}.workflow_events SET actor = %s WHERE event_id = %s",
            ("drifted_source_actor", owner_ref["command_source_event_id"]),
        )
        self._assert_inspect_rejects_without_effects(
            start_occurrence=occurrence,
            owner_ref=owner_ref,
            suffix="registered_foreign_physical",
        )

    def test_start_occurrence_and_canonical_physical_owner_matrix_is_fail_closed(self) -> None:
        cases = (
            (
                "start_slot_generation",
                "UPDATE {schema}.agent_tool_result_slots SET slot_generation = %s WHERE result_slot_id = %s",
                lambda occurrence, _owner: (2, occurrence.result_slot_id),
            ),
            (
                "receipt_event_id",
                "UPDATE {schema}.operation_events SET event_id = %s WHERE event_stream_id = %s AND sequence_number = 2",
                lambda _occurrence, owner: ("opevt_drifted_receipt", owner["action_id"]),
            ),
            (
                "receipt_actor",
                "UPDATE {schema}.operation_events SET actor = %s WHERE event_stream_id = %s AND sequence_number = 2",
                lambda _occurrence, owner: ("drifted_receipt_actor", owner["action_id"]),
            ),
            (
                "receipt_source",
                "UPDATE {schema}.operation_events SET source = %s WHERE event_stream_id = %s AND sequence_number = 2",
                lambda _occurrence, owner: ("drifted_receipt_source", owner["action_id"]),
            ),
            (
                "receipt_idempotency",
                "UPDATE {schema}.operation_events SET idempotency_key = %s "
                "WHERE event_stream_id = %s AND sequence_number = 2",
                lambda _occurrence, owner: ("drifted_receipt_key", owner["action_id"]),
            ),
            (
                "command_stage",
                "UPDATE {schema}.workflow_commands SET stage_id = %s WHERE command_id = %s",
                lambda _occurrence, owner: ("drifted_stage", owner["workflow_command_id"]),
            ),
            (
                "command_causal_group",
                "UPDATE {schema}.workflow_commands SET causal_group_id = %s WHERE command_id = %s",
                lambda _occurrence, owner: ("drifted_group", owner["workflow_command_id"]),
            ),
            (
                "command_parent",
                "UPDATE {schema}.workflow_commands SET parent_command_id = %s WHERE command_id = %s",
                lambda _occurrence, owner: ("cmd_drifted_parent", owner["workflow_command_id"]),
            ),
            (
                "command_causality_schema",
                "UPDATE {schema}.workflow_commands SET causality_schema_version = %s WHERE command_id = %s",
                lambda _occurrence, owner: ("command_causality_v999", owner["workflow_command_id"]),
            ),
        )
        for ordinal, (label, sql, params_for) in enumerate(cases, start=1):
            with self.subTest(field=label):
                occurrence, owner_ref = self._arrange_created(suffix=f"physical_{ordinal}")
                execute = (
                    self._execute_with_result_slot_trigger_disabled
                    if label == "start_slot_generation"
                    else self._execute
                )
                execute(sql, params_for(occurrence, owner_ref))
                self._assert_inspect_rejects_without_effects(
                    start_occurrence=occurrence,
                    owner_ref=owner_ref,
                    suffix=f"physical_{ordinal}",
                )

    def test_bool_and_float_owner_mirrors_do_not_alias_exact_integers(self) -> None:
        for label, field, hostile in (
            ("terminal_bool", "terminal_winner_sequence_number", True),
            ("source_float", "command_source_event_sequence_number", 2.0),
        ):
            with self.subTest(alias=label):
                occurrence, owner_ref = self._arrange_created(suffix=f"mirror_{label}")
                action_ref = dict(owner_ref)
                action_ref[field] = hostile
                self._execute(
                    "UPDATE {schema}.agent_actions SET result_ref_json = %s WHERE action_id = %s",
                    (json.dumps(action_ref), owner_ref["action_id"]),
                )
                self._assert_inspect_rejects_without_effects(
                    start_occurrence=occurrence,
                    owner_ref=owner_ref,
                    suffix=f"mirror_{label}",
                )

    def test_referenced_start_slot_advisory_lock_blocks_prepare_without_writes(self) -> None:
        occurrence, owner_ref = self._arrange_created(suffix="start_slot_lock")
        inspect_occurrence = self._inspect_occurrence(
            start_occurrence=occurrence,
            owner_ref=owner_ref,
            suffix="start_slot_lock",
        )
        baseline = self._acceptance_snapshot()
        fixture, _quoted_schema = self._schema_connection()
        assert psycopg is not None
        physical_lock_key = self.adapter._advisory_lock_key(  # noqa: SLF001
            f"agent_tool_result_slots:id:{occurrence.result_slot_id}"
        )

        with psycopg.connect(fixture.dsn, client_encoding="utf8") as blocker:
            with blocker.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_lock(hashtext(%s))", (physical_lock_key,))
            blocker.commit()
            started = time.monotonic()
            try:
                with self.assertRaises(ControlPlaneAdvisoryLockBusy):
                    self.adapter.prepare_inspect_operation_tool_result(
                        table_name="agent_tool_result_slots",
                        occurrence=inspect_occurrence,
                        result_attempt_id="attempt_s1e2b_inspect_start_slot_lock",
                        provider_call_id="provider_s1e2b_inspect_start_slot_lock",
                        tool_call_id="tool_s1e2b_inspect_start_slot_lock",
                        action_id=str(owner_ref["action_id"]),
                        operation_run_id=str(owner_ref["operation_run_id"]),
                        lock_timeout_seconds=0.15,
                    )
            finally:
                with blocker.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (physical_lock_key,))
                blocker.commit()

        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(self._acceptance_snapshot(), baseline)

    def test_true_non_v2_legacy_planned_event_remains_compatible(self) -> None:
        workflow_ref = {
            "workflow_run_id": "workflow_legacy",
            "command_id": "command_legacy",
            "command_type": "profile.fetch",
            "owner": "profile_writer",
        }
        event = {
            "schema_version": "operation_event_v1",
            "sequence_number": 1,
            "event_id": "opevt_legacy",
        }
        self.assertEqual(
            _operation_command_planned_event_identity(
                event,
                {**workflow_ref, "module_state_mutated": False},
                normalized_workflow_ref=workflow_ref,
                workspace_id="workspace_legacy",
                action_id="action_legacy",
                operation_run_id="operation_legacy",
                action={},
                operation={},
                require_start_acceptance_event=False,
            ),
            workflow_ref,
        )

    def _prepare_start_terminal(self, *, start_occurrence: AgentToolOccurrence, suffix: str) -> Any:
        return self.repository.prepare_start_acquisition_tool_result(
            occurrence=start_occurrence,
            result_attempt_id=f"attempt_s1e2b_start_accept_{suffix}",
            provider_call_id=f"provider_s1e2b_start_accept_{suffix}",
            tool_call_id=f"tool_s1e2b_start_accept_{suffix}",
        )

    def _command_row(self, command_id: str) -> dict[str, Any]:
        rows = self._rows("workflow_commands", where="command_id = %s", params=(command_id,))
        self.assertEqual(len(rows), 1)
        return rows[0]

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

    def _release_start_hold(self, *, start_occurrence: AgentToolOccurrence, suffix: str) -> None:
        start_terminal = self._prepare_start_terminal(start_occurrence=start_occurrence, suffix=suffix)
        accepted = self.repository.accept_start_acquisition_tool_result_uow(
            occurrence=start_occurrence,
            terminal=start_terminal,
            attempted_slot_generation=start_occurrence.slot_generation,
        )
        self.assertEqual(accepted["outcome"], "accepted")

    def _complete_root_with_real_child(self, *, owner_ref: dict[str, Any], suffix: str) -> dict[str, Any]:
        """Progress one released root through the root-completion owner path."""

        command_id = str(owner_ref["workflow_command_id"])
        lease_owner = f"s1e2b_progressed_lease_{suffix}"
        lease_expires_at = "2099-01-01 00:00:00"
        self._execute(
            "UPDATE {schema}.workflow_commands SET status = %s, lease_owner = %s, lease_expires_at = %s, "
            "attempt = %s WHERE command_id = %s",
            ("running", lease_owner, lease_expires_at, 1, command_id),
        )
        root = dict(self.store.get_workflow_command(command_id) or {})
        owner = self._orchestrator()._acquisition_command_owner  # noqa: SLF001
        contract = owner._acquisition_root_intent_plan_contract(root, claim_attempt=1)  # noqa: SLF001
        self.assertTrue(contract)
        completed = self.repository.complete_acquisition_root_command(
            command_id,
            expected_lease_owner=lease_owner,
            expected_lease_expires_at=lease_expires_at,
            expected_attempt=1,
            expected_root_command=owner._acquisition_root_locked_identity(root),  # noqa: SLF001
            plan_event=dict(contract.get("plan_event") or {}),
            child_command=dict(contract.get("child_command") or {}),
            child_causality=dict(contract.get("child_causality") or {}),
            root_result=dict(contract.get("root_result") or {}),
        )
        self.assertEqual(completed["outcome"], "applied")
        child = dict(completed["child_command"] or {})
        event = dict(completed["event"] or {})
        root_row = self._command_row(command_id)
        self.assertEqual(root_row["not_before_at"], "")
        self.assertEqual(json.loads(root_row["downstream_command_ids_json"]), [child["command_id"]])
        return {"root": root_row, "child": child, "event": event}

    def test_inspect_preparation_and_acceptance_follow_command_lifecycle_after_s1e2c_release(self) -> None:
        start_occurrence, owner_ref = self._arrange_created(suffix="lifecycle_release")
        command_id = str(owner_ref["workflow_command_id"])
        self.assertEqual(self._command_row(command_id)["not_before_at"], "9999-12-31 23:59:59")

        pending_occurrence = self._inspect_occurrence(
            start_occurrence=start_occurrence,
            owner_ref=owner_ref,
            suffix="lifecycle_release_pending",
        )
        pending_terminal = self._prepare_inspect(
            occurrence=pending_occurrence,
            owner_ref=owner_ref,
            suffix="lifecycle_release_pending",
        )
        self.assertEqual(pending_terminal.serialized_result["variant"], "success")

        start_terminal = self._prepare_start_terminal(start_occurrence=start_occurrence, suffix="lifecycle_release")
        accepted = self.repository.accept_start_acquisition_tool_result_uow(
            occurrence=start_occurrence,
            terminal=start_terminal,
            attempted_slot_generation=start_occurrence.slot_generation,
        )
        self.assertEqual(accepted["outcome"], "accepted")
        self.assertEqual(self._command_row(command_id)["not_before_at"], "")

        released_occurrence = self._inspect_occurrence(
            start_occurrence=start_occurrence,
            owner_ref=owner_ref,
            suffix="lifecycle_release_released",
        )
        released_terminal = self._prepare_inspect(
            occurrence=released_occurrence,
            owner_ref=owner_ref,
            suffix="lifecycle_release_released",
        )
        self.assertEqual(released_terminal.serialized_result["variant"], "success")
        inspect_accepted = self.repository.accept_inspect_operation_tool_result_uow(
            occurrence=released_occurrence,
            terminal=released_terminal,
            attempted_slot_generation=released_occurrence.slot_generation,
        )
        self.assertEqual(inspect_accepted["outcome"], "accepted")

    def test_inspect_preparation_follows_progressed_command_with_linked_downstream_child(self) -> None:
        start_occurrence, owner_ref = self._arrange_created(suffix="lifecycle_progressed")
        command_id = str(owner_ref["workflow_command_id"])
        self._release_start_hold(start_occurrence=start_occurrence, suffix="lifecycle_progressed")
        progressed = self._complete_root_with_real_child(owner_ref=owner_ref, suffix="lifecycle_progressed")
        child = progressed["child"]
        self.assertEqual(str(child.get("parent_command_id") or ""), command_id)

        progressed_occurrence = self._inspect_occurrence(
            start_occurrence=start_occurrence,
            owner_ref=owner_ref,
            suffix="lifecycle_progressed",
        )
        progressed_terminal = self._prepare_inspect(
            occurrence=progressed_occurrence,
            owner_ref=owner_ref,
            suffix="lifecycle_progressed",
        )
        self.assertEqual(progressed_terminal.serialized_result["variant"], "success")
        inspect_accepted = self.repository.accept_inspect_operation_tool_result_uow(
            occurrence=progressed_occurrence,
            terminal=progressed_terminal,
            attempted_slot_generation=progressed_occurrence.slot_generation,
        )
        self.assertEqual(inspect_accepted["outcome"], "accepted")

    def test_inspect_rejects_progressed_command_with_dangling_child_identifier(self) -> None:
        start_occurrence, owner_ref = self._arrange_created(suffix="progressed_dangling")
        command_id = str(owner_ref["workflow_command_id"])
        self._release_start_hold(start_occurrence=start_occurrence, suffix="progressed_dangling")
        self._execute(
            "UPDATE {schema}.workflow_commands SET downstream_command_ids_json = %s WHERE command_id = %s",
            (json.dumps([f"cmd_s1e2b_invented_{command_id.removeprefix('cmd_')}"]), command_id),
        )
        self._assert_inspect_rejects_without_effects(
            start_occurrence=start_occurrence,
            owner_ref=owner_ref,
            suffix="progressed_dangling",
        )

    def test_inspect_rejects_progressed_command_with_foreign_operation_child(self) -> None:
        foreign_occurrence, foreign_ref = self._arrange_created(suffix="progressed_foreign_source")
        self._release_start_hold(start_occurrence=foreign_occurrence, suffix="progressed_foreign_source")
        foreign = self._complete_root_with_real_child(owner_ref=foreign_ref, suffix="progressed_foreign_source")
        foreign_child_id = str(foreign["child"]["command_id"])

        start_occurrence, owner_ref = self._arrange_created(suffix="progressed_foreign_target")
        command_id = str(owner_ref["workflow_command_id"])
        self._release_start_hold(start_occurrence=start_occurrence, suffix="progressed_foreign_target")
        self._execute(
            "UPDATE {schema}.workflow_commands SET downstream_command_ids_json = %s WHERE command_id = %s",
            (json.dumps([foreign_child_id]), command_id),
        )
        self._assert_inspect_rejects_without_effects(
            start_occurrence=start_occurrence,
            owner_ref=owner_ref,
            suffix="progressed_foreign_target",
        )

    def test_inspect_rejects_progressed_command_with_wrong_parent_child(self) -> None:
        start_occurrence, owner_ref = self._arrange_created(suffix="progressed_wrong_parent")
        self._release_start_hold(start_occurrence=start_occurrence, suffix="progressed_wrong_parent")
        progressed = self._complete_root_with_real_child(owner_ref=owner_ref, suffix="progressed_wrong_parent")
        child_id = str(progressed["child"]["command_id"])
        self._execute(
            "UPDATE {schema}.workflow_commands SET parent_command_id = %s WHERE command_id = %s",
            ("cmd_s1e2b_unrelated_parent", child_id),
        )
        self._assert_inspect_rejects_without_effects(
            start_occurrence=start_occurrence,
            owner_ref=owner_ref,
            suffix="progressed_wrong_parent",
        )

    def test_inspect_rejects_progressed_command_with_missing_child_event(self) -> None:
        start_occurrence, owner_ref = self._arrange_created(suffix="progressed_missing_event")
        self._release_start_hold(start_occurrence=start_occurrence, suffix="progressed_missing_event")
        progressed = self._complete_root_with_real_child(owner_ref=owner_ref, suffix="progressed_missing_event")
        child_id = str(progressed["child"]["command_id"])
        child_row = self._command_row(child_id)
        source_event_id = str(child_row["source_event_id"])
        self.assertTrue(source_event_id)
        self._execute(
            "DELETE FROM {schema}.workflow_events WHERE event_id = %s",
            (source_event_id,),
        )
        self._assert_inspect_rejects_without_effects(
            start_occurrence=start_occurrence,
            owner_ref=owner_ref,
            suffix="progressed_missing_event",
        )

    def _rewrite_command_payload(self, command_id: str, mutate: Any) -> None:
        row = self._command_row(command_id)
        payload = json.loads(row["payload_json"])
        mutate(payload)
        self._execute(
            "UPDATE {schema}.workflow_commands SET payload_json = %s WHERE command_id = %s",
            (json.dumps(payload), command_id),
        )

    def _rewrite_event_payload(self, event_id: str, mutate: Any) -> None:
        rows = self._rows("workflow_events", where="event_id = %s", params=(event_id,))
        self.assertEqual(len(rows), 1)
        payload = json.loads(rows[0]["payload_json"])
        mutate(payload)
        self._execute(
            "UPDATE {schema}.workflow_events SET payload_json = %s WHERE event_id = %s",
            (json.dumps(payload), event_id),
        )

    def test_inspect_rejects_progressed_child_outside_the_versioned_child_contract(self) -> None:
        def _foreign_type(child_id: str) -> None:
            # The shape trigger fences identity-moving UPDATEs in normal writes;
            # a semantically foreign child can only exist when that floor is
            # bypassed, so the probe installs one with triggers disabled and
            # keeps column and payload causality fully self-consistent.
            self._execute_with_workflow_command_trigger_disabled(
                "UPDATE {schema}.workflow_commands SET command_type = %s WHERE command_id = %s",
                ("foreign.command", child_id),
            )
            self._rewrite_command_payload(
                child_id,
                lambda payload: payload["causality"].update(command_type="foreign.command"),
            )

        def _foreign_owner(child_id: str) -> None:
            self._execute(
                "UPDATE {schema}.workflow_commands SET owner = %s WHERE command_id = %s",
                ("foreign_owner", child_id),
            )
            self._rewrite_command_payload(
                child_id,
                lambda payload: payload["causality"].update(owner="foreign_owner"),
            )

        column_cases = (
            (
                "stage_drift",
                "UPDATE {schema}.workflow_commands SET stage_id = %s WHERE command_id = %s",
                lambda _child_id: ("drifted_stage",),
            ),
            (
                "causal_group_drift",
                "UPDATE {schema}.workflow_commands SET causal_group_id = %s WHERE command_id = %s",
                lambda _child_id: ("drifted_group",),
            ),
            (
                "causality_schema_drift",
                "UPDATE {schema}.workflow_commands SET causality_schema_version = %s WHERE command_id = %s",
                lambda _child_id: ("command_causality_v999",),
            ),
            (
                "readiness_effect_drift",
                "UPDATE {schema}.workflow_commands SET readiness_effect = %s WHERE command_id = %s",
                lambda _child_id: ("drifted_readiness",),
            ),
            (
                "input_artifact_refs_drift",
                "UPDATE {schema}.workflow_commands SET input_artifact_refs_json = %s WHERE command_id = %s",
                lambda _child_id: (json.dumps(["artifact://drifted"]),),
            ),
            (
                "produced_counts_drift",
                "UPDATE {schema}.workflow_commands SET produced_entity_counts_json = %s WHERE command_id = %s",
                lambda _child_id: (json.dumps({"people": 99}),),
            ),
        )
        for ordinal, (label, sql, extra_params) in enumerate(column_cases, start=1):
            with self.subTest(drift=label):
                start_occurrence, owner_ref = self._arrange_created(suffix=f"child_contract_col_{ordinal}")
                self._release_start_hold(start_occurrence=start_occurrence, suffix=f"child_contract_col_{ordinal}")
                progressed = self._complete_root_with_real_child(
                    owner_ref=owner_ref,
                    suffix=f"child_contract_col_{ordinal}",
                )
                child_id = str(progressed["child"]["command_id"])
                self._execute(sql, (*extra_params(child_id), child_id))
                self._assert_inspect_rejects_without_effects(
                    start_occurrence=start_occurrence,
                    owner_ref=owner_ref,
                    suffix=f"child_contract_col_{ordinal}",
                )

        for label, mutate in (("wrong_type", _foreign_type), ("wrong_owner", _foreign_owner)):
            with self.subTest(drift=label):
                start_occurrence, owner_ref = self._arrange_created(suffix=f"child_contract_{label}")
                self._release_start_hold(start_occurrence=start_occurrence, suffix=f"child_contract_{label}")
                progressed = self._complete_root_with_real_child(
                    owner_ref=owner_ref,
                    suffix=f"child_contract_{label}",
                )
                mutate(str(progressed["child"]["command_id"]))
                self._assert_inspect_rejects_without_effects(
                    start_occurrence=start_occurrence,
                    owner_ref=owner_ref,
                    suffix=f"child_contract_{label}",
                )

    def test_inspect_rejects_progressed_child_plan_event_identity_drift(self) -> None:
        def _event_id_for(progressed: dict[str, Any]) -> str:
            child_row = self._command_row(str(progressed["child"]["command_id"]))
            source_event_id = str(child_row["source_event_id"])
            self.assertTrue(source_event_id)
            return source_event_id

        event_cases = (
            (
                "event_idempotency",
                lambda event_id: self._execute(
                    "UPDATE {schema}.workflow_events SET idempotency_key = %s WHERE event_id = %s",
                    ("drifted_plan_idempotency", event_id),
                ),
            ),
            (
                "event_actor",
                lambda event_id: self._execute(
                    "UPDATE {schema}.workflow_events SET actor = %s WHERE event_id = %s",
                    ("drifted_actor", event_id),
                ),
            ),
            (
                "event_source",
                lambda event_id: self._execute(
                    "UPDATE {schema}.workflow_events SET source = %s WHERE event_id = %s",
                    ("drifted.source", event_id),
                ),
            ),
            (
                "event_nested_payload",
                lambda event_id: self._rewrite_event_payload(
                    event_id,
                    lambda payload: payload["payload"].update(query="drifted query"),
                ),
            ),
            (
                "event_artifact_refs",
                lambda event_id: self._execute(
                    "UPDATE {schema}.workflow_events SET artifact_refs_json = %s WHERE event_id = %s",
                    (json.dumps(["artifact://drifted"]), event_id),
                ),
            ),
            (
                "event_workflow_type",
                lambda event_id: self._rewrite_event_payload(
                    event_id,
                    lambda payload: payload.update(workflow_type="foreign.workflow"),
                ),
            ),
        )
        for ordinal, (label, mutate) in enumerate(event_cases, start=1):
            with self.subTest(drift=label):
                start_occurrence, owner_ref = self._arrange_created(suffix=f"child_event_{ordinal}")
                self._release_start_hold(start_occurrence=start_occurrence, suffix=f"child_event_{ordinal}")
                progressed = self._complete_root_with_real_child(
                    owner_ref=owner_ref,
                    suffix=f"child_event_{ordinal}",
                )
                mutate(_event_id_for(progressed))
                self._assert_inspect_rejects_without_effects(
                    start_occurrence=start_occurrence,
                    owner_ref=owner_ref,
                    suffix=f"child_event_{ordinal}",
                )

    def test_inspect_rejects_progressed_child_semantic_and_version_drift(self) -> None:
        # Self-consistent semantic drift, missing/extra payload fields, and
        # historical contract-pin versions must all fail the builder-rebuilt
        # exact identity instead of passing as valid progression.
        payload_cases = (
            (
                "self_consistent_target_company",
                lambda payload: payload.update(target_company="Drifted Corp"),
            ),
            (
                "missing_field",
                lambda payload: payload.pop("intent_count", None),
            ),
            (
                "extra_field",
                lambda payload: payload.update(unexpected_extra=1),
            ),
            (
                "pin_digest_drift",
                lambda payload: payload["causality"]["progressed_child_contract"].update(digest="0" * 64),
            ),
            (
                "pin_version_drift",
                lambda payload: payload["causality"]["progressed_child_contract"].update(
                    version="progressed_workflow_child_contract_v1"
                ),
            ),
            (
                "pin_removed",
                lambda payload: payload["causality"].pop("progressed_child_contract", None),
            ),
        )
        for ordinal, (label, mutate) in enumerate(payload_cases, start=1):
            with self.subTest(drift=label):
                start_occurrence, owner_ref = self._arrange_created(suffix=f"child_semantic_{ordinal}")
                self._release_start_hold(start_occurrence=start_occurrence, suffix=f"child_semantic_{ordinal}")
                progressed = self._complete_root_with_real_child(
                    owner_ref=owner_ref,
                    suffix=f"child_semantic_{ordinal}",
                )
                self._rewrite_command_payload(str(progressed["child"]["command_id"]), mutate)
                self._assert_inspect_rejects_without_effects(
                    start_occurrence=start_occurrence,
                    owner_ref=owner_ref,
                    suffix=f"child_semantic_{ordinal}",
                )

    def _complete_root_with_drifted_spec(
        self,
        *,
        owner_ref: dict[str, Any],
        suffix: str,
        mutate: Any,
    ) -> dict[str, Any]:
        command_id = str(owner_ref["workflow_command_id"])
        lease_owner = f"s1e2b_first_call_lease_{suffix}"
        lease_expires_at = "2099-01-01 00:00:00"
        self._execute(
            "UPDATE {schema}.workflow_commands SET status = %s, lease_owner = %s, lease_expires_at = %s, "
            "attempt = %s WHERE command_id = %s",
            ("running", lease_owner, lease_expires_at, 1, command_id),
        )
        root = dict(self.store.get_workflow_command(command_id) or {})
        owner = self._orchestrator()._acquisition_command_owner  # noqa: SLF001
        contract = owner._acquisition_root_intent_plan_contract(root, claim_attempt=1)  # noqa: SLF001
        self.assertTrue(contract)
        plan_event = dict(contract.get("plan_event") or {})
        child_command = dict(contract.get("child_command") or {})
        child_causality = dict(contract.get("child_causality") or {})
        root_result = dict(contract.get("root_result") or {})
        mutate(plan_event, child_command, child_causality, root_result)
        return self.repository.complete_acquisition_root_command(
            command_id,
            expected_lease_owner=lease_owner,
            expected_lease_expires_at=lease_expires_at,
            expected_attempt=1,
            expected_root_command=owner._acquisition_root_locked_identity(root),  # noqa: SLF001
            plan_event=plan_event,
            child_command=child_command,
            child_causality=child_causality,
            root_result=root_result,
        )

    def test_first_call_completion_rejects_drifted_spec_without_writes(self) -> None:
        # A first-time completion with a caller-drifted spec must abort before
        # any insert: no plan event, no child, no parent terminal update.
        drift_cases = (
            (
                "event_payload_extra_key",
                lambda event, _child, _causality, _result: event["payload"].update(unexpected_extra=1),
            ),
            (
                "child_payload_drift",
                lambda _event, child, _causality, _result: child["payload"].update(target_company="Drifted Corp"),
            ),
            (
                "retry_policy_drift",
                lambda _event, child, _causality, _result: child["retry_policy"].update(retry_delay_seconds=99),
            ),
            (
                "artifact_refs_drift",
                lambda _event, child, _causality, _result: child.update(artifact_refs=["artifact://drifted"]),
            ),
            (
                "causality_stage_drift",
                lambda _event, _child, causality, _result: causality.update(stage_id="drifted_stage"),
            ),
            (
                "root_result_drift",
                lambda _event, _child, _causality, result: result.update(reason="drifted_reason"),
            ),
        )
        for ordinal, (label, mutate) in enumerate(drift_cases, start=1):
            with self.subTest(drift=label):
                start_occurrence, owner_ref = self._arrange_created(suffix=f"first_call_{ordinal}")
                self._release_start_hold(start_occurrence=start_occurrence, suffix=f"first_call_{ordinal}")
                command_id = str(owner_ref["workflow_command_id"])
                baseline_command_count = len(self._rows("workflow_commands"))
                completed = self._complete_root_with_drifted_spec(
                    owner_ref=owner_ref,
                    suffix=f"first_call_{ordinal}",
                    mutate=mutate,
                )
                self.assertEqual(completed.get("outcome"), "conflict", completed)
                self.assertEqual(
                    completed.get("reason"),
                    "acquisition_root_plan_reconstruction_mismatch",
                    completed,
                )
                self.assertEqual(len(self._rows("workflow_commands")), baseline_command_count)
                self.assertEqual(
                    self._rows("workflow_commands", where="parent_command_id = %s", params=(command_id,)),
                    [],
                )
                self.assertEqual(
                    self._rows(
                        "workflow_events",
                        where="idempotency_key = %s",
                        params=(f"acquisition.intent.resolve:parent:{command_id}:plan",),
                    ),
                    [],
                )
                root_row = self._command_row(command_id)
                self.assertEqual(root_row["status"], "running")
                self.assertEqual(json.loads(root_row["downstream_command_ids_json"]), [])
                self.assertEqual(
                    self._rows(
                        "workflow_entity_deltas",
                        where="command_id = %s",
                        params=(command_id,),
                    ),
                    [],
                )

    def test_inspect_rejects_command_lifecycle_states_outside_the_explicit_contract(self) -> None:
        cases = (
            (
                "hold_with_linked_children",
                "UPDATE {schema}.workflow_commands SET downstream_command_ids_json = %s WHERE command_id = %s",
                lambda command_id: (json.dumps(["cmd_s1e2b_child_1"]), command_id),
            ),
            (
                "progressed_with_retry_backoff",
                "UPDATE {schema}.workflow_commands SET not_before_at = %s, downstream_command_ids_json = %s "
                "WHERE command_id = %s",
                lambda command_id: ("2026-07-19 09:30:00", json.dumps(["cmd_s1e2b_child_2"]), command_id),
            ),
            (
                "invalid_not_before_marker",
                "UPDATE {schema}.workflow_commands SET not_before_at = %s WHERE command_id = %s",
                lambda command_id: ("released", command_id),
            ),
            (
                "duplicate_linked_children",
                "UPDATE {schema}.workflow_commands SET not_before_at = '', downstream_command_ids_json = %s "
                "WHERE command_id = %s",
                lambda command_id: (json.dumps(["cmd_s1e2b_child_3", "cmd_s1e2b_child_3"]), command_id),
            ),
            (
                "non_string_linked_child",
                "UPDATE {schema}.workflow_commands SET not_before_at = '', downstream_command_ids_json = %s "
                "WHERE command_id = %s",
                lambda command_id: (json.dumps([7]), command_id),
            ),
            (
                "blank_linked_child",
                "UPDATE {schema}.workflow_commands SET not_before_at = '', downstream_command_ids_json = %s "
                "WHERE command_id = %s",
                lambda command_id: (json.dumps([" cmd_s1e2b_child_4 "]), command_id),
            ),
        )
        for ordinal, (label, sql, params_for) in enumerate(cases, start=1):
            with self.subTest(lifecycle=label):
                start_occurrence, owner_ref = self._arrange_created(suffix=f"lifecycle_reject_{ordinal}")
                command_id = str(owner_ref["workflow_command_id"])
                self._execute(sql, params_for(command_id))
                self._assert_inspect_rejects_without_effects(
                    start_occurrence=start_occurrence,
                    owner_ref=owner_ref,
                    suffix=f"lifecycle_reject_{ordinal}",
                )

    def test_inspect_preparation_follows_released_command_with_retry_backoff_timestamp(self) -> None:
        start_occurrence, owner_ref = self._arrange_created(suffix="lifecycle_backoff")
        command_id = str(owner_ref["workflow_command_id"])
        self._execute(
            "UPDATE {schema}.workflow_commands SET not_before_at = %s WHERE command_id = %s",
            ("2026-07-19 09:30:00+00:00", command_id),
        )

        backoff_occurrence = self._inspect_occurrence(
            start_occurrence=start_occurrence,
            owner_ref=owner_ref,
            suffix="lifecycle_backoff",
        )
        backoff_terminal = self._prepare_inspect(
            occurrence=backoff_occurrence,
            owner_ref=owner_ref,
            suffix="lifecycle_backoff",
        )
        self.assertEqual(backoff_terminal.serialized_result["variant"], "success")

    def test_owner_ref_digest_helper_is_type_sensitive(self) -> None:
        self.assertNotEqual(_digest({"sequence": True}), _digest({"sequence": 1}))
        self.assertNotEqual(_digest({"sequence": 2.0}), _digest({"sequence": 2}))


class ProgressedChildContractUnitTest(unittest.TestCase):
    """Pure contract-level probes: exact shape, no trimming, durable pin."""

    @staticmethod
    def _synthetic_root() -> dict[str, Any]:
        return {
            "command_id": "cmd_unit_root",
            "workflow_run_id": "wf_unit_root",
            "operation_id": "op_unit_root",
            "command_type": "acquisition.run.create",
            "owner": "acquisition_run_writer",
            "causal_group_id": "",
            "payload": {
                "workflow_payload": {"target_company": "Acme", "query": "find people"},
                "target_company": "Acme",
                "query": "find people",
                "plan_review_id": "plan-review-1",
                "action_id": "action-1",
            },
        }

    def _expected_bundle(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], str]:
        plan = build_acquisition_root_intent_plan(self._synthetic_root(), claim_attempt=1)
        self.assertTrue(plan)
        contract = progressed_child_completion_contract("acquisition_root")
        assert contract is not None
        event_id = progressed_child_plan_event_id("wf_unit_root", 2, plan["plan_event"]["idempotency_key"])
        identity = canonical_progressed_child_identity(
            expected_progressed_child_row(
                contract=contract,
                parent_command_id="cmd_unit_root",
                workflow_run_id="wf_unit_root",
                operation_id="op_unit_root",
                child_command=plan["child_command"],
                child_causality={
                    **plan["child_causality"],
                    "source_event_id": event_id,
                    "source_event_type": "CommandPlanRequested",
                },
            )
        )
        self.assertIsNotNone(identity)
        assert identity is not None
        return plan, contract, identity, event_id

    def _event_row(self, plan: dict[str, Any], event_id: str, **overrides: Any) -> dict[str, Any]:
        row = {
            "event_id": event_id,
            "workflow_run_id": "wf_unit_root",
            "operation_id": "op_unit_root",
            "command_id": "cmd_unit_root",
            "activity_attempt_id": "",
            "event_family": "workflow_event",
            "event_type": "CommandPlanRequested",
            "sequence_number": 2,
            "idempotency_key": plan["plan_event"]["idempotency_key"],
            "actor": "acquisition_run_create_owner",
            "source": "acquisition_run_create.command_owner",
            "payload": dict(plan["plan_event"]["payload"]),
            "artifact_refs": [],
            "schema_version": "workflow_event_v1",
        }
        row.update(overrides)
        return row

    def _violation(
        self, plan: dict[str, Any], contract: dict[str, Any], identity: dict[str, Any], event: dict[str, Any]
    ) -> str:
        return progressed_child_plan_event_violation(
            contract=contract,
            parent_command_id="cmd_unit_root",
            parent_source_sequence=1,
            child_identity=identity,
            event=event,
            expected_payload=dict(plan["plan_event"]["payload"]),
        )

    def test_valid_constructed_bundle_passes(self) -> None:
        plan, contract, identity, event_id = self._expected_bundle()
        self.assertEqual(self._violation(plan, contract, identity, self._event_row(plan, event_id)), "")

    def test_padded_and_drifted_event_fields_fail_exact_comparison(self) -> None:
        plan, contract, identity, event_id = self._expected_bundle()
        cases = (
            ("padded_actor", {"actor": " acquisition_run_create_owner "}),
            ("padded_source", {"source": "acquisition_run_create.command_owner "}),
            ("padded_workflow_run", {"workflow_run_id": " wf_unit_root "}),
            ("padded_schema_version", {"schema_version": " workflow_event_v1 "}),
            ("foreign_workflow_type", None),
            ("extra_payload_key", "extra"),
            ("missing_payload_key", "missing"),
        )
        for label, overrides in cases:
            with self.subTest(case=label):
                event = self._event_row(plan, event_id)
                if overrides is None:
                    event["payload"] = {**event["payload"], "workflow_type": "foreign.workflow"}
                elif overrides == "extra":
                    event["payload"] = {**event["payload"], "unexpected_extra": 1}
                elif overrides == "missing":
                    event["payload"] = {
                        key: value for key, value in dict(event["payload"]).items() if key != "stage_key"
                    }
                else:
                    event.update(overrides)
                self.assertNotEqual(self._violation(plan, contract, identity, event), "", label)

    def test_mirror_consistent_drift_is_not_the_expected_identity(self) -> None:
        plan, contract, identity, event_id = self._expected_bundle()
        row = expected_progressed_child_row(
            contract=contract,
            parent_command_id="cmd_unit_root",
            workflow_run_id="wf_unit_root",
            operation_id="op_unit_root",
            child_command=plan["child_command"],
            child_causality={
                **plan["child_causality"],
                "source_event_id": event_id,
                "source_event_type": "CommandPlanRequested",
            },
        )

        def _drifted(row_mutate: Any) -> dict[str, Any]:
            drifted = {**row, "payload": {**row["payload"], "causality": dict(row["payload"]["causality"])}}
            row_mutate(drifted)
            return drifted

        # Padded stage, mirror-consistent (column and causality padded alike):
        # identity is computable but must not equal the builder's exact row.
        padded = _drifted(
            lambda r: (
                r.update(stage_id=f" {r['stage_id']} "),
                r["payload"]["causality"].update(stage_id=r["stage_id"]),
            )
        )
        padded_identity = canonical_progressed_child_identity(padded)
        self.assertIsNotNone(padded_identity)
        self.assertNotEqual(padded_identity, identity)

        # Self-consistent foreign values are likewise not the expected row.
        foreign = _drifted(
            lambda r: (
                r.update(stage_id="foreign_stage", causal_group_id="foreign_group"),
                r["payload"]["causality"].update(stage_id="foreign_stage", causal_group_id="foreign_group"),
            )
        )
        foreign_identity = canonical_progressed_child_identity(foreign)
        self.assertIsNotNone(foreign_identity)
        self.assertNotEqual(foreign_identity, identity)

        # A missing or drifted contract pin fails identity computation itself.
        unpinned = _drifted(lambda r: r["payload"]["causality"].pop("progressed_child_contract", None))
        self.assertIsNone(canonical_progressed_child_identity(unpinned))
        foreign_pinned = _drifted(
            lambda r: r["payload"]["causality"]["progressed_child_contract"].update(digest="0" * 64)
        )
        self.assertIsNone(canonical_progressed_child_identity(foreign_pinned))


if __name__ == "__main__":
    unittest.main()
