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
from sourcing_agent.control_plane_live_postgres import ControlPlaneAdvisoryLockBusy
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
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

    def test_owner_ref_digest_helper_is_type_sensitive(self) -> None:
        self.assertNotEqual(_digest({"sequence": True}), _digest({"sequence": 1}))
        self.assertNotEqual(_digest({"sequence": 2.0}), _digest({"sequence": 2}))
