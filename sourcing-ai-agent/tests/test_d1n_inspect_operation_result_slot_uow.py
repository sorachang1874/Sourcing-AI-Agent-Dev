from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest import mock

from sourcing_agent.agent_canary_registry import (
    INSPECT_OPERATION_TOOL_SPEC,
    INSPECT_OPERATION_TOOL_SPEC_V1,
    INSPECT_OPERATION_TOOL_SPEC_V2,
)
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence, AgentToolResultSlotError
from sourcing_agent.control_plane_repository import ControlPlaneAuthoritativeReadError
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin
from tests.test_d1n_acquisition_plan_preview_uow import _uow_kwargs


class _EqualityAliasString(str):
    """Carry forged bytes while also comparing equal to one expected value."""

    _equal_to: str

    def __new__(cls, value: str, *, equal_to: str) -> _EqualityAliasString:
        instance = super().__new__(cls, value)
        instance._equal_to = equal_to
        return instance

    def __eq__(self, other: object) -> bool:
        return bool(str.__eq__(self, other)) or other == self._equal_to

    __hash__ = str.__hash__


class D1nInspectOperationResultSlotUowPGTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1n_inspect_result"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(Path(self.tempdir.name) / "inspect-operation-result.db")
        self.repository = self.store.repos.workflow_runtime
        self.adapter = self.store._control_plane_postgres  # noqa: SLF001

    def _preview_bundle(self, *, suffix: str = "1") -> dict[str, Any]:
        return self.repository.create_acquisition_plan_preview_uow(**_uow_kwargs(suffix=suffix))

    def _command_backed_bundle(self, *, event_command_id: str = "") -> dict[str, Any]:
        from sourcing_agent.operation_runtime import ACTION_START_ACQUISITION_RUN, DEFAULT_ACTION_REGISTRY

        spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN)
        action_id = "act_start_inspect"
        operation_run_id = "oprun_start_inspect"
        workflow_run_id = "workflow_start_inspect"
        command_id = "command_start_inspect"
        workflow_ref = {
            "workflow_run_id": workflow_run_id,
            "command_id": command_id,
            "command_type": spec.default_workflow_command_type,
            "owner": spec.owner_module,
        }
        # Since a98e3df ("Close S1e2b fixed-forward closure review findings
        # (FF-G)", 2026-07-19) a start-candidate Operation carrying the exact
        # `acquisition.run.create` workflow reference WITHOUT the legacy request
        # pin pair is classified as drifted start-v2 provenance and the generic
        # control preflight fails closed (`operation_event_v1` planned events
        # are then rejected).  This fixture models the legacy schema-defined
        # start path, so it must carry the schema-defined legacy request pins
        # to stay legacy-coherent (`non_v2` -> preflight `ready`).  The old
        # pin-free fixture pinned the pre-FF-G downgrade-to-non_v2 behavior
        # that FF-G finding #2 intentionally removed.
        action = self.repository.upsert_action(
            action_id=action_id,
            workspace_id="workspace_1",
            conversation_id="conversation_1",
            action_type=spec.action_type,
            owner_module=spec.owner_module,
            operation_type=spec.operation_type,
            target_ref={},
            input_payload={},
            request_schema_version=spec.request_schema_version,
            request_schema_digest=spec.request_schema_digest,
            approval_status="approved",
            approval_policy=spec.approval_policy,
            budget={"max_cost_micro_usd": 1_000_000},
            idempotency_key="start-inspect",
            status="planned",
        )
        operation = self.repository.upsert_operation(
            operation_run_id=operation_run_id,
            workspace_id="workspace_1",
            action_id=action_id,
            owner_module=spec.owner_module,
            operation_type=spec.operation_type,
            request_schema_version=spec.request_schema_version,
            request_schema_digest=spec.request_schema_digest,
            status="running",
            progress={"phase": "workflow_command_planned"},
            workflow_ref=workflow_ref,
            idempotency_key="start-inspect",
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id=workflow_run_id,
            operation_id=operation_run_id,
            command_id=command_id,
            command_type=spec.default_workflow_command_type,
            owner=spec.owner_module,
            idempotency_key="start-inspect-command",
            payload={"fixture": "inspect_operation"},
        )
        event = self.repository.append_operation_event(
            event_stream_id=operation_run_id,
            event_family="operation_event",
            event_type="OperationCommandPlanned",
            idempotency_key="start-inspect-event",
            workspace_id="workspace_1",
            operation_run_id=operation_run_id,
            action_id=action_id,
            actor="test-user",
            source="test.d1n.inspect",
            payload={
                **workflow_ref,
                "command_id": event_command_id or command_id,
                "module_state_mutated": False,
            },
        )
        return {"action": action, "operation_run": operation, "workflow_command": command, "event": event}

    def _occurrence(
        self,
        bundle: dict[str, Any],
        *,
        suffix: str = "1",
        workspace_id: str = "workspace_1",
        actor_id: str = "requester_1",
        tool_spec=INSPECT_OPERATION_TOOL_SPEC,
    ) -> AgentToolOccurrence:
        operation = dict(bundle["operation_run"])
        return AgentToolOccurrence.from_tool_spec(
            result_slot_id=f"inspectslot_{suffix}",
            slot_generation=1,
            workspace_id=workspace_id,
            actor_id=actor_id,
            runtime_namespace="isolated_local_canary",
            provider_mode="simulate",
            turn_id=f"turn_inspect_{suffix}",
            step_id="step_inspect",
            tool_spec=tool_spec,
            canonical_args={"operation_run_id": str(operation["operation_run_id"])},
            occurrence_ordinal=1,
        )

    def _prepare(
        self,
        bundle: dict[str, Any],
        occurrence: AgentToolOccurrence,
        *,
        attempt_id: str = "inspectattempt_1",
        action_id: str = "",
    ) -> Any:
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        return self.repository.prepare_inspect_operation_tool_result(
            occurrence=occurrence,
            result_attempt_id=attempt_id,
            provider_call_id=f"provider-{attempt_id}",
            tool_call_id=f"tool-{attempt_id}",
            action_id=action_id or str(action["action_id"]),
            operation_run_id=str(operation["operation_run_id"]),
        )

    def _result_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                for table_name in (
                    "agent_tool_result_slots",
                    "agent_tool_result_attempts",
                    "agent_tool_result_journal",
                ):
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    counts[table_name] = int(cursor.fetchone()[0])
        return counts

    def _seed_brownfield_progress_reason(self, operation_run_id: str, reason: str) -> dict[str, Any]:
        """Seed retained pre-registry reason text through the migration-level native adapter."""

        current = self.repository.get_operation(operation_run_id)
        updated = self.adapter.update_operation_run_state(
            operation_run_id,
            table_name="operation_runs",
            expected_status=str(current["status"]),
            status=str(current["status"]),
            progress={**dict(current.get("progress") or {}), "reason": reason},
        )
        if updated is None:
            raise AssertionError("brownfield operation progress seed produced no row")
        return self.repository.get_operation(operation_run_id)

    def _assert_pending_without_terminal_effects(self, occurrence: AgentToolOccurrence) -> None:
        slot = self.repository.get_agent_tool_result_slot(
            occurrence.result_slot_id,
            workspace_id=occurrence.workspace_id,
            actor_id=occurrence.actor_id,
            runtime_namespace=occurrence.runtime_namespace,
            provider_mode="simulate",
        )
        self.assertEqual(slot["status"], "pending")
        counts = self._result_counts()
        self.assertEqual(counts["agent_tool_result_attempts"], 0)
        self.assertEqual(counts["agent_tool_result_journal"], 0)

    def test_prepare_accept_lost_ack_replays_and_keeps_operation_domain_unchanged(self) -> None:
        bundle = self._preview_bundle()
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        occurrence = self._occurrence(bundle)
        terminal = self._prepare(bundle, occurrence)
        self.assertEqual(terminal.serialized_result["variant"], "success")
        self.assertEqual(terminal.serialized_result["status"], "ready")
        self.assertEqual(terminal.owner_target_id, operation["operation_run_id"])
        self.assertEqual(terminal.terminal_winner_id, dict(bundle["event"])["event_id"])
        self.assertFalse(terminal.workflow_command_id)

        before_action = self.repository.get_action(str(action["action_id"]))
        before_operation = self.repository.get_operation(str(operation["operation_run_id"]))
        before_events = self.repository.list_operation_events(str(operation["operation_run_id"]))
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)

        with self.assertRaisesRegex(RuntimeError, "after commit"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
                fault_injection_point="after_commit",
            )
        replay = self.repository.accept_inspect_operation_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=1,
        )

        self.assertEqual(replay["outcome"], "replayed")
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["slot"]["status"], "accepted")
        self.assertEqual(replay["slot"]["tool_result_message"]["content"], terminal.serialized_result_json)
        self.assertEqual(self.repository.get_action(str(action["action_id"])), before_action)
        self.assertEqual(
            self.repository.get_operation(str(operation["operation_run_id"])),
            before_operation,
        )
        self.assertEqual(
            self.repository.list_operation_events(str(operation["operation_run_id"])),
            before_events,
        )
        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 1,
                "agent_tool_result_attempts": 1,
                "agent_tool_result_journal": 1,
            },
        )

    def test_retained_v1_v2_occurrences_prepare_accept_and_replay_with_their_exact_serializer(self) -> None:
        historical_specs = (INSPECT_OPERATION_TOOL_SPEC_V1, INSPECT_OPERATION_TOOL_SPEC_V2)
        for index, tool_spec in enumerate(historical_specs, start=1):
            with self.subTest(tool_spec_version=tool_spec.tool_spec_version):
                suffix = f"history_{index}"
                bundle = self._preview_bundle(suffix=suffix)
                operation = dict(bundle["operation_run"])
                updated = self._seed_brownfield_progress_reason(
                    str(operation["operation_run_id"]),
                    "operation_retry_requested",
                )
                bundle["operation_run"] = updated
                occurrence = self._occurrence(bundle, suffix=suffix, tool_spec=tool_spec)
                terminal = self._prepare(
                    bundle,
                    occurrence,
                    attempt_id=f"inspectattempt_{suffix}",
                )

                self.assertEqual(terminal.serialized_result["progress"]["reason"], "operation_retry_requested")
                self.assertEqual(occurrence.result_schema_version, tool_spec.result.schema_version)
                self.assertEqual(
                    terminal.owner_result_ref["physical_owner_fingerprint_schema_version"],
                    "inspect_operation_physical_owner_fingerprint_v2",
                )
                self.assertNotIn("audit_event_stream_digest", terminal.owner_result_ref)
                self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
                with self.assertRaisesRegex(RuntimeError, "after commit"):
                    self.adapter.accept_inspect_operation_tool_result_uow(
                        occurrence=occurrence,
                        terminal=terminal,
                        attempted_slot_generation=1,
                        fault_injection_point="after_commit",
                    )
                replay = self.repository.accept_inspect_operation_tool_result_uow(
                    occurrence=occurrence,
                    terminal=terminal,
                    attempted_slot_generation=1,
                )
                self.assertEqual(replay["outcome"], "replayed")
                self.assertTrue(replay["replayed"])

    def test_v3_omits_operator_reason_text_but_fences_its_raw_progress_digest(self) -> None:
        reasons = (
            "Retry after operator review",
            "人工复核后重试",
            "operator note " + ("x" * 500),
        )
        for index, reason in enumerate(reasons, start=1):
            with self.subTest(reason=reason[:32]):
                suffix = f"operator_reason_{index}"
                bundle = self._preview_bundle(suffix=suffix)
                operation = dict(bundle["operation_run"])
                updated = self._seed_brownfield_progress_reason(
                    str(operation["operation_run_id"]),
                    reason,
                )
                bundle["operation_run"] = updated
                occurrence = self._occurrence(bundle, suffix=suffix)
                terminal = self._prepare(
                    bundle,
                    occurrence,
                    attempt_id=f"inspectattempt_{suffix}",
                )

                self.assertNotIn("reason", terminal.serialized_result["progress"])
                self.assertNotIn(reason, terminal.serialized_result_json)
                self.assertEqual(
                    terminal.owner_result_ref["physical_owner_fingerprint_schema_version"],
                    "inspect_operation_physical_owner_fingerprint_v3",
                )
                self.assertEqual(len(terminal.owner_result_ref["raw_progress_digest"]), 64)
                self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
                accepted = self.repository.accept_inspect_operation_tool_result_uow(
                    occurrence=occurrence,
                    terminal=terminal,
                    attempted_slot_generation=1,
                )
                self.assertEqual(accepted["outcome"], "accepted")

    def test_v3_raw_operator_reason_drift_after_prepare_leaves_slot_pending(self) -> None:
        bundle = self._preview_bundle(suffix="operator_reason_drift")
        operation = dict(bundle["operation_run"])
        first = self._seed_brownfield_progress_reason(
            str(operation["operation_run_id"]),
            "first operator note",
        )
        bundle["operation_run"] = first
        occurrence = self._occurrence(bundle, suffix="operator_reason_drift")
        terminal = self._prepare(bundle, occurrence, attempt_id="inspectattempt_operator_reason_drift")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        self._seed_brownfield_progress_reason(
            str(operation["operation_run_id"]),
            "second operator note",
        )

        with self.assertRaisesRegex(ValueError, "physical owner or serializer mismatch"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )
        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 1,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_v3_event_actor_and_source_drift_leave_slot_pending_without_terminal_effects(self) -> None:
        for index, field_name in enumerate(("actor", "source"), start=1):
            with self.subTest(field_name=field_name):
                suffix = f"audit_{field_name}_{index}"
                bundle = self._preview_bundle(suffix=suffix)
                occurrence = self._occurrence(bundle, suffix=suffix)
                terminal = self._prepare(
                    bundle,
                    occurrence,
                    attempt_id=f"inspectattempt_{suffix}",
                )
                self.assertEqual(len(terminal.owner_result_ref["audit_event_stream_digest"]), 64)
                self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
                event = dict(bundle["event"])
                with self.adapter._connect() as connection:  # noqa: SLF001
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"UPDATE operation_events SET {field_name} = %s WHERE event_id = %s",
                            (f"drifted.{field_name}", event["event_id"]),
                        )

                with self.assertRaisesRegex(ValueError, "physical owner or serializer mismatch"):
                    self.adapter.accept_inspect_operation_tool_result_uow(
                        occurrence=occurrence,
                        terminal=terminal,
                        attempted_slot_generation=1,
                    )
                self._assert_pending_without_terminal_effects(occurrence)

    def test_v3_invalid_event_actor_or_source_fails_before_result_writes(self) -> None:
        for index, field_name in enumerate(("actor", "source"), start=1):
            with self.subTest(field_name=field_name):
                suffix = f"invalid_audit_{field_name}_{index}"
                bundle = self._preview_bundle(suffix=suffix)
                occurrence = self._occurrence(bundle, suffix=suffix)
                event = dict(bundle["event"])
                with self.adapter._connect() as connection:  # noqa: SLF001
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"UPDATE operation_events SET {field_name} = %s WHERE event_id = %s",
                            (" ", event["event_id"]),
                        )

                with self.assertRaisesRegex(ValueError, f"operation event {field_name} invalid"):
                    self._prepare(
                        bundle,
                        occurrence,
                        attempt_id=f"inspectattempt_{suffix}",
                    )
                self.assertEqual(
                    self._result_counts(),
                    {
                        "agent_tool_result_slots": 0,
                        "agent_tool_result_attempts": 0,
                        "agent_tool_result_journal": 0,
                    },
                )

    def test_mixed_or_unknown_historical_inspect_pins_fail_before_owner_read_with_zero_writes(self) -> None:
        bundle = self._preview_bundle(suffix="historical_pin_drift")
        occurrence = self._occurrence(
            bundle,
            suffix="historical_pin_drift",
            tool_spec=INSPECT_OPERATION_TOOL_SPEC_V1,
        )
        forged_result = replace(
            occurrence,
            result_schema_version=INSPECT_OPERATION_TOOL_SPEC_V2.result.schema_version,
            result_schema_digest=INSPECT_OPERATION_TOOL_SPEC_V2.result.schema_digest,
            serializer_owner=INSPECT_OPERATION_TOOL_SPEC_V2.result.serializer_owner.owner_id,
            serializer_revision=INSPECT_OPERATION_TOOL_SPEC_V2.result.serializer_owner.owner_revision,
            serializer_contract_digest=(INSPECT_OPERATION_TOOL_SPEC_V2.result.serializer_owner.owner_contract_digest),
        )
        unknown_tool = replace(occurrence, tool_spec_digest="f" * 64)

        with mock.patch.object(self.adapter, "_connect_with_timeout") as connect:
            for forged, message in (
                (forged_result, "historical_spec_pin_mismatch"),
                (unknown_tool, "historical_spec_missing"),
            ):
                with self.subTest(message=message):
                    with self.assertRaisesRegex(ValueError, message):
                        self._prepare(bundle, forged, attempt_id=f"inspectattempt_{message}")
            connect.assert_not_called()
        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_equality_alias_occurrence_is_rejected_before_inspect_prepare_owner_access(self) -> None:
        cases = (
            ("provider_mode", "live", "provider_mode_invalid"),
            ("canonical_args_json", "{}", "canonical_args_invalid"),
        )

        for index, (field_name, forged_value, expected_error) in enumerate(cases, start=1):
            with self.subTest(field_name=field_name):
                bundle = self._preview_bundle(suffix=f"prepare_alias_{index}")
                occurrence = self._occurrence(bundle, suffix=f"prepare_alias_{index}")
                object.__setattr__(
                    occurrence,
                    field_name,
                    _EqualityAliasString(forged_value, equal_to=str(getattr(occurrence, field_name))),
                )
                if field_name == "canonical_args_json":
                    object.__setattr__(
                        occurrence,
                        "canonical_args_digest",
                        hashlib.sha256(forged_value.encode("utf-8")).hexdigest(),
                    )

                with (
                    mock.patch.object(self.adapter, "_ensure_table_write_schema") as ensure_schema,
                    mock.patch.object(self.adapter, "_connect_with_timeout") as connect,
                    self.assertRaisesRegex(AgentToolResultSlotError, expected_error),
                ):
                    self._prepare(
                        bundle,
                        occurrence,
                        attempt_id=f"inspectattempt_prepare_alias_{index}",
                    )
                ensure_schema.assert_not_called()
                connect.assert_not_called()

        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_equality_alias_occurrence_is_rejected_before_inspect_accept_owner_access(self) -> None:
        bundle = self._preview_bundle(suffix="accept_occurrence_alias")
        occurrence = self._occurrence(bundle, suffix="accept_occurrence_alias")
        terminal = self._prepare(
            bundle,
            occurrence,
            attempt_id="inspectattempt_accept_occurrence_alias",
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        object.__setattr__(
            occurrence,
            "canonical_args_json",
            _EqualityAliasString("{}", equal_to=occurrence.canonical_args_json),
        )
        object.__setattr__(
            occurrence,
            "canonical_args_digest",
            hashlib.sha256(b"{}").hexdigest(),
        )

        with (
            mock.patch.object(self.adapter, "_ensure_table_write_schema") as ensure_schema,
            mock.patch.object(self.adapter, "_connect_with_timeout") as connect,
            self.assertRaisesRegex(AgentToolResultSlotError, "canonical_args_invalid"),
        ):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )
        ensure_schema.assert_not_called()
        connect.assert_not_called()
        self._assert_pending_without_terminal_effects(occurrence)

    def test_equality_alias_terminal_json_is_rejected_before_inspect_accept_owner_access(self) -> None:
        cases = (
            ("owner_result_ref_json", "{}", "owner_result_ref_invalid"),
            ("serialized_result_json", "{}", "serialized_result_invalid"),
        )

        for index, (field_name, forged_value, expected_error) in enumerate(cases, start=1):
            with self.subTest(field_name=field_name):
                suffix = f"accept_terminal_alias_{index}"
                bundle = self._preview_bundle(suffix=suffix)
                occurrence = self._occurrence(bundle, suffix=suffix)
                terminal = self._prepare(
                    bundle,
                    occurrence,
                    attempt_id=f"inspectattempt_{suffix}",
                )
                self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
                object.__setattr__(
                    terminal,
                    field_name,
                    _EqualityAliasString(forged_value, equal_to=str(getattr(terminal, field_name))),
                )
                if field_name == "serialized_result_json":
                    object.__setattr__(
                        terminal,
                        "serialized_result_digest",
                        hashlib.sha256(forged_value.encode("utf-8")).hexdigest(),
                    )

                with (
                    mock.patch.object(self.adapter, "_ensure_table_write_schema") as ensure_schema,
                    mock.patch.object(self.adapter, "_connect_with_timeout") as connect,
                    self.assertRaisesRegex(AgentToolResultSlotError, expected_error),
                ):
                    self.adapter.accept_inspect_operation_tool_result_uow(
                        occurrence=occurrence,
                        terminal=terminal,
                        attempted_slot_generation=1,
                    )
                ensure_schema.assert_not_called()
                connect.assert_not_called()
                self._assert_pending_without_terminal_effects(occurrence)

    def test_malformed_request_identifiers_fail_before_prepare_dependencies_or_masked_absence(self) -> None:
        bundle = self._preview_bundle(suffix="malformed_request")
        operation = dict(bundle["operation_run"])
        action = dict(bundle["action"])
        cases: tuple[tuple[str, AgentToolOccurrence, str, str, str], ...] = (
            (
                "workspace",
                self._occurrence(
                    bundle,
                    suffix="malformed_workspace",
                    workspace_id="workspace invalid",
                ),
                str(action["action_id"]),
                str(operation["operation_run_id"]),
                "workspace_id",
            ),
            (
                "actor",
                self._occurrence(
                    bundle,
                    suffix="malformed_actor",
                    actor_id="requester invalid",
                ),
                str(action["action_id"]),
                str(operation["operation_run_id"]),
                "actor_id",
            ),
            (
                "masked_action",
                self._occurrence(bundle, suffix="malformed_action"),
                " action_missing",
                str(operation["operation_run_id"]),
                "action_id",
            ),
            (
                "operation",
                AgentToolOccurrence.from_tool_spec(
                    result_slot_id="inspectslot_malformed_operation",
                    slot_generation=1,
                    workspace_id="workspace_1",
                    actor_id="requester_1",
                    runtime_namespace="isolated_local_canary",
                    provider_mode="simulate",
                    turn_id="turn_inspect_malformed_operation",
                    step_id="step_inspect",
                    tool_spec=INSPECT_OPERATION_TOOL_SPEC,
                    canonical_args={"operation_run_id": "operation invalid"},
                    occurrence_ordinal=1,
                ),
                str(action["action_id"]),
                "operation invalid",
                "operation_run_id",
            ),
        )

        for label, occurrence, action_id, operation_run_id, invalid_field in cases:
            with self.subTest(label=label):
                expected_error = (
                    "inspect_operation_request_invalid"
                    if label == "operation"
                    else f"identifier_invalid: {invalid_field}"
                )
                with (
                    mock.patch.object(self.adapter, "_ensure_table_write_schema") as ensure_schema,
                    mock.patch.object(self.adapter, "_connect_with_timeout") as connect,
                    self.assertRaisesRegex(ValueError, expected_error),
                ):
                    self.repository.prepare_inspect_operation_tool_result(
                        occurrence=occurrence,
                        result_attempt_id=f"inspectattempt_malformed_{label}",
                        provider_call_id=f"provider-malformed-{label}",
                        tool_call_id=f"tool-malformed-{label}",
                        action_id=action_id,
                        operation_run_id=operation_run_id,
                    )
                ensure_schema.assert_not_called()
                connect.assert_not_called()

        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_malformed_bound_request_fails_before_accept_dependencies_and_keeps_slot_pending(self) -> None:
        bundle = self._preview_bundle(suffix="malformed_accept")
        occurrence = self._occurrence(bundle, suffix="malformed_accept")
        terminal = self._prepare(
            bundle,
            occurrence,
            attempt_id="inspectattempt_malformed_accept",
            action_id="action_missing_malformed_accept",
        )
        self.assertTrue(terminal.is_error)
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        canonical_actor = occurrence.actor_id
        object.__setattr__(occurrence, "actor_id", "requester invalid")

        with (
            mock.patch.object(self.adapter, "_ensure_table_write_schema") as ensure_schema,
            mock.patch.object(self.adapter, "_connect_with_timeout") as connect,
            self.assertRaisesRegex(ValueError, "identifier_invalid: actor_id"),
        ):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )
        ensure_schema.assert_not_called()
        connect.assert_not_called()
        object.__setattr__(occurrence, "actor_id", canonical_actor)
        self._assert_pending_without_terminal_effects(occurrence)

    def test_prepare_is_read_only_and_never_bootstraps_write_schema(self) -> None:
        bundle = self._preview_bundle(suffix="readonly")
        occurrence = self._occurrence(bundle, suffix="readonly")

        with mock.patch.object(
            self.adapter,
            "_ensure_table_write_schema",
            wraps=self.adapter._ensure_table_write_schema,  # noqa: SLF001
        ) as ensure_write_schema:
            terminal = self._prepare(bundle, occurrence, attempt_id="inspectattempt_readonly")

        self.assertEqual(terminal.serialized_result["status"], "ready")
        ensure_write_schema.assert_not_called()

    def test_prepare_wraps_infrastructure_failure_and_returns_masked_domain_error(self) -> None:
        bundle = self._preview_bundle(suffix="read-failure")
        occurrence = self._occurrence(bundle, suffix="read-failure")

        with mock.patch.object(
            self.adapter,
            "prepare_inspect_operation_tool_result",
            side_effect=ConnectionError("synthetic owner read failure"),
        ):
            with self.assertRaisesRegex(ControlPlaneAuthoritativeReadError, "ConnectionError"):
                self._prepare(bundle, occurrence, attempt_id="inspectattempt_read_failure")

        terminal = self._prepare(
            bundle,
            occurrence,
            attempt_id="inspectattempt_domain_failure",
            action_id="action_missing_domain_failure",
        )
        self.assertTrue(terminal.is_error)
        self.assertEqual(
            terminal.serialized_result,
            {
                "variant": "error",
                "status": "failed",
                "reason": "operation_not_found",
                "retryable": False,
            },
        )

    def test_event_revision_drift_before_acceptance_leaves_slot_pending(self) -> None:
        bundle = self._preview_bundle()
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        occurrence = self._occurrence(bundle)
        terminal = self._prepare(bundle, occurrence)
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        drift_event = self.repository.append_operation_event(
            event_stream_id=str(operation["operation_run_id"]),
            event_family="operation_event",
            event_type="OperationInspectionRevisionAdvanced",
            idempotency_key="inspect-revision-drift",
            workspace_id="workspace_1",
            operation_run_id=str(operation["operation_run_id"]),
            action_id=str(action["action_id"]),
            actor="test-user",
            source="test.d1n.inspect",
            payload={"reason": "revision_drift_fixture"},
        )
        self.assertTrue(drift_event)

        with self.assertRaisesRegex(ValueError, "physical owner or serializer mismatch"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )

        slot = self.repository.get_agent_tool_result_slot(
            occurrence.result_slot_id,
            workspace_id=occurrence.workspace_id,
            actor_id=occurrence.actor_id,
            runtime_namespace=occurrence.runtime_namespace,
            provider_mode=occurrence.provider_mode,
        )
        self.assertEqual(slot["status"], "pending")
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

    def test_stale_generation_and_late_winner_require_owner_before_quarantine_write(self) -> None:
        bundle = self._preview_bundle(suffix="quarantine-owner")
        occurrence = self._occurrence(bundle, suffix="quarantine-owner")
        terminal = self._prepare(bundle, occurrence, attempt_id="inspectattempt_quarantine_owner")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)

        stale_missing_owner = replace(
            terminal,
            result_attempt_id="inspectattempt_stale_missing_owner",
            action_id="action_missing_stale",
        )
        with self.assertRaisesRegex(ValueError, "physical owner or serializer mismatch"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=stale_missing_owner,
                attempted_slot_generation=2,
            )
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)

        accepted = self.repository.accept_inspect_operation_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=1,
        )
        self.assertEqual(accepted["outcome"], "accepted")

        late_missing_owner = replace(
            terminal,
            result_attempt_id="inspectattempt_late_missing_owner",
            action_id="action_missing_late",
        )
        with self.assertRaisesRegex(ValueError, "physical owner or serializer mismatch"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=late_missing_owner,
                attempted_slot_generation=1,
            )
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 1)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 1)

    def test_command_backed_operation_projects_registered_control_policy_and_accepts(self) -> None:
        bundle = self._command_backed_bundle()
        occurrence = self._occurrence(bundle, suffix="command")
        terminal = self._prepare(bundle, occurrence, attempt_id="inspectattempt_command")

        result = terminal.serialized_result
        self.assertEqual(result["control_policy"]["status"], "available")
        self.assertEqual(result["control_policy"]["command_type"], "acquisition.run.create")
        self.assertEqual(result["provenance"]["workflow_command_count"], 1)
        self.assertEqual(result["provenance"]["latest_workflow_command_id"], "command_start_inspect")
        self.assertEqual(result["result_readiness"]["status"], "pending")
        owner_ref = terminal.owner_result_ref
        self.assertEqual(
            owner_ref["physical_owner_fingerprint_schema_version"],
            "inspect_operation_physical_owner_fingerprint_v3",
        )
        self.assertEqual(owner_ref["selected_plan_event"]["event_id"], dict(bundle["event"])["event_id"])
        self.assertEqual(
            owner_ref["workflow_command_causal_identity"]["workflow_run_id"],
            "workflow_start_inspect",
        )
        for private_field in (
            "physical_owner_fingerprint_schema_version",
            "workflow_command_causal_identity",
            "selected_plan_event",
        ):
            self.assertNotIn(private_field, terminal.serialized_result_json)
            self.assertNotIn(private_field, terminal.tool_result_message_record()["content"])

        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        accepted = self.repository.accept_inspect_operation_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=1,
        )
        self.assertEqual(accepted["outcome"], "accepted")

    def test_completed_operation_without_result_ref_stays_pending_fail_closed(self) -> None:
        bundle = self._command_backed_bundle()
        operation = dict(bundle["operation_run"])
        completed = self.repository.update_operation_state(
            str(operation["operation_run_id"]),
            status="completed",
        )
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["result_ref"], {})
        bundle["operation_run"] = completed

        terminal = self._prepare(
            bundle,
            self._occurrence(bundle, suffix="completed-no-result"),
            attempt_id="inspectattempt_completed_no_result",
        )

        self.assertEqual(
            terminal.serialized_result["result_readiness"],
            {
                "status": "pending",
                "result_ref_present": False,
                "source_of_truth": "operation_query_service.result_readiness_projection",
                "fallback_status": "fail_closed",
            },
        )

    def test_non_operation_event_family_cannot_advance_query_revision(self) -> None:
        bundle = self._preview_bundle(suffix="foreign-event-family")
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        occurrence = self._occurrence(bundle, suffix="foreign-event-family")
        self.repository.append_operation_event(
            event_stream_id=str(operation["operation_run_id"]),
            event_family="workflow_event",
            event_type="OperationInspectionRevisionAdvanced",
            idempotency_key="inspect-foreign-event-family",
            workspace_id="workspace_1",
            operation_run_id=str(operation["operation_run_id"]),
            action_id=str(action["action_id"]),
            actor="test-user",
            source="test.d1n.inspect",
            payload={"reason": "foreign_family_fixture"},
        )

        with self.assertRaisesRegex(ValueError, "operation event owner mismatch"):
            self._prepare(bundle, occurrence, attempt_id="inspectattempt_foreign_event_family")
        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_command_owner_drift_fails_before_terminal_writes(self) -> None:
        bundle = self._command_backed_bundle()
        occurrence = self._occurrence(bundle, suffix="command-owner-drift")
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE workflow_commands SET owner = %s WHERE command_id = %s",
                    ("foreign.command.owner", "command_start_inspect"),
                )

        with self.assertRaisesRegex(ValueError, "workflow command link mismatch"):
            self._prepare(bundle, occurrence, attempt_id="inspectattempt_command_owner_drift")
        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_command_workflow_link_drift_fails_before_terminal_writes(self) -> None:
        bundle = self._command_backed_bundle()
        occurrence = self._occurrence(bundle, suffix="command-link-drift")
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE workflow_commands SET workflow_run_id = %s WHERE command_id = %s",
                    ("workflow_foreign", "command_start_inspect"),
                )
        with self.assertRaisesRegex(ValueError, "workflow command link mismatch"):
            self._prepare(bundle, occurrence, attempt_id="inspectattempt_workflow_link_drift")

        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_command_planned_event_link_drift_fails_before_terminal_writes(self) -> None:
        bundle = self._command_backed_bundle(event_command_id="command_foreign")
        occurrence = self._occurrence(bundle, suffix="command-event-link-drift")

        with self.assertRaisesRegex(ValueError, "workflow command link mismatch"):
            self._prepare(bundle, occurrence, attempt_id="inspectattempt_event_link_drift")
        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_unreferenced_same_operation_command_is_outside_exact_query_owner(self) -> None:
        from sourcing_agent.operation_runtime import ACTION_START_ACQUISITION_RUN, DEFAULT_ACTION_REGISTRY

        bundle = self._command_backed_bundle()
        operation = dict(bundle["operation_run"])
        spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN)
        unrelated = self.store.upsert_workflow_command(
            workflow_run_id="workflow_unrelated",
            operation_id=str(operation["operation_run_id"]),
            command_id="command_unrelated",
            command_type=spec.default_workflow_command_type,
            owner=spec.owner_module,
            idempotency_key="unrelated-command",
            payload={"fixture": "unreferenced_same_operation"},
        )
        self.assertEqual(unrelated["command_id"], "command_unrelated")

        occurrence = self._occurrence(bundle, suffix="exact-command-owner")
        terminal = self._prepare(bundle, occurrence, attempt_id="inspectattempt_exact_command_owner")
        self.assertEqual(terminal.serialized_result["provenance"]["workflow_command_count"], 1)
        self.assertEqual(
            terminal.serialized_result["provenance"]["latest_workflow_command_id"],
            "command_start_inspect",
        )

    def test_missing_and_foreign_owner_accept_the_same_masked_result_with_lost_ack(self) -> None:
        bundle = self._preview_bundle()
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        occurrence = self._occurrence(bundle)
        foreign = self._occurrence(
            bundle,
            suffix="foreign",
            workspace_id="workspace_foreign",
            actor_id="requester_foreign",
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        self.repository.reserve_agent_tool_result_slot(occurrence=foreign)
        before_action = self.repository.get_action(str(action["action_id"]))
        before_operation = self.repository.get_operation(str(operation["operation_run_id"]))
        before_events = self.repository.list_operation_events(str(operation["operation_run_id"]))

        missing_terminal = self._prepare(
            bundle,
            occurrence,
            attempt_id="inspectattempt_missing",
            action_id="action_missing",
        )
        foreign_terminal = self._prepare(
            bundle,
            foreign,
            attempt_id="inspectattempt_foreign",
            action_id=str(action["action_id"]),
        )
        expected_json = '{"reason":"operation_not_found","retryable":false,"status":"failed","variant":"error"}'
        self.assertEqual(missing_terminal.serialized_result_json, expected_json)
        self.assertEqual(foreign_terminal.serialized_result_json, expected_json)
        self.assertTrue(missing_terminal.is_error)
        self.assertTrue(foreign_terminal.is_error)
        self.assertNotIn("foreign", expected_json)
        self.assertNotIn("workspace", expected_json)

        with self.assertRaisesRegex(RuntimeError, "after commit"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=missing_terminal,
                attempted_slot_generation=1,
                fault_injection_point="after_commit",
            )
        replay = self.repository.accept_inspect_operation_tool_result_uow(
            occurrence=occurrence,
            terminal=missing_terminal,
            attempted_slot_generation=1,
        )
        accepted = self.repository.accept_inspect_operation_tool_result_uow(
            occurrence=foreign,
            terminal=foreign_terminal,
            attempted_slot_generation=1,
        )

        self.assertEqual(replay["outcome"], "replayed")
        self.assertTrue(replay["replayed"])
        self.assertEqual(accepted["outcome"], "accepted")
        for result in (replay, accepted):
            self.assertTrue(result["slot"]["is_error"])
            self.assertTrue(result["attempt"]["is_error"])
            self.assertTrue(result["journal"]["is_error"])
            self.assertEqual(result["slot"]["tool_result_message"]["content"], expected_json)
        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 2,
                "agent_tool_result_attempts": 2,
                "agent_tool_result_journal": 2,
            },
        )
        self.assertEqual(self.repository.get_action(str(action["action_id"])), before_action)
        self.assertEqual(self.repository.get_operation(str(operation["operation_run_id"])), before_operation)
        self.assertEqual(self.repository.list_operation_events(str(operation["operation_run_id"])), before_events)

    def test_foreign_workspace_event_cannot_be_hidden_during_acceptance(self) -> None:
        bundle = self._preview_bundle(suffix="foreign-stream")
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        occurrence = self._occurrence(bundle, suffix="foreign-stream")
        terminal = self._prepare(bundle, occurrence, attempt_id="inspectattempt_foreign_stream")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        appended = self.repository.append_operation_event(
            event_stream_id=str(operation["operation_run_id"]),
            event_family="operation_event",
            event_type="OperationInspectionRevisionAdvanced",
            idempotency_key="inspect-foreign-workspace-stream",
            workspace_id="workspace_foreign",
            operation_run_id=str(operation["operation_run_id"]),
            action_id=str(action["action_id"]),
            actor="foreign-test-user",
            source="test.d1n.inspect.foreign",
            payload={"reason": "foreign_workspace_fixture"},
        )
        self.assertEqual(appended["workspace_id"], "workspace_foreign")

        with self.assertRaisesRegex(ValueError, "operation event owner mismatch"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )

        slot = self.repository.get_agent_tool_result_slot(
            occurrence.result_slot_id,
            workspace_id=occurrence.workspace_id,
            actor_id=occurrence.actor_id,
            runtime_namespace=occurrence.runtime_namespace,
            provider_mode=occurrence.provider_mode,
        )
        self.assertEqual(slot["status"], "pending")
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

    def test_every_event_owner_identity_and_sequence_is_fenced(self) -> None:
        mutations: tuple[tuple[str, str, Any], ...] = (
            ("operation_run", "operation_run_id", "oprun_event_foreign"),
            ("action", "action_id", "action_event_foreign"),
            ("schema", "schema_version", "operation_event_v2"),
            ("sequence", "sequence_number", 2),
        )
        for label, column, value in mutations:
            with self.subTest(label=label):
                bundle = self._preview_bundle(suffix=f"event-owner-{label}")
                occurrence = self._occurrence(bundle, suffix=f"event-owner-{label}")
                terminal = self._prepare(
                    bundle,
                    occurrence,
                    attempt_id=f"inspectattempt_event_owner_{label}",
                )
                self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
                event = dict(bundle["event"])
                with self.adapter._connect() as connection:  # noqa: SLF001
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"UPDATE operation_events SET {column} = %s WHERE event_id = %s",
                            (value, event["event_id"]),
                        )

                with self.assertRaisesRegex(ValueError, "operation event owner mismatch"):
                    self.adapter.accept_inspect_operation_tool_result_uow(
                        occurrence=occurrence,
                        terminal=terminal,
                        attempted_slot_generation=1,
                    )
                slot = self.repository.get_agent_tool_result_slot(
                    occurrence.result_slot_id,
                    workspace_id=occurrence.workspace_id,
                    actor_id=occurrence.actor_id,
                    runtime_namespace=occurrence.runtime_namespace,
                    provider_mode=occurrence.provider_mode,
                )
                self.assertEqual(slot["status"], "pending")
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

    def test_foreign_malformed_owner_is_masked_like_missing(self) -> None:
        bundle = self._command_backed_bundle()
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        foreign = self._occurrence(
            bundle,
            suffix="foreign-malformed",
            workspace_id="workspace_foreign",
            actor_id="requester_foreign",
        )
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE operation_runs SET workflow_ref_json = %s WHERE operation_run_id = %s",
                    ("{", operation["operation_run_id"]),
                )
        foreign_terminal = self._prepare(
            bundle,
            foreign,
            attempt_id="inspectattempt_foreign_malformed",
            action_id=str(action["action_id"]),
        )
        missing_terminal = self._prepare(
            bundle,
            foreign,
            attempt_id="inspectattempt_missing_malformed",
            action_id="action_missing_malformed",
        )
        self.assertEqual(foreign_terminal.serialized_result_json, missing_terminal.serialized_result_json)
        self.assertEqual(foreign_terminal.serialized_result["reason"], "operation_not_found")
        self.assertTrue(foreign_terminal.is_error)
        self.assertTrue(missing_terminal.is_error)

    def test_duplicate_command_plan_proof_is_rejected_before_result_writes(self) -> None:
        bundle = self._command_backed_bundle()
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        workflow_ref = dict(operation["workflow_ref"])
        duplicate = self.repository.append_operation_event(
            event_stream_id=str(operation["operation_run_id"]),
            event_family="operation_event",
            event_type="OperationCommandPlanned",
            idempotency_key="start-inspect-event-duplicate",
            workspace_id="workspace_1",
            operation_run_id=str(operation["operation_run_id"]),
            action_id=str(action["action_id"]),
            actor="test-user",
            source="test.d1n.inspect",
            payload={
                **workflow_ref,
                "workflow_run_id": "workflow_conflicting_duplicate",
                "module_state_mutated": False,
            },
        )
        self.assertTrue(duplicate)

        with self.assertRaisesRegex(ValueError, "workflow command link mismatch"):
            self._prepare(
                bundle,
                self._occurrence(bundle, suffix="duplicate-plan"),
                attempt_id="inspectattempt_duplicate_plan",
            )
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

    def test_unreferenced_command_plan_event_is_rejected_from_the_complete_stream(self) -> None:
        bundle = self._command_backed_bundle()
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        workflow_ref = dict(operation["workflow_ref"])
        unrelated = self.repository.append_operation_event(
            event_stream_id=str(operation["operation_run_id"]),
            event_family="operation_event",
            event_type="OperationCommandPlanned",
            idempotency_key="start-inspect-event-unreferenced",
            workspace_id="workspace_1",
            operation_run_id=str(operation["operation_run_id"]),
            action_id=str(action["action_id"]),
            actor="test-user",
            source="test.d1n.inspect",
            payload={
                **workflow_ref,
                "command_id": "command_unreferenced_plan",
                "module_state_mutated": False,
            },
        )
        self.assertTrue(unrelated)

        with self.assertRaisesRegex(ValueError, "workflow command link mismatch"):
            self._prepare(
                bundle,
                self._occurrence(bundle, suffix="unreferenced-plan"),
                attempt_id="inspectattempt_unreferenced_plan",
            )
        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_commandless_plan_event_or_plan_phase_is_rejected_before_result_writes(self) -> None:
        plan_event_bundle = self._preview_bundle(suffix="commandless_plan_event")
        action = dict(plan_event_bundle["action"])
        operation = dict(plan_event_bundle["operation_run"])
        self.repository.append_operation_event(
            event_stream_id=str(operation["operation_run_id"]),
            event_family="operation_event",
            event_type="OperationCommandPlanned",
            idempotency_key="commandless-illegal-plan-event",
            workspace_id="workspace_1",
            operation_run_id=str(operation["operation_run_id"]),
            action_id=str(action["action_id"]),
            actor="test-user",
            source="test.d1n.inspect",
            payload={
                "workflow_run_id": "workflow_illegal_commandless",
                "command_id": "command_illegal_commandless",
                "command_type": "acquisition.run.create",
                "owner": "acquisition_run_writer",
                "module_state_mutated": False,
            },
        )
        with self.assertRaisesRegex(ValueError, "workflow command link mismatch"):
            self._prepare(
                plan_event_bundle,
                self._occurrence(plan_event_bundle, suffix="commandless_plan_event"),
                attempt_id="inspectattempt_commandless_plan_event",
            )

        plan_phase_bundle = self._preview_bundle(suffix="commandless_plan_phase")
        plan_phase_operation = dict(plan_phase_bundle["operation_run"])
        self.repository.update_operation_state(
            str(plan_phase_operation["operation_run_id"]),
            progress_patch={"phase": "workflow_command_planned"},
        )
        with self.assertRaisesRegex(ValueError, "workflow command link mismatch"):
            self._prepare(
                plan_phase_bundle,
                self._occurrence(plan_phase_bundle, suffix="commandless_plan_phase"),
                attempt_id="inspectattempt_commandless_plan_phase",
            )

        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_plan_topology_drift_before_acceptance_keeps_slot_pending(self) -> None:
        bundle = self._command_backed_bundle()
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        occurrence = self._occurrence(bundle, suffix="plan_topology_drift")
        terminal = self._prepare(bundle, occurrence, attempt_id="inspectattempt_plan_topology_drift")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        self.repository.append_operation_event(
            event_stream_id=str(operation["operation_run_id"]),
            event_family="operation_event",
            event_type="OperationCommandPlanned",
            idempotency_key="start-inspect-event-topology-drift",
            workspace_id="workspace_1",
            operation_run_id=str(operation["operation_run_id"]),
            action_id=str(action["action_id"]),
            actor="test-user",
            source="test.d1n.inspect",
            payload={
                **dict(operation["workflow_ref"]),
                "command_id": "command_topology_drift",
                "module_state_mutated": False,
            },
        )

        with self.assertRaisesRegex(ValueError, "workflow command link mismatch"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )
        self._assert_pending_without_terminal_effects(occurrence)

    def test_plan_payload_drift_without_revision_advance_is_fenced(self) -> None:
        bundle = self._command_backed_bundle()
        occurrence = self._occurrence(bundle, suffix="plan-payload-drift")
        terminal = self._prepare(bundle, occurrence, attempt_id="inspectattempt_plan_payload_drift")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        event = dict(bundle["event"])
        payload = dict(event["payload"])
        payload["module_state_mutated"] = True
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE operation_events SET payload_json = %s WHERE event_id = %s",
                    (json.dumps(payload, sort_keys=True, separators=(",", ":")), event["event_id"]),
                )

        with self.assertRaisesRegex(ValueError, "physical owner or serializer mismatch"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

    def test_coordinated_workflow_lineage_drift_is_fenced(self) -> None:
        bundle = self._command_backed_bundle()
        occurrence = self._occurrence(bundle, suffix="lineage-drift")
        terminal = self._prepare(bundle, occurrence, attempt_id="inspectattempt_lineage_drift")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        operation = dict(bundle["operation_run"])
        event = dict(bundle["event"])
        workflow_ref = {**dict(operation["workflow_ref"]), "workflow_run_id": "workflow_coordinated_drift"}
        event_payload = {**dict(event["payload"]), "workflow_run_id": "workflow_coordinated_drift"}
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE operation_runs SET workflow_ref_json = %s WHERE operation_run_id = %s",
                    (
                        json.dumps(workflow_ref, sort_keys=True, separators=(",", ":")),
                        operation["operation_run_id"],
                    ),
                )
                cursor.execute(
                    "UPDATE workflow_commands SET workflow_run_id = %s WHERE command_id = %s",
                    ("workflow_coordinated_drift", workflow_ref["command_id"]),
                )
                cursor.execute(
                    "UPDATE operation_events SET payload_json = %s WHERE event_id = %s",
                    (
                        json.dumps(event_payload, sort_keys=True, separators=(",", ":")),
                        event["event_id"],
                    ),
                )

        with self.assertRaisesRegex(ValueError, "physical owner or serializer mismatch"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

    def test_command_causal_identity_drift_is_fenced(self) -> None:
        bundle = self._command_backed_bundle()
        occurrence = self._occurrence(bundle, suffix="command-causal-drift")
        terminal = self._prepare(bundle, occurrence, attempt_id="inspectattempt_command_causal_drift")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE workflow_commands SET stage_id = %s WHERE command_id = %s",
                    ("stage_drift", "command_start_inspect"),
                )

        with self.assertRaisesRegex(ValueError, "physical owner or serializer mismatch"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

    def test_workflow_ref_and_plan_identity_are_strict_typed_contracts(self) -> None:
        bundle = self._command_backed_bundle()
        operation = dict(bundle["operation_run"])
        event = dict(bundle["event"])
        exact_ref = dict(operation["workflow_ref"])
        malformed_refs: tuple[tuple[str, Any], ...] = (
            ("empty_text", ""),
            ("null", "null"),
            ("list", "[]"),
            ("invalid_json", "{"),
            ("unknown_only", {"unknown": "value"}),
            ("partial", {"command_id": exact_ref["command_id"]}),
            ("conflicting_extra", {**exact_ref, "workflow_run": exact_ref["workflow_run_id"]}),
            ("numeric", {**exact_ref, "command_id": 123}),
            ("padded", {**exact_ref, "command_id": f" {exact_ref['command_id']} "}),
        )
        for label, malformed in malformed_refs:
            with self.subTest(label=label):
                encoded = malformed if type(malformed) is str else json.dumps(malformed)
                with self.adapter._connect() as connection:  # noqa: SLF001
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "UPDATE operation_runs SET workflow_ref_json = %s WHERE operation_run_id = %s",
                            (encoded, operation["operation_run_id"]),
                        )
                with self.assertRaisesRegex(ValueError, "operation workflow ref"):
                    self._prepare(
                        bundle,
                        self._occurrence(bundle, suffix=f"strict-ref-{label}"),
                        attempt_id=f"inspectattempt_strict_ref_{label}",
                    )

        malformed_plan_payloads = (
            ("padded", {**dict(event["payload"]), "command_id": f" {exact_ref['command_id']} "}),
            ("numeric", {**dict(event["payload"]), "command_id": 123}),
            ("missing", {key: value for key, value in dict(event["payload"]).items() if key != "owner"}),
        )
        for label, malformed_payload in malformed_plan_payloads:
            with self.subTest(plan_payload=label):
                with self.adapter._connect() as connection:  # noqa: SLF001
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "UPDATE operation_runs SET workflow_ref_json = %s WHERE operation_run_id = %s",
                            (json.dumps(exact_ref), operation["operation_run_id"]),
                        )
                        cursor.execute(
                            "UPDATE operation_events SET payload_json = %s WHERE event_id = %s",
                            (json.dumps(malformed_payload), event["event_id"]),
                        )
                with self.assertRaisesRegex(ValueError, "workflow command link mismatch"):
                    self._prepare(
                        bundle,
                        self._occurrence(bundle, suffix=f"strict-plan-{label}"),
                        attempt_id=f"inspectattempt_strict_plan_{label}",
                    )
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

    def test_masked_error_owner_reappears_before_acceptance_and_forged_unions_fail_closed(self) -> None:
        bundle = self._preview_bundle(suffix="masked-recheck")
        action = dict(bundle["action"])
        operation = dict(bundle["operation_run"])
        foreign = self._occurrence(
            bundle,
            suffix="masked-recheck",
            workspace_id="workspace_foreign",
            actor_id="requester_foreign",
        )
        error_terminal = self._prepare(
            bundle,
            foreign,
            attempt_id="inspectattempt_masked_recheck",
            action_id=str(action["action_id"]),
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=foreign)
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE agent_actions SET workspace_id = %s WHERE action_id = %s",
                    ("workspace_foreign", action["action_id"]),
                )
                cursor.execute(
                    "UPDATE operation_runs SET workspace_id = %s WHERE operation_run_id = %s",
                    ("workspace_foreign", operation["operation_run_id"]),
                )
                cursor.execute(
                    "UPDATE operation_events SET workspace_id = %s WHERE event_stream_id = %s",
                    ("workspace_foreign", operation["operation_run_id"]),
                )

        with self.assertRaisesRegex(ValueError, "physical owner or serializer mismatch"):
            self.adapter.accept_inspect_operation_tool_result_uow(
                occurrence=foreign,
                terminal=error_terminal,
                attempted_slot_generation=1,
            )
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

        # Restore the foreign owner state so the forged-union matrix exercises
        # the pre-acceptance closed union rather than the owner recheck.
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE agent_actions SET workspace_id = %s WHERE action_id = %s",
                    ("workspace_1", action["action_id"]),
                )
                cursor.execute(
                    "UPDATE operation_runs SET workspace_id = %s WHERE operation_run_id = %s",
                    ("workspace_1", operation["operation_run_id"]),
                )
                cursor.execute(
                    "UPDATE operation_events SET workspace_id = %s WHERE event_stream_id = %s",
                    ("workspace_1", operation["operation_run_id"]),
                )
        success = self._prepare(
            bundle,
            self._occurrence(bundle, suffix="masked-success-shape"),
            attempt_id="inspectattempt_success_shape",
        )
        forged_json = '{"reason":"forged","retryable":false,"status":"failed","variant":"error"}'
        forged_terminals = (
            replace(success, is_error=True),
            replace(error_terminal, is_error=False),
            replace(
                error_terminal,
                serialized_result_json=forged_json,
                serialized_result_digest=hashlib.sha256(forged_json.encode("utf-8")).hexdigest(),
            ),
            replace(error_terminal, owner_target_kind="operation_state_event_v1"),
            replace(error_terminal, owner_target_generation=2),
        )
        for forged in forged_terminals:
            with self.subTest(forged=forged.to_record()):
                with self.assertRaisesRegex(ValueError, "matching Operation owner result kind"):
                    self.adapter.accept_inspect_operation_tool_result_uow(
                        occurrence=foreign,
                        terminal=forged,
                        attempted_slot_generation=1,
                    )
        self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
        self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

    def test_masked_error_faults_rollback_before_commit(self) -> None:
        bundle = self._preview_bundle(suffix="masked-faults")
        occurrence = self._occurrence(bundle, suffix="masked-faults")
        terminal = self._prepare(
            bundle,
            occurrence,
            attempt_id="inspectattempt_masked_faults",
            action_id="action_missing_masked_faults",
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        for fault in ("after_attempt_write", "after_slot_write", "after_journal_write"):
            with self.subTest(fault=fault):
                with self.assertRaisesRegex(RuntimeError, fault.replace("_", " ")):
                    self.adapter.accept_inspect_operation_tool_result_uow(
                        occurrence=occurrence,
                        terminal=terminal,
                        attempted_slot_generation=1,
                        fault_injection_point=fault,
                    )
                slot = self.repository.get_agent_tool_result_slot(
                    occurrence.result_slot_id,
                    workspace_id=occurrence.workspace_id,
                    actor_id=occurrence.actor_id,
                    runtime_namespace=occurrence.runtime_namespace,
                    provider_mode=occurrence.provider_mode,
                )
                self.assertEqual(slot["status"], "pending")
                self.assertEqual(self._result_counts()["agent_tool_result_attempts"], 0)
                self.assertEqual(self._result_counts()["agent_tool_result_journal"], 0)

    def test_forged_query_contract_is_rejected_before_owner_read(self) -> None:
        from sourcing_agent.agent_canary_registry import PLAN_ACQUISITION_TOOL_SPEC

        bundle = self._preview_bundle()
        operation = dict(bundle["operation_run"])
        forged = AgentToolOccurrence.from_tool_spec(
            result_slot_id="inspectslot_forged",
            slot_generation=1,
            workspace_id="workspace_1",
            actor_id="requester_1",
            runtime_namespace="isolated_local_canary",
            provider_mode="simulate",
            turn_id="turn_forged",
            step_id="step_forged",
            tool_spec=PLAN_ACQUISITION_TOOL_SPEC,
            canonical_args={"operation_run_id": str(operation["operation_run_id"])},
            occurrence_ordinal=1,
        )

        with self.assertRaisesRegex(ValueError, "historical contract unavailable"):
            self._prepare(bundle, forged)
        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )
