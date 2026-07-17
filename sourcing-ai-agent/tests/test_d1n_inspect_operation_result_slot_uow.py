from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest import mock

from sourcing_agent.agent_canary_registry import INSPECT_OPERATION_TOOL_SPEC
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence
from sourcing_agent.control_plane_repository import ControlPlaneAuthoritativeReadError
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin
from tests.test_d1n_acquisition_plan_preview_uow import _uow_kwargs


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
        action = self.repository.upsert_action(
            action_id=action_id,
            workspace_id="workspace_1",
            conversation_id="conversation_1",
            action_type=spec.action_type,
            owner_module=spec.owner_module,
            operation_type=spec.operation_type,
            target_ref={},
            input_payload={},
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
            tool_spec=INSPECT_OPERATION_TOOL_SPEC,
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

    def test_prepare_wraps_infrastructure_failure_but_preserves_domain_errors(self) -> None:
        bundle = self._preview_bundle(suffix="read-failure")
        occurrence = self._occurrence(bundle, suffix="read-failure")

        with mock.patch.object(
            self.adapter,
            "prepare_inspect_operation_tool_result",
            side_effect=ConnectionError("synthetic owner read failure"),
        ):
            with self.assertRaisesRegex(ControlPlaneAuthoritativeReadError, "ConnectionError"):
                self._prepare(bundle, occurrence, attempt_id="inspectattempt_read_failure")

        with self.assertRaisesRegex(ValueError, "exact owner not found"):
            self._prepare(
                bundle,
                occurrence,
                attempt_id="inspectattempt_domain_failure",
                action_id="action_missing_domain_failure",
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
        with self.assertRaisesRegex(ValueError, "exact owner not found"):
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
        with self.assertRaisesRegex(ValueError, "exact owner not found"):
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

    def test_missing_and_foreign_owner_fail_the_same_before_terminal_writes(self) -> None:
        bundle = self._preview_bundle()
        occurrence = self._occurrence(bundle)
        foreign = self._occurrence(
            bundle,
            suffix="foreign",
            workspace_id="workspace_foreign",
            actor_id="requester_foreign",
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        self.repository.reserve_agent_tool_result_slot(occurrence=foreign)

        errors: list[str] = []
        for candidate, action_id in (
            (occurrence, "action_missing"),
            (foreign, str(dict(bundle["action"])["action_id"])),
        ):
            with self.assertRaises(ValueError) as raised:
                self._prepare(bundle, candidate, action_id=action_id)
            errors.append(str(raised.exception))

        self.assertEqual(errors, ["agent tool inspect result exact owner not found"] * 2)
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

        with self.assertRaisesRegex(ValueError, "occurrence contract mismatch"):
            self._prepare(bundle, forged)
        self.assertEqual(
            self._result_counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )
