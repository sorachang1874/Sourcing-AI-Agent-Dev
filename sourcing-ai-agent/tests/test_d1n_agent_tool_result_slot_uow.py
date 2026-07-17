from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest import mock

from sourcing_agent import agent_tool_result_postgres as result_postgres
from sourcing_agent.acquisition_plan_preview import (
    AcquisitionPlanPreview,
    acquisition_plan_preview_success_result,
)
from sourcing_agent.agent_canary_registry import (
    PLAN_ACQUISITION_TOOL_SPEC,
    START_ACQUISITION_RUN_TOOL_SPEC,
    START_ACQUISITION_RUN_TOOL_SPEC_V2,
)
from sourcing_agent.agent_tool_result_postgres import ACQUISITION_PLAN_PREVIEW_OWNER_TARGET_KIND
from sourcing_agent.agent_tool_result_slot import (
    AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION,
    AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION_V2,
    AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION,
    AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION_V2,
    AGENT_TOOL_RESULT_SLOT_SCHEMA_VERSION,
    AgentToolOccurrence,
    AgentToolResultSlotError,
    AgentToolTerminalResult,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin
from tests.test_d1n_acquisition_plan_preview_uow import _uow_kwargs

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None


class _EqualityAliasString(str):
    """Carry forged bytes while comparing equal to one expected plain string."""

    _equal_to: str

    def __new__(cls, value: str, *, equal_to: str) -> _EqualityAliasString:
        instance = super().__new__(cls, value)
        instance._equal_to = equal_to
        return instance

    def __eq__(self, other: object) -> bool:
        return bool(str.__eq__(self, other)) or other == self._equal_to

    __hash__ = str.__hash__


class D1nAgentToolResultSlotUowPGTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1n_tool_result"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(Path(self.tempdir.name) / "agent-tool-result.db")
        self.repository = self.store.repos.workflow_runtime
        self.adapter = self.store._control_plane_postgres  # noqa: SLF001

    def _occurrence(
        self,
        *,
        suffix: str = "1",
        slot_generation: int = 1,
        tool_spec=PLAN_ACQUISITION_TOOL_SPEC,
        actor_id: str = "requester_1",
        canonical_args: Mapping[str, Any] | None = None,
    ) -> AgentToolOccurrence:
        kwargs = _uow_kwargs(suffix=suffix)
        occurrence_args = (
            {
                "input_payload": kwargs["input_payload"],
                "target_ref": kwargs["target_ref"],
            }
            if canonical_args is None
            else dict(canonical_args)
        )
        return AgentToolOccurrence.from_tool_spec(
            result_slot_id=f"toolslot_{suffix}",
            slot_generation=slot_generation,
            workspace_id=str(kwargs["workspace_id"]),
            actor_id=actor_id,
            runtime_namespace="isolated_local_canary",
            provider_mode="simulate",
            turn_id=f"turn_{suffix}",
            step_id="step_1",
            tool_spec=tool_spec,
            canonical_args=occurrence_args,
            occurrence_ordinal=1,
        )

    def _preview_bundle(self, *, suffix: str = "1") -> dict[str, object]:
        return self.repository.create_acquisition_plan_preview_uow(**_uow_kwargs(suffix=suffix))

    def _terminal(
        self,
        bundle: dict[str, object],
        *,
        attempt_id: str = "resultattempt_1",
        provider_call_id: str = "provider-call-1",
        tool_call_id: str = "tool-call-1",
        serialized_result: Mapping[str, Any] | None = None,
    ) -> AgentToolTerminalResult:
        action = dict(cast(Mapping[str, Any], bundle["action"]))
        operation = dict(cast(Mapping[str, Any], bundle["operation_run"]))
        preview = dict(cast(Mapping[str, Any], bundle["preview"]))
        event = dict(cast(Mapping[str, Any], bundle["event"]))
        owner_output = acquisition_plan_preview_success_result(AcquisitionPlanPreview(preview["preview"]))
        return AgentToolTerminalResult.from_serialized_result(
            result_attempt_id=attempt_id,
            provider_call_id=provider_call_id,
            tool_call_id=tool_call_id,
            action_id=str(action["action_id"]),
            operation_run_id=str(operation["operation_run_id"]),
            owner_target_kind=ACQUISITION_PLAN_PREVIEW_OWNER_TARGET_KIND,
            owner_target_id=str(preview["preview_id"]),
            owner_target_revision=int(preview["preview_revision"]),
            terminal_winner_id=str(event["event_id"]),
            owner_result_ref=dict(action["result_ref"]),
            owner_result_digest=str(preview["preview_digest"]),
            serialized_result=owner_output if serialized_result is None else serialized_result,
            is_error=False,
        )

    def _counts(self) -> dict[str, int]:
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                counts: dict[str, int] = {}
                for table_name in (
                    "agent_tool_result_slots",
                    "agent_tool_result_attempts",
                    "agent_tool_result_journal",
                ):
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    counts[table_name] = int(cursor.fetchone()[0])
                return counts

    def _assert_pending_without_terminal_effects(self, occurrence: AgentToolOccurrence) -> None:
        slot = self.repository.get_agent_tool_result_slot(
            occurrence.result_slot_id,
            workspace_id=occurrence.workspace_id,
            actor_id=occurrence.actor_id,
            runtime_namespace=occurrence.runtime_namespace,
            provider_mode="simulate",
        )
        self.assertEqual(slot["status"], "pending")
        self.assertEqual(self.repository.list_agent_tool_result_attempts(occurrence.result_slot_id), [])
        self.assertEqual(self._counts()["agent_tool_result_journal"], 0)

    def _token_terminal(
        self,
        bundle: dict[str, object],
        *,
        attempt_id: str = "resultattempt_token_1",
        provider_call_id: str = "provider-call-token-1",
        tool_call_id: str = "tool-call-token-1",
        token: str = "membership_revision:01HZX.same-token",
    ) -> AgentToolTerminalResult:
        return replace(
            self._terminal(
                bundle,
                attempt_id=attempt_id,
                provider_call_id=provider_call_id,
                tool_call_id=tool_call_id,
            ),
            owner_target_revision=0,
            owner_target_generation=0,
            owner_target_revision_token=token,
        )

    def _accept_synthetic_token_owner(
        self,
        *,
        occurrence: AgentToolOccurrence,
        terminal: AgentToolTerminalResult,
        attempted_slot_generation: int = 1,
        physical_owner_revision_token: str = "membership_revision:01HZX.same-token",
    ) -> dict[str, Any]:
        def load_base_owner(
            cursor: Any,
            *,
            occurrence: AgentToolOccurrence,
            terminal: AgentToolTerminalResult,
        ) -> dict[str, str]:
            del cursor, occurrence, terminal
            return {
                "owner": "synthetic_equality_only_storage_owner_v1",
                "owner_target_revision_token": physical_owner_revision_token,
            }

        def assert_locked_owner(
            cursor: Any,
            *,
            occurrence: AgentToolOccurrence,
            terminal: AgentToolTerminalResult,
            base_owner: dict[str, str],
        ) -> None:
            del cursor, occurrence
            self.assertEqual(base_owner["owner"], "synthetic_equality_only_storage_owner_v1")
            if terminal.owner_target_revision_token != base_owner["owner_target_revision_token"]:
                raise ValueError("synthetic storage owner revision token mismatch")
            self.assertEqual(terminal.owner_target_revision, 0)
            self.assertEqual(terminal.owner_target_generation, 0)

        outcome = result_postgres._accept_exact_agent_tool_result_uow(  # noqa: SLF001
            self.adapter,
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=attempted_slot_generation,
            required_tables=(
                "agent_tool_result_slots",
                "agent_tool_result_attempts",
                "agent_tool_result_journal",
            ),
            lock_groups=((f"agent_tool_result_slots:id:{occurrence.result_slot_id}",),),
            load_base_owner=load_base_owner,
            assert_locked_owner=assert_locked_owner,
        )
        assert outcome is not None
        return outcome

    def test_reserve_is_exactly_replayable_and_rejects_split_logical_identity(self) -> None:
        occurrence = self._occurrence()

        with self.assertRaisesRegex(ValueError, "finite and positive"):
            self.adapter.reserve_agent_tool_result_slot(occurrence=occurrence, lock_timeout_seconds=True)

        first = self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        replay = self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)

        self.assertEqual(first["outcome"], "reserved")
        self.assertFalse(first["replayed"])
        self.assertEqual(replay["outcome"], "replayed")
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["slot"]["logical_occurrence_digest"], occurrence.logical_occurrence_digest)
        self.assertEqual(first["slot"]["canonical_args"], occurrence.canonical_args)
        self.assertEqual(first["slot"]["result_link_policy"], occurrence.result_link_policy)
        collision = AgentToolOccurrence.from_tool_spec(
            result_slot_id="toolslot_forged",
            slot_generation=1,
            workspace_id=occurrence.workspace_id,
            actor_id=occurrence.actor_id,
            runtime_namespace=occurrence.runtime_namespace,
            provider_mode=occurrence.provider_mode,
            turn_id=occurrence.turn_id,
            step_id=occurrence.step_id,
            tool_spec=PLAN_ACQUISITION_TOOL_SPEC,
            canonical_args=occurrence.canonical_args,
            occurrence_ordinal=occurrence.occurrence_ordinal,
        )
        with self.assertRaisesRegex(ValueError, "immutable identity collision"):
            self.adapter.reserve_agent_tool_result_slot(occurrence=collision)
        self.assertEqual(self._counts()["agent_tool_result_slots"], 1)

    def test_result_link_policy_is_exact_slot_identity_without_changing_logical_identity_record(self) -> None:
        occurrence = self._occurrence(suffix="policy", tool_spec=START_ACQUISITION_RUN_TOOL_SPEC)
        reserved = self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        replay = self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        self.assertEqual(replay["outcome"], "replayed")
        self.assertNotIn("result_link_policy", occurrence.logical_identity_record())

        collision_row = dict(reserved["slot"])
        collision_row["result_link_policy"] = "activity_attempt_terminal_v1"
        with self.assertRaisesRegex(ValueError, "immutable identity collision: result_link_policy"):
            result_postgres._assert_exact_slot(collision_row, occurrence)  # noqa: SLF001

        self.assertEqual(
            self._counts(),
            {
                "agent_tool_result_slots": 1,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_incoherent_occurrence_policy_is_rejected_before_reserve_with_zero_writes(self) -> None:
        occurrence = self._occurrence(suffix="policyincoherent")

        with self.assertRaisesRegex(AgentToolResultSlotError, "link_policy_effect_mismatch"):
            replace(occurrence, result_link_policy="workflow_command_acceptance_v1")

        self.assertEqual(
            self._counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_compatible_policy_swap_is_rejected_before_reserve_with_zero_writes(self) -> None:
        cases = (
            (
                self._occurrence(suffix="policy_v2", tool_spec=START_ACQUISITION_RUN_TOOL_SPEC_V2),
                "workflow_command_acceptance_v1",
            ),
            (
                self._occurrence(suffix="policy_v3", tool_spec=START_ACQUISITION_RUN_TOOL_SPEC),
                "activity_attempt_terminal_v1",
            ),
        )

        for occurrence, forged_policy in cases:
            with self.subTest(tool_spec_version=occurrence.tool_spec_version):
                forged = replace(occurrence, result_link_policy=forged_policy)
                self.assertEqual(forged.logical_occurrence_digest, occurrence.logical_occurrence_digest)
                with self.assertRaisesRegex(
                    AgentToolResultSlotError,
                    "historical_spec_pin_mismatch:result_link_policy",
                ):
                    self.adapter.reserve_agent_tool_result_slot(occurrence=forged)

        self.assertEqual(
            self._counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_shared_accept_rebinds_historical_spec_before_owner_reads_or_terminal_writes(self) -> None:
        occurrence = self._occurrence(suffix="acceptpolicy", tool_spec=START_ACQUISITION_RUN_TOOL_SPEC)
        forged = replace(occurrence, result_link_policy="activity_attempt_terminal_v1")
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                result_postgres._insert_dict(  # noqa: SLF001
                    cursor,
                    table_name="agent_tool_result_slots",
                    row=result_postgres._slot_insert_row(forged),  # noqa: SLF001
                )
            connection.commit()

        bundle = self._preview_bundle(suffix="acceptpolicy")
        terminal = replace(
            self._terminal(bundle, attempt_id="resultattempt_acceptpolicy"),
            workflow_command_id="command_acceptpolicy",
            activity_run_id="activity_acceptpolicy",
            activity_attempt_id="activityattempt_acceptpolicy",
            command_attempt=1,
            command_generation=1,
            control_epoch=1,
        )

        with self.assertRaisesRegex(
            AgentToolResultSlotError,
            "historical_spec_pin_mismatch:result_link_policy",
        ):
            self._accept_synthetic_token_owner(occurrence=forged, terminal=terminal)

        self.assertEqual(
            self._counts(),
            {
                "agent_tool_result_slots": 1,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT status FROM agent_tool_result_slots WHERE result_slot_id = %s",
                    (forged.result_slot_id,),
                )
                self.assertEqual(cursor.fetchone()[0], "pending")

    def test_equality_alias_occurrence_is_rejected_before_reserve_schema_or_owner_access(self) -> None:
        cases = (
            (
                "result_link_policy",
                "workflow_command_acceptance_v1",
                "link_policy_invalid",
            ),
            ("canonical_args_json", "{}", "canonical_args_invalid"),
        )

        for index, (field_name, forged_value, expected_error) in enumerate(cases, start=1):
            with self.subTest(field_name=field_name):
                occurrence = self._occurrence(suffix=f"reserve_alias_{index}")
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
                    self.adapter.reserve_agent_tool_result_slot(occurrence=occurrence)
                ensure_schema.assert_not_called()
                connect.assert_not_called()

        self.assertEqual(
            self._counts(),
            {
                "agent_tool_result_slots": 0,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )

    def test_equality_alias_occurrence_is_rejected_before_plan_accept_schema_or_owner_access(self) -> None:
        occurrence = self._occurrence(suffix="accept_occurrence_alias")
        bundle = self._preview_bundle(suffix="accept_occurrence_alias")
        terminal = self._terminal(bundle, attempt_id="resultattempt_accept_occurrence_alias")
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
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )
        ensure_schema.assert_not_called()
        connect.assert_not_called()
        self._assert_pending_without_terminal_effects(occurrence)

    def test_equality_alias_terminal_json_is_rejected_before_plan_accept_schema_or_owner_access(self) -> None:
        cases = (
            ("owner_result_ref_json", "{}", "owner_result_ref_invalid"),
            ("serialized_result_json", "{}", "serialized_result_invalid"),
        )

        for index, (field_name, forged_value, expected_error) in enumerate(cases, start=1):
            with self.subTest(field_name=field_name):
                suffix = f"accept_terminal_alias_{index}"
                occurrence = self._occurrence(suffix=suffix)
                bundle = self._preview_bundle(suffix=suffix)
                terminal = self._terminal(bundle, attempt_id=f"resultattempt_{suffix}")
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
                    self.adapter.accept_acquisition_plan_tool_result_uow(
                        occurrence=occurrence,
                        terminal=terminal,
                        attempted_slot_generation=1,
                    )
                ensure_schema.assert_not_called()
                connect.assert_not_called()
                self._assert_pending_without_terminal_effects(occurrence)

    def test_accept_reloads_exact_owner_serializes_and_recovers_lost_ack(self) -> None:
        occurrence = self._occurrence()
        bundle = self._preview_bundle()
        terminal = self._terminal(bundle)
        pending = self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        self.assertEqual(pending["slot"]["status"], "pending")
        self.assertEqual(pending["slot"]["schema_version"], AGENT_TOOL_RESULT_SLOT_SCHEMA_VERSION)

        with self.assertRaisesRegex(ValueError, "generation must be positive"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=True,
            )

        with self.assertRaisesRegex(RuntimeError, "after commit"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
                fault_injection_point="after_commit",
            )
        replay = self.repository.accept_acquisition_plan_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=1,
        )

        self.assertEqual(replay["outcome"], "replayed")
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["slot"]["status"], "accepted")
        self.assertEqual(replay["slot"]["tool_result_message"]["content"], terminal.serialized_result_json)
        self.assertEqual(replay["attempt"]["disposition"], "accepted")
        self.assertEqual(replay["journal"]["tool_spec_digest"], occurrence.tool_spec_digest)
        self.assertEqual(replay["slot"]["schema_version"], AGENT_TOOL_RESULT_SLOT_SCHEMA_VERSION)
        self.assertEqual(replay["slot"]["result_link_policy"], occurrence.result_link_policy)
        self.assertEqual(replay["attempt"]["result_link_policy"], occurrence.result_link_policy)
        self.assertEqual(replay["journal"]["result_link_policy"], occurrence.result_link_policy)
        self.assertEqual(replay["attempt"]["schema_version"], AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION)
        self.assertEqual(replay["journal"]["schema_version"], AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION)
        self.assertEqual(
            self._counts(),
            {
                "agent_tool_result_slots": 1,
                "agent_tool_result_attempts": 1,
                "agent_tool_result_journal": 1,
            },
        )

    def test_same_workspace_cross_action_result_fails_before_terminal_effects(self) -> None:
        occurrence = self._occurrence(suffix="cross_action_source")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        foreign_kwargs = _uow_kwargs(
            suffix="cross_action_foreign",
            thematic_constraints=["Post-training"],
        )
        foreign_bundle = self.repository.create_acquisition_plan_preview_uow(**foreign_kwargs)
        foreign_terminal = self._terminal(
            foreign_bundle,
            attempt_id="resultattempt_cross_action_foreign",
            provider_call_id="provider-call-cross-action-foreign",
            tool_call_id="tool-call-cross-action-foreign",
        )

        with self.assertRaisesRegex(ValueError, "action canonical args mismatch"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=foreign_terminal,
                attempted_slot_generation=1,
            )

        self._assert_pending_without_terminal_effects(occurrence)

    def test_same_owner_set_order_is_canonicalized_before_exact_action_binding(self) -> None:
        suffix = "canonical_owner_order"
        kwargs = _uow_kwargs(
            suffix=suffix,
            thematic_constraints=["Pre-training", "AI Safety"],
        )
        cohort = dict(cast(Mapping[str, Any], kwargs["input_payload"])["cohort_selection"])
        cohort["role_bucket_ids"] = ["engineering", "research"]
        cohort["employment_statuses"] = ["former", "current"]
        raw_input = dict(cast(Mapping[str, Any], kwargs["input_payload"]))
        raw_input["cohort_selection"] = cohort
        kwargs["input_payload"] = raw_input
        raw_target = dict(cast(Mapping[str, Any], kwargs["target_ref"]))
        raw_company = dict(cast(Mapping[str, Any], raw_target["company_target"]))
        raw_company["provider_company_labels"] = ["thinkingmachinesai", "Thinking Machines Lab"]
        raw_target["company_target"] = raw_company
        kwargs["target_ref"] = raw_target
        bundle = self.repository.create_acquisition_plan_preview_uow(**kwargs)
        occurrence = self._occurrence(
            suffix=suffix,
            canonical_args={"input_payload": raw_input, "target_ref": raw_target},
        )
        terminal = self._terminal(bundle, attempt_id="resultattempt_canonical_owner_order")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)

        accepted = self.adapter.accept_acquisition_plan_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=1,
        )

        self.assertEqual(accepted["outcome"], "accepted")
        self.assertEqual(accepted["slot"]["status"], "accepted")

    def test_malformed_action_input_fails_closed_before_terminal_effects(self) -> None:
        suffix = "malformed_action_input"
        occurrence = self._occurrence(suffix=suffix)
        bundle = self._preview_bundle(suffix=suffix)
        terminal = self._terminal(bundle, attempt_id="resultattempt_malformed_action_input")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        action = dict(cast(Mapping[str, Any], bundle["action"]))
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE agent_actions SET input_json = %s WHERE action_id = %s",
                    ("[]", str(action["action_id"])),
                )

        with self.assertRaisesRegex(ValueError, "persisted_json_expected_dict"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )

        self._assert_pending_without_terminal_effects(occurrence)

    def test_action_target_workspace_drift_fails_closed_before_terminal_effects(self) -> None:
        suffix = "action_target_workspace_drift"
        occurrence = self._occurrence(suffix=suffix)
        bundle = self._preview_bundle(suffix=suffix)
        terminal = self._terminal(bundle, attempt_id="resultattempt_action_target_workspace_drift")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        action = dict(cast(Mapping[str, Any], bundle["action"]))
        drifted_target = dict(cast(Mapping[str, Any], action["target_ref"]))
        drifted_target["workspace_id"] = "workspace_foreign"
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE agent_actions SET target_ref_json = %s WHERE action_id = %s",
                    (json.dumps(drifted_target, sort_keys=True), str(action["action_id"])),
                )

        with self.assertRaisesRegex(ValueError, "action target workspace mismatch"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )

        self._assert_pending_without_terminal_effects(occurrence)

    def test_cross_requester_result_fails_before_terminal_effects(self) -> None:
        foreign_kwargs = _uow_kwargs(suffix="cross_requester")
        foreign_kwargs["requester_id"] = "requester_2"
        foreign_target = dict(cast(Mapping[str, Any], foreign_kwargs["target_ref"]))
        foreign_target["requester_id"] = "requester_2"
        foreign_kwargs["target_ref"] = foreign_target
        foreign_bundle = self.repository.create_acquisition_plan_preview_uow(**foreign_kwargs)
        occurrence = self._occurrence(
            suffix="cross_requester",
            actor_id="requester_1",
            canonical_args={
                "input_payload": foreign_kwargs["input_payload"],
                "target_ref": foreign_target,
            },
        )
        terminal = self._terminal(
            foreign_bundle,
            attempt_id="resultattempt_cross_requester",
            provider_call_id="provider-call-cross-requester",
            tool_call_id="tool-call-cross-requester",
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)

        with self.assertRaisesRegex(ValueError, "action requester mismatch"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=1,
            )

        self._assert_pending_without_terminal_effects(occurrence)

    def test_mismatched_occurrence_argument_fails_before_stale_generation_quarantine(self) -> None:
        bundle = self._preview_bundle(suffix="argument_mismatch")
        kwargs = _uow_kwargs(suffix="argument_mismatch")
        mismatched_input = dict(cast(Mapping[str, Any], kwargs["input_payload"]))
        mismatched_input["thematic_constraints"] = ["Post-training"]
        occurrence = self._occurrence(
            suffix="argument_mismatch",
            canonical_args={
                "input_payload": mismatched_input,
                "target_ref": kwargs["target_ref"],
            },
        )
        terminal = self._terminal(bundle, attempt_id="resultattempt_argument_mismatch")
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)

        with self.assertRaisesRegex(ValueError, "action canonical args mismatch"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=terminal,
                attempted_slot_generation=2,
            )

        self._assert_pending_without_terminal_effects(occurrence)

    def test_stale_slot_generation_is_quarantined_without_consuming_pending_slot(self) -> None:
        occurrence = self._occurrence(slot_generation=2)
        bundle = self._preview_bundle()
        terminal = self._terminal(bundle)
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)

        stale = self.repository.accept_acquisition_plan_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=1,
        )

        self.assertEqual(stale["outcome"], "quarantined")
        self.assertEqual(stale["attempt"]["quarantine_reason"], "slot_generation_mismatch")
        self.assertEqual(stale["attempt"]["result_link_policy"], occurrence.result_link_policy)
        self.assertEqual(stale["slot"]["status"], "pending")
        terminal_2 = self._terminal(
            bundle,
            attempt_id="resultattempt_2",
            provider_call_id="provider-call-2",
            tool_call_id="tool-call-2",
        )
        accepted = self.repository.accept_acquisition_plan_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal_2,
            attempted_slot_generation=2,
        )
        self.assertEqual(accepted["outcome"], "accepted")
        attempts = self.repository.list_agent_tool_result_attempts(occurrence.result_slot_id)
        self.assertEqual([attempt["disposition"] for attempt in attempts], ["quarantined", "accepted"])

    def test_late_attempt_is_append_only_quarantine_and_cannot_replace_winner(self) -> None:
        occurrence = self._occurrence()
        bundle = self._preview_bundle()
        winner = self._terminal(bundle)
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        accepted = self.repository.accept_acquisition_plan_tool_result_uow(
            occurrence=occurrence,
            terminal=winner,
            attempted_slot_generation=1,
        )
        late = self._terminal(
            bundle,
            attempt_id="resultattempt_late",
            provider_call_id="provider-call-late",
            tool_call_id="tool-call-late",
        )

        quarantined = self.repository.accept_acquisition_plan_tool_result_uow(
            occurrence=occurrence,
            terminal=late,
            attempted_slot_generation=1,
        )

        self.assertEqual(quarantined["outcome"], "quarantined")
        self.assertEqual(quarantined["attempt"]["quarantine_reason"], "terminal_winner_already_accepted")
        self.assertEqual(quarantined["attempt"]["result_link_policy"], occurrence.result_link_policy)
        self.assertEqual(quarantined["slot"]["result_link_policy"], occurrence.result_link_policy)
        self.assertEqual(quarantined["journal"]["result_link_policy"], occurrence.result_link_policy)
        self.assertEqual(quarantined["slot"]["result_attempt_id"], winner.result_attempt_id)
        self.assertEqual(quarantined["journal"]["journal_id"], accepted["journal"]["journal_id"])
        self.assertEqual(self._counts()["agent_tool_result_attempts"], 2)

    def test_opaque_revision_token_exact_copies_through_accept_quarantine_and_replay(self) -> None:
        occurrence = self._occurrence(suffix="token")
        bundle = self._preview_bundle(suffix="token")
        winner = self._token_terminal(bundle)
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)

        accepted = self._accept_synthetic_token_owner(occurrence=occurrence, terminal=winner)
        replayed = self._accept_synthetic_token_owner(occurrence=occurrence, terminal=winner)

        self.assertEqual(accepted["outcome"], "accepted")
        self.assertEqual(replayed["outcome"], "replayed")
        for aggregate_row in (accepted["slot"], accepted["attempt"], accepted["journal"]):
            self.assertEqual(
                aggregate_row["owner_target_revision_token"],
                winner.owner_target_revision_token,
            )
            self.assertEqual(aggregate_row["owner_target_revision"], 0)
            self.assertEqual(aggregate_row["owner_target_generation"], 0)
            self.assertEqual(aggregate_row["result_link_policy"], occurrence.result_link_policy)
        self.assertEqual(accepted["slot"]["schema_version"], AGENT_TOOL_RESULT_SLOT_SCHEMA_VERSION)
        self.assertEqual(accepted["attempt"]["schema_version"], AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION_V2)
        self.assertEqual(accepted["journal"]["schema_version"], AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION_V2)

        late = self._token_terminal(
            bundle,
            attempt_id="resultattempt_token_late",
            provider_call_id="provider-call-token-late",
            tool_call_id="tool-call-token-late",
            token=winner.owner_target_revision_token,
        )
        quarantined = self._accept_synthetic_token_owner(occurrence=occurrence, terminal=late)
        quarantine_replay = self._accept_synthetic_token_owner(occurrence=occurrence, terminal=late)

        self.assertEqual(quarantined["outcome"], "quarantined")
        self.assertFalse(quarantined["replayed"])
        self.assertEqual(quarantine_replay["outcome"], "quarantined")
        self.assertTrue(quarantine_replay["replayed"])
        self.assertEqual(
            quarantined["attempt"]["owner_target_revision_token"],
            late.owner_target_revision_token,
        )
        self.assertEqual(
            quarantined["slot"]["owner_target_revision_token"],
            winner.owner_target_revision_token,
        )
        self.assertEqual(
            quarantined["journal"]["owner_target_revision_token"],
            winner.owner_target_revision_token,
        )
        self.assertEqual(quarantined["attempt"]["schema_version"], AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION_V2)

    def test_synthetic_storage_owner_token_mismatch_fails_before_terminal_writes(self) -> None:
        occurrence = self._occurrence(suffix="tokenownermismatch")
        bundle = self._preview_bundle(suffix="tokenownermismatch")
        terminal = self._token_terminal(
            bundle,
            attempt_id="resultattempt_tokenownermismatch",
            token="membership_revision:01HZX.proposed-token",
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)

        with self.assertRaisesRegex(ValueError, "synthetic storage owner revision token mismatch"):
            self._accept_synthetic_token_owner(
                occurrence=occurrence,
                terminal=terminal,
                physical_owner_revision_token="membership_revision:01HZX.physical-token",
            )

        slot = self.repository.get_agent_tool_result_slot(
            occurrence.result_slot_id,
            workspace_id=occurrence.workspace_id,
            actor_id=occurrence.actor_id,
            runtime_namespace=occurrence.runtime_namespace,
            provider_mode=occurrence.provider_mode,
        )
        self.assertEqual(slot["status"], "pending")
        self.assertEqual(slot["schema_version"], AGENT_TOOL_RESULT_SLOT_SCHEMA_VERSION)
        self.assertEqual(self.repository.list_agent_tool_result_attempts(occurrence.result_slot_id), [])
        self.assertEqual(self._counts()["agent_tool_result_journal"], 0)

    def test_opaque_revision_token_collision_fails_exact_replay(self) -> None:
        occurrence = self._occurrence(suffix="tokencollision")
        bundle = self._preview_bundle(suffix="tokencollision")
        terminal = self._token_terminal(bundle)
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        self._accept_synthetic_token_owner(occurrence=occurrence, terminal=terminal)

        collision = replace(
            terminal,
            owner_target_revision_token="membership_revision:01HZX.colliding-token",
        )
        with self.assertRaisesRegex(ValueError, "accepted slot terminal collision.*owner_target_revision_token"):
            self._accept_synthetic_token_owner(occurrence=occurrence, terminal=collision)

    def test_owner_or_serializer_mismatch_fails_with_zero_terminal_writes(self) -> None:
        occurrence = self._occurrence()
        bundle = self._preview_bundle()
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)

        missing_owner = self._terminal(bundle)
        object.__setattr__(missing_owner, "owner_target_id", "preview_missing")
        with self.assertRaisesRegex(ValueError, "exact owner not found"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=missing_owner,
                attempted_slot_generation=1,
            )
        foreign_kwargs = _uow_kwargs(suffix="foreign")
        foreign_kwargs["workspace_id"] = "workspace_foreign"
        foreign_kwargs["requester_id"] = "requester_foreign"
        foreign_target = dict(foreign_kwargs["target_ref"])
        foreign_target["workspace_id"] = "workspace_foreign"
        foreign_target["requester_id"] = "requester_foreign"
        foreign_kwargs["target_ref"] = foreign_target
        foreign_bundle = self.repository.create_acquisition_plan_preview_uow(**foreign_kwargs)
        foreign_owner = self._terminal(
            foreign_bundle,
            attempt_id="resultattempt_foreign",
            provider_call_id="provider-call-foreign",
            tool_call_id="tool-call-foreign",
        )
        with self.assertRaisesRegex(ValueError, "exact-owner mismatch"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=foreign_owner,
                attempted_slot_generation=1,
            )
        forged_owner_token = replace(
            self._terminal(bundle),
            owner_target_revision_token="membership_revision:forged-plan-owner-token",
        )
        with self.assertRaisesRegex(ValueError, "exact-owner mismatch: owner_target_revision_token"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=forged_owner_token,
                attempted_slot_generation=1,
            )
        changed_output = self._terminal(
            bundle,
            serialized_result={
                "variant": "success",
                "status": "ready",
                "preview": {"preview_id": "forged"},
            },
        )
        with self.assertRaisesRegex(ValueError, "serializer output mismatch"):
            self.adapter.accept_acquisition_plan_tool_result_uow(
                occurrence=occurrence,
                terminal=changed_output,
                attempted_slot_generation=1,
            )
        self.assertEqual(
            self._counts(),
            {
                "agent_tool_result_slots": 1,
                "agent_tool_result_attempts": 0,
                "agent_tool_result_journal": 0,
            },
        )
        self.assertEqual(
            self.repository.get_agent_tool_result_slot(
                occurrence.result_slot_id,
                workspace_id=occurrence.workspace_id,
                actor_id=occurrence.actor_id,
                runtime_namespace=occurrence.runtime_namespace,
                provider_mode=occurrence.provider_mode,
            )["status"],
            "pending",
        )

    def test_each_precommit_fault_rolls_back_attempt_slot_and_journal(self) -> None:
        for index, fault in enumerate(("after_attempt_write", "after_slot_write", "after_journal_write"), start=1):
            with self.subTest(fault=fault):
                suffix = f"fault{index}"
                occurrence = self._occurrence(suffix=suffix)
                bundle = self._preview_bundle(suffix=suffix)
                terminal = self._terminal(bundle, attempt_id=f"resultattempt_{suffix}")
                self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
                with self.assertRaisesRegex(RuntimeError, "injected agent tool result"):
                    self.adapter.accept_acquisition_plan_tool_result_uow(
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
                self.assertEqual(self.repository.list_agent_tool_result_attempts(occurrence.result_slot_id), [])

    def test_competing_terminal_attempts_produce_one_winner_and_one_quarantine(self) -> None:
        occurrence = self._occurrence()
        bundle = self._preview_bundle()
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        terminals = (
            self._terminal(bundle, attempt_id="resultattempt_a", provider_call_id="provider-a", tool_call_id="tool-a"),
            self._terminal(bundle, attempt_id="resultattempt_b", provider_call_id="provider-b", tool_call_id="tool-b"),
        )
        barrier = threading.Barrier(2)
        outcomes: list[str] = []
        errors: list[BaseException] = []

        def worker(terminal: AgentToolTerminalResult) -> None:
            try:
                barrier.wait(timeout=5)
                result = self.adapter.accept_acquisition_plan_tool_result_uow(
                    occurrence=occurrence,
                    terminal=terminal,
                    attempted_slot_generation=1,
                )
                outcomes.append(str((result or {}).get("outcome")))
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(terminal,)) for terminal in terminals]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertEqual(sorted(outcomes), ["accepted", "quarantined"])
        attempts = self.repository.list_agent_tool_result_attempts(occurrence.result_slot_id)
        self.assertEqual(
            sorted(attempt["disposition"] for attempt in attempts),
            ["accepted", "quarantined"],
        )
        self.assertEqual(self._counts()["agent_tool_result_journal"], 1)

    @unittest.skipIf(psycopg is None, "psycopg unavailable")
    def test_deferred_database_guard_rejects_standalone_accepted_attempt(self) -> None:
        occurrence = self._occurrence()
        bundle = self._preview_bundle()
        terminal = self._terminal(bundle)
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        attempt_row = result_postgres._attempt_insert_row(  # noqa: SLF001
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=1,
            disposition="accepted",
            quarantine_reason="",
        )

        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                result_postgres._insert_dict(  # noqa: SLF001
                    cursor,
                    table_name="agent_tool_result_attempts",
                    row=attempt_row,
                )
            with self.assertRaises(psycopg.errors.RaiseException) as raised:
                connection.commit()
            self.assertEqual(raised.exception.diag.constraint_name, "agent_tool_terminal_aggregate_incomplete")
            connection.rollback()
        self.assertEqual(self.repository.list_agent_tool_result_attempts(occurrence.result_slot_id), [])

    @unittest.skipIf(psycopg is None, "psycopg unavailable")
    def test_deferred_database_guard_rejects_opaque_revision_token_aggregate_mismatch(self) -> None:
        occurrence = self._occurrence(suffix="tokenaggregate")
        bundle = self._preview_bundle(suffix="tokenaggregate")
        terminal = self._token_terminal(bundle, attempt_id="resultattempt_tokenaggregate")
        mismatched_terminal = replace(
            terminal,
            owner_target_revision_token="membership_revision:01HZX.aggregate-mismatch",
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        attempt_row = result_postgres._attempt_insert_row(  # noqa: SLF001
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=1,
            disposition="accepted",
            quarantine_reason="",
        )
        slot_updates = result_postgres._slot_terminal_update(mismatched_terminal)  # noqa: SLF001
        journal_row = result_postgres._journal_insert_row(  # noqa: SLF001
            occurrence=occurrence,
            terminal=terminal,
            journal_id="tooljournal_tokenaggregate",
        )

        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                result_postgres._insert_dict(  # noqa: SLF001
                    cursor,
                    table_name="agent_tool_result_attempts",
                    row=attempt_row,
                )
                assignments = ", ".join(f'"{column}" = %s' for column in slot_updates)
                cursor.execute(
                    (
                        f"UPDATE agent_tool_result_slots SET {assignments}, "
                        "accepted_at = transaction_timestamp() "
                        "WHERE result_slot_id = %s AND slot_generation = %s AND status = 'pending'"
                    ),
                    (*tuple(slot_updates.values()), occurrence.result_slot_id, occurrence.slot_generation),
                )
                journal_columns = list(journal_row)
                quoted_journal_columns = ", ".join('"' + column + '"' for column in journal_columns)
                cursor.execute(
                    (
                        "INSERT INTO agent_tool_result_journal "
                        f"({quoted_journal_columns}, accepted_at) "
                        f"VALUES ({', '.join(['%s'] * len(journal_columns))}, transaction_timestamp())"
                    ),
                    tuple(journal_row[column] for column in journal_columns),
                )
            with self.assertRaises(psycopg.errors.RaiseException) as raised:
                connection.commit()
            self.assertEqual(raised.exception.diag.constraint_name, "agent_tool_terminal_aggregate_incomplete")
            connection.rollback()
        self.assertEqual(self.repository.list_agent_tool_result_attempts(occurrence.result_slot_id), [])

    @unittest.skipIf(psycopg is None, "psycopg unavailable")
    def test_database_guards_reject_slot_rewrite_and_append_row_mutation(self) -> None:
        occurrence = self._occurrence()
        bundle = self._preview_bundle()
        terminal = self._terminal(bundle)
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        self.repository.accept_acquisition_plan_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=1,
        )

        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                with self.assertRaises(psycopg.errors.RaiseException):
                    cursor.execute(
                        "UPDATE agent_tool_result_slots SET actor_id = 'forged' WHERE result_slot_id = %s",
                        (occurrence.result_slot_id,),
                    )
                connection.rollback()
                with connection.cursor() as cursor_2:
                    with self.assertRaises(psycopg.errors.RaiseException):
                        cursor_2.execute(
                            "DELETE FROM agent_tool_result_attempts WHERE result_attempt_id = %s",
                            (terminal.result_attempt_id,),
                        )
                connection.rollback()


if __name__ == "__main__":
    unittest.main()
