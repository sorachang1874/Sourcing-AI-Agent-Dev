from __future__ import annotations

import tempfile
import threading
import unittest
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from sourcing_agent import agent_tool_result_postgres as result_postgres
from sourcing_agent.acquisition_plan_preview import (
    AcquisitionPlanPreview,
    acquisition_plan_preview_success_result,
)
from sourcing_agent.agent_canary_registry import PLAN_ACQUISITION_TOOL_SPEC, START_ACQUISITION_RUN_TOOL_SPEC
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
    ) -> AgentToolOccurrence:
        kwargs = _uow_kwargs(suffix=suffix)
        return AgentToolOccurrence.from_tool_spec(
            result_slot_id=f"toolslot_{suffix}",
            slot_generation=slot_generation,
            workspace_id=str(kwargs["workspace_id"]),
            actor_id="requester_1",
            runtime_namespace="isolated_local_canary",
            provider_mode="simulate",
            turn_id=f"turn_{suffix}",
            step_id="step_1",
            tool_spec=tool_spec,
            canonical_args={
                "input_payload": kwargs["input_payload"],
                "target_ref": kwargs["target_ref"],
            },
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
