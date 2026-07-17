from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import sourcing_agent.agent_canary_registry as canary_registry
import sourcing_agent.agent_tool_result_postgres as result_postgres
from sourcing_agent.agent_canary_registry import (
    INSPECT_OPERATION_TOOL_SPEC_V1,
    INSPECT_OPERATION_TOOL_SPEC_V2,
    INSPECT_OPERATION_TOOL_SPEC_V3,
    LOCAL_CANARY_AGENT_TOOL_REGISTRY,
)
from sourcing_agent.agent_operation_query_postgres import _resolve_inspect_operation_contract
from sourcing_agent.agent_projection_query import (
    INSPECT_OPERATION_RESULT_SPEC_V1,
    INSPECT_OPERATION_RESULT_SPEC_V2,
    serialize_inspect_operation_result_for_spec,
)
from sourcing_agent.agent_tool_registry import AgentToolRegistry
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence, AgentToolTerminalResult
from sourcing_agent.control_plane_live_postgres import _quote_identifier
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin
from tests.test_d1n_acquisition_plan_preview_uow import _uow_kwargs

_FIXTURE_ROOT = Path(__file__).with_name("fixtures") / "d1n"
_V1_FIXTURE_PATH = _FIXTURE_ROOT / "inspect_operation_result_v1.json"
_V2_FIXTURE_PATH = _FIXTURE_ROOT / "inspect_operation_result_v2_accepted.json"


def _load_fixture(path: Path) -> dict[str, Any]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def test_retained_v1_v2_serializers_match_frozen_historical_commit_bytes() -> None:
    cases = (
        (
            _load_fixture(_V1_FIXTURE_PATH),
            INSPECT_OPERATION_RESULT_SPEC_V1,
            "18c758314643ceb7959163f1767f31d02d598276",
            "c08bfc1655d2b88bfaf31faf29d58183c16c4e01",
        ),
        (
            _load_fixture(_V2_FIXTURE_PATH),
            INSPECT_OPERATION_RESULT_SPEC_V2,
            "38013035f0000fbe978e56b2ae55d8cbbc33fab3",
            "1ef5d0bd1ce1a3e75d6144944e61b9be08b708e6",
        ),
    )
    for fixture, result_spec, producer_commit, producer_blob in cases:
        frozen_result = dict(fixture["serialized_result"])
        frozen_json = _canonical_json(frozen_result)
        assert fixture["producer_commit"] == producer_commit
        assert fixture["producer_serializer_blob"] == producer_blob
        assert fixture["result_schema_version"] == result_spec.result_schema_version
        assert fixture["result_schema_digest"] == result_spec.result_schema_digest
        assert hashlib.sha256(frozen_json.encode("utf-8")).hexdigest() == fixture["serialized_result_sha256"]
        assert (
            serialize_inspect_operation_result_for_spec(
                frozen_result,
                result_spec=result_spec,
            )
            == frozen_json
        )


def test_retained_v1_serializer_matches_historical_reason_bearing_probe_bytes() -> None:
    fixture = _load_fixture(_V1_FIXTURE_PATH)
    probe = dict(fixture["historical_reason_probe"])
    assert probe == {
        "probe_schema_version": "inspect_operation_historical_reason_probe_v1",
        "producer_commit": "18c758314643ceb7959163f1767f31d02d598276",
        "producer_serializer_blob": "c08bfc1655d2b88bfaf31faf29d58183c16c4e01",
        "base_serialized_result_sha256": fixture["serialized_result_sha256"],
        "progress_reason": "operation_retry_requested",
        "serialized_result_sha256": "a482982fd143122b81e49c4c47b41b99433ce8a974a37fcec4f74243b86af334",
    }

    reason_bearing_result = json.loads(json.dumps(fixture["serialized_result"]))
    reason_bearing_result["progress"]["reason"] = probe["progress_reason"]
    reason_bearing_json = _canonical_json(reason_bearing_result)
    assert hashlib.sha256(reason_bearing_json.encode("utf-8")).hexdigest() == probe["serialized_result_sha256"]
    assert (
        serialize_inspect_operation_result_for_spec(
            reason_bearing_result,
            result_spec=INSPECT_OPERATION_RESULT_SPEC_V1,
        )
        == reason_bearing_json
    )


def test_retained_v3_physical_binding_ignores_future_current_alias_and_registry_input_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic_v4 = replace(
        INSPECT_OPERATION_TOOL_SPEC_V3,
        tool_spec_version="inspect_operation_tool_v4",
    )
    reordered_registry = AgentToolRegistry.from_specs(
        (
            synthetic_v4,
            INSPECT_OPERATION_TOOL_SPEC_V2,
            INSPECT_OPERATION_TOOL_SPEC_V3,
            INSPECT_OPERATION_TOOL_SPEC_V1,
        ),
        current_release_owner=LOCAL_CANARY_AGENT_TOOL_REGISTRY.current_release_owner,
    )
    monkeypatch.setattr(canary_registry, "LOCAL_CANARY_AGENT_TOOL_REGISTRY", reordered_registry)
    monkeypatch.setattr(canary_registry, "INSPECT_OPERATION_TOOL_SPEC", synthetic_v4)

    def occurrence(tool_spec: Any, *, suffix: str) -> AgentToolOccurrence:
        return AgentToolOccurrence.from_tool_spec(
            result_slot_id=f"inspectslot_{suffix}",
            slot_generation=1,
            workspace_id="workspace_1",
            actor_id="requester_1",
            runtime_namespace="isolated_local_canary",
            provider_mode="simulate",
            turn_id=f"turn_{suffix}",
            step_id="step_inspect",
            tool_spec=tool_spec,
            canonical_args={"operation_run_id": "oprun_historical_v3"},
            occurrence_ordinal=1,
        )

    retained_v3 = _resolve_inspect_operation_contract(occurrence(INSPECT_OPERATION_TOOL_SPEC_V3, suffix="retained_v3"))
    assert retained_v3.tool_spec is INSPECT_OPERATION_TOOL_SPEC_V3
    assert retained_v3.physical_owner_revision == "v3"
    assert retained_v3.include_progress_reason is False
    with pytest.raises(ValueError, match="historical contract unavailable"):
        _resolve_inspect_operation_contract(occurrence(synthetic_v4, suffix="future_v4"))


class D1nInspectOperationHistoricalFixtureReplayPGTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1n_inspect_history"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(Path(self.tempdir.name) / "inspect-history.db")
        self.repository = self.store.repos.workflow_runtime
        self.adapter = self.store._control_plane_postgres  # noqa: SLF001

    def _occurrence_from_fixture(self, fixture: dict[str, Any]) -> AgentToolOccurrence:
        historical = dict(fixture["historical_occurrence"])
        canonical_args = dict(historical["canonical_args"])
        occurrence = AgentToolOccurrence.from_tool_spec(
            result_slot_id=str(historical["result_slot_id"]),
            slot_generation=int(historical["slot_generation"]),
            workspace_id=str(historical["workspace_id"]),
            actor_id=str(historical["actor_id"]),
            runtime_namespace=str(historical["runtime_namespace"]),
            provider_mode=str(historical["provider_mode"]),
            turn_id=str(historical["turn_id"]),
            step_id=str(historical["step_id"]),
            tool_spec=INSPECT_OPERATION_TOOL_SPEC_V2,
            canonical_args=canonical_args,
            occurrence_ordinal=int(historical["occurrence_ordinal"]),
        )
        current_record = occurrence.to_record()
        self.assertEqual(current_record.pop("result_link_policy"), fixture["post_0013_result_link_policy"])
        self.assertEqual(current_record, historical)
        return occurrence

    def _terminal_from_fixture(self, fixture: dict[str, Any]) -> AgentToolTerminalResult:
        frozen = dict(fixture["accepted_terminal"])
        terminal = AgentToolTerminalResult.from_serialized_result(
            result_attempt_id=str(frozen["result_attempt_id"]),
            provider_call_id=str(frozen["provider_call_id"]),
            tool_call_id=str(frozen["tool_call_id"]),
            action_id=str(frozen["action_id"]),
            operation_run_id=str(frozen["operation_run_id"]),
            workflow_command_id=str(frozen["workflow_command_id"]),
            activity_run_id=str(frozen["activity_run_id"]),
            activity_attempt_id=str(frozen["activity_attempt_id"]),
            command_attempt=int(frozen["command_attempt"]),
            command_generation=int(frozen["command_generation"]),
            control_epoch=int(frozen["control_epoch"]),
            owner_target_kind=str(frozen["owner_target_kind"]),
            owner_target_id=str(frozen["owner_target_id"]),
            owner_target_revision=int(frozen["owner_target_revision"]),
            owner_target_generation=int(frozen["owner_target_generation"]),
            terminal_winner_id=str(frozen["terminal_winner_id"]),
            owner_result_ref=dict(frozen["owner_result_ref"]),
            owner_result_digest=str(frozen["owner_result_digest"]),
            serialized_result=dict(fixture["serialized_result"]),
            is_error=bool(frozen["is_error"]),
        )
        self.assertEqual(terminal.serialized_result_digest, fixture["serialized_result_sha256"])
        self.assertEqual(terminal.tool_result_message_digest, frozen["tool_result_message_digest"])
        self.assertEqual(terminal.serialized_result_json, _canonical_json(fixture["serialized_result"]))
        return terminal

    def _persist_frozen_accepted_aggregate(
        self,
        *,
        occurrence: AgentToolOccurrence,
        terminal: AgentToolTerminalResult,
        journal_id: str,
    ) -> None:
        attempt_row = result_postgres._attempt_insert_row(  # noqa: SLF001
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=occurrence.slot_generation,
            disposition="accepted",
            quarantine_reason="",
        )
        slot_updates = result_postgres._slot_terminal_update(terminal)  # noqa: SLF001
        journal_row = result_postgres._journal_insert_row(  # noqa: SLF001
            occurrence=occurrence,
            terminal=terminal,
            journal_id=journal_id,
        )
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                inserted_attempt = result_postgres._insert_dict(  # noqa: SLF001
                    cursor,
                    table_name="agent_tool_result_attempts",
                    row=attempt_row,
                )
                self.assertEqual(inserted_attempt["result_attempt_id"], terminal.result_attempt_id)
                assignments = ", ".join(f"{_quote_identifier(column)} = %s" for column in slot_updates)
                cursor.execute(
                    f"UPDATE agent_tool_result_slots SET {assignments}, "
                    "accepted_at = transaction_timestamp() "
                    "WHERE result_slot_id = %s AND slot_generation = %s AND status = 'pending' "
                    "RETURNING result_slot_id",
                    (
                        *tuple(slot_updates.values()),
                        occurrence.result_slot_id,
                        occurrence.slot_generation,
                    ),
                )
                self.assertEqual(cursor.fetchone(), (occurrence.result_slot_id,))
                columns = list(journal_row)
                cursor.execute(
                    "INSERT INTO agent_tool_result_journal "
                    f"({', '.join(_quote_identifier(column) for column in columns)}, accepted_at) "
                    f"VALUES ({', '.join(['%s'] * len(columns))}, transaction_timestamp()) "
                    "RETURNING journal_id",
                    tuple(journal_row[column] for column in columns),
                )
                self.assertEqual(cursor.fetchone(), (journal_id,))
            connection.commit()

    def test_head_replays_frozen_v2_terminal_accepted_by_historical_3801303(self) -> None:
        fixture = _load_fixture(_V2_FIXTURE_PATH)
        bundle = self.repository.create_acquisition_plan_preview_uow(**_uow_kwargs(suffix="historical_v2"))
        self.assertEqual(bundle["action"]["action_id"], fixture["accepted_terminal"]["action_id"])
        self.assertEqual(
            bundle["operation_run"]["operation_run_id"],
            fixture["accepted_terminal"]["operation_run_id"],
        )
        occurrence = self._occurrence_from_fixture(fixture)
        terminal = self._terminal_from_fixture(fixture)
        reserved = self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        self.assertEqual(reserved["outcome"], "reserved")
        acceptance = dict(fixture["historical_acceptance"])
        self._persist_frozen_accepted_aggregate(
            occurrence=occurrence,
            terminal=terminal,
            journal_id=str(acceptance["journal_id"]),
        )

        replay = self.repository.accept_inspect_operation_tool_result_uow(
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=occurrence.slot_generation,
        )

        self.assertEqual(replay["outcome"], "replayed")
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["slot"]["schema_version"], acceptance["slot_schema_version"])
        self.assertEqual(replay["attempt"]["schema_version"], acceptance["attempt_schema_version"])
        self.assertEqual(replay["journal"]["schema_version"], acceptance["journal_schema_version"])
        self.assertEqual(replay["slot"]["result_link_policy"], fixture["post_0013_result_link_policy"])
        self.assertEqual(replay["slot"]["serialized_result_json"], terminal.serialized_result_json)
        self.assertEqual(replay["slot"]["serialized_result_digest"], fixture["serialized_result_sha256"])
