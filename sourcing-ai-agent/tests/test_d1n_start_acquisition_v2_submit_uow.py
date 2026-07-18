from __future__ import annotations

import copy
import hashlib
import json
import re
import tempfile
import time
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from sourcing_agent import acquisition_start_v2_postgres as start_pg
from sourcing_agent.acquisition_start_v2 import (
    ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT,
    ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    AcquisitionStartV2BindContext,
    AcquisitionStartV2Error,
    AcquisitionStartV2OwnerBinder,
    AcquisitionStartV2ToolPins,
)
from sourcing_agent.agent_canary_registry import (
    START_ACQUISITION_RUN_TOOL_SPEC,
    START_ACQUISITION_RUN_TOOL_SPEC_V2,
)
from sourcing_agent.agent_tool_result_postgres import _slot_insert_row
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence, AgentToolResultSlotError
from sourcing_agent.control_plane_live_postgres import ControlPlaneAdvisoryLockBusy
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin
from tests.test_d1n_acquisition_plan_preview_uow import _uow_kwargs
from tests.test_d1n_start_acquisition_v2 import (
    _CONTEXT,
    _NOW,
    _preview,
    _PreviewRepository,
    _reference,
)


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _current_bound(preview=None):
    candidate = preview or _preview()
    pins = AcquisitionStartV2ToolPins(
        tool_spec_version=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version,
        tool_spec_digest=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_digest,
    )
    bound = AcquisitionStartV2OwnerBinder(_PreviewRepository(candidate)).bind(
        input_payload=_reference(candidate),
        context=_CONTEXT,
        tool_pins=pins,
        now=_NOW,
    )
    return candidate, bound


def _occurrence(*, provider_mode: str = "simulate", tool_spec=START_ACQUISITION_RUN_TOOL_SPEC):
    preview = _preview()
    pins = AcquisitionStartV2ToolPins(
        tool_spec_version=tool_spec.tool_spec_version,
        tool_spec_digest=tool_spec.tool_spec_digest,
    )
    bound = AcquisitionStartV2OwnerBinder(_PreviewRepository(preview)).bind(
        input_payload=_reference(preview),
        context=_CONTEXT,
        tool_pins=pins,
        now=_NOW,
    )
    occurrence = AgentToolOccurrence.from_tool_spec(
        result_slot_id="slot_start_submit_1",
        slot_generation=1,
        workspace_id=_CONTEXT.workspace_id,
        actor_id=_CONTEXT.requester_id,
        runtime_namespace="isolated_local_canary",
        provider_mode=provider_mode,
        turn_id="turn_start_submit_1",
        step_id="step_start_submit_1",
        tool_spec=tool_spec,
        canonical_args=bound.to_record(),
        occurrence_ordinal=1,
    )
    return preview, occurrence


def _pending_slot(occurrence: AgentToolOccurrence) -> dict[str, Any]:
    row = _slot_insert_row(occurrence)
    row.update(
        {
            "result_attempt_id": "",
            "provider_call_id": "",
            "tool_call_id": "",
            "action_id": "",
            "operation_run_id": "",
            "workflow_command_id": "",
            "activity_run_id": "",
            "activity_attempt_id": "",
            "command_attempt": 0,
            "command_generation": 0,
            "control_epoch": 0,
            "owner_target_kind": "",
            "owner_target_id": "",
            "owner_target_revision": 0,
            "owner_target_generation": 0,
            "owner_target_revision_token": "",
            "terminal_winner_id": "",
            "owner_result_ref_json": {},
            "owner_result_digest": "",
            "serialized_result_json": "",
            "serialized_result_digest": "",
            "tool_result_message_json": {},
            "tool_result_message_digest": "",
            "is_error": False,
            "created_at": datetime(2026, 7, 17, 0, 29, tzinfo=timezone.utc),
            "accepted_at": None,
        }
    )
    return row


def _preview_row(preview) -> dict[str, Any]:
    record = preview.to_record()
    return {
        "preview_id": preview.preview_id,
        "workspace_id": record["workspace_id"],
        "requester_id": record["requester_id"],
        "preview_revision": preview.preview_revision,
        "preview_digest": preview.preview_digest,
        "preview_json": copy.deepcopy(record),
        # psycopg returns physical TIMESTAMPTZ values as aware datetimes while
        # the immutable nested preview uses canonical ISO-Z strings.
        "created_at": datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc),
        "expires_at": datetime(2026, 7, 17, 1, 0, tzinfo=timezone.utc),
    }


class _FakeDatabase:
    def __init__(self, *, preview, occurrence: AgentToolOccurrence) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = {
            "agent_actions": [],
            "operation_events": [],
            "agent_tool_result_slots": [_pending_slot(occurrence)],
            "acquisition_plan_previews": [_preview_row(preview)],
        }
        self.transaction_timestamp_iso = "2026-07-17T00:30:00Z"
        self.timestamp_select_count = 0


class _FakeCursor:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection
        self.description: list[Any] = []
        self._rows: list[dict[str, Any]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        compact = " ".join(sql.split())
        values = tuple(params or ())
        self.connection.executed.append((compact, values))
        self._rows = []
        tables = self.connection.working
        if compact.startswith("SELECT set_config"):
            return
        if compact.startswith("SELECT * FROM agent_actions"):
            action_id, workspace_id, idempotency = values
            self._rows = [
                copy.deepcopy(row)
                for row in tables["agent_actions"]
                if row.get("action_id") == action_id
                or (row.get("workspace_id") == workspace_id and row.get("idempotency_key") == idempotency)
            ]
            return
        if compact.startswith("SELECT * FROM agent_tool_result_slots"):
            slot_id, digest = values
            self._rows = [
                copy.deepcopy(row)
                for row in tables["agent_tool_result_slots"]
                if row.get("result_slot_id") == slot_id or row.get("logical_occurrence_digest") == digest
            ]
            return
        if compact.startswith("SELECT * FROM acquisition_plan_previews"):
            preview_id, workspace_id, requester_id, revision, preview_digest = values
            self._rows = [
                copy.deepcopy(row)
                for row in tables["acquisition_plan_previews"]
                if row.get("preview_id") == preview_id
                and row.get("workspace_id") == workspace_id
                and row.get("requester_id") == requester_id
                and row.get("preview_revision") == revision
                and row.get("preview_digest") == preview_digest
            ]
            return
        if compact.startswith("SELECT * FROM operation_events"):
            event_id, stream_by_sequence, stream_by_key, event_key = values
            self._rows = [
                copy.deepcopy(row)
                for row in tables["operation_events"]
                if row.get("event_id") == event_id
                or (row.get("event_stream_id") == stream_by_sequence and row.get("sequence_number") == 1)
                or (row.get("event_stream_id") == stream_by_key and row.get("idempotency_key") == event_key)
            ]
            return
        if compact.startswith("SELECT to_char(date_trunc"):
            self.connection.database.timestamp_select_count += 1
            self._rows = [
                {"transaction_timestamp_iso": self.connection.database.transaction_timestamp_iso}
            ]
            return
        match = re.match(r'INSERT INTO ([a-z_]+) \((.+)\) VALUES \((.+)\) RETURNING \*', compact)
        if match:
            table_name = match.group(1)
            columns = [token.strip().strip('"') for token in match.group(2).split(",")]
            row = dict(zip(columns, values))
            tables[table_name].append(copy.deepcopy(row))
            self._rows = [copy.deepcopy(row)]
            return
        raise AssertionError(f"unexpected fake SQL: {compact}")

    def fetchall(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self._rows)

    def fetchone(self) -> dict[str, Any] | None:
        return copy.deepcopy(self._rows[0]) if self._rows else None


class _FakeConnection:
    def __init__(self, database: _FakeDatabase) -> None:
        self.database = database
        self.working = copy.deepcopy(database.tables)
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.database.tables = copy.deepcopy(self.working)
        self.commit_count += 1

    def rollback(self) -> None:
        self.working = copy.deepcopy(self.database.tables)
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


class _FakeAdapter:
    def __init__(self, database: _FakeDatabase, *, authoritative: bool = True) -> None:
        self.database = database
        self.authoritative = authoritative
        self.connections: list[_FakeConnection] = []
        self.connect_timeouts: list[float] = []
        self.lock_keys: list[str] = []
        self.dependency_reads: list[str] = []

    def should_prefer_read(self, table_name: str) -> bool:
        self.dependency_reads.append(table_name)
        return True

    def is_authoritative(self, table_name: str) -> bool:
        del table_name
        return self.authoritative

    def _connect_with_timeout(self, timeout: float) -> _FakeConnection:
        self.connect_timeouts.append(timeout)
        connection = _FakeConnection(self.database)
        self.connections.append(connection)
        return connection

    def _try_acquire_transaction_lock(self, cursor: _FakeCursor, lock_key: str) -> bool:
        del cursor
        self.lock_keys.append(lock_key)
        return True


def _fake_runtime(*, provider_mode: str = "simulate"):
    preview, occurrence = _occurrence(provider_mode=provider_mode)
    database = _FakeDatabase(preview=preview, occurrence=occurrence)
    return occurrence, database, _FakeAdapter(database)


def _advance_action_to_approved(database: _FakeDatabase) -> None:
    action = database.tables["agent_actions"][0]
    action.update(
        {
            "status": "queued",
            "approval_status": "approved",
            "result_ref_json": _json_dump({"schema_version": "successor_owner_ref.v1"}),
            "updated_at": "2026-07-17T00:31:00Z",
        }
    )


@pytest.mark.parametrize("provider_mode", ["live", "replay"])
def test_disallowed_modes_fail_before_dependency_schema_or_connection(provider_mode: str) -> None:
    occurrence, database, adapter = _fake_runtime(provider_mode=provider_mode)
    baseline = copy.deepcopy(database.tables)

    with pytest.raises(AgentToolResultSlotError, match="runtime_mode_not_allowed"):
        start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert adapter.dependency_reads == []
    assert adapter.connections == []
    assert database.tables == baseline


def test_disallowed_namespace_fails_before_dependency_schema_or_connection() -> None:
    occurrence, database, adapter = _fake_runtime()
    forbidden = replace(occurrence, runtime_namespace="hosted")
    baseline = copy.deepcopy(database.tables)

    with pytest.raises(AgentToolResultSlotError, match="runtime_mode_not_allowed"):
        start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=forbidden)

    assert adapter.dependency_reads == []
    assert adapter.connections == []
    assert database.tables == baseline


def test_historical_but_noncurrent_v2_tool_fails_before_adapter_access() -> None:
    preview, occurrence = _occurrence(tool_spec=START_ACQUISITION_RUN_TOOL_SPEC_V2)
    database = _FakeDatabase(preview=preview, occurrence=occurrence)
    adapter = _FakeAdapter(database)

    with pytest.raises(AgentToolResultSlotError, match="current_tool_pin_mismatch"):
        start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert adapter.dependency_reads == []
    assert adapter.connections == []


def test_noncanonical_full_root_fails_before_adapter_access() -> None:
    occurrence, database, adapter = _fake_runtime()
    forged_json = _json_dump({"preview_id": "preview_1"})
    forged = replace(
        occurrence,
        canonical_args_json=forged_json,
        canonical_args_digest=hashlib.sha256(forged_json.encode("utf-8")).hexdigest(),
    )
    database.tables["agent_tool_result_slots"] = [_pending_slot(forged)]

    with pytest.raises(AcquisitionStartV2Error, match="acquisition_start_v2_request_invalid"):
        start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=forged)

    assert adapter.dependency_reads == []
    assert adapter.connections == []


def test_read_preferred_but_non_authoritative_dependency_fails_without_connection_or_bootstrap() -> None:
    occurrence, database, _ = _fake_runtime()
    adapter = _FakeAdapter(database, authoritative=False)

    with pytest.raises(RuntimeError, match="authoritative PostgreSQL"):
        start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert adapter.connections == []
    assert adapter.dependency_reads == ["agent_actions"]


def test_slow_connection_cannot_continue_after_the_overall_deadline() -> None:
    occurrence, database, adapter = _fake_runtime()
    baseline = copy.deepcopy(database.tables)
    original_connect = adapter._connect_with_timeout

    def slow_connect(timeout: float) -> _FakeConnection:
        time.sleep(max(timeout, 0.03))
        return original_connect(timeout)

    adapter._connect_with_timeout = slow_connect  # type: ignore[method-assign]

    with pytest.raises(ControlPlaneAdvisoryLockBusy):
        start_pg.submit_acquisition_start_v2_action_uow(
            adapter,
            occurrence=occurrence,
            lock_timeout_seconds=0.01,
        )

    assert database.tables == baseline
    assert len(adapter.connections) == 1
    assert adapter.connections[0].rollback_count == 1


def test_transaction_deadline_configures_lock_and_statement_timeouts() -> None:
    occurrence, _, adapter = _fake_runtime()

    start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    deadline_statements = [
        (sql, params)
        for connection in adapter.connections
        for sql, params in connection.executed
        if sql.startswith("SELECT set_config")
    ]
    assert deadline_statements
    assert all("lock_timeout" in sql and "statement_timeout" in sql for sql, _ in deadline_statements)
    assert all(len(params) == 2 for _, params in deadline_statements)


@pytest.mark.parametrize("provider_mode", ["simulate", "scripted"])
def test_submit_writes_exact_pending_action_and_sequence_one_event(provider_mode: str) -> None:
    occurrence, database, adapter = _fake_runtime(provider_mode=provider_mode)
    result = start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert result["outcome"] == "submitted"
    assert result["replayed"] is False
    assert len(database.tables["agent_actions"]) == 1
    assert len(database.tables["operation_events"]) == 1
    assert database.timestamp_select_count == 1
    assert len(adapter.connections) == 1
    assert adapter.connections[0].commit_count == 1
    action = database.tables["agent_actions"][0]
    event = database.tables["operation_events"][0]
    assert (action["status"], action["approval_status"], action["approval_policy"]) == (
        "approval_required",
        "required",
        "required",
    )
    assert action["created_at"] == action["updated_at"] == "2026-07-17T00:30:00Z"
    assert action["input_json"] == json.dumps(
        occurrence.canonical_args["input_payload"], ensure_ascii=False
    )
    assert json.loads(action["target_ref_json"]) == occurrence.canonical_args["target_ref"]
    assert json.loads(action["metadata_json"]) == {
        "result_occurrence_ref": {
            "result_slot_id": occurrence.result_slot_id,
            "slot_generation": occurrence.slot_generation,
            "logical_occurrence_digest": occurrence.logical_occurrence_digest,
        }
    }
    assert event["event_type"] == "ActionApprovalRequired"
    assert event["sequence_number"] == 1
    assert event["operation_run_id"] == ""
    assert event["actor"] == occurrence.actor_id
    assert event["source"] == "agent_start_v2_submit_uow"
    assert event["schema_version"] == "acquisition_start_approval_required.v1"
    payload = json.loads(event["payload_json"])
    assert list(payload) == [
        "schema_version",
        "action_id",
        "workspace_id",
        "requester_id",
        "action_type",
        "request",
        "request_schema_ref",
        "tool_spec_ref",
        "result_contract_ref",
        "result_occurrence_ref",
        "start_snapshot_digest",
    ]
    assert payload["request"] == occurrence.canonical_args
    assert payload["result_occurrence_ref"] == json.loads(action["metadata_json"])["result_occurrence_ref"]


def test_submit_uses_exact_bytewise_lock_groups_in_required_order() -> None:
    occurrence, _, adapter = _fake_runtime()
    binding = start_pg.revalidate_acquisition_start_v2_occurrence(occurrence)
    expected = [key for group in start_pg.acquisition_start_v2_submit_lock_groups(binding) for key in group]

    start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert adapter.lock_keys == expected
    assert expected[0] == f"operation_events:{binding.action_id}"
    assert expected == [key for group in start_pg.acquisition_start_v2_submit_lock_groups(binding) for key in group]
    assert all(list(group) == sorted(group, key=lambda value: value.encode("utf-8")) for group in start_pg.acquisition_start_v2_submit_lock_groups(binding))


def test_exact_replay_uses_persisted_timestamp_and_writes_nothing() -> None:
    occurrence, database, adapter = _fake_runtime()
    first = start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)
    baseline = copy.deepcopy(database.tables)

    replay = start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert first["outcome"] == "submitted"
    assert replay["outcome"] == "replayed"
    assert replay["replayed"] is True
    assert database.tables == baseline
    assert database.timestamp_select_count == 1


def test_submit_replays_after_the_ratified_create_successor() -> None:
    occurrence, database, adapter = _fake_runtime()
    start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)
    _advance_action_to_approved(database)
    baseline = copy.deepcopy(database.tables)

    replay = start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert replay["outcome"] == "replayed"
    assert replay["action"]["status"] == "queued"
    assert replay["action"]["approval_status"] == "approved"
    assert database.tables == baseline


def test_submit_replays_after_the_ratified_result_accept_successor() -> None:
    occurrence, database, adapter = _fake_runtime()
    start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)
    _advance_action_to_approved(database)
    database.tables["agent_tool_result_slots"][0].update(
        {
            "status": "accepted",
            "result_attempt_id": "attempt_start_1",
            "accepted_at": datetime(2026, 7, 17, 0, 32, tzinfo=timezone.utc),
        }
    )
    baseline = copy.deepcopy(database.tables)

    replay = start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert replay["outcome"] == "replayed"
    assert database.tables == baseline


def test_accepted_slot_without_the_approved_action_successor_is_a_collision() -> None:
    occurrence, database, adapter = _fake_runtime()
    start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)
    database.tables["agent_tool_result_slots"][0].update(
        {
            "status": "accepted",
            "result_attempt_id": "attempt_start_1",
            "accepted_at": datetime(2026, 7, 17, 0, 32, tzinfo=timezone.utc),
        }
    )
    baseline = copy.deepcopy(database.tables)

    with pytest.raises(ValueError, match="successor lifecycle collision"):
        start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert database.tables == baseline


@pytest.mark.parametrize("fault_point", ["after_action_write", "after_event_write"])
def test_precommit_faults_roll_back_action_and_event(fault_point: str) -> None:
    occurrence, database, adapter = _fake_runtime()

    with pytest.raises(RuntimeError, match="injected acquisition start submit fault"):
        start_pg.submit_acquisition_start_v2_action_uow(
            adapter,
            occurrence=occurrence,
            fault_injection_point=fault_point,
        )

    assert database.tables["agent_actions"] == []
    assert database.tables["operation_events"] == []
    assert adapter.connections[0].rollback_count == 1


def test_postcommit_lost_ack_replays_without_duplicate_or_new_timestamp() -> None:
    occurrence, database, adapter = _fake_runtime()
    with pytest.raises(RuntimeError, match="after commit"):
        start_pg.submit_acquisition_start_v2_action_uow(
            adapter,
            occurrence=occurrence,
            fault_injection_point="after_commit",
        )

    assert len(database.tables["agent_actions"]) == 1
    assert len(database.tables["operation_events"]) == 1
    replay = start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)
    assert replay["outcome"] == "replayed"
    assert len(database.tables["agent_actions"]) == 1
    assert len(database.tables["operation_events"]) == 1
    assert database.timestamp_select_count == 1


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("slot_missing", "result slot identity collision"),
        ("slot_invalid", "result slot is not pending"),
        ("preview_foreign", ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT),
        ("preview_missing", ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT),
        ("preview_expired", ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT),
    ],
)
def test_missing_foreign_or_terminal_owners_write_zero_rows(mutation: str, error: str) -> None:
    occurrence, database, adapter = _fake_runtime()
    if mutation == "slot_missing":
        database.tables["agent_tool_result_slots"] = []
    elif mutation == "slot_invalid":
        database.tables["agent_tool_result_slots"][0]["status"] = "forged"
    elif mutation == "preview_foreign":
        database.tables["acquisition_plan_previews"][0]["workspace_id"] = "foreign"
    elif mutation == "preview_expired":
        database.transaction_timestamp_iso = "2026-07-17T01:00:00Z"
    else:
        database.tables["acquisition_plan_previews"] = []

    with pytest.raises((ValueError, AcquisitionStartV2Error), match=error):
        start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert database.tables["agent_actions"] == []
    assert database.tables["operation_events"] == []


@pytest.mark.parametrize("mutation,error", [("action", "Action immutable"), ("event", "event immutable")])
def test_existing_collision_writes_zero_new_rows(mutation: str, error: str) -> None:
    occurrence, database, adapter = _fake_runtime()
    start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)
    if mutation == "action":
        database.tables["agent_actions"][0]["owner_module"] = "forged_owner"
    else:
        database.tables["operation_events"][0]["actor"] = "forged_actor"
    baseline = copy.deepcopy(database.tables)

    with pytest.raises(ValueError, match=error):
        start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert database.tables == baseline


def test_partial_existing_aggregate_is_a_zero_write_collision() -> None:
    occurrence, database, adapter = _fake_runtime()
    start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)
    database.tables["operation_events"] = []
    baseline = copy.deepcopy(database.tables)

    with pytest.raises(ValueError, match="partial aggregate collision"):
        start_pg.submit_acquisition_start_v2_action_uow(adapter, occurrence=occurrence)

    assert database.tables == baseline


def test_physical_preview_timestamp_codec_accepts_pg_aware_datetime_and_rejects_subseconds() -> None:
    assert start_pg._utc_second_iso(datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc)) == "2026-07-17T00:00:00Z"
    assert start_pg._utc_second_iso("2026-07-17T08:00:00+08:00") == "2026-07-17T00:00:00Z"
    assert start_pg._utc_second_iso(datetime(2026, 7, 17, 0, 0, 0, 1, tzinfo=timezone.utc)) == ""


class _NestedPreviewReader:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def get_acquisition_plan_preview(self, preview_id: str, **owner: Any) -> dict[str, Any] | None:
        row = self.repository.get_acquisition_plan_preview(preview_id, **owner)
        preview = row.get("preview") if isinstance(row, dict) else None
        return dict(preview) if isinstance(preview, dict) else None


class D1nStartAcquisitionV2SubmitUowPGTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1n_start_submit"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(Path(self.tempdir.name) / "start-submit.db")
        self.repository = self.store.repos.workflow_runtime
        self.adapter = self.store._control_plane_postgres  # noqa: SLF001

    def _physical_count(self, table_name: str) -> int:
        with self.adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                return int(cursor.fetchone()[0])

    def test_real_pg_submit_and_replay_accept_jsonb_and_timestamptz_rows(self) -> None:
        kwargs = _uow_kwargs(suffix="start_submit_pg")
        kwargs["start_request_schema_version"] = ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        kwargs["start_request_schema_digest"] = ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
        preview_bundle = self.repository.create_acquisition_plan_preview_uow(**kwargs)
        preview = dict(preview_bundle["preview"])
        reference = {
            "preview_id": preview["preview_id"],
            "preview_revision": preview["preview_revision"],
            "preview_digest": preview["preview_digest"],
        }
        pins = AcquisitionStartV2ToolPins(
            tool_spec_version=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version,
            tool_spec_digest=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_digest,
        )
        bound = AcquisitionStartV2OwnerBinder(_NestedPreviewReader(self.repository)).bind(
            input_payload=reference,
            context=AcquisitionStartV2BindContext(
                workspace_id=kwargs["workspace_id"],
                requester_id=kwargs["requester_id"],
            ),
            tool_pins=pins,
            now=datetime.now(timezone.utc),
        )
        occurrence = AgentToolOccurrence.from_tool_spec(
            result_slot_id="slot_start_submit_pg",
            slot_generation=1,
            workspace_id=kwargs["workspace_id"],
            actor_id=kwargs["requester_id"],
            runtime_namespace="isolated_local_canary",
            provider_mode="simulate",
            turn_id="turn_start_submit_pg",
            step_id="step_start_submit_pg",
            tool_spec=START_ACQUISITION_RUN_TOOL_SPEC,
            canonical_args=bound.to_record(),
            occurrence_ordinal=1,
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        baseline_actions = self._physical_count("agent_actions")
        baseline_events = self._physical_count("operation_events")

        first = start_pg.submit_acquisition_start_v2_action_uow(self.adapter, occurrence=occurrence)
        replay = start_pg.submit_acquisition_start_v2_action_uow(self.adapter, occurrence=occurrence)

        self.assertEqual(first["outcome"], "submitted")
        self.assertEqual(replay["outcome"], "replayed")
        self.assertEqual(self._physical_count("agent_actions"), baseline_actions + 1)
        self.assertEqual(self._physical_count("operation_events"), baseline_events + 1)
        self.assertEqual(self._physical_count("operation_runs"), 1)
        self.assertEqual(self._physical_count("workflow_commands"), 0)
        self.assertEqual(self._physical_count("runtime_outbox"), 0)
