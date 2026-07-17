"""PostgreSQL UoWs for exact Agent tool occurrences and result winners.

This module is deliberately below the tool registry and above raw SQL.  The
generic reserve path accepts any structurally valid historical tool spec.  The
first terminal owner adapter is intentionally narrow: the commandless
``plan_acquisition`` result is accepted only after the exact action, operation,
preview revision/digest, and terminal event are reloaded in the same
transaction and the registered serializer is re-run from that owner state.
"""

from __future__ import annotations

import math
import time
from hashlib import sha1
from typing import Any

from .agent_tool_result_slot import (
    AGENT_TOOL_RESULT_SLOT_SCHEMA_VERSION,
    AgentToolOccurrence,
    AgentToolTerminalResult,
)
from .json_contract import json_contract_equal

ACQUISITION_PLAN_PREVIEW_OWNER_TARGET_KIND = "acquisition_plan_preview_v1"


class _ResultSlotLockBusy(RuntimeError):
    pass


def _runtime_dependencies(adapter: Any, table_names: tuple[str, ...]) -> bool:
    for table_name in table_names:
        if not adapter.should_prefer_read(table_name):
            return False
        adapter._ensure_table_write_schema(table_name)
    return True


def _read_dependencies(adapter: Any, table_names: tuple[str, ...]) -> bool:
    """Check native read routing without bootstrapping or mutating schemas."""

    return all(adapter.should_prefer_read(table_name) for table_name in table_names)


def _deadline_seconds(lock_timeout_seconds: float) -> tuple[float, float]:
    if isinstance(lock_timeout_seconds, bool):
        raise ValueError("agent tool result lock timeout must be finite and positive")
    timeout_seconds = float(lock_timeout_seconds)
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("agent tool result lock timeout must be finite and positive")
    return timeout_seconds, time.monotonic() + timeout_seconds


def _slot_insert_row(occurrence: AgentToolOccurrence) -> dict[str, Any]:
    return {
        "result_slot_id": occurrence.result_slot_id,
        "slot_generation": occurrence.slot_generation,
        "workspace_id": occurrence.workspace_id,
        "actor_id": occurrence.actor_id,
        "runtime_namespace": occurrence.runtime_namespace,
        "provider_mode": occurrence.provider_mode,
        "turn_id": occurrence.turn_id,
        "step_id": occurrence.step_id,
        "tool_name": occurrence.tool_name,
        "tool_kind": occurrence.tool_kind,
        "effect_class": occurrence.effect_class,
        "result_link_policy": occurrence.result_link_policy,
        "tool_spec_version": occurrence.tool_spec_version,
        "tool_spec_digest": occurrence.tool_spec_digest,
        "canonical_args_json": occurrence.canonical_args_json,
        "canonical_args_digest": occurrence.canonical_args_digest,
        "occurrence_ordinal": occurrence.occurrence_ordinal,
        "logical_occurrence_digest": occurrence.logical_occurrence_digest,
        "request_schema_version": occurrence.request_schema_version,
        "request_schema_digest": occurrence.request_schema_digest,
        "result_schema_version": occurrence.result_schema_version,
        "result_schema_digest": occurrence.result_schema_digest,
        "serializer_owner": occurrence.serializer_owner,
        "serializer_revision": occurrence.serializer_revision,
        "serializer_contract_digest": occurrence.serializer_contract_digest,
        "status": "pending",
        "schema_version": AGENT_TOOL_RESULT_SLOT_SCHEMA_VERSION,
    }


_SLOT_IDENTITY_FIELDS = (
    "result_slot_id",
    "slot_generation",
    "workspace_id",
    "actor_id",
    "runtime_namespace",
    "provider_mode",
    "turn_id",
    "step_id",
    "tool_name",
    "tool_kind",
    "effect_class",
    "result_link_policy",
    "tool_spec_version",
    "tool_spec_digest",
    "canonical_args_digest",
    "occurrence_ordinal",
    "logical_occurrence_digest",
    "request_schema_version",
    "request_schema_digest",
    "result_schema_version",
    "result_schema_digest",
    "serializer_owner",
    "serializer_revision",
    "serializer_contract_digest",
    "schema_version",
)


def _assert_exact_slot(row: dict[str, Any], occurrence: AgentToolOccurrence) -> None:
    expected = _slot_insert_row(occurrence)
    mismatches = [field for field in _SLOT_IDENTITY_FIELDS if str(row.get(field)) != str(expected.get(field))]
    if mismatches:
        raise ValueError("agent tool result slot immutable identity collision: " + ", ".join(mismatches))
    from .control_plane_live_postgres import _json_load_dict

    if not json_contract_equal(_json_load_dict(row.get("canonical_args_json")), occurrence.canonical_args):
        raise ValueError("agent tool result slot canonical args collision")


def _attempt_insert_row(
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    attempted_slot_generation: int,
    disposition: str,
    quarantine_reason: str,
) -> dict[str, Any]:
    return {
        "result_attempt_id": terminal.result_attempt_id,
        "result_slot_id": occurrence.result_slot_id,
        "result_link_policy": occurrence.result_link_policy,
        "attempted_slot_generation": attempted_slot_generation,
        "disposition": disposition,
        "quarantine_reason": quarantine_reason,
        "provider_call_id": terminal.provider_call_id,
        "tool_call_id": terminal.tool_call_id,
        "action_id": terminal.action_id,
        "operation_run_id": terminal.operation_run_id,
        "workflow_command_id": terminal.workflow_command_id,
        "activity_run_id": terminal.activity_run_id,
        "activity_attempt_id": terminal.activity_attempt_id,
        "command_attempt": terminal.command_attempt,
        "command_generation": terminal.command_generation,
        "control_epoch": terminal.control_epoch,
        "owner_target_kind": terminal.owner_target_kind,
        "owner_target_id": terminal.owner_target_id,
        "owner_target_revision": terminal.owner_target_revision,
        "owner_target_generation": terminal.owner_target_generation,
        "owner_target_revision_token": terminal.owner_target_revision_token,
        "terminal_winner_id": terminal.terminal_winner_id,
        "owner_result_ref_json": terminal.owner_result_ref_json,
        "owner_result_digest": terminal.owner_result_digest,
        "serialized_result_json": terminal.serialized_result_json,
        "serialized_result_digest": terminal.serialized_result_digest,
        "tool_result_message_json": _json_dump(terminal.tool_result_message_record()),
        "tool_result_message_digest": terminal.tool_result_message_digest,
        "is_error": terminal.is_error,
        "schema_version": terminal.attempt_schema_version,
    }


_ATTEMPT_FIELDS = (
    "result_attempt_id",
    "result_slot_id",
    "result_link_policy",
    "attempted_slot_generation",
    "disposition",
    "quarantine_reason",
    "provider_call_id",
    "tool_call_id",
    "action_id",
    "operation_run_id",
    "workflow_command_id",
    "activity_run_id",
    "activity_attempt_id",
    "command_attempt",
    "command_generation",
    "control_epoch",
    "owner_target_kind",
    "owner_target_id",
    "owner_target_revision",
    "owner_target_generation",
    "owner_target_revision_token",
    "terminal_winner_id",
    "owner_result_digest",
    "serialized_result_json",
    "serialized_result_digest",
    "tool_result_message_digest",
    "is_error",
    "schema_version",
)


def _assert_exact_attempt(row: dict[str, Any], expected: dict[str, Any]) -> None:
    mismatches = [field for field in _ATTEMPT_FIELDS if str(row.get(field)) != str(expected.get(field))]
    if mismatches:
        raise ValueError("agent tool result attempt immutable identity collision: " + ", ".join(mismatches))
    from .control_plane_live_postgres import _json_load_dict

    for field in ("owner_result_ref_json", "tool_result_message_json"):
        if not json_contract_equal(_json_load_dict(row.get(field)), _json_load_dict(expected.get(field))):
            raise ValueError(f"agent tool result attempt {field} collision")


def _json_dump(value: Any) -> str:
    from .control_plane_live_postgres import _json_dump as live_json_dump

    return live_json_dump(value)


def _insert_dict(cursor: Any, *, table_name: str, row: dict[str, Any]) -> dict[str, Any]:
    from .control_plane_live_postgres import _fetch_one_dict_row, _quote_identifier

    columns = list(row)
    cursor.execute(
        (
            f"INSERT INTO {table_name} ({', '.join(_quote_identifier(column) for column in columns)}) "
            f"VALUES ({', '.join(['%s'] * len(columns))}) RETURNING *"
        ),
        tuple(row[column] for column in columns),
    )
    return _fetch_one_dict_row(cursor, cursor.fetchone()) or {}


def _with_retry_dependencies() -> tuple[Any, Any, Any, int, float]:
    from .control_plane_live_postgres import (
        _CONTROL_PLANE_POSTGRES_MAX_RETRIES,
        _SESSION_ADVISORY_LOCK_POLL_SECONDS,
        ControlPlaneAdvisoryLockBusy,
        _control_plane_postgres_retry_delay_seconds,
        _is_retryable_postgres_exception,
    )

    return (
        ControlPlaneAdvisoryLockBusy,
        _is_retryable_postgres_exception,
        _control_plane_postgres_retry_delay_seconds,
        _CONTROL_PLANE_POSTGRES_MAX_RETRIES,
        _SESSION_ADVISORY_LOCK_POLL_SECONDS,
    )


def reserve_agent_tool_result_slot(
    adapter: Any,
    *,
    occurrence: AgentToolOccurrence,
    lock_timeout_seconds: float = 5.0,
    fault_injection_point: str = "",
) -> dict[str, Any] | None:
    """Insert or exact-reload one pending logical occurrence."""

    from .control_plane_live_postgres import _fetch_all_dict_rows, _fetch_one_dict_row, _quote_identifier

    if not isinstance(occurrence, AgentToolOccurrence):
        raise ValueError("reserve_agent_tool_result_slot requires AgentToolOccurrence")
    occurrence = occurrence.revalidated()
    if not _runtime_dependencies(adapter, ("agent_tool_result_slots",)):
        return None
    if fault_injection_point not in {"", "after_slot_write", "after_commit"}:
        raise ValueError("unsupported agent tool result reserve fault injection point")
    timeout_seconds, deadline = _deadline_seconds(lock_timeout_seconds)
    expected = _slot_insert_row(occurrence)
    retry_attempt = 0
    last_busy_key = f"agent_tool_result_slots:id:{occurrence.result_slot_id}"
    (
        busy_error,
        is_retryable,
        retry_delay,
        max_retries,
        poll_seconds,
    ) = _with_retry_dependencies()

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
        connection = adapter._connect_with_timeout(remaining)
        try:
            with connection.cursor() as cursor:
                milliseconds = max(1, int((deadline - time.monotonic()) * 1000))
                cursor.execute("SELECT set_config('lock_timeout', %s, true)", (f"{milliseconds}ms",))
                lock_keys = sorted(
                    {
                        f"agent_tool_result_slots:id:{occurrence.result_slot_id}",
                        f"agent_tool_result_slots:occurrence:{occurrence.logical_occurrence_digest}",
                    }
                )
                for lock_key in lock_keys:
                    last_busy_key = lock_key
                    if not adapter._try_acquire_transaction_lock(cursor, lock_key):
                        raise _ResultSlotLockBusy
                cursor.execute(
                    "SELECT * FROM agent_tool_result_slots "
                    "WHERE result_slot_id = %s OR logical_occurrence_digest = %s "
                    "ORDER BY result_slot_id FOR UPDATE",
                    (occurrence.result_slot_id, occurrence.logical_occurrence_digest),
                )
                candidates = _fetch_all_dict_rows(cursor)
                if len(candidates) > 1:
                    raise ValueError("agent tool result slot split logical identity collision")
                if candidates:
                    row = candidates[0]
                    _assert_exact_slot(row, occurrence)
                    outcome = {"outcome": "replayed", "replayed": True, "slot": row}
                else:
                    columns = list(expected)
                    cursor.execute(
                        (
                            "INSERT INTO agent_tool_result_slots "
                            f"({', '.join(_quote_identifier(column) for column in columns)}) "
                            f"VALUES ({', '.join(['%s'] * len(columns))}) RETURNING *"
                        ),
                        tuple(expected[column] for column in columns),
                    )
                    row = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                    if not row:
                        raise RuntimeError("agent tool result slot insert returned no row")
                    _assert_exact_slot(row, occurrence)
                    if fault_injection_point == "after_slot_write":
                        raise RuntimeError("injected agent tool result reserve fault after slot write")
                    outcome = {"outcome": "reserved", "replayed": False, "slot": row}
            connection.commit()
            if fault_injection_point == "after_commit":
                raise RuntimeError("injected agent tool result reserve fault after commit")
            return outcome
        except _ResultSlotLockBusy:
            connection.rollback()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
            time.sleep(min(poll_seconds, remaining))
        except Exception as exc:
            connection.rollback()
            retry_attempt += 1
            if is_retryable(exc) and retry_attempt < max_retries and deadline > time.monotonic():
                time.sleep(min(retry_delay(retry_attempt), deadline - time.monotonic()))
            else:
                raise
        finally:
            connection.close()


def _assert_plan_owner(
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    action: dict[str, Any],
    operation: dict[str, Any],
    preview: dict[str, Any],
    event: dict[str, Any],
) -> None:
    from .acquisition_plan_preview import (
        AcquisitionPlanPreview,
        acquisition_plan_preview_success_result,
        serialize_acquisition_plan_preview_result,
    )
    from .control_plane_live_postgres import _json_load_dict

    expected_pins = {
        "tool_name": occurrence.tool_name,
        "tool_spec_version": occurrence.tool_spec_version,
        "tool_spec_digest": occurrence.tool_spec_digest,
        "request_schema_version": occurrence.request_schema_version,
        "request_schema_digest": occurrence.request_schema_digest,
        "result_schema_version": occurrence.result_schema_version,
        "result_schema_digest": occurrence.result_schema_digest,
        "result_serializer_owner": occurrence.serializer_owner,
        "result_serializer_revision": occurrence.serializer_revision,
        "result_serializer_contract_digest": occurrence.serializer_contract_digest,
    }
    action_expected = {
        "action_id": terminal.action_id,
        "workspace_id": occurrence.workspace_id,
        "action_type": "plan_acquisition",
        "status": "completed",
        **expected_pins,
    }
    operation_expected = {
        "operation_run_id": terminal.operation_run_id,
        "workspace_id": occurrence.workspace_id,
        "action_id": terminal.action_id,
        "status": "completed",
        **expected_pins,
    }
    for label, row, expected in (
        ("action", action, action_expected),
        ("operation", operation, operation_expected),
    ):
        mismatches = [field for field, value in expected.items() if str(row.get(field)) != str(value)]
        if mismatches:
            raise ValueError(f"agent tool plan result {label} exact-owner mismatch: " + ", ".join(mismatches))

    preview_expected = {
        "preview_id": terminal.owner_target_id,
        "workspace_id": occurrence.workspace_id,
        "action_id": terminal.action_id,
        "operation_run_id": terminal.operation_run_id,
        "preview_revision": terminal.owner_target_revision,
        "preview_digest": terminal.owner_result_digest,
    }
    mismatches = [field for field, value in preview_expected.items() if str(preview.get(field)) != str(value)]
    if terminal.owner_target_generation != 0:
        mismatches.append("owner_target_generation")
    if terminal.owner_target_revision_token:
        mismatches.append("owner_target_revision_token")
    if mismatches:
        raise ValueError("agent tool plan result preview exact-owner mismatch: " + ", ".join(mismatches))
    event_expected = {
        "event_id": terminal.terminal_winner_id,
        "workspace_id": occurrence.workspace_id,
        "operation_run_id": terminal.operation_run_id,
        "action_id": terminal.action_id,
        "event_type": "AcquisitionPlanPreviewCreated",
    }
    mismatches = [field for field, value in event_expected.items() if str(event.get(field)) != str(value)]
    if mismatches:
        raise ValueError("agent tool plan result terminal winner mismatch: " + ", ".join(mismatches))

    action_result_ref = _json_load_dict(action.get("result_ref_json"))
    operation_result_ref = _json_load_dict(operation.get("result_ref_json"))
    if not json_contract_equal(action_result_ref, terminal.owner_result_ref) or not json_contract_equal(
        operation_result_ref,
        terminal.owner_result_ref,
    ):
        raise ValueError("agent tool plan result owner result ref mismatch")
    owner_preview = AcquisitionPlanPreview(_json_load_dict(preview.get("preview_json")))
    expected_serialized = serialize_acquisition_plan_preview_result(
        acquisition_plan_preview_success_result(owner_preview)
    )
    if terminal.serialized_result_json != expected_serialized or terminal.is_error:
        raise ValueError("agent tool plan result serializer output mismatch")


def _load_plan_base_owner(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
) -> dict[str, dict[str, Any]]:
    del occurrence
    from .control_plane_live_postgres import _fetch_one_dict_row

    cursor.execute(
        "SELECT * FROM operation_runs WHERE operation_run_id = %s FOR UPDATE",
        (terminal.operation_run_id,),
    )
    operation = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
    cursor.execute("SELECT * FROM agent_actions WHERE action_id = %s FOR UPDATE", (terminal.action_id,))
    action = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
    return {"action": action, "operation_run": operation}


def _assert_locked_plan_owner(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    base_owner: dict[str, dict[str, Any]],
) -> None:
    from .control_plane_live_postgres import _fetch_one_dict_row

    cursor.execute(
        "SELECT * FROM acquisition_plan_previews WHERE preview_id = %s FOR UPDATE",
        (terminal.owner_target_id,),
    )
    preview = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
    cursor.execute(
        "SELECT * FROM operation_events WHERE event_id = %s FOR UPDATE",
        (terminal.terminal_winner_id,),
    )
    event = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
    action = dict(base_owner.get("action") or {})
    operation = dict(base_owner.get("operation_run") or {})
    if not all((operation, action, preview, event)):
        raise ValueError("agent tool plan result exact owner not found")
    _assert_plan_owner(
        occurrence=occurrence,
        terminal=terminal,
        action=action,
        operation=operation,
        preview=preview,
        event=event,
    )


def _journal_insert_row(
    *, occurrence: AgentToolOccurrence, terminal: AgentToolTerminalResult, journal_id: str
) -> dict[str, Any]:
    return {
        "journal_id": journal_id,
        "result_slot_id": occurrence.result_slot_id,
        "result_attempt_id": terminal.result_attempt_id,
        "workspace_id": occurrence.workspace_id,
        "actor_id": occurrence.actor_id,
        "runtime_namespace": occurrence.runtime_namespace,
        "provider_mode": occurrence.provider_mode,
        "turn_id": occurrence.turn_id,
        "step_id": occurrence.step_id,
        "tool_name": occurrence.tool_name,
        "tool_spec_version": occurrence.tool_spec_version,
        "tool_spec_digest": occurrence.tool_spec_digest,
        "result_link_policy": occurrence.result_link_policy,
        "canonical_args_digest": occurrence.canonical_args_digest,
        "occurrence_ordinal": occurrence.occurrence_ordinal,
        "request_schema_version": occurrence.request_schema_version,
        "request_schema_digest": occurrence.request_schema_digest,
        "result_schema_version": occurrence.result_schema_version,
        "result_schema_digest": occurrence.result_schema_digest,
        "serializer_owner": occurrence.serializer_owner,
        "serializer_revision": occurrence.serializer_revision,
        "serializer_contract_digest": occurrence.serializer_contract_digest,
        "action_id": terminal.action_id,
        "operation_run_id": terminal.operation_run_id,
        "workflow_command_id": terminal.workflow_command_id,
        "activity_run_id": terminal.activity_run_id,
        "activity_attempt_id": terminal.activity_attempt_id,
        "command_attempt": terminal.command_attempt,
        "command_generation": terminal.command_generation,
        "control_epoch": terminal.control_epoch,
        "owner_target_kind": terminal.owner_target_kind,
        "owner_target_id": terminal.owner_target_id,
        "owner_target_revision": terminal.owner_target_revision,
        "owner_target_generation": terminal.owner_target_generation,
        "owner_target_revision_token": terminal.owner_target_revision_token,
        "terminal_winner_id": terminal.terminal_winner_id,
        "owner_result_ref_json": terminal.owner_result_ref_json,
        "owner_result_digest": terminal.owner_result_digest,
        "serialized_result_json": terminal.serialized_result_json,
        "serialized_result_digest": terminal.serialized_result_digest,
        "tool_result_message_json": _json_dump(terminal.tool_result_message_record()),
        "tool_result_message_digest": terminal.tool_result_message_digest,
        "is_error": terminal.is_error,
        "schema_version": terminal.journal_schema_version,
    }


def _slot_terminal_update(terminal: AgentToolTerminalResult) -> dict[str, Any]:
    return {
        "status": "accepted",
        "result_attempt_id": terminal.result_attempt_id,
        "provider_call_id": terminal.provider_call_id,
        "tool_call_id": terminal.tool_call_id,
        "action_id": terminal.action_id,
        "operation_run_id": terminal.operation_run_id,
        "workflow_command_id": terminal.workflow_command_id,
        "activity_run_id": terminal.activity_run_id,
        "activity_attempt_id": terminal.activity_attempt_id,
        "command_attempt": terminal.command_attempt,
        "command_generation": terminal.command_generation,
        "control_epoch": terminal.control_epoch,
        "owner_target_kind": terminal.owner_target_kind,
        "owner_target_id": terminal.owner_target_id,
        "owner_target_revision": terminal.owner_target_revision,
        "owner_target_generation": terminal.owner_target_generation,
        "owner_target_revision_token": terminal.owner_target_revision_token,
        "terminal_winner_id": terminal.terminal_winner_id,
        "owner_result_ref_json": terminal.owner_result_ref_json,
        "owner_result_digest": terminal.owner_result_digest,
        "serialized_result_json": terminal.serialized_result_json,
        "serialized_result_digest": terminal.serialized_result_digest,
        "tool_result_message_json": _json_dump(terminal.tool_result_message_record()),
        "tool_result_message_digest": terminal.tool_result_message_digest,
        "is_error": terminal.is_error,
    }


def _assert_exact_terminal_slot(row: dict[str, Any], terminal: AgentToolTerminalResult) -> None:
    from .control_plane_live_postgres import _json_load_dict

    expected = _slot_terminal_update(terminal)
    json_fields = {"owner_result_ref_json", "tool_result_message_json"}
    mismatches = [
        field for field, value in expected.items() if field not in json_fields and str(row.get(field)) != str(value)
    ]
    if mismatches:
        raise ValueError("agent tool result accepted slot terminal collision: " + ", ".join(mismatches))
    for field in json_fields:
        if not json_contract_equal(_json_load_dict(row.get(field)), _json_load_dict(expected.get(field))):
            raise ValueError(f"agent tool result accepted slot {field} collision")


def _assert_exact_journal(
    row: dict[str, Any],
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    journal_id: str,
) -> None:
    from .control_plane_live_postgres import _json_load_dict

    expected = _journal_insert_row(occurrence=occurrence, terminal=terminal, journal_id=journal_id)
    json_fields = {"owner_result_ref_json", "tool_result_message_json"}
    mismatches = [
        field for field, value in expected.items() if field not in json_fields and str(row.get(field)) != str(value)
    ]
    if mismatches:
        raise ValueError("agent tool result journal immutable identity collision: " + ", ".join(mismatches))
    for field in json_fields:
        if not json_contract_equal(_json_load_dict(row.get(field)), _json_load_dict(expected.get(field))):
            raise ValueError(f"agent tool result journal {field} collision")


def _accept_exact_agent_tool_result_uow(
    adapter: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    attempted_slot_generation: int,
    required_tables: tuple[str, ...],
    lock_groups: tuple[tuple[str, ...], ...],
    load_base_owner: Any,
    assert_locked_owner: Any,
    lock_timeout_seconds: float = 5.0,
    fault_injection_point: str = "",
) -> dict[str, Any] | None:
    """Shared pending-to-accepted state machine around one physical owner."""

    from .control_plane_live_postgres import _fetch_one_dict_row, _quote_identifier

    if not isinstance(occurrence, AgentToolOccurrence) or not isinstance(terminal, AgentToolTerminalResult):
        raise ValueError("accept tool result requires exact occurrence and terminal result")
    occurrence = occurrence.revalidated()
    terminal = terminal.revalidated()
    terminal.validate_for_occurrence(occurrence)
    if type(attempted_slot_generation) is not int or attempted_slot_generation <= 0:
        raise ValueError("agent tool result attempted slot generation must be positive")
    if not _runtime_dependencies(adapter, required_tables):
        return None
    allowed_faults = {"", "after_attempt_write", "after_slot_write", "after_journal_write", "after_commit"}
    if fault_injection_point not in allowed_faults:
        raise ValueError("unsupported agent tool result acceptance fault injection point")
    timeout_seconds, deadline = _deadline_seconds(lock_timeout_seconds)
    retry_attempt = 0
    last_busy_key = f"operation_events:{terminal.operation_run_id}"
    journal_id = (
        "tooljournal_"
        + sha1(f"{occurrence.result_slot_id}:{terminal.result_attempt_id}".encode("utf-8")).hexdigest()[:24]
    )
    (
        busy_error,
        is_retryable,
        retry_delay,
        max_retries,
        poll_seconds,
    ) = _with_retry_dependencies()

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
        connection = adapter._connect_with_timeout(remaining)
        try:
            with connection.cursor() as cursor:
                milliseconds = max(1, int((deadline - time.monotonic()) * 1000))
                cursor.execute("SELECT set_config('lock_timeout', %s, true)", (f"{milliseconds}ms",))
                for lock_group in lock_groups:
                    for lock_key in lock_group:
                        last_busy_key = lock_key
                        if not adapter._try_acquire_transaction_lock(cursor, lock_key):
                            raise _ResultSlotLockBusy

                base_owner = load_base_owner(
                    cursor,
                    occurrence=occurrence,
                    terminal=terminal,
                )
                cursor.execute(
                    "SELECT * FROM agent_tool_result_slots WHERE result_slot_id = %s FOR UPDATE",
                    (occurrence.result_slot_id,),
                )
                slot = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                if not slot:
                    raise ValueError("agent tool result slot not found")
                _assert_exact_slot(slot, occurrence)

                cursor.execute(
                    "SELECT * FROM agent_tool_result_attempts WHERE result_attempt_id = %s",
                    (terminal.result_attempt_id,),
                )
                existing_attempt = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                cursor.execute(
                    "SELECT * FROM agent_tool_result_journal WHERE result_slot_id = %s",
                    (occurrence.result_slot_id,),
                )
                existing_journal = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}

                owner_asserted_for_write = False

                def assert_owner_before_write() -> None:
                    nonlocal owner_asserted_for_write
                    if owner_asserted_for_write:
                        return
                    assert_locked_owner(
                        cursor,
                        occurrence=occurrence,
                        terminal=terminal,
                        base_owner=base_owner,
                    )
                    owner_asserted_for_write = True

                if str(slot.get("status") or "") == "accepted":
                    if terminal.result_attempt_id == str(slot.get("result_attempt_id") or ""):
                        expected_attempt = _attempt_insert_row(
                            occurrence=occurrence,
                            terminal=terminal,
                            attempted_slot_generation=attempted_slot_generation,
                            disposition="accepted",
                            quarantine_reason="",
                        )
                        if not existing_attempt or not existing_journal:
                            raise ValueError("agent tool result accepted aggregate is partial")
                        _assert_exact_terminal_slot(slot, terminal)
                        _assert_exact_attempt(existing_attempt, expected_attempt)
                        _assert_exact_journal(
                            existing_journal,
                            occurrence=occurrence,
                            terminal=terminal,
                            journal_id=journal_id,
                        )
                        outcome = {
                            "outcome": "replayed",
                            "replayed": True,
                            "slot": slot,
                            "attempt": existing_attempt,
                            "journal": existing_journal,
                        }
                    else:
                        expected_attempt = _attempt_insert_row(
                            occurrence=occurrence,
                            terminal=terminal,
                            attempted_slot_generation=attempted_slot_generation,
                            disposition="quarantined",
                            quarantine_reason="terminal_winner_already_accepted",
                        )
                        if existing_attempt:
                            _assert_exact_attempt(existing_attempt, expected_attempt)
                            quarantined_attempt = existing_attempt
                            replayed = True
                        else:
                            assert_owner_before_write()
                            quarantined_attempt = _insert_dict(
                                cursor,
                                table_name="agent_tool_result_attempts",
                                row=expected_attempt,
                            )
                            replayed = False
                        outcome = {
                            "outcome": "quarantined",
                            "replayed": replayed,
                            "slot": slot,
                            "attempt": quarantined_attempt,
                            "journal": existing_journal,
                        }
                elif str(slot.get("status") or "") != "pending":
                    raise ValueError("agent tool result slot state invalid")
                elif attempted_slot_generation != occurrence.slot_generation:
                    expected_attempt = _attempt_insert_row(
                        occurrence=occurrence,
                        terminal=terminal,
                        attempted_slot_generation=attempted_slot_generation,
                        disposition="quarantined",
                        quarantine_reason="slot_generation_mismatch",
                    )
                    if existing_attempt:
                        _assert_exact_attempt(existing_attempt, expected_attempt)
                        quarantined_attempt = existing_attempt
                        replayed = True
                    else:
                        assert_owner_before_write()
                        quarantined_attempt = _insert_dict(
                            cursor,
                            table_name="agent_tool_result_attempts",
                            row=expected_attempt,
                        )
                        replayed = False
                    outcome = {
                        "outcome": "quarantined",
                        "replayed": replayed,
                        "slot": slot,
                        "attempt": quarantined_attempt,
                        "journal": {},
                    }
                else:
                    if existing_attempt or existing_journal:
                        raise ValueError("agent tool result pending aggregate has partial terminal rows")
                    assert_owner_before_write()
                    accepted_attempt_row = _attempt_insert_row(
                        occurrence=occurrence,
                        terminal=terminal,
                        attempted_slot_generation=attempted_slot_generation,
                        disposition="accepted",
                        quarantine_reason="",
                    )
                    accepted_attempt = _insert_dict(
                        cursor,
                        table_name="agent_tool_result_attempts",
                        row=accepted_attempt_row,
                    )
                    if fault_injection_point == "after_attempt_write":
                        raise RuntimeError("injected agent tool result fault after attempt write")

                    updates = _slot_terminal_update(terminal)
                    assignments = ", ".join(f"{_quote_identifier(column)} = %s" for column in updates)
                    cursor.execute(
                        (
                            f"UPDATE agent_tool_result_slots SET {assignments}, accepted_at = transaction_timestamp() "
                            "WHERE result_slot_id = %s AND slot_generation = %s AND status = 'pending' RETURNING *"
                        ),
                        (*tuple(updates.values()), occurrence.result_slot_id, occurrence.slot_generation),
                    )
                    accepted_slot = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                    if not accepted_slot:
                        raise ValueError("agent tool result terminal CAS lost")
                    if fault_injection_point == "after_slot_write":
                        raise RuntimeError("injected agent tool result fault after slot write")

                    journal_row = _journal_insert_row(
                        occurrence=occurrence,
                        terminal=terminal,
                        journal_id=journal_id,
                    )
                    journal_columns = list(journal_row)
                    cursor.execute(
                        (
                            "INSERT INTO agent_tool_result_journal "
                            f"({', '.join(_quote_identifier(column) for column in journal_columns)}, accepted_at) "
                            f"VALUES ({', '.join(['%s'] * len(journal_columns))}, transaction_timestamp()) "
                            "RETURNING *"
                        ),
                        tuple(journal_row[column] for column in journal_columns),
                    )
                    journal = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                    if not journal:
                        raise RuntimeError("agent tool result journal insert returned no row")
                    if fault_injection_point == "after_journal_write":
                        raise RuntimeError("injected agent tool result fault after journal write")
                    outcome = {
                        "outcome": "accepted",
                        "replayed": False,
                        "slot": accepted_slot,
                        "attempt": accepted_attempt,
                        "journal": journal,
                    }
            connection.commit()
            if fault_injection_point == "after_commit":
                raise RuntimeError("injected agent tool result acceptance fault after commit")
            return outcome
        except _ResultSlotLockBusy:
            connection.rollback()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
            time.sleep(min(poll_seconds, remaining))
        except Exception as exc:
            connection.rollback()
            retry_attempt += 1
            if is_retryable(exc) and retry_attempt < max_retries and deadline > time.monotonic():
                time.sleep(min(retry_delay(retry_attempt), deadline - time.monotonic()))
            else:
                raise
        finally:
            connection.close()


def accept_acquisition_plan_tool_result_uow(
    adapter: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    attempted_slot_generation: int,
    lock_timeout_seconds: float = 5.0,
    fault_injection_point: str = "",
) -> dict[str, Any] | None:
    """Accept one exact plan-preview result or append a late-attempt quarantine."""

    if not isinstance(occurrence, AgentToolOccurrence) or not isinstance(terminal, AgentToolTerminalResult):
        raise ValueError("accept plan result requires exact occurrence and terminal result")
    if (
        occurrence.tool_name != "plan_acquisition"
        or occurrence.effect_class != "commandless_action"
        or terminal.owner_target_kind != ACQUISITION_PLAN_PREVIEW_OWNER_TARGET_KIND
    ):
        raise ValueError("accept plan result requires plan_acquisition preview owner")
    return _accept_exact_agent_tool_result_uow(
        adapter,
        occurrence=occurrence,
        terminal=terminal,
        attempted_slot_generation=attempted_slot_generation,
        required_tables=(
            "operation_events",
            "operation_runs",
            "agent_actions",
            "agent_tool_result_slots",
            "acquisition_plan_previews",
            "agent_tool_result_attempts",
            "agent_tool_result_journal",
        ),
        lock_groups=(
            (f"operation_events:{terminal.operation_run_id}",),
            (f"operation_runs:id:{terminal.operation_run_id}",),
            (f"agent_actions:id:{terminal.action_id}",),
            (f"agent_tool_result_slots:id:{occurrence.result_slot_id}",),
            (f"acquisition_plan_previews:id:{terminal.owner_target_id}",),
        ),
        load_base_owner=_load_plan_base_owner,
        assert_locked_owner=_assert_locked_plan_owner,
        lock_timeout_seconds=lock_timeout_seconds,
        fault_injection_point=fault_injection_point,
    )


def prepare_inspect_operation_tool_result(
    adapter: Any,
    *,
    occurrence: AgentToolOccurrence,
    result_attempt_id: str,
    provider_call_id: str,
    tool_call_id: str,
    action_id: str,
    operation_run_id: str,
    lock_timeout_seconds: float = 5.0,
) -> AgentToolTerminalResult | None:
    """Read and serialize one exact Operation snapshot without result-slot writes."""

    from .agent_operation_query_postgres import (
        inspect_operation_result_lock_groups,
        load_inspect_operation_base_owner,
        terminal_from_locked_inspect_operation_owner,
        validate_inspect_operation_occurrence,
    )

    if not isinstance(occurrence, AgentToolOccurrence):
        raise ValueError("prepare inspect result requires exact occurrence")
    occurrence = occurrence.revalidated()
    validate_inspect_operation_occurrence(
        occurrence,
        action_id=action_id,
        operation_run_id=operation_run_id,
    )
    required_tables = (
        "operation_events",
        "operation_runs",
        "agent_actions",
        "workflow_commands",
    )
    if not _read_dependencies(adapter, required_tables):
        return None
    timeout_seconds, deadline = _deadline_seconds(lock_timeout_seconds)
    retry_attempt = 0
    last_busy_key = f"operation_events:{operation_run_id}"
    lock_groups = inspect_operation_result_lock_groups(
        occurrence=occurrence,
        action_id=action_id,
        operation_run_id=operation_run_id,
        include_result_slot=False,
    )
    (
        busy_error,
        is_retryable,
        retry_delay,
        max_retries,
        poll_seconds,
    ) = _with_retry_dependencies()

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
        connection = adapter._connect_with_timeout(remaining)
        try:
            with connection.cursor() as cursor:
                milliseconds = max(1, int((deadline - time.monotonic()) * 1000))
                cursor.execute("SELECT set_config('lock_timeout', %s, true)", (f"{milliseconds}ms",))
                for lock_group in lock_groups:
                    for lock_key in lock_group:
                        last_busy_key = lock_key
                        if not adapter._try_acquire_transaction_lock(cursor, lock_key):
                            raise _ResultSlotLockBusy
                base_owner = load_inspect_operation_base_owner(
                    cursor,
                    action_id=action_id,
                    operation_run_id=operation_run_id,
                )
                terminal = terminal_from_locked_inspect_operation_owner(
                    cursor,
                    occurrence=occurrence,
                    result_attempt_id=result_attempt_id,
                    provider_call_id=provider_call_id,
                    tool_call_id=tool_call_id,
                    action_id=action_id,
                    operation_run_id=operation_run_id,
                    base_owner=base_owner,
                )
            connection.commit()
            return terminal
        except _ResultSlotLockBusy:
            connection.rollback()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
            time.sleep(min(poll_seconds, remaining))
        except Exception as exc:
            connection.rollback()
            retry_attempt += 1
            if is_retryable(exc) and retry_attempt < max_retries and deadline > time.monotonic():
                time.sleep(min(retry_delay(retry_attempt), deadline - time.monotonic()))
            else:
                raise
        finally:
            connection.close()


def accept_inspect_operation_tool_result_uow(
    adapter: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    attempted_slot_generation: int,
    lock_timeout_seconds: float = 5.0,
    fault_injection_point: str = "",
) -> dict[str, Any] | None:
    """Accept one revision-bound, read-only Operation query result."""

    from .agent_operation_query_postgres import (
        INSPECT_OPERATION_OWNER_TARGET_KIND,
        assert_exact_inspect_operation_terminal,
        inspect_operation_result_lock_groups,
        load_inspect_operation_base_owner,
        validate_inspect_operation_occurrence,
    )

    if not isinstance(occurrence, AgentToolOccurrence) or not isinstance(terminal, AgentToolTerminalResult):
        raise ValueError("accept inspect result requires exact occurrence and terminal result")
    validate_inspect_operation_occurrence(
        occurrence,
        action_id=terminal.action_id,
        operation_run_id=terminal.operation_run_id,
    )
    if (
        terminal.owner_target_kind != INSPECT_OPERATION_OWNER_TARGET_KIND
        or terminal.owner_target_id != terminal.operation_run_id
        or terminal.is_error
    ):
        raise ValueError("accept inspect result requires exact Operation event owner")

    def load_owner(
        cursor: Any,
        *,
        occurrence: AgentToolOccurrence,
        terminal: AgentToolTerminalResult,
    ) -> dict[str, dict[str, Any]]:
        del occurrence
        return load_inspect_operation_base_owner(
            cursor,
            action_id=terminal.action_id,
            operation_run_id=terminal.operation_run_id,
        )

    return _accept_exact_agent_tool_result_uow(
        adapter,
        occurrence=occurrence,
        terminal=terminal,
        attempted_slot_generation=attempted_slot_generation,
        required_tables=(
            "operation_events",
            "operation_runs",
            "agent_actions",
            "workflow_commands",
            "agent_tool_result_slots",
            "agent_tool_result_attempts",
            "agent_tool_result_journal",
        ),
        lock_groups=inspect_operation_result_lock_groups(
            occurrence=occurrence,
            action_id=terminal.action_id,
            operation_run_id=terminal.operation_run_id,
            include_result_slot=True,
        ),
        load_base_owner=load_owner,
        assert_locked_owner=assert_exact_inspect_operation_terminal,
        lock_timeout_seconds=lock_timeout_seconds,
        fault_injection_point=fault_injection_point,
    )


__all__ = [
    "ACQUISITION_PLAN_PREVIEW_OWNER_TARGET_KIND",
    "accept_acquisition_plan_tool_result_uow",
    "accept_inspect_operation_tool_result_uow",
    "prepare_inspect_operation_tool_result",
    "reserve_agent_tool_result_slot",
]
