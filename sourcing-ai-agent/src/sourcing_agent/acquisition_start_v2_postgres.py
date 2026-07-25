"""PostgreSQL authority UoWs for the bounded acquisition-start v2 adapter.

This module begins with the pending-Action submit transaction.  It deliberately
does not bootstrap schemas, create an OperationRun, approve an Action, enqueue a
command, accept a tool result, or call a provider/model/network transport.  The
caller must already have reserved the exact Agent result slot from an initial
read-only owner bind.
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any

from .acquisition_start_v2 import (
    ACQUISITION_START_ACTION_TYPE,
    ACQUISITION_START_V2_REQUEST_INVALID,
    AcquisitionStartV2BindContext,
    AcquisitionStartV2BoundRequest,
    AcquisitionStartV2Error,
    AcquisitionStartV2OwnerBinder,
    AcquisitionStartV2ToolPins,
)
from .agent_tool_result_slot import AgentToolOccurrence, AgentToolResultSlotError
from .operation_runtime import operation_action_id

_ALLOWED_RUNTIME_NAMESPACE = "isolated_local_canary"
_ALLOWED_PROVIDER_MODES = frozenset({"simulate", "scripted"})
_ACTION_OWNER_MODULE = "acquisition_run_writer"
_ACTION_OPERATION_TYPE = "acquisition_run"
_APPROVAL_REQUIRED_EVENT_TYPE = "ActionApprovalRequired"
_APPROVAL_REQUIRED_SCHEMA_VERSION = "acquisition_start_approval_required.v1"
_APPROVAL_REQUIRED_EVENT_SOURCE = "agent_start_v2_submit_uow"
_BUDGET_FIELDS = (
    "max_provider_calls",
    "max_provider_items",
    "max_output_candidates",
    "max_cost_micro_usd",
    "max_elapsed_seconds",
)


class _StartSubmitLockBusy(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class AcquisitionStartV2SubmissionBinding:
    """Server-revalidated identity used by every submit lock and row."""

    occurrence: AgentToolOccurrence
    bound_request: AcquisitionStartV2BoundRequest
    start_idempotency: str
    action_id: str
    preview_id: str
    preview_revision: int
    preview_digest: str

    @property
    def result_occurrence_ref(self) -> dict[str, Any]:
        return {
            "result_slot_id": self.occurrence.result_slot_id,
            "slot_generation": self.occurrence.slot_generation,
            "logical_occurrence_digest": self.occurrence.logical_occurrence_digest,
        }

    @property
    def budget(self) -> dict[str, Any]:
        preview = self.bound_request.snapshot.preview.to_record()
        raw_budget = dict(preview.get("effective_request") or {}).get("budget")
        if not isinstance(raw_budget, Mapping) or set(raw_budget) != set(_BUDGET_FIELDS):
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, "budget")
        return {field: raw_budget[field] for field in _BUDGET_FIELDS}


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID) from exc


def revalidate_acquisition_start_v2_occurrence(
    occurrence: AgentToolOccurrence,
) -> AcquisitionStartV2SubmissionBinding:
    """Fail closed on mode, historical pins, and the exact full bound root.

    This function performs no adapter access.  In particular, it runs before a
    connection or schema/bootstrap path can be reached.
    """

    if not isinstance(occurrence, AgentToolOccurrence):
        raise ValueError("acquisition start submit requires AgentToolOccurrence")

    from .agent_canary_registry import LOCAL_CANARY_AGENT_TOOL_REGISTRY, START_ACQUISITION_RUN_TOOL_SPEC

    exact = occurrence.revalidated_for_registry(LOCAL_CANARY_AGENT_TOOL_REGISTRY)
    if (
        exact.runtime_namespace != _ALLOWED_RUNTIME_NAMESPACE
        or exact.provider_mode not in _ALLOWED_PROVIDER_MODES
    ):
        raise AgentToolResultSlotError("acquisition_start_v2_runtime_mode_not_allowed")
    if (
        exact.tool_name != START_ACQUISITION_RUN_TOOL_SPEC.tool_name
        or exact.tool_spec_version != START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version
        or exact.tool_spec_digest != START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_digest
    ):
        raise AgentToolResultSlotError("acquisition_start_v2_current_tool_pin_mismatch")

    bound_request = AcquisitionStartV2BoundRequest(exact.canonical_args)
    if _canonical_json_bytes(bound_request.to_record()) != exact.canonical_args_json.encode("utf-8"):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, "canonical_args")
    if (
        str(bound_request.target_ref.get("workspace_id") or "") != exact.workspace_id
        or str(bound_request.target_ref.get("requester_id") or "") != exact.actor_id
        or bound_request.snapshot.tool_pins.to_record()
        != {
            "tool_spec_version": exact.tool_spec_version,
            "tool_spec_digest": exact.tool_spec_digest,
        }
    ):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, "occurrence_owner")

    preview_ref = dict(bound_request.input_payload)
    preview_id = str(preview_ref.get("preview_id") or "")
    preview_revision = preview_ref.get("preview_revision")
    preview_digest = str(preview_ref.get("preview_digest") or "")
    if type(preview_revision) is not int:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, "preview_revision")
    start_idempotency = f"agent-start-v2:{exact.logical_occurrence_digest}"
    return AcquisitionStartV2SubmissionBinding(
        occurrence=exact,
        bound_request=bound_request,
        start_idempotency=start_idempotency,
        action_id=operation_action_id(
            workspace_id=exact.workspace_id,
            action_type=ACQUISITION_START_ACTION_TYPE,
            idempotency_key=start_idempotency,
        ),
        preview_id=preview_id,
        preview_revision=preview_revision,
        preview_digest=preview_digest,
    )


def acquisition_start_v2_operation_event_id(
    *,
    event_stream_id: str,
    sequence_number: int,
    idempotency_key: str,
) -> str:
    seed = f"{event_stream_id}:{sequence_number}:{idempotency_key}"
    return "opevt_" + sha1(seed.encode("utf-8")).hexdigest()[:24]


def acquisition_start_v2_submit_lock_groups(
    binding: AcquisitionStartV2SubmissionBinding,
) -> tuple[tuple[str, ...], ...]:
    """Return applicable S1e1 lock groups in physical order."""

    if not isinstance(binding, AcquisitionStartV2SubmissionBinding):
        raise ValueError("acquisition start submit lock binding is required")

    def _sorted(*keys: str) -> tuple[str, ...]:
        return tuple(sorted(set(keys), key=lambda value: value.encode("utf-8")))

    occurrence = binding.occurrence
    return (
        _sorted(f"operation_events:{binding.action_id}"),
        _sorted(
            f"agent_actions:id:{binding.action_id}",
            f"agent_actions:idempotency:{occurrence.workspace_id}:{binding.start_idempotency}",
        ),
        _sorted(
            f"agent_tool_result_slots:id:{occurrence.result_slot_id}",
            f"agent_tool_result_slots:occurrence:{occurrence.logical_occurrence_digest}",
        ),
        _sorted(
            f"acquisition_plan_previews:id:{binding.preview_id}",
            f"acquisition_plan_previews:revision:{binding.preview_revision}",
        ),
    )


def _approval_required_event_key(binding: AcquisitionStartV2SubmissionBinding) -> str:
    return f"{binding.start_idempotency}:{_APPROVAL_REQUIRED_EVENT_TYPE}"


def _approval_required_payload(binding: AcquisitionStartV2SubmissionBinding) -> dict[str, Any]:
    occurrence = binding.occurrence
    return {
        "schema_version": _APPROVAL_REQUIRED_SCHEMA_VERSION,
        "action_id": binding.action_id,
        "workspace_id": occurrence.workspace_id,
        "requester_id": occurrence.actor_id,
        "action_type": ACQUISITION_START_ACTION_TYPE,
        "request": binding.bound_request.to_record(),
        "request_schema_ref": {
            "schema_version": occurrence.request_schema_version,
            "schema_digest": occurrence.request_schema_digest,
        },
        "tool_spec_ref": {
            "tool_spec_version": occurrence.tool_spec_version,
            "tool_spec_digest": occurrence.tool_spec_digest,
        },
        "result_contract_ref": {
            "result_schema_version": occurrence.result_schema_version,
            "result_schema_digest": occurrence.result_schema_digest,
            "serializer_owner": occurrence.serializer_owner,
            "serializer_revision": occurrence.serializer_revision,
            "serializer_contract_digest": occurrence.serializer_contract_digest,
        },
        "result_occurrence_ref": binding.result_occurrence_ref,
        "start_snapshot_digest": binding.bound_request.snapshot.snapshot_digest,
    }


def _canonical_submit_rows(
    binding: AcquisitionStartV2SubmissionBinding,
    *,
    submitted_at: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    from .control_plane_live_postgres import _json_dump

    occurrence = binding.occurrence
    event_key = _approval_required_event_key(binding)
    request_record = binding.bound_request.to_record()
    action = {
        "action_id": binding.action_id,
        "workspace_id": occurrence.workspace_id,
        "conversation_id": "",
        "action_type": ACQUISITION_START_ACTION_TYPE,
        "owner_module": _ACTION_OWNER_MODULE,
        "operation_type": _ACTION_OPERATION_TYPE,
        "target_ref_json": _json_dump(request_record["target_ref"]),
        "input_json": _json_dump(request_record["input_payload"]),
        "request_schema_version": occurrence.request_schema_version,
        "request_schema_digest": occurrence.request_schema_digest,
        "tool_name": occurrence.tool_name,
        "tool_spec_version": occurrence.tool_spec_version,
        "tool_spec_digest": occurrence.tool_spec_digest,
        "result_schema_version": occurrence.result_schema_version,
        "result_schema_digest": occurrence.result_schema_digest,
        "result_serializer_owner": occurrence.serializer_owner,
        "result_serializer_revision": occurrence.serializer_revision,
        "result_serializer_contract_digest": occurrence.serializer_contract_digest,
        "approval_status": "required",
        "approval_policy": "required",
        "budget_json": _json_dump(binding.budget),
        "idempotency_key": binding.start_idempotency,
        "status": "approval_required",
        "result_ref_json": _json_dump({}),
        "metadata_json": _json_dump({"result_occurrence_ref": binding.result_occurrence_ref}),
        "created_at": submitted_at,
        "updated_at": submitted_at,
    }
    event = {
        "event_id": acquisition_start_v2_operation_event_id(
            event_stream_id=binding.action_id,
            sequence_number=1,
            idempotency_key=event_key,
        ),
        "workspace_id": occurrence.workspace_id,
        "event_stream_id": binding.action_id,
        "operation_run_id": "",
        "action_id": binding.action_id,
        "event_family": "operation_event",
        "event_type": _APPROVAL_REQUIRED_EVENT_TYPE,
        "sequence_number": 1,
        "idempotency_key": event_key,
        "occurred_at": submitted_at,
        "recorded_at": submitted_at,
        "actor": occurrence.actor_id,
        "source": _APPROVAL_REQUIRED_EVENT_SOURCE,
        "payload_json": _json_dump(_approval_required_payload(binding)),
        "schema_version": _APPROVAL_REQUIRED_SCHEMA_VERSION,
        "created_at": submitted_at,
    }
    return action, event


def _assert_exact_json(label: str, actual: Any, expected: Any) -> None:
    from .control_plane_live_postgres import _json_load_dict
    from .json_contract import json_contract_equal

    if not json_contract_equal(_json_load_dict(actual), _json_load_dict(expected)):
        raise ValueError(f"acquisition start submit {label} immutable identity collision")


def _assert_exact_action(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    json_fields = {
        "target_ref_json",
        "input_json",
        "budget_json",
        "result_ref_json",
        "metadata_json",
    }
    scalar_fields = tuple(field for field in expected if field not in json_fields)
    mismatches = [
        field
        for field in scalar_fields
        if str(actual.get(field) if actual.get(field) is not None else "")
        != str(expected.get(field) if expected.get(field) is not None else "")
    ]
    if mismatches:
        raise ValueError("acquisition start submit Action immutable identity collision: " + ", ".join(mismatches))
    for field in sorted(json_fields.intersection(expected)):
        _assert_exact_json(f"Action.{field}", actual.get(field), expected.get(field))


def _assert_replayable_action(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> str:
    """Validate submit-owned identity while allowing the ratified create successor."""

    mutable_fields = {"status", "approval_status", "result_ref_json", "updated_at"}
    immutable_expected = {field: value for field, value in expected.items() if field not in mutable_fields}
    _assert_exact_action(actual, immutable_expected)
    status = str(actual.get("status") or "")
    approval_status = str(actual.get("approval_status") or "")
    if (status, approval_status) == ("approval_required", "required"):
        _assert_exact_action(actual, expected)
        return "pending"
    if (status, approval_status) != ("queued", "approved"):
        raise ValueError("acquisition start submit Action successor lifecycle collision")
    from .control_plane_live_postgres import _json_load_dict

    if not _json_load_dict(actual.get("result_ref_json")):
        raise ValueError("acquisition start submit Action successor result ref missing")
    created_at = _utc_second_iso(actual.get("created_at"))
    updated_at = _utc_second_iso(actual.get("updated_at"))
    if not created_at or not updated_at or updated_at < created_at:
        raise ValueError("acquisition start submit Action successor timestamp collision")
    return "approved"


def _assert_exact_event(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    scalar_fields = tuple(field for field in expected if field != "payload_json")
    mismatches = [
        field
        for field in scalar_fields
        if str(actual.get(field) if actual.get(field) is not None else "")
        != str(expected.get(field) if expected.get(field) is not None else "")
    ]
    if mismatches:
        raise ValueError("acquisition start submit event immutable identity collision: " + ", ".join(mismatches))
    _assert_exact_json("event.payload_json", actual.get("payload_json"), expected.get("payload_json"))


def _assert_pending_result_slot(row: dict[str, Any], occurrence: AgentToolOccurrence) -> None:
    from .agent_tool_result_postgres import _assert_exact_slot

    _assert_exact_slot(row, occurrence)
    if str(row.get("status") or "") != "pending":
        raise ValueError("acquisition start submit result slot is not pending")
    blank_fields = (
        "result_attempt_id",
        "provider_call_id",
        "tool_call_id",
        "action_id",
        "operation_run_id",
        "workflow_command_id",
        "activity_run_id",
        "activity_attempt_id",
        "owner_target_kind",
        "owner_target_id",
        "owner_target_revision_token",
        "terminal_winner_id",
        "owner_result_digest",
        "serialized_result_json",
        "serialized_result_digest",
        "tool_result_message_digest",
    )
    zero_fields = (
        "command_attempt",
        "command_generation",
        "control_epoch",
        "owner_target_revision",
        "owner_target_generation",
    )
    if (
        any(str(row.get(field) or "") for field in blank_fields)
        or any(int(row.get(field) or 0) != 0 for field in zero_fields)
        or bool(row.get("is_error"))
        or row.get("accepted_at") is not None
    ):
        raise ValueError("acquisition start submit result slot has terminal state")
    _assert_exact_json("result_slot.owner_result_ref_json", row.get("owner_result_ref_json"), {})
    _assert_exact_json("result_slot.tool_result_message_json", row.get("tool_result_message_json"), {})


def _assert_replayable_result_slot(row: dict[str, Any], occurrence: AgentToolOccurrence) -> str:
    from .agent_tool_result_postgres import _assert_exact_slot

    _assert_exact_slot(row, occurrence)
    status = str(row.get("status") or "")
    if status == "pending":
        _assert_pending_result_slot(row, occurrence)
        return status
    if status != "accepted" or row.get("accepted_at") is None:
        raise ValueError("acquisition start submit result slot successor state invalid")
    return status


class _LockedPreviewReader:
    def __init__(self, row: Mapping[str, Any]) -> None:
        self._row = dict(row)

    def get_acquisition_plan_preview(
        self,
        preview_id: str,
        *,
        workspace_id: str,
        requester_id: str,
        preview_revision: int,
        preview_digest: str,
    ) -> dict[str, Any] | None:
        from .control_plane_live_postgres import _json_load_dict

        expected = {
            "preview_id": preview_id,
            "workspace_id": workspace_id,
            "requester_id": requester_id,
            "preview_revision": preview_revision,
            "preview_digest": preview_digest,
        }
        for field, value in expected.items():
            actual = self._row.get(field)
            if field == "preview_revision":
                if type(actual) is bool or int(actual or 0) != value:
                    return None
            elif str(actual or "") != str(value):
                return None
        preview = _json_load_dict(self._row.get("preview_json"))
        if not preview:
            return None
        for field, value in expected.items():
            actual = preview.get(field)
            if field == "preview_revision":
                if type(actual) is bool or int(actual or 0) != value:
                    return None
            elif str(actual or "") != str(value):
                return None
        if (
            _utc_second_iso(self._row.get("created_at")) != _utc_second_iso(preview.get("created_at"))
            or _utc_second_iso(self._row.get("expires_at")) != _utc_second_iso(preview.get("expires_at"))
        ):
            return None
        return preview


def _utc_second_iso(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None or value.microsecond:
            return ""
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if type(value) is not str or not value or value != value.strip():
        return ""
    try:
        if value.endswith("Z"):
            parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        else:
            parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None or parsed.microsecond:
            return ""
    except ValueError:
        return ""
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _bind_locked_preview(
    *,
    binding: AcquisitionStartV2SubmissionBinding,
    preview_row: Mapping[str, Any],
    submitted_at: str,
) -> AcquisitionStartV2BoundRequest:
    try:
        now = datetime.strptime(submitted_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError) as exc:
        raise ValueError("acquisition start submit DB timestamp is invalid") from exc
    rebound = AcquisitionStartV2OwnerBinder(_LockedPreviewReader(preview_row)).bind(
        input_payload=binding.bound_request.input_payload,
        context=AcquisitionStartV2BindContext(
            workspace_id=binding.occurrence.workspace_id,
            requester_id=binding.occurrence.actor_id,
        ),
        tool_pins=AcquisitionStartV2ToolPins(
            tool_spec_version=binding.occurrence.tool_spec_version,
            tool_spec_digest=binding.occurrence.tool_spec_digest,
        ),
        now=now,
    )
    if _canonical_json_bytes(rebound.to_record()) != binding.occurrence.canonical_args_json.encode("utf-8"):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, "locked_preview_drift")
    return rebound


def _insert_row(cursor: Any, *, table_name: str, row: dict[str, Any]) -> dict[str, Any]:
    from .control_plane_live_postgres import _fetch_one_dict_row, _quote_identifier

    columns = list(row)
    cursor.execute(
        (
            f"INSERT INTO {table_name} ({', '.join(_quote_identifier(column) for column in columns)}) "
            f"VALUES ({', '.join(['%s'] * len(columns))}) RETURNING *"
        ),
        tuple(row[column] for column in columns),
    )
    inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
    if inserted is None:
        raise RuntimeError(f"acquisition start submit {table_name} insert returned no row")
    return inserted


def _require_authoritative_dependencies(adapter: Any) -> None:
    required = (
        "agent_actions",
        "operation_events",
        "agent_tool_result_slots",
        "acquisition_plan_previews",
    )
    authoritative = getattr(adapter, "is_authoritative", None)
    if not callable(authoritative) or not all(
        adapter.should_prefer_read(table_name) and authoritative(table_name) for table_name in required
    ):
        raise RuntimeError("acquisition start submit requires authoritative PostgreSQL dependencies")


def submit_acquisition_start_v2_action_uow(
    adapter: Any,
    *,
    occurrence: AgentToolOccurrence,
    lock_timeout_seconds: float = 5.0,
    fault_injection_point: str = "",
) -> dict[str, Any]:
    """Create or exact-replay one pending start Action and its sequence-1 event."""

    binding = revalidate_acquisition_start_v2_occurrence(occurrence)
    if isinstance(lock_timeout_seconds, bool):
        raise ValueError("acquisition start submit lock timeout must be finite and positive")
    timeout_seconds = float(lock_timeout_seconds)
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("acquisition start submit lock timeout must be finite and positive")
    if fault_injection_point not in {"", "after_action_write", "after_event_write", "after_commit"}:
        raise ValueError("unsupported acquisition start submit fault injection point")
    _require_authoritative_dependencies(adapter)

    from .agent_tool_result_postgres import _with_retry_dependencies
    from .control_plane_live_postgres import _fetch_all_dict_rows, _fetch_one_dict_row

    deadline = time.monotonic() + timeout_seconds
    retry_attempt = 0
    last_busy_key = f"operation_events:{binding.action_id}"
    busy_error, is_retryable, retry_delay, max_retries, poll_seconds = _with_retry_dependencies()

    def _require_remaining_deadline() -> float:
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
        return remaining_seconds

    def _refresh_transaction_deadline(cursor: Any) -> None:
        milliseconds = max(1, int(_require_remaining_deadline() * 1000))
        cursor.execute(
            "SELECT set_config('lock_timeout', %s, true), "
            "set_config('statement_timeout', %s, true)",
            (f"{milliseconds}ms", f"{milliseconds}ms"),
        )

    while True:
        remaining = _require_remaining_deadline()
        connection = adapter._connect_with_timeout(remaining)
        try:
            outcome: dict[str, Any] | None = None
            with connection.cursor() as cursor:
                _refresh_transaction_deadline(cursor)
                for lock_group in acquisition_start_v2_submit_lock_groups(binding):
                    for lock_key in lock_group:
                        last_busy_key = lock_key
                        _require_remaining_deadline()
                        if not adapter._try_acquire_transaction_lock(cursor, lock_key):
                            raise _StartSubmitLockBusy

                _refresh_transaction_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM agent_actions "
                    "WHERE action_id = %s OR (workspace_id = %s AND idempotency_key = %s) "
                    "ORDER BY action_id FOR UPDATE",
                    (
                        binding.action_id,
                        binding.occurrence.workspace_id,
                        binding.start_idempotency,
                    ),
                )
                action_candidates = _fetch_all_dict_rows(cursor)
                if len(action_candidates) > 1:
                    raise ValueError("acquisition start submit Action split identity collision")
                existing_action = action_candidates[0] if action_candidates else None

                _refresh_transaction_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM agent_tool_result_slots "
                    "WHERE result_slot_id = %s OR logical_occurrence_digest = %s "
                    "ORDER BY result_slot_id FOR UPDATE",
                    (
                        binding.occurrence.result_slot_id,
                        binding.occurrence.logical_occurrence_digest,
                    ),
                )
                slot_candidates = _fetch_all_dict_rows(cursor)
                if len(slot_candidates) != 1:
                    raise ValueError("acquisition start submit result slot identity collision")
                if existing_action is None:
                    _assert_pending_result_slot(slot_candidates[0], binding.occurrence)
                    slot_state = "pending"
                else:
                    slot_state = _assert_replayable_result_slot(slot_candidates[0], binding.occurrence)

                _refresh_transaction_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM acquisition_plan_previews "
                    "WHERE preview_id = %s AND workspace_id = %s AND requester_id = %s "
                    "AND preview_revision = %s AND preview_digest = %s "
                    "ORDER BY preview_id FOR UPDATE",
                    (
                        binding.preview_id,
                        binding.occurrence.workspace_id,
                        binding.occurrence.actor_id,
                        binding.preview_revision,
                        binding.preview_digest,
                    ),
                )
                preview_candidates = _fetch_all_dict_rows(cursor)
                if len(preview_candidates) > 1:
                    raise ValueError("acquisition start submit preview identity collision")
                # Empty and owner-mismatched rows flow through the same locked
                # owner reader so missing/foreign/stale/expired stay one public
                # zero-write conflict class.
                preview_row = preview_candidates[0] if preview_candidates else {}

                event_key = _approval_required_event_key(binding)
                event_id = acquisition_start_v2_operation_event_id(
                    event_stream_id=binding.action_id,
                    sequence_number=1,
                    idempotency_key=event_key,
                )
                _refresh_transaction_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM operation_events "
                    "WHERE event_id = %s "
                    "OR (event_stream_id = %s AND sequence_number = 1) "
                    "OR (event_stream_id = %s AND idempotency_key = %s) "
                    "ORDER BY event_stream_id, sequence_number, event_id FOR UPDATE",
                    (event_id, binding.action_id, binding.action_id, event_key),
                )
                event_candidates = _fetch_all_dict_rows(cursor)
                if len(event_candidates) > 1:
                    raise ValueError("acquisition start submit event split identity collision")
                existing_event = event_candidates[0] if event_candidates else None

                existing_count = int(existing_action is not None) + int(existing_event is not None)
                if existing_count not in {0, 2}:
                    raise ValueError("acquisition start submit partial aggregate collision")

                if existing_count == 2:
                    assert existing_action is not None
                    submitted_at = str(existing_action.get("created_at") or "")
                else:
                    _refresh_transaction_deadline(cursor)
                    cursor.execute(
                        "SELECT to_char("
                        "date_trunc('second', transaction_timestamp()) AT TIME ZONE 'UTC', "
                        "'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'"
                        ") AS transaction_timestamp_iso"
                    )
                    allocation = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                    submitted_at = str(allocation.get("transaction_timestamp_iso") or "")

                rebound = _bind_locked_preview(
                    binding=binding,
                    preview_row=preview_row,
                    submitted_at=submitted_at,
                )
                _require_remaining_deadline()
                if _canonical_json_bytes(rebound.to_record()) != _canonical_json_bytes(
                    binding.bound_request.to_record()
                ):
                    raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, "locked_preview_drift")
                expected_action, expected_event = _canonical_submit_rows(binding, submitted_at=submitted_at)

                if existing_count == 2:
                    assert existing_action is not None and existing_event is not None
                    action_state = _assert_replayable_action(existing_action, expected_action)
                    _assert_exact_event(existing_event, expected_event)
                    if action_state == "pending" and slot_state != "pending":
                        raise ValueError("acquisition start submit successor lifecycle collision")
                    outcome = {
                        "outcome": "replayed",
                        "replayed": True,
                        "action": existing_action,
                        "event": existing_event,
                    }
                else:
                    _refresh_transaction_deadline(cursor)
                    inserted_action = _insert_row(cursor, table_name="agent_actions", row=expected_action)
                    _assert_exact_action(inserted_action, expected_action)
                    if fault_injection_point == "after_action_write":
                        raise RuntimeError("injected acquisition start submit fault after action write")
                    _refresh_transaction_deadline(cursor)
                    inserted_event = _insert_row(cursor, table_name="operation_events", row=expected_event)
                    _assert_exact_event(inserted_event, expected_event)
                    if fault_injection_point == "after_event_write":
                        raise RuntimeError("injected acquisition start submit fault after event write")
                    outcome = {
                        "outcome": "submitted",
                        "replayed": False,
                        "action": inserted_action,
                        "event": inserted_event,
                    }
            _require_remaining_deadline()
            connection.commit()
            if fault_injection_point == "after_commit":
                raise RuntimeError("injected acquisition start submit fault after commit")
            assert outcome is not None
            return outcome
        except _StartSubmitLockBusy:
            connection.rollback()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
            time.sleep(min(poll_seconds, remaining))
        except Exception as exc:
            connection.rollback()
            retry_attempt += 1
            sqlstate = str(getattr(exc, "sqlstate", "") or "").strip().upper()
            remaining = deadline - time.monotonic()
            if sqlstate == "57014" or (sqlstate == "55P03" and remaining <= 0):
                raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds) from exc
            if sqlstate == "55P03" and remaining > 0:
                time.sleep(min(poll_seconds, remaining))
            elif is_retryable(exc) and retry_attempt < max_retries and remaining > 0:
                time.sleep(min(retry_delay(retry_attempt), remaining))
            else:
                raise
        finally:
            connection.close()


__all__ = [
    "AcquisitionStartV2SubmissionBinding",
    "acquisition_start_v2_operation_event_id",
    "acquisition_start_v2_submit_lock_groups",
    "revalidate_acquisition_start_v2_occurrence",
    "submit_acquisition_start_v2_action_uow",
]
