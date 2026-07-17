from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any

import pytest

from sourcing_agent.acquisition_plan_preview import (
    ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT,
    ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE,
    AcquisitionPlanPreview,
    build_acquisition_plan_preview,
)
from sourcing_agent.acquisition_start_v2 import (
    ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION,
    ACQUISITION_START_ACTION_TYPE,
    ACQUISITION_START_V2_APPROVAL_INVALID,
    ACQUISITION_START_V2_COMMAND_INVALID,
    ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT,
    ACQUISITION_START_V2_REQUEST_INVALID,
    ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    ACQUISITION_START_V2_REQUEST_TOOL_SPEC,
    ACQUISITION_START_V2_RESULT_SPEC,
    ACQUISITION_START_V2_ROOT_COMMAND_PAYLOAD_SCHEMA_VERSION,
    ACQUISITION_START_V2_ROOT_COMMAND_TYPE,
    ACQUISITION_START_V2_SNAPSHOT_SCHEMA_VERSION,
    AcquisitionConfirmationReceipt,
    AcquisitionStartV2BindContext,
    AcquisitionStartV2BoundRequest,
    AcquisitionStartV2Error,
    AcquisitionStartV2OwnerBinder,
    AcquisitionStartV2Snapshot,
    AcquisitionStartV2ToolPins,
    acquisition_start_v2_deferred_result,
    acquisition_start_v2_error_result,
    acquisition_start_v2_persisted_action_record,
    acquisition_start_v2_request_schema,
    acquisition_start_v2_success_result,
    build_acquisition_start_v2_root_command_payload,
    serialize_acquisition_start_v2_result,
)
from sourcing_agent.action_result_schema import ActionResultSchemaError
from sourcing_agent.operation_runtime import (
    ACQUISITION_ROOT_ACTION_REQUEST_CONTRACTS,
    ACTION_START_ACQUISITION_RUN,
    DEFAULT_ACTION_REGISTRY,
)

_REGISTRY_DIGEST = hashlib.sha256(b"company-registry-v1").hexdigest()
_TOOL_PINS = AcquisitionStartV2ToolPins(
    tool_spec_version="start_acquisition_run_tool_v2",
    tool_spec_digest=hashlib.sha256(b"start-acquisition-tool-v2").hexdigest(),
)
_CONTEXT = AcquisitionStartV2BindContext(workspace_id="workspace_1", requester_id="requester_1")
_NOW = datetime(2026, 7, 17, 0, 30, tzinfo=timezone.utc)


def _cohort(
    *,
    roles: list[str] | None = None,
    statuses: list[str] | None = None,
    role_match: str = "any",
) -> dict[str, Any]:
    return {
        "schema_version": "cohort_selection.v1",
        "role_bucket_ids": ["research", "engineering"] if roles is None else roles,
        "employment_statuses": ["current", "former"] if statuses is None else statuses,
        "role_match": role_match,
        "source": "user_explicit",
    }


def _preview(
    *,
    roles: list[str] | None = None,
    statuses: list[str] | None = None,
    role_match: str = "any",
    preview_id: str = "preview_1",
    preview_revision: int = 1,
    workspace_id: str = "workspace_1",
    requester_id: str = "requester_1",
    created_at: str = "2026-07-17T00:00:00Z",
    expires_at: str = "2026-07-17T01:00:00Z",
    start_version: str = ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    start_digest: str = ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
) -> AcquisitionPlanPreview:
    return build_acquisition_plan_preview(
        input_payload={
            "cohort_selection": _cohort(roles=roles, statuses=statuses, role_match=role_match),
            "source_preferences": [ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE],
            "coverage_intent": ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT,
            "thematic_constraints": ["Pre-training"],
            "provider_mode_intent": "simulate",
            "budget": {
                "max_provider_calls": 4,
                "max_provider_items": 20,
                "max_output_candidates": 10,
                "max_cost_micro_usd": 2_000_000,
                "max_elapsed_seconds": 900,
            },
        },
        target_ref={
            "workspace_id": workspace_id,
            "requester_id": requester_id,
            "company_target": {
                "canonical_company_id": "thinkingmachineslab",
                "canonical_name": "Thinking Machines Lab",
                "company_registry_revision": "company_registry.v1",
                "company_registry_digest": _REGISTRY_DIGEST,
                "provider_company_labels": ["Thinking Machines Lab", "thinkingmachinesai"],
            },
        },
        preview_id=preview_id,
        preview_revision=preview_revision,
        created_at=created_at,
        expires_at=expires_at,
        intended_start_request_schema_version=start_version,
        intended_start_request_schema_digest=start_digest,
    )


def _reference(preview: AcquisitionPlanPreview) -> dict[str, Any]:
    return {
        "preview_id": preview.preview_id,
        "preview_revision": preview.preview_revision,
        "preview_digest": preview.preview_digest,
    }


class _PreviewRepository:
    def __init__(self, *previews: AcquisitionPlanPreview) -> None:
        self.rows = {preview.preview_id: preview.to_record() for preview in previews}
        self.calls: list[tuple[str, str, str, int, str]] = []

    def get_acquisition_plan_preview(
        self,
        preview_id: str,
        *,
        workspace_id: str,
        requester_id: str,
        preview_revision: int,
        preview_digest: str,
    ) -> dict[str, Any] | None:
        self.calls.append((preview_id, workspace_id, requester_id, preview_revision, preview_digest))
        row = self.rows.get(preview_id)
        if (
            row is None
            or row["workspace_id"] != workspace_id
            or row["requester_id"] != requester_id
            or row["preview_revision"] != preview_revision
            or row["preview_digest"] != preview_digest
        ):
            return None
        return copy.deepcopy(row)


class _WriteCountingAdapter:
    _WRITE_KEYS = (
        "agent_action",
        "operation_run",
        "workflow_command",
        "budget",
        "event",
        "provider",
    )

    def __init__(self, repository: _PreviewRepository) -> None:
        self.repository = repository
        self.binder = AcquisitionStartV2OwnerBinder(repository)
        self.writes = dict.fromkeys(self._WRITE_KEYS, 0)

    def submit(self, reference: dict[str, Any], *, context: AcquisitionStartV2BindContext, now: datetime) -> None:
        self.binder.bind(input_payload=reference, context=context, tool_pins=_TOOL_PINS, now=now)
        self.writes["agent_action"] += 1

    def confirm(
        self,
        action: dict[str, Any],
        *,
        context: AcquisitionStartV2BindContext,
        approved_at: str = "2026-07-17T00:31:00Z",
    ) -> None:
        self.binder.confirm_exact_action(
            persisted_action=action,
            context=context,
            approval_actor_id="human_1",
            approval_actor_kind="authenticated_user",
            receipt_id="receipt_1",
            approval_policy_revision="approval_policy.v1",
            approved_at=approved_at,
        )
        for key in ("operation_run", "workflow_command", "budget", "event"):
            self.writes[key] += 1


def _bind(preview: AcquisitionPlanPreview | None = None) -> tuple[_PreviewRepository, AcquisitionStartV2BoundRequest]:
    candidate = preview or _preview()
    repository = _PreviewRepository(candidate)
    bound = AcquisitionStartV2OwnerBinder(repository).bind(
        input_payload=_reference(candidate),
        context=_CONTEXT,
        tool_pins=_TOOL_PINS,
        now=_NOW,
    )
    return repository, bound


def _receipt(
    repository: _PreviewRepository,
    bound: AcquisitionStartV2BoundRequest,
) -> AcquisitionConfirmationReceipt:
    return AcquisitionStartV2OwnerBinder(repository).confirm_exact_action(
        persisted_action=acquisition_start_v2_persisted_action_record(
            action_id="action_1",
            bound_request=bound,
        ),
        context=_CONTEXT,
        approval_actor_id="human_1",
        approval_actor_kind="authenticated_user",
        receipt_id="receipt_1",
        approval_policy_revision="approval_policy.v1",
        approved_at="2026-07-17T00:31:00Z",
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def test_request_v2_is_closed_and_caller_owns_only_the_preview_reference() -> None:
    schema = acquisition_start_v2_request_schema()
    assert schema["additionalProperties"] is False
    assert set(schema["properties"]) == {"input_payload", "target_ref"}
    assert set(schema["properties"]["input_payload"]["properties"]) == {
        "preview_id",
        "preview_revision",
        "preview_digest",
    }
    assert schema["properties"]["input_payload"]["additionalProperties"] is False
    assert set(schema["properties"]["target_ref"]["properties"]) == {
        "workspace_id",
        "requester_id",
        "start_snapshot",
    }
    assert ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST == ACQUISITION_START_V2_REQUEST_TOOL_SPEC.input_schema_digest


@pytest.mark.parametrize(
    "forbidden_field,value",
    [
        ("target_company", "Thinking Machines Lab"),
        ("query", "pre-training researchers"),
        ("cohort_selection", _cohort()),
        ("source_preferences", ["linkedin_profile_search"]),
        ("coverage_intent", "requested_cohort"),
        ("budget", {"max_provider_calls": 4}),
        ("provider_planning_manifest", {}),
        ("workspace_id", "workspace_1"),
        ("requester_id", "requester_1"),
    ],
)
def test_v2_rejects_every_inline_override(forbidden_field: str, value: Any) -> None:
    preview = _preview()
    candidate = {**_reference(preview), forbidden_field: value}
    with pytest.raises(AcquisitionStartV2Error, match=ACQUISITION_START_V2_REQUEST_INVALID):
        AcquisitionStartV2OwnerBinder(_PreviewRepository(preview)).bind(
            input_payload=candidate,
            context=_CONTEXT,
            tool_pins=_TOOL_PINS,
            now=_NOW,
        )


@pytest.mark.parametrize(
    "roles,statuses,role_match",
    [
        ([], ["current"], "any"),
        ([], ["former"], "all"),
        (["research"], ["current", "former"], "any"),
        (["engineering"], ["former"], "all"),
        (["research", "engineering"], ["current"], "any"),
        (["research", "engineering"], ["current", "former"], "all"),
    ],
)
def test_canonical_multiselect_round_trips_without_reinterpretation(
    roles: list[str],
    statuses: list[str],
    role_match: str,
) -> None:
    preview = _preview(roles=roles, statuses=statuses, role_match=role_match)
    _, bound = _bind(preview)
    preview_record = preview.to_record()
    copied = bound.snapshot.to_record()["preview"]
    assert copied == preview_record
    assert copied["effective_request"]["cohort_selection"] == preview_record["effective_request"]["cohort_selection"]
    assert copied["effective_request"]["budget"] == preview_record["effective_request"]["budget"]
    assert copied["provider_planning_manifest"] == preview_record["provider_planning_manifest"]


def test_same_owner_bind_copies_complete_preview_and_all_start_pins() -> None:
    preview = _preview()
    repository, bound = _bind(preview)
    snapshot = bound.snapshot.to_record()
    assert repository.calls == [
        (
            "preview_1",
            "workspace_1",
            "requester_1",
            preview.preview_revision,
            preview.preview_digest,
        )
    ]
    assert bound.input_payload == _reference(preview)
    assert bound.target_ref["workspace_id"] == "workspace_1"
    assert bound.target_ref["requester_id"] == "requester_1"
    assert snapshot["schema_version"] == ACQUISITION_START_V2_SNAPSHOT_SCHEMA_VERSION
    assert snapshot["preview"] == preview.to_record()
    assert snapshot["request_pins"] == {
        "schema_version": ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
        "schema_digest": ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    }
    assert snapshot["result_pins"]["schema_digest"] == ACQUISITION_START_V2_RESULT_SPEC.result_schema_digest
    assert snapshot["result_pins"]["serializer_owner"] == ACQUISITION_START_V2_RESULT_SPEC.serializer_owner
    assert snapshot["result_pins"]["serializer_revision"] == ACQUISITION_START_V2_RESULT_SPEC.serializer_revision
    assert snapshot["tool_pins"] == _TOOL_PINS.to_record()
    digest_input = dict(snapshot)
    assert digest_input.pop("snapshot_digest") == _digest(digest_input)


def test_bound_request_and_snapshot_are_deeply_immutable_and_defensive() -> None:
    _, bound = _bind()
    assert isinstance(bound.target_ref, MappingProxyType)
    assert isinstance(bound.snapshot._record, MappingProxyType)
    with pytest.raises(TypeError):
        bound.target_ref["workspace_id"] = "foreign"  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        bound._record = {}  # type: ignore[misc]
    exported = bound.to_record()
    exported["target_ref"]["start_snapshot"]["preview"]["company_target"]["canonical_name"] = "Changed"
    assert bound.snapshot.preview.to_record()["company_target"]["canonical_name"] == "Thinking Machines Lab"


@pytest.mark.parametrize(
    "case", ["missing", "foreign_workspace", "foreign_requester", "stale_revision", "stale_digest", "expired"]
)
def test_missing_foreign_stale_and_expired_previews_are_one_zero_write_class(case: str) -> None:
    preview = _preview()
    repository = _PreviewRepository(preview)
    adapter = _WriteCountingAdapter(repository)
    reference = _reference(preview)
    context = _CONTEXT
    now = _NOW
    if case == "missing":
        repository.rows.clear()
    elif case == "foreign_workspace":
        context = AcquisitionStartV2BindContext(workspace_id="workspace_foreign", requester_id="requester_1")
    elif case == "foreign_requester":
        context = AcquisitionStartV2BindContext(workspace_id="workspace_1", requester_id="requester_foreign")
    elif case == "stale_revision":
        reference["preview_revision"] = 2
    elif case == "stale_digest":
        reference["preview_digest"] = "f" * 64
    else:
        now = datetime(2026, 7, 17, 1, 0, tzinfo=timezone.utc)
    with pytest.raises(
        AcquisitionStartV2Error,
        match=ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT,
    ):
        adapter.submit(reference, context=context, now=now)
    assert adapter.writes == dict.fromkeys(adapter._WRITE_KEYS, 0)


def test_repository_corruption_is_indistinguishable_and_zero_write() -> None:
    preview = _preview()
    repository = _PreviewRepository(preview)
    repository.rows[preview.preview_id]["company_target"]["canonical_name"] = "Forged"
    adapter = _WriteCountingAdapter(repository)
    with pytest.raises(
        AcquisitionStartV2Error,
        match=ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT,
    ):
        adapter.submit(_reference(preview), context=_CONTEXT, now=_NOW)
    assert adapter.writes == dict.fromkeys(adapter._WRITE_KEYS, 0)


def test_non_v2_preview_is_not_reinterpreted_and_v1_contract_stays_historical() -> None:
    v1_digest = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN).request_schema_digest
    preview = _preview(start_version="acquisition_root_request_v1", start_digest=str(v1_digest))
    with pytest.raises(
        AcquisitionStartV2Error,
        match=ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT,
    ):
        AcquisitionStartV2OwnerBinder(_PreviewRepository(preview)).bind(
            input_payload=_reference(preview),
            context=_CONTEXT,
            tool_pins=_TOOL_PINS,
            now=_NOW,
        )
    assert ACTION_START_ACQUISITION_RUN == ACQUISITION_START_ACTION_TYPE
    assert (
        ACQUISITION_ROOT_ACTION_REQUEST_CONTRACTS[ACTION_START_ACQUISITION_RUN]["request_schema_version"]
        == "acquisition_root_request_v1"
    )
    assert ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION == "acquisition_root_request_v2"


def test_exact_human_confirmation_reloads_preview_and_binds_every_identity() -> None:
    repository, bound = _bind()
    receipt = _receipt(repository, bound)
    record = receipt.to_record()
    preview = bound.snapshot.preview.to_record()
    effective = preview["effective_request"]
    company = preview["company_target"]
    manifest = preview["provider_planning_manifest"]
    assert repository.calls == [
        (
            "preview_1",
            "workspace_1",
            "requester_1",
            bound.snapshot.preview.preview_revision,
            bound.snapshot.preview.preview_digest,
        ),
        (
            "preview_1",
            "workspace_1",
            "requester_1",
            bound.snapshot.preview.preview_revision,
            bound.snapshot.preview.preview_digest,
        ),
    ]
    assert record["schema_version"] == ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION
    assert record["action_id"] == "action_1"
    assert record["approval_actor_id"] == "human_1"
    assert record["approval_actor_kind"] == "authenticated_user"
    assert record["preview_ref"] == _reference(bound.snapshot.preview)
    assert record["effective_request_digest"] == preview["effective_request_digest"]
    assert record["company_identity"] == {
        "canonical_company_id": company["canonical_company_id"],
        "company_registry_revision": company["company_registry_revision"],
        "company_registry_digest": company["company_registry_digest"],
        "company_target_digest": company["company_target_digest"],
    }
    assert record["cohort_identity"]["cohort_selection"] == effective["cohort_selection"]
    assert record["cohort_identity"]["cohort_selection_digest"] == effective["cohort_selection_digest"]
    assert record["provider_manifest_identity"] == {
        "schema_version": manifest["schema_version"],
        "manifest_digest": manifest["manifest_digest"],
        "physical_query_digest": manifest["physical_query_digest"],
    }
    assert record["budget"] == effective["budget"]
    assert record["start_snapshot_digest"] == bound.snapshot.snapshot_digest
    assert record["tool_pins"] == _TOOL_PINS.to_record()
    digest_input = dict(record)
    assert digest_input.pop("receipt_digest") == _digest(digest_input)


@pytest.mark.parametrize("actor_kind", ["model", "model_agent", "service"])
def test_model_or_nonhuman_actor_cannot_mint_confirmation(actor_kind: str) -> None:
    repository, bound = _bind()
    action = acquisition_start_v2_persisted_action_record(action_id="action_1", bound_request=bound)
    with pytest.raises(AcquisitionStartV2Error, match=ACQUISITION_START_V2_APPROVAL_INVALID):
        AcquisitionStartV2OwnerBinder(repository).confirm_exact_action(
            persisted_action=action,
            context=_CONTEXT,
            approval_actor_id="model_1",
            approval_actor_kind=actor_kind,
            receipt_id="receipt_1",
            approval_policy_revision="approval_policy.v1",
            approved_at="2026-07-17T00:31:00Z",
        )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda action: action.update({"workspace_id": "workspace_foreign"}),
        lambda action: action.update({"requester_id": "requester_foreign"}),
        lambda action: action.update({"request_schema_version": "acquisition_root_request_v1"}),
        lambda action: action.update({"request_schema_digest": "f" * 64}),
        lambda action: action.update({"state": "approved"}),
        lambda action: action["request"]["input_payload"].update({"preview_digest": "f" * 64}),
        lambda action: action["request"]["target_ref"]["start_snapshot"]["tool_pins"].update(
            {"tool_spec_digest": "f" * 64}
        ),
        lambda action: action["request"]["target_ref"]["start_snapshot"]["preview"]["effective_request"][
            "cohort_selection"
        ].update({"role_match": "all"}),
    ],
)
def test_approval_tamper_fails_before_run_command_budget_or_event(mutator: Any) -> None:
    repository, bound = _bind()
    action = acquisition_start_v2_persisted_action_record(action_id="action_1", bound_request=bound)
    mutator(action)
    adapter = _WriteCountingAdapter(repository)
    with pytest.raises(
        AcquisitionStartV2Error,
        match=ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT,
    ):
        adapter.confirm(action, context=_CONTEXT)
    assert adapter.writes == dict.fromkeys(adapter._WRITE_KEYS, 0)


def test_approval_missing_or_expired_reload_uses_the_same_zero_write_conflict() -> None:
    repository, bound = _bind()
    action = acquisition_start_v2_persisted_action_record(action_id="action_1", bound_request=bound)
    for missing, approved_at in ((True, "2026-07-17T00:31:00Z"), (False, "2026-07-17T01:00:00Z")):
        current_repository = _PreviewRepository(bound.snapshot.preview)
        if missing:
            current_repository.rows.clear()
        adapter = _WriteCountingAdapter(current_repository)
        with pytest.raises(
            AcquisitionStartV2Error,
            match=ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT,
        ):
            adapter.confirm(action, context=_CONTEXT, approved_at=approved_at)
        assert adapter.writes == dict.fromkeys(adapter._WRITE_KEYS, 0)


def test_root_command_exact_copies_receipt_ref_and_complete_snapshot() -> None:
    repository, bound = _bind()
    receipt = _receipt(repository, bound)
    payload = build_acquisition_start_v2_root_command_payload(
        snapshot=bound.snapshot,
        receipt=receipt,
        operation_run_id="operation_1",
        workflow_run_id="workflow_1",
    ).to_record()
    assert payload["schema_version"] == ACQUISITION_START_V2_ROOT_COMMAND_PAYLOAD_SCHEMA_VERSION
    assert payload["command_type"] == ACQUISITION_START_V2_ROOT_COMMAND_TYPE
    assert payload["action_id"] == "action_1"
    assert payload["confirmation_receipt_ref"] == {
        "receipt_id": receipt.receipt_id,
        "receipt_digest": receipt.receipt_digest,
    }
    assert payload["start_snapshot"] == bound.snapshot.to_record()
    assert payload["start_snapshot_digest"] == bound.snapshot.snapshot_digest
    assert "target_company" not in payload
    assert "query" not in payload
    digest_input = dict(payload)
    assert digest_input.pop("payload_digest") == _digest(digest_input)


def test_rehashed_receipt_identity_tamper_cannot_build_root_command() -> None:
    repository, bound = _bind()
    receipt_record = _receipt(repository, bound).to_record()
    receipt_record["company_identity"]["canonical_company_id"] = "anthropic"
    receipt_record["receipt_digest"] = _digest(
        {key: value for key, value in receipt_record.items() if key != "receipt_digest"}
    )
    forged = AcquisitionConfirmationReceipt(receipt_record)
    with pytest.raises(AcquisitionStartV2Error, match=ACQUISITION_START_V2_COMMAND_INVALID):
        build_acquisition_start_v2_root_command_payload(
            snapshot=bound.snapshot,
            receipt=forged,
            operation_run_id="operation_1",
            workflow_run_id="workflow_1",
        )


def test_snapshot_digest_tamper_and_root_payload_digest_tamper_fail_closed() -> None:
    repository, bound = _bind()
    snapshot_record = bound.snapshot.to_record()
    snapshot_record["snapshot_digest"] = "f" * 64
    with pytest.raises(AcquisitionStartV2Error, match=ACQUISITION_START_V2_REQUEST_INVALID):
        AcquisitionStartV2Snapshot(snapshot_record)

    receipt = _receipt(repository, bound)
    payload = build_acquisition_start_v2_root_command_payload(
        snapshot=bound.snapshot,
        receipt=receipt,
        operation_run_id="operation_1",
        workflow_run_id="workflow_1",
    ).to_record()
    payload["payload_digest"] = "f" * 64
    from sourcing_agent.acquisition_start_v2 import AcquisitionStartV2RootCommandPayload

    with pytest.raises(AcquisitionStartV2Error, match=ACQUISITION_START_V2_COMMAND_INVALID):
        AcquisitionStartV2RootCommandPayload(payload)


def test_start_result_spec_is_f1_compatible_and_serializes_exact_receipt_identity() -> None:
    repository, bound = _bind()
    receipt = _receipt(repository, bound)
    result = acquisition_start_v2_success_result(
        action_id="action_1",
        operation_run_id="operation_1",
        workflow_command_id="command_1",
        snapshot=bound.snapshot,
        receipt=receipt,
    )
    serialized = serialize_acquisition_start_v2_result(result)
    assert json.loads(serialized) == result
    assert ACQUISITION_START_V2_RESULT_SPEC.tool_name == ACQUISITION_START_ACTION_TYPE
    assert ACQUISITION_START_V2_RESULT_SPEC.tool_kind == "action"
    assert ACQUISITION_START_V2_RESULT_SPEC.action_type == ACQUISITION_START_ACTION_TYPE
    assert ACQUISITION_START_V2_RESULT_SPEC.result_schema_version == "acquisition_start_result_v2"
    assert len(ACQUISITION_START_V2_RESULT_SPEC.result_schema_digest) == 64


def test_start_result_deferred_error_and_invalid_owner_output() -> None:
    deferred = acquisition_start_v2_deferred_result(reason="approval_pending", retryable=True)
    error = acquisition_start_v2_error_result(reason="preview_conflict")
    assert json.loads(serialize_acquisition_start_v2_result(deferred)) == deferred
    assert json.loads(serialize_acquisition_start_v2_result(error)) == error
    with pytest.raises(ActionResultSchemaError):
        serialize_acquisition_start_v2_result({"variant": "success", "status": "accepted"})


def test_leaf_has_no_registry_serving_dispatch_or_provider_side_effect_surface() -> None:
    import sourcing_agent.acquisition_start_v2 as module

    forbidden = {
        "DEFAULT_ACTION_REGISTRY",
        "DEFAULT_AGENT_TOOL_REGISTRY",
        "served",
        "dispatch",
        "provider_client",
        "model_client",
        "create_operation_run",
        "create_workflow_command",
    }
    assert forbidden.isdisjoint(module.__dict__)
