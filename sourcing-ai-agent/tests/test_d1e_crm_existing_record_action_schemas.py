from __future__ import annotations

import copy
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from sourcing_agent.action_request_schema import ActionRequestSchemaBuilder
from sourcing_agent.action_target_binding import (
    AUTHORIZATION_MODE_AUTHENTICATED,
    AUTHORIZATION_MODE_OPEN_OPERATOR,
    CRM_RECORD_TARGET_NOT_FOUND,
    CRM_RECORD_TARGET_STALE,
    ActionBindContext,
    ActionTargetBinderRegistry,
    ActionTargetBinderSpec,
    ActionTargetBindingError,
    CRMRecordTargetBinder,
    build_crm_existing_record_target_binder_registry,
)
from sourcing_agent.crm_contract import CRM_STAGE_VALUES
from sourcing_agent.operation_runtime import (
    ACTION_ADD_CRM_NOTE,
    ACTION_CREATE_CRM_TASK,
    ACTION_SET_CRM_STAGE,
    CRM_EXISTING_RECORD_ACTION_REQUEST_CONTRACTS,
    DEFAULT_ACTION_REGISTRY,
    ActionRegistry,
    ActionRequestSpec,
    ActionRequestValidationError,
    OperationRuntimeWriter,
    OwnerBoundTargetRef,
)
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin

CRM_ACTIONS = (ACTION_SET_CRM_STAGE, ACTION_ADD_CRM_NOTE, ACTION_CREATE_CRM_TASK)
CRM_TARGET: dict[str, Any] = {
    "crm_record_id": "crm-record-a",
    "workspace_id": "user-alice",
    "owner_user_id": "alice",
    "crm_version": 1,
}
VALID_INPUTS: dict[str, dict[str, Any]] = {
    ACTION_SET_CRM_STAGE: {"stage": "researching", "quality_score": 87.5, "comment": "reviewed"},
    ACTION_ADD_CRM_NOTE: {"note": "Reach out after the conference."},
    ACTION_CREATE_CRM_TASK: {
        "title": "Prepare intro",
        "description": "Draft a concise introduction.",
        "due_at": "2026-07-20T09:00:00Z",
    },
}
EXPECTED_SCHEMA_DIGESTS = {
    ACTION_SET_CRM_STAGE: "a6359c447a44fa597d242cf97d00ed5946bc97bc1aa1ad91670a5d372c00a1cb",
    ACTION_ADD_CRM_NOTE: "2e5dcb023b15da5090d179f0ba7d73e2f0c90ab0f43a57eef05833d94bb36637",
    ACTION_CREATE_CRM_TASK: "8bf7d372ae725b7a9c6e753023a9472c9d586d604db5a57d1eb3d466a1e3ab7e",
}


class _CRMRecordStore:
    def __init__(self, records: dict[str, dict[str, Any]] | None = None) -> None:
        self.records = copy.deepcopy(records or {})
        self.lookups: list[str] = []

    def get_crm_record(self, crm_record_id: str) -> dict[str, Any]:
        self.lookups.append(crm_record_id)
        return copy.deepcopy(self.records.get(crm_record_id) or {})


def _record(
    crm_record_id: str = "crm-record-a",
    *,
    workspace_id: str = "user-alice",
    owner_user_id: str = "alice",
    crm_version: int = 1,
) -> dict[str, Any]:
    return {
        "crm_record_id": crm_record_id,
        "workspace_id": workspace_id,
        "owner_user_id": owner_user_id,
        "crm_version": crm_version,
        "person_identity_key": f"person::{crm_record_id}",
    }


def _authenticated_context(
    crm_record_id: str = "crm-record-a",
    *,
    workspace_id: str = "user-alice",
    owner_user_id: str = "alice",
) -> ActionBindContext:
    return ActionBindContext(
        authorization_mode=AUTHORIZATION_MODE_AUTHENTICATED,
        workspace_id=workspace_id,
        owner_user_id=owner_user_id,
        target_selector={"crm_record_id": crm_record_id},
    )


def _declared_spec(action_type: str) -> ActionRequestSpec:
    return replace(
        DEFAULT_ACTION_REGISTRY.spec_for(action_type),
        **dict(CRM_EXISTING_RECORD_ACTION_REQUEST_CONTRACTS[action_type]),
    )


def _declared_registry() -> ActionRegistry:
    return ActionRegistry({action_type: _declared_spec(action_type) for action_type in CRM_ACTIONS})


def test_schema_builder_always_closes_root_and_both_owner_segments() -> None:
    schema = ActionRequestSchemaBuilder().build(
        input_properties={"note": {"type": "string"}},
        input_required=("note",),
        target_properties={"crm_record_id": {"type": "string"}},
        target_required=("crm_record_id",),
    )

    assert schema["additionalProperties"] is False
    assert schema["required"] == ["input_payload", "target_ref"]
    assert schema["properties"]["input_payload"]["additionalProperties"] is False
    assert schema["properties"]["target_ref"]["additionalProperties"] is False

    with pytest.raises(ValueError, match="dual owners"):
        ActionRequestSchemaBuilder().build(
            input_properties={"crm_record_id": {"type": "string"}},
            target_properties={"crm_record_id": {"type": "string"}},
        )
    with pytest.raises(ValueError, match="required fields are invalid"):
        ActionRequestSchemaBuilder().build(
            input_properties={},
            input_required=("unknown",),
            target_properties={},
        )


def test_crm_action_schemas_are_exact_closed_digest_pinned_and_not_activated() -> None:
    assert set(CRM_EXISTING_RECORD_ACTION_REQUEST_CONTRACTS) == set(CRM_ACTIONS)
    assert all(
        not DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema
        for action_type in DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    )

    for action_type in CRM_ACTIONS:
        spec = _declared_spec(action_type)
        schema = dict(spec.request_schema or {})
        segments = dict(schema["properties"])
        input_schema = dict(segments["input_payload"])
        target_schema = dict(segments["target_ref"])
        assert schema["additionalProperties"] is False
        assert input_schema["additionalProperties"] is False
        assert target_schema["additionalProperties"] is False
        assert set(target_schema["properties"]) == {
            "crm_record_id",
            "workspace_id",
            "owner_user_id",
            "crm_version",
        }
        assert set(target_schema["required"]) == set(target_schema["properties"])
        assert spec.request_schema_digest == EXPECTED_SCHEMA_DIGESTS[action_type]
        assert spec.owner_module == "crm_writer"
        assert spec.validate_request(
            input_payload=VALID_INPUTS[action_type],
            target_ref=CRM_TARGET,
        ) == (VALID_INPUTS[action_type], CRM_TARGET)

    stage_schema = dict(dict(_declared_spec(ACTION_SET_CRM_STAGE).request_schema or {})["properties"])["input_payload"]
    assert tuple(dict(stage_schema)["properties"]["stage"]["enum"]) == CRM_STAGE_VALUES


@pytest.mark.parametrize("action_type", CRM_ACTIONS)
def test_crm_action_schema_rejects_caller_target_aliases_and_forged_targets(action_type: str) -> None:
    spec = _declared_spec(action_type)
    for forged_input in (
        {**VALID_INPUTS[action_type], "crm_record_id": "forged"},
        {**VALID_INPUTS[action_type], "record_id": "forged"},
        {**VALID_INPUTS[action_type], "workspace_id": "user-bob"},
        {**VALID_INPUTS[action_type], "owner_user_id": "bob"},
        {**VALID_INPUTS[action_type], "record_version": 999},
    ):
        with pytest.raises(ActionRequestValidationError, match="action_request_schema_validation_failed"):
            spec.validate_request(input_payload=forged_input, target_ref=CRM_TARGET)

    for forged_target in (
        {**CRM_TARGET, "extra": True},
        {**CRM_TARGET, "workspace_id": ""},
        {**CRM_TARGET, "crm_version": 0},
        {key: value for key, value in CRM_TARGET.items() if key != "owner_user_id"},
    ):
        with pytest.raises(ActionRequestValidationError, match="action_request_schema_validation_failed"):
            spec.validate_request(input_payload=VALID_INPUTS[action_type], target_ref=forged_target)


def test_crm_schema_specific_business_constraints_fail_closed() -> None:
    stage_spec = _declared_spec(ACTION_SET_CRM_STAGE)
    for invalid in ({}, {"stage": "unknown"}, {"stage": "new", "quality_score": 101}):
        with pytest.raises(ActionRequestValidationError):
            stage_spec.validate_request(input_payload=invalid, target_ref=CRM_TARGET)

    note_spec = _declared_spec(ACTION_ADD_CRM_NOTE)
    for invalid in ({}, {"note": "   "}, {"note": "x" * 20_001}):
        with pytest.raises(ActionRequestValidationError):
            note_spec.validate_request(input_payload=invalid, target_ref=CRM_TARGET)

    task_spec = _declared_spec(ACTION_CREATE_CRM_TASK)
    for invalid in ({}, {"title": "   "}, {"title": "x" * 501}):
        with pytest.raises(ActionRequestValidationError):
            task_spec.validate_request(input_payload=invalid, target_ref=CRM_TARGET)


def test_bind_context_is_deeply_immutable_and_rejects_partial_authorization() -> None:
    selector: dict[str, Any] = {"crm_record_id": "crm-record-a", "nested": {"values": ["one"]}}
    context = ActionBindContext(
        authorization_mode=AUTHORIZATION_MODE_OPEN_OPERATOR,
        workspace_id="default",
        target_selector=selector,
    )
    selector["crm_record_id"] = "mutated"
    selector["nested"]["values"].append("two")
    assert context.target_selector["crm_record_id"] == "crm-record-a"
    assert context.target_selector["nested"]["values"] == ("one",)
    with pytest.raises(TypeError):
        context.target_selector["crm_record_id"] = "forbidden"  # type: ignore[index]

    with pytest.raises(ActionTargetBindingError, match="owner_invalid"):
        ActionBindContext(
            authorization_mode=AUTHORIZATION_MODE_AUTHENTICATED,
            workspace_id="user-alice",
            target_selector={"crm_record_id": "crm-record-a"},
        )
    with pytest.raises(ActionTargetBindingError, match="open_owner_forbidden"):
        ActionBindContext(
            authorization_mode=AUTHORIZATION_MODE_OPEN_OPERATOR,
            workspace_id="default",
            owner_user_id="alice",
            target_selector={"crm_record_id": "crm-record-a"},
        )


def test_crm_binder_registry_is_exact_and_mints_owner_snapshot_for_all_three_actions() -> None:
    store = _CRMRecordStore({"crm-record-a": _record()})
    registry = build_crm_existing_record_target_binder_registry(store)
    assert registry.to_record() == {action_type: {"owner_module": "crm_writer"} for action_type in sorted(CRM_ACTIONS)}

    for action_type in CRM_ACTIONS:
        target = registry.bind(action_type=action_type, context=_authenticated_context())
        assert target.owner_module == "crm_writer"
        assert dict(target.target_ref) == CRM_TARGET

    assert store.lookups == ["crm-record-a"] * len(CRM_ACTIONS)
    with pytest.raises(ActionTargetBindingError, match="action_target_binder_missing:unknown"):
        registry.bind(action_type="unknown", context=_authenticated_context())


@pytest.mark.parametrize("action_type", CRM_ACTIONS)
def test_authenticated_foreign_and_missing_records_share_one_zero_write_outcome(action_type: str) -> None:
    store = _CRMRecordStore(
        {
            "foreign-workspace": _record(
                "foreign-workspace",
                workspace_id="user-bob",
                owner_user_id="bob",
            ),
            "foreign-adjunct": _record("foreign-adjunct", owner_user_id="bob"),
        }
    )
    registry = build_crm_existing_record_target_binder_registry(store)

    outcomes: list[str] = []
    for record_id in ("missing", "foreign-workspace", "foreign-adjunct"):
        with pytest.raises(ActionTargetBindingError) as exc_info:
            registry.bind(
                action_type=action_type,
                context=_authenticated_context(record_id),
            )
        outcomes.append(exc_info.value.reason)

    assert outcomes == [CRM_RECORD_TARGET_NOT_FOUND] * 3
    assert store.records == {
        "foreign-workspace": _record("foreign-workspace", workspace_id="user-bob", owner_user_id="bob"),
        "foreign-adjunct": _record("foreign-adjunct", owner_user_id="bob"),
    }


def test_authenticated_blank_owner_adjunct_and_open_operator_exact_workspace_remain_compatible() -> None:
    store = _CRMRecordStore(
        {
            "blank-adjunct": _record("blank-adjunct", owner_user_id=""),
            "operator-record": _record("operator-record", workspace_id="operator-a", owner_user_id="legacy-user"),
        }
    )
    registry = build_crm_existing_record_target_binder_registry(store)

    authenticated = registry.bind(
        action_type=ACTION_ADD_CRM_NOTE,
        context=_authenticated_context("blank-adjunct"),
    )
    assert authenticated.target_ref["workspace_id"] == "user-alice"
    assert authenticated.target_ref["owner_user_id"] == ""

    operator = registry.bind(
        action_type=ACTION_CREATE_CRM_TASK,
        context=ActionBindContext(
            authorization_mode=AUTHORIZATION_MODE_OPEN_OPERATOR,
            workspace_id="operator-a",
            target_selector={"crm_record_id": "operator-record"},
        ),
    )
    assert operator.target_ref["workspace_id"] == "operator-a"
    with pytest.raises(ActionTargetBindingError, match=CRM_RECORD_TARGET_NOT_FOUND):
        registry.bind(
            action_type=ACTION_CREATE_CRM_TASK,
            context=ActionBindContext(
                authorization_mode=AUTHORIZATION_MODE_OPEN_OPERATOR,
                workspace_id="default",
                target_selector={"crm_record_id": "operator-record"},
            ),
        )


def test_forged_selector_is_rejected_without_owner_lookup() -> None:
    store = _CRMRecordStore({"crm-record-a": _record()})
    registry = build_crm_existing_record_target_binder_registry(store)
    for selector in (
        {"record_id": "crm-record-a"},
        {"crm_record_id": "crm-record-a", "workspace_id": "user-alice"},
        {"crm_record_id": "crm-record-a", "owner_user_id": "alice"},
    ):
        with pytest.raises(ActionTargetBindingError, match="crm_record_target_selector_invalid"):
            registry.bind(
                action_type=ACTION_SET_CRM_STAGE,
                context=ActionBindContext(
                    authorization_mode=AUTHORIZATION_MODE_AUTHENTICATED,
                    workspace_id="user-alice",
                    owner_user_id="alice",
                    target_selector=selector,
                ),
            )
    assert store.lookups == []


def test_registry_rejects_cross_owner_binder_result() -> None:
    registry = ActionTargetBinderRegistry(
        (
            ActionTargetBinderSpec(
                action_type="synthetic",
                owner_module="expected-owner",
                binder=lambda _: OwnerBoundTargetRef(owner_module="foreign-owner", target_ref={}),
            ),
        )
    )
    with pytest.raises(ActionTargetBindingError, match="action_target_binder_owner_mismatch"):
        registry.bind(
            action_type="synthetic",
            context=ActionBindContext(
                authorization_mode=AUTHORIZATION_MODE_OPEN_OPERATOR,
                workspace_id="default",
                target_selector={},
            ),
        )


def test_crm_snapshot_revalidation_distinguishes_owner_loss_from_version_staleness() -> None:
    store = _CRMRecordStore({"crm-record-a": _record()})
    binder = CRMRecordTargetBinder(store)
    target = binder(_authenticated_context())
    assert (
        binder.revalidate_snapshot(
            target_ref=target.target_ref,
            operation_workspace_id="user-alice",
        )["crm_record_id"]
        == "crm-record-a"
    )

    store.records["crm-record-a"]["crm_version"] = 2
    with pytest.raises(ActionTargetBindingError, match=CRM_RECORD_TARGET_STALE):
        binder.revalidate_snapshot(
            target_ref=target.target_ref,
            operation_workspace_id="user-alice",
        )

    store.records["crm-record-a"] = _record(workspace_id="user-bob", owner_user_id="bob")
    with pytest.raises(ActionTargetBindingError, match=CRM_RECORD_TARGET_NOT_FOUND):
        binder.revalidate_snapshot(
            target_ref=target.target_ref,
            operation_workspace_id="user-alice",
        )


class D1eCRMExistingRecordActionPGTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir, schema_label="d1e_crm_action")
        self.store = ControlPlaneStore(self.runtime_dir / "sourcing_agent.db")
        self.writer = OperationRuntimeWriter(self.store, action_registry=_declared_registry())

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _seed_record(
        self,
        crm_record_id: str,
        *,
        workspace_id: str,
        owner_user_id: str,
    ) -> dict[str, Any]:
        return self.store.upsert_crm_record(
            {
                "crm_record_id": crm_record_id,
                "workspace_id": workspace_id,
                "owner_user_id": owner_user_id,
                "person_identity_key": f"person::{crm_record_id}",
                "display_name_cache": crm_record_id,
            }
        )

    def test_pg_bind_submit_replay_and_invalid_zero_write_matrix(self) -> None:
        owned = self._seed_record(
            "pg-owned",
            workspace_id="user-alice",
            owner_user_id="alice",
        )
        self._seed_record(
            "pg-foreign",
            workspace_id="user-bob",
            owner_user_id="bob",
        )
        registry = build_crm_existing_record_target_binder_registry(self.store)

        before_actions = self.store.repos.workflow_runtime.list_actions(workspace_id="user-alice", limit=100)
        before_owned = self.store.get_crm_record("pg-owned")
        before_tasks = self.store.list_crm_tasks(workspace_id="user-alice", limit=100)
        for record_id in ("pg-missing", "pg-foreign"):
            for action_type in CRM_ACTIONS:
                with self.assertRaisesRegex(ActionTargetBindingError, CRM_RECORD_TARGET_NOT_FOUND):
                    registry.bind(
                        action_type=action_type,
                        context=_authenticated_context(record_id),
                    )
        self.assertEqual(
            self.store.repos.workflow_runtime.list_actions(workspace_id="user-alice", limit=100),
            before_actions,
        )
        self.assertEqual(self.store.get_crm_record("pg-owned"), before_owned)
        self.assertEqual(self.store.list_crm_tasks(workspace_id="user-alice", limit=100), before_tasks)

        submitted: dict[str, Any] = {}
        for action_type in CRM_ACTIONS:
            target = registry.bind(
                action_type=action_type,
                context=_authenticated_context("pg-owned"),
            )
            result = self.writer.submit_action(
                action_type=action_type,
                workspace_id="user-alice",
                owner_bound_target_ref=target,
                input_payload=VALID_INPUTS[action_type],
                idempotency_key=f"d1e:{action_type}",
                actor="alice",
                source="test.d1e",
            )
            self.assertEqual(result.action["target_ref"]["crm_record_id"], "pg-owned")
            self.assertEqual(result.action["target_ref"]["crm_version"], owned["crm_version"])
            self.assertEqual(
                result.action["request_schema_digest"],
                EXPECTED_SCHEMA_DIGESTS[action_type],
            )
            submitted[action_type] = result.action

            replay_target = registry.bind(
                action_type=action_type,
                context=_authenticated_context("pg-owned"),
            )
            replay = self.writer.submit_action(
                action_type=action_type,
                workspace_id="user-alice",
                owner_bound_target_ref=replay_target,
                input_payload=VALID_INPUTS[action_type],
                idempotency_key=f"d1e:{action_type}",
            )
            self.assertEqual(replay.action["created_at"], result.action["created_at"])

        action_snapshot = self.store.repos.workflow_runtime.list_actions(workspace_id="user-alice", limit=100)
        target = registry.bind(
            action_type=ACTION_ADD_CRM_NOTE,
            context=_authenticated_context("pg-owned"),
        )
        with self.assertRaisesRegex(ActionRequestValidationError, "schema_validation_failed"):
            self.writer.submit_action(
                action_type=ACTION_ADD_CRM_NOTE,
                workspace_id="user-alice",
                owner_bound_target_ref=target,
                input_payload={"note": "valid", "crm_record_id": "forged"},
                idempotency_key="d1e:forged-input",
            )
        with self.assertRaisesRegex(ActionRequestValidationError, "target_must_be_owner_bound"):
            self.writer.submit_action(
                action_type=ACTION_ADD_CRM_NOTE,
                workspace_id="user-alice",
                target_ref={"crm_record_id": "pg-owned"},
                owner_bound_target_ref=target,
                input_payload={"note": "valid"},
                idempotency_key="d1e:forged-target",
            )
        self.assertEqual(
            self.store.repos.workflow_runtime.list_actions(workspace_id="user-alice", limit=100),
            action_snapshot,
        )
        self.assertEqual(self.store.get_crm_record("pg-owned"), before_owned)
        self.assertEqual(self.store.list_crm_tasks(workspace_id="user-alice", limit=100), before_tasks)
        self.assertEqual(set(submitted), set(CRM_ACTIONS))
