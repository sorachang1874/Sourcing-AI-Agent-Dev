from __future__ import annotations

import copy
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import psycopg
import pytest

from sourcing_agent.model_tool_runtime import ToolSpec
from sourcing_agent.operation_runtime import (
    ACTION_EXTERNAL_INTAKE,
    ACTION_SEARCH_PROJECTION,
    APPROVAL_REQUIRED,
    DEFAULT_ACTION_REGISTRY,
    DISPATCH_ADAPTER_PROJECTION_READ,
    ActionRegistry,
    ActionRequestSpec,
    ActionRequestValidationError,
    OperationRuntimeStateConflict,
    OperationRuntimeWriter,
    OwnerBoundTargetRef,
    operation_action_id,
    operation_retry_run_id_for,
    operation_run_id_for,
)
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin


def _request_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "input_payload": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "query": {"type": "string", "minLength": 1, "maxLength": 200},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            "target_ref": {
                "type": "object",
                "properties": {
                    "projection_id": {"type": "string", "minLength": 1, "maxLength": 200},
                },
                "required": ["projection_id"],
                "additionalProperties": False,
            },
        },
        "required": ["input_payload", "target_ref"],
        "additionalProperties": False,
    }


def _schema_spec(*, approval_policy: str = "not_required") -> ActionRequestSpec:
    return ActionRequestSpec(
        action_type="synthetic_projection_search",
        owner_module="projection_search_service",
        operation_type="projection_search",
        dispatch_adapter=DISPATCH_ADAPTER_PROJECTION_READ,
        request_schema=_request_schema(),
        request_schema_version="synthetic_projection_search_request_v1",
        target_ref_field_aliases=(("projection_id", ("serving_projection_id",)),),
        approval_policy=approval_policy,
        description="Synthetic D1c request-contract fixture.",
        display_label="Synthetic projection search",
        display_category="test",
    )


def _owner_target() -> OwnerBoundTargetRef:
    return OwnerBoundTargetRef(
        owner_module="projection_search_service",
        target_ref={"projection_id": "projection-a"},
    )


def test_owner_bound_target_ref_is_a_deep_immutable_snapshot() -> None:
    source: dict[str, Any] = {"projection_id": "projection-a", "nested": {"members": ["one"]}}
    target = OwnerBoundTargetRef(owner_module="projection_search_service", target_ref=source)
    source["projection_id"] = "mutated"
    source["nested"]["members"].append("two")

    assert target.target_ref["projection_id"] == "projection-a"
    assert target.target_ref["nested"]["members"] == ("one",)
    with pytest.raises(TypeError):
        target.target_ref["projection_id"] = "forbidden"  # type: ignore[index]


def test_action_request_spec_uses_tool_spec_schema_owner_and_canonical_digest() -> None:
    schema = _request_schema()
    reordered = {
        "additionalProperties": False,
        "required": ["input_payload", "target_ref"],
        "properties": {
            "target_ref": copy.deepcopy(schema["properties"]["target_ref"]),
            "input_payload": copy.deepcopy(schema["properties"]["input_payload"]),
        },
        "type": "object",
    }
    first = _schema_spec()
    second = replace(first, request_schema=reordered)
    tool = ToolSpec(
        name=first.action_type,
        description=first.description,
        input_schema=first.request_schema or {},
        schema_version=first.request_schema_version,
        approval_policy=first.approval_policy,
        budget_required=first.budget_required,
    )

    assert first.request_schema_digest == second.request_schema_digest == tool.input_schema_digest
    assert len(first.request_schema_digest) == 64
    schema["properties"]["input_payload"]["properties"]["query"]["maxLength"] = 1
    assert first.request_schema_digest == second.request_schema_digest
    with pytest.raises(TypeError):
        assert first.request_schema is not None
        first.request_schema["type"] = "string"  # type: ignore[index]

    valid_input, valid_target = first.validate_request(
        input_payload={"query": "platform", "limit": 10},
        target_ref={"projection_id": "projection-a"},
    )
    assert valid_input == {"query": "platform", "limit": 10}
    assert valid_target == {"projection_id": "projection-a"}


@pytest.mark.parametrize(
    "mutate,reason",
    [
        (lambda schema: schema.update({"oneOf": []}), "invalid action request schema"),
        (lambda schema: schema["properties"].pop("target_ref"), "invalid action request schema"),
        (
            lambda schema: schema["properties"]["input_payload"]["properties"].update(
                {"projection_id": {"type": "string"}}
            ),
            "dual owners",
        ),
        (
            lambda schema: schema["properties"]["input_payload"]["properties"].update(
                {"serving_projection_id": {"type": "string"}}
            ),
            "aliases cannot be caller-owned",
        ),
    ],
)
def test_action_request_spec_rejects_invalid_shape_and_owner_aliases(
    mutate: Callable[[dict[str, Any]], Any],
    reason: str,
) -> None:
    schema = _request_schema()
    mutate(schema)
    with pytest.raises(ValueError, match=reason):
        replace(_schema_spec(), request_schema=schema)


def test_action_request_spec_rejects_schema_version_partial_pairs() -> None:
    with pytest.raises(ValueError, match="schema-less action"):
        replace(_schema_spec(), request_schema=None)
    with pytest.raises(ValueError, match="request_schema_version"):
        replace(_schema_spec(), request_schema_version="")
    with pytest.raises(ValueError, match="request_schema_version"):
        replace(_schema_spec(), request_schema_version="v" * 129)
    with pytest.raises(ValueError, match="request_schema_version"):
        replace(_schema_spec(), request_schema_version="v/1")


def test_production_action_registry_remains_schema_less_and_unserved() -> None:
    records = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    assert len(records) == 15
    assert all(DEFAULT_ACTION_REGISTRY.spec_for(action_type).request_schema is None for action_type in records)
    assert all(DEFAULT_ACTION_REGISTRY.spec_for(action_type).request_schema_digest == "" for action_type in records)
    assert all(
        not {
            "request_schema",
            "request_schema_version",
            "request_schema_digest",
            "agent_tool_enabled",
            "served_tool_status",
        }
        & set(record)
        for record in records.values()
    )


class _DispatchPreflightProbe:
    def __init__(self) -> None:
        self.operation_runtime_writer = OperationRuntimeWriter(SimpleNamespace())
        self.handler_calls = 0

    @staticmethod
    def _operation_run_control_response_record(record: dict[str, Any]) -> dict[str, Any]:
        return record

    def _projection_handler(self, **_: Any) -> dict[str, Any]:
        self.handler_calls += 1
        return {"status": "completed", "module_state_mutated": False}

    def _operation_dispatch_adapter_bindings(self) -> dict[str, Callable[..., dict[str, Any]]]:
        return {DISPATCH_ADAPTER_PROJECTION_READ: self._projection_handler}


@pytest.mark.parametrize(
    "payload",
    [
        {"request_schema_version": "caller-v1"},
        {"metadata": {"request_schema_digest": "f" * 64}},
    ],
)
def test_submit_api_rejects_pin_overrides_before_binding_or_writer(payload: dict[str, Any]) -> None:
    class _SubmissionProbe:
        operation_runtime_writer = SimpleNamespace(submit_action=lambda **_: pytest.fail("writer must not be called"))

        @staticmethod
        def _bind_operation_projection_membership(**_: Any) -> dict[str, Any]:
            pytest.fail("binding must not be called")
            raise AssertionError("unreachable")

    result = SourcingOrchestrator.submit_operation_action(
        _SubmissionProbe(),
        {"action_type": ACTION_EXTERNAL_INTAKE, **payload},
    )
    assert result["status"] == "invalid"
    assert result["reason"].startswith("action_request_pin_fields_are_owner_reserved:")


def test_dispatch_pin_preflight_is_zero_write_and_matching_schema_less_path_stays_compatible() -> None:
    probe = _DispatchPreflightProbe()
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_SEARCH_PROJECTION)
    action = {
        "action_id": "action-a",
        "action_type": ACTION_SEARCH_PROJECTION,
        "workspace_id": "default",
        "owner_module": spec.owner_module,
        "operation_type": spec.operation_type,
        "input": {},
        "target_ref": {},
        "request_schema_version": "",
        "request_schema_digest": "",
    }
    operation_run = {
        "operation_run_id": "operation-a",
        "action_id": "action-a",
        "workspace_id": "default",
        "owner_module": spec.owner_module,
        "operation_type": spec.operation_type,
        "request_schema_version": "",
        "request_schema_digest": "",
    }
    success = SourcingOrchestrator._dispatch_operation_run_from_records(
        probe,
        operation_run=operation_run,
        action=action,
        actor="d1c-test",
    )
    assert success["status"] == "completed"
    assert probe.handler_calls == 1

    conflict = SourcingOrchestrator._dispatch_operation_run_from_records(
        probe,
        operation_run={**operation_run, "request_schema_digest": "f" * 64},
        action=action,
        actor="d1c-test",
    )
    assert conflict["status"] == "conflict"
    assert conflict["reason"] == "operation_run_request_schema_pin_conflict"
    assert conflict["module_state_mutated"] is False
    assert conflict["request_schema_revalidation_required"] is True
    assert probe.handler_calls == 1


class D1ActionRequestContractPGTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _writer(self, spec: ActionRequestSpec | None = None) -> OperationRuntimeWriter:
        registry = ActionRegistry({spec.action_type: spec}) if spec is not None else DEFAULT_ACTION_REGISTRY
        return OperationRuntimeWriter(self.store, action_registry=registry)

    def test_schema_defined_submit_validates_before_write_and_copies_physical_pins(self) -> None:
        spec = _schema_spec()
        writer = self._writer(spec)
        result = writer.submit_action(
            action_type=spec.action_type,
            workspace_id="workspace-a",
            owner_bound_target_ref=_owner_target(),
            input_payload={"query": "platform", "limit": 10},
            idempotency_key="schema-submit-a",
            actor="d1c-test",
            source="test.d1c",
        )

        self.assertEqual(result.action["request_schema_version"], spec.request_schema_version)
        self.assertEqual(result.action["request_schema_digest"], spec.request_schema_digest)
        self.assertEqual(result.operation_run["request_schema_version"], spec.request_schema_version)
        self.assertEqual(result.operation_run["request_schema_digest"], spec.request_schema_digest)
        self.assertEqual(result.action["metadata"]["request_schema_status"], "validated")
        self.assertFalse(result.action["metadata"]["request_schema_compatibility_hit"])
        self.assertEqual(result.events[0]["payload"]["request_schema_status"], "validated")

        replay = writer.submit_action(
            action_type=spec.action_type,
            workspace_id="workspace-a",
            owner_bound_target_ref=_owner_target(),
            input_payload={"query": "platform", "limit": 10},
            idempotency_key="schema-submit-a",
        )
        self.assertEqual(replay.action["created_at"], result.action["created_at"])
        self.assertEqual(replay.operation_run["created_at"], result.operation_run["created_at"])

        action_after_state = self.store.repos.workflow_runtime.update_action_state(
            result.action["action_id"],
            status="planned",
        )
        operation_after_state = self.store.repos.workflow_runtime.update_operation_state(
            result.operation_run["operation_run_id"],
            status="planned",
        )
        self.assertEqual(action_after_state["request_schema_digest"], spec.request_schema_digest)
        self.assertEqual(operation_after_state["request_schema_digest"], spec.request_schema_digest)

    def test_action_replay_schema_drift_and_unique_key_collision_are_typed_zero_write(self) -> None:
        spec = _schema_spec(approval_policy=APPROVAL_REQUIRED)
        writer = self._writer(spec)
        submitted = writer.submit_action(
            action_type=spec.action_type,
            workspace_id="workspace-action-drift",
            owner_bound_target_ref=_owner_target(),
            input_payload={"query": "platform"},
            idempotency_key="action-drift",
        )
        events_before = self.store.repos.workflow_runtime.list_operation_events(submitted.action["action_id"])
        changed_schema = _request_schema()
        changed_schema["properties"]["input_payload"]["properties"]["mode"] = {"type": "string"}
        drifted_writer = self._writer(
            replace(
                spec,
                request_schema=changed_schema,
                request_schema_version="synthetic_request_v2",
            )
        )

        with self.assertRaisesRegex(
            OperationRuntimeStateConflict,
            "operation_action_request_schema_pin_conflict",
        ):
            drifted_writer.submit_action(
                action_type=spec.action_type,
                workspace_id="workspace-action-drift",
                owner_bound_target_ref=_owner_target(),
                input_payload={"query": "platform"},
                idempotency_key="action-drift",
            )

        unchanged = self.store.repos.workflow_runtime.get_action(submitted.action["action_id"])
        self.assertEqual(unchanged["created_at"], submitted.action["created_at"])
        self.assertEqual(unchanged["updated_at"], submitted.action["updated_at"])
        self.assertEqual(unchanged["status"], "approval_required")
        self.assertEqual(
            self.store.repos.workflow_runtime.list_operation_events(submitted.action["action_id"]),
            events_before,
        )

        collision_workspace = "workspace-action-collision"
        collision_key = "action-collision"
        expected_action_id = operation_action_id(
            workspace_id=collision_workspace,
            action_type=spec.action_type,
            idempotency_key=collision_key,
        )
        alternate_action_id = f"alternate-{expected_action_id}"
        self.store.repos.workflow_runtime.upsert_action(
            action_id=alternate_action_id,
            workspace_id=collision_workspace,
            action_type=spec.action_type,
            owner_module=spec.owner_module,
            operation_type=spec.operation_type,
            target_ref={"projection_id": "projection-a"},
            input_payload={"query": "platform"},
            request_schema_version=spec.request_schema_version,
            request_schema_digest=spec.request_schema_digest,
            approval_status="required",
            approval_policy=spec.approval_policy,
            idempotency_key=collision_key,
            status="approval_required",
        )

        with self.assertRaisesRegex(
            OperationRuntimeStateConflict,
            "operation_action_idempotency_payload_conflict",
        ):
            writer.submit_action(
                action_type=spec.action_type,
                workspace_id=collision_workspace,
                owner_bound_target_ref=_owner_target(),
                input_payload={"query": "platform"},
                idempotency_key=collision_key,
            )

        self.assertFalse(self.store.repos.workflow_runtime.get_action(expected_action_id))
        self.assertEqual(
            [
                action["action_id"]
                for action in self.store.repos.workflow_runtime.list_actions(workspace_id=collision_workspace)
            ],
            [alternate_action_id],
        )
        self.assertEqual(
            self.store.repos.workflow_runtime.list_operation_events(expected_action_id),
            [],
        )

    def test_invalid_or_caller_pinned_request_is_zero_write(self) -> None:
        spec = _schema_spec()
        writer = self._writer(spec)
        invalid_calls = (
            lambda: writer.submit_action(
                action_type=spec.action_type,
                workspace_id="workspace-invalid",
                owner_bound_target_ref=_owner_target(),
                input_payload={"query": "platform", "extra": True},
                idempotency_key="invalid-extra",
            ),
            lambda: writer.submit_action(
                action_type=spec.action_type,
                workspace_id="workspace-invalid",
                target_ref={"projection_id": "caller-owned"},
                owner_bound_target_ref=_owner_target(),
                input_payload={"query": "platform"},
                idempotency_key="invalid-target",
            ),
            lambda: writer.submit_action(
                action_type=spec.action_type,
                workspace_id="workspace-invalid",
                owner_bound_target_ref=_owner_target(),
                input_payload={"query": "platform"},
                idempotency_key="invalid-pin",
                metadata={"request_schema_digest": "f" * 64},
            ),
            lambda: writer.submit_action(
                action_type=spec.action_type,
                workspace_id="workspace-invalid",
                input_payload={"query": "platform"},
                idempotency_key="missing-owner-target",
            ),
            lambda: writer.submit_action(
                action_type=spec.action_type,
                workspace_id="workspace-invalid",
                owner_bound_target_ref=OwnerBoundTargetRef(
                    owner_module="foreign_owner",
                    target_ref={"projection_id": "projection-a"},
                ),
                input_payload={"query": "platform"},
                idempotency_key="foreign-owner-target",
            ),
        )
        for invoke in invalid_calls:
            with self.assertRaises(ActionRequestValidationError):
                invoke()
        self.assertEqual(
            self.store.repos.workflow_runtime.list_actions(workspace_id="workspace-invalid"),
            [],
        )

        schema_less_writer = self._writer()
        with self.assertRaisesRegex(
            ActionRequestValidationError,
            "action_request_pin_fields_are_owner_reserved",
        ):
            schema_less_writer.submit_action(
                action_type=ACTION_EXTERNAL_INTAKE,
                workspace_id="workspace-invalid-schema-less",
                idempotency_key="invalid-schema-less-metadata-pin",
                metadata={"request_schema_version": "caller-v1"},
            )
        with self.assertRaisesRegex(
            ActionRequestValidationError,
            "schema_less_action_cannot_accept_owner_bound_target",
        ):
            schema_less_writer.submit_action(
                action_type=ACTION_EXTERNAL_INTAKE,
                workspace_id="workspace-invalid-schema-less",
                idempotency_key="schema-less-owner-target",
                owner_bound_target_ref=OwnerBoundTargetRef(
                    owner_module="external_intake",
                    target_ref={"legacy_target": "preserved"},
                ),
            )
        self.assertEqual(
            self.store.repos.workflow_runtime.list_actions(workspace_id="workspace-invalid-schema-less"),
            [],
        )

    def test_schema_less_bridge_uses_empty_physical_pin_and_durable_hit_marker(self) -> None:
        result = self._writer().submit_action(
            action_type=ACTION_EXTERNAL_INTAKE,
            workspace_id="workspace-legacy",
            target_ref={"legacy_target": "preserved"},
            input_payload={"legacy_option": True},
            idempotency_key="schema-less-a",
            actor="d1c-test",
            source="api.operation_action_submit",
        )
        self.assertEqual(result.action["request_schema_version"], "")
        self.assertEqual(result.action["request_schema_digest"], "")
        self.assertEqual(result.operation_run["request_schema_version"], "")
        self.assertEqual(result.operation_run["request_schema_digest"], "")
        self.assertEqual(result.action["metadata"]["request_schema_status"], "schema_less_compatibility")
        self.assertTrue(result.action["metadata"]["request_schema_compatibility_hit"])
        self.assertTrue(result.events[0]["payload"]["request_schema_compatibility_hit"])

    def test_approve_copies_pin_and_registry_drift_fails_before_first_write(self) -> None:
        spec = _schema_spec(approval_policy=APPROVAL_REQUIRED)
        writer = self._writer(spec)
        submitted = writer.submit_action(
            action_type=spec.action_type,
            workspace_id="workspace-approval",
            owner_bound_target_ref=_owner_target(),
            input_payload={"query": "platform"},
            idempotency_key="approval-a",
        )
        self.assertFalse(submitted.operation_run)
        approved = writer.approve_action(action_id=submitted.action["action_id"], actor="approver")
        self.assertEqual(approved.operation_run["request_schema_digest"], spec.request_schema_digest)

        pending = writer.submit_action(
            action_type=spec.action_type,
            workspace_id="workspace-approval",
            owner_bound_target_ref=_owner_target(),
            input_payload={"query": "different"},
            idempotency_key="approval-drift",
        )
        changed_schema = _request_schema()
        changed_schema["properties"]["input_payload"]["properties"]["mode"] = {"type": "string"}
        drifted_spec = replace(spec, request_schema=changed_schema, request_schema_version="synthetic_request_v2")
        drifted_writer = self._writer(drifted_spec)
        events_before = self.store.repos.workflow_runtime.list_operation_events(pending.action["action_id"])
        with self.assertRaisesRegex(OperationRuntimeStateConflict, "operation_action_request_schema_pin_conflict"):
            drifted_writer.approve_action(action_id=pending.action["action_id"], actor="approver")
        unchanged = self.store.repos.workflow_runtime.get_action(pending.action["action_id"])
        self.assertEqual(unchanged["status"], "approval_required")
        self.assertEqual(unchanged["approval_status"], "required")
        self.assertEqual(
            self.store.repos.workflow_runtime.list_operation_events(pending.action["action_id"]),
            events_before,
        )

    def test_approve_preflights_alternate_run_unique_key_before_action_write(self) -> None:
        spec = _schema_spec(approval_policy=APPROVAL_REQUIRED)
        writer = self._writer(spec)
        collision_cases = (
            ("same-pin", spec.request_schema_version, spec.request_schema_digest, "approval_run_identity_conflict"),
            ("pin-mismatch", "other_request_v1", "f" * 64, "request_schema_pin_conflict"),
        )
        for suffix, version, digest, expected_reason in collision_cases:
            with self.subTest(suffix=suffix):
                submitted = writer.submit_action(
                    action_type=spec.action_type,
                    workspace_id=f"workspace-approval-{suffix}",
                    owner_bound_target_ref=_owner_target(),
                    input_payload={"query": suffix},
                    idempotency_key=f"approval-{suffix}",
                )
                action = submitted.action
                expected_run_id = operation_run_id_for(
                    action_id=action["action_id"],
                    operation_type=action["operation_type"],
                    idempotency_key=action["idempotency_key"],
                )
                self.store.repos.workflow_runtime.upsert_operation(
                    operation_run_id=f"alternate-{expected_run_id}",
                    workspace_id=action["workspace_id"],
                    action_id=action["action_id"],
                    owner_module=action["owner_module"],
                    operation_type=action["operation_type"],
                    request_schema_version=version,
                    request_schema_digest=digest,
                    idempotency_key=action["idempotency_key"],
                )
                events_before = self.store.repos.workflow_runtime.list_operation_events(action["action_id"])

                with self.assertRaisesRegex(OperationRuntimeStateConflict, expected_reason):
                    writer.approve_action(action_id=action["action_id"], actor="approver")

                unchanged = self.store.repos.workflow_runtime.get_action(action["action_id"])
                self.assertEqual(unchanged["status"], "approval_required")
                self.assertEqual(unchanged["approval_status"], "required")
                self.assertFalse(self.store.repos.workflow_runtime.get_operation(expected_run_id))
                self.assertEqual(
                    self.store.repos.workflow_runtime.list_operation_events(action["action_id"]),
                    events_before,
                )

    def test_retry_child_copies_pin_and_registry_drift_is_zero_requeue(self) -> None:
        spec = _schema_spec()
        writer = self._writer(spec)
        submitted = writer.submit_action(
            action_type=spec.action_type,
            workspace_id="workspace-retry",
            owner_bound_target_ref=_owner_target(),
            input_payload={"query": "platform"},
            idempotency_key="retry-a",
        )
        action = self.store.repos.workflow_runtime.update_action_state(submitted.action["action_id"], status="failed")
        operation = self.store.repos.workflow_runtime.update_operation_state(
            submitted.operation_run["operation_run_id"],
            status="failed",
        )
        retried = writer.retry_operation(
            operation_run_id=operation["operation_run_id"],
            idempotency_key="retry-child-a",
        )
        self.assertEqual(retried["operation_run"]["request_schema_digest"], spec.request_schema_digest)

        second = writer.submit_action(
            action_type=spec.action_type,
            workspace_id="workspace-retry",
            owner_bound_target_ref=_owner_target(),
            input_payload={"query": "second"},
            idempotency_key="retry-drift",
        )
        self.store.repos.workflow_runtime.update_action_state(second.action["action_id"], status="failed")
        failed_parent = self.store.repos.workflow_runtime.update_operation_state(
            second.operation_run["operation_run_id"],
            status="failed",
        )
        changed_schema = _request_schema()
        changed_schema["properties"]["input_payload"]["properties"]["mode"] = {"type": "string"}
        drifted_writer = self._writer(
            replace(spec, request_schema=changed_schema, request_schema_version="synthetic_request_v2")
        )
        retry_key = "retry-drift-child"
        retry_run_id = operation_retry_run_id_for(
            parent_operation_run_id=failed_parent["operation_run_id"],
            idempotency_key=retry_key,
        )
        with self.assertRaisesRegex(OperationRuntimeStateConflict, "operation_action_request_schema_pin_conflict"):
            drifted_writer.retry_operation(
                operation_run_id=failed_parent["operation_run_id"],
                idempotency_key=retry_key,
            )
        unchanged = self.store.repos.workflow_runtime.get_action(second.action["action_id"])
        self.assertEqual(unchanged["status"], "failed")
        self.assertFalse(self.store.repos.workflow_runtime.get_operation(retry_run_id))
        self.assertEqual(action["request_schema_digest"], spec.request_schema_digest)

    def test_retry_preflights_alternate_child_unique_key_before_requeue(self) -> None:
        spec = _schema_spec()
        writer = self._writer(spec)
        collision_cases = (
            ("same-pin", spec.request_schema_version, spec.request_schema_digest, "retry_identity_conflict"),
            ("pin-mismatch", "other_request_v1", "f" * 64, "request_schema_pin_conflict"),
        )
        for suffix, version, digest, expected_reason in collision_cases:
            with self.subTest(suffix=suffix):
                submitted = writer.submit_action(
                    action_type=spec.action_type,
                    workspace_id=f"workspace-retry-{suffix}",
                    owner_bound_target_ref=_owner_target(),
                    input_payload={"query": suffix},
                    idempotency_key=f"retry-{suffix}",
                )
                action = self.store.repos.workflow_runtime.update_action_state(
                    submitted.action["action_id"],
                    status="failed",
                )
                parent = self.store.repos.workflow_runtime.update_operation_state(
                    submitted.operation_run["operation_run_id"],
                    status="failed",
                )
                retry_key = f"retry-child-{suffix}"
                retry_run_id = operation_retry_run_id_for(
                    parent_operation_run_id=parent["operation_run_id"],
                    idempotency_key=retry_key,
                )
                persisted_retry_key = f"operation_retry:{parent['operation_run_id']}:{retry_run_id}"
                self.store.repos.workflow_runtime.upsert_operation(
                    operation_run_id=f"alternate-{retry_run_id}",
                    workspace_id=action["workspace_id"],
                    action_id=action["action_id"],
                    owner_module=action["owner_module"],
                    operation_type=action["operation_type"],
                    request_schema_version=version,
                    request_schema_digest=digest,
                    idempotency_key=persisted_retry_key,
                )
                events_before = self.store.repos.workflow_runtime.list_operation_events(parent["operation_run_id"])

                with self.assertRaisesRegex(OperationRuntimeStateConflict, expected_reason):
                    writer.retry_operation(
                        operation_run_id=parent["operation_run_id"],
                        idempotency_key=retry_key,
                    )

                unchanged = self.store.repos.workflow_runtime.get_action(action["action_id"])
                self.assertEqual(unchanged["status"], "failed")
                self.assertFalse(self.store.repos.workflow_runtime.get_operation(retry_run_id))
                self.assertEqual(
                    self.store.repos.workflow_runtime.list_operation_events(parent["operation_run_id"]),
                    events_before,
                )

    def test_submit_preflights_same_pin_unrelated_run_before_action_write(self) -> None:
        writer = self._writer()
        spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_EXTERNAL_INTAKE)
        workspace_id = "workspace-submit-collision"
        idempotency_key = "submit-collision"
        self.store.repos.workflow_runtime.upsert_operation(
            operation_run_id="alternate-submit-run",
            workspace_id=workspace_id,
            action_id="unrelated-action",
            owner_module=spec.owner_module,
            operation_type=spec.operation_type,
            request_schema_version="",
            request_schema_digest="",
            idempotency_key=idempotency_key,
        )

        with self.assertRaisesRegex(
            OperationRuntimeStateConflict,
            "operation_run_idempotency_payload_conflict",
        ):
            writer.submit_action(
                action_type=ACTION_EXTERNAL_INTAKE,
                workspace_id=workspace_id,
                idempotency_key=idempotency_key,
            )

        self.assertEqual(
            self.store.repos.workflow_runtime.list_actions(workspace_id=workspace_id),
            [],
        )

    def test_native_upserts_reject_request_pin_identity_drift(self) -> None:
        spec = _schema_spec()
        writer = self._writer(spec)
        submitted = writer.submit_action(
            action_type=spec.action_type,
            workspace_id="workspace-storage",
            owner_bound_target_ref=_owner_target(),
            input_payload={"query": "platform"},
            idempotency_key="storage-a",
        )
        action = submitted.action
        with self.assertRaisesRegex(RuntimeError, "immutable request schema pin collision"):
            self.store.repos.workflow_runtime.upsert_action(
                action_id=action["action_id"],
                workspace_id=action["workspace_id"],
                conversation_id=action["conversation_id"],
                action_type=action["action_type"],
                owner_module=action["owner_module"],
                operation_type=action["operation_type"],
                target_ref=action["target_ref"],
                input_payload=action["input"],
                request_schema_version=action["request_schema_version"],
                request_schema_digest="f" * 64,
                approval_status=action["approval_status"],
                approval_policy=action["approval_policy"],
                budget=action["budget"],
                idempotency_key=action["idempotency_key"],
                status=action["status"],
                metadata=action["metadata"],
            )

        operation = submitted.operation_run
        with self.assertRaisesRegex(RuntimeError, "immutable request schema pin collision"):
            self.store.repos.workflow_runtime.upsert_operation(
                operation_run_id=operation["operation_run_id"],
                workspace_id=operation["workspace_id"],
                action_id=operation["action_id"],
                owner_module=operation["owner_module"],
                operation_type=operation["operation_type"],
                request_schema_version=operation["request_schema_version"],
                request_schema_digest="f" * 64,
                status=operation["status"],
                progress=operation["progress"],
                workflow_ref=operation["workflow_ref"],
                cost_budget=operation["cost_budget"],
                idempotency_key=operation["idempotency_key"],
                result_ref=operation["result_ref"],
                metadata=operation["metadata"],
            )

    def test_database_checks_reject_invalid_pin_pairs_on_both_tables(self) -> None:
        result = self._writer().submit_action(
            action_type=ACTION_EXTERNAL_INTAKE,
            workspace_id="workspace-check",
            idempotency_key="check-a",
        )
        adapter = self.store._control_plane_postgres
        invalid_pairs = (
            ("invalid-without-digest", ""),
            ("valid_v1", "F" * 64),
            ("v" * 129, "f" * 64),
            ("\tvalid_v1\t", "f" * 64),
            ("valid_v1\n", "f" * 64),
            ("valid_v1", ("f" * 64) + "\n"),
            ("", "f" * 64),
        )
        records = (
            ("agent_actions", "action_id", result.action["action_id"]),
            ("operation_runs", "operation_run_id", result.operation_run["operation_run_id"]),
        )
        for table_name, id_column, record_id in records:
            for version, digest in invalid_pairs:
                with self.subTest(table_name=table_name, version=version, digest=digest[:4]):
                    with self.assertRaises(psycopg.errors.CheckViolation):
                        with adapter._connect() as connection:  # noqa: SLF001
                            with connection.cursor() as cursor:
                                cursor.execute(
                                    psycopg.sql.SQL(
                                        "UPDATE {} SET request_schema_version = %s, "
                                        "request_schema_digest = %s WHERE {} = %s"
                                    ).format(
                                        psycopg.sql.Identifier(table_name),
                                        psycopg.sql.Identifier(id_column),
                                    ),
                                    (version, digest, record_id),
                                )
        persisted = self.store.repos.workflow_runtime.get_action(result.action["action_id"])
        self.assertEqual(persisted["request_schema_version"], "")
        self.assertEqual(persisted["request_schema_digest"], "")
        persisted_run = self.store.repos.workflow_runtime.get_operation(result.operation_run["operation_run_id"])
        self.assertEqual(persisted_run["request_schema_version"], "")
        self.assertEqual(persisted_run["request_schema_digest"], "")
