from __future__ import annotations

import ast
import hashlib
import json
from collections import Counter
from dataclasses import fields
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import sourcing_agent.operation_runtime as operation_runtime
from sourcing_agent.durable_runtime import (
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
    ACTIVITY_SPINE_LEGACY_INTERNAL,
    DEFAULT_COMMAND_OWNER_REGISTRY,
    EXCEL_INTAKE_RUN_COMMAND_TYPE,
    workflow_command_activity_spine_policy,
    workflow_command_control_policy,
)
from sourcing_agent.operation_runtime import DEFAULT_ACTION_REGISTRY, OperationRuntimeWriter
from sourcing_agent.orchestrator import SourcingOrchestrator

REPO_ROOT = Path(__file__).resolve().parents[1]
OPERATION_RUNTIME_PATH = REPO_ROOT / "src" / "sourcing_agent" / "operation_runtime.py"
ORCHESTRATOR_PATH = REPO_ROOT / "src" / "sourcing_agent" / "orchestrator.py"

EXPECTED_ACTION_COMMAND_SURFACE = {
    "plan_acquisition": (),
    "start_acquisition_run": ("acquisition.run.create",),
    "fetch_profile_sample": ("linkedin.profile_fetch.activity.run",),
    "continue_acquisition_run": (
        "linkedin.discovery_query.run",
        "linkedin.profile_fetch.activity.run",
        "linkedin.profile_fetch.provider.fetch",
        "linkedin.profile_terminal.admit",
        "projection.profile_admission.apply",
        "projection.person_search_index.build",
        "collection.authoritative.merge",
    ),
    "search_projection": (),
    "filter_projection": (),
    "add_to_crm": ("crm.record.add_from_projection",),
    "set_crm_stage": ("crm.record.update",),
    "add_crm_note": ("crm.note.add",),
    "create_crm_task": ("crm.task.create",),
    "enrich_person_public_web": ("crm.public_web.queue_batch",),
    "refresh_company_public_web_assets": ("company.public_web.refresh",),
    "promote_person_assertion": (),
    "export_candidates": ("export.projection.generate", "export.crm_public_web.generate"),
    "external_intake": ("excel.intake.run",),
}

EXPECTED_ACTION_DEFAULT_COMMANDS = {
    "plan_acquisition": "",
    "start_acquisition_run": "acquisition.run.create",
    "fetch_profile_sample": "linkedin.profile_fetch.activity.run",
    "continue_acquisition_run": "",
    "search_projection": "",
    "filter_projection": "",
    "add_to_crm": "crm.record.add_from_projection",
    "set_crm_stage": "crm.record.update",
    "add_crm_note": "crm.note.add",
    "create_crm_task": "crm.task.create",
    "enrich_person_public_web": "crm.public_web.queue_batch",
    "refresh_company_public_web_assets": "company.public_web.refresh",
    "promote_person_assertion": "",
    "export_candidates": "export.projection.generate",
    "external_intake": "excel.intake.run",
}

EXPECTED_ACTION_DISPATCH_ADAPTERS = {
    "plan_acquisition": "",
    "start_acquisition_run": operation_runtime.DISPATCH_ADAPTER_AGENT_CALLABLE_WORKFLOW_COMMAND,
    "fetch_profile_sample": operation_runtime.DISPATCH_ADAPTER_AGENT_CALLABLE_WORKFLOW_COMMAND,
    "continue_acquisition_run": operation_runtime.DISPATCH_ADAPTER_AGENT_CALLABLE_WORKFLOW_COMMAND,
    "search_projection": operation_runtime.DISPATCH_ADAPTER_PROJECTION_READ,
    "filter_projection": operation_runtime.DISPATCH_ADAPTER_PROJECTION_READ,
    "add_to_crm": operation_runtime.DISPATCH_ADAPTER_CRM_WRITER,
    "set_crm_stage": operation_runtime.DISPATCH_ADAPTER_CRM_WRITER,
    "add_crm_note": operation_runtime.DISPATCH_ADAPTER_CRM_WRITER,
    "create_crm_task": operation_runtime.DISPATCH_ADAPTER_CRM_WRITER,
    "enrich_person_public_web": operation_runtime.DISPATCH_ADAPTER_PERSON_PUBLIC_WEB,
    "refresh_company_public_web_assets": operation_runtime.DISPATCH_ADAPTER_AGENT_CALLABLE_WORKFLOW_COMMAND,
    "promote_person_assertion": "",
    "export_candidates": operation_runtime.DISPATCH_ADAPTER_EXPORT,
    "external_intake": "",
}

SUBMIT_ACTION_CALL_INVENTORY_SHA256 = "d2d65c9ca1ea86d4784ba8ad220ef7c2f695f75e02f7e41f3e2ca8c672a88ce6"


def _class_method(tree: ast.Module, class_name: str, method_name: str) -> ast.FunctionDef:
    class_nodes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name]
    assert len(class_nodes) == 1, f"expected one {class_name}, found {len(class_nodes)}"
    method_nodes = [
        node for node in class_nodes[0].body if isinstance(node, ast.FunctionDef) and node.name == method_name
    ]
    assert len(method_nodes) == 1, f"expected one {class_name}.{method_name}, found {len(method_nodes)}"
    return method_nodes[0]


def _dotted_name(node: ast.AST | None) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def _unparse_optional(node: ast.expr | None) -> str | None:
    return ast.unparse(node) if node is not None else None


def _required_argument_contract(argument: ast.arg) -> tuple[str, str | None]:
    return argument.arg, _unparse_optional(argument.annotation)


def _argument_contract(argument: ast.arg | None) -> tuple[str, str | None] | None:
    return _required_argument_contract(argument) if argument is not None else None


def _signature_contract(method: ast.FunctionDef) -> dict[str, Any]:
    return {
        "positional_only": tuple(_argument_contract(argument) for argument in method.args.posonlyargs),
        "positional": tuple(_argument_contract(argument) for argument in method.args.args),
        "vararg": _argument_contract(method.args.vararg),
        "keyword_only": tuple(
            (
                *_required_argument_contract(argument),
                _unparse_optional(default),
            )
            for argument, default in zip(method.args.kwonlyargs, method.args.kw_defaults, strict=True)
        ),
        "kwarg": _argument_contract(method.args.kwarg),
        "returns": _unparse_optional(method.returns),
        "decorators": tuple(ast.unparse(decorator) for decorator in method.decorator_list),
    }


def _call_inventory(method: ast.FunctionDef) -> tuple[str, ...]:
    calls = sorted(
        (node for node in ast.walk(method) if isinstance(node, ast.Call)),
        key=lambda node: (
            node.lineno,
            node.col_offset,
            node.end_lineno or node.lineno,
            node.end_col_offset or node.col_offset,
        ),
    )
    return tuple(ast.dump(node, annotate_fields=True, include_attributes=False) for node in calls)


def _call_inventory_digest(method: ast.FunctionDef) -> str:
    serialized = json.dumps(_call_inventory(method), ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _call_inventory_roots(method: ast.FunctionDef) -> tuple[str, ...]:
    return tuple(
        ast.unparse(node.func)
        for node in sorted(
            (candidate for candidate in ast.walk(method) if isinstance(candidate, ast.Call)),
            key=lambda candidate: (
                candidate.lineno,
                candidate.col_offset,
                candidate.end_lineno or candidate.lineno,
                candidate.end_col_offset or candidate.col_offset,
            ),
        )
    )


def _action_constants() -> dict[str, str]:
    return {
        name: value
        for name, value in vars(operation_runtime).items()
        if name.startswith("ACTION_") and isinstance(value, str)
    }


def _surface_inventory() -> dict[str, Any]:
    records = DEFAULT_ACTION_REGISTRY.to_record()
    command_refs = [
        command_type for record in records.values() for command_type in record["allowed_workflow_command_types"]
    ]
    return {
        "action_types": frozenset(records),
        "actions_with_commands": frozenset(
            action_type for action_type, record in records.items() if record["allowed_workflow_command_types"]
        ),
        "command_refs": tuple(command_refs),
        "unique_command_types": frozenset(command_refs),
    }


class _WorkflowRuntimeRepoProbe:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def upsert_action(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("upsert_action", dict(kwargs)))
        return {
            "action_id": kwargs["action_id"],
            "workspace_id": kwargs["workspace_id"],
            "conversation_id": kwargs["conversation_id"],
            "action_type": kwargs["action_type"],
            "owner_module": kwargs["owner_module"],
            "operation_type": kwargs["operation_type"],
            "target_ref": dict(kwargs["target_ref"]),
            "input": dict(kwargs["input_payload"]),
            "request_schema_version": kwargs["request_schema_version"],
            "request_schema_digest": kwargs["request_schema_digest"],
            "approval_status": kwargs["approval_status"],
            "approval_policy": kwargs["approval_policy"],
            "budget": dict(kwargs["budget"]),
            "idempotency_key": kwargs["idempotency_key"],
            "status": kwargs["status"],
            "metadata": dict(kwargs["metadata"]),
        }

    def get_action(self, action_id: str) -> dict[str, Any]:
        self.calls.append(("get_action", {"action_id": action_id}))
        return {}

    def get_action_by_idempotency(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("get_action_by_idempotency", dict(kwargs)))
        return {}

    def get_operation(self, operation_run_id: str) -> dict[str, Any]:
        self.calls.append(("get_operation", {"operation_run_id": operation_run_id}))
        return {}

    def get_operation_by_idempotency(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("get_operation_by_idempotency", dict(kwargs)))
        return {}

    def upsert_operation(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("upsert_operation", dict(kwargs)))
        return dict(kwargs)

    def append_operation_event(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("append_operation_event", dict(kwargs)))
        return {"event_id": f"event-{len(self.calls)}", **dict(kwargs)}


class _FailClosedRepoNamespace:
    def __init__(self, workflow_runtime: _WorkflowRuntimeRepoProbe) -> None:
        self.workflow_runtime = workflow_runtime

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"submit_action accessed unexpected repository/outbox surface: {name}")


class _FailClosedStoreProbe:
    def __init__(self, workflow_runtime: _WorkflowRuntimeRepoProbe) -> None:
        self.repos = _FailClosedRepoNamespace(workflow_runtime)

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"submit_action accessed unexpected store callback/outbox surface: {name}")


class _FailClosedOperationRuntimeWriter(OperationRuntimeWriter):
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"submit_action invoked unexpected writer callback/outbox hook: {name}")


class _DispatchOperationRuntimeWriterProbe:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def validate_persisted_action_request(self, **kwargs: Any) -> None:
        self.calls.append(("validate_persisted_action_request", dict(kwargs)))

    def record_schema_less_compatibility_observation(self, **kwargs: Any) -> None:
        self.calls.append(("record_schema_less_compatibility_observation", dict(kwargs)))


class _DispatchProbe:
    _operation_dispatch_adapter_bindings = SourcingOrchestrator._operation_dispatch_adapter_bindings

    def __init__(self) -> None:
        self.operation_runtime_writer = _DispatchOperationRuntimeWriterProbe()

    @staticmethod
    def _operation_run_control_response_record(record: dict[str, Any]) -> dict[str, Any]:
        return record

    @staticmethod
    def _dispatch_projection_read_operation(**_: Any) -> dict[str, Any]:
        return {"adapter": "projection_read"}

    @staticmethod
    def _dispatch_person_public_web_enrichment_operation(**_: Any) -> dict[str, Any]:
        return {"adapter": "person_public_web"}

    @staticmethod
    def _dispatch_export_candidates_operation(**_: Any) -> dict[str, Any]:
        return {"adapter": "export"}

    @staticmethod
    def _dispatch_agent_callable_workflow_command_operation(**_: Any) -> dict[str, Any]:
        return {"adapter": "agent_callable_workflow_command"}

    @staticmethod
    def _dispatch_crm_writer_operation(**_: Any) -> dict[str, Any]:
        return {"adapter": "crm_writer"}


def _plan_probe() -> SimpleNamespace:
    return SimpleNamespace(
        _agent_callable_workflow_command_types_for_action=(
            SourcingOrchestrator._agent_callable_workflow_command_types_for_action
        ),
        _acquisition_decomposition_downstream_command_types=lambda: ["intent.resolve"],
    )


def test_action_request_spec_and_registry_record_freeze_the_schema_foundation_surface() -> None:
    assert operation_runtime.ActionSpec is operation_runtime.ActionRequestSpec
    assert tuple(field.name for field in fields(operation_runtime.ActionSpec)) == (
        "action_type",
        "owner_module",
        "operation_type",
        "dispatch_adapter",
        "request_schema",
        "request_schema_version",
        "target_ref_field_aliases",
        "approval_policy",
        "budget_required",
        "description",
        "display_label",
        "display_category",
        "allowed_workflow_command_types",
        "default_workflow_command_type",
    )

    action_constants = _action_constants()
    records = DEFAULT_ACTION_REGISTRY.to_record()
    assert set(records) == set(action_constants.values())
    assert len(records) == len(action_constants)
    assert {
        action_type: tuple(record["allowed_workflow_command_types"]) for action_type, record in records.items()
    } == EXPECTED_ACTION_COMMAND_SURFACE
    assert {
        action_type: record["default_workflow_command_type"] for action_type, record in records.items()
    } == EXPECTED_ACTION_DEFAULT_COMMANDS
    assert {
        action_type: DEFAULT_ACTION_REGISTRY.spec_for(action_type).dispatch_adapter for action_type in records
    } == EXPECTED_ACTION_DISPATCH_ADAPTERS
    assert all(
        DEFAULT_ACTION_REGISTRY.spec_for(action_type).request_schema is None
        and DEFAULT_ACTION_REGISTRY.spec_for(action_type).request_schema_version == ""
        and DEFAULT_ACTION_REGISTRY.spec_for(action_type).request_schema_digest == ""
        for action_type in records
    )

    base_record_keys = {
        "owner_module",
        "operation_type",
        "approval_policy",
        "budget_required",
        "description",
        "display_contract",
        "allowed_workflow_command_types",
        "default_workflow_command_type",
        "workflow_command_exposure_gate",
        "workflow_command_exposure_status",
    }
    compact_records = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    assert set(compact_records) == set(records)
    for action_type, record in compact_records.items():
        assert set(record) == base_record_keys, action_type
        assert not {
            "request_schema",
            "request_schema_version",
            "request_schema_digest",
            "dispatch_adapter",
            "model_safe_result_schema",
            "agent_tool_enabled",
            "served_tool_status",
        } & set(record)

    inventory = _surface_inventory()
    assert inventory["action_types"] == frozenset(action_constants.values())
    assert inventory["actions_with_commands"] <= inventory["action_types"]
    assert set(inventory["command_refs"]) == inventory["unique_command_types"]
    assert {command_type: count for command_type, count in Counter(inventory["command_refs"]).items() if count > 1} == {
        "linkedin.profile_fetch.activity.run": 2
    }


def test_action_command_contracts_share_owner_activity_and_fail_closed_control_sources() -> None:
    records = DEFAULT_ACTION_REGISTRY.to_record()
    owner_registry = DEFAULT_COMMAND_OWNER_REGISTRY.to_record()

    for action_type, record in records.items():
        command_types = list(record["allowed_workflow_command_types"])
        command_contracts = list(record["allowed_workflow_command_contracts"])
        assert [contract["command_type"] for contract in command_contracts] == command_types, action_type
        assert SourcingOrchestrator._agent_callable_workflow_command_types_for_action(action_type) == set(command_types)
        for command_type, contract in zip(command_types, command_contracts, strict=True):
            owner = owner_registry[command_type]
            activity_policy = workflow_command_activity_spine_policy(command_type=command_type, owner=owner)
            control_policy = workflow_command_control_policy(command_type=command_type, owner=owner)
            assert contract["owner"] == owner
            assert contract["activity_spine_policy"] == activity_policy.to_record()
            assert contract["control_policy"] == control_policy.to_record()
            assert activity_policy.agent_callable is True
            assert activity_policy.requirement != ACTIVITY_SPINE_LEGACY_INTERNAL
            assert activity_policy.fallback_status == "fail_closed"
            assert control_policy.to_record()["fallback_status"] == "fail_closed"
            assert contract["agent_exposure_gate"] == (
                "operation_runtime.ActionRegistry.allowed_workflow_command_types"
            )
            assert contract["agent_exposure_status"] == "action_registry_allowlisted"

    assert SourcingOrchestrator._agent_callable_workflow_command_types_for_action("unknown_action") == set()


def test_submit_action_ast_freezes_keyword_only_surface_and_write_order() -> None:
    source = OPERATION_RUNTIME_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    method = _class_method(tree, "OperationRuntimeWriter", "submit_action")

    expected_signature = {
        "positional_only": (),
        "positional": (("self", None),),
        "vararg": None,
        "keyword_only": (
            ("action_type", "str", None),
            ("workspace_id", "str", "'default'"),
            ("conversation_id", "str", "''"),
            ("target_ref", "dict[str, Any] | None", "None"),
            ("owner_bound_target_ref", "OwnerBoundTargetRef | None", "None"),
            ("input_payload", "dict[str, Any] | None", "None"),
            ("budget", "dict[str, Any] | None", "None"),
            ("idempotency_key", "str", "''"),
            ("actor", "str", "'operation_runtime'"),
            ("source", "str", "'operation_runtime'"),
            ("metadata", "dict[str, Any] | None", "None"),
        ),
        "kwarg": None,
        "returns": "OperationSubmissionResult",
        "decorators": (),
    }
    assert _signature_contract(method) == expected_signature

    call_inventory = _call_inventory(method)
    assert len(call_inventory) == 33
    assert _call_inventory_digest(method) == SUBMIT_ACTION_CALL_INVENTORY_SHA256, _call_inventory_roots(method)
    assert _call_inventory_roots(method).count("self.record_schema_less_compatibility_observation") == 1

    repository_calls = sorted(
        (
            node.lineno,
            _dotted_name(node.func).removeprefix("self.store.repos.workflow_runtime."),
        )
        for node in ast.walk(method)
        if isinstance(node, ast.Call) and _dotted_name(node.func).startswith("self.store.repos.workflow_runtime.")
    )
    assert [name for _, name in repository_calls] == [
        "upsert_action",
        "append_operation_event",
        "upsert_operation",
        "append_operation_event",
    ]
    assert not [
        node
        for node in ast.walk(method)
        if isinstance(node, ast.Call)
        and any(token in _dotted_name(node.func) for token in ("dispatch", "workflow_command"))
    ]

    kwargs_mutation = source.replace(
        "        metadata: dict[str, Any] | None = None,\n    ) -> OperationSubmissionResult:",
        "        metadata: dict[str, Any] | None = None,\n        **kwargs: Any,\n    ) -> OperationSubmissionResult:",
        1,
    )
    assert kwargs_mutation != source
    kwargs_method = _class_method(
        ast.parse(kwargs_mutation),
        "OperationRuntimeWriter",
        "submit_action",
    )
    assert _signature_contract(kwargs_method) != expected_signature
    assert _signature_contract(kwargs_method)["kwarg"] == ("kwargs", "Any")

    runner_mutation = source.replace(
        '        event_type = "ActionApprovalRequired" if spec.requires_approval else "AgentActionQueued"\n',
        '        runner(action)\n        event_type = "ActionApprovalRequired" if spec.requires_approval else "AgentActionQueued"\n',
        1,
    )
    assert runner_mutation != source
    runner_method = _class_method(
        ast.parse(runner_mutation),
        "OperationRuntimeWriter",
        "submit_action",
    )
    assert len(_call_inventory(runner_method)) == len(call_inventory) + 1
    assert _call_inventory_digest(runner_method) != SUBMIT_ACTION_CALL_INVENTORY_SHA256
    assert "runner" in _call_inventory_roots(runner_method)


def test_submit_action_preserves_current_validation_and_non_dispatch_baseline() -> None:
    repository = _WorkflowRuntimeRepoProbe()
    writer = _FailClosedOperationRuntimeWriter(_FailClosedStoreProbe(repository))

    target_ref = {"owner_bound_future_field": {"unexpected": [1, 2, 3]}}
    input_payload = {"unregistered_option": {"nested": True}}
    result = writer.submit_action(
        action_type=operation_runtime.ACTION_EXTERNAL_INTAKE,
        workspace_id=" workspace-a ",
        conversation_id="conversation-a",
        target_ref=target_ref,
        input_payload=input_payload,
        idempotency_key="external-intake-a",
        actor="characterization",
        source="test",
    )

    assert result.action["target_ref"] == target_ref
    assert result.action["input"] == input_payload
    assert result.operation_run["status"] == "queued"
    assert [name for name, _ in repository.calls] == [
        "get_action",
        "get_action_by_idempotency",
        "get_operation",
        "get_operation_by_idempotency",
        "upsert_action",
        "append_operation_event",
        "upsert_operation",
        "append_operation_event",
    ]
    assert [payload["event_type"] for name, payload in repository.calls if name == "append_operation_event"] == [
        "AgentActionQueued",
        "OperationRunQueued",
    ]

    empty_repository = _WorkflowRuntimeRepoProbe()
    guarded_writer = _FailClosedOperationRuntimeWriter(_FailClosedStoreProbe(empty_repository))
    with pytest.raises(KeyError, match="unknown operation action type"):
        guarded_writer.submit_action(action_type="unknown_action")
    assert empty_repository.calls == []

    with pytest.raises(ValueError, match="requires explicit budget"):
        guarded_writer.submit_action(action_type=operation_runtime.ACTION_FETCH_PROFILE_SAMPLE)
    assert empty_repository.calls == []


def test_runtime_dispatch_inventory_distinguishes_registration_from_adapter_support() -> None:
    expected_adapters = {
        action_type: adapter for action_type, adapter in EXPECTED_ACTION_DISPATCH_ADAPTERS.items() if adapter
    }
    expected_unsupported = {
        action_type for action_type, adapter in EXPECTED_ACTION_DISPATCH_ADAPTERS.items() if not adapter
    }
    inventory = _surface_inventory()
    assert inventory["action_types"] == frozenset(expected_adapters) | expected_unsupported
    assert {
        action_type: DEFAULT_ACTION_REGISTRY.spec_for(action_type).dispatch_adapter
        for action_type in inventory["action_types"]
    } == EXPECTED_ACTION_DISPATCH_ADAPTERS

    probe = _DispatchProbe()
    for action_type in sorted(inventory["action_types"]):
        operation_run = {"operation_run_id": f"operation-{action_type}"}
        action = {"action_id": f"action-{action_type}", "action_type": action_type}
        call_count = len(probe.operation_runtime_writer.calls)
        result = SourcingOrchestrator._dispatch_operation_run_from_records(
            probe,
            operation_run=operation_run,
            action=action,
            actor="characterization",
        )
        assert probe.operation_runtime_writer.calls[call_count:] == [
            (
                "validate_persisted_action_request",
                {"action": action, "operation_run": operation_run},
            ),
            (
                "record_schema_less_compatibility_observation",
                {
                    "action": action,
                    "operation_run": operation_run,
                    "observation": "dispatch",
                    "actor": "characterization",
                    "source": "api.operation_run_dispatch",
                },
            ),
        ]
        if action_type in expected_unsupported:
            assert result["status"] == "unsupported"
            assert result["reason"] == f"operation action {action_type!r} has no W9b owner adapter"
        else:
            assert result == {"adapter": expected_adapters[action_type]}

    external_intake = DEFAULT_ACTION_REGISTRY.to_record()[operation_runtime.ACTION_EXTERNAL_INTAKE]
    assert external_intake["allowed_workflow_command_types"] == [EXCEL_INTAKE_RUN_COMMAND_TYPE]
    assert external_intake["default_workflow_command_type"] == EXCEL_INTAKE_RUN_COMMAND_TYPE
    assert operation_runtime.ACTION_EXTERNAL_INTAKE in expected_unsupported


def test_command_plan_selection_and_fields_preserve_input_target_default_precedence() -> None:
    source = ORCHESTRATOR_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    method = _class_method(tree, "SourcingOrchestrator", "_build_agent_callable_workflow_command_plan")
    command_type_assignments = [
        node
        for node in method.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "command_type" for target in node.targets)
    ]
    assert len(command_type_assignments) == 1
    expected_precedence = (
        "str(input_payload.get('command_type') or target_ref.get('command_type') or default_type).strip()"
    )
    assert ast.unparse(command_type_assignments[0].value) == expected_precedence

    mutated_source = source.replace(
        'input_payload.get("command_type") or target_ref.get("command_type") or default_type',
        'target_ref.get("command_type") or input_payload.get("command_type") or default_type',
        1,
    )
    mutated_method = _class_method(
        ast.parse(mutated_source),
        "SourcingOrchestrator",
        "_build_agent_callable_workflow_command_plan",
    )
    mutated_assignment = next(
        node
        for node in mutated_method.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "command_type" for target in node.targets)
    )
    assert ast.unparse(mutated_assignment.value) != expected_precedence

    query_text_assignments = [
        node
        for node in ast.walk(method)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "query_text" for target in node.targets)
        and "raw_user_request" in ast.unparse(node.value)
    ]
    assert len(query_text_assignments) == 1
    expected_query_precedence = (
        "str(input_payload.get('query') or input_payload.get('raw_user_request') or target_ref.get('query') "
        "or explicit_workflow_payload.get('raw_user_request') or explicit_workflow_payload.get('query') or "
        "'').strip()"
    )
    assert ast.unparse(query_text_assignments[0].value) == expected_query_precedence

    raw_request_target_query_order = 'input_payload.get("raw_user_request")\n                or target_ref.get("query")'
    assert raw_request_target_query_order in source
    query_order_mutation = source.replace(
        raw_request_target_query_order,
        'target_ref.get("query")\n                or input_payload.get("raw_user_request")',
        1,
    )
    mutated_query_method = _class_method(
        ast.parse(query_order_mutation),
        "SourcingOrchestrator",
        "_build_agent_callable_workflow_command_plan",
    )
    mutated_query_assignments = [
        node
        for node in ast.walk(mutated_query_method)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "query_text" for target in node.targets)
        and "raw_user_request" in ast.unparse(node.value)
    ]
    assert len(mutated_query_assignments) == 1
    assert ast.unparse(mutated_query_assignments[0].value) != expected_query_precedence

    operation_run = {
        "operation_run_id": "operation-start-a",
        "workspace_id": "workspace-a",
        "idempotency_key": "operation-start-a",
    }

    def build_start_plan(
        suffix: str,
        *,
        input_payload: dict[str, Any],
        target_ref: dict[str, Any],
    ) -> dict[str, Any]:
        return SourcingOrchestrator._build_agent_callable_workflow_command_plan(
            _plan_probe(),
            operation_run=operation_run,
            action={
                "action_id": f"action-start-{suffix}",
                "action_type": operation_runtime.ACTION_START_ACQUISITION_RUN,
                "input": input_payload,
                "target_ref": target_ref,
            },
        )

    query_precedence_cases: tuple[tuple[str, dict[str, Any], dict[str, Any], str], ...] = (
        (
            "input-query",
            {
                "query": "sentinel-input-query",
                "raw_user_request": "sentinel-input-raw",
                "workflow_payload": {
                    "raw_user_request": "sentinel-nested-raw",
                    "query": "sentinel-nested-query",
                },
            },
            {"query": "sentinel-target-query"},
            "sentinel-input-query",
        ),
        (
            "input-raw-over-target",
            {
                "raw_user_request": "sentinel-input-raw",
                "workflow_payload": {
                    "raw_user_request": "sentinel-nested-raw",
                    "query": "sentinel-nested-query",
                },
            },
            {"query": "sentinel-target-query"},
            "sentinel-input-raw",
        ),
        (
            "target-over-nested",
            {
                "workflow_payload": {
                    "raw_user_request": "sentinel-nested-raw",
                    "query": "sentinel-nested-query",
                }
            },
            {"query": "sentinel-target-query"},
            "sentinel-target-query",
        ),
        (
            "nested-raw-over-query",
            {
                "workflow_payload": {
                    "raw_user_request": "sentinel-nested-raw",
                    "query": "sentinel-nested-query",
                }
            },
            {},
            "sentinel-nested-raw",
        ),
        (
            "nested-query-fallback",
            {"workflow_payload": {"query": "sentinel-nested-query"}},
            {},
            "sentinel-nested-query",
        ),
    )
    for suffix, input_payload, target_ref, expected_query in query_precedence_cases:
        plan = build_start_plan(suffix, input_payload=input_payload, target_ref=target_ref)
        assert plan["status"] == "ok", suffix
        assert plan["command_payload"]["query"] == expected_query, suffix

    input_nested_source = build_start_plan(
        "input-nested-source",
        input_payload={
            "workflow_payload": {"query": "sentinel-input-nested"},
            "command_payload": {"workflow_payload": {"query": "sentinel-command-nested"}},
        },
        target_ref={"workflow_payload": {"query": "sentinel-target-nested"}},
    )
    assert input_nested_source["command_payload"]["query"] == "sentinel-input-nested"

    target_nested_source = build_start_plan(
        "target-nested-source",
        input_payload={
            "command_payload": {"workflow_payload": {"query": "sentinel-command-nested"}},
        },
        target_ref={"workflow_payload": {"query": "sentinel-target-nested"}},
    )
    assert target_nested_source["command_payload"]["query"] == "sentinel-target-nested"

    command_nested_source = build_start_plan(
        "command-nested-source",
        input_payload={
            "command_payload": {"workflow_payload": {"query": "sentinel-command-nested"}},
        },
        target_ref={},
    )
    assert command_nested_source["command_payload"]["query"] == "sentinel-command-nested"

    default_plan = SourcingOrchestrator._build_agent_callable_workflow_command_plan(
        _plan_probe(),
        operation_run=operation_run,
        action={
            "action_id": "action-start-a",
            "action_type": operation_runtime.ACTION_START_ACQUISITION_RUN,
            "input": {
                "target_company": "Input Company",
                "query": "input query",
                "workflow_payload": {"target_company": "Workflow Company", "query": "workflow query"},
            },
            "target_ref": {"target_company": "Target Company", "query": "target query"},
        },
    )
    assert default_plan["status"] == "ok"
    assert default_plan["command_type"] == ACQUISITION_RUN_CREATE_COMMAND_TYPE
    assert default_plan["command_payload"]["target_company"] == "Input Company"
    assert default_plan["command_payload"]["query"] == "input query"

    target_over_nested_plan = SourcingOrchestrator._build_agent_callable_workflow_command_plan(
        _plan_probe(),
        operation_run=operation_run,
        action={
            "action_id": "action-start-target-over-nested",
            "action_type": operation_runtime.ACTION_START_ACQUISITION_RUN,
            "input": {
                "workflow_payload": {
                    "target_company": "Nested Company",
                    "query": "nested query",
                }
            },
            "target_ref": {
                "target_company": "Target Company",
                "query": "target query",
            },
        },
    )
    assert target_over_nested_plan["status"] == "ok"
    assert target_over_nested_plan["command_payload"]["target_company"] == "Target Company"
    assert target_over_nested_plan["command_payload"]["query"] == "target query"

    nested_fallback_plan = SourcingOrchestrator._build_agent_callable_workflow_command_plan(
        _plan_probe(),
        operation_run=operation_run,
        action={
            "action_id": "action-start-nested-fallback",
            "action_type": operation_runtime.ACTION_START_ACQUISITION_RUN,
            "input": {
                "workflow_payload": {
                    "target_company": "Nested Company",
                    "query": "nested query",
                }
            },
            "target_ref": {},
        },
    )
    assert nested_fallback_plan["status"] == "ok"
    assert nested_fallback_plan["command_payload"]["target_company"] == "Nested Company"
    assert nested_fallback_plan["command_payload"]["query"] == "nested query"

    input_override = SourcingOrchestrator._build_agent_callable_workflow_command_plan(
        _plan_probe(),
        operation_run=operation_run,
        action={
            "action_id": "action-start-b",
            "action_type": operation_runtime.ACTION_START_ACQUISITION_RUN,
            "input": {"command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE, "target_company": "Input Company"},
            "target_ref": {"command_type": ACQUISITION_RUN_CREATE_COMMAND_TYPE},
        },
    )
    assert input_override == {
        "status": "invalid",
        "reason": "unsupported_agent_callable_workflow_command_type",
        "command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE,
        "allowed_command_types": [ACQUISITION_RUN_CREATE_COMMAND_TYPE],
    }

    target_override = SourcingOrchestrator._build_agent_callable_workflow_command_plan(
        _plan_probe(),
        operation_run=operation_run,
        action={
            "action_id": "action-start-c",
            "action_type": operation_runtime.ACTION_START_ACQUISITION_RUN,
            "input": {"target_company": "Input Company"},
            "target_ref": {"command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE},
        },
    )
    assert target_override["status"] == "invalid"
    assert target_override["command_type"] == EXCEL_INTAKE_RUN_COMMAND_TYPE
