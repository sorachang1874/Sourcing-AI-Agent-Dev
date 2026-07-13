from __future__ import annotations

import ast
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
            "approval_status": kwargs["approval_status"],
            "approval_policy": kwargs["approval_policy"],
            "budget": dict(kwargs["budget"]),
            "idempotency_key": kwargs["idempotency_key"],
            "status": kwargs["status"],
            "metadata": dict(kwargs["metadata"]),
        }

    def upsert_operation(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("upsert_operation", dict(kwargs)))
        return dict(kwargs)

    def append_operation_event(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("append_operation_event", dict(kwargs)))
        return {"event_id": f"event-{len(self.calls)}", **dict(kwargs)}


class _DispatchProbe:
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


def test_action_spec_and_registry_record_freeze_the_pre_request_schema_surface() -> None:
    assert tuple(field.name for field in fields(operation_runtime.ActionSpec)) == (
        "action_type",
        "owner_module",
        "operation_type",
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
    tree = ast.parse(OPERATION_RUNTIME_PATH.read_text(encoding="utf-8"))
    method = _class_method(tree, "OperationRuntimeWriter", "submit_action")

    assert [argument.arg for argument in method.args.args] == ["self"]
    assert [argument.arg for argument in method.args.kwonlyargs] == [
        "action_type",
        "workspace_id",
        "conversation_id",
        "target_ref",
        "input_payload",
        "budget",
        "idempotency_key",
        "actor",
        "source",
        "metadata",
    ]
    assert [_unparse_optional(default) for default in method.args.kw_defaults] == [
        None,
        "'default'",
        "''",
        "None",
        "None",
        "None",
        "''",
        "'operation_runtime'",
        "'operation_runtime'",
        "None",
    ]
    assert _unparse_optional(method.returns) == "OperationSubmissionResult"

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


def test_submit_action_preserves_current_validation_and_non_dispatch_baseline() -> None:
    repository = _WorkflowRuntimeRepoProbe()
    writer = OperationRuntimeWriter(SimpleNamespace(repos=SimpleNamespace(workflow_runtime=repository)))

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
    guarded_writer = OperationRuntimeWriter(SimpleNamespace(repos=SimpleNamespace(workflow_runtime=empty_repository)))
    with pytest.raises(KeyError, match="unknown operation action type"):
        guarded_writer.submit_action(action_type="unknown_action")
    assert empty_repository.calls == []

    with pytest.raises(ValueError, match="requires explicit budget"):
        guarded_writer.submit_action(action_type=operation_runtime.ACTION_FETCH_PROFILE_SAMPLE)
    assert empty_repository.calls == []


def test_runtime_dispatch_inventory_distinguishes_registration_from_adapter_support() -> None:
    expected_adapters = {
        operation_runtime.ACTION_FILTER_PROJECTION: "projection_read",
        operation_runtime.ACTION_SEARCH_PROJECTION: "projection_read",
        operation_runtime.ACTION_ENRICH_PERSON_PUBLIC_WEB: "person_public_web",
        operation_runtime.ACTION_EXPORT_CANDIDATES: "export",
        operation_runtime.ACTION_START_ACQUISITION_RUN: "agent_callable_workflow_command",
        operation_runtime.ACTION_FETCH_PROFILE_SAMPLE: "agent_callable_workflow_command",
        operation_runtime.ACTION_CONTINUE_ACQUISITION_RUN: "agent_callable_workflow_command",
        operation_runtime.ACTION_REFRESH_COMPANY_PUBLIC_WEB: "agent_callable_workflow_command",
        operation_runtime.ACTION_ADD_TO_CRM: "crm_writer",
        operation_runtime.ACTION_SET_CRM_STAGE: "crm_writer",
        operation_runtime.ACTION_ADD_CRM_NOTE: "crm_writer",
        operation_runtime.ACTION_CREATE_CRM_TASK: "crm_writer",
    }
    expected_unsupported = {
        operation_runtime.ACTION_PLAN_ACQUISITION,
        operation_runtime.ACTION_PROMOTE_PERSON_ASSERTION,
        operation_runtime.ACTION_EXTERNAL_INTAKE,
    }
    inventory = _surface_inventory()
    assert inventory["action_types"] == frozenset(expected_adapters) | expected_unsupported

    probe = _DispatchProbe()
    for action_type in sorted(inventory["action_types"]):
        result = SourcingOrchestrator._dispatch_operation_run_from_records(
            probe,
            operation_run={"operation_run_id": f"operation-{action_type}"},
            action={"action_id": f"action-{action_type}", "action_type": action_type},
            actor="characterization",
        )
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

    operation_run = {
        "operation_run_id": "operation-start-a",
        "workspace_id": "workspace-a",
        "idempotency_key": "operation-start-a",
    }
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
