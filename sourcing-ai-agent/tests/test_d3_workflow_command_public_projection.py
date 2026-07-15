from __future__ import annotations

import ast
import copy
import json
import re
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from sourcing_agent.command_kernel import (
    _WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_FIELDS,
    _WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_LIST_FIELDS,
    _WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_FIELDS,
    _WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_LIST_FIELDS,
    _WORKFLOW_COMMAND_PUBLIC_CARRIER_FIELDS,
    _WORKFLOW_COMMAND_PUBLIC_CARRIER_LIST_FIELDS,
    _WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_FIELDS,
    _WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_LIST_FIELDS,
    _WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS,
    _WORKFLOW_PUBLIC_MIRROR_MAX_DEPTH,
    _WORKFLOW_PUBLIC_MIRROR_MAX_NODES,
    WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_FIELDS,
    WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_FIELDS,
    WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS,
    WORKFLOW_COMMAND_OPERATION_SYNC_PUBLIC_FIELDS,
    WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS,
    WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS,
    WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS,
    WORKFLOW_ENTITY_DELTA_PUBLIC_FIELDS,
    CommandKernel,
)
from sourcing_agent.durable_runtime import (
    WORKFLOW_COMMAND_CONTROL_POLICY_BOOLEAN_FIELDS,
    WORKFLOW_COMMAND_CONTROL_POLICY_PUBLIC_FIELDS,
    WORKFLOW_COMMAND_CONTROL_POLICY_STRING_ARRAY_FIELDS,
    WORKFLOW_COMMAND_CONTROL_POLICY_STRING_FIELDS,
    workflow_command_control_policy,
)
from sourcing_agent.repositories.workflow_runtime import (
    WORKFLOW_ACTIVITY_ATTEMPTS,
    WORKFLOW_ACTIVITY_RUNS,
    WORKFLOW_COMMANDS,
    WORKFLOW_ENTITY_DELTAS,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
COMMAND_KERNEL_PATH = SOURCE_ROOT / "command_kernel.py"
ACQUISITION_OWNER_PATH = SOURCE_ROOT / "acquisition_command_owner.py"
ORCHESTRATOR_PATH = SOURCE_ROOT / "orchestrator.py"
API_PATH = SOURCE_ROOT / "api.py"
FRONTEND_SCHEMA_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.schema.json"
FRONTEND_TYPES_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.ts"
FRONTEND_ADAPTER_PATH = REPO_ROOT / "contracts" / "frontend_api_adapter.ts"
FRONTEND_RUNTIME_CONTRACT_PATH = REPO_ROOT / "contracts" / "frontend_api_runtime_contract.ts"
FRONTEND_DEMO_API_PATH = REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts"
ESBUILD_MODULE_PATH = REPO_ROOT / "frontend-demo" / "node_modules" / "esbuild" / "lib" / "main.js"
TYPESCRIPT_COMPILER_PATH = REPO_ROOT / "frontend-demo" / "node_modules" / "typescript" / "bin" / "tsc"
FRONTEND_ACTION_STATUS_TYPES_PATH = REPO_ROOT / "tests" / "frontend_api_action_status_types.test.ts"

SAFE_DIAGNOSTIC_FIELDS = ("claim_generation", "control_epoch")
MAX_SAFE_DIAGNOSTIC_INTEGER = 9_007_199_254_740_991
D3C2A_DORMANT_INTERNAL_FIELDS = (
    "runtime_namespace",
    "provider_mode",
    "workspace_id",
    "scope_digest",
    "coordination_plan_review_id",
    "claim_authority_spec_digest",
    "expected_predecessor_intent_id",
    "expected_predecessor_phase_generation",
    "expected_predecessor_source_control_epoch",
    "expected_predecessor_decision_source_event_id",
    "d3_business_fence_digest",
    "claim_selection_generation",
    "consumed_claim_authority_id",
    "claim_token_digest",
    "heartbeat_sequence",
    "last_heartbeat_id",
    "terminal_event_id",
    "terminal_outcome_digest",
)
D3B_PRIVATE_BOOTSTRAP_CAPABILITY_FIELDS = (
    "bootstrap_authority",
    "bootstrap_authority_id",
    "bootstrap_authority_digest",
    "bootstrap_receipt",
    "issuer_digest",
    "issuer_revision",
    "lease_identity",
    "scoped_review_session_bootstrap_authority",
    "scoped_review_session_bootstrap_receipt",
)
EXPECTED_DERIVED_FIELDS = (
    "agent_exposure_gate",
    "agent_exposure_status",
    "display_contract",
    "control_policy",
    "control_state",
    "activity_spine_policy",
    "execution_summary",
)
EXPECTED_OPERATION_SYNC_FIELDS = (
    "status",
    "reason",
    "operation_run_id",
    "operation_status",
    "control_action",
    "command_status",
    "operation_run",
    "event",
    "workflow_command",
)
EXPECTED_ACTIVITY_RUN_DERIVED_FIELDS = (
    "control_target",
    "module_state_mutated",
    "mutation_contract",
)
EXPECTED_ACTIVITY_ATTEMPT_DERIVED_FIELDS = (
    "activity_type",
    "owner",
    "control_target",
    "module_state_mutated",
    "mutation_contract",
)
EXPECTED_ENTITY_DELTA_DERIVED_FIELDS = EXPECTED_ACTIVITY_ATTEMPT_DERIVED_FIELDS
EXPECTED_ACTIVITY_CONTROL_TARGET_FIELDS = (
    "target_type",
    "command_id",
    "command_type",
    "owner",
    "command_status",
    "display_contract",
    "control_policy",
    "control_state",
    "activity_spine_policy",
    "fallback_status",
)
EXPECTED_RECURSIVE_ACTIVITY_CARRIER_FIELDS = frozenset(
    {
        "workflow_activity",
        "workflow_activity_run",
        "workflow_activity_attempt",
        "workflow_entity_delta",
        "workflow_activities",
        "workflow_activity_runs",
        "workflow_activity_attempts",
        "workflow_entity_deltas",
    }
)
EXPECTED_RECURSIVE_COMMAND_CARRIER_FIELDS = frozenset(
    {
        "workflow_command",
        "latest_workflow_command",
        "workflow_commands",
    }
)
EXPECTED_SERVED_CONTROL_ACTIVITY_CARRIER_FIELDS = frozenset(
    {
        "workflow_activity",
        "workflow_activity_run",
        "workflow_activity_attempt",
        "workflow_entity_delta",
        "workflow_activity_runs",
        "workflow_activity_attempts",
        "workflow_entity_deltas",
    }
)
PRIVATE_ALIAS_KEYS = (
    "CLAIMToken",
    "CLAIMAuthoritySeal",
    "claimCapabilityPreview",
    "claimIdentityEnvelope",
    "claimReceiptEnvelope",
    "claimSecretPreview",
    "leaseIdentityDigest",
    "leaseTokenDigest",
)
RouteBinding = tuple[str, str, str, str]

# Each binding is (method, path, transport handler, owning orchestrator method).
# Values are the exact owner-side projectors that must be reachable from that
# route.  Route discovery below inventories every ``add(...)`` registration
# before selecting bindings by projector reachability; it never filters by
# these expected paths first.
WORKFLOW_COMMAND_ROUTE_MANIFEST: dict[RouteBinding, frozenset[str]] = {
    ("GET", "/api/workflow/commands", "get_workflow_commands", "list_workflow_commands_api"): frozenset(
        {"_workflow_command_api_record", "_workflow_command_api_record_with_execution_summary"}
    ),
    (
        "GET",
        "/api/workflow/commands/{command_id}",
        "get_workflow_command",
        "get_workflow_command_api",
    ): frozenset({"_workflow_command_api_record_with_execution_summary"}),
    (
        "POST",
        "/api/workflow/commands/{command_id}/cancel",
        "post_workflow_command_cancel",
        "cancel_workflow_command_api",
    ): frozenset(
        {
            "_workflow_command_api_record",
            "_workflow_command_control_public_api_record",
            "_workflow_command_operation_sync_api_record",
        }
    ),
    (
        "POST",
        "/api/workflow/commands/{command_id}/retry",
        "post_workflow_command_retry",
        "retry_workflow_command_api",
    ): frozenset(
        {
            "_workflow_command_api_record",
            "_workflow_command_control_public_api_record",
            "_workflow_command_operation_sync_api_record",
        }
    ),
    (
        "POST",
        "/api/workflow/commands/{command_id}/resume",
        "post_workflow_command_resume",
        "resume_workflow_command_api",
    ): frozenset(
        {
            "_workflow_command_api_record",
            "_workflow_command_control_public_api_record",
            "_workflow_command_operation_sync_api_record",
        }
    ),
    ("GET", "/api/operations/actions", "get_operation_actions", "list_operation_actions_api"): frozenset(
        {"_operation_action_api_record"}
    ),
    ("GET", "/api/operations/runs", "get_operation_runs", "list_operation_runs_api"): frozenset(
        {"_operation_run_api_record", "_operation_run_api_record_with_status_summary"}
    ),
    (
        "GET",
        "/api/operations/runs/{run_id}/provenance",
        "get_operation_run_provenance",
        "get_operation_run_provenance_api",
    ): frozenset(
        {
            "_operation_action_api_record",
            "_operation_event_api_records",
            "_operation_run_api_record_with_status_summary",
            "_workflow_command_api_record_with_execution_summary",
        }
    ),
    (
        "GET",
        "/api/operations/actions/{action_id}",
        "get_operation_action",
        "get_operation_action_api",
    ): frozenset({"_operation_action_api_record", "_operation_event_api_records"}),
    ("GET", "/api/operations/runs/{run_id}", "get_operation_run", "get_operation_run_api"): frozenset(
        {"_operation_event_api_records", "_operation_run_api_record_with_status_summary"}
    ),
    ("POST", "/api/operations/actions", "post_operation_actions", "submit_operation_action"): frozenset(
        {
            "_operation_action_api_record",
            "_operation_event_api_records",
            "_operation_run_api_record_with_status_summary",
        }
    ),
    (
        "POST",
        "/api/operations/actions/{action_id}/approve",
        "post_operation_action_approve",
        "approve_operation_action_api",
    ): frozenset(
        {
            "_operation_action_api_record",
            "_operation_event_api_records",
            "_operation_run_api_record_with_status_summary",
        }
    ),
    (
        "POST",
        "/api/operations/actions/{action_id}/reject",
        "post_operation_action_reject",
        "reject_operation_action_api",
    ): frozenset({"_operation_action_api_record", "_operation_event_api_records"}),
    (
        "POST",
        "/api/operations/runs/{run_id}/cancel",
        "post_operation_run_cancel",
        "cancel_operation_run_api",
    ): frozenset({"_operation_event_api_records", "_operation_run_api_record_with_status_summary"}),
    (
        "POST",
        "/api/operations/runs/{run_id}/retry",
        "post_operation_run_retry",
        "retry_operation_run_api",
    ): frozenset(
        {
            "_operation_action_api_record",
            "_operation_event_api_records",
            "_operation_run_api_record_with_status_summary",
        }
    ),
    (
        "POST",
        "/api/operations/runs/{run_id}/resume",
        "post_operation_run_resume",
        "resume_operation_run_api",
    ): frozenset({"_operation_event_api_records", "_operation_run_api_record_with_status_summary"}),
    (
        "POST",
        "/api/operations/runs/{run_id}/dispatch",
        "post_operation_run_dispatch",
        "dispatch_operation_run_api",
    ): frozenset({"_operation_run_control_response_record"}),
}

WORKFLOW_ACTIVITY_ROUTE_MANIFEST: dict[RouteBinding, frozenset[str]] = {
    ("GET", "/api/workflow/activities", "get_workflow_activities", "list_workflow_activities_api"): frozenset(
        {"_workflow_activity_api_record"}
    ),
    (
        "GET",
        "/api/workflow/activities/{activity_id}",
        "get_workflow_activity",
        "get_workflow_activity_api",
    ): frozenset({"_workflow_activity_api_record", "_workflow_activity_attempt_api_record"}),
    (
        "GET",
        "/api/workflow/activity-attempts",
        "get_workflow_activity_attempts",
        "list_workflow_activity_attempts_api",
    ): frozenset({"_workflow_activity_attempt_api_record"}),
    (
        "GET",
        "/api/workflow/activity-attempts/{attempt_id}",
        "get_workflow_activity_attempt",
        "get_workflow_activity_attempt_api",
    ): frozenset({"_workflow_activity_attempt_api_record"}),
    (
        "GET",
        "/api/workflow/entity-deltas",
        "get_workflow_entity_deltas",
        "list_workflow_entity_deltas_api",
    ): frozenset({"_workflow_entity_delta_api_record"}),
    (
        "GET",
        "/api/workflow/entity-deltas/{delta_id}",
        "get_workflow_entity_delta",
        "get_workflow_entity_delta_api",
    ): frozenset({"_workflow_entity_delta_api_record"}),
}

COMPACT_COMMAND_ROUTE_MANIFEST: dict[RouteBinding, frozenset[str]] = {
    (
        "GET",
        "/api/jobs/{job_id:sourcing_ident}/materialization-items",
        "get_job_materialization_items",
        "get_job_materialization_items",
    ): frozenset({"_compact_public_workflow_command_payload", "_workflow_command_api_record"})
}

PUBLIC_PROJECTION_ROUTE_MANIFEST = {
    **WORKFLOW_COMMAND_ROUTE_MANIFEST,
    **WORKFLOW_ACTIVITY_ROUTE_MANIFEST,
    **COMPACT_COMMAND_ROUTE_MANIFEST,
}

# Candidate population is intentionally independent of the projection helpers
# checked by ``PUBLIC_PROJECTION_ROUTE_MANIFEST``.  Each signature pairs a
# domain source/mutation call with the public response key emitted by the same
# response-building definition.  Return-reachable wrappers are followed below,
# so a raw response introduced behind a thin route owner cannot evade the
# inventory merely by omitting a projector call.
_ROUTE_SOURCE_SINK_MARKER_SIGNATURES = (
    (frozenset({"list_workflow_commands"}), frozenset({"workflow_commands"})),
    (frozenset({"get_workflow_command"}), frozenset({"workflow_command", "latest_workflow_command"})),
    (
        frozenset({"cancel_workflow_command", "retry_workflow_command", "resume_workflow_command"}),
        frozenset({"workflow_command"}),
    ),
    (frozenset({"list_actions"}), frozenset({"actions"})),
    (frozenset({"get_action"}), frozenset({"action"})),
    (frozenset({"submit_action", "approve_action"}), frozenset({"action", "operation_run", "events"})),
    (frozenset({"reject_action"}), frozenset({"action", "events"})),
    (frozenset({"list_operations"}), frozenset({"operation_runs"})),
    (frozenset({"get_operation"}), frozenset({"operation_run"})),
    (
        frozenset({"cancel_operation", "retry_operation", "resume_operation"}),
        frozenset({"operation_run", "events"}),
    ),
    (frozenset({"list_activity_runs"}), frozenset({"workflow_activities"})),
    (frozenset({"get_activity_run"}), frozenset({"workflow_activity"})),
    (frozenset({"list_activity_attempts"}), frozenset({"workflow_activity_attempts"})),
    (frozenset({"get_activity_attempt"}), frozenset({"workflow_activity_attempt"})),
    (frozenset({"list_entity_deltas"}), frozenset({"workflow_entity_deltas"})),
    (frozenset({"get_entity_delta"}), frozenset({"workflow_entity_delta"})),
    (frozenset({"list_job_materialization_items"}), frozenset({"workflow_commands"})),
)
_ROUTE_STANDALONE_SOURCE_SINK_MARKERS = frozenset({"_dispatch_operation_run_from_records"})

OPERATION_SYNC_OWNER_MANIFEST = {
    COMMAND_KERNEL_PATH: {"_sync_operation_run_from_workflow_command": 1},
    ACQUISITION_OWNER_PATH: {
        "_sync_acquisition_plan_ready_from_workflow_command": 1,
        "_sync_acquisition_plan_review_requested_from_workflow_command": 1,
        "_sync_acquisition_plan_committed_from_workflow_command": 1,
        "_sync_acquisition_run_phase_from_workflow_command": 1,
    },
    ORCHESTRATOR_PATH: {"_sync_operation_run_from_workflow_command_control": 2},
}


class SyntheticClaimAuthority:
    def __eq__(self, other: object) -> bool:
        return isinstance(other, SyntheticClaimAuthority)


def _full_command_record() -> dict[str, Any]:
    record: dict[str, Any] = {}
    list_fields = {
        "input_artifact_refs",
        "output_artifact_refs",
        "downstream_command_ids",
        "artifact_refs",
    }
    object_fields = {"produced_entity_counts", "payload", "retry_policy", "result"}
    number_fields = {"attempt", "max_attempts", "claim_generation", "control_epoch"}
    for field in WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS:
        if field in list_fields:
            record[field] = [f"{field}-value"]
        elif field in object_fields:
            record[field] = {"business_value": field}
        elif field in number_fields:
            record[field] = 3
        else:
            record[field] = f"{field}-value"
    return record


def _structured_artifact_refs() -> list[Any]:
    return [
        "artifact://profile/plain",
        {
            "profile_url": "https://example.test/researcher",
            "raw_path": "profiles/researcher.json",
            "metadata": {
                "safe": "preserved",
                "CLAIMToken": "secret",
            },
            "claimReceiptEnvelope": {"secret": True},
        },
    ]


def _expected_structured_artifact_refs() -> list[Any]:
    return [
        "artifact://profile/plain",
        {
            "profile_url": "https://example.test/researcher",
            "raw_path": "profiles/researcher.json",
            "metadata": {"safe": "preserved"},
        },
    ]


def _normalized_key(value: str) -> str:
    raw = str(value or "").strip().replace("-", "_")
    raw = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", raw)
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", raw).lower()


def _assert_no_private_capability(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = _normalized_key(str(key))
            assert normalized not in WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS
            assert normalized not in D3B_PRIVATE_BOOTSTRAP_CAPABILITY_FIELDS
            assert not any(
                normalized == root or normalized.startswith(f"{root}_")
                for root in WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS
            )
            _assert_no_private_capability(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_private_capability(item)
    else:
        assert not isinstance(value, SyntheticClaimAuthority)


def _assert_alias_keys_absent(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert str(key) not in PRIVATE_ALIAS_KEYS
            _assert_alias_keys_absent(item)
    elif isinstance(value, list):
        for item in value:
            _assert_alias_keys_absent(item)


def _schema_ref_count(value: Any, target: str) -> int:
    if isinstance(value, dict):
        return int(value.get("$ref") == target) + sum(_schema_ref_count(item, target) for item in value.values())
    if isinstance(value, list):
        return sum(_schema_ref_count(item, target) for item in value)
    return 0


def _typescript_segment(source: str, start: str, end: str) -> str:
    start_offset = source.index(start)
    return source[start_offset : source.index(end, start_offset + len(start))]


def _typescript_string_collection(source: str, declaration_name: str) -> frozenset[str]:
    declaration_offset = source.index(declaration_name)
    open_offset = source.index("[", declaration_offset)
    close_offset = source.index("]", open_offset)
    return frozenset(re.findall(r'["\']([^"\']+)["\']', source[open_offset : close_offset + 1]))


def _typescript_interface_fields(source: str, interface_name: str, next_interface_name: str) -> frozenset[str]:
    segment = _typescript_segment(
        source,
        f"export interface {interface_name}",
        f"export interface {next_interface_name}",
    )
    return frozenset(re.findall(r"^\s{2}([A-Za-z_][A-Za-z0-9_]*)\??:", segment, flags=re.MULTILINE))


def _typescript_mapper_fields(source: str, function_name: str, next_function_name: str) -> frozenset[str]:
    segment = _typescript_segment(
        source,
        f"export function {function_name}",
        f"export function {next_function_name}",
    )
    return frozenset(re.findall(r"^\s{4}([A-Za-z_][A-Za-z0-9_]*):", segment, flags=re.MULTILINE))


def _typescript_private_family_roots(source: str) -> frozenset[str]:
    return frozenset(
        item.rstrip("_")
        for item in _typescript_string_collection(source, "WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_ROOTS")
    )


def _function_definitions(tree: ast.AST) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    return {node.name: node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _terminal_call_name(call: ast.Call) -> str:
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    if isinstance(call.func, ast.Name):
        return call.func.id
    return ""


def _orchestrator_call_graph() -> tuple[
    dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
    dict[str, frozenset[str]],
]:
    return _call_graph_for_source(ORCHESTRATOR_PATH.read_text(encoding="utf-8"))


def _call_graph_for_source(
    source: str,
) -> tuple[
    dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
    dict[str, frozenset[str]],
]:
    tree = ast.parse(source)
    definitions = _function_definitions(tree)
    graph: dict[str, frozenset[str]] = {}
    for name, function in definitions.items():
        calls: set[str] = set()
        for call in ast.walk(function):
            if not isinstance(call, ast.Call):
                continue
            if isinstance(call.func, ast.Attribute):
                owner = call.func.value
                if isinstance(owner, ast.Name) and owner.id == "self":
                    calls.add(call.func.attr)
                elif (
                    isinstance(owner, ast.Attribute)
                    and isinstance(owner.value, ast.Name)
                    and owner.value.id == "self"
                    and owner.attr == "_command_kernel"
                ):
                    calls.add(call.func.attr)
            elif isinstance(call.func, ast.Name):
                calls.add(call.func.id)
        graph[name] = frozenset(calls)
    return definitions, graph


def _reachable_calls(owner: str, definitions: dict[str, Any], graph: dict[str, frozenset[str]]) -> frozenset[str]:
    pending = [owner]
    visited: set[str] = set()
    reachable: set[str] = set()
    while pending:
        method = pending.pop()
        if method in visited:
            continue
        visited.add(method)
        for called in graph.get(method, frozenset()):
            reachable.add(called)
            if called in definitions:
                pending.append(called)
    return frozenset(reachable)


def _all_api_route_bindings(source: str | None = None) -> frozenset[RouteBinding]:
    tree = ast.parse(source if source is not None else API_PATH.read_text(encoding="utf-8"))
    definitions = _function_definitions(tree)
    bindings: set[RouteBinding] = set()
    for call in ast.walk(tree):
        if not (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Name)
            and call.func.id == "add"
            and len(call.args) >= 3
            and isinstance(call.args[0], (ast.List, ast.Tuple))
            and isinstance(call.args[1], ast.Constant)
            and isinstance(call.args[1].value, str)
            and isinstance(call.args[2], ast.Name)
        ):
            continue
        handler_name = call.args[2].id
        handler = definitions[handler_name]
        owners = {
            nested.func.attr
            for nested in ast.walk(handler)
            if isinstance(nested, ast.Call)
            and isinstance(nested.func, ast.Attribute)
            and isinstance(nested.func.value, ast.Name)
            and nested.func.value.id == "orchestrator"
        }
        for method_node in call.args[0].elts:
            if not isinstance(method_node, ast.Constant) or not isinstance(method_node.value, str):
                continue
            for owner in owners:
                bindings.add((method_node.value, call.args[1].value, handler_name, owner))
    return frozenset(bindings)


def _return_call_graph(
    definitions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
) -> dict[str, frozenset[str]]:
    graph: dict[str, frozenset[str]] = {}
    for name, function in definitions.items():
        returned_calls = {
            _terminal_call_name(call)
            for returned in ast.walk(function)
            if isinstance(returned, ast.Return) and returned.value is not None
            for call in ast.walk(returned.value)
            if isinstance(call, ast.Call)
        }
        graph[name] = frozenset(call for call in returned_calls if call in definitions)
    return graph


def _route_owner_response_definitions(
    owner: str,
    definitions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
    graph: dict[str, frozenset[str]] | None = None,
) -> frozenset[str]:
    response_graph = graph if graph is not None else _return_call_graph(definitions)
    pending = [owner]
    visited: set[str] = set()
    while pending:
        name = pending.pop()
        if name in visited or name not in definitions:
            continue
        visited.add(name)
        pending.extend(response_graph.get(name, frozenset()))
    return frozenset(visited)


def _definition_has_route_source_sink_marker(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
) -> bool:
    calls = {_terminal_call_name(node) for node in ast.walk(function) if isinstance(node, ast.Call)}
    if calls & _ROUTE_STANDALONE_SOURCE_SINK_MARKERS:
        return True
    response_keys = {
        node.value for node in ast.walk(function) if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    return any(
        bool(calls & source_markers) and bool(response_keys & sink_markers)
        for source_markers, sink_markers in _ROUTE_SOURCE_SINK_MARKER_SIGNATURES
    )


def _route_candidate_bindings(
    bindings: frozenset[RouteBinding],
    definitions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
) -> frozenset[RouteBinding]:
    response_graph = _return_call_graph(definitions)
    return frozenset(
        binding
        for binding in bindings
        if any(
            _definition_has_route_source_sink_marker(definitions[name])
            for name in _route_owner_response_definitions(binding[3], definitions, response_graph)
        )
    )


ExpressionState = tuple[frozenset[str], bool, bool]
_EMPTY_EXPRESSION_STATE: ExpressionState = (frozenset(), False, False)
_COMMAND_RECORD_PROJECTORS = frozenset(
    {
        "_workflow_command_api_record",
        "_workflow_command_api_record_with_execution_summary",
        "_workflow_command_observation",
    }
)
_CARRIER_PROJECTORS = frozenset(
    {"_workflow_command_operation_sync_api_record", "_workflow_command_public_carrier_api_record"}
)
_SINGULAR_COMMAND_CARRIER_KEYS = frozenset({"workflow_command", "latest_workflow_command"})
_PLURAL_COMMAND_CARRIER_KEYS = frozenset({"workflow_commands"})


def _merge_expression_states(states: list[ExpressionState]) -> ExpressionState:
    if not states:
        return _EMPTY_EXPRESSION_STATE
    violations = frozenset().union(*(state[0] for state in states))
    return (violations, all(state[1] for state in states), all(state[2] for state in states))


def _fresh_expression_state(state: ExpressionState) -> ExpressionState:
    return (frozenset(state[0]), bool(state[1]), bool(state[2]))


def _mapping_write_state(
    current: ExpressionState,
    *,
    key: str,
    value: ExpressionState,
    line: int,
) -> ExpressionState:
    normalized = _normalized_key(key)
    violations = set(current[0]) | set(value[0])
    if normalized in _SINGULAR_COMMAND_CARRIER_KEYS and not value[1]:
        violations.add(f"line {line}: unprojected {normalized}")
    if normalized in _PLURAL_COMMAND_CARRIER_KEYS and not value[2]:
        violations.add(f"line {line}: unprojected {normalized}")
    return (frozenset(violations), False, False)


def _replace_aliased_expression_state(
    name: str,
    state: ExpressionState,
    environment: dict[str, ExpressionState],
) -> None:
    previous = environment.get(name)
    aliases = [name]
    if previous is not None and previous is not _EMPTY_EXPRESSION_STATE:
        aliases = [candidate for candidate, candidate_state in environment.items() if candidate_state is previous]
    for alias in aliases:
        environment[alias] = state


def _constant_subscript_key(node: ast.Subscript) -> str | None:
    if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
        return node.slice.value
    return None


def _apply_mapping_mutation(
    call: ast.Call,
    environment: dict[str, ExpressionState],
) -> bool:
    if not (
        isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.attr in {"update", "setdefault"}
    ):
        return False
    name = call.func.value.id
    state = environment.get(name, _EMPTY_EXPRESSION_STATE)
    if call.func.attr == "update":
        for value in call.args:
            merged = _merge_expression_states([state, _expression_state(value, environment)])
            state = (merged[0], False, False)
        for keyword in call.keywords:
            if keyword.arg is None:
                merged = _merge_expression_states([state, _expression_state(keyword.value, environment)])
                state = (merged[0], False, False)
                continue
            state = _mapping_write_state(
                state,
                key=keyword.arg,
                value=_expression_state(keyword.value, environment),
                line=getattr(keyword.value, "lineno", getattr(call, "lineno", 0)),
            )
    elif call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str):
        value_node = call.args[1] if len(call.args) > 1 else None
        state = _mapping_write_state(
            state,
            key=call.args[0].value,
            value=_expression_state(value_node, environment),
            line=getattr(value_node, "lineno", getattr(call, "lineno", 0)),
        )
    _replace_aliased_expression_state(name, state, environment)
    return True


def _expression_state(node: ast.AST | None, environment: dict[str, ExpressionState]) -> ExpressionState:
    if node is None:
        return _EMPTY_EXPRESSION_STATE
    if isinstance(node, ast.Name):
        return environment.get(node.id, _EMPTY_EXPRESSION_STATE)
    if isinstance(node, ast.Call):
        call_name = _terminal_call_name(node)
        if call_name in _CARRIER_PROJECTORS:
            return _EMPTY_EXPRESSION_STATE
        if call_name in _COMMAND_RECORD_PROJECTORS:
            return (frozenset(), True, False)
        if call_name == "dict" and node.args:
            return _fresh_expression_state(_expression_state(node.args[0], environment))
        if isinstance(node.func, ast.Attribute) and node.func.attr == "copy" and not node.args:
            return _fresh_expression_state(_expression_state(node.func.value, environment))
        child_states = [
            _expression_state(item, environment) for item in (*node.args, *(kw.value for kw in node.keywords))
        ]
        violations = frozenset().union(*(state[0] for state in child_states)) if child_states else frozenset()
        return (violations, False, False)
    if isinstance(node, ast.Dict):
        child_states = [_expression_state(item, environment) for item in node.values]
        violations: set[str] = set().union(*(state[0] for state in child_states)) if child_states else set()
        for key, value, state in zip(node.keys, node.values, child_states, strict=True):
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                continue
            normalized = _normalized_key(key.value)
            if normalized in _SINGULAR_COMMAND_CARRIER_KEYS and not state[1]:
                violations.add(f"line {getattr(value, 'lineno', 0)}: unprojected {normalized}")
            if normalized in _PLURAL_COMMAND_CARRIER_KEYS and not state[2]:
                violations.add(f"line {getattr(value, 'lineno', 0)}: unprojected {normalized}")
        return (frozenset(violations), False, False)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        states = [_expression_state(item, environment) for item in node.elts]
        merged = _merge_expression_states(states)
        return (merged[0], False, bool(states) and all(state[1] for state in states))
    if isinstance(node, ast.IfExp):
        return _merge_expression_states(
            [_expression_state(node.body, environment), _expression_state(node.orelse, environment)]
        )
    if isinstance(node, ast.BoolOp):
        return _merge_expression_states([_expression_state(item, environment) for item in node.values])
    child_states = [_expression_state(item, environment) for item in ast.iter_child_nodes(node)]
    violations = frozenset().union(*(state[0] for state in child_states)) if child_states else frozenset()
    return (violations, False, False)


def _assign_expression_state(target: ast.AST, state: ExpressionState, environment: dict[str, ExpressionState]) -> None:
    if isinstance(target, ast.Name):
        environment[target.id] = state
    elif isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):
        key = _constant_subscript_key(target)
        if key is not None:
            current = environment.get(target.value.id, _EMPTY_EXPRESSION_STATE)
            updated = _mapping_write_state(
                current,
                key=key,
                value=state,
                line=getattr(target, "lineno", 0),
            )
            _replace_aliased_expression_state(target.value.id, updated, environment)
    elif isinstance(target, (ast.Tuple, ast.List)):
        for item in target.elts:
            _assign_expression_state(item, _EMPTY_EXPRESSION_STATE, environment)


def _merge_environments(*environments: dict[str, ExpressionState]) -> dict[str, ExpressionState]:
    keys = set().union(*(environment for environment in environments))
    return {
        key: _merge_expression_states([environment.get(key, _EMPTY_EXPRESSION_STATE) for environment in environments])
        for key in keys
    }


def _analyze_statements(
    statements: list[ast.stmt],
    environment: dict[str, ExpressionState],
) -> tuple[frozenset[str], dict[str, ExpressionState]]:
    violations: set[str] = set()
    for statement in statements:
        if isinstance(statement, ast.Assign):
            state = _expression_state(statement.value, environment)
            for target in statement.targets:
                _assign_expression_state(target, state, environment)
        elif isinstance(statement, ast.AnnAssign):
            _assign_expression_state(statement.target, _expression_state(statement.value, environment), environment)
        elif isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call):
            _apply_mapping_mutation(statement.value, environment)
        elif isinstance(statement, ast.Return):
            violations.update(_expression_state(statement.value, environment)[0])
        elif isinstance(statement, ast.If):
            body_violations, body_environment = _analyze_statements(statement.body, dict(environment))
            else_violations, else_environment = _analyze_statements(statement.orelse, dict(environment))
            violations.update(body_violations)
            violations.update(else_violations)
            environment = _merge_environments(body_environment, else_environment)
        elif isinstance(statement, ast.Try):
            branch_results = [_analyze_statements(statement.body, dict(environment))]
            branch_results.extend(
                _analyze_statements(handler.body, dict(environment)) for handler in statement.handlers
            )
            if statement.orelse:
                branch_results.append(_analyze_statements(statement.orelse, dict(environment)))
            for branch_violations, _ in branch_results:
                violations.update(branch_violations)
            environment = _merge_environments(*(branch_environment for _, branch_environment in branch_results))
            final_violations, environment = _analyze_statements(statement.finalbody, environment)
            violations.update(final_violations)
        elif isinstance(statement, (ast.For, ast.AsyncFor, ast.While)):
            body_violations, body_environment = _analyze_statements(statement.body, dict(environment))
            else_violations, else_environment = _analyze_statements(statement.orelse, dict(environment))
            violations.update(body_violations)
            violations.update(else_violations)
            environment = _merge_environments(environment, body_environment, else_environment)
        elif isinstance(statement, (ast.With, ast.AsyncWith)):
            nested_violations, environment = _analyze_statements(statement.body, environment)
            violations.update(nested_violations)
        elif isinstance(statement, ast.Match):
            branch_results = [_analyze_statements(case.body, dict(environment)) for case in statement.cases]
            for branch_violations, _ in branch_results:
                violations.update(branch_violations)
            environment = _merge_environments(environment, *(item[1] for item in branch_results))
    return frozenset(violations), environment


def _raw_command_return_violations(function: ast.FunctionDef | ast.AsyncFunctionDef) -> frozenset[str]:
    return _analyze_statements(function.body, {})[0]


def _private_alias_payload() -> dict[str, Any]:
    return {"safe": "preserved", **{key: {"secret": key} for key in PRIVATE_ALIAS_KEYS}}


class _ActivityRuntimeRepository:
    def __init__(
        self,
        *,
        activities: list[dict[str, Any]],
        attempts: list[dict[str, Any]],
        deltas: list[dict[str, Any]],
    ) -> None:
        self._activities = copy.deepcopy(activities)
        self._attempts = copy.deepcopy(attempts)
        self._deltas = copy.deepcopy(deltas)

    def list_activity_runs(self, **_: Any) -> list[dict[str, Any]]:
        return copy.deepcopy(self._activities)

    def list_activity_attempts(self, **_: Any) -> list[dict[str, Any]]:
        return copy.deepcopy(self._attempts)

    def list_entity_deltas(self, **_: Any) -> list[dict[str, Any]]:
        return copy.deepcopy(self._deltas)

    def get_activity_run(self, activity_run_id: str) -> dict[str, Any]:
        return next(
            (copy.deepcopy(row) for row in self._activities if row.get("activity_run_id") == activity_run_id),
            {},
        )

    def get_action(self, _: str) -> dict[str, Any]:
        return {}

    def list_operation_events(self, _: str) -> list[dict[str, Any]]:
        return []


class _ActivitySummaryStore:
    def __init__(
        self,
        *,
        command: dict[str, Any],
        activities: list[dict[str, Any]],
        attempts: list[dict[str, Any]],
        deltas: list[dict[str, Any]],
    ) -> None:
        self._command = copy.deepcopy(command)
        self.repos = SimpleNamespace(
            workflow_runtime=_ActivityRuntimeRepository(
                activities=activities,
                attempts=attempts,
                deltas=deltas,
            )
        )

    def get_workflow_command(self, command_id: str) -> dict[str, Any]:
        if command_id == self._command.get("command_id"):
            return copy.deepcopy(self._command)
        return {}

    def list_workflow_commands(self, **_: Any) -> list[dict[str, Any]]:
        return [copy.deepcopy(self._command)]


def _projection_orchestrator(store: _ActivitySummaryStore) -> Any:
    from sourcing_agent.orchestrator import SourcingOrchestrator

    orchestrator = object.__new__(SourcingOrchestrator)
    orchestrator.store = store
    orchestrator._command_kernel = CommandKernel(store=store)
    return orchestrator


def test_partially_constructed_orchestrator_requires_explicit_kernel_injection() -> None:
    from sourcing_agent.orchestrator import SourcingOrchestrator

    command = {
        "command_id": "cmd-explicit-kernel",
        "command_type": "test.command",
        "owner": "test-owner",
        "status": "running",
    }
    store = _ActivitySummaryStore(command=command, activities=[], attempts=[], deltas=[])
    orchestrator = object.__new__(SourcingOrchestrator)
    orchestrator.store = store

    with pytest.raises(AttributeError):
        _ = orchestrator._command_kernel

    kernel = CommandKernel(store=store)
    orchestrator._command_kernel = kernel
    assert orchestrator._command_kernel is kernel
    assert orchestrator._workflow_command_api_record(command)["command_id"] == command["command_id"]


def test_public_json_copier_rejects_hostile_types_cycles_and_over_budget_members() -> None:
    class StatefulKey:
        def __init__(self) -> None:
            self.bool_calls = 0
            self.string_calls = 0

        def __hash__(self) -> int:
            return 7

        def __eq__(self, other: object) -> bool:
            return self is other

        def __bool__(self) -> bool:
            self.bool_calls += 1
            return True

        def __str__(self) -> str:
            self.string_calls += 1
            return "safe" if self.string_calls < 3 else "claim_token"

    class ThrowingMapping(dict[str, Any]):
        def items(self) -> Any:
            raise AssertionError("mapping subclass methods must never execute")

    kernel = CommandKernel(store=None)
    hostile_key = StatefulKey()
    cycle: dict[str, Any] = {"safe": "preserved"}
    cycle["self"] = cycle
    deep: dict[str, Any] = {"leaf": "preserved"}
    for _ in range(_WORKFLOW_PUBLIC_MIRROR_MAX_DEPTH + 20):
        deep = {"next": deep}
    projected = kernel._workflow_command_public_carrier_api_record(
        {
            "safe": "preserved",
            "hostile_key_record": {hostile_key: "secret", "safe": True},
            "throwing_mapping": ThrowingMapping({"claim_token": "secret"}),
            "cycle": cycle,
            "deep": deep,
            "bounded_collection": list(range(_WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS + 5)),
        }
    )

    assert hostile_key.bool_calls == 0
    assert hostile_key.string_calls == 0
    assert projected["hostile_key_record"] == {"safe": True}
    assert "throwing_mapping" not in projected
    assert projected["cycle"] == {"safe": "preserved"}
    assert len(projected["bounded_collection"]) == _WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS
    cursor = projected["deep"]
    copied_depth = 0
    while type(cursor) is dict and "next" in cursor:
        copied_depth += 1
        cursor = cursor["next"]
    assert copied_depth <= _WORKFLOW_PUBLIC_MIRROR_MAX_DEPTH

    node_heavy = {
        "items": [
            {"index": index, "values": list(range(100))}
            for index in range(_WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS)
        ]
    }
    bounded = kernel._workflow_command_public_carrier_api_record(node_heavy)

    def _node_count(value: Any) -> int:
        if type(value) is dict:
            return 1 + sum(_node_count(item) for item in value.values())
        if type(value) is list:
            return 1 + sum(_node_count(item) for item in value)
        return 1

    assert _node_count(bounded) <= _WORKFLOW_PUBLIC_MIRROR_MAX_NODES
    assert len(bounded["items"]) < _WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS


def test_alternating_command_activity_carriers_use_one_bounded_backend_traversal() -> None:
    class CountingKernel(CommandKernel):
        def __init__(self) -> None:
            super().__init__(store=None)
            self.command_projection_calls = 0

        def _workflow_command_api_record(
            self,
            command: dict[str, Any],
            **kwargs: Any,
        ) -> dict[str, Any]:
            self.command_projection_calls += 1
            return super()._workflow_command_api_record(command, **kwargs)

    nested_command: dict[str, Any] = {
        "command_id": "cmd-alternating-leaf",
        "command_type": "test.command",
        "owner": "test-owner",
        "status": "running",
    }
    chain_depth = 8
    for index in range(chain_depth):
        nested_command = {
            "command_id": f"cmd-alternating-{index}",
            "command_type": "test.command",
            "owner": "test-owner",
            "status": "running",
            "result": {
                "workflow_activity_run": {
                    "activity_run_id": f"activity-alternating-{index}",
                    "command_id": f"cmd-alternating-{index}",
                    "activity_type": "test.activity",
                    "owner": "test-activity-owner",
                    "metadata": {"workflow_command": nested_command},
                }
            },
        }

    kernel = CountingKernel()
    started_at = time.monotonic()
    projected = kernel._workflow_command_api_record(nested_command)
    elapsed_seconds = time.monotonic() - started_at

    assert projected["command_id"] == f"cmd-alternating-{chain_depth - 1}"
    assert kernel.command_projection_calls <= chain_depth + 1
    assert elapsed_seconds < 2.0

    singular_carriers = (
        _WORKFLOW_COMMAND_PUBLIC_CARRIER_FIELDS
        | _WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_FIELDS
        | _WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_FIELDS
        | _WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_FIELDS
    )
    plural_carriers = (
        _WORKFLOW_COMMAND_PUBLIC_CARRIER_LIST_FIELDS
        | _WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_LIST_FIELDS
        | _WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_LIST_FIELDS
        | _WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_LIST_FIELDS
    )

    def _assert_no_empty_carriers(value: Any) -> None:
        if type(value) is list:
            for item in value:
                _assert_no_empty_carriers(item)
            return
        if type(value) is not dict:
            return
        for key, item in value.items():
            if key in singular_carriers:
                assert item != {}
            elif key in plural_carriers and type(item) is list:
                assert all(member != {} for member in item)
            _assert_no_empty_carriers(item)

    _assert_no_empty_carriers(projected)


def test_control_policy_public_field_families_are_producer_owned_and_strict() -> None:
    policy = workflow_command_control_policy("test.command", owner="test-owner").to_record()
    assert frozenset(policy) == WORKFLOW_COMMAND_CONTROL_POLICY_PUBLIC_FIELDS
    assert (
        WORKFLOW_COMMAND_CONTROL_POLICY_STRING_FIELDS
        | WORKFLOW_COMMAND_CONTROL_POLICY_STRING_ARRAY_FIELDS
        | WORKFLOW_COMMAND_CONTROL_POLICY_BOOLEAN_FIELDS
    ) == WORKFLOW_COMMAND_CONTROL_POLICY_PUBLIC_FIELDS

    malformed = {field: False for field in WORKFLOW_COMMAND_CONTROL_POLICY_STRING_FIELDS}
    malformed.update({field: "bad" for field in WORKFLOW_COMMAND_CONTROL_POLICY_STRING_ARRAY_FIELDS})
    malformed.update({field: 1 for field in WORKFLOW_COMMAND_CONTROL_POLICY_BOOLEAN_FIELDS})
    malformed["safe_extension"] = "preserved"
    projected = CommandKernel(store=None)._workflow_activity_control_target_public_api_record(
        {"target_type": "workflow_command", "control_policy": malformed}
    )
    assert projected["control_policy"] == {
        **{field: [] for field in WORKFLOW_COMMAND_CONTROL_POLICY_STRING_ARRAY_FIELDS},
        "safe_extension": "preserved",
    }


def test_checked_in_public_field_contract_matches_descriptor_and_is_exactly_42() -> None:
    descriptor_fields = tuple(column.key for column in WORKFLOW_COMMANDS.columns)
    assert len(descriptor_fields) == 33
    assert WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS == (*descriptor_fields, *SAFE_DIAGNOSTIC_FIELDS)
    assert WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS == EXPECTED_DERIVED_FIELDS
    assert len(set(WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS) | set(WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS)) == 42
    assert WORKFLOW_COMMAND_OPERATION_SYNC_PUBLIC_FIELDS == EXPECTED_OPERATION_SYNC_FIELDS


def test_activity_public_field_contracts_are_descriptor_anchored_and_exact() -> None:
    assert WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS == (
        *(column.key for column in WORKFLOW_ACTIVITY_RUNS.columns),
        *EXPECTED_ACTIVITY_RUN_DERIVED_FIELDS,
    )
    assert WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_FIELDS == (
        *(column.key for column in WORKFLOW_ACTIVITY_ATTEMPTS.columns),
        *EXPECTED_ACTIVITY_ATTEMPT_DERIVED_FIELDS,
    )
    assert WORKFLOW_ENTITY_DELTA_PUBLIC_FIELDS == (
        *(column.key for column in WORKFLOW_ENTITY_DELTAS.columns),
        *EXPECTED_ENTITY_DELTA_DERIVED_FIELDS,
    )
    assert WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_FIELDS == EXPECTED_ACTIVITY_CONTROL_TARGET_FIELDS
    assert (
        len(WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS),
        len(WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_FIELDS),
        len(WORKFLOW_ENTITY_DELTA_PUBLIC_FIELDS),
        len(WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_FIELDS),
    ) == (23, 27, 26, 10)

    recursive_carriers = frozenset().union(
        _WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_FIELDS,
        _WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_FIELDS,
        _WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_FIELDS,
        _WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_LIST_FIELDS,
        _WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_LIST_FIELDS,
        _WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_LIST_FIELDS,
    )
    assert recursive_carriers == EXPECTED_RECURSIVE_ACTIVITY_CARRIER_FIELDS
    assert (
        _WORKFLOW_COMMAND_PUBLIC_CARRIER_FIELDS | _WORKFLOW_COMMAND_PUBLIC_CARRIER_LIST_FIELDS
        == EXPECTED_RECURSIVE_COMMAND_CARRIER_FIELDS
    )

    function = _function_definitions(ast.parse(ORCHESTRATOR_PATH.read_text(encoding="utf-8")))[
        "_workflow_command_control_public_api_record"
    ]
    served_carriers: set[str] = set()
    for assignment in ast.walk(function):
        if isinstance(assignment, ast.Assign):
            targets = assignment.targets
        elif isinstance(assignment, ast.AnnAssign):
            targets = [assignment.target]
        else:
            continue
        if not any(
            isinstance(target, ast.Name) and target.id in {"activity_projectors", "activity_list_projectors"}
            for target in targets
        ) or not isinstance(assignment.value, ast.Dict):
            continue
        served_carriers.update(
            key.value for key in assignment.value.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)
        )
    assert served_carriers == EXPECTED_SERVED_CONTROL_ACTIVITY_CARRIER_FIELDS


def test_backend_private_alias_and_diagnostic_boundary_matrix_is_fail_closed() -> None:
    kernel = CommandKernel(store=None)
    projected = kernel._workflow_command_api_record(
        {
            "command_id": "cmd-private-alias-matrix",
            "command_type": "test.command",
            "owner": "test-owner",
            "status": "claimed",
            "payload": _private_alias_payload(),
        }
    )

    assert projected["payload"] == {"safe": "preserved"}
    _assert_alias_keys_absent(projected)
    _assert_no_private_capability(projected)

    observation = kernel._workflow_command_observation(
        {
            "command_id": "cmd-observation",
            "command_type": "test.command",
            "input_artifact_refs": [{"claimToken": "secret"}, "artifact://safe"],
            "produced_entity_counts": {"claimAuthoritySeal": "secret", "safe": 1},
            "downstream_command_ids": [{"leaseToken": "secret"}, "cmd-safe"],
            "status": False,
            "attempt": "bad",
        },
        migration_phase="characterization",
    )
    assert observation["input_artifact_refs"] == ["artifact://safe"]
    assert observation["produced_entity_counts"] == {"safe": 1}
    assert observation["downstream_command_ids"] == ["cmd-safe"]
    assert "status" not in observation
    assert "attempt" not in observation
    assert observation["migration_phase"] == "characterization"
    assert observation["normal_path"] is True
    _assert_no_private_capability(observation)

    accepted: tuple[tuple[Any, int], ...] = (
        (0, 0),
        (7, 7),
        (1.0, 1),
        (-0.0, 0),
        (MAX_SAFE_DIAGNOSTIC_INTEGER, MAX_SAFE_DIAGNOSTIC_INTEGER),
    )
    rejected: tuple[Any, ...] = (
        True,
        -1,
        1.5,
        MAX_SAFE_DIAGNOSTIC_INTEGER + 1,
        9_223_372_036_854_775_807,
        "1",
    )
    for field in SAFE_DIAGNOSTIC_FIELDS:
        for value, expected in accepted:
            record = kernel._workflow_command_api_record({"command_id": "cmd-boundary", field: value})
            assert record[field] == expected
            assert isinstance(record[field], int)
        for value in rejected:
            record = kernel._workflow_command_api_record({"command_id": "cmd-boundary", field: value})
            assert field not in record


def test_backend_projector_is_sparse_closed_recursive_and_non_mutating() -> None:
    kernel = CommandKernel(store=None)
    command = _full_command_record()
    command.update(
        {
            "unknown_future_column": "must-not-project",
            **{field: f"d3c2a-{field}" for field in D3C2A_DORMANT_INTERNAL_FIELDS},
            "claim_token": "top-level-secret",
            "payload": {
                "business_value": "preserved",
                "artifact_digest": "legitimate-business-digest",
                "nested": {
                    "claimToken": "secret",
                    "claim-token-digest": "secret",
                    "CLAIM_AUTHORITY_ID": "secret",
                    "lastHeartbeatId": "secret",
                },
                "items": (
                    {"leaseToken": "secret"},
                    {"safe": True, "claimAuthorityEnvelope": "secret"},
                    SyntheticClaimAuthority(),
                ),
                "bootstrapReceipt": {"safe": "must-not-survive-private-carrier"},
                "scopedReviewSessionBootstrapAuthority": {"safe": "must-not-survive-private-carrier"},
            },
            "result": {"safe_result": 7, "claimTokenPreview": "secret"},
            "retry_policy": {
                "bootstrapAuthorityId": "secret",
                "bootstrap-authority-digest": "secret",
                "issuerDigest": "secret",
                "issuerRevision": 7,
                "leaseIdentity": "secret",
                "safe_retry": True,
            },
            "produced_entity_counts": {"finite": 1.5, "nan": float("nan"), "inf": float("inf")},
        }
    )
    original = copy.deepcopy(command)

    projected = kernel._workflow_command_api_record(command)

    assert command == original
    assert "unknown_future_column" not in projected
    assert "claim_token" not in projected
    assert set(D3C2A_DORMANT_INTERNAL_FIELDS).isdisjoint(projected)
    assert projected["claim_generation"] == 3
    assert projected["control_epoch"] == 3
    assert projected["payload"]["business_value"] == "preserved"
    assert projected["payload"]["artifact_digest"] == "legitimate-business-digest"
    assert projected["payload"]["nested"] == {}
    assert projected["payload"]["items"] == [{}, {"safe": True}]
    assert projected["result"] == {"safe_result": 7}
    assert projected["retry_policy"] == {"safe_retry": True}
    assert projected["produced_entity_counts"] == {"finite": 1.5}
    assert projected["artifact_refs"] == ["artifact_refs-value"]
    assert set(projected) == (
        set(WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS)
        | (set(WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS) - {"execution_summary"})
    )
    _assert_no_private_capability(projected)
    for private_field in D3B_PRIVATE_BOOTSTRAP_CAPABILITY_FIELDS:
        assert private_field in WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS

    sparse = kernel._workflow_command_api_record({"command_id": "cmd-sparse", "status": None})
    assert "status" not in sparse
    assert "payload" not in sparse
    assert "claim_generation" not in sparse


def test_d3c2a_raw_postgres_columns_remain_dormant_behind_the_closed_descriptor() -> None:
    raw_row = {column: None for column in WORKFLOW_COMMANDS.column_names()}
    raw_row.update(
        {
            **{field: f"d3c2a-{field}" for field in D3C2A_DORMANT_INTERNAL_FIELDS},
            "claim_generation": 7,
            "control_epoch": 11,
        }
    )

    mapped = WORKFLOW_COMMANDS.from_row(raw_row)
    descriptor_fields = tuple(column.key for column in WORKFLOW_COMMANDS.columns)

    assert set(mapped) == set(descriptor_fields)
    assert set(D3C2A_DORMANT_INTERNAL_FIELDS).isdisjoint(mapped)
    assert set(SAFE_DIAGNOSTIC_FIELDS).isdisjoint(mapped)
    assert len(descriptor_fields) == 33


def test_activity_carriers_and_canonical_execution_summary_share_the_capability_seal() -> None:
    command = {
        "command_id": "cmd-activity-summary",
        "command_type": "test.command",
        "owner": "test-owner",
        "status": "running",
        "payload": {"workspace_id": "default"},
        "execution_summary": {"source": "forged-command-row", "activity_count": 999},
    }
    activity = {
        "activity_run_id": "activity-1",
        "command_id": command["command_id"],
        "activity_type": command["command_type"],
        "owner": command["owner"],
        "status": "completed",
        "artifact_refs": _structured_artifact_refs(),
        "metadata": _private_alias_payload(),
        "non_finite": float("nan"),
    }
    attempt = {
        "attempt_id": "attempt-1",
        "activity_run_id": activity["activity_run_id"],
        "command_id": command["command_id"],
        "activity_type": command["command_type"],
        "owner": command["owner"],
        "status": "completed",
        "artifact_refs": _structured_artifact_refs(),
        "input": _private_alias_payload(),
        "output": _private_alias_payload(),
        "error": _private_alias_payload(),
        "rate_limit_ref": _private_alias_payload(),
        "metadata": _private_alias_payload(),
        "non_finite": float("inf"),
    }
    delta = {
        "delta_id": "delta-1",
        "activity_run_id": activity["activity_run_id"],
        "attempt_id": attempt["attempt_id"],
        "command_id": command["command_id"],
        "activity_type": command["command_type"],
        "owner": command["owner"],
        "status": "applied",
        "artifact_refs": _structured_artifact_refs(),
        "entity_payload": _private_alias_payload(),
        "non_finite": float("-inf"),
    }
    originals = copy.deepcopy((activity, attempt, delta))
    store = _ActivitySummaryStore(
        command=command,
        activities=[activity],
        attempts=[attempt],
        deltas=[delta],
    )
    orchestrator = _projection_orchestrator(store)

    projected_activity = orchestrator._workflow_activity_api_record(activity)
    projected_attempt = orchestrator._workflow_activity_attempt_api_record(attempt)
    projected_delta = orchestrator._workflow_entity_delta_api_record(delta)
    kernel = orchestrator._command_kernel

    assert (activity, attempt, delta) == originals
    for projected in (projected_activity, projected_attempt, projected_delta):
        assert "non_finite" not in projected
        _assert_alias_keys_absent(projected)
        _assert_no_private_capability(projected)
    assert projected_activity["metadata"] == {"safe": "preserved"}
    assert projected_activity["artifact_refs"] == _expected_structured_artifact_refs()
    for field in ("input", "output", "error", "rate_limit_ref", "metadata"):
        assert projected_attempt[field] == {"safe": "preserved"}
    assert projected_attempt["artifact_refs"] == _expected_structured_artifact_refs()
    assert projected_delta["entity_payload"] == {"safe": "preserved"}
    assert projected_delta["artifact_refs"] == _expected_structured_artifact_refs()
    for value, expected in (
        (0, 0),
        (1.0, 1),
        (-0.0, 0),
        (MAX_SAFE_DIAGNOSTIC_INTEGER, MAX_SAFE_DIAGNOSTIC_INTEGER),
    ):
        boundary = orchestrator._workflow_activity_attempt_api_record({**attempt, "attempt_number": value})
        assert boundary["attempt_number"] == expected
        assert isinstance(boundary["attempt_number"], int)
    for value in (True, -1, 1.5, MAX_SAFE_DIAGNOSTIC_INTEGER + 1, "1"):
        boundary = orchestrator._workflow_activity_attempt_api_record({**attempt, "attempt_number": value})
        assert "attempt_number" not in boundary

    malformed_command = kernel._workflow_command_api_record(
        {
            "command_id": 7,
            "input_artifact_refs": "bad",
            "output_artifact_refs": [1, "artifact://kept"],
            "produced_entity_counts": [],
            "payload": "bad",
            "artifact_refs": [1, "artifact://kept"],
            "attempt": "bad",
            "max_attempts": True,
            "status": False,
            "result": {
                "safe": True,
                "__proto__": {"polluted": True},
                "prototype": {"polluted": True},
                "constructor": {"polluted": True},
            },
        }
    )
    for field in (
        "command_id",
        "input_artifact_refs",
        "produced_entity_counts",
        "payload",
        "attempt",
        "max_attempts",
        "status",
    ):
        assert field not in malformed_command
    assert malformed_command["output_artifact_refs"] == ["artifact://kept"]
    assert malformed_command["artifact_refs"] == ["artifact://kept"]
    assert malformed_command["result"] == {"safe": True}

    malformed_activity = kernel._workflow_activity_public_api_record(
        {
            "activity_run_id": 7,
            "owner": "kept-owner",
            "status": False,
            "provider_ref": [],
            "input": "bad",
            "artifact_refs": "bad",
            "module_state_mutated": "false",
            "control_target": {
                "target_type": 9,
                "owner": "kept-owner",
                "command_status": 7,
                "display_contract": "bad",
                "control_policy": [],
                "fallback_status": False,
            },
        },
        include_trusted_derived=True,
    )
    assert malformed_activity == {"owner": "kept-owner", "control_target": {"owner": "kept-owner"}}
    malformed_attempt = kernel._workflow_activity_attempt_public_api_record(
        {
            "attempt_id": 7,
            "owner": "kept-owner",
            "attempt_number": True,
            "rate_limit_ref": [],
            "artifact_refs": {},
            "module_state_mutated": 0,
        },
        include_trusted_derived=True,
    )
    assert malformed_attempt == {"owner": "kept-owner"}
    malformed_delta = kernel._workflow_entity_delta_public_api_record(
        {
            "delta_id": 7,
            "owner": "kept-owner",
            "entity_payload": [],
            "artifact_refs": "bad",
            "module_state_mutated": "false",
        },
        include_trusted_derived=True,
    )
    assert malformed_delta == {"owner": "kept-owner"}
    nested_malformed_activity = kernel._workflow_command_api_record(
        {
            "command_id": "cmd-malformed-activity",
            "command_type": "test.command",
            "owner": "test-owner",
            "status": "completed",
            "result": {"workflow_activity_run": {"activity_run_id": 7, "artifact_refs": "bad"}},
        }
    )["result"]["workflow_activity_run"]
    assert nested_malformed_activity == {}

    nested_command = {
        "command_id": "cmd-in-direct-activity",
        "command_type": "test.command",
        "owner": "test-owner",
        "status": "completed",
        "runtime_namespace": "internal",
        "unknown_future_column": "drop",
        "execution_summary": {"source": "forged-command"},
        "payload": {"executionSummary": {"source": "forged-command-payload"}},
    }
    direct_activity_cases = (
        (
            kernel._workflow_activity_public_api_record,
            "activity_run_id",
            "metadata",
        ),
        (
            kernel._workflow_activity_attempt_public_api_record,
            "attempt_id",
            "output",
        ),
        (
            kernel._workflow_entity_delta_public_api_record,
            "delta_id",
            "entity_payload",
        ),
    )
    for projector, identity_field, container_field in direct_activity_cases:
        projected = projector(
            {
                identity_field: f"{identity_field}-nested-command",
                container_field: {
                    "safe": True,
                    "execution_summary": {"source": "forged-container"},
                    "executionSummary": {"source": "forged-container-alias"},
                    "workflow_command": nested_command,
                    "workflowCommands": [nested_command, "malformed"],
                },
            }
        )
        container = projected[container_field]
        assert "execution_summary" not in container
        assert "executionSummary" not in container
        for nested in (container["workflow_command"], container["workflowCommands"][0]):
            assert nested["command_id"] == nested_command["command_id"]
            assert "runtime_namespace" not in nested
            assert "unknown_future_column" not in nested
            assert "execution_summary" not in nested
            assert nested["payload"] == {}
        assert len(container["workflowCommands"]) == 1

    malformed_trusted_summary = kernel._workflow_command_trusted_execution_summary_api_record(
        {
            "source": 7,
            "fallback_used": "true",
            "module_state_mutated": 1,
            "activity_count": "3",
            "attempt_count": True,
            "entity_delta_count": float("nan"),
            "activity_status_counts": "bad",
            "attempt_status_counts": {"completed": "4"},
            "entity_delta_status_counts": {"applied": 2, "bad": False},
            "entity_delta_kind_counts": [],
            "latest_activity": {
                "activity_run_id": "activity-summary-typed",
                "unknown_future_column": "drop",
                "claimToken": "secret",
            },
            "latest_attempt": "bad",
            "latest_entity_delta": [],
            "sample_limit": "4",
            "sample_truncated": "yes",
        }
    )
    assert malformed_trusted_summary == {
        "activity_status_counts": {},
        "attempt_status_counts": {},
        "entity_delta_status_counts": {"applied": 2},
        "entity_delta_kind_counts": {},
        "latest_activity": {"activity_run_id": "activity-summary-typed"},
    }

    projected_command = orchestrator._workflow_command_api_record_with_execution_summary(command)
    summary = projected_command["execution_summary"]
    assert summary["source"] == "workflow_activity_runs+workflow_activity_attempts+workflow_entity_deltas"
    assert summary["activity_count"] == 1
    assert summary["attempt_count"] == 1
    assert summary["entity_delta_count"] == 1
    assert summary["latest_activity"]["metadata"] == {"safe": "preserved"}
    assert summary["latest_activity"]["artifact_refs"] == _expected_structured_artifact_refs()
    assert summary["latest_attempt"]["input"] == {"safe": "preserved"}
    assert summary["latest_attempt"]["artifact_refs"] == _expected_structured_artifact_refs()
    assert summary["latest_entity_delta"]["entity_payload"] == {"safe": "preserved"}
    assert summary["latest_entity_delta"]["artifact_refs"] == _expected_structured_artifact_refs()
    assert summary.get("activity_count") != 999
    _assert_alias_keys_absent(summary)
    _assert_no_private_capability(summary)

    operation_run = {
        "operation_run_id": "operation-1",
        "action_id": "",
        "workspace_id": "default",
        "status": "running",
        "progress": {"phase": "executing"},
        "metadata": {
            "workflow_command": {
                **command,
                "execution_summary": {"source": "forged-operation-metadata", "activity_count": 999},
            }
        },
    }
    projected_run = orchestrator._operation_run_api_record_with_status_summary(operation_run)
    trusted_run_summary = projected_run["status_summary"]["latest_workflow_command"]["execution_summary"]
    assert trusted_run_summary["source"] == summary["source"]
    assert trusted_run_summary["activity_count"] == 1
    assert "execution_summary" not in projected_run["metadata"]["workflow_command"]

    projected_control = orchestrator._operation_run_control_response_record(
        {"status": "running", "operation_run": operation_run}
    )
    trusted_control_summary = projected_control["operation_run"]["status_summary"]["latest_workflow_command"][
        "execution_summary"
    ]
    assert trusted_control_summary["source"] == summary["source"]
    assert trusted_control_summary["activity_count"] == 1

    malformed_operation_action = orchestrator._operation_action_api_record(
        {
            "action_id": 7,
            "action_type": "unknown.action",
            "budget": [],
            "input": "bad",
            "approval_status": False,
            "safe_extension": {"claimToken": "secret", "safe": True},
        }
    )
    assert "action_id" not in malformed_operation_action
    assert "budget" not in malformed_operation_action
    assert "input" not in malformed_operation_action
    assert "approval_status" not in malformed_operation_action
    assert malformed_operation_action["safe_extension"] == {"safe": True}
    _assert_no_private_capability(malformed_operation_action)

    malformed_operation_event = orchestrator._operation_event_api_record(
        {
            "event_id": 7,
            "sequence_number": "4",
            "payload": "bad",
            "actor": False,
            "safe_extension": {"claimToken": "secret", "safe": True},
        }
    )
    assert malformed_operation_event == {"safe_extension": {"safe": True}}
    _assert_no_private_capability(malformed_operation_event)

    malformed_operation_run = kernel._operation_run_public_api_record(
        {
            "operation_run_id": 7,
            "status": False,
            "progress": "bad",
            "control_state": {
                "can_cancel": "true",
                "allowed_actions": "bad",
                "safe_extension": True,
            },
            "safe_extension": {"claimToken": "secret", "safe": True},
        }
    )
    assert "operation_run_id" not in malformed_operation_run
    assert "status" not in malformed_operation_run
    assert "progress" not in malformed_operation_run
    assert malformed_operation_run["control_state"] == {
        "allowed_actions": [],
        "safe_extension": True,
    }
    assert malformed_operation_run["safe_extension"] == {"safe": True}
    _assert_no_private_capability(malformed_operation_run)


def test_activity_control_target_and_command_control_activity_payloads_are_closed() -> None:
    command = {
        "command_id": "cmd-control-activity",
        "workflow_run_id": "workflow-control-activity",
        "command_type": "test.command",
        "owner": "test-owner",
        "status": "running",
    }
    store = _ActivitySummaryStore(command=command, activities=[], attempts=[], deltas=[])
    orchestrator = _projection_orchestrator(store)
    raw_control_target = {
        "target_type": "workflow_command",
        "command_id": command["command_id"],
        "command_type": command["command_type"],
        "owner": command["owner"],
        "command_status": command["status"],
        "display_contract": _private_alias_payload(),
        "control_policy": _private_alias_payload(),
        "control_state": _private_alias_payload(),
        "activity_spine_policy": _private_alias_payload(),
        "fallback_status": "fail_closed",
        "unknown_future_target_field": "drop",
        "CLAIMToken": "secret",
    }
    closed_target = orchestrator._command_kernel._workflow_activity_control_target_public_api_record(raw_control_target)
    assert set(closed_target) == set(WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_FIELDS)
    assert "unknown_future_target_field" not in closed_target
    _assert_alias_keys_absent(closed_target)
    _assert_no_private_capability(closed_target)

    malformed_nested_target = orchestrator._command_kernel._workflow_activity_control_target_public_api_record(
        {
            "target_type": "workflow_command",
            "display_contract": {"schema_version": 7, "safe_extension": True},
            "control_state": {
                "can_cancel": "yes",
                "disabled_reasons": "bad",
                "safe_extension": True,
            },
        }
    )
    assert malformed_nested_target == {
        "target_type": "workflow_command",
        "display_contract": {"safe_extension": True},
        "control_state": {"safe_extension": True},
    }

    raw_activity_run = {
        "activity_run_id": "activity-control",
        "workspace_id": "workspace-control-activity",
        "workflow_run_id": command["workflow_run_id"],
        "command_id": command["command_id"],
        "activity_type": command["command_type"],
        "owner": command["owner"],
        "status": "cancelled",
        "artifact_refs": _structured_artifact_refs(),
        "unknown_future_activity_field": "drop",
    }
    raw_activity_attempt = {
        "attempt_id": "attempt-control",
        "workspace_id": raw_activity_run["workspace_id"],
        "activity_run_id": "activity-control",
        "workflow_run_id": command["workflow_run_id"],
        "command_id": command["command_id"],
        "attempt_number": 1.0,
        "activity_type": command["command_type"],
        "owner": command["owner"],
        "status": "cancelled",
        "artifact_refs": _structured_artifact_refs(),
        "unknown_future_attempt_field": "drop",
    }
    raw_entity_delta = {
        "delta_id": "delta-control",
        "workspace_id": raw_activity_run["workspace_id"],
        "activity_run_id": "activity-control",
        "workflow_run_id": command["workflow_run_id"],
        "attempt_id": "attempt-control",
        "command_id": command["command_id"],
        "activity_type": command["command_type"],
        "owner": command["owner"],
        "status": "discarded",
        "artifact_refs": _structured_artifact_refs(),
        "unknown_future_delta_field": "drop",
    }
    raw_activity_records = {
        "workflow_activity": raw_activity_run,
        "workflow_activity_run": raw_activity_run,
        "workflow_activity_attempt": raw_activity_attempt,
        "workflow_entity_delta": raw_entity_delta,
        "workflow_activity_runs": [raw_activity_run],
        "workflow_activity_attempts": [raw_activity_attempt],
        "workflow_entity_deltas": [raw_entity_delta],
    }
    store.repos.workflow_runtime._activities = [copy.deepcopy(raw_activity_run)]
    raw_response = {
        "status": "cancelled",
        "workflow_command": command,
        **raw_activity_records,
        "workflowActivityRun": {
            **raw_activity_run,
            "control_target": {"command_id": "forged", "command_status": "forged"},
        },
        "workflow-activities": [raw_activity_run],
    }
    originals = copy.deepcopy(raw_response)
    orchestrator._cancel_workflow_command_api_unprojected = lambda _command_id, _payload=None: raw_response
    orchestrator._retry_workflow_command_api_unprojected = lambda _command_id, _payload=None: raw_response
    orchestrator._resume_workflow_command_api_unprojected = lambda _command_id, _payload=None: raw_response

    responses = (
        orchestrator.cancel_workflow_command_api(command["command_id"]),
        orchestrator.retry_workflow_command_api(command["command_id"]),
        orchestrator.resume_workflow_command_api(command["command_id"]),
    )
    assert raw_response == originals
    expected_fields_by_carrier = {
        "workflow_activity": set(WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS),
        "workflow_activity_run": set(WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS),
        "workflow_activity_attempt": set(WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_FIELDS),
        "workflow_entity_delta": set(WORKFLOW_ENTITY_DELTA_PUBLIC_FIELDS),
    }
    for response in responses:
        assert "workflowActivityRun" not in response
        assert "workflow-activities" not in response
        for carrier, expected_fields in expected_fields_by_carrier.items():
            projected = response[carrier]
            assert set(projected) <= expected_fields
            assert projected["artifact_refs"] == _expected_structured_artifact_refs()
            assert set(projected["control_target"]) == set(WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_FIELDS)
            assert projected["module_state_mutated"] is False
            assert projected["mutation_contract"].startswith("read_only_")
            _assert_alias_keys_absent(projected)
            _assert_no_private_capability(projected)
        for carrier, expected_fields in {
            "workflow_activity_runs": set(WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS),
            "workflow_activity_attempts": set(WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_FIELDS),
            "workflow_entity_deltas": set(WORKFLOW_ENTITY_DELTA_PUBLIC_FIELDS),
        }.items():
            assert len(response[carrier]) == 1
            projected = response[carrier][0]
            assert set(projected) <= expected_fields
            assert projected["artifact_refs"] == _expected_structured_artifact_refs()
            assert set(projected["control_target"]) == set(WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_FIELDS)
            assert projected["module_state_mutated"] is False
            assert projected["mutation_contract"].startswith("read_only_")
            _assert_alias_keys_absent(projected)
            _assert_no_private_capability(projected)


def test_trusted_activity_provenance_is_reattached_only_from_consistent_current_evidence() -> None:
    command = {
        "command_id": "cmd-current-evidence",
        "workflow_run_id": "workflow-current-evidence",
        "command_type": "test.command",
        "owner": "test-owner",
        "status": "running",
    }
    activity = {
        "activity_run_id": "activity-current-evidence",
        "workspace_id": "workspace-current-evidence",
        "workflow_run_id": command["workflow_run_id"],
        "command_id": command["command_id"],
        "activity_type": command["command_type"],
        "owner": command["owner"],
        "status": "running",
        "control_target": {"command_id": "forged-source"},
    }
    store = _ActivitySummaryStore(command=command, activities=[activity], attempts=[], deltas=[])
    orchestrator = _projection_orchestrator(store)
    attempt = {
        "attempt_id": "attempt-current-evidence",
        "workspace_id": activity["workspace_id"],
        "activity_run_id": activity["activity_run_id"],
        "workflow_run_id": activity["workflow_run_id"],
        "command_id": command["command_id"],
        "attempt_number": 1,
        "status": "running",
        "activity_type": "forged-type",
        "owner": "forged-owner",
        "control_target": {"command_id": "forged-source"},
    }
    delta = {
        "delta_id": "delta-current-evidence",
        "workspace_id": activity["workspace_id"],
        "activity_run_id": activity["activity_run_id"],
        "workflow_run_id": activity["workflow_run_id"],
        "attempt_id": attempt["attempt_id"],
        "command_id": command["command_id"],
        "status": "pending",
        "activity_type": "forged-type",
        "owner": "forged-owner",
        "control_target": {"command_id": "forged-source"},
    }

    projected_activity = orchestrator._workflow_activity_api_record(activity)
    projected_forged_activity = orchestrator._workflow_activity_api_record(
        {**activity, "activity_type": "forged-type", "owner": "forged-owner"}
    )
    projected_attempt = orchestrator._workflow_activity_attempt_api_record(attempt)
    projected_delta = orchestrator._workflow_entity_delta_api_record(delta)
    for record in (projected_activity, projected_forged_activity, projected_attempt, projected_delta):
        assert record["control_target"]["command_id"] == command["command_id"]
        assert record["control_target"]["command_type"] == command["command_type"]
        assert record["control_target"]["owner"] == command["owner"]
    assert projected_forged_activity["activity_type"] == command["command_type"]
    assert projected_forged_activity["owner"] == command["owner"]
    assert projected_attempt["activity_type"] == command["command_type"]
    assert projected_attempt["owner"] == command["owner"]
    assert projected_delta["activity_type"] == command["command_type"]
    assert projected_delta["owner"] == command["owner"]

    cross_owner_activity = {
        **activity,
        "activity_type": "downstream.activity",
        "owner": "downstream-owner",
    }
    store.repos.workflow_runtime._activities = [cross_owner_activity]
    projected_cross_owner_activity = orchestrator._workflow_activity_api_record(cross_owner_activity)
    projected_cross_owner_attempt = orchestrator._workflow_activity_attempt_api_record(attempt)
    projected_cross_owner_delta = orchestrator._workflow_entity_delta_api_record(delta)
    for record in (
        projected_cross_owner_activity,
        projected_cross_owner_attempt,
        projected_cross_owner_delta,
    ):
        assert record["activity_type"] == cross_owner_activity["activity_type"]
        assert record["owner"] == cross_owner_activity["owner"]
        assert record["control_target"]["command_type"] == command["command_type"]
        assert record["control_target"]["owner"] == command["owner"]

    store.repos.workflow_runtime._activities = [{**activity, "command_id": "different-command"}]
    for record in (
        orchestrator._workflow_activity_api_record(activity),
        orchestrator._workflow_activity_attempt_api_record(attempt),
        orchestrator._workflow_entity_delta_api_record(delta),
    ):
        for field in ("activity_type", "owner", "control_target"):
            assert field not in record

    missing_link_activity = {
        **activity,
        "activity_run_id": "activity-missing-link",
        "command_id": "missing-command",
        "control_target": {"command_id": "forged-source"},
    }
    projected_missing = orchestrator._workflow_activity_api_record(missing_link_activity)
    assert "control_target" not in projected_missing

    store.repos.workflow_runtime._activities = [activity]
    store._command = {}
    for record in (
        orchestrator._workflow_activity_api_record(activity),
        orchestrator._workflow_activity_attempt_api_record(attempt),
        orchestrator._workflow_entity_delta_api_record(delta),
    ):
        assert record["activity_type"] == activity["activity_type"]
        assert record["owner"] == activity["owner"]
        assert "control_target" not in record


def test_control_response_envelopes_reproject_every_canonical_member_fail_closed() -> None:
    class CollidingHostileKey:
        def __init__(self) -> None:
            self.equality_calls = 0

        def __hash__(self) -> int:
            return hash("status")

        def __eq__(self, other: object) -> bool:
            self.equality_calls += 1
            raise AssertionError(f"hostile equality executed for {other!r}")

    command = {
        "command_id": "cmd-envelope",
        "command_type": "test.command",
        "owner": "test-owner",
        "status": "running",
    }
    store = _ActivitySummaryStore(command=command, activities=[], attempts=[], deltas=[])
    orchestrator = _projection_orchestrator(store)

    malformed_command_response = orchestrator._workflow_command_control_public_api_record(
        {
            "status": False,
            "reason": "must-not-override-malformed-reason",
            "command_status": [],
            "workflow_command": "bad",
            "operation_sync": [],
            "control_policy": {"provider_after_start_control_status": False},
            "control_state": [],
            "display_contract": False,
            "activity_spine_policy": "bad",
            "workflow_activity": [],
            "module_state_mutated": "true",
            "owner_specific_control": 1,
            "safe_extension": {"safe": True},
        }
    )
    assert malformed_command_response == {
        "safe_extension": {"safe": True},
        "status": "invalid",
        "reason": "malformed_workflow_command_control_response",
    }

    valid_command_response = orchestrator._workflow_command_control_public_api_record(
        {
            "status": "queued",
            "workflow_command": command,
            "control_policy": {
                "provider_after_start_control_status": False,
                "module_state_mutated_on_provider_after_start_control": "false",
            },
        }
    )
    assert valid_command_response["status"] == "queued"
    assert valid_command_response["workflow_command"]["command_id"] == command["command_id"]
    assert valid_command_response["control_policy"]["generic_control_contract"] == ("w11_workflow_command_control_v1")
    assert valid_command_response["control_policy"]["provider_after_start_control_status"] == "not_applicable"
    assert valid_command_response["control_policy"]["module_state_mutated_on_provider_after_start_control"] is False

    empty_sync_response = orchestrator._workflow_command_control_public_api_record(
        {"status": "already_applied", "operation_sync": {}}
    )
    assert empty_sync_response == {"status": "already_applied", "operation_sync": {}}

    malformed_operation_response = orchestrator._operation_run_control_response_record(
        {
            "status": 7,
            "reason": "must-not-override-malformed-reason",
            "action": [],
            "operation_run": "bad",
            "parent_operation_run": [],
            "workflow_command": False,
            "event": [],
            "events": ["bad", {"event_id": 7, "payload": "bad"}],
            "display_contract": [],
            "control_state": "bad",
            "module_state_mutated": "true",
            "request_schema_revalidation_required": 1,
            "safe_extension": {"safe": True},
        }
    )
    assert malformed_operation_response == {
        "safe_extension": {"safe": True},
        "status": "invalid",
        "reason": "malformed_operation_control_response",
        "events": [],
    }

    hostile_command_key = CollidingHostileKey()
    hostile_operation_key = CollidingHostileKey()
    assert orchestrator._workflow_command_control_public_api_record({hostile_command_key: "queued"}) == {
        "status": "invalid",
        "reason": "malformed_workflow_command_control_response",
    }
    assert orchestrator._operation_run_control_response_record({hostile_operation_key: "cancelled"}) == {
        "status": "invalid",
        "reason": "malformed_operation_control_response",
    }
    assert hostile_command_key.equality_calls == 0
    assert hostile_operation_key.equality_calls == 0


def test_operation_sync_and_recursive_public_carriers_share_the_same_sanitizer() -> None:
    kernel = CommandKernel(store=None)
    assert kernel._workflow_command_operation_sync_api_record(
        {
            "status": 7,
            "reason": "kept",
            "operation_run_id": False,
            "operation_status": [],
            "control_action": {},
            "command_status": 9,
            "operation_run": "bad",
            "event": [],
            "workflow_command": "bad",
        }
    ) == {"reason": "kept"}
    nested_malformed_sync = kernel._workflow_command_operation_sync_api_record(
        {
            "status": "running",
            "operation_run": {
                "operation_run_id": "operation-malformed-nested",
                "metadata": "bad",
                "status_summary": "bad",
            },
            "event": {"event_id": "event-malformed-nested", "payload": "bad"},
        }
    )
    assert nested_malformed_sync == {
        "status": "running",
        "operation_run": {"operation_run_id": "operation-malformed-nested"},
        "event": {"event_id": "event-malformed-nested"},
    }
    raw_command = {
        "command_id": "cmd-operation-sync",
        "command_type": "test.command",
        "owner": "test-owner",
        "status": "claimed",
        "claim_generation": 9,
        "control_epoch": 4,
        "claim_token": "secret",
        "unknown": "drop",
        "payload": {"execution_summary": {"source": "forged-command-payload"}},
        "result": {
            "safe": True,
            "claimAuthoritySeal": "secret",
            "workflow_activity_run": {
                "activity_run_id": "activity-in-command-result",
                "activity_type": "test.command",
                "owner": "test-owner",
                "status": "completed",
                "artifact_refs": _structured_artifact_refs(),
                "metadata": _private_alias_payload(),
                "control_target": {"command_id": "forged", "command_status": "running"},
                "module_state_mutated": True,
                "mutation_contract": "forged-write",
                "unknown_future_activity_field": "drop",
            },
            "workflowActivityRun": {
                "activity_run_id": "activity-alias-in-command-result",
                "activity_type": "test.command",
                "owner": "test-owner",
                "status": "completed",
                "artifact_refs": _structured_artifact_refs(),
                "unknown_future_activity_field": "drop",
            },
            "workflow_activity_attempt": {
                "attempt_id": "attempt-in-command-result",
                "activity_run_id": "activity-in-command-result",
                "activity_type": "forged-type",
                "owner": "forged-owner",
                "status": "completed",
                "control_target": {"command_id": "forged"},
                "module_state_mutated": True,
                "mutation_contract": "forged-write",
            },
            "workflow_entity_delta": {
                "delta_id": "delta-in-command-result",
                "activity_run_id": "activity-in-command-result",
                "activity_type": "forged-type",
                "owner": "forged-owner",
                "status": "applied",
                "control_target": {"command_id": "forged"},
                "module_state_mutated": True,
                "mutation_contract": "forged-write",
            },
        },
    }
    operation_sync = kernel._workflow_command_operation_sync_api_record(
        {
            "status": "running",
            "unknown_sync_field": "drop",
            "operation_run": {
                "metadata": {"execution_summary": {"source": "forged-operation-metadata"}},
                "result_ref": {
                    "safe": True,
                    "executionSummary": {"source": "forged-operation-result"},
                    "claim_token_digest": "secret",
                    "workflow_command": {
                        "command_id": "cmd-operation-sync-nested",
                        "command_type": "test.command",
                        "owner": "test-owner",
                        "status": "completed",
                        "runtime_namespace": "internal",
                        "unknown_future_column": "drop",
                    },
                },
            },
            "event": {
                "payload": {
                    "safe": True,
                    "leaseToken": "secret",
                    "executionSummary": {"source": "forged-event-payload"},
                    "workflowCommands": [
                        {
                            "command_id": "cmd-operation-sync-list",
                            "command_type": "test.command",
                            "owner": "test-owner",
                            "status": "completed",
                            "runtime_namespace": "internal",
                            "unknown_future_column": "drop",
                        }
                    ],
                }
            },
            "workflow_command": raw_command,
        }
    )

    assert set(operation_sync) == {"status", "operation_run", "event", "workflow_command"}
    nested_sync_command = operation_sync["operation_run"]["result_ref"]["workflow_command"]
    assert nested_sync_command["command_id"] == "cmd-operation-sync-nested"
    assert "runtime_namespace" not in nested_sync_command
    assert "unknown_future_column" not in nested_sync_command
    nested_sync_list_command = operation_sync["event"]["payload"]["workflowCommands"][0]
    assert nested_sync_list_command["command_id"] == "cmd-operation-sync-list"
    assert "runtime_namespace" not in nested_sync_list_command
    assert "unknown_future_column" not in nested_sync_list_command
    assert operation_sync["workflow_command"]["claim_generation"] == 9
    assert "unknown" not in operation_sync["workflow_command"]
    assert operation_sync["workflow_command"]["payload"] == {}
    assert operation_sync["operation_run"]["metadata"] == {}
    assert "executionSummary" not in operation_sync["operation_run"]["result_ref"]
    assert "executionSummary" not in operation_sync["event"]["payload"]
    _assert_no_private_capability(operation_sync)

    carrier = kernel._workflow_command_public_carrier_api_record(
        {
            "action": {"input": {"safe": True, "claimToken": "secret"}},
            "events": [{"payload": {"claim_receipt": {"token": "secret"}, "safe": True}}],
            "workflow_command": {
                **raw_command,
                "unknown_nested_descriptor": "drop",
                "execution_summary": {"latest_activity": {"claimToken": "secret", "safe": True}},
            },
            "latest_workflow_command": {
                **raw_command,
                "execution_summary": {"source": "forged-latest", "activity_count": 999},
            },
            "workflow_commands": [
                {
                    **raw_command,
                    "execution_summary": {"source": "forged-list", "activity_count": 999},
                }
            ],
            "execution_summary": {"latest_activity": {"metadata": {"claim-authority-seal": "secret", "safe": True}}},
        }
    )
    _assert_no_private_capability(carrier)
    assert carrier["events"] == [{"payload": {"safe": True}}]
    assert "unknown_nested_descriptor" not in carrier["workflow_command"]
    assert "execution_summary" not in carrier["workflow_command"]
    assert "execution_summary" not in carrier["latest_workflow_command"]
    assert "execution_summary" not in carrier["workflow_commands"][0]
    assert "execution_summary" not in carrier
    nested_activity = carrier["workflow_command"]["result"]["workflow_activity_run"]
    assert set(nested_activity) <= set(WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS)
    assert nested_activity["artifact_refs"] == _expected_structured_artifact_refs()
    assert nested_activity["metadata"] == {"safe": "preserved"}
    assert "unknown_future_activity_field" not in nested_activity
    assert "control_target" not in nested_activity
    assert "module_state_mutated" not in nested_activity
    assert "mutation_contract" not in nested_activity
    normalized_alias_activity = carrier["workflow_command"]["result"]["workflowActivityRun"]
    assert set(normalized_alias_activity) <= set(WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS)
    assert "unknown_future_activity_field" not in normalized_alias_activity
    for field in ("workflow_activity_attempt", "workflow_entity_delta"):
        nested_evidence = carrier["workflow_command"]["result"][field]
        assert "activity_type" not in nested_evidence
        assert "owner" not in nested_evidence
        assert "control_target" not in nested_evidence
        assert "module_state_mutated" not in nested_evidence
        assert "mutation_contract" not in nested_evidence

    nested_command_record = kernel._workflow_command_api_record(
        {
            "command_id": "cmd-recursive-outer",
            "command_type": "test.command",
            "owner": "test-owner",
            "status": "claimed",
            "result": {
                "workflow_command": {
                    "command_id": "cmd-recursive-inner",
                    "command_type": "test.command",
                    "owner": "test-owner",
                    "status": "completed",
                    "runtime_namespace": "internal",
                    "unknown_future_column": "drop",
                    "execution_summary": {"source": "forged", "activity_count": 999},
                    "payload": {
                        "execution_summary": {"source": "forged-payload"},
                        "executionSummary": {"source": "forged-payload-alias"},
                    },
                    "result": {
                        "workflowCommand": {
                            "command_id": "cmd-recursive-deep",
                            "command_type": "test.command",
                            "owner": "test-owner",
                            "status": "completed",
                            "unknown_future_column": "drop",
                        }
                    },
                },
                "workflowCommands": [
                    {
                        "command_id": "cmd-recursive-list",
                        "command_type": "test.command",
                        "owner": "test-owner",
                        "status": "completed",
                        "unknown_future_column": "drop",
                    },
                    "malformed",
                ],
            },
        }
    )
    nested_result = nested_command_record["result"]
    inner_command = nested_result["workflow_command"]
    assert "runtime_namespace" not in inner_command
    assert "unknown_future_column" not in inner_command
    assert "execution_summary" not in inner_command
    assert inner_command["payload"] == {}
    deep_command = inner_command["result"]["workflowCommand"]
    assert "unknown_future_column" not in deep_command
    assert [item["command_id"] for item in nested_result["workflowCommands"]] == ["cmd-recursive-list"]


def test_operation_sync_bypass_scan_is_manifested_and_alias_dataflow_aware() -> None:
    total_projector_calls = 0
    for path, expected_methods in OPERATION_SYNC_OWNER_MANIFEST.items():
        definitions = _function_definitions(ast.parse(path.read_text(encoding="utf-8")))
        assert expected_methods.keys() <= definitions.keys()
        for method_name, expected_call_count in expected_methods.items():
            function = definitions[method_name]
            actual_call_count = sum(
                1
                for node in ast.walk(function)
                if isinstance(node, ast.Call)
                and _terminal_call_name(node) == "_workflow_command_operation_sync_api_record"
            )
            assert actual_call_count == expected_call_count
            assert _raw_command_return_violations(function) == frozenset()
            total_projector_calls += actual_call_count
    assert total_projector_calls == 7

    unsafe_sources = {
        "alias": """
def candidate(command_payload):
    alias = command_payload
    return {"workflow_command": alias}
""",
        "dict_call": """
def candidate(command_payload):
    return {"workflow_command": dict(command_payload)}
""",
        "arbitrary_call": """
def candidate(command_payload):
    return {"workflow_command": clone_command(command_payload)}
""",
        "branch": """
def candidate(command_payload, use_copy):
    projected = self._workflow_command_api_record(command_payload)
    selected = projected if not use_copy else dict(command_payload)
    return {"workflow_command": selected}
""",
        "subscript_write": """
def candidate(command_payload):
    response = {}
    response["workflow_command"] = command_payload
    return response
""",
        "update": """
def candidate(command_payload):
    response = {}
    response.update({"workflow_command": command_payload})
    return response
""",
        "setdefault": """
def candidate(command_payload):
    response = {}
    response.setdefault("workflow_command", command_payload)
    return response
""",
        "mutated_alias": """
def candidate(command_payload):
    response = {}
    alias = response
    alias["workflow_command"] = command_payload
    return response
""",
        "mutated_copy": """
def candidate(command_payload):
    response = {}
    copied = response.copy()
    copied.update({"workflow_command": command_payload})
    return copied
""",
        "mutated_branch": """
def candidate(command_payload, use_copy):
    response = {}
    selected = response if not use_copy else response.copy()
    selected.setdefault("workflow_command", command_payload)
    return selected
""",
    }
    for label, source in unsafe_sources.items():
        candidate = _function_definitions(ast.parse(source))["candidate"]
        assert _raw_command_return_violations(candidate), label

    safe_source = """
def candidate(command_payload, branch):
    alias = command_payload
    response = {"workflow_command": alias}
    projected = self._workflow_command_operation_sync_api_record(response)
    direct = self._workflow_command_api_record(alias)
    safe_response = {}
    safe_response["workflow_command"] = direct
    safe_response.update({"latest_workflow_command": direct})
    safe_response.setdefault("workflow_commands", [direct])
    return projected if branch else safe_response
"""
    safe_candidate = _function_definitions(ast.parse(safe_source))["candidate"]
    assert _raw_command_return_violations(safe_candidate) == frozenset()


def test_route_inventory_is_exact_before_expected_manifest_comparison() -> None:
    definitions, graph = _orchestrator_call_graph()
    all_bindings = _all_api_route_bindings()
    actual_candidates = _route_candidate_bindings(all_bindings, definitions)

    assert len(WORKFLOW_COMMAND_ROUTE_MANIFEST) == 17
    assert len({binding[1] for binding in WORKFLOW_COMMAND_ROUTE_MANIFEST}) == 16
    assert len(WORKFLOW_ACTIVITY_ROUTE_MANIFEST) == 6
    assert len(COMPACT_COMMAND_ROUTE_MANIFEST) == 1
    assert actual_candidates == frozenset(PUBLIC_PROJECTION_ROUTE_MANIFEST)

    for binding, required_projectors in PUBLIC_PROJECTION_ROUTE_MANIFEST.items():
        reachable = _reachable_calls(binding[3], definitions, graph)
        assert required_projectors <= reachable, {
            "binding": binding,
            "required_projectors": sorted(required_projectors),
            "reachable": sorted(reachable),
        }

    synthetic_binding: RouteBinding = (
        "GET",
        "/api/audit/synthetic-raw-workflow-command",
        "get_synthetic_raw_workflow_command",
        "synthetic_raw_workflow_command_api",
    )
    synthetic_orchestrator_source = (
        ORCHESTRATOR_PATH.read_text(encoding="utf-8")
        + """

def synthetic_raw_workflow_command_api(self, command_id):
    command = self.store.get_workflow_command(command_id)
    response = {}
    response["workflow_command"] = command
    return response
"""
    )
    synthetic_api_source = (
        API_PATH.read_text(encoding="utf-8")
        + """

def get_synthetic_raw_workflow_command(request):
    return orchestrator.synthetic_raw_workflow_command_api("synthetic-command")

add(["GET"], "/api/audit/synthetic-raw-workflow-command", get_synthetic_raw_workflow_command)
"""
    )
    synthetic_definitions, synthetic_graph = _call_graph_for_source(synthetic_orchestrator_source)
    synthetic_candidates = _route_candidate_bindings(
        _all_api_route_bindings(synthetic_api_source),
        synthetic_definitions,
    )
    assert synthetic_candidates == actual_candidates | {synthetic_binding}
    projector_names = frozenset().union(*PUBLIC_PROJECTION_ROUTE_MANIFEST.values())
    assert not (_reachable_calls(synthetic_binding[3], synthetic_definitions, synthetic_graph) & projector_names)


def test_frontend_schema_and_mappers_are_closed_and_operation_sync_is_typed() -> None:
    schema = json.loads(FRONTEND_SCHEMA_PATH.read_text(encoding="utf-8"))
    workflow_command = schema["$defs"]["WorkflowCommandRecord"]
    operation_sync = schema["$defs"]["WorkflowCommandOperationSync"]
    expected_fields = set(WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS) | set(WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS)

    assert workflow_command["additionalProperties"] is False
    assert set(workflow_command["properties"]) == expected_fields
    assert len(workflow_command["properties"]) == 42
    assert operation_sync["additionalProperties"] is False
    assert set(operation_sync["properties"]) == set(EXPECTED_OPERATION_SYNC_FIELDS)
    assert _schema_ref_count(schema, "#/$defs/WorkflowCommandRecord") == 7
    for field in SAFE_DIAGNOSTIC_FIELDS:
        diagnostic_schema = workflow_command["properties"][field]
        assert diagnostic_schema["type"] == "integer"
        assert diagnostic_schema["minimum"] == 0
        assert diagnostic_schema["maximum"] == MAX_SAFE_DIAGNOSTIC_INTEGER

    types_source = FRONTEND_TYPES_PATH.read_text(encoding="utf-8")
    adapter_source = FRONTEND_ADAPTER_PATH.read_text(encoding="utf-8")
    runtime_contract_source = FRONTEND_RUNTIME_CONTRACT_PATH.read_text(encoding="utf-8")
    demo_source = FRONTEND_DEMO_API_PATH.read_text(encoding="utf-8")
    assert (
        _typescript_interface_fields(
            types_source,
            "WorkflowCommandRecord",
            "WorkflowCommandOperationSync",
        )
        == expected_fields
    )
    assert (
        _typescript_mapper_fields(
            adapter_source,
            "mapWorkflowCommandRecord",
            "mapWorkflowCommandOperationSync",
        )
        == expected_fields
    )
    assert (
        _typescript_string_collection(
            demo_source,
            "WORKFLOW_COMMAND_PUBLIC_WIRE_FIELDS",
        )
        == expected_fields
    )
    activity_layers = (
        (
            "WorkflowActivityRecord",
            "WorkflowActivityAttemptRecord",
            "mapWorkflowActivityRecord",
            "mapWorkflowActivityAttemptRecord",
            "WORKFLOW_ACTIVITY_PUBLIC_WIRE_FIELDS",
            frozenset(WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS),
        ),
        (
            "WorkflowActivityAttemptRecord",
            "WorkflowEntityDeltaRecord",
            "mapWorkflowActivityAttemptRecord",
            "mapWorkflowEntityDeltaRecord",
            "WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_WIRE_FIELDS",
            frozenset(WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_FIELDS),
        ),
        (
            "WorkflowEntityDeltaRecord",
            "AcquisitionDiscoveryLaneRecord",
            "mapWorkflowEntityDeltaRecord",
            "mapWorkflowActivityListResponse",
            "WORKFLOW_ENTITY_DELTA_PUBLIC_WIRE_FIELDS",
            frozenset(WORKFLOW_ENTITY_DELTA_PUBLIC_FIELDS),
        ),
    )
    for schema_name, next_type, mapper, next_mapper, demo_fields, backend_fields in activity_layers:
        assert schema["$defs"][schema_name]["additionalProperties"] is False
        assert frozenset(schema["$defs"][schema_name]["properties"]) == backend_fields
        assert _typescript_interface_fields(types_source, schema_name, next_type) == backend_fields
        assert _typescript_mapper_fields(adapter_source, mapper, next_mapper) == backend_fields
        assert _typescript_string_collection(demo_source, demo_fields) == backend_fields
    expected_control_target_fields = frozenset(WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_FIELDS)
    control_target_schema = schema["$defs"]["WorkflowActivityControlTarget"]
    assert control_target_schema["additionalProperties"] is False
    assert frozenset(control_target_schema["properties"]) == expected_control_target_fields
    assert (
        _typescript_interface_fields(
            types_source,
            "WorkflowActivityControlTarget",
            "WorkflowActivityRecord",
        )
        == expected_control_target_fields
    )
    assert (
        _typescript_mapper_fields(
            adapter_source,
            "mapWorkflowActivityControlTarget",
            "mapWorkflowActivityRecord",
        )
        == expected_control_target_fields
    )
    assert (
        _typescript_string_collection(
            demo_source,
            "WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_WIRE_FIELDS",
        )
        == expected_control_target_fields
    )
    json_value_array_schema = {"type": "array", "items": {}}
    for schema_name in (
        "WorkflowActivityRecord",
        "WorkflowActivityAttemptRecord",
        "WorkflowEntityDeltaRecord",
    ):
        assert schema["$defs"][schema_name]["properties"]["artifact_refs"] == json_value_array_schema
    expected_control_activity_carriers = {
        "workflow_activity",
        "workflow_activity_run",
        "workflow_activity_attempt",
        "workflow_entity_delta",
        "workflow_activity_runs",
        "workflow_activity_attempts",
        "workflow_entity_deltas",
    }
    control_response_properties = schema["$defs"]["WorkflowCommandControlResponse"]["properties"]
    assert (
        set(control_response_properties) & EXPECTED_RECURSIVE_ACTIVITY_CARRIER_FIELDS
        == expected_control_activity_carriers
    )
    assert control_response_properties["workflow_activity_run"] == {"$ref": "#/$defs/WorkflowActivityRecord"}
    assert (
        _typescript_interface_fields(
            types_source,
            "WorkflowCommandControlResponse",
            "WorkflowActivityControlTarget",
        )
        & EXPECTED_RECURSIVE_ACTIVITY_CARRIER_FIELDS
        == expected_control_activity_carriers
    )
    assert (
        _typescript_mapper_fields(
            adapter_source,
            "mapWorkflowCommandControlResponse",
            "mapWorkflowActivityControlTarget",
        )
        & EXPECTED_RECURSIVE_ACTIVITY_CARRIER_FIELDS
        == expected_control_activity_carriers
    )
    assert workflow_command["properties"]["artifact_refs"] == {
        "type": "array",
        "items": {"type": "string"},
    }
    attempt_number_schema = schema["$defs"]["WorkflowActivityAttemptRecord"]["properties"]["attempt_number"]
    assert attempt_number_schema == {
        "type": "integer",
        "minimum": 0,
        "maximum": MAX_SAFE_DIAGNOSTIC_INTEGER,
    }

    expected_private_roots = frozenset(WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS)
    assert "WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_ROOTS" in adapter_source
    assert "WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_ROOTS" in demo_source
    assert _typescript_private_family_roots(adapter_source) == expected_private_roots
    assert _typescript_private_family_roots(demo_source) == expected_private_roots
    assert "new Set(WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_ROOTS)" in adapter_source
    assert "new Set(WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_ROOTS)" in demo_source

    type_segment = _typescript_segment(
        types_source,
        "export interface WorkflowCommandRecord",
        "export interface WorkflowCommandExecutionSummary",
    )
    adapter_segment = _typescript_segment(
        adapter_source,
        "export function mapWorkflowCommandRecord",
        "export function mapWorkflowCommandListResponse",
    )
    demo_segment = _typescript_segment(
        demo_source,
        "function deriveWorkflowCommandRecord",
        "function deriveWorkflowActivityRecord",
    )
    assert "extends JsonObject" not in type_segment
    assert "[key: string]" not in type_segment
    assert "...(source as JsonObject)" not in adapter_segment
    assert "raw: record" not in demo_segment
    assert "WorkflowCommandOperationSync" in types_source
    assert "mapWorkflowCommandOperationSync" in adapter_source
    artifact_ref_type_segments = (
        ("WorkflowActivityRecord", "WorkflowActivityAttemptRecord"),
        ("WorkflowActivityAttemptRecord", "WorkflowEntityDeltaRecord"),
        ("WorkflowEntityDeltaRecord", "AcquisitionDiscoveryLaneRecord"),
    )
    for interface_name, next_interface_name in artifact_ref_type_segments:
        segment = _typescript_segment(
            types_source,
            f"export interface {interface_name}",
            f"export interface {next_interface_name}",
        )
        assert re.search(r"^\s{2}artifact_refs\?: JsonValue\[\];", segment, flags=re.MULTILINE)
    assert re.search(r"^\s{2}artifact_refs\?: string\[\];", type_segment, flags=re.MULTILINE)
    for field in SAFE_DIAGNOSTIC_FIELDS:
        assert re.search(rf"^\s{{2}}{field}\?: number;", type_segment, flags=re.MULTILINE)

    number_record_ref = {"$ref": "#/$defs/NumberRecord"}
    assert schema["$defs"]["NumberRecord"] == {
        "type": "object",
        "additionalProperties": {"type": "number"},
    }
    numeric_record_fields = {
        "OperationRunStatusSummary": ("command_status_counts",),
        "WorkflowCommandExecutionSummary": (
            "activity_status_counts",
            "attempt_status_counts",
            "entity_delta_status_counts",
            "entity_delta_kind_counts",
        ),
    }
    for schema_name, fields in numeric_record_fields.items():
        for field in fields:
            assert schema["$defs"][schema_name]["properties"][field] == number_record_ref
            assert re.search(rf"^\s{{2}}{field}\?: NumberRecord;", types_source, flags=re.MULTILINE)
    assert "export type NumberRecord = Record<string, number>;" in types_source

    expected_status_contracts = {
        "OperationActionDetailResponse": (
            "OperationActionDetailSuccessStatus",
            ["ok", "queued", "approval_required", "rejected"],
        ),
        "OperationRunProvenanceResponse": ("OperationRunProvenanceSuccessStatus", ["ok"]),
        "OperationRunControlResponse": (
            "OperationRunControlAppliedOutcome",
            ["cancelled", "queued", "planned"],
        ),
        "WorkflowCommandControlResponse": (
            "WorkflowCommandControlAppliedOutcome",
            ["cancelled", "queued"],
        ),
    }
    for response_name, (status_name, values) in expected_status_contracts.items():
        assert schema["$defs"][response_name]["properties"]["status"] == {"$ref": f"#/$defs/{status_name}"}
        assert schema["$defs"][status_name]["enum"] == values
    for symbol in (
        "OPERATION_ACTION_DECISION_APPLIED_OUTCOMES",
        "OPERATION_RUN_CONTROL_APPLIED_OUTCOMES",
        "OPERATION_RUN_PROVENANCE_SUCCESS_STATUSES",
        "WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES",
        "requirePublicResponseOutcome",
    ):
        assert symbol in types_source or symbol in demo_source or symbol in runtime_contract_source
    runtime_contract_import = 'from "./frontend_api_runtime_contract"'
    assert runtime_contract_import in types_source
    assert runtime_contract_import in adapter_source
    assert 'from "../../../contracts/frontend_api_runtime_contract"' in demo_source
    for symbol in (
        "OPERATION_ACTION_DECISION_APPLIED_OUTCOMES",
        "OPERATION_RUN_CONTROL_APPLIED_OUTCOMES",
        "OPERATION_RUN_PROVENANCE_SUCCESS_STATUSES",
        "WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES",
        "WORKFLOW_PUBLIC_PROJECTION_LIMITS",
    ):
        assert f"export const {symbol}" in runtime_contract_source
        assert f"const {symbol} =" not in demo_source
    for projector_source in (adapter_source, demo_source):
        assert "defaultMemo: new WeakMap<object" in projector_source
        assert "executionSummaryMemo: new WeakMap<object" in projector_source
        assert "PublicProjectionOccurrence" in projector_source
        assert "entry.nodes, entry.bytes" in projector_source
        assert "entry.blocked = true" in projector_source
        assert "Object.getOwnPropertyDescriptor" in projector_source
        assert "Reflect.ownKeys" in projector_source
    demo_status_validator = _typescript_segment(
        demo_source,
        "function requirePublicResponseStatus",
        "function requirePublicResponseOutcome",
    )
    assert "asString(" not in demo_status_validator
    assert "value.length === 0" in demo_status_validator
    assert "return value;" in demo_status_validator

    provider_after_start_string_fields = {
        "generic_control_contract",
        "provider_after_start_control_contract",
        "provider_after_start_control_status",
        "provider_after_start_control_mode",
        "provider_after_start_control_owner",
        "provider_after_start_control_blocked_reason",
    }
    policy_properties = schema["$defs"]["WorkflowCommandControlPolicy"]["properties"]
    for field in provider_after_start_string_fields:
        assert policy_properties[field] == {"type": "string"}
        assert re.search(rf"^\s{{2}}{field}\?: string;", types_source, flags=re.MULTILINE)
        assert f"{field}: asOptionalString(source.{field})" in adapter_source or field in adapter_source
    assert policy_properties["provider_after_start_control_upgrade_requirements"] == {"$ref": "#/$defs/StringArray"}
    assert policy_properties["module_state_mutated_on_provider_after_start_control"] == {"type": "boolean"}
    assert "provider_after_start_control_upgrade_requirements?: string[];" in types_source
    assert "module_state_mutated_on_provider_after_start_control?: boolean;" in types_source
    assert "provider_after_start_control_upgrade_requirements" in demo_source
    assert "module_state_mutated_on_provider_after_start_control" in demo_source


def test_frontend_action_methods_compile_with_exact_status_outcomes() -> None:
    compile_result = subprocess.run(
        [
            "node",
            str(TYPESCRIPT_COMPILER_PATH),
            "--noEmit",
            "--skipLibCheck",
            "--strict",
            "false",
            "--target",
            "ES2022",
            "--module",
            "ESNext",
            "--moduleResolution",
            "Bundler",
            "--lib",
            "ES2022,DOM,DOM.Iterable",
            "--types",
            "vite/client",
            str(FRONTEND_ACTION_STATUS_TYPES_PATH),
        ],
        cwd=REPO_ROOT / "frontend-demo",
        capture_output=True,
        text=True,
        check=False,
    )
    assert compile_result.returncode == 0, compile_result.stdout + compile_result.stderr


def test_frontend_mappers_executably_drop_unknown_and_nested_capability_fields(tmp_path: Path) -> None:
    public_adapter_bundle = tmp_path / "frontend_api_adapter.cjs"
    demo_api_bundle = tmp_path / "demo_api.cjs"
    runtime_contract_bundle = tmp_path / "frontend_api_runtime_contract.cjs"
    bundle_script = """
const esbuild = require(process.argv[1]);
esbuild.buildSync({
  entryPoints: [process.argv[2]],
  outfile: process.argv[3],
  bundle: true,
  platform: "node",
  format: "cjs",
  target: "node20",
  define: { "import.meta.env": '{"VITE_API_BASE_URL":"https://api.test"}' },
});
"""
    for entrypoint, output_path in (
        (FRONTEND_ADAPTER_PATH, public_adapter_bundle),
        (FRONTEND_DEMO_API_PATH, demo_api_bundle),
        (FRONTEND_RUNTIME_CONTRACT_PATH, runtime_contract_bundle),
    ):
        build = subprocess.run(
            [
                "node",
                "-e",
                bundle_script,
                str(ESBUILD_MODULE_PATH),
                str(entrypoint),
                str(output_path),
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert build.returncode == 0, build.stderr

    assertion_script = """
const assert = require("node:assert/strict");
const publicAdapter = require(process.argv[1]);
const demoApi = require(process.argv[2]);
const runtimeContract = require(process.argv[6]);

function structuredArtifactRefs() {
  return [
    "artifact://profile/plain",
    {
      profile_url: "https://example.test/researcher",
      raw_path: "profiles/researcher.json",
      metadata: { safe: "preserved", CLAIMToken: "secret" },
      claimReceiptEnvelope: { secret: true },
    },
  ];
}

const expectedStructuredArtifactRefs = [
  "artifact://profile/plain",
  {
    profile_url: "https://example.test/researcher",
    raw_path: "profiles/researcher.json",
    metadata: { safe: "preserved" },
  },
];

const input = {
  command_id: "cmd-frontend-projection",
  command_type: "test.command",
  owner: "test-owner",
  status: "claimed",
  claim_generation: 5,
  control_epoch: 8,
  unknown_future_column: "drop",
  claim_token: "secret",
  payload: {
    safe: true,
    artifact_digest: "legitimate-business-digest",
    nested: { claimTokenDigest: "secret", safe: "preserved" },
    CLAIMToken: "secret",
    CLAIMAuthoritySeal: "secret",
    claimCapabilityPreview: "secret",
    claimIdentityEnvelope: "secret",
    claimReceiptEnvelope: "secret",
    claimSecretPreview: "secret",
    leaseIdentityDigest: "secret",
    leaseTokenDigest: "secret",
    bootstrapReceipt: { safe: "must-not-survive-private-carrier" },
    scopedReviewSessionBootstrapAuthority: { safe: "must-not-survive-private-carrier" },
  },
  result: { safe_result: 7, "claim-authority-seal": "secret" },
  produced_entity_counts: { finite: 1.5, nan: Number.NaN, inf: Number.POSITIVE_INFINITY },
  artifact_refs: ["artifact://command/plain"],
  execution_summary: { source: "trusted-derived", activity_count: 3 },
  retry_policy: {
    bootstrapAuthorityId: "secret",
    "bootstrap-authority-digest": "secret",
    issuerDigest: "secret",
    issuerRevision: 7,
    leaseIdentity: "secret",
    safe_retry: true,
  },
};

const privateRoots = JSON.parse(process.argv[3]);
const forbiddenAliasKeys = new Set(JSON.parse(process.argv[4]));
const maxSafeDiagnosticInteger = Number(process.argv[5]);

function normalizedKey(value) {
  return String(value || "").trim().replace(/[-\\s]+/g, "_")
    .replace(/(?<=[A-Z])(?=[A-Z][a-z])/g, "_")
    .replace(/(?<=[a-z0-9])(?=[A-Z])/g, "_")
    .replace(/_+/g, "_").replace(/^_+|_+$/g, "").toLowerCase();
}

function assertNoPrivate(value) {
  if (Array.isArray(value)) {
    for (const item of value) assertNoPrivate(item);
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const [key, item] of Object.entries(value)) {
    const normalized = normalizedKey(key);
    const compact = normalized.replaceAll("_", "");
    assert.equal(forbiddenAliasKeys.has(key), false);
    assert.equal(privateRoots.some((root) => {
      const compactRoot = root.replaceAll("_", "");
      return normalized === root || normalized.startsWith(`${root}_`)
        || compact === compactRoot || compact.startsWith(compactRoot);
    }), false);
    assertNoPrivate(item);
  }
}

function assertNoHazardousOwn(value) {
  if (Array.isArray(value)) {
    for (const item of value) assertNoHazardousOwn(item);
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const field of ["__proto__", "prototype", "constructor"]) {
    assert.equal(Object.prototype.hasOwnProperty.call(value, field), false);
  }
  for (const item of Object.values(value)) assertNoHazardousOwn(item);
}

const mapped = publicAdapter.mapWorkflowCommandRecord(input);
const demo = demoApi.deriveWorkflowCommandRecord(input);
assert.equal(mapped.unknown_future_column, undefined);
assert.equal(mapped.claim_token, undefined);
assert.equal(mapped.payload.safe, true);
assert.equal(mapped.payload.artifact_digest, "legitimate-business-digest");
assert.deepEqual(mapped.payload.nested, { safe: "preserved" });
assert.deepEqual(mapped.result, { safe_result: 7 });
assert.deepEqual(mapped.produced_entity_counts, { finite: 1.5 });
assert.deepEqual(mapped.retry_policy, { safe_retry: true });
assert.deepEqual(mapped.artifact_refs, ["artifact://command/plain"]);
assert.equal(mapped.execution_summary.source, "trusted-derived");
assert.equal(mapped.execution_summary.activity_count, 3);
assert.equal(demo.raw.unknown_future_column, undefined);
assert.equal(demo.raw.claim_token, undefined);
assert.equal(demo.raw.payload.artifact_digest, "legitimate-business-digest");
assert.deepEqual(demo.raw.produced_entity_counts, { finite: 1.5 });
assert.deepEqual(demo.raw.retry_policy, { safe_retry: true });
assert.deepEqual(demo.raw.artifact_refs, ["artifact://command/plain"]);
assert.equal(demo.raw.execution_summary.source, "trusted-derived");
assert.equal(demo.raw.execution_summary.activity_count, 3);
assertNoPrivate(mapped);
assertNoPrivate(demo);

const providerAfterStartPolicy = {
  generic_control_contract: "generic-control-v1",
  provider_after_start_control_contract: "provider-after-start-v1",
  provider_after_start_control_status: "supported",
  provider_after_start_control_mode: "owner_delegate",
  provider_after_start_control_owner: "provider-owner",
  provider_after_start_control_blocked_reason: "",
  provider_after_start_control_upgrade_requirements: ["lease_fence", "owner_ack"],
  module_state_mutated_on_provider_after_start_control: false,
};
const mappedProviderAfterStartPolicy = publicAdapter.mapWorkflowCommandControlPolicy(
  providerAfterStartPolicy,
);
const demoProviderAfterStartPolicy = demoApi.deriveWorkflowCommandRecord({
  command_id: "cmd-provider-after-start-policy",
  control_policy: providerAfterStartPolicy,
}).controlPolicy;
assert.deepEqual(demoProviderAfterStartPolicy.raw, providerAfterStartPolicy);
for (const [field, value] of Object.entries(providerAfterStartPolicy)) {
  assert.deepEqual(mappedProviderAfterStartPolicy[field], value);
}
assert.equal(demoProviderAfterStartPolicy.genericControlContract, "generic-control-v1");
assert.equal(
  demoProviderAfterStartPolicy.providerAfterStartControlContract,
  "provider-after-start-v1",
);
assert.deepEqual(
  demoProviderAfterStartPolicy.providerAfterStartControlUpgradeRequirements,
  ["lease_fence", "owner_ack"],
);
assert.equal(demoProviderAfterStartPolicy.moduleStateMutatedOnProviderAfterStartControl, false);

const malformedProviderAfterStartPolicy = {
  generic_control_contract: 7,
  provider_after_start_control_contract: false,
  provider_after_start_control_status: [],
  provider_after_start_control_mode: {},
  provider_after_start_control_owner: 9,
  provider_after_start_control_blocked_reason: true,
  provider_after_start_control_upgrade_requirements: "bad",
  module_state_mutated_on_provider_after_start_control: "false",
};
const malformedMappedProviderAfterStartPolicy = publicAdapter.mapWorkflowCommandControlPolicy(
  malformedProviderAfterStartPolicy,
);
const malformedDemoProviderAfterStartPolicy = demoApi.deriveWorkflowCommandRecord({
  command_id: "cmd-malformed-provider-after-start-policy",
  control_policy: malformedProviderAfterStartPolicy,
}).controlPolicy;
for (const field of [
  "generic_control_contract",
  "provider_after_start_control_contract",
  "provider_after_start_control_status",
  "provider_after_start_control_mode",
  "provider_after_start_control_owner",
  "provider_after_start_control_blocked_reason",
  "module_state_mutated_on_provider_after_start_control",
]) {
  assert.equal(mappedProviderAfterStartPolicy[field] === undefined, false);
  assert.equal(malformedMappedProviderAfterStartPolicy[field], undefined);
  assert.equal(malformedDemoProviderAfterStartPolicy.raw[field], undefined);
}
assert.deepEqual(
  malformedMappedProviderAfterStartPolicy.provider_after_start_control_upgrade_requirements,
  [],
);
assert.deepEqual(
  malformedDemoProviderAfterStartPolicy.raw.provider_after_start_control_upgrade_requirements,
  [],
);

const diagnosticCases = [
  [0, 0],
  [1.0, 1],
  [-0.0, 0],
  [maxSafeDiagnosticInteger, maxSafeDiagnosticInteger],
  [true, undefined],
  [-1, undefined],
  [1.5, undefined],
  [maxSafeDiagnosticInteger + 1, undefined],
  [Number.NaN, undefined],
  ["1", undefined],
];
for (const field of ["claim_generation", "control_epoch"]) {
  for (const [value, expected] of diagnosticCases) {
    const candidate = { ...input, [field]: value };
    const mappedCandidate = publicAdapter.mapWorkflowCommandRecord(candidate);
    const demoCandidate = demoApi.deriveWorkflowCommandRecord(candidate);
    assert.equal(mappedCandidate[field], expected, `${field} adapter ${String(value)}`);
    assert.equal(demoCandidate.raw[field], expected, `${field} demo ${String(value)}`);
  }
}

const malformedCommandInput = {
  command_id: 7,
  input_artifact_refs: "bad",
  output_artifact_refs: [1, "artifact://kept"],
  produced_entity_counts: [],
  payload: "bad",
  artifact_refs: [1, "artifact://kept"],
  attempt: "bad",
  max_attempts: true,
  status: false,
  result: JSON.parse(
    '{"safe":true,"__proto__":{"polluted":"yes"},"prototype":{"polluted":"yes"},"constructor":{"polluted":"yes"}}',
  ),
};
const malformedMappedCommand = publicAdapter.mapWorkflowCommandRecord(malformedCommandInput);
const malformedDemoCommand = demoApi.deriveWorkflowCommandRecord(malformedCommandInput).raw;
for (const record of [malformedMappedCommand, malformedDemoCommand]) {
  for (const field of [
    "command_id",
    "input_artifact_refs",
    "produced_entity_counts",
    "payload",
    "attempt",
    "max_attempts",
    "status",
  ]) {
    assert.equal(record[field], undefined);
  }
  assert.deepEqual(record.output_artifact_refs, ["artifact://kept"]);
  assert.deepEqual(record.artifact_refs, ["artifact://kept"]);
  assert.deepEqual(record.result, { safe: true });
}

function privateAliasPayload() {
  return {
    safe: "preserved",
    CLAIMToken: "secret",
    CLAIMAuthoritySeal: "secret",
    claimCapabilityPreview: "secret",
    claimIdentityEnvelope: "secret",
    claimReceiptEnvelope: "secret",
    claimSecretPreview: "secret",
    leaseIdentityDigest: "secret",
    leaseTokenDigest: "secret",
  };
}

const controlTargetInput = {
  target_type: "workflow_command",
  command_id: input.command_id,
  command_type: input.command_type,
  owner: input.owner,
  command_status: input.status,
  fallback_status: "fail_closed",
  unknown_future_target_field: "drop",
  CLAIMToken: "secret",
};
const expectedControlTarget = {
  target_type: "workflow_command",
  command_id: input.command_id,
  command_type: input.command_type,
  owner: input.owner,
  command_status: input.status,
  fallback_status: "fail_closed",
};
const expectedMappedControlTarget = {
  ...expectedControlTarget,
  display_contract: undefined,
  control_policy: undefined,
  control_state: undefined,
  activity_spine_policy: undefined,
};

const activityInput = {
  activity_run_id: "activity-frontend",
  activity_type: "test.command",
  owner: "test-owner",
  status: "completed",
  artifact_refs: structuredArtifactRefs(),
  control_target: controlTargetInput,
  module_state_mutated: false,
  mutation_contract: "read_only_activity_evidence",
  input: privateAliasPayload(),
  output: privateAliasPayload(),
  metadata: privateAliasPayload(),
  unknown_future_column: "drop",
  non_finite: Number.NaN,
};
const attemptInput = {
  attempt_id: "attempt-frontend",
  activity_run_id: activityInput.activity_run_id,
  activity_type: activityInput.activity_type,
  owner: activityInput.owner,
  status: "completed",
  attempt_number: 3,
  artifact_refs: structuredArtifactRefs(),
  control_target: controlTargetInput,
  module_state_mutated: false,
  mutation_contract: "read_only_activity_attempt_evidence",
  rate_limit_ref: privateAliasPayload(),
  error: privateAliasPayload(),
  input: privateAliasPayload(),
  output: privateAliasPayload(),
  metadata: privateAliasPayload(),
  unknown_future_column: "drop",
  non_finite: Number.POSITIVE_INFINITY,
};
const deltaInput = {
  delta_id: "delta-frontend",
  activity_run_id: activityInput.activity_run_id,
  attempt_id: attemptInput.attempt_id,
  activity_type: activityInput.activity_type,
  owner: activityInput.owner,
  status: "applied",
  artifact_refs: structuredArtifactRefs(),
  control_target: controlTargetInput,
  module_state_mutated: false,
  mutation_contract: "read_only_entity_delta_evidence",
  source_ref: privateAliasPayload(),
  entity_payload: privateAliasPayload(),
  projection_effect: privateAliasPayload(),
  metadata: privateAliasPayload(),
  unknown_future_column: "drop",
  non_finite: Number.NEGATIVE_INFINITY,
};

const mappedActivity = publicAdapter.mapWorkflowActivityRecord(activityInput);
const mappedAttempt = publicAdapter.mapWorkflowActivityAttemptRecord(attemptInput);
const mappedDelta = publicAdapter.mapWorkflowEntityDeltaRecord(deltaInput);
const demoActivity = demoApi.deriveWorkflowActivityRecord(activityInput);
const demoAttempt = demoApi.deriveWorkflowActivityAttemptRecord(attemptInput);
const demoDelta = demoApi.deriveWorkflowEntityDeltaRecord(deltaInput);
const cloneProjectionInput = (value) => structuredClone(value);
const mappedControlResponse = publicAdapter.mapWorkflowCommandControlResponse({
  status: "cancelled",
  workflow_activity: cloneProjectionInput(activityInput),
  workflow_activity_run: cloneProjectionInput(activityInput),
  workflow_activity_attempt: cloneProjectionInput(attemptInput),
  workflow_entity_delta: cloneProjectionInput(deltaInput),
  workflow_activities: [cloneProjectionInput(activityInput)],
  workflow_activity_runs: [cloneProjectionInput(activityInput)],
  workflow_activity_attempts: [cloneProjectionInput(attemptInput)],
  workflow_entity_deltas: [cloneProjectionInput(deltaInput)],
  workflowActivityRun: {
    ...cloneProjectionInput(activityInput),
    control_target: { command_id: "forged", command_status: "forged" },
  },
  "workflow-activities": [cloneProjectionInput(activityInput)],
});

for (const record of [mappedActivity, mappedAttempt, mappedDelta]) {
  assert.equal(record.unknown_future_column, undefined);
  assert.equal(record.non_finite, undefined);
  assertNoPrivate(record);
}
for (const record of [demoActivity, demoAttempt, demoDelta]) {
  assert.equal(record.raw.unknown_future_column, undefined);
  assert.equal(record.raw.non_finite, undefined);
  assertNoPrivate(record);
}
assert.deepEqual(mappedActivity.input, { safe: "preserved" });
assert.deepEqual(mappedActivity.output, { safe: "preserved" });
assert.deepEqual(mappedActivity.metadata, { safe: "preserved" });
assert.deepEqual(mappedActivity.artifact_refs, expectedStructuredArtifactRefs);
for (const field of ["rate_limit_ref", "error", "input", "output", "metadata"]) {
assert.deepEqual(mappedAttempt[field], { safe: "preserved" });
}
assert.deepEqual(mappedAttempt.artifact_refs, expectedStructuredArtifactRefs);
for (const field of ["source_ref", "entity_payload", "projection_effect", "metadata"]) {
assert.deepEqual(mappedDelta[field], { safe: "preserved" });
}
assert.deepEqual(mappedDelta.artifact_refs, expectedStructuredArtifactRefs);
assert.deepEqual(demoActivity.raw.input, { safe: "preserved" });
assert.deepEqual(demoAttempt.raw.input, { safe: "preserved" });
assert.deepEqual(demoDelta.raw.entity_payload, { safe: "preserved" });
for (const [mappedRecord, demoRecord] of [
  [mappedActivity, demoActivity],
  [mappedAttempt, demoAttempt],
  [mappedDelta, demoDelta],
]) {
  assert.deepEqual(mappedRecord.control_target, expectedMappedControlTarget);
  assert.deepEqual(mappedRecord.artifact_refs, expectedStructuredArtifactRefs);
  assert.deepEqual(demoRecord.controlTarget.raw, expectedControlTarget);
  assert.deepEqual(demoRecord.artifactRefs, expectedStructuredArtifactRefs);
  assert.deepEqual(demoRecord.raw.control_target, expectedControlTarget);
  assert.deepEqual(demoRecord.raw.artifact_refs, expectedStructuredArtifactRefs);
}
assert.deepEqual(mappedControlResponse.workflow_activity, mappedActivity);
assert.deepEqual(mappedControlResponse.workflow_activity_run, mappedActivity);
assert.deepEqual(mappedControlResponse.workflow_activity_attempt, mappedAttempt);
assert.deepEqual(mappedControlResponse.workflow_entity_delta, mappedDelta);
assert.equal(mappedControlResponse.workflow_activities, undefined);
assert.equal(mappedControlResponse.workflowActivityRun, undefined);
assert.equal(mappedControlResponse["workflow-activities"], undefined);
assert.deepEqual(mappedControlResponse.workflow_activity_runs, [mappedActivity]);
assert.deepEqual(mappedControlResponse.workflow_activity_attempts, [mappedAttempt]);
assert.deepEqual(mappedControlResponse.workflow_entity_deltas, [mappedDelta]);
assertNoPrivate(mappedControlResponse);

const nestedCarrierInput = {
  ...input,
  result: {
    safe_result: 7,
    workflow_activity: cloneProjectionInput(activityInput),
    workflow_activity_run: cloneProjectionInput(activityInput),
    workflow_activity_attempt: cloneProjectionInput(attemptInput),
    workflow_entity_delta: cloneProjectionInput(deltaInput),
    workflow_activities: [cloneProjectionInput(activityInput)],
    workflow_activity_runs: [cloneProjectionInput(activityInput)],
    workflow_activity_attempts: [cloneProjectionInput(attemptInput)],
    workflow_entity_deltas: [cloneProjectionInput(deltaInput)],
    workflowActivityRun: cloneProjectionInput(activityInput),
    "workflow-activity-attempts": [cloneProjectionInput(attemptInput)],
    workflow_command: {
      command_id: "cmd-frontend-inner",
      command_type: "test.command",
      owner: "test-owner",
      status: "completed",
      runtime_namespace: "internal",
      unknown_future_column: "drop",
      execution_summary: { source: "forged", activity_count: 999 },
      payload: {
        safe: "kept",
        execution_summary: { source: "forged-payload" },
        executionSummary: { source: "forged-payload-alias" },
      },
      result: {
        workflowCommand: {
          command_id: "cmd-frontend-deep",
          command_type: "test.command",
          owner: "test-owner",
          status: "completed",
          unknown_future_column: "drop",
          executionSummary: { source: "forged-alias", activity_count: 999 },
          payload: {
            safe: "kept",
            "execution-summary": { source: "forged-deep-payload" },
          },
        },
      },
    },
    latest_workflow_command: {
      command_id: "cmd-frontend-latest",
      command_type: "test.command",
      owner: "test-owner",
      status: "completed",
      runtime_namespace: "internal",
      unknown_future_column: "drop",
      execution_summary: { source: "forged-latest", activity_count: 999 },
    },
    latestWorkflowCommand: {
      command_id: "cmd-frontend-latest-alias",
      command_type: "test.command",
      owner: "test-owner",
      status: "completed",
      runtime_namespace: "internal",
      unknown_future_column: "drop",
      executionSummary: { source: "forged-latest-alias", activity_count: 999 },
    },
    workflowCommands: [
      {
        command_id: "cmd-frontend-list",
        command_type: "test.command",
        owner: "test-owner",
        status: "completed",
        unknown_future_column: "drop",
        "execution-summary": { source: "forged-hyphen", activity_count: 999 },
        payload: { execution_summary: { source: "forged-list-payload" } },
      },
      "malformed",
    ],
  },
};
const mappedNestedResult = publicAdapter.mapWorkflowCommandRecord(nestedCarrierInput).result;
const demoNestedResult = demoApi.deriveWorkflowCommandRecord(nestedCarrierInput).raw.result;
for (const result of [mappedNestedResult, demoNestedResult]) {
  for (const field of ["workflow_activity", "workflow_activity_run"]) {
    assert.equal(result[field].unknown_future_column, undefined);
    assert.deepEqual(result[field].artifact_refs, expectedStructuredArtifactRefs);
    assert.equal(result[field].control_target, undefined);
    assert.equal(result[field].module_state_mutated, undefined);
    assert.equal(result[field].mutation_contract, undefined);
  }
  assert.equal(result.workflow_activity_attempt.unknown_future_column, undefined);
  assert.equal(result.workflow_entity_delta.unknown_future_column, undefined);
  for (const field of ["workflow_activity_attempt", "workflow_entity_delta"]) {
    assert.equal(result[field].activity_type, undefined);
    assert.equal(result[field].owner, undefined);
    assert.equal(result[field].control_target, undefined);
    assert.equal(result[field].module_state_mutated, undefined);
    assert.equal(result[field].mutation_contract, undefined);
  }
  for (const field of ["workflow_activities", "workflow_activity_runs"]) {
    assert.equal(result[field][0].unknown_future_column, undefined);
    assert.deepEqual(result[field][0].artifact_refs, expectedStructuredArtifactRefs);
    assert.equal(result[field][0].control_target, undefined);
    assert.equal(result[field][0].module_state_mutated, undefined);
    assert.equal(result[field][0].mutation_contract, undefined);
  }
  assert.equal(result.workflow_activity_attempts[0].unknown_future_column, undefined);
  assert.equal(result.workflow_entity_deltas[0].unknown_future_column, undefined);
  assert.equal(result.workflow_activity_attempts[0].owner, undefined);
  assert.equal(result.workflow_entity_deltas[0].owner, undefined);
  assert.equal(result.workflow_activity_attempts[0].module_state_mutated, undefined);
  assert.equal(result.workflow_entity_deltas[0].module_state_mutated, undefined);
  assert.equal(result.workflow_activity_attempts[0].mutation_contract, undefined);
  assert.equal(result.workflow_entity_deltas[0].mutation_contract, undefined);
  assert.equal(result.workflowActivityRun.unknown_future_column, undefined);
  assert.equal(result.workflowActivityRun.control_target, undefined);
  assert.equal(result.workflowActivityRun.module_state_mutated, undefined);
  assert.equal(result.workflowActivityRun.mutation_contract, undefined);
  assert.equal(result["workflow-activity-attempts"][0].unknown_future_column, undefined);
  assert.equal(result["workflow-activity-attempts"][0].owner, undefined);
  assert.equal(result.workflow_command.runtime_namespace, undefined);
  assert.equal(result.workflow_command.unknown_future_column, undefined);
  assert.equal(result.workflow_command.execution_summary, undefined);
  assert.deepEqual(result.workflow_command.payload, { safe: "kept" });
  assert.equal(result.workflow_command.result.workflowCommand.unknown_future_column, undefined);
  assert.equal(result.workflow_command.result.workflowCommand.execution_summary, undefined);
  assert.deepEqual(result.workflow_command.result.workflowCommand.payload, { safe: "kept" });
  assert.deepEqual(result.workflowCommands.map((command) => command.command_id), ["cmd-frontend-list"]);
  assert.equal(result.workflowCommands[0].execution_summary, undefined);
  assert.deepEqual(result.workflowCommands[0].payload, {});
  for (const field of ["latest_workflow_command", "latestWorkflowCommand"]) {
    assert.equal(result[field].runtime_namespace, undefined);
    assert.equal(result[field].unknown_future_column, undefined);
    assert.equal(result[field].execution_summary, undefined);
  }
  assertNoPrivate(result);
}

const projectionLimits = publicAdapter.WORKFLOW_PUBLIC_PROJECTION_LIMITS;
assert.deepEqual(projectionLimits, {
  maxDepth: 32,
  maxNodes: 4096,
  maxCollectionEntries: 256,
  maxKeyBytes: 1024,
  maxStringBytes: 65536,
  maxOccurrenceBytes: 1048576,
  maxTransportBodyBytes: 4194304,
});
assert.deepEqual(runtimeContract.WORKFLOW_PUBLIC_PROJECTION_LIMITS, projectionLimits);
assert.deepEqual(runtimeContract.OPERATION_ACTION_DECISION_APPLIED_OUTCOMES, {
  approve: ["queued"],
  reject: ["rejected"],
});
assert.deepEqual(runtimeContract.OPERATION_RUN_CONTROL_APPLIED_OUTCOMES, {
  cancel: ["cancelled"],
  retry: ["queued"],
  resume: ["queued"],
  dispatch: ["planned"],
});
assert.deepEqual(runtimeContract.WORKFLOW_COMMAND_CONTROL_APPLIED_OUTCOMES, {
  cancel: ["cancelled"],
  retry: ["queued"],
  resume: ["queued"],
});
assert.equal(
  runtimeContract.workflowPublicTransportBodyIsOverLimit(
    "x".repeat(projectionLimits.maxTransportBodyBytes + 1),
  ),
  true,
);
assert.equal(
  runtimeContract.workflowPublicTransportContentLengthIsOverLimit("9".repeat(100)),
  true,
);
assert.equal(
  runtimeContract.workflowPublicTransportContentLengthIsOverLimit("not-a-length"),
  false,
);

const oversizedStringSharedLeaf = { blob: "x".repeat(100_000) };
const oversizedStringAliasInput = {
  payload: {
    aliases: Array.from(
      { length: projectionLimits.maxCollectionEntries },
      () => oversizedStringSharedLeaf,
    ),
  },
};
const oversizedStringAliasRecord = publicAdapter.mapWorkflowCommandRecord(
  oversizedStringAliasInput,
);
const oversizedStringAliasDemo = demoApi.deriveWorkflowCommandRecord(
  oversizedStringAliasInput,
);
assert.deepEqual(oversizedStringAliasRecord.payload.aliases, []);
assert.deepEqual(oversizedStringAliasDemo.raw.payload.aliases, []);
assert.equal(Object.prototype.propertyIsEnumerable.call(oversizedStringAliasDemo, "raw"), false);

const nearLimitSharedLeaf = { blob: "x".repeat(60_000) };
const nearLimitAliasRecord = publicAdapter.mapWorkflowCommandRecord({
  payload: {
    aliases: Array.from(
      { length: projectionLimits.maxCollectionEntries },
      () => nearLimitSharedLeaf,
    ),
  },
});
assert.ok(nearLimitAliasRecord.payload.aliases.length > 0);
assert.ok(nearLimitAliasRecord.payload.aliases.length < projectionLimits.maxCollectionEntries);
assert.ok(
  runtimeContract.workflowPublicUtf8ByteLength(JSON.stringify(nearLimitAliasRecord)) <=
    projectionLimits.maxOccurrenceBytes,
);

const exactCollectionBoundary = publicAdapter.mapWorkflowCommandRecord({
  payload: { items: Array.from({ length: projectionLimits.maxCollectionEntries }, (_, index) => index) },
});
assert.equal(exactCollectionBoundary.payload.items.length, projectionLimits.maxCollectionEntries);
const exactDemoCollectionBoundary = demoApi.deriveWorkflowCommandRecord({
  payload: { items: Array.from({ length: projectionLimits.maxCollectionEntries }, (_, index) => index) },
}).raw;
assert.equal(exactDemoCollectionBoundary.payload.items.length, projectionLimits.maxCollectionEntries);
const overCollectionBoundary = publicAdapter.mapWorkflowCommandRecord({
  payload: { items: Array.from({ length: projectionLimits.maxCollectionEntries + 1 }, (_, index) => index) },
});
assert.equal(overCollectionBoundary.payload.items, undefined);
const overDemoCollectionBoundary = demoApi.deriveWorkflowCommandRecord({
  payload: { items: Array.from({ length: projectionLimits.maxCollectionEntries + 1 }, (_, index) => index) },
}).raw;
assert.equal(overDemoCollectionBoundary.payload.items, undefined);

function nestedDepthPayload(wrapperCount) {
  let value = { leaf: "kept-at-boundary" };
  for (let index = 0; index < wrapperCount; index += 1) {
    value = { next: value };
  }
  return value;
}
function readNestedDepthPayload(value, wrapperCount) {
  let current = value;
  for (let index = 0; index < wrapperCount; index += 1) {
    current = current.next;
  }
  return current.leaf;
}
const exactDepthWrapperCount = projectionLimits.maxDepth - 2;
const exactDepthRecord = publicAdapter.mapWorkflowCommandRecord({
  payload: nestedDepthPayload(exactDepthWrapperCount),
});
const exactDemoDepthRecord = demoApi.deriveWorkflowCommandRecord({
  payload: nestedDepthPayload(exactDepthWrapperCount),
}).raw;
assert.equal(
  readNestedDepthPayload(exactDepthRecord.payload, exactDepthWrapperCount),
  "kept-at-boundary",
);
assert.equal(
  readNestedDepthPayload(exactDemoDepthRecord.payload, exactDepthWrapperCount),
  "kept-at-boundary",
);
const overDepthWrapperCount = exactDepthWrapperCount + 1;
const overDepthRecord = publicAdapter.mapWorkflowCommandRecord({
  payload: nestedDepthPayload(overDepthWrapperCount),
});
const overDemoDepthRecord = demoApi.deriveWorkflowCommandRecord({
  payload: nestedDepthPayload(overDepthWrapperCount),
}).raw;
assert.equal(readNestedDepthPayload(overDepthRecord.payload, overDepthWrapperCount), undefined);
assert.equal(readNestedDepthPayload(overDemoDepthRecord.payload, overDepthWrapperCount), undefined);

const exactNodeBudgetRecord = publicAdapter.mapWorkflowCommandRecord({
  payload: {
    matrix: Array.from({ length: 16 }, () =>
      Array.from({ length: projectionLimits.maxCollectionEntries }, (_, index) => index),
    ),
    after_budget: "must-be-omitted",
  },
});
assert.equal(exactNodeBudgetRecord.payload.matrix.length, 16);
assert.equal(exactNodeBudgetRecord.payload.matrix[14].length, projectionLimits.maxCollectionEntries);
assert.equal(exactNodeBudgetRecord.payload.matrix[15].length, 237);
assert.equal(exactNodeBudgetRecord.payload.after_budget, undefined);
const exactDemoNodeBudgetRecord = demoApi.deriveWorkflowCommandRecord({
  payload: {
    matrix: Array.from({ length: 16 }, () =>
      Array.from({ length: projectionLimits.maxCollectionEntries }, (_, index) => index),
    ),
    after_budget: "must-be-omitted",
  },
}).raw;
assert.equal(exactDemoNodeBudgetRecord.payload.matrix.length, 16);
assert.equal(exactDemoNodeBudgetRecord.payload.matrix[14].length, projectionLimits.maxCollectionEntries);
assert.equal(exactDemoNodeBudgetRecord.payload.matrix[15].length, 237);
assert.equal(exactDemoNodeBudgetRecord.payload.after_budget, undefined);

const cyclicPayload = { safe: true };
cyclicPayload.self = cyclicPayload;
const cyclicRecord = publicAdapter.mapWorkflowCommandRecord({ payload: cyclicPayload });
assert.deepEqual(cyclicRecord.payload, { safe: true });
const cyclicDemoRecord = demoApi.deriveWorkflowCommandRecord({ payload: cyclicPayload }).raw;
assert.deepEqual(cyclicDemoRecord.payload, { safe: true });

function nestedCarrierListDepthPayload(wrapperCount) {
  let value = { workflow_commands: [] };
  for (let index = 0; index < wrapperCount; index += 1) {
    value = { next: value };
  }
  return value;
}
function readNestedCarrierListDepthPayload(value, wrapperCount) {
  let current = value;
  for (let index = 0; index < wrapperCount; index += 1) {
    current = current.next;
  }
  return current;
}
const exactCarrierListDepthWrapperCount = projectionLimits.maxDepth - 2;
const exactCarrierListDepthRecord = publicAdapter.mapWorkflowCommandRecord({
  payload: nestedCarrierListDepthPayload(exactCarrierListDepthWrapperCount),
});
const exactDemoCarrierListDepthRecord = demoApi.deriveWorkflowCommandRecord({
  payload: nestedCarrierListDepthPayload(exactCarrierListDepthWrapperCount),
}).raw;
assert.deepEqual(
  readNestedCarrierListDepthPayload(
    exactCarrierListDepthRecord.payload,
    exactCarrierListDepthWrapperCount,
  ).workflow_commands,
  [],
);
assert.deepEqual(
  readNestedCarrierListDepthPayload(
    exactDemoCarrierListDepthRecord.payload,
    exactCarrierListDepthWrapperCount,
  ).workflow_commands,
  [],
);
const overCarrierListDepthWrapperCount = exactCarrierListDepthWrapperCount + 1;
const overCarrierListDepthRecord = publicAdapter.mapWorkflowCommandRecord({
  payload: nestedCarrierListDepthPayload(overCarrierListDepthWrapperCount),
});
const overDemoCarrierListDepthRecord = demoApi.deriveWorkflowCommandRecord({
  payload: nestedCarrierListDepthPayload(overCarrierListDepthWrapperCount),
}).raw;
assert.equal(
  readNestedCarrierListDepthPayload(
    overCarrierListDepthRecord.payload,
    overCarrierListDepthWrapperCount,
  ).workflow_commands,
  undefined,
);
assert.equal(
  readNestedCarrierListDepthPayload(
    overDemoCarrierListDepthRecord.payload,
    overCarrierListDepthWrapperCount,
  ).workflow_commands,
  undefined,
);

const cyclicCommandCarrierList = [];
const cyclicCommandCarrierChild = {
  command_id: "cmd-carrier-cycle-child",
  result: { workflow_commands: cyclicCommandCarrierList },
};
cyclicCommandCarrierList.push(cyclicCommandCarrierChild);
const cyclicCommandCarrierRecord = publicAdapter.mapWorkflowCommandRecord({
  result: { workflow_commands: cyclicCommandCarrierList },
});
const cyclicDemoCommandCarrierRecord = demoApi.deriveWorkflowCommandRecord({
  result: { workflow_commands: cyclicCommandCarrierList },
}).raw;
assert.equal(cyclicCommandCarrierRecord.result.workflow_commands.length, 1);
assert.deepEqual(cyclicCommandCarrierRecord.result.workflow_commands[0].result, {});
assert.equal(cyclicDemoCommandCarrierRecord.result.workflow_commands.length, 1);
assert.deepEqual(cyclicDemoCommandCarrierRecord.result.workflow_commands[0].result, {});

const cyclicActivityCarrierList = [];
const cyclicActivityCarrierChild = {
  activity_run_id: "activity-carrier-cycle-child",
  metadata: { workflow_activities: cyclicActivityCarrierList },
};
cyclicActivityCarrierList.push(cyclicActivityCarrierChild);
const cyclicActivityCarrierRecord = publicAdapter.mapWorkflowCommandRecord({
  result: { workflow_activities: cyclicActivityCarrierList },
});
const cyclicDemoActivityCarrierRecord = demoApi.deriveWorkflowCommandRecord({
  result: { workflow_activities: cyclicActivityCarrierList },
}).raw;
assert.equal(cyclicActivityCarrierRecord.result.workflow_activities.length, 1);
assert.deepEqual(cyclicActivityCarrierRecord.result.workflow_activities[0].metadata, {});
assert.equal(cyclicDemoActivityCarrierRecord.result.workflow_activities.length, 1);
assert.deepEqual(cyclicDemoActivityCarrierRecord.result.workflow_activities[0].metadata, {});

const hostilePrototypeProxy = new Proxy({}, {
  getPrototypeOf() {
    throw new Error("hostile prototype trap");
  },
});
let hostileLengthReads = 0;
const hostileLengthCarrierArray = new Proxy([], {
  get(target, property, receiver) {
    if (property === "length") hostileLengthReads += 1;
    return Reflect.get(target, property, receiver);
  },
});
let hostileAccessorReads = 0;
const hostileAccessorObject = {};
Object.defineProperty(hostileAccessorObject, "secret", {
  enumerable: true,
  get() {
    hostileAccessorReads += 1;
    throw new Error("hostile member getter");
  },
});
const hostileTraversalRecord = publicAdapter.mapWorkflowCommandRecord({
  payload: {
    safe: true,
    hostile_prototype: hostilePrototypeProxy,
    hostile_accessor: hostileAccessorObject,
    workflow_commands: hostileLengthCarrierArray,
  },
});
assert.deepEqual(hostileTraversalRecord.payload, { safe: true, workflow_commands: [] });
const hostileDemoTraversalRecord = demoApi.deriveWorkflowCommandRecord({
  payload: {
    safe: true,
    hostile_prototype: hostilePrototypeProxy,
    hostile_accessor: hostileAccessorObject,
    workflow_commands: hostileLengthCarrierArray,
  },
}).raw;
assert.deepEqual(hostileDemoTraversalRecord.payload, { safe: true, workflow_commands: [] });
assert.equal(hostileAccessorReads, 0);
assert.equal(hostileLengthReads, 0);

let privateAccessorReads = 0;
const privateAccessorObject = { safe: "preserved" };
Object.defineProperty(privateAccessorObject, "claim_token", {
  enumerable: true,
  get() {
    privateAccessorReads += 1;
    return "secret";
  },
});
assert.deepEqual(
  publicAdapter.mapWorkflowCommandRecord({ payload: privateAccessorObject }).payload,
  { safe: "preserved" },
);
assert.deepEqual(
  demoApi.deriveWorkflowCommandRecord({ payload: privateAccessorObject }).raw.payload,
  { safe: "preserved" },
);
assert.equal(privateAccessorReads, 0);

let overWidthGetterReads = 0;
const overWidthGetterObject = {};
for (let index = 0; index < projectionLimits.maxCollectionEntries + 1; index += 1) {
  Object.defineProperty(overWidthGetterObject, `field_${index}`, {
    enumerable: true,
    get() {
      overWidthGetterReads += 1;
      return index;
    },
  });
}
assert.equal(
  publicAdapter.mapWorkflowCommandRecord({ payload: { over_width: overWidthGetterObject } })
    .payload.over_width,
  undefined,
);
assert.equal(
  demoApi.deriveWorkflowCommandRecord({ payload: { over_width: overWidthGetterObject } })
    .raw.payload.over_width,
  undefined,
);
assert.equal(overWidthGetterReads, 0);

function multiplicativeCommand(level, branch) {
  if (level === 0) {
    return { command_id: `cmd-budget-leaf-${branch}`, payload: { safe: true } };
  }
  return {
    command_id: `cmd-budget-${level}-${branch}`,
    result: {
      workflow_command: multiplicativeCommand(level - 1, `${branch}-single`),
      workflowCommands: [
        multiplicativeCommand(level - 1, `${branch}-left`),
        multiplicativeCommand(level - 1, `${branch}-right`),
      ],
    },
  };
}
const multiplicativeProjectionStartedAt = Date.now();
const multiplicativeRecord = publicAdapter.mapWorkflowCommandRecord(
  multiplicativeCommand(8, "root"),
);
const multiplicativeProjectionElapsedMs = Date.now() - multiplicativeProjectionStartedAt;
const multiplicativeDemoProjectionStartedAt = Date.now();
const multiplicativeDemoRecord = demoApi.deriveWorkflowCommandRecord(
  multiplicativeCommand(8, "demo-root"),
).raw;
const multiplicativeDemoProjectionElapsedMs = Date.now() - multiplicativeDemoProjectionStartedAt;
assert.equal(multiplicativeRecord.command_id, "cmd-budget-8-root");
assert.equal(multiplicativeDemoRecord.command_id, "cmd-budget-8-demo-root");
assert.ok(JSON.stringify(multiplicativeRecord).length < 1_000_000);
assert.ok(JSON.stringify(multiplicativeDemoRecord).length < 1_000_000);
assert.ok(
  multiplicativeProjectionElapsedMs < 2_000,
  `multiplicative projection exceeded runtime budget: ${multiplicativeProjectionElapsedMs}ms`,
);
assert.ok(
  multiplicativeDemoProjectionElapsedMs < 2_000,
  `demo multiplicative projection exceeded runtime budget: ${multiplicativeDemoProjectionElapsedMs}ms`,
);

const directActivityNestedCommandInput = {
  activity_run_id: "activity-direct-nested-command",
  metadata: {
    safe: true,
    execution_summary: { source: "forged-container" },
    executionSummary: { source: "forged-container-alias" },
    workflow_command: nestedCarrierInput.result.workflow_command,
    workflowCommands: [nestedCarrierInput.result.workflow_command, "malformed"],
  },
};
const directActivityNestedRecords = [
  publicAdapter.mapWorkflowActivityRecord(directActivityNestedCommandInput),
  demoApi.deriveWorkflowActivityRecord(directActivityNestedCommandInput).raw,
];
for (const record of directActivityNestedRecords) {
  assert.equal(record.metadata.execution_summary, undefined);
  assert.equal(record.metadata.executionSummary, undefined);
  assert.equal(record.metadata.workflow_command.unknown_future_column, undefined);
  assert.equal(record.metadata.workflow_command.execution_summary, undefined);
  assert.deepEqual(record.metadata.workflow_command.payload, { safe: "kept" });
  assert.equal(record.metadata.workflowCommands.length, 1);
  assert.equal(record.metadata.workflowCommands[0].unknown_future_column, undefined);
}

const prototypeSmugglingInput = JSON.parse(
  '{"__proto__":{"command_id":"smuggled","claim_generation":7,"payload":{"safe":"smuggled"}}}',
);
const mappedPrototype = publicAdapter.mapWorkflowCommandRecord(prototypeSmugglingInput);
const demoPrototype = demoApi.deriveWorkflowCommandRecord(prototypeSmugglingInput);
assert.equal(mappedPrototype.command_id, undefined);
assert.equal(mappedPrototype.claim_generation, undefined);
assert.equal(mappedPrototype.payload, undefined);
assert.equal(demoPrototype.raw.command_id, undefined);
assert.equal(demoPrototype.raw.claim_generation, undefined);

const nestedPrototypeInput = JSON.parse(
  '{"command_id":"outer","payload":{"__proto__":{"polluted":"yes"},"prototype":{"polluted":"yes"},"constructor":{"polluted":"yes"}}}',
);
const mappedNestedPrototype = publicAdapter.mapWorkflowCommandRecord(nestedPrototypeInput);
const demoNestedPrototype = demoApi.deriveWorkflowCommandRecord(nestedPrototypeInput);
for (const payload of [mappedNestedPrototype.payload, demoNestedPrototype.raw.payload]) {
  assert.equal(Object.getPrototypeOf(payload), Object.prototype);
  for (const field of ["__proto__", "prototype", "constructor"]) {
    assert.equal(Object.hasOwn(payload, field), false);
  }
  const assigned = Object.assign({}, payload);
  assert.equal(Object.getPrototypeOf(assigned), Object.prototype);
  assert.equal(assigned.polluted, undefined);
}

const mappedOperationSync = publicAdapter.mapWorkflowCommandOperationSync({
  status: "running",
  operation_run: {
    operation_run_id: "operation-frontend",
    metadata: {
      safe: true,
      execution_summary: { source: "forged-operation-metadata" },
    },
    result_ref: {
      executionSummary: { source: "forged-operation-result" },
      workflow_command: {
        command_id: "cmd-operation-nested",
        command_type: "test.command",
        owner: "test-owner",
        status: "completed",
        payload: { execution_summary: { source: "forged-command-payload" } },
      },
    },
  },
  event: {
    event_id: "event-frontend",
    payload: { safe: true, executionSummary: { source: "forged-event-payload" } },
  },
  workflow_command: input,
});
assert.deepEqual(mappedOperationSync.operation_run.metadata, { safe: true });
assert.equal(mappedOperationSync.operation_run.result_ref.executionSummary, undefined);
assert.deepEqual(mappedOperationSync.operation_run.result_ref.workflow_command.payload, {});
assert.deepEqual(mappedOperationSync.event.payload, { safe: true });
assert.equal(mappedOperationSync.workflow_command.execution_summary, undefined);

for (const malformedControlTarget of ["bad", [], { display_contract: "bad" }]) {
  const malformedActivity = publicAdapter.mapWorkflowActivityRecord({
    ...activityInput,
    control_target: malformedControlTarget,
  });
  if (typeof malformedControlTarget === "object" && !Array.isArray(malformedControlTarget)) {
    assert.equal(malformedActivity.control_target.display_contract, undefined);
  } else {
    assert.equal(malformedActivity.control_target, undefined);
  }
}
const malformedMappedActivity = publicAdapter.mapWorkflowActivityRecord({
  activity_run_id: 7,
  owner: "kept-owner",
  status: false,
  provider_ref: [],
  input: "bad",
  artifact_refs: "bad",
  module_state_mutated: "false",
  control_target: {
    target_type: 9,
    owner: "kept-owner",
    command_status: 7,
    display_contract: "bad",
    control_policy: [],
    fallback_status: false,
  },
});
const malformedDemoActivity = demoApi.deriveWorkflowActivityRecord({
  activity_run_id: 7,
  owner: "kept-owner",
  status: false,
  provider_ref: [],
  input: "bad",
  artifact_refs: "bad",
  module_state_mutated: "false",
  control_target: {
    target_type: 9,
    owner: "kept-owner",
    command_status: 7,
    display_contract: "bad",
    control_policy: [],
    fallback_status: false,
  },
}).raw;
for (const record of [malformedMappedActivity, malformedDemoActivity]) {
  assert.equal(record.activity_run_id, undefined);
  assert.equal(record.status, undefined);
  assert.equal(record.provider_ref, undefined);
  assert.equal(record.input, undefined);
  assert.equal(record.artifact_refs, undefined);
  assert.equal(record.module_state_mutated, undefined);
  assert.equal(record.owner, "kept-owner");
  assert.equal(record.control_target.owner, "kept-owner");
  assert.equal(record.control_target.target_type, undefined);
  assert.equal(record.control_target.command_status, undefined);
  assert.equal(record.control_target.display_contract, undefined);
  assert.equal(record.control_target.control_policy, undefined);
  assert.equal(record.control_target.fallback_status, undefined);
}
const nestedMalformedControlTargetInput = {
  activity_run_id: "activity-malformed-nested-control",
  control_target: {
    target_type: "workflow_command",
    display_contract: { schema_version: 7, safe_extension: true },
    control_state: {
      can_cancel: "yes",
      disabled_reasons: "bad",
      safe_extension: true,
    },
  },
};
const nestedMalformedControlRecords = [
  publicAdapter.mapWorkflowActivityRecord(nestedMalformedControlTargetInput),
  demoApi.deriveWorkflowActivityRecord(nestedMalformedControlTargetInput).raw,
];
for (const record of nestedMalformedControlRecords) {
  assert.equal(record.control_target.display_contract.safe_extension, true);
  assert.equal(record.control_target.display_contract.schema_version, undefined);
  assert.equal(record.control_target.control_state.safe_extension, true);
  assert.equal(record.control_target.control_state.can_cancel, undefined);
  assert.equal(record.control_target.control_state.disabled_reasons, undefined);
}
const filteredMalformedControlList = publicAdapter.mapWorkflowCommandControlResponse({
  status: "cancelled",
  workflow_activity_runs: ["bad", activityInput, []],
});
assert.deepEqual(filteredMalformedControlList.workflow_activity_runs, [mappedActivity]);
const malformedOperationSync = publicAdapter.mapWorkflowCommandOperationSync({
  operation_run: "bad",
  event: [],
  workflow_command: "bad",
});
assert.equal(malformedOperationSync, undefined);
assert.throws(
  () => publicAdapter.mapWorkflowCommandOperationSync(null),
  /must be an object/,
);
const hiddenOperationSyncTarget = { claim_token: "private" };
const hiddenOperationSyncProxy = new Proxy(hiddenOperationSyncTarget, {
  ownKeys() {
    return [];
  },
});
assert.equal(
  publicAdapter.mapWorkflowCommandOperationSync(hiddenOperationSyncProxy),
  undefined,
);
const nestedMalformedOperationSync = publicAdapter.mapWorkflowCommandOperationSync({
  status: "running",
  operation_run: {
    operation_run_id: "operation-malformed-nested",
    metadata: "bad",
    status_summary: "bad",
  },
  event: { event_id: "event-malformed-nested", payload: "bad" },
});
assert.equal(nestedMalformedOperationSync.operation_run.metadata, undefined);
assert.equal(nestedMalformedOperationSync.operation_run.status_summary, undefined);
assert.equal(nestedMalformedOperationSync.event.payload, undefined);
const malformedControlOperationSync = publicAdapter.mapWorkflowCommandControlResponse({
  status: "cancelled",
  operation_sync: "bad",
});
assert.equal(malformedControlOperationSync.operation_sync, undefined);
assert.equal(Object.hasOwn(malformedControlOperationSync, "operation_sync"), false);
const exactEmptyControlOperationSync = publicAdapter.mapWorkflowCommandControlResponse({
  status: "cancelled",
  operation_sync: {},
});
assert.equal(Object.hasOwn(exactEmptyControlOperationSync, "operation_sync"), true);
assert.deepEqual(exactEmptyControlOperationSync.operation_sync, {});
for (const projectedEmptySource of [
  { claim_token: "private-only" },
  { status: 7 },
]) {
  const projectedEmptyControlOperationSync = publicAdapter.mapWorkflowCommandControlResponse({
    status: "cancelled",
    operation_sync: projectedEmptySource,
  });
  assert.equal(projectedEmptyControlOperationSync.operation_sync, undefined);
  assert.equal(Object.hasOwn(projectedEmptyControlOperationSync, "operation_sync"), false);
}
const partiallyValidControlOperationSync = publicAdapter.mapWorkflowCommandControlResponse({
  status: "cancelled",
  operation_sync: {
    status: 7,
    reason: "valid-public-reason",
    claim_token: "private",
  },
});
assert.deepEqual(partiallyValidControlOperationSync.operation_sync, {
  reason: "valid-public-reason",
});
let mutableOperationSyncStatusReads = 0;
const mutableOperationSync = {};
Object.defineProperty(mutableOperationSync, "status", {
  enumerable: true,
  get() {
    mutableOperationSyncStatusReads += 1;
    return mutableOperationSyncStatusReads === 1 ? "captured-status" : 7;
  },
});
const capturedOnceControlOperationSync = publicAdapter.mapWorkflowCommandControlResponse({
  status: "cancelled",
  operation_sync: mutableOperationSync,
});
assert.equal(mutableOperationSyncStatusReads, 0);
assert.equal(capturedOnceControlOperationSync.operation_sync, undefined);
assert.equal(Object.hasOwn(capturedOnceControlOperationSync, "operation_sync"), false);
const minimallyValidCommand = { command_id: "cmd-malformed-member-survivor" };
const filteredCommandList = publicAdapter.mapWorkflowCommandListResponse({
  workflow_commands: ["bad", minimallyValidCommand, []],
});
assert.deepEqual(filteredCommandList.workflow_commands.map((command) => command.command_id), [
  minimallyValidCommand.command_id,
]);
const filteredProvenance = publicAdapter.mapOperationRunProvenanceResponse({
  status: "ok",
  workflow_commands: ["bad", minimallyValidCommand, []],
});
assert.deepEqual(filteredProvenance.workflow_commands.map((command) => command.command_id), [
  minimallyValidCommand.command_id,
]);
const filteredStatusSummary = publicAdapter.mapOperationRunStatusSummary({
  latest_workflow_command: "bad",
});
assert.equal(filteredStatusSummary.latest_workflow_command, undefined);
const closedStatusSummary = publicAdapter.mapOperationRunStatusSummary({
  latest_workflow_command: nestedCarrierInput.result.latest_workflow_command,
});
assert.equal(closedStatusSummary.latest_workflow_command.command_id, "cmd-frontend-latest");
assert.equal(closedStatusSummary.latest_workflow_command.runtime_namespace, undefined);
assert.equal(closedStatusSummary.latest_workflow_command.execution_summary.source, "forged-latest");
const filteredOperationControl = publicAdapter.mapOperationRunControlResponse({
  status: "cancelled",
  workflow_command: "bad",
});
assert.equal(filteredOperationControl.workflow_command, undefined);

const malformedSummaryInput = {
  source: 7,
  fallback_used: "true",
  module_state_mutated: 1,
  activity_count: "3",
  attempt_count: true,
  entity_delta_count: Number.NaN,
  activity_status_counts: "bad",
  attempt_status_counts: { completed: "4" },
  entity_delta_status_counts: { applied: 2, bad: false },
  entity_delta_kind_counts: [],
  latest_activity: {
    activity_run_id: "activity-summary-frontend-typed",
    unknown_future_column: "drop",
    claimToken: "secret",
  },
  latest_attempt: "bad",
  latest_entity_delta: [],
  sample_limit: "4",
  sample_truncated: "yes",
};
const malformedSummaryRecords = [
  publicAdapter.mapWorkflowCommandRecord({
    command_id: "cmd-malformed-summary-adapter",
    execution_summary: malformedSummaryInput,
  }),
  demoApi.deriveWorkflowCommandRecord({
    command_id: "cmd-malformed-summary-demo",
    execution_summary: malformedSummaryInput,
  }).raw,
];
for (const record of malformedSummaryRecords) {
  const summary = record.execution_summary;
  for (const field of [
    "source",
    "fallback_used",
    "module_state_mutated",
    "activity_count",
    "attempt_count",
    "entity_delta_count",
    "sample_limit",
    "sample_truncated",
  ]) {
    assert.equal(summary[field], undefined);
  }
  assert.deepEqual(summary.activity_status_counts, {});
  assert.deepEqual(summary.attempt_status_counts, {});
  assert.deepEqual(summary.entity_delta_status_counts, { applied: 2 });
  assert.deepEqual(summary.entity_delta_kind_counts, {});
  assert.equal(summary.latest_activity.activity_run_id, "activity-summary-frontend-typed");
  assert.deepEqual(
    Object.keys(summary.latest_activity).filter((field) => summary.latest_activity[field] !== undefined),
    ["activity_run_id"],
  );
  assert.equal(summary.latest_attempt, undefined);
  assert.equal(summary.latest_entity_delta, undefined);
  assertNoPrivate(summary);
  assertNoHazardousOwn(summary);
}

const hostileOperationExtension = {
  safe: true,
  claimToken: "secret",
  constructor: { polluted: true },
  execution_summary: { source: "forged-generic-operation-extension" },
  workflow_command: nestedCarrierInput.result.workflow_command,
  workflow_activity_attempt: {
    attempt_id: "attempt-generic-operation-extension",
    activity_type: "forged-derived",
    owner: "forged-derived",
    module_state_mutated: true,
    mutation_contract: "forged-derived",
    unknown_future_column: "drop",
  },
};
const cloneHostileOperationExtension = () => structuredClone(hostileOperationExtension);
const hostileOperationActionInput = {
  action_id: "action-operation-seal",
  claimToken: "secret",
  unknown_extension: cloneHostileOperationExtension(),
  display_contract: { schema_version: 7, safe_extension: true },
  target_ref: cloneHostileOperationExtension(),
  input: cloneHostileOperationExtension(),
  budget: cloneHostileOperationExtension(),
  result_ref: cloneHostileOperationExtension(),
  metadata: cloneHostileOperationExtension(),
};
const operationActionRecords = [
  publicAdapter.mapOperationActionRecord(hostileOperationActionInput),
  demoApi.deriveOperationActionRecord(hostileOperationActionInput).raw,
];
for (const record of operationActionRecords) {
  assert.equal(record.claimToken, undefined);
  assert.equal(record.display_contract.schema_version, undefined);
  assert.equal(record.display_contract.safe_extension, true);
  for (const field of ["unknown_extension", "target_ref", "input", "budget", "result_ref", "metadata"]) {
    const nested = record[field];
    assert.equal(nested.safe, true);
    assert.equal(nested.execution_summary, undefined);
    assert.equal(nested.workflow_command.unknown_future_column, undefined);
    assert.equal(nested.workflow_command.execution_summary, undefined);
    assert.equal(nested.workflow_activity_attempt.activity_type, undefined);
    assert.equal(nested.workflow_activity_attempt.owner, undefined);
    assert.equal(nested.workflow_activity_attempt.module_state_mutated, undefined);
    assert.equal(nested.workflow_activity_attempt.mutation_contract, undefined);
  }
  assertNoPrivate(record);
  assertNoHazardousOwn(record);
}

const hostileOperationEventInput = {
  event_id: "event-operation-seal",
  sequence_number: "4",
  actor: false,
  claimToken: "secret",
  unknown_extension: cloneHostileOperationExtension(),
  payload: cloneHostileOperationExtension(),
};
const mappedOperationEvent = publicAdapter.mapOperationEventRecord(hostileOperationEventInput);
const demoOperationEvent = demoApi.deriveOperationEventRecord(hostileOperationEventInput);
for (const record of [mappedOperationEvent, demoOperationEvent.raw]) {
  assert.equal(record.sequence_number, undefined);
  assert.equal(record.actor, undefined);
  assert.equal(record.claimToken, undefined);
  assert.equal(record.payload.execution_summary, undefined);
  assert.equal(record.payload.workflow_command.execution_summary, undefined);
  assertNoPrivate(record);
  assertNoHazardousOwn(record);
}
assert.equal(demoOperationEvent.sequenceNumber, 0);

const hostileOperationRunInput = {
  operation_run_id: "operation-run-seal",
  claimToken: "secret",
  unknown_extension: cloneHostileOperationExtension(),
  progress: cloneHostileOperationExtension(),
  workflow_ref: cloneHostileOperationExtension(),
  result_ref: cloneHostileOperationExtension(),
  metadata: cloneHostileOperationExtension(),
  control_state: {
    can_cancel: "true",
    module_state_mutated_on_control: 1,
    allowed_actions: "bad",
    disabled_reasons: cloneHostileOperationExtension(),
    safe_extension: true,
  },
  status_summary: {
    fallback_used: "true",
    module_state_mutated: 1,
    workflow_command_count: "3",
    operation_event_count: true,
    command_status_counts: { running: "4" },
    latest_workflow_command: {
      command_id: "cmd-trusted-operation-summary",
      execution_summary: malformedSummaryInput,
    },
    safe_extension: true,
  },
};
const operationRunRecords = [
  publicAdapter.mapOperationRunRecord(hostileOperationRunInput),
  demoApi.deriveOperationRunRecord(hostileOperationRunInput).raw,
];
for (const record of operationRunRecords) {
  assert.equal(record.claimToken, undefined);
  assert.equal(record.unknown_extension.execution_summary, undefined);
  for (const field of ["progress", "workflow_ref", "result_ref", "metadata"]) {
    assert.equal(record[field].execution_summary, undefined);
    assert.equal(record[field].workflow_command.execution_summary, undefined);
  }
  assert.equal(record.control_state.can_cancel, undefined);
  assert.equal(record.control_state.module_state_mutated_on_control, undefined);
  assert.deepEqual(record.control_state.allowed_actions, []);
  assert.equal(record.control_state.safe_extension, true);
  assert.equal(record.status_summary.fallback_used, undefined);
  assert.equal(record.status_summary.module_state_mutated, undefined);
  assert.equal(record.status_summary.workflow_command_count, undefined);
  assert.equal(record.status_summary.operation_event_count, undefined);
  assert.deepEqual(record.status_summary.command_status_counts, {});
  assert.equal(record.status_summary.safe_extension, true);
  const trustedSummary = record.status_summary.latest_workflow_command.execution_summary;
  assert.deepEqual(trustedSummary.activity_status_counts, {});
  assert.deepEqual(trustedSummary.entity_delta_status_counts, { applied: 2 });
  assertNoPrivate(record);
  assertNoHazardousOwn(record);
}

assert.equal(publicAdapter.mapOperationActionRecord({ input: "bad", target_ref: [] }).input, undefined);
assert.equal(publicAdapter.mapOperationActionRecord({ input: "bad", target_ref: [] }).target_ref, undefined);
assert.deepEqual(
  publicAdapter.mapOperationActionListResponse({
    actions: ["bad", { action_id: "action-survivor" }, []],
  }).actions.map((action) => action.action_id),
  ["action-survivor"],
);
assert.equal(publicAdapter.mapOperationActionDetailResponse({ status: "ok", action: "bad" }).action, undefined);
assert.deepEqual(
  publicAdapter.mapOperationRunListResponse({
    operation_runs: ["bad", { operation_run_id: "run-survivor" }, []],
  }).operation_runs.map((run) => run.operation_run_id),
  ["run-survivor"],
);
assert.equal(
  publicAdapter.mapOperationRunDetailResponse({ status: "ok", operation_run: "bad" }).operation_run,
  undefined,
);
const malformedProvenanceMembers = publicAdapter.mapOperationRunProvenanceResponse({
  status: "ok",
  action: "bad",
  operation_run: [],
  action_events: ["bad", { event_id: "event-survivor" }],
  workflow_commands: ["bad", { command_id: "command-survivor" }],
});
assert.equal(malformedProvenanceMembers.action, undefined);
assert.equal(malformedProvenanceMembers.operation_run, undefined);
assert.deepEqual(malformedProvenanceMembers.action_events.map((event) => event.event_id), ["event-survivor"]);
assert.deepEqual(malformedProvenanceMembers.workflow_commands.map((command) => command.command_id), [
  "command-survivor",
]);
assert.equal(
  publicAdapter.mapOperationRunControlResponse({ status: "cancelled", operation_run: "bad" }).operation_run,
  undefined,
);
for (const rejectedStatus of ["conflict", "approval_required", "not_found", "future_success"]) {
  if (rejectedStatus === "approval_required") {
    assert.equal(
      publicAdapter.mapOperationActionDetailResponse({ status: rejectedStatus }).status,
      "approval_required",
    );
  } else {
    assert.throws(
      () => publicAdapter.mapOperationActionDetailResponse({ status: rejectedStatus }),
      new RegExp(`unsupported status: ${rejectedStatus}`),
    );
  }
  assert.throws(
    () => publicAdapter.mapOperationRunProvenanceResponse({ status: rejectedStatus }),
    new RegExp(`unsupported status: ${rejectedStatus}`),
  );
  assert.throws(
    () => publicAdapter.mapOperationRunControlResponse({ status: rejectedStatus }),
    new RegExp(`unsupported status: ${rejectedStatus}`),
  );
  assert.throws(
    () => publicAdapter.mapWorkflowCommandControlResponse({ status: rejectedStatus }),
    new RegExp(`unsupported status: ${rejectedStatus}`),
  );
}

const exactWrapperCommands = Array.from(
  { length: projectionLimits.maxCollectionEntries },
  (_, index) => ({ command_id: `wrapper-exact-${index}` }),
);
assert.equal(
  publicAdapter.mapWorkflowCommandListResponse({ workflow_commands: exactWrapperCommands })
    .workflow_commands.length,
  projectionLimits.maxCollectionEntries,
);
const overWrapperCommands = Array.from(
  { length: projectionLimits.maxCollectionEntries + 1 },
  (_, index) => ({ command_id: `wrapper-over-${index}` }),
);
assert.deepEqual(
  publicAdapter.mapWorkflowCommandListResponse({ workflow_commands: overWrapperCommands })
    .workflow_commands,
  [],
);
assert.deepEqual(
  publicAdapter.mapWorkflowCommandListResponse({ workflow_commands: hostileLengthCarrierArray })
    .workflow_commands,
  [],
);
const nodeBudgetWrapperCommands = Array.from(
  { length: projectionLimits.maxCollectionEntries },
  (_, index) => ({
    command_id: `wrapper-node-${index}`,
    payload: { items: Array.from({ length: 32 }, () => index) },
  }),
);
const nodeBudgetWrapperResult = publicAdapter.mapWorkflowCommandListResponse({
  workflow_commands: nodeBudgetWrapperCommands,
});
assert.ok(nodeBudgetWrapperResult.workflow_commands.length > 0);
assert.ok(nodeBudgetWrapperResult.workflow_commands.length < projectionLimits.maxCollectionEntries);
assert.ok(
  nodeBudgetWrapperResult.workflow_commands.every(
    (command) => JSON.stringify(command) !== "{}",
  ),
);
function countProjectedJsonNodes(value) {
  if (value === undefined) return 0;
  if (!value || typeof value !== "object") return 1;
  if (Array.isArray(value)) {
    return 1 + value.reduce((total, item) => total + countProjectedJsonNodes(item), 0);
  }
  return 1 + Object.values(value).reduce(
    (total, item) => total + countProjectedJsonNodes(item),
    0,
  );
}
const sharedLargeCommand = {
  command_id: "wrapper-shared-large",
  payload: {
    matrix: Array.from(
      { length: 15 },
      () => Array.from({ length: projectionLimits.maxCollectionEntries }, (_, index) => index),
    ),
  },
};
const sharedAliasCommandList = Array.from(
  { length: projectionLimits.maxCollectionEntries },
  () => sharedLargeCommand,
);
const sharedAliasWrapperResult = publicAdapter.mapWorkflowCommandListResponse({
  workflow_commands: sharedAliasCommandList,
});
assert.equal(sharedAliasWrapperResult.workflow_commands.length, 1);
assert.ok(countProjectedJsonNodes(sharedAliasWrapperResult) <= projectionLimits.maxNodes);
assert.ok(JSON.stringify(sharedAliasWrapperResult).length < 100_000);
const sharedAliasDemoCarrierResult = demoApi.deriveWorkflowCommandRecord({
  command_id: "demo-wrapper-shared-root",
  result: { workflow_commands: sharedAliasCommandList },
}).raw;
assert.equal(sharedAliasDemoCarrierResult.result.workflow_commands.length, 1);
assert.ok(countProjectedJsonNodes(sharedAliasDemoCarrierResult) <= projectionLimits.maxNodes);
assert.ok(JSON.stringify(sharedAliasDemoCarrierResult).length < 100_000);
assert.deepEqual(
  publicAdapter.mapWorkflowActivityListResponse({
    workflow_activities: Array.from(
      { length: projectionLimits.maxCollectionEntries + 1 },
      (_, index) => ({ activity_run_id: `activity-over-${index}` }),
    ),
  }).workflow_activities,
  [],
);
const overBudgetProvenance = publicAdapter.mapOperationRunProvenanceResponse({
  status: "ok",
  action_events: Array.from(
    { length: projectionLimits.maxCollectionEntries + 1 },
    (_, index) => ({ event_id: `event-over-${index}` }),
  ),
  workflow_commands: overWrapperCommands,
});
assert.deepEqual(overBudgetProvenance.action_events, []);
assert.deepEqual(overBudgetProvenance.workflow_commands, []);
assert.equal(
  publicAdapter.mapWorkflowCommandControlResponse({
    status: "cancelled",
    workflow_activity_runs: Array.from(
      { length: projectionLimits.maxCollectionEntries + 1 },
      (_, index) => ({ activity_run_id: `control-activity-over-${index}` }),
    ),
  }).workflow_activity_runs,
  undefined,
);

let mutableActivityCarrierReads = 0;
const mutableActivityCarrierEnvelope = { status: "cancelled" };
Object.defineProperty(mutableActivityCarrierEnvelope, "workflow_activity", {
  enumerable: true,
  get() {
    mutableActivityCarrierReads += 1;
    return mutableActivityCarrierReads === 1
      ? {
          activity_run_id: "activity-captured-once",
          activity_type: "trusted-activity-type",
          owner: "trusted-owner",
          module_state_mutated: false,
          mutation_contract: "trusted-mutation-contract",
          control_target: { target_type: "workflow_command", owner: "trusted-owner" },
        }
      : {
          activity_run_id: "activity-forged-second-read",
          activity_type: "forged-activity-type",
          owner: "forged-owner",
          module_state_mutated: true,
          mutation_contract: "forged-mutation-contract",
          control_target: { target_type: "workflow_command", owner: "forged-owner" },
        };
  },
});
const capturedOnceActivityResponse = publicAdapter.mapWorkflowCommandControlResponse(
  mutableActivityCarrierEnvelope,
);
assert.equal(mutableActivityCarrierReads, 0);
assert.equal(capturedOnceActivityResponse.workflow_activity, undefined);

const trustedEnvelopeCommand = {
  command_id: "command-envelope-trusted",
  execution_summary: { source: "trusted-envelope", activity_count: 1 },
};
const genericEnvelopeCommand = {
  command_id: "command-envelope-generic",
  claimToken: "secret",
  unknown_future_column: "drop",
  execution_summary: { source: "forged-envelope" },
};
const commandListEnvelope = publicAdapter.mapWorkflowCommandListResponse({
  workflow_commands: [trustedEnvelopeCommand],
  workflowCommands: [genericEnvelopeCommand],
});
assert.equal(commandListEnvelope.workflow_commands[0].execution_summary.source, "trusted-envelope");
assert.equal(commandListEnvelope.workflowCommands[0].execution_summary, undefined);
assert.equal(commandListEnvelope.workflowCommands[0].unknown_future_column, undefined);
const commandDetailEnvelope = publicAdapter.mapWorkflowCommandDetailResponse({
  status: "ok",
  workflow_command: trustedEnvelopeCommand,
  workflowCommand: genericEnvelopeCommand,
});
assert.equal(commandDetailEnvelope.workflow_command.execution_summary.source, "trusted-envelope");
assert.equal(commandDetailEnvelope.workflowCommand.execution_summary, undefined);

const trustedEnvelopeActivity = {
  activity_run_id: "activity-envelope-trusted",
  activity_type: "trusted-derived",
  owner: "trusted-derived",
  module_state_mutated: false,
  mutation_contract: "read_only",
  control_target: { target_type: "workflow_command" },
};
const trustedEnvelopeAttempt = {
  attempt_id: "attempt-envelope-trusted",
  activity_type: "trusted-derived",
  owner: "trusted-derived",
  module_state_mutated: false,
  mutation_contract: "read_only",
};
const trustedEnvelopeDelta = {
  delta_id: "delta-envelope-trusted",
  activity_type: "trusted-derived",
  owner: "trusted-derived",
  module_state_mutated: false,
  mutation_contract: "read_only",
};
const activityListEnvelope = publicAdapter.mapWorkflowActivityListResponse({
  workflow_activities: [trustedEnvelopeActivity],
  workflowActivities: [trustedEnvelopeActivity],
});
assert.equal(activityListEnvelope.workflow_activities[0].mutation_contract, "read_only");
assert.equal(activityListEnvelope.workflowActivities[0].mutation_contract, undefined);
const activityDetailEnvelope = publicAdapter.mapWorkflowActivityDetailResponse({
  status: "ok",
  workflow_activity: trustedEnvelopeActivity,
  workflowActivity: trustedEnvelopeActivity,
});
assert.equal(activityDetailEnvelope.workflow_activity.control_target.target_type, "workflow_command");
assert.equal(activityDetailEnvelope.workflowActivity.control_target, undefined);
const attemptListEnvelope = publicAdapter.mapWorkflowActivityAttemptListResponse({
  workflow_activity_attempts: [trustedEnvelopeAttempt],
  workflowActivityAttempts: [trustedEnvelopeAttempt],
});
assert.equal(attemptListEnvelope.workflow_activity_attempts[0].owner, "trusted-derived");
assert.equal(attemptListEnvelope.workflowActivityAttempts[0].owner, undefined);
const attemptDetailEnvelope = publicAdapter.mapWorkflowActivityAttemptDetailResponse({
  status: "ok",
  workflow_activity_attempt: trustedEnvelopeAttempt,
  workflowActivityAttempt: trustedEnvelopeAttempt,
});
assert.equal(attemptDetailEnvelope.workflow_activity_attempt.owner, "trusted-derived");
assert.equal(attemptDetailEnvelope.workflowActivityAttempt.owner, undefined);
const deltaListEnvelope = publicAdapter.mapWorkflowEntityDeltaListResponse({
  workflow_entity_deltas: [trustedEnvelopeDelta],
  workflowEntityDeltas: [trustedEnvelopeDelta],
});
assert.equal(deltaListEnvelope.workflow_entity_deltas[0].owner, "trusted-derived");
assert.equal(deltaListEnvelope.workflowEntityDeltas[0].owner, undefined);
const deltaDetailEnvelope = publicAdapter.mapWorkflowEntityDeltaDetailResponse({
  status: "ok",
  workflow_entity_delta: trustedEnvelopeDelta,
  workflowEntityDelta: trustedEnvelopeDelta,
});
assert.equal(deltaDetailEnvelope.workflow_entity_delta.owner, "trusted-derived");
assert.equal(deltaDetailEnvelope.workflowEntityDelta.owner, undefined);

const commandRegistry = publicAdapter.mapWorkflowCommandRegistryResponse({
  status: "ok",
  claimToken: "secret",
  command_registry: {
    test: {
      command_type: "test.command",
      claimToken: "secret",
      control_policy: { claimToken: "secret", constructor: { polluted: true } },
    },
  },
});
assertNoPrivate(commandRegistry);
assertNoHazardousOwn(commandRegistry);
const operationRegistry = publicAdapter.mapOperationActionRegistryResponse({
  status: "ok",
  claimToken: "secret",
  action_registry: {
    test: {
      claimToken: "secret",
      allowed_workflow_command_contracts: [
        "bad",
        { command_type: "test.command", claimToken: "secret" },
      ],
      workflow_command_control_summary: {
        command_count: "7",
        has_fail_closed_running_controls: "true",
        running_control_maturity_counts: { safe: 2, bad: "2" },
      },
    },
  },
});
assert.equal(operationRegistry.action_registry.test.allowed_workflow_command_contracts.length, 1);
assert.equal(operationRegistry.action_registry.test.workflow_command_control_summary.command_count, undefined);
assert.equal(
  operationRegistry.action_registry.test.workflow_command_control_summary.has_fail_closed_running_controls,
  undefined,
);
assert.deepEqual(
  operationRegistry.action_registry.test.workflow_command_control_summary.running_control_maturity_counts,
  { safe: 2 },
);
assertNoPrivate(operationRegistry);
assertNoHazardousOwn(operationRegistry);
const isolatedMalformedRegistryChildren = publicAdapter.mapOperationActionRegistryResponse({
  status: "ok",
  action_registry: {
    test: {
      owner_module: "safe-owner",
      allowed_workflow_command_contracts: [{ bad: true }, { command_type: "safe.command" }],
      default_workflow_command_contract: { bad: true },
    },
  },
});
assert.equal(isolatedMalformedRegistryChildren.action_registry.test.owner_module, "safe-owner");
assert.deepEqual(
  isolatedMalformedRegistryChildren.action_registry.test.allowed_workflow_command_contracts.map(
    (contract) => contract.command_type,
  ),
  ["safe.command"],
);
assert.equal(
  isolatedMalformedRegistryChildren.action_registry.test.default_workflow_command_contract,
  undefined,
);

for (const [value, expected] of diagnosticCases) {
  const candidate = { ...attemptInput, attempt_number: value };
  const mappedCandidate = publicAdapter.mapWorkflowActivityAttemptRecord(candidate);
  const demoCandidate = demoApi.deriveWorkflowActivityAttemptRecord(candidate);
  assert.equal(mappedCandidate.attempt_number, expected, `attempt adapter ${String(value)}`);
  assert.equal(demoCandidate.attemptNumber, expected, `attempt demo ${String(value)}`);
  assert.equal(demoCandidate.raw.attempt_number, expected, `attempt raw ${String(value)}`);
}

(async () => {
  const jsonResponse = (payload) => ({
    ok: true,
    status: 200,
    headers: new Headers({ "Content-Type": "application/json" }),
    text: async () => JSON.stringify(payload),
  });
  let oversizedHeaderBodyReads = 0;
  const oversizedHeaderResponse = () => ({
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new Headers({
      "Content-Type": "application/json",
      "Content-Length": String(projectionLimits.maxTransportBodyBytes + 1),
    }),
    text: async () => {
      oversizedHeaderBodyReads += 1;
      return '{"status":"ok"}';
    },
  });
  const oversizedHeaderClient = new publicAdapter.SourcingAgentApiClient({
    baseUrl: "https://api.test",
    fetchImpl: async () => oversizedHeaderResponse(),
  });
  await assert.rejects(
    () => oversizedHeaderClient.getOperationAction("oversized-header"),
    /transport body budget/,
  );
  global.fetch = async () => oversizedHeaderResponse();
  await assert.rejects(
    () => demoApi.listWorkflowActivities({}),
    /transport body budget/,
  );
  assert.equal(oversizedHeaderBodyReads, 0);
  const clientOutcomeCases = [
    {
      label: "action-submit",
      accepted: ["queued", "approval_required"],
      invoke: (client) => client.submitOperationAction({}),
    },
    {
      label: "action-query",
      accepted: ["ok"],
      invoke: (client) => client.getOperationAction("action-client-matrix"),
    },
    {
      label: "action-approve",
      accepted: ["queued"],
      invoke: (client) => client.approveOperationAction("action-client-matrix"),
    },
    {
      label: "action-reject",
      accepted: ["rejected"],
      invoke: (client) => client.rejectOperationAction("action-client-matrix"),
    },
    {
      label: "operation-cancel",
      accepted: ["cancelled"],
      invoke: (client) => client.cancelOperationRun("run-client-matrix"),
    },
    {
      label: "operation-retry",
      accepted: ["queued"],
      invoke: (client) => client.retryOperationRun("run-client-matrix"),
    },
    {
      label: "operation-resume",
      accepted: ["queued"],
      invoke: (client) => client.resumeOperationRun("run-client-matrix"),
    },
    {
      label: "operation-dispatch",
      accepted: ["planned"],
      invoke: (client) => client.dispatchOperationRun("run-client-matrix"),
    },
    {
      label: "command-cancel",
      accepted: ["cancelled"],
      invoke: (client) => client.cancelWorkflowCommand("command-client-matrix"),
    },
    {
      label: "command-retry",
      accepted: ["queued"],
      invoke: (client) => client.retryWorkflowCommand("command-client-matrix"),
    },
    {
      label: "command-resume",
      accepted: ["queued"],
      invoke: (client) => client.resumeWorkflowCommand("command-client-matrix"),
    },
  ];
  const clientOutcomeMatrix = [
    "ok",
    "queued",
    "approval_required",
    "rejected",
    "cancelled",
    "planned",
    "conflict",
    "not_found",
    "future_success",
    " queued ",
    "cancelled ",
    " planned",
    "\\trejected\\n",
    " ",
    "",
  ];
  for (const outcomeCase of clientOutcomeCases) {
    for (const candidateStatus of clientOutcomeMatrix) {
      const client = new publicAdapter.SourcingAgentApiClient({
        baseUrl: "https://api.test",
        fetchImpl: async () => jsonResponse({ status: candidateStatus }),
      });
      if (outcomeCase.accepted.includes(candidateStatus)) {
        const response = await outcomeCase.invoke(client);
        assert.equal(response.status, candidateStatus, outcomeCase.label);
      } else {
        await assert.rejects(
          () => outcomeCase.invoke(client),
          /unsupported status/,
          `${outcomeCase.label} accepted cross-action outcome ${candidateStatus}`,
        );
      }
    }
  }
  global.fetch = async () => jsonResponse({
    status: "ok",
    action_events: Array.from(
      { length: projectionLimits.maxCollectionEntries + 1 },
      (_, index) => ({ event_id: `demo-event-over-${index}` }),
    ),
    workflow_commands: overWrapperCommands,
  });
  const demoOverBudgetProvenance = await demoApi.getOperationRunProvenance(
    "operation-run-over-budget",
  );
  assert.deepEqual(demoOverBudgetProvenance.actionEvents, []);
  assert.deepEqual(demoOverBudgetProvenance.workflowCommands, []);

  global.fetch = async () => jsonResponse({
    status: "ok",
    workflow_commands: nodeBudgetWrapperCommands,
  });
  const demoNodeBudgetProvenance = await demoApi.getOperationRunProvenance(
    "operation-run-node-budget",
  );
  assert.ok(demoNodeBudgetProvenance.workflowCommands.length > 0);
  assert.ok(
    demoNodeBudgetProvenance.workflowCommands.length < projectionLimits.maxCollectionEntries,
  );
  assert.ok(
    demoNodeBudgetProvenance.workflowCommands.every(
      (command) => JSON.stringify(command.raw) !== "{}",
    ),
  );
  assert.equal(
    Object.prototype.propertyIsEnumerable.call(demoNodeBudgetProvenance, "raw"),
    false,
  );
  assert.ok(
    runtimeContract.workflowPublicUtf8ByteLength(JSON.stringify(demoNodeBudgetProvenance)) <=
      projectionLimits.maxOccurrenceBytes,
  );
  assert.ok(countProjectedJsonNodes(demoNodeBudgetProvenance) <= projectionLimits.maxNodes);

  global.fetch = async () => jsonResponse({
    workflow_activities: Array.from(
      { length: projectionLimits.maxCollectionEntries + 1 },
      (_, index) => ({ activity_run_id: `demo-activity-over-${index}` }),
    ),
  });
  assert.deepEqual(await demoApi.listWorkflowActivities({}), []);

  global.fetch = async () => jsonResponse({
    status: "ok",
    contract: false,
    module_state_mutated: "true",
    claimToken: "secret",
    action: hostileOperationActionInput,
    operation_run: hostileOperationRunInput,
    action_events: ["bad", { event_id: 7, sequence_number: "4", payload: "bad" }],
    operation_events: [[], hostileOperationEventInput],
    event_timeline: [null, hostileOperationEventInput],
    workflow_commands: ["bad", trustedEnvelopeCommand, []],
  });
  const provenance = await demoApi.getOperationRunProvenance("operation-run-seal");
  assert.deepEqual(provenance.workflowCommands.map((command) => command.commandId), [
    "command-envelope-trusted",
  ]);
  assert.equal(provenance.actionEvents.length, 0);
  assert.equal(provenance.operationEvents.length, 1);
  assert.equal(provenance.eventTimeline.length, 1);
  assert.equal(provenance.raw.claimToken, undefined);
  assert.equal(provenance.raw.contract, undefined);
  assert.equal(provenance.raw.module_state_mutated, undefined);
  assert.deepEqual(provenance.raw.action_events, []);
  assert.equal(provenance.raw.action.claimToken, undefined);
  assert.equal(provenance.raw.operation_run.claimToken, undefined);
  assert.equal(provenance.raw.workflow_commands[0].execution_summary.source, "trusted-envelope");
  assertNoPrivate(provenance.raw);
  assertNoHazardousOwn(provenance.raw);

  global.fetch = async () => jsonResponse({
    status: "queued",
    contract: false,
    module_state_mutated: "true",
    claimToken: "secret",
    action: hostileOperationActionInput,
    operation_run: hostileOperationRunInput,
    events: [{ event_id: 7, sequence_number: "4", payload: "bad" }],
  });
  const decision = await demoApi.approveOperationAction("action-operation-seal");
  assert.equal(decision.raw.claimToken, undefined);
  assert.equal(decision.raw.contract, undefined);
  assert.equal(decision.raw.module_state_mutated, undefined);
  assert.deepEqual(decision.raw.events, []);
  assert.equal(decision.raw.action.claimToken, undefined);
  assert.equal(decision.raw.operation_run.claimToken, undefined);
  assertNoPrivate(decision.raw);
  assertNoHazardousOwn(decision.raw);

  const forgedControlCommand = {
    command_id: "command-control-generic",
    execution_summary: { source: "forged-control", activity_count: 999 },
    control_state: { safe: true },
    display_contract: { safe: true },
    control_policy: { safe: true },
    activity_spine_policy: { safe: true },
  };
  const demoOutcomeCases = [
    {
      label: "demo-action-approve",
      accepted: ["queued"],
      invoke: () => demoApi.approveOperationAction("demo-action-matrix"),
    },
    {
      label: "demo-action-reject",
      accepted: ["rejected"],
      invoke: () => demoApi.rejectOperationAction("demo-action-matrix"),
    },
    {
      label: "demo-operation-cancel",
      accepted: ["cancelled"],
      invoke: () => demoApi.cancelOperationRun("demo-operation-matrix"),
    },
    {
      label: "demo-operation-retry",
      accepted: ["queued"],
      invoke: () => demoApi.retryOperationRun("demo-operation-matrix"),
    },
    {
      label: "demo-operation-resume",
      accepted: ["queued"],
      invoke: () => demoApi.resumeOperationRun("demo-operation-matrix"),
    },
    {
      label: "demo-operation-dispatch",
      accepted: ["planned"],
      invoke: () => demoApi.dispatchOperationRun("demo-operation-matrix"),
    },
    {
      label: "demo-command-cancel",
      accepted: ["cancelled"],
      invoke: () => demoApi.cancelWorkflowCommand("demo-command-matrix"),
    },
    {
      label: "demo-command-retry",
      accepted: ["queued"],
      invoke: () => demoApi.retryWorkflowCommand("demo-command-matrix"),
    },
    {
      label: "demo-command-resume",
      accepted: ["queued"],
      invoke: () => demoApi.resumeWorkflowCommand("demo-command-matrix"),
    },
  ];
  for (const outcomeCase of demoOutcomeCases) {
    for (const candidateStatus of clientOutcomeMatrix) {
      global.fetch = async () => jsonResponse({
        status: candidateStatus,
        action: { action_id: "demo-action-matrix" },
        operation_run: { operation_run_id: "demo-operation-matrix" },
        workflow_command: forgedControlCommand,
      });
      if (outcomeCase.accepted.includes(candidateStatus)) {
        assert.ok(await outcomeCase.invoke(), outcomeCase.label);
      } else {
        await assert.rejects(
          () => outcomeCase.invoke(),
          candidateStatus === "" ? /missing status/ : /unexpected status/,
          `${outcomeCase.label} accepted cross-action outcome ${candidateStatus}`,
        );
      }
    }
  }
  global.fetch = async () => jsonResponse({
    status: "cancelled",
    workflow_command: forgedControlCommand,
  });
  const controlledCommand = await demoApi.cancelWorkflowCommand("command-control-generic");
  assert.equal(controlledCommand.executionSummary, undefined);
  assert.equal(controlledCommand.raw.execution_summary, undefined);

  global.fetch = async () => jsonResponse({
    status: 7,
    action: hostileOperationActionInput,
    operation_run: hostileOperationRunInput,
  });
  await assert.rejects(
    () => demoApi.approveOperationAction("action-malformed-status"),
    /malformed status/,
  );
  global.fetch = async () => jsonResponse({
    status: 7,
    operation_run: { operation_run_id: "run-malformed-status" },
  });
  await assert.rejects(
    () => demoApi.cancelOperationRun("run-malformed-status"),
    /malformed status/,
  );
  global.fetch = async () => jsonResponse({
    status: 7,
    workflow_command: forgedControlCommand,
  });
  await assert.rejects(
    () => demoApi.cancelWorkflowCommand("command-malformed-status"),
    /malformed status/,
  );
  global.fetch = async () => jsonResponse({ status: 7 });
  await assert.rejects(
    () => demoApi.getOperationRunProvenance("run-malformed-provenance"),
    /malformed status/,
  );
  for (const rejectedStatus of ["conflict", "approval_required", "not_found", "future_success"]) {
    global.fetch = async () => jsonResponse({
      status: rejectedStatus,
      action: hostileOperationActionInput,
      operation_run: hostileOperationRunInput,
    });
    await assert.rejects(
      () => demoApi.approveOperationAction(`action-rejected-${rejectedStatus}`),
      new RegExp(`unexpected status ${rejectedStatus}`),
    );
    global.fetch = async () => jsonResponse({
      status: rejectedStatus,
      operation_run: { operation_run_id: `run-rejected-${rejectedStatus}` },
    });
    await assert.rejects(
      () => demoApi.cancelOperationRun(`run-rejected-${rejectedStatus}`),
      new RegExp(`unexpected status ${rejectedStatus}`),
    );
    global.fetch = async () => jsonResponse({
      status: rejectedStatus,
      workflow_command: forgedControlCommand,
    });
    await assert.rejects(
      () => demoApi.cancelWorkflowCommand(`command-rejected-${rejectedStatus}`),
      new RegExp(`unexpected status ${rejectedStatus}`),
    );
    global.fetch = async () => jsonResponse({ status: rejectedStatus });
    await assert.rejects(
      () => demoApi.getOperationRunProvenance(`provenance-rejected-${rejectedStatus}`),
      new RegExp(`unexpected status ${rejectedStatus}`),
    );
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
"""
    assertion = subprocess.run(
        [
            "node",
            "-e",
            assertion_script,
            str(public_adapter_bundle),
            str(demo_api_bundle),
            json.dumps(sorted(WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS)),
            json.dumps(PRIVATE_ALIAS_KEYS),
            str(MAX_SAFE_DIAGNOSTIC_INTEGER),
            str(runtime_contract_bundle),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert assertion.returncode == 0, assertion.stderr
