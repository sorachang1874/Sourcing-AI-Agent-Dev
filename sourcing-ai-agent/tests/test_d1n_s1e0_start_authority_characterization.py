from __future__ import annotations

import ast
import hashlib
import re
from pathlib import Path
from typing import cast

import sourcing_agent.agent_tool_result_postgres as agent_tool_result_postgres
import sourcing_agent.repositories.workflow_runtime as workflow_runtime_descriptors
from sourcing_agent.acquisition_start_v2 import (
    ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION,
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
)
from sourcing_agent.agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC
from sourcing_agent.agent_tool_registry import DEFAULT_AGENT_TOOL_REGISTRY, AgentActionToolRoute
from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter
from sourcing_agent.durable_runtime import (
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
    ACQUISITION_RUN_CREATE_OWNER,
    DEFAULT_COMMAND_OWNER_REGISTRY,
    default_stage_id_for_command_type,
)
from sourcing_agent.operation_runtime import DEFAULT_ACTION_REGISTRY
from sourcing_agent.repositories.workflow_runtime import (
    AGENT_ACTIONS,
    OPERATION_RUNS,
    WORKFLOW_COMMANDS,
    WorkflowRuntimeRepository,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
MIGRATIONS_ROOT = SOURCE_ROOT / "migrations"
OPERATION_RUNTIME_PATH = SOURCE_ROOT / "operation_runtime.py"
ORCHESTRATOR_PATH = SOURCE_ROOT / "orchestrator.py"
DECISION_DOC_PATH = REPO_ROOT / "docs" / "TRACK_D_D1N_S1E0_START_AUTHORITY_CHARACTERIZATION.md"

APPROVE_ACTION_AST_SHA256 = "06c52f165740401ad7675b9e99e75d60461e50c041fbdc86c087be208f8e6f48"
DISPATCH_START_AST_SHA256 = "0942086c6b68218f2c002bb2f1c7ffe1c1a243cc5f4ad8eec3fe7edc923c5b60"
PLAN_ROOT_COMMAND_AST_SHA256 = "6e992fdfdb908288e72005aca09c5d4d352420f443bcdc15cf362ac82a3634d5"

EXPECTED_BUDGET_OWNER_RECORD = {
    "owner_id": "acquisition.parent_budget_reservation",
    "owner_revision": "acquisition_parent_budget_v1",
    "owner_contract_digest": "50c72166a683f8a49826bab1af82fdc0922026d5993a3725403582dff24c5670",
}

UNRATIFIED_BUDGET_TABLES = frozenset(
    {
        "cost_reservations",
        "parent_budget_reservations",
        "acquisition_budget_reservations",
    }
)


def _class_method(path: Path, class_name: str, method_name: str) -> ast.FunctionDef:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name]
    assert len(classes) == 1, f"expected exactly one {class_name}"
    methods = [node for node in classes[0].body if isinstance(node, ast.FunctionDef) and node.name == method_name]
    assert len(methods) == 1, f"expected exactly one {class_name}.{method_name}"
    return methods[0]


def _ast_digest(node: ast.AST) -> str:
    payload = ast.dump(node, annotate_fields=True, include_attributes=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _dotted_name(node: ast.AST | None) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def _calls(method: ast.FunctionDef, dotted_name: str) -> list[ast.Call]:
    return sorted(
        [node for node in ast.walk(method) if isinstance(node, ast.Call) and _dotted_name(node.func) == dotted_name],
        key=lambda node: (node.lineno, node.col_offset),
    )


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    matches = [keyword.value for keyword in call.keywords if keyword.arg == name]
    assert len(matches) <= 1
    return matches[0] if matches else None


def _constant_keyword(call: ast.Call, name: str) -> object:
    value = _keyword(call, name)
    assert isinstance(value, ast.Constant), f"expected constant keyword {name}"
    return value.value


def _explicit_dict_keys(value: ast.expr | None) -> tuple[str, ...]:
    assert isinstance(value, ast.Dict)
    keys: list[str] = []
    for key in value.keys:
        if key is None:
            continue
        assert isinstance(key, ast.Constant) and isinstance(key.value, str)
        keys.append(key.value)
    return tuple(keys)


def _ordered_selected_calls(method: ast.FunctionDef, selected_names: frozenset[str]) -> tuple[str, ...]:
    calls = sorted(
        [node for node in ast.walk(method) if isinstance(node, ast.Call)],
        key=lambda node: (node.lineno, node.col_offset),
    )
    return tuple(name for call in calls if (name := _dotted_name(call.func)) in selected_names)


def _descriptor_table_names() -> frozenset[str]:
    return frozenset(
        value.table
        for value in vars(workflow_runtime_descriptors).values()
        if hasattr(value, "table") and isinstance(getattr(value, "table"), str)
    )


def _migration_table_names() -> frozenset[str]:
    sql = "\n".join(path.read_text(encoding="utf-8") for path in sorted(MIGRATIONS_ROOT.glob("*.sql")))
    return frozenset(
        match.group(1).lower()
        for match in re.finditer(
            r"\bCREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-z_][a-z0-9_]*)",
            sql,
            flags=re.IGNORECASE,
        )
    )


def test_generic_action_approval_is_not_the_v2_confirmation_receipt_authority() -> None:
    method = _class_method(OPERATION_RUNTIME_PATH, "OperationRuntimeWriter", "approve_action")
    assert _ast_digest(method) == APPROVE_ACTION_AST_SHA256
    assert ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION == "acquisition_confirmation_receipt.v1"

    write_order = _ordered_selected_calls(
        method,
        frozenset(
            {
                "self.store.repos.workflow_runtime.update_action_state",
                "self.store.repos.workflow_runtime.append_operation_event",
                "self.store.repos.workflow_runtime.upsert_operation",
            }
        ),
    )
    assert write_order == (
        "self.store.repos.workflow_runtime.update_action_state",
        "self.store.repos.workflow_runtime.append_operation_event",
        "self.store.repos.workflow_runtime.upsert_operation",
        "self.store.repos.workflow_runtime.append_operation_event",
    )

    event_calls = _calls(method, "self.store.repos.workflow_runtime.append_operation_event")
    approved_calls = [call for call in event_calls if _constant_keyword(call, "event_type") == "ActionApproved"]
    assert len(approved_calls) == 1
    approved_call = approved_calls[0]
    assert _keyword(approved_call, "operation_run_id") is None
    assert _keyword(approved_call, "schema_version") is None
    assert _explicit_dict_keys(_keyword(approved_call, "payload")) == ("action_type", "owner_module")
    assert "AcquisitionConfirmationReceipt" not in ast.unparse(method)
    assert "acquisition_confirmation_receipt.v1" not in ast.unparse(method)


def test_generic_root_command_and_planned_event_are_split_before_winner_ratification() -> None:
    historical_action = DEFAULT_ACTION_REGISTRY.spec_for("start_acquisition_run")
    assert historical_action.request_schema_version == "acquisition_root_request_v1"
    assert historical_action.allowed_workflow_command_types == (ACQUISITION_RUN_CREATE_COMMAND_TYPE,)
    assert DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(ACQUISITION_RUN_CREATE_COMMAND_TYPE) == ACQUISITION_RUN_CREATE_OWNER
    assert default_stage_id_for_command_type(ACQUISITION_RUN_CREATE_COMMAND_TYPE) == "acquisition_run_create"

    dispatch = _class_method(
        ORCHESTRATOR_PATH,
        "SourcingOrchestrator",
        "_dispatch_agent_callable_workflow_command_operation",
    )
    planner = _class_method(
        ORCHESTRATOR_PATH,
        "SourcingOrchestrator",
        "_plan_agent_callable_workflow_command",
    )
    assert _ast_digest(dispatch) == DISPATCH_START_AST_SHA256
    assert _ast_digest(planner) == PLAN_ROOT_COMMAND_AST_SHA256

    assert _ordered_selected_calls(
        dispatch,
        frozenset(
            {
                "self._plan_agent_callable_workflow_command",
                "self.store.repos.workflow_runtime.update_operation_state",
                "self.store.repos.workflow_runtime.update_action_state",
                "self.store.repos.workflow_runtime.append_operation_event",
            }
        ),
    ) == (
        "self._plan_agent_callable_workflow_command",
        "self.store.repos.workflow_runtime.update_operation_state",
        "self.store.repos.workflow_runtime.update_action_state",
        "self.store.repos.workflow_runtime.append_operation_event",
    )

    planned_event_calls = [
        call
        for call in _calls(dispatch, "self.store.repos.workflow_runtime.append_operation_event")
        if _constant_keyword(call, "event_type") == "OperationCommandPlanned"
    ]
    assert len(planned_event_calls) == 1
    planned_event = planned_event_calls[0]
    assert _keyword(planned_event, "sequence_number") is None
    assert _keyword(planned_event, "schema_version") is None
    assert _explicit_dict_keys(_keyword(planned_event, "payload")) == (
        "module_state_mutated",
        "migration_phase",
    )
    planned_payload = cast(ast.Dict, _keyword(planned_event, "payload"))
    assert sum(key is None for key in planned_payload.keys) == 1
    assert "confirmation_receipt" not in ast.unparse(planned_event)
    assert "budget_reservation" not in ast.unparse(planned_event)

    reducer_calls = _calls(planner, "self.durable_runtime_writer.append_event_and_reduce")
    assert [_constant_keyword(call, "event_type") for call in reducer_calls] == [
        "WorkflowStarted",
        "CommandPlanRequested",
    ]
    assert "build_acquisition_start_v2_root_command_payload" not in ast.unparse(planner)


def test_parent_budget_owner_is_only_a_registry_pin_without_a_physical_reservation_owner() -> None:
    assert START_ACQUISITION_RUN_TOOL_SPEC.budget.mode == "parent_reservation_required"
    budget_owner = START_ACQUISITION_RUN_TOOL_SPEC.budget.budget_owner
    assert budget_owner is not None
    assert budget_owner.to_fingerprint_record() == EXPECTED_BUDGET_OWNER_RECORD

    action_columns = {column.name for column in AGENT_ACTIONS.columns}
    operation_columns = {column.name for column in OPERATION_RUNS.columns}
    command_columns = {column.name for column in WORKFLOW_COMMANDS.columns}
    assert "budget_json" in action_columns
    assert "cost_budget_json" in operation_columns
    assert "budget_reservation_ref" not in command_columns

    assert _descriptor_table_names().isdisjoint(UNRATIFIED_BUDGET_TABLES)
    assert _migration_table_names().isdisjoint(UNRATIFIED_BUDGET_TABLES)
    migration_sql = "\n".join(path.read_text(encoding="utf-8") for path in sorted(MIGRATIONS_ROOT.glob("*.sql")))
    assert "budget_reservation_ref" not in migration_sql

    repository_budget_methods = {
        name for name in dir(WorkflowRuntimeRepository) if "budget" in name.lower() or "reservation" in name.lower()
    }
    adapter_budget_methods = {
        name
        for name in dir(LiveControlPlanePostgresAdapter)
        if "budget" in name.lower() or "reservation" in name.lower()
    }
    assert repository_budget_methods == set()
    assert adapter_budget_methods == set()


def test_s1e2a_keeps_start_shadow_only_while_exposing_only_the_submit_uow() -> None:
    assert DEFAULT_AGENT_TOOL_REGISTRY.declared_tool_count == 0
    assert DEFAULT_AGENT_TOOL_REGISTRY.tool_names == ()
    assert ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION == "acquisition_root_request_v2"
    assert DEFAULT_ACTION_REGISTRY.spec_for("start_acquisition_run").request_schema_version == (
        "acquisition_root_request_v1"
    )

    assert START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version == "start_acquisition_run_tool_v3"
    assert START_ACQUISITION_RUN_TOOL_SPEC.behavior.result_link_policy == "workflow_command_acceptance_v1"
    route = cast(AgentActionToolRoute, START_ACQUISITION_RUN_TOOL_SPEC.route)
    assert route.adapter.owner_id == "acquisition.start_v2_pg_uow"
    assert route.adapter.owner_revision == "acquisition_start_v2_pg_uow_v1"

    assert (SOURCE_ROOT / "acquisition_start_v2_postgres.py").exists()
    assert hasattr(WorkflowRuntimeRepository, "submit_acquisition_start_v2_action_uow")
    assert hasattr(LiveControlPlanePostgresAdapter, "submit_acquisition_start_v2_action_uow")
    assert not hasattr(WorkflowRuntimeRepository, "create_acquisition_start_v2_uow")
    assert not hasattr(LiveControlPlanePostgresAdapter, "create_acquisition_start_v2_uow")
    assert not hasattr(agent_tool_result_postgres, "prepare_start_acquisition_tool_result")
    assert not hasattr(agent_tool_result_postgres, "accept_start_acquisition_tool_result_uow")


def test_decision_doc_carries_exact_non_closure_and_two_file_scope() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    required_tokens = (
        "> Status: Historical non-live decision-lock characterization",
        "S1e2a has implemented only the specialized pending-submit UoW",
        "approval_receipt_physical_owner_status=characterized_not_ratified",
        "command_acceptance_winner_status=characterized_not_ratified",
        "parent_budget_physical_owner_status=declared_policy_pin_only",
        "cost_reservation_status=decision_locked_not_implemented",
        "default_public_agent_served_population=0",
        "R-019",
        "R-029",
        "OB-2.2",
        "OB-10.3",
        "OB-10.4",
        "Plan §6#6 action-root durable-scope gate",
        "provider/model/live invocation count at zero",
        "docs/TRACK_D_D1N_S1E0_START_AUTHORITY_CHARACTERIZATION.md",
        "tests/test_d1n_s1e0_start_authority_characterization.py",
    )
    for token in required_tokens:
        assert token in document
    assert "formal `GO`" in document
    assert "git add" not in document
