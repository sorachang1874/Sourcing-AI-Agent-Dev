from __future__ import annotations

import ast
import json
import re
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

from sourcing_agent.command_kernel import CommandKernel
from sourcing_agent.repositories.workflow_runtime import WORKFLOW_COMMANDS

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
MIGRATIONS_ROOT = SOURCE_ROOT / "migrations"

CONTROL_PLANE_PATH = SOURCE_ROOT / "control_plane_live_postgres.py"
STORAGE_PATH = SOURCE_ROOT / "storage.py"
ORCHESTRATOR_PATH = SOURCE_ROOT / "orchestrator.py"
FRONTEND_CONTRACT_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.ts"
FRONTEND_SCHEMA_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.schema.json"
FRONTEND_ADAPTER_PATH = REPO_ROOT / "contracts" / "frontend_api_adapter.ts"
FRONTEND_DEMO_API_PATH = REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts"
CHARACTERIZATION_DOC_PATH = REPO_ROOT / "docs" / "TRACK_D_D3A_WORKFLOW_COMMAND_CLAIM_IDENTITY_CHARACTERIZATION.md"

UNIMPLEMENTED_RUNTIME_CLAIM_FIELDS = frozenset(
    {
        "claim_generation",
        "claim_token",
        "lease_token",
        "control_epoch",
    }
)
DORMANT_PHYSICAL_CLAIM_FIELDS = frozenset({"claim_generation", "control_epoch"})
FORBIDDEN_RAW_CAPABILITY_COLUMNS = frozenset({"claim_token", "lease_token"})
D3C2A_DORMANT_COMMAND_COLUMNS = frozenset(
    {
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
        "claim_generation",
        "claim_token_digest",
        "control_epoch",
        "heartbeat_sequence",
        "last_heartbeat_id",
        "terminal_event_id",
        "terminal_outcome_digest",
    }
)

EXPECTED_PRODUCTION_CLAIM_CALLERS = {
    "acquisition_command_owner.py": 6,
    "crm_public_web_owner.py": 3,
    "enrichment.py": 2,
    "profile_fetch_owner.py": 1,
    "excel_intake_owner.py": 1,
    "orchestrator.py": 16,
}


def _class_method(path: Path, class_name: str, method_name: str) -> tuple[ast.FunctionDef, str]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name]
    assert len(classes) == 1, f"expected one {class_name}, found {len(classes)}"
    methods = [node for node in classes[0].body if isinstance(node, ast.FunctionDef) and node.name == method_name]
    assert len(methods) == 1, f"expected one {class_name}.{method_name}, found {len(methods)}"
    segment = ast.get_source_segment(source, methods[0])
    assert segment is not None
    return methods[0], segment


def _normalized_source(source: str) -> str:
    return re.sub(r"\s+", " ", source).strip()


def _migration_workflow_command_schema_statements() -> tuple[str, ...]:
    statements: list[str] = []
    for path in sorted(MIGRATIONS_ROOT.glob("*.sql")):
        for statement in path.read_text(encoding="utf-8").split(";"):
            normalized = _normalized_source(statement).lower()
            if "workflow_commands" not in normalized:
                continue
            if "create table" in normalized or "alter table" in normalized:
                statements.append(normalized)
    return tuple(statements)


def _bootstrap_workflow_command_schema_literals() -> tuple[str, ...]:
    tree = ast.parse(CONTROL_PLANE_PATH.read_text(encoding="utf-8"))
    statements = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        normalized = _normalized_source(node.value).lower()
        if "workflow_commands" not in normalized:
            continue
        if "create table" in normalized or "alter table" in normalized:
            statements.append(normalized)
    return tuple(statements)


def _physical_claim_writer_methods() -> tuple[tuple[str, str, str], ...]:
    matches: list[tuple[str, str, str]] = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        for method in (node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))):
            segment = ast.get_source_segment(source, method) or ""
            normalized = _normalized_source(segment).lower()
            if "update workflow_commands" not in normalized or "set status = 'claimed'" not in normalized:
                continue
            class_name = "<module>"
            current: ast.AST = method
            while current in parents:
                current = parents[current]
                if isinstance(current, ast.ClassDef):
                    class_name = current.name
                    break
            matches.append((path.relative_to(SOURCE_ROOT).as_posix(), class_name, method.name))
    return tuple(matches)


def _physical_workflow_command_mutator_sql_literals() -> tuple[tuple[str, str], ...]:
    matches: list[tuple[str, str]] = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            normalized = _normalized_source(node.value).lower()
            if not any(
                statement in normalized
                for statement in (
                    "insert into workflow_commands",
                    "update workflow_commands",
                    "delete from workflow_commands",
                )
            ):
                continue
            matches.append((path.relative_to(SOURCE_ROOT).as_posix(), normalized))
    return tuple(matches)


def _production_claim_callers() -> Counter[str]:
    callers: Counter[str] = Counter()
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        count = sum(
            1
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and (
                (isinstance(node.func, ast.Attribute) and node.func.attr == "claim_workflow_command")
                or (isinstance(node.func, ast.Name) and node.func.id == "claim_workflow_command")
            )
        )
        if count:
            callers[path.relative_to(SOURCE_ROOT).as_posix()] = count
    return callers


def _typescript_function_source(path: Path, signature: str, next_signature: str) -> str:
    source = path.read_text(encoding="utf-8")
    start = source.index(signature)
    end = source.index(next_signature, start + len(signature))
    return source[start:end]


def test_d3c2a_physical_claim_fields_are_dormant_and_raw_capabilities_remain_absent() -> None:
    migration_statements = _migration_workflow_command_schema_statements()
    bootstrap_statements = _bootstrap_workflow_command_schema_literals()

    assert migration_statements
    assert bootstrap_statements
    baseline_create = next(surface for surface in migration_statements if "create table workflow_commands" in surface)
    d3c2a_alter = next(
        surface
        for surface in migration_statements
        if "alter table workflow_commands" in surface and "claim_generation" in surface
    )
    assert "attempt" in baseline_create
    assert DORMANT_PHYSICAL_CLAIM_FIELDS.isdisjoint(set(re.findall(r"[a-z_]+", baseline_create)))
    assert DORMANT_PHYSICAL_CLAIM_FIELDS <= set(re.findall(r"[a-z_]+", d3c2a_alter))
    for surface in (*migration_statements, *bootstrap_statements):
        assert FORBIDDEN_RAW_CAPABILITY_COLUMNS.isdisjoint(set(re.findall(r"[a-z_]+", surface)))
    for surface in bootstrap_statements:
        assert DORMANT_PHYSICAL_CLAIM_FIELDS.isdisjoint(set(re.findall(r"[a-z_]+", surface)))
    assert any("create table" in surface and "attempt" in surface for surface in bootstrap_statements)

    descriptor_columns = frozenset(WORKFLOW_COMMANDS.column_names())
    assert {"attempt", "lease_owner", "lease_expires_at"} <= descriptor_columns
    assert UNIMPLEMENTED_RUNTIME_CLAIM_FIELDS.isdisjoint(descriptor_columns)
    assert D3C2A_DORMANT_COMMAND_COLUMNS.isdisjoint(descriptor_columns)

    mutator_sql_literals = _physical_workflow_command_mutator_sql_literals()
    assert mutator_sql_literals
    for path, sql in mutator_sql_literals:
        observed_tokens = set(re.findall(r"[a-z0-9_]+", sql))
        assert D3C2A_DORMANT_COMMAND_COLUMNS.isdisjoint(observed_tokens), path


def test_attempt_is_resettable_retry_accounting_not_a_monotonic_claim_generation() -> None:
    claim_method, claim_source = _class_method(
        CONTROL_PLANE_PATH,
        "LiveControlPlanePostgresAdapter",
        "claim_workflow_command",
    )
    assert "attempt = attempt + 1" in _normalized_source(claim_source)
    assert (
        not {node.id for node in ast.walk(claim_method) if isinstance(node, ast.Name)}
        & UNIMPLEMENTED_RUNTIME_CLAIM_FIELDS
    )

    for method_name in (
        "mark_workflow_command_partial_progress",
        "mark_workflow_command_waiting_prerequisite",
        "resume_workflow_command",
    ):
        _, method_source = _class_method(
            CONTROL_PLANE_PATH,
            "LiveControlPlanePostgresAdapter",
            method_name,
        )
        normalized = _normalized_source(method_source)
        assert 'attempt = max(0, int(current.get("attempt") or 0) - 1)' in normalized
        assert "attempt = %s" in normalized

    _, retry_source = _class_method(
        CONTROL_PLANE_PATH,
        "LiveControlPlanePostgresAdapter",
        "retry_workflow_command",
    )
    assert "attempt = 0" in _normalized_source(retry_source)


def test_claim_has_one_physical_writer_behind_one_postgres_only_facade() -> None:
    assert _physical_claim_writer_methods() == (
        (
            "control_plane_live_postgres.py",
            "LiveControlPlanePostgresAdapter",
            "claim_workflow_command",
        ),
    )

    _, facade_source = _class_method(STORAGE_PATH, "ControlPlaneStore", "claim_workflow_command")
    normalized_facade = _normalized_source(facade_source)
    assert 'self._call_control_plane_postgres_native( "claim_workflow_command"' in normalized_facade
    assert "UPDATE workflow_commands" not in normalized_facade
    assert "legacy SQLite tail retired" in normalized_facade


def test_all_29_production_claim_callers_are_mechanically_frozen() -> None:
    callers = _production_claim_callers()
    assert callers == Counter(EXPECTED_PRODUCTION_CLAIM_CALLERS)
    assert sum(callers.values()) == 29


def test_descriptor_to_api_path_uses_the_d3c1_closed_public_projection() -> None:
    synthetic_identity = {
        "claim_generation": 7,
        "claim_token": "synthetic-secret-token",
        "lease_token": "synthetic-lease-token",
        "control_epoch": 11,
    }
    command = {
        "command_id": "cmd-characterization",
        "command_type": "characterization.only",
        "owner": "test-owner",
        "status": "claimed",
        "payload": {
            "business_value": "preserved",
            "nested": {"claimTokenDigest": "synthetic-private-verifier"},
        },
        "future_unknown_descriptor": "must-not-project",
        **synthetic_identity,
    }
    probe = SimpleNamespace(
        _workflow_command_agent_exposure_record=lambda _command_type: {},
        _workflow_command_display_contract_record=lambda **_kwargs: {},
        _workflow_command_control_policy_record=lambda **_kwargs: {},
        _workflow_command_control_state_record=lambda **_kwargs: {},
        _workflow_command_activity_spine_policy_record=lambda **_kwargs: {},
    )
    api_record = CommandKernel._workflow_command_api_record(probe, command)  # type: ignore[arg-type]
    assert api_record["claim_generation"] == 7
    assert api_record["control_epoch"] == 11
    assert api_record["payload"] == {"business_value": "preserved", "nested": {}}
    assert "claim_token" not in api_record
    assert "lease_token" not in api_record
    assert "future_unknown_descriptor" not in api_record

    schema = json.loads(FRONTEND_SCHEMA_PATH.read_text(encoding="utf-8"))
    workflow_command_schema = schema["$defs"]["WorkflowCommandRecord"]
    assert workflow_command_schema["additionalProperties"] is False
    assert {"claim_generation", "control_epoch"} <= set(workflow_command_schema["properties"])
    assert {"claim_token", "lease_token"}.isdisjoint(workflow_command_schema["properties"])

    frontend_contract = FRONTEND_CONTRACT_PATH.read_text(encoding="utf-8")
    assert "export interface WorkflowCommandRecord extends JsonObject" not in frontend_contract

    adapter_source = _typescript_function_source(
        FRONTEND_ADAPTER_PATH,
        "export function mapWorkflowCommandRecord",
        "export function mapWorkflowCommandListResponse",
    )
    assert "...(source as JsonObject)" not in adapter_source

    demo_source = _typescript_function_source(
        FRONTEND_DEMO_API_PATH,
        "function deriveWorkflowCommandRecord",
        "function deriveWorkflowActivityRecord",
    )
    assert "raw: record" not in demo_source

    _, list_source = _class_method(ORCHESTRATOR_PATH, "SourcingOrchestrator", "list_workflow_commands_api")
    _, provenance_source = _class_method(
        ORCHESTRATOR_PATH,
        "SourcingOrchestrator",
        "get_operation_run_provenance_api",
    )
    assert "self._workflow_command_api_record" in list_source
    assert "self._workflow_command_api_record_with_execution_summary" in provenance_source


def test_characterization_document_keeps_r019_and_next_decisions_open() -> None:
    document = CHARACTERIZATION_DOC_PATH.read_text(encoding="utf-8")
    assert "Status:" in "\n".join(document.splitlines()[:8])
    for required_text in (
        "characterization-only",
        "29 direct production claim call sites",
        "R-019 remains pending remediation",
        "does not close or waive R-019",
        "## 6. Historical decisions required before implementation",
        "API exposure and redaction",
        "claim + ActivityAttempt",
    ):
        assert required_text in document
