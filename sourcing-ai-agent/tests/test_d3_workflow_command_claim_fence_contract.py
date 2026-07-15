from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

from sourcing_agent.command_kernel import CommandKernel
from sourcing_agent.repositories.workflow_runtime import (
    OPERATION_EVENTS,
    OPERATION_RUNS,
    RUNTIME_OUTBOX,
    WORKFLOW_ACTIVITY_ATTEMPTS,
    WORKFLOW_ACTIVITY_RUNS,
    WORKFLOW_COMMANDS,
    WORKFLOW_ENTITY_DELTAS,
    WORKFLOW_EVENTS,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"

API_PATH = SOURCE_ROOT / "api.py"
COMMAND_KERNEL_PATH = SOURCE_ROOT / "command_kernel.py"
ORCHESTRATOR_PATH = SOURCE_ROOT / "orchestrator.py"
WORKFLOW_RUNTIME_REPOSITORY_PATH = SOURCE_ROOT / "repositories" / "workflow_runtime.py"
LIVE_POSTGRES_PATH = SOURCE_ROOT / "control_plane_live_postgres.py"
FRONTEND_CONTRACT_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.ts"
FRONTEND_SCHEMA_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.schema.json"
FRONTEND_ADAPTER_PATH = REPO_ROOT / "contracts" / "frontend_api_adapter.ts"
FRONTEND_DEMO_API_PATH = REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts"
D3B_CONTRACT_PATH = REPO_ROOT / "docs" / "TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md"
D0_RUNTIME_DESIGN_PATH = REPO_ROOT / "docs" / "TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md"
D3_DESIGN_PATH = REPO_ROOT / "docs" / "TRACK_D_D3_COMPANY_IDENTITY_SELF_VERIFICATION_DESIGN.md"
TRACK_D_PLAN_PATH = REPO_ROOT / "docs" / "TRACK_D_AGENT_RUNTIME_PLAN.md"
NEXT_TODO_PATH = REPO_ROOT / "docs" / "NEXT_TODO.md"
RESIDUAL_LEDGER_PATH = REPO_ROOT / "docs" / "RESIDUAL_LEDGER.md"
DOCS_INDEX_PATH = REPO_ROOT / "docs" / "INDEX.md"
BASELINE_MIGRATION_PATH = SOURCE_ROOT / "migrations" / "0001_baseline.sql"
D3C2A_MIGRATION_PATH = SOURCE_ROOT / "migrations" / "0003_workflow_command_claim_fence_foundation.sql"
D3C2A_IMPLEMENTATION_PATH = (
    REPO_ROOT / "docs" / "TRACK_D_D3C2A_WORKFLOW_COMMAND_CLAIM_FENCE_MIGRATION_IMPLEMENTATION.md"
)
D3C2B_MIGRATION_PATH = SOURCE_ROOT / "migrations" / "0004_d3_scoped_root_foundation.sql"
D3C2B_IMPLEMENTATION_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2B_SCOPED_ROOT_MIGRATION_IMPLEMENTATION.md"
D3C2D_MIGRATION_PATH = SOURCE_ROOT / "migrations" / "0005_d3_activity_claim_chain_foundation.sql"
D3C2D_IMPLEMENTATION_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2D_ACTIVITY_CLAIM_CHAIN_MIGRATION_IMPLEMENTATION.md"

D3C2A_COMMAND_COLUMNS = (
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
)
D3C2B_SCOPED_SESSION_COLUMNS = (
    "runtime_namespace",
    "provider_mode",
    "workspace_id",
    "scope_issuer",
    "scope_digest",
    "creation_source_workflow_command_id",
    "creation_source_event_id",
    "creation_plan_id",
    "creation_plan_revision",
    "creation_plan_bundle_digest",
    "creation_idempotency_key",
)
D3C2B_OPERATION_ROOT_COLUMNS = (
    "runtime_namespace",
    "provider_mode",
    "scope_issuer",
    "scope_digest",
    "coordination_plan_review_id",
)
D3C2D_ACTIVITY_RUN_COLUMNS = (
    "runtime_namespace",
    "provider_mode",
    "scope_digest",
    "coordination_plan_review_id",
    "claim_authority_spec_digest",
    "d3_business_fence_digest",
)
D3C2D_ACTIVITY_ATTEMPT_COLUMNS = (
    "operation_run_id",
    "runtime_namespace",
    "provider_mode",
    "scope_digest",
    "coordination_plan_review_id",
    "claim_authority_spec_digest",
    "d3_business_fence_digest",
    "claim_generation",
    "command_attempt",
    "control_epoch",
)

# D3b is a characterization/decision batch. These CURRENT_* values intentionally
# describe the debt at its pinned baseline; D3c must replace the assertions when
# it installs the physical fence and public projection boundary.
CURRENT_WORKFLOW_COMMAND_COLUMN_COUNT = 33
# D3c1a's two final control envelopes now reproject their canonical nested
# command explicitly; both calls are intentional public-boundary coverage.
CURRENT_PUBLIC_MAPPER_CALL_COUNT = 85
CURRENT_PUBLIC_MAPPER_CALLING_FUNCTION_COUNT = 34
CURRENT_CONTROL_SYNC_CALL_COUNT = 27
CURRENT_CONTROL_SYNC_CALLING_FUNCTION_COUNT = 26
CURRENT_CONTROL_SYNC_FILE_COUNT = 5
CURRENT_RAW_COMMAND_RETURN_COUNT = 0
CURRENT_FRONTEND_WORKFLOW_COMMAND_REF_COUNT = 7
CURRENT_COMMAND_EFFECT_CALL_COUNTS = {
    "append_event_and_reduce": 62,
    "claim_workflow_command": 29,
    "mark_workflow_command_failed": 71,
    "mark_workflow_command_partial_progress": 15,
    "mark_workflow_command_running": 29,
    "mark_workflow_command_succeeded": 39,
    "mark_workflow_command_waiting_prerequisite": 3,
}
CURRENT_COMMAND_PLAN_REQUESTED_CALL_COUNT = 34
CURRENT_R019_STATE_SYNC_COUNTS = {
    "src/sourcing_agent/acquisition_command_owner.py": 8,
    "src/sourcing_agent/command_kernel.py": 2,
    "src/sourcing_agent/crm_public_web_owner.py": 2,
    "src/sourcing_agent/operation_runtime.py": 2,
    "src/sourcing_agent/orchestrator.py": 12,
}

EXPECTED_PHYSICAL_WORKFLOW_COMMAND_MUTATORS = frozenset(
    {
        "cancel_acquisition_owner_command",
        "cancel_workflow_command",
        "claim_workflow_command",
        "mark_workflow_command_failed",
        "mark_workflow_command_partial_progress",
        "mark_workflow_command_running",
        "mark_workflow_command_succeeded",
        "mark_workflow_command_waiting_prerequisite",
        "reawaken_waiting_prerequisite_workflow_commands",
        "resume_workflow_command",
        "retry_workflow_command",
        "update_workflow_command_payload",
        "upsert_workflow_command",
    }
)

# These TARGET_* values are acceptance targets for D3c, not assertions about the
# current product code. Keeping them separate prevents this D3b decision batch
# from going red merely because it deliberately contains no implementation.
D3C_TARGET_RAW_COMMAND_RETURN_COUNT = 0
D3C_TARGET_PRIVATE_CAPABILITY_PUBLIC_OCCURRENCES = 0

PUBLIC_DIAGNOSTIC_FIELDS = frozenset({"claim_generation", "control_epoch"})
IMMUTABLE_COMMAND_SCOPE_FIELDS = frozenset({"runtime_namespace", "provider_mode", "workspace_id"})
PRIVATE_CAPABILITY_FIELDS = frozenset(
    {
        "authority_id",
        "authority_seal",
        "claim_authority_id",
        "claim_authority_seal",
        "claim_authority_spec_digest",
        "claim_token",
        "claim_token_digest",
        "lease_token",
        "claim_capability",
        "claim_secret",
        "claim_selection_generation",
        "consumed_claim_authority_id",
        "last_heartbeat_id",
    }
)
CURRENT_SYNTHETIC_IDENTITY = {
    "authority_id": "d3b-synthetic-private-authority-id-alias",
    "authority_seal": "d3b-synthetic-private-authority-seal-alias",
    "claim_generation": 7,
    "claim_selection_generation": 5,
    "claim_token": "d3b-synthetic-private-capability",
    "claim_token_digest": "d3b-synthetic-private-verifier",
    "claim_authority_id": "d3b-synthetic-private-authority-id",
    "claim_authority_seal": "d3b-synthetic-private-authority-seal",
    "claim_authority_spec_digest": "d3b-synthetic-private-authority-spec-digest",
    "consumed_claim_authority_id": "d3b-synthetic-private-consumed-authority-id",
    "last_heartbeat_id": "d3b-synthetic-private-heartbeat-occurrence-id",
    "lease_token": "d3b-synthetic-lease-capability",
    "control_epoch": 11,
}

EXPECTED_PUBLIC_COMMAND_CARRIER_ROUTES = frozenset(
    {
        "/api/workflow/commands",
        "/api/workflow/commands/{command_id}",
        "/api/workflow/commands/{command_id}/cancel",
        "/api/workflow/commands/{command_id}/retry",
        "/api/workflow/commands/{command_id}/resume",
        "/api/operations/runs",
        "/api/operations/runs/{run_id}",
        "/api/operations/runs/{run_id}/provenance",
        "/api/operations/actions",
        "/api/operations/actions/{action_id}",
        "/api/operations/actions/{action_id}/approve",
        "/api/operations/actions/{action_id}/reject",
        "/api/operations/runs/{run_id}/cancel",
        "/api/operations/runs/{run_id}/retry",
        "/api/operations/runs/{run_id}/resume",
        "/api/operations/runs/{run_id}/dispatch",
    }
)

EXPECTED_ACTIVITY_EVIDENCE_ROUTES = frozenset(
    {
        "/api/workflow/activities",
        "/api/workflow/activities/{activity_id}",
        "/api/workflow/activity-attempts",
        "/api/workflow/activity-attempts/{attempt_id}",
        "/api/workflow/entity-deltas",
        "/api/workflow/entity-deltas/{delta_id}",
    }
)


def _normalized(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().casefold()


def _assert_any(document: str, *alternatives: str) -> None:
    normalized = _normalized(document)
    assert any(_normalized(alternative) in normalized for alternative in alternatives), alternatives


def _document_section(document: str, start_marker: str, end_marker: str | None) -> str:
    assert document.count(start_marker) == 1, start_marker
    start = document.index(start_marker)
    if end_marker is None:
        return document[start:]
    assert document.count(end_marker) == 1, end_marker
    end = document.index(end_marker, start + len(start_marker))
    return document[start:end]


def _assert_in_order(document: str, *fragments: str) -> None:
    normalized = _normalized(document)
    cursor = 0
    for fragment in fragments:
        normalized_fragment = _normalized(fragment)
        position = normalized.find(normalized_fragment, cursor)
        assert position >= 0, fragment
        cursor = position + len(normalized_fragment)


def _assert_all(document: str, *fragments: str) -> None:
    normalized = _normalized(document)
    for fragment in fragments:
        assert _normalized(fragment) in normalized, fragment


def _unique_line(document: str, marker: str) -> str:
    matches = [line for line in document.splitlines() if marker in line]
    assert len(matches) == 1, marker
    return matches[0]


def _markdown_table(section: str) -> tuple[list[str], list[list[str]]]:
    lines = [line for line in section.splitlines() if line.startswith("|")]
    assert len(lines) >= 3
    parsed = [[cell.replace(r"\|", "|").strip() for cell in re.split(r"(?<!\\)\|", line.strip("|"))] for line in lines]
    assert all(re.fullmatch(r":?-{3,}:?", cell) for cell in parsed[1])
    return parsed[0], parsed[2:]


def _class_method_source(path: Path, class_name: str, method_name: str) -> str:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name]
    assert len(classes) == 1
    methods = [
        node
        for node in classes[0].body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == method_name
    ]
    assert len(methods) == 1
    segment = ast.get_source_segment(source, methods[0])
    assert segment is not None
    return segment


def _call_population(path: Path, names: frozenset[str]) -> tuple[int, set[str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    calls: list[ast.Call] = []
    owners: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        called = ""
        if isinstance(node.func, ast.Attribute):
            called = node.func.attr
        elif isinstance(node.func, ast.Name):
            called = node.func.id
        if called not in names:
            continue
        calls.append(node)
        current: ast.AST = node
        while current in parents:
            current = parents[current]
            if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
                owners.add(current.name)
                break
    return len(calls), owners


def _repo_call_population(name: str) -> tuple[int, set[tuple[str, str]]]:
    calls = 0
    owners: set[tuple[str, str]] = set()
    for path in sorted(SOURCE_ROOT.glob("*.py")):
        source = path.read_text(encoding="utf-8")
        if name not in source:
            continue
        tree = ast.parse(source)
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            called = ""
            if isinstance(node.func, ast.Attribute):
                called = node.func.attr
            elif isinstance(node.func, ast.Name):
                called = node.func.id
            if called != name:
                continue
            calls += 1
            current: ast.AST = node
            while current in parents:
                current = parents[current]
                if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    owners.add((path.name, current.name))
                    break
    return calls, owners


def _raw_command_return_population() -> tuple[int, set[str]]:
    count = 0
    files: set[str] = set()
    for path in sorted(SOURCE_ROOT.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            for key, value in zip(node.keys, node.values, strict=True):
                if (
                    isinstance(key, ast.Constant)
                    and key.value == "workflow_command"
                    and isinstance(value, ast.Name)
                    and value.id == "command_payload"
                ):
                    count += 1
                    files.add(path.name)
    return count, files


def _schema_ref_count(value: Any, target: str) -> int:
    if isinstance(value, dict):
        return int(value.get("$ref") == target) + sum(_schema_ref_count(item, target) for item in value.values())
    if isinstance(value, list):
        return sum(_schema_ref_count(item, target) for item in value)
    return 0


def _physical_workflow_command_sql_mutators() -> set[str]:
    source = LIVE_POSTGRES_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    adapters = [
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "LiveControlPlanePostgresAdapter"
    ]
    assert len(adapters) == 1
    mutators: set[str] = set()
    for method in adapters[0].body:
        if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        string_literals = "\n".join(
            str(node.value)
            for node in ast.walk(method)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        )
        if re.search(r"\b(?:INSERT\s+INTO|UPDATE)\s+workflow_commands\b", string_literals, flags=re.IGNORECASE):
            mutators.add(method.name)
    return mutators


def _command_effect_and_r019_populations(
    names: frozenset[str],
) -> tuple[dict[str, list[tuple[str, str, str]]], dict[str, int]]:
    calls: dict[str, list[tuple[str, str, str]]] = {name: [] for name in names}
    state_sync_counts: dict[str, int] = {}
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        state_sync_count = 0
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            called = ""
            if isinstance(node.func, ast.Attribute):
                called = node.func.attr
            elif isinstance(node.func, ast.Name):
                called = node.func.id

            # This exactly mirrors the canonical R-019 receiver boundary in
            # test_storage_surface_guardrails.py::_workflow_runtime_state_update_count.
            if called in {"update_action_state", "update_operation_state"} and isinstance(node.func, ast.Attribute):
                repository = node.func.value
                if isinstance(repository, ast.Attribute) and repository.attr == "workflow_runtime":
                    repos = repository.value
                    if isinstance(repos, ast.Attribute) and repos.attr == "repos":
                        state_sync_count += 1

            if called not in calls:
                continue
            current: ast.AST = node
            owner = ""
            while current in parents:
                current = parents[current]
                if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    owner = current.name
                    break
            event_type = ""
            for keyword in node.keywords:
                if (
                    keyword.arg == "event_type"
                    and isinstance(keyword.value, ast.Constant)
                    and isinstance(keyword.value.value, str)
                ):
                    event_type = keyword.value.value
                    break
            calls[called].append((str(path.relative_to(REPO_ROOT)), owner, event_type))
        if state_sync_count:
            state_sync_counts[str(path.relative_to(REPO_ROOT))] = state_sync_count
    return calls, state_sync_counts


def test_d3b_document_declares_characterization_scope_and_open_gates() -> None:
    assert D3B_CONTRACT_PATH.exists()
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")

    _assert_any(
        document,
        "characterization-only",
        "characterization and decision only",
        "decision-lock document",
        "documentation only",
        "决策/机械 characterization",
    )
    _assert_any(document, "zero-product-code", "zero product code", "零产品码")
    _assert_any(document, "zero migration", "zero-migration", "零 migration")
    _assert_any(document, "later implementation", "later implementation/migration commit", "D3c")
    _assert_any(document, "R-019 remains open", "R-019 remains pending", "R-019 保持 open", "R-019 仍 open")
    _assert_any(document, "served=0", "served = 0", "served agent tool population remains **zero**")
    _assert_any(
        document,
        "formal review pending",
        "formal independent review",
        "fresh pinned non-author review is required",
        "正式 review pending",
    )


def test_d3c2a_migration_is_exactly_the_dormant_command_subbatch() -> None:
    sql = D3C2A_MIGRATION_PATH.read_text(encoding="utf-8")
    normalized = _normalized(sql)
    added_columns = tuple(re.findall(r"\bADD COLUMN ([a-z0-9_]+)\b", sql, flags=re.IGNORECASE))

    assert added_columns == D3C2A_COMMAND_COLUMNS
    assert len(added_columns) == 20
    assert "operation_run_id" not in added_columns
    assert "SET LOCAL lock_timeout = '5s'" in sql
    assert "SET LOCAL lock_timeout = DEFAULT" in sql
    assert normalized.count("not valid") == 16
    assert "validate constraint" not in normalized
    assert "foreign key" not in normalized
    assert "create index" not in normalized
    assert "claim_token " not in normalized
    assert "lease_token" not in normalized
    assert set(D3C2A_COMMAND_COLUMNS).isdisjoint(WORKFLOW_COMMANDS.column_names())


def test_d3c2a_document_keeps_full_migration_runtime_and_residual_gates_open() -> None:
    document = D3C2A_IMPLEMENTATION_PATH.read_text(encoding="utf-8")

    _assert_all(
        document,
        "only the `workflow_commands` command subbatch",
        "does not complete Migration A",
        "twenty command columns",
        "sixteen local `CHECK ... NOT VALID` constraints",
        "33-column `WORKFLOW_COMMANDS` descriptor",
        "No command insert, selection, claim, heartbeat, control, terminal, retry, resume, dispatch",
        "Full D3b Migration A still requires",
        "OB-10.1/10.2/10.3/10.4",
        "R-019 remain pending",
    )
    _assert_any(document, "serve an Agent tool", "served Agent tool")
    _assert_any(document, "fresh pinned non-author review", "pinned non-author review")


def test_d3c2b_migration_is_exactly_the_dormant_scoped_root_subbatch() -> None:
    sql = D3C2B_MIGRATION_PATH.read_text(encoding="utf-8")
    normalized = _normalized(sql)
    alter_sections = re.findall(
        r"ALTER TABLE ([a-z0-9_]+)(.*?);",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )

    assert [table for table, _section in alter_sections] == ["plan_review_sessions", "operation_runs"]
    session_columns = tuple(re.findall(r"\bADD COLUMN ([a-z0-9_]+)\b", alter_sections[0][1], flags=re.IGNORECASE))
    operation_columns = tuple(re.findall(r"\bADD COLUMN ([a-z0-9_]+)\b", alter_sections[1][1], flags=re.IGNORECASE))
    assert session_columns == D3C2B_SCOPED_SESSION_COLUMNS
    assert operation_columns == D3C2B_OPERATION_ROOT_COLUMNS
    assert "SET LOCAL lock_timeout = '5s'" in sql
    assert "SET LOCAL lock_timeout = DEFAULT" in sql
    assert normalized.count("not valid") == 16
    assert "validate constraint" not in normalized
    assert "foreign key" not in normalized
    assert "create index" not in normalized
    assert set(D3C2B_OPERATION_ROOT_COLUMNS).isdisjoint(OPERATION_RUNS.column_names())


def test_d3c2b_document_keeps_scoped_runtime_and_later_rollout_steps_open() -> None:
    document = D3C2B_IMPLEMENTATION_PATH.read_text(encoding="utf-8")

    _assert_all(
        document,
        "Dormant scoped review-session and OperationRun root foundation",
        "eleven scope, causal-plan, and idempotency columns",
        "five scope exact-copy and nullable coordination columns",
        "sixteen local `CHECK ... NOT VALID` constraints",
        "both existing runtime mappings remain closed",
        "ActivityRun/ActivityAttempt",
        "response/failure receipt",
        "Registry/policy pins",
        "cannot precede complete Migration A",
        "OB-10.1/10.2/10.3/10.4",
        "R-019",
    )
    _assert_any(document, "served Agent tool", "served Agent command")
    _assert_any(document, "fresh pinned non-author review", "pinned non-author review")


def test_d3c2d_migration_is_exactly_the_dormant_activity_claim_chain_subbatch() -> None:
    sql = D3C2D_MIGRATION_PATH.read_text(encoding="utf-8")
    normalized = _normalized(sql)
    alter_sections = re.findall(
        r"ALTER TABLE ([a-z0-9_]+)(.*?);",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )

    assert [table for table, _section in alter_sections] == [
        "workflow_activity_runs",
        "workflow_activity_attempts",
    ]
    run_columns = tuple(re.findall(r"\bADD COLUMN ([a-z0-9_]+)\b", alter_sections[0][1], flags=re.IGNORECASE))
    attempt_columns = tuple(re.findall(r"\bADD COLUMN ([a-z0-9_]+)\b", alter_sections[1][1], flags=re.IGNORECASE))
    assert run_columns == D3C2D_ACTIVITY_RUN_COLUMNS
    assert attempt_columns == D3C2D_ACTIVITY_ATTEMPT_COLUMNS
    assert "SET LOCAL lock_timeout = '5s'" in sql
    assert "SET LOCAL lock_timeout = DEFAULT" in sql
    assert normalized.count("not valid") == 18
    assert "validate constraint" not in normalized
    assert "foreign key" not in normalized
    assert "create index" not in normalized
    assert "claim_token" not in normalized
    assert "attempt_number" not in attempt_columns
    assert set(D3C2D_ACTIVITY_RUN_COLUMNS).isdisjoint(WORKFLOW_ACTIVITY_RUNS.column_names())
    assert set(D3C2D_ACTIVITY_ATTEMPT_COLUMNS).isdisjoint(WORKFLOW_ACTIVITY_ATTEMPTS.column_names())


def test_d3c2d_document_keeps_activity_runtime_and_later_migration_fragments_open() -> None:
    document = D3C2D_IMPLEMENTATION_PATH.read_text(encoding="utf-8")

    _assert_all(
        document,
        "Dormant ActivityRun / ActivityAttempt claim-chain foundation",
        "six ActivityRun columns",
        "ten ActivityAttempt columns",
        "eighteen local `CHECK ... NOT VALID` constraints",
        "20/22-column descriptors",
        "attempt_number",
        "command_attempt",
        "workflow-event fragment",
        "response/failure receipt",
        "OB-10.1/10.2/10.3/10.4",
        "R-019",
    )
    _assert_any(document, "served Agent tool", "served Agent command")
    _assert_any(document, "fresh pinned non-author review", "pinned non-author review")


def test_d3b_document_pins_owner_sot_and_three_part_identity() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")

    _assert_any(document, "owner", "归属")
    _assert_any(document, "source of truth", "source-of-truth", "SoT")
    _assert_any(document, "three-part identity", "three-part claim identity", "三元 identity", "三元身份")
    for required in ("command_id", "claim_generation", "control_epoch"):
        assert required in document
    _assert_any(document, "claim_token", "opaque capability", "opaque claim capability")
    _assert_any(document, "ActivityAttempt", "activity_attempt_id")
    _assert_any(
        document,
        "`attempt` remains retry-budget accounting",
        "not attempt",
        "attempt is not",
        "infer generation from `attempt`",
        "attempt 不是",
        "不得从 attempt",
    )


def test_d3b_document_pins_immutable_command_scope_without_fallbacks() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")

    for field in IMMUTABLE_COMMAND_SCOPE_FIELDS:
        assert field in document
    _assert_any(
        document,
        "workflow_commands also has no physical `runtime_namespace`, `provider_mode`, or `workspace_id`",
        "current table has none of the scope columns",
    )
    _assert_any(
        document,
        "workflow-runtime command-creation repository copies the trusted runtime scope once",
        "command-creation owner supplies",
    )
    _assert_any(document, "immutable after insert", "copies them into the command row in the same UoW")
    _assert_any(document, "caller payload", "command payload")
    _assert_any(document, "claim-time environment fallback", "ambient environment read")
    _assert_any(
        document,
        "fails closed rather than inventing `default` or using the current worker environment",
        "cannot infer command scope from ambient environment/JSON",
    )


def test_d3b_document_covers_transitions_uow_cas_and_stale_zero_write() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    normalized = _normalized(document)

    for transition in ("claim", "heartbeat", "partial", "prerequisite", "terminal", "cancel", "retry", "resume"):
        assert transition in normalized
    _assert_any(document, "timeout/reclaim", "timeout + reclaim", "timeout", "reclaim")
    _assert_any(document, "unit of work", "UoW")
    _assert_any(document, "atomic claim mint", "atomic mint", "atomically mint", "原子 mint", "原子签发")
    _assert_any(document, "compare-and-swap", "CAS")
    _assert_any(document, "mandatory consumers", "consumer matrix", "CAS consumer matrix", "consumer 矩阵")
    assert "stale_claim" in document
    _assert_any(document, "zero durable writes", "zero-write", "zero write", "零写")
    _assert_any(document, "child command", "child-command")
    _assert_any(document, "EntityDelta", "entity delta")


def test_d3b_document_pins_dispatch_authorization_linearization() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")

    _assert_any(document, "pre-transport claim check by itself is not sufficient")
    _assert_any(document, "dispatch-authorization linearization point", "transport linearization boundary")
    _assert_any(document, "same transaction-scoped advisory coordination lock")
    _assert_any(
        document,
        "operation root -> optional plan/review/gate -> all participating `workflow_commands` in deterministic order -> intent/current predecessor -> ActivityRun/Attempt -> optional grant/cost/receipt",
    )
    _assert_any(
        document,
        "invalidation first, the dispatch UoW fails and there is no send",
        "control-first = no dispatch row/no send",
    )
    _assert_any(document, "already authorized in-flight work", "dispatch-first = one authorized exposure")
    _assert_any(
        document,
        "every result-slot, event, command, artifact, EntityDelta, and domain-apply CAS reject the old result",
        "zero stale result application",
    )
    _assert_any(document, "No database transaction is held across network I/O")


def test_d3b_document_clears_terminal_digest_and_replays_without_capability() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")

    _assert_any(document, "active claim -> `succeeded`")
    _assert_any(document, "active claim -> `failed_terminal`")
    _assert_any(document, "terminal transition", "terminal success/failure")
    _assert_all(
        document,
        "clear on every invalidating or terminal transition",
        "only result-terminal reopen clears the present command terminal pair before requeue",
        "historical event stays immutable",
    )
    _assert_any(
        document,
        "terminal replay uses committed result/event idempotency",
        "terminal exact replay relies on result/event idempotency",
    )
    _assert_any(document, "never a retained capability", "never needs token material", "no claim token")


def test_d3b_document_separates_public_diagnostics_from_private_capability() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")

    _assert_any(document, "non-authoritative diagnostics", "public diagnostic", "public diagnostics", "公开 diagnostic")
    _assert_any(
        document,
        "private, opaque authorization capability",
        "private capability",
        "private opaque capability",
        "私有 capability",
    )
    _assert_any(document, "allowlist projection", "public allowlist", "allowlist projector")
    _assert_any(document, "public schemas and TypeScript adapters list the two diagnostic fields explicitly")
    for surface in ("payload", "result", "operation events", "frontend", "logs"):
        assert surface in document
    _assert_any(document, "zero-hit", "zero occurrences", "0 occurrences", "零出现", "零落盘")


def test_d3b_document_covers_brownfield_rolling_deploy_and_bridge_deletion() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")

    _assert_any(document, "brownfield", "existing rows", "存量行")
    _assert_any(document, "sentinel triple", "existing rows therefore receive", "backfill")
    _assert_any(document, "rolling deploy", "rolling deployment", "滚动部署")
    _assert_any(
        document,
        "legacy active sentinel claims",
        "pre-migration lease",
        "pre-migration claims",
        "迁移前 lease",
    )
    _assert_any(document, "compatibility bridge", "migration bridge", "兼容 bridge")
    _assert_any(document, "bridge deletion gate", "deletion condition", "removal condition", "删除条件")
    _assert_any(document, "fail closed", "fail-closed")


def test_d3b_preclaim_authority_is_private_exact_command_authorization() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    authority = _document_section(
        document,
        "### 5.1 Canonical registry-issued pre-claim `ClaimAuthority`",
        "### 5.2 Durable scoped review-session root and propagation chain",
    )

    _assert_in_order(
        authority,
        "At process composition, the private registry/scheduler owner installs an unexported factory used by the trusted exact-command selection path",
        "owner wrappers receive no startup-minted or standing authority",
        "first proves that any prior execution lease or exact-selection reservation is absent or expired at repository time",
        "Only that selection UoW increments `claim_selection_generation`",
        "clears `consumed_claim_authority_id`",
        "persist a short exact-selection reservation",
        "A second selection cannot replace it before repository-time expiry",
        "The returned authority expiry exactly equals that persisted reservation expiry",
        "Only then, after proving the operation scope, does the factory server-mint one fresh sealed immutable `ClaimAuthority`",
        "pass it to the registered owner invocation for that selected command",
    )
    for bound_field in (
        "expected_operation_id",
        "expected_command_id",
        "expected_command_type",
        "expected_command_owner",
        "trusted_runtime_namespace",
        "trusted_provider_mode",
        "trusted_workspace_id",
        "trusted_scope_digest",
        "worker_identity",
        "lease_identity",
        "expected_claim_selection_generation",
        "expected_control_epoch",
        "authority_expires_at",
    ):
        assert bound_field in authority
    _assert_in_order(
        authority,
        "Knowing a `command_id` or holding the wrapper across operations is insufficient to call Stage A",
        "Stage A accepts only `(sealed ClaimAuthority, command_id)`",
        "claimant/owner/type/scope/lease fields are read from the authority, never separate arguments",
    )
    _assert_all(
        authority,
        "`consumed_claim_authority_id=''`",
        "installs `consumed_claim_authority_id=authority.authority_id`",
        "expired, previously consumed, or concurrently duplicated authority",
    )
    assert "comparing `row.owner` to itself" in authority
    assert "constructing a lookalike dataclass is not authorization" in authority
    _assert_in_order(
        authority,
        "`not_applied(reason=stale_claim, detail_code=claim_authority_rejected)` before claim/effect writes",
    )


def test_d3b_scope_issuer_and_full_activity_chain_are_physical_and_fail_closed() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    scope = _document_section(
        document,
        "### 5.2 Durable scoped review-session root and propagation chain",
        "### 5.3 Heartbeat occurrence identity",
    )

    assert "The scoped review-session repository is the sole strict-D3 scope/coordination issuer" in scope
    _assert_all(scope, "legacy `create_plan_review_session(...)`", "JSON-scan lookup/reuse paths cannot seed")
    for review_column in (
        "plan_review_sessions.runtime_namespace TEXT NOT NULL DEFAULT ''",
        "plan_review_sessions.provider_mode TEXT NOT NULL DEFAULT ''",
        "plan_review_sessions.workspace_id TEXT NOT NULL DEFAULT ''",
        "plan_review_sessions.scope_issuer TEXT NOT NULL DEFAULT ''",
        "plan_review_sessions.scope_digest TEXT NOT NULL DEFAULT ''",
    ):
        assert review_column in scope
    for root_column in (
        "operation_runs.runtime_namespace TEXT NOT NULL DEFAULT ''",
        "operation_runs.provider_mode TEXT NOT NULL DEFAULT ''",
        "operation_runs.scope_issuer TEXT NOT NULL DEFAULT ''",
        "operation_runs.scope_digest TEXT NOT NULL DEFAULT ''",
    ):
        assert root_column in scope
    assert "strict D3: plan_review_session" in scope
    _assert_in_order(
        scope,
        "The scoped review session is created first and is the physical scope root",
        "OperationRun creation locks that session",
        "exact-copies",
    )
    for downstream_table in (
        "`workflow_commands`",
        "`workflow_activity_runs`",
        "`workflow_activity_attempts`",
        "`workflow_events` terminal rows",
        "`verification_intent`",
    ):
        assert downstream_table in scope
    assert "operation -> command -> ActivityRun -> ActivityAttempt chain" in scope
    assert "no downstream repository defaults a missing workspace to `default`" in scope
    _assert_in_order(scope, "reads ambient environment, or trusts payload JSON")
    assert "Action-backed strict D3 remains disabled until" in scope
    _assert_all(
        scope,
        "Plan section 6 item 6 action-root durable-scope gate",
        "separate scoped prerequisite with no OB-ID",
        "must not be credited to or conflated with OB-10.4",
    )


def test_d3b_heartbeat_uses_repository_occurrence_identity_not_caller_time() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    heartbeat = _document_section(
        document,
        "### 5.3 Heartbeat occurrence identity",
        "## 6. Two-stage safe transaction boundary",
    )

    for field in (
        "command_id",
        "claim_generation",
        "expected_heartbeat_sequence",
        "heartbeat_id             # repository-issued UUIDv7",
    ):
        assert field in heartbeat
    _assert_in_order(
        heartbeat,
        "validates the complete `ClaimAuthority`, exact current sequence, and registry-owned renewal horizon",
        "increments `heartbeat_sequence` once",
        "computes expiry from repository time",
        "The caller cannot provide expiry",
        "An exact retry with the same heartbeat id and expected sequence returns the already committed row",
        "without extending again",
    )
    for rejected_occurrence in (
        "a different id at an old sequence",
        "an occurrence from another generation",
        "an expired claim",
        "an occurrence beyond the maximum renewal horizon",
    ):
        assert rejected_occurrence in heartbeat
    assert "rejected with zero write" in heartbeat


def test_d3b_stage_b_atomically_binds_source_and_independent_record_claims() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    stage_b = _document_section(
        document,
        "### 6.2 Stage B — claim-bound execution start and owner convergence",
        "### 6.3 Why generic claim does not create ActivityAttempt in Stage A",
    )

    _assert_in_order(
        stage_b,
        "lock the root verification-intent lineage and its exact current predecessor",
        "both Stage B and asynchronous owner supersession call the same repository primitive with the physical expected predecessor tuple",
        "exact-CAS the current predecessor against that stored tuple",
        "atomically supersede it",
        "create the successor intent/source binding for this command/generation/attempt",
        "for an independent record command, leave the source binding immutable",
        "create only the record command's own ActivityRun/Attempt execution identity",
        "commit ActivityRun, ActivityAttempt, old-intent supersession, successor intent or record execution, and running state together",
    )
    for source_binding_field in (
        "source_verification_command_id",
        "source_claim_generation",
        "source_control_epoch",
        "source_activity_run_id",
        "source_activity_attempt_id",
        "source_claim_authority_spec_digest",
        "expected_source_terminal_status",
        "expected_source_terminal_event_id",
        "expected_source_terminal_outcome_digest",
    ):
        assert source_binding_field in stage_b
    _assert_in_order(
        stage_b,
        "The core tuple never changes",
        "The terminal tuple is initially all-null",
        "appended only by the source command's section-6.6 terminal UoW under an exact expected-null CAS",
        "once present it is immutable",
        "A registered source-command reopen may clear the command row's terminal pair before requeue",
        "it cannot rewrite an intent's already appended historical source tuple",
        "the successor phase receives a new intent/source binding",
    )
    _assert_all(
        stage_b,
        "Only a terminal `final_adjudication` source event",
        "`authorizable | awaiting_budget | needs_human | failed | timed_out`",
        "one record planner and one record command type",
    )
    _assert_in_order(
        stage_b,
        "Its record/apply UoW must simultaneously validate",
        "the record command's current sealed authority + receipt and still-live lease",
        "the immutable source verification binding and exact committed source terminal status/event/outcome digest",
        "locks the source verification and record command rows plus every current idempotency-target command row",
        "current source row's",
        "terminal status, `terminal_event_id`, and `terminal_outcome_digest` to exact-equal the frozen intent binding",
    )
    for current_source_field in (
        "`operation_id`",
        "coordination review id",
        "business-fence digest",
        "`claim_generation`",
        "`control_epoch`",
    ):
        assert current_source_field in stage_b
    assert "the phase manifest does not require, lock, or consume a Tier-2 grant" in stage_b
    for transport_only_predicate in (
        "cost reservation",
        "physical-call exposure",
        "transport kind",
        "physical-call index",
    ):
        _assert_in_order(stage_b, transport_only_predicate)
    assert "A valid source binding with a stale record claim writes nothing" in stage_b
    assert (
        "`not_applied(reason=business_precondition_conflict, detail_code=source_terminal_mismatch)` and also writes nothing"
        in stage_b
    )


def test_d3b_source_binding_and_outcome_table_are_exact_across_design_docs() -> None:
    contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    design = D3_DESIGN_PATH.read_text(encoding="utf-8")
    stage_b = _document_section(
        contract,
        "### 6.2 Stage B — claim-bound execution start and owner convergence",
        "### 6.3 Why generic claim does not create ActivityAttempt in Stage A",
    )
    design_claim = _document_section(
        design,
        "### 4c. verification intent（v4 绑定物理执行身份，round-3 #4）",
        "## 5. 「人永远赢」",
    )

    def _text_block_after(document: str, marker: str) -> tuple[str, ...]:
        match = re.search(re.escape(marker) + r"\s*```text\s*(.*?)\s*```", document, flags=re.DOTALL)
        assert match is not None, marker
        return tuple(
            line.split("#", maxsplit=1)[0].strip()
            for line in match.group(1).splitlines()
            if line.split("#", maxsplit=1)[0].strip()
        )

    def _bold_tuple_after(document: str, marker: str) -> tuple[str, ...]:
        match = re.search(re.escape(marker) + r"\s*\*\*(.*?)\*\*", document, flags=re.DOTALL)
        assert match is not None, marker
        return tuple(part.strip() for part in re.sub(r"\s+", " ", match.group(1)).split("+"))

    source_core = (
        "source_verification_command_id",
        "source_claim_generation",
        "source_control_epoch",
        "source_command_attempt",
        "source_activity_run_id",
        "source_activity_attempt_id",
        "source_claim_authority_spec_digest",
    )
    terminal_tuple = (
        "expected_source_terminal_status",
        "expected_source_terminal_event_id",
        "expected_source_terminal_outcome_digest",
        "expected_source_terminal_transport_variant",
        "expected_source_terminal_provenance_policy_digest",
        "expected_source_response_spec_digest",
        "expected_source_transport_response_receipt_id",
        "expected_source_transport_attempt_failure_receipt_id",
        "expected_source_dispatch_exposure_id",
        "expected_source_physical_call_index",
        "expected_source_provider_call_id_state",
        "expected_source_provider_call_id",
        "expected_source_model_invocation_envelope_ref",
        "expected_source_model_invocation_envelope_digest",
        "expected_source_terminal_reason",
        "expected_source_response_occurrence_id",
        "expected_source_canonical_response_digest",
        "expected_source_canonical_result_digest",
        "expected_source_result_artifact_ref",
        "expected_source_result_artifact_digest",
        "expected_source_failure_occurrence_id",
        "expected_source_failure_code",
        "expected_source_failure_spec_digest",
        "expected_source_canonical_failure_digest",
        "expected_source_retry_policy_revision",
        "expected_source_retry_disposition",
        "expected_source_failure_artifact_ref",
        "expected_source_failure_artifact_digest",
        "expected_source_no_exposure_spec_digest",
    )
    scope_envelope = (
        "runtime_namespace",
        "provider_mode",
        "workspace_id",
        "scope_digest",
        "operation_run_id",
        "coordination_plan_review_id",
        "review_session_id",
    )
    assert _text_block_after(contract, "The verification intent's immutable core source binding is:") == source_core
    assert _text_block_after(contract, "Its separately append-once terminal tuple is:") == terminal_tuple
    assert _bold_tuple_after(design_claim, "exact scope envelope =") == scope_envelope
    assert _bold_tuple_after(design_claim, "不可变 source core tuple =") == source_core
    assert _bold_tuple_after(design_claim, "append-once terminal tuple =") == terminal_tuple
    _assert_all(
        design_claim,
        "coordination_plan_review_id = review_session_id = plan_review_sessions.review_id",
        "同一 canonical positive `BIGINT` id，不是两个 coordination owner/alias",
    )

    outcome_header, outcome_rows = _markdown_table(stage_b)
    assert outcome_header == [
        "`record_outcome`",
        "Exact source condition",
        "`verification_state` after record",
        "`intent_state` after record",
        "Owner event",
        "Gate / resume effect",
    ]
    assert [(row[0], row[2], row[3], row[4]) for row in outcome_rows] == [
        (
            "`authorizable`",
            "`shadow_would_verify`",
            "`applied`",
            "`company_identity_verification_recorded(record_outcome=authorizable)`",
        ),
        (
            "`awaiting_budget`",
            "exactly `pending`; never `needs_human`",
            "`awaiting_budget`",
            "`company_identity_verification_recorded(record_outcome=awaiting_budget)`",
        ),
        (
            "`needs_human`",
            "`needs_human`",
            "`applied`",
            "`company_identity_verification_recorded(record_outcome=needs_human)`",
        ),
        ("`failed`", "`failed`", "`applied`", "`company_identity_verification_recorded(record_outcome=failed)`"),
        (
            "`timed_out`",
            "`timed_out`",
            "`applied`",
            "`company_identity_verification_recorded(record_outcome=timed_out)`",
        ),
    ]
    design_outcome = _document_section(
        design,
        "**`final_adjudication.record_outcome` 的 exact owner transition（D3b round4）**：",
        "**反向失效与 commit 复查（v6，R5#1）**",
    )
    design_header, design_rows = _markdown_table(design_outcome)
    assert design_header == outcome_header
    assert design_rows == outcome_rows
    assert len(design_rows) == 5
    _assert_all(
        stage_b,
        "model fallback",
        "policy-invalid result",
        "non-retryable verification execution failure",
        "company_identity_verification_control_timed_out",
        "does not forge a source `final_adjudication`",
    )


def test_d3b_round4_coordination_business_and_convergence_contract_is_closed() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    authority = _document_section(
        document,
        "### 5.1 Canonical registry-issued pre-claim `ClaimAuthority`",
        "### 5.2 Durable scoped review-session root and propagation chain",
    )
    dispatch = _document_section(
        document,
        "### 6.5 Dispatch authorization and the network boundary",
        "### 6.6 Atomic terminal result and event UoW",
    )
    convergence = _document_section(
        document,
        "#### 6.2.3 Awaiting-budget grant convergence (no lost wakeup)",
        "### 6.3 Why generic claim does not create ActivityAttempt in Stage A",
    )
    transition_table = _document_section(
        document,
        "## 7. Complete command-transition table",
        "## 8. Central claim predicate",
    )
    business = _document_section(
        document,
        "### 8.1 Centralized phase-parameterized D3 business predicate",
        "### 8.2 Mandatory consumers",
    )

    _assert_all(
        authority,
        "allowed_stage_ids",
        "`stage_policy in {required_membership, optional_membership, forbidden}`",
        "expected_stage_id",
        "expected owner, the exact selected row `stage_id` admitted by the registry stage policy",
        "strings, dicts, row contents, caller text, and public manifests cannot create it",
    )
    _assert_all(
        dispatch,
        "`d3-dispatch-v2\\0runtime_namespace\\0provider_mode\\0workspace_id\\0coordination_plan_review_id`",
        "canonical unsigned-decimal ASCII encoding of the positive BIGINT",
        "`plan_review_sessions.review_id`",
        "No owner derives it from a Stage-B `root_intent_id`",
        "Missing/NULL/non-positive lineage",
        "every D3 `OperationRun` terminal/cancel/retry/requeue/resume/reset/rebuild/recovery mutation",
        "There is no third exemption for an OperationRun owner",
    )
    _assert_all(
        business,
        "verify_d3_business_fence(phase, locked_rows, context)",
        "sealed ClaimedCommandContext",
        "sealed AggregateControlContext",
        "stage_b/terminal/record/dispatch/resume_after_grant",
        "`control` accepts only `AggregateControlContext`",
        "All six phases call this same implementation",
        "typed relational columns are the only inputs",
        "control-first zero attempt/intent/event/command/source-tuple write",
    )
    phase_header, phase_rows = _markdown_table(business)
    assert phase_header == ["`phase`", "Additional required pins/rows", "Explicitly not required"]
    assert [row[0] for row in phase_rows] == [
        "`stage_b`",
        "`terminal`",
        "`record`",
        "`dispatch`",
        "`resume_after_grant`",
        "`control`",
    ]
    _assert_all(
        convergence,
        "Grant issue and `record_outcome=awaiting_budget` converge through durable owner state",
        "`company.identity.verification.resume_after_grant`",
        "resume-after-grant-v2:<scope_digest>:<positive-coordination-review-decimal>:<intent_id>:<phase_generation>:<recorded_event_id>",
        "post-apply typed pins",
        "`maybe_plan_resume`",
        "`retry_wait -> queued`",
        "detail_code=resume_convergence_missing)` with zero writes",
        "grant-first",
        "record-first",
        "no one-shot wakeup window",
    )
    _assert_all(
        transition_table,
        "every accepted executable-input change, invalidating control, resume/requeue/reopen",
        "First result-terminalization is coordinated and clears token/lease but deliberately retains the current epoch",
        "Every dispatch-invalidating D3 OperationRun transition uses the same advisory lock/global order",
    )


def test_d3b_successor_convergence_never_commits_two_live_intent_phases() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    convergence = _document_section(
        document,
        "### 6.4 Verification-intent timing decision",
        "### 6.5 Dispatch authorization and the network boundary",
    )

    assert "Generic retry/requeue does **not** mint a successor verification intent" in convergence
    _assert_in_order(
        convergence,
        "physically pins the exact expected predecessor intent id, phase generation, source control epoch, and decision-source event id",
        "Stage B invokes the shared predecessor-CAS primitive",
        "atomically supersedes the exact still-live predecessor",
        "creates the successor intent with the new ActivityAttempt",
        "no Stage-B commit containing both a live predecessor and a new successor",
        "no successor can exist without its current claim-bound attempt",
    )
    assert "Cancel/timeout/human-decision paths that intentionally create no successor" in convergence
    _assert_all(
        convergence,
        "calls the same repository primitive and exact predecessor tuple",
        "A competing tuple mismatch writes zero",
    )
    assert "readers never perform supersession as repair" in convergence


def test_d3b_cross_owner_dispatch_uses_one_lock_order_and_typed_predicates() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    dispatch = _document_section(
        document,
        "### 6.5 Dispatch authorization and the network boundary",
        "### 6.6 Atomic terminal result and event UoW",
    )
    business = _document_section(
        document,
        "### 8.1 Centralized phase-parameterized D3 business predicate",
        "### 8.2 Mandatory consumers",
    )

    _assert_in_order(dispatch, "must first acquire the same transaction-scoped advisory coordination lock")
    for coordinated_mutation in (
        "plan recompile",
        "review decision",
        "gate-epoch/human-transition",
        "grant issue/revoke/supersede mutation",
    ):
        assert coordinated_mutation in dispatch
    assert "`d3_dispatch_coordination_lock_acquire_budget_ms=250`" in dispatch
    assert "`pg_try_advisory_xact_lock`" in dispatch
    assert "takes no row lock/write before success" in dispatch
    _assert_in_order(
        dispatch,
        "operation_runs scope/status parent",
        "-> optional canonical plan row",
        "-> optional plan-review session",
        "-> optional gate current-state row",
        "-> all participating workflow_commands rows in ascending (scope_digest, operation_id, command_id)",
        "-> verification_intent / current predecessor",
        "-> workflow_activity_runs / workflow_activity_attempts",
        "-> optional identity_search_budget_grant",
        "-> optional cost reservation / physical-call exposure / transport evidence receipt",
    )
    _assert_all(
        dispatch,
        "`operation root -> optional plan/review/gate -> all participating workflow_commands in deterministic order -> "
        "intent/predecessor -> ActivityRun/Attempt -> optional grant/cost/transport evidence receipt`",
    )
    _assert_all(
        dispatch,
        "every current owner, source, record, resume, supersession, and current idempotency-target row",
        "Once the transaction enters intent it may not insert, lock, or discover another command row",
    )
    for typed_predicate in (
        "canonical plan id, immutable plan-bundle digest, and `plan_revision`",
        "plan-review session is still `pending`, its `review_revision` matches, it has no terminal decision",
        "`human_transition_pending=false`",
        "gate control epoch, gate revision, and blocking-reason digest",
    ):
        _assert_in_order(business, typed_predicate)
    for dispatch_predicate in (
        "grant id + issuance generation + policy revision to be `active`",
        "OB-10.4 execution context",
        "`(budget_reservation_ref, activity_attempt_id, physical_call_index)`",
    ):
        _assert_in_order(dispatch, dispatch_predicate)
    assert "must expose plan/review/gate revisions, digests, epochs, human-transition state" in dispatch
    _assert_in_order(dispatch, "parsing `plan_json` or `gate_json` inside an authorization CAS is forbidden")
    assert "Every cross-owner invalidating transition" in dispatch
    assert "first acquires the common advisory lock" in dispatch
    _assert_all(
        dispatch,
        "`operation root -> optional plan/review/gate -> all participating command rows -> intent/predecessor -> "
        "ActivityRun/Attempt -> optional grant/cost/receipt` order",
    )
    _assert_all(
        dispatch,
        "generic and owner-specific cancel",
        "retry/requeue/resume",
        "timeout",
        "rebuild/reset/recovery requeue",
        "first result-terminalization to `succeeded`/`failed_terminal`",
        "heartbeat occurrence and a read-only/exact terminal replay",
        "including every cancel variant, are not exempt",
    )


def test_d3b_terminal_digest_event_and_source_binding_share_one_uow() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    terminal = _document_section(
        document,
        "### 6.6 Atomic terminal result and event UoW",
        "## 7. Complete command-transition table",
    )

    _assert_in_order(
        terminal,
        "Transport-backed terminalization consumes one of two typed durable evidence receipts",
        "A valid terminal response uses `TransportResponseReceipt`",
        "canonical D0 §2.2 `ModelInvocationEnvelopeV1`",
        "response_occurrence_id",
        "cannot commit `succeeded`/`failed_terminal` result state separately from its terminal result event",
        "This result-terminalization is dispatch-invalidating",
        "only a read-only exact terminal replay is exempt",
    )
    assert "performs one PG UoW" in terminal
    assert "`terminal_outcome_digest = sha256(canonical_json(terminal_outcome_v1)).hexdigest()`" in terminal
    _assert_all(
        terminal,
        "exact physical identity over scope digest, coordination review id",
        "workflow run id, operation id, command id, activity run/attempt ids",
        "claim generation, control epoch, authority-spec and business-fence digests",
        "transport variant, exact response- or attempt-failure-receipt/exposure/provider/",
        "envelope/response/result/failure provenance",
        "event family/type/id, terminal outcome digest, and idempotency key",
        "existing event `operation_id` equals the command `operation_id` and root `operation_runs.operation_run_id`",
    )
    _assert_in_order(
        terminal,
        "terminalize the ActivityAttempt and command",
        "atomically copy the exact event id + outcome digest",
        "fill the intent's full expected-null source terminal status/event/outcome plus",
        "terminal provenance tuple from that same locked variant in the same UoW",
        "commit all rows together",
    )
    _assert_all(
        terminal,
        "composite unique key `(scope_digest, coordination_plan_review_id, operation_id, command_id, event_id, terminal_outcome_digest)`",
        "one `MATCH SIMPLE DEFERRABLE` composite foreign key",
        "require terminal event id/outcome digest both null or both non-null",
        "A `CHECK` never claims to prove an event row in another table",
    )
    _assert_all(
        terminal,
        "`MATCH FULL` would reject that partial-null composite",
        "any present pair has all six referencing columns non-null and the FK enforces the exact event",
    )
    assert "Terminal exact replay does not use a cleared claim token" in terminal
    _assert_in_order(terminal, "requires exact equality of every physical identity field and terminal digest")
    assert "typed `terminal_replay_conflict` with zero write" in terminal
    assert "cannot create a replacement event" in terminal
    _assert_all(
        terminal,
        "transport-response-occurrence-v1 + scope_digest + dispatch_exposure_id + canonical_delivery_identity",
        "Redelivery therefore returns the same receipt",
        "transport_response_receipt_collision",
        "checked-in typed `NoExposureTerminalSpec`",
        "complete-none `no_exposure` variant",
    )


def test_d3b_late_result_quarantine_is_durable_and_never_authorizable() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    quarantine = _document_section(
        document,
        "#### 9.3.1 Durable late-result quarantine",
        "### 9.4 Current-claim deterministic non-authorized outcome — owner write allowed",
    )

    assert "an authorization sink, never a result slot" in quarantine
    for required_field in (
        "dispatch_exposure_id",
        "physical_call_index",
        "provider_call_id",
        "response_occurrence_id",
        "terminal_reason",
        "canonical_result_digest",
        "result_artifact_ref",
        "result_artifact_digest",
        "rejection_reason",
        "cost_state",
        "retention_state",
        "authorizable=false",
    ):
        assert required_field in quarantine
    assert "single SQL mutation gateway" in quarantine
    _assert_all(
        quarantine,
        "typed result-acceptance insert entrypoint is the only insert writer",
        "typed transport/cost reconciliation CAS entrypoint",
        "typed quarantine retention CAS entrypoint",
        "disjoint typed CAS entrypoints",
    )
    _assert_in_order(
        quarantine,
        "only from the same immutable `TransportResponseReceipt` defined in §6.6",
        "proving its previously committed dispatch exposure",
    )
    _assert_in_order(quarantine, "a stale `ClaimReceipt` alone cannot authorize the insert")
    assert "`(scope_digest, dispatch_exposure_id, response_occurrence_id)`" in quarantine
    _assert_all(quarantine, "any mismatch returns a typed collision", "zero quarantine mutation")
    _assert_all(
        quarantine,
        "TransportResponseReceipt",
        "canonical_delivery_identity",
        "transport-response-occurrence-v1",
        "late-response-v1",
        "Redelivery",
    )
    _assert_all(
        quarantine,
        "registered missing-provider-call-id response state remains valid",
        "complete, parse-valid D0 envelope",
        "terminal reason is `length` or `content_filter` remains a response receipt",
        "When stale it may enter this response-only quarantine",
        "incomplete/truncated wire envelope",
        "registered-protocol parse failure",
        "attempt-failure evidence, not a response receipt or quarantine row",
    )
    _assert_all(
        quarantine,
        "The row is not globally append-only",
        "exactly two monotonic axes may advance through disjoint typed CAS entrypoints",
        "Cost reconciliation and data retention are orthogonal state machines",
        "one mixed `disposition` is forbidden",
        "cost_state: pending_reconciliation -> reconciled_confirmed | reconciled_uncertain | reconciled_no_call",
        "retention_state: retained -> purged_tombstone",
        "Neither axis resets or gates the other",
    )
    _assert_in_order(quarantine, "There is no promotion, retry, or restore transition")
    _assert_in_order(quarantine, "Neither owner may change `authorizable`")
    _assert_in_order(quarantine, "or feed reducer planning")
    assert "absent from public command/activity APIs, model context, normal evidence bundles" in quarantine


def test_d3b_invariant_matrix_has_ten_cells_per_mechanism_and_carries_obligations() -> None:
    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    design = D3_DESIGN_PATH.read_text(encoding="utf-8")
    owner_matrix = _document_section(
        document,
        "## 4. Owner and source-of-truth matrix",
        "## 5. Exact physical command identity contract",
    )
    domain_addendum = _document_section(
        design,
        "**company-identity domain-field owner addendum（6 data rows；沿用同一 10-column shape）**",
        "### 4c. verification intent（v4 绑定物理执行身份，round-3 #4）",
    )
    obligations = _document_section(
        document,
        "## 2. Controlling authority and obligation identity",
        "## 3. Characterized baseline",
    )
    audit = _document_section(document, "## 17. Mechanism × 10-invariant self-audit", None)

    exact_owner_header = [
        "Field/object",
        "Single owner",
        "Physical SOT",
        "Allowed values",
        "Derivation rule",
        "Consumers",
        "Forbidden consumers",
        "Fallback/brownfield status",
        "Migration status",
        "Deletion condition",
    ]
    owner_header, owner_rows = _markdown_table(owner_matrix)
    assert owner_header == exact_owner_header
    assert len(owner_rows) == 26
    assert all(len(row) == 10 for row in owner_rows)
    assert all(all(cell for cell in row) for row in owner_rows)
    owner_rows_by_field = {row[0]: row for row in owner_rows}
    assert len(owner_rows_by_field) == len(owner_rows)
    _assert_all(
        " ".join(owner_rows_by_field["transport terminal provenance registry"]),
        "TERMINAL_PROVENANCE_SPECS",
        "sole semantic owner",
        "TransportResponseSpec",
        "TransportAttemptFailureSpec",
        "NoExposureTerminalSpec",
        "exactly one applicable entry",
        "CommandTypeSpec.terminal_provenance_policy_digest",
        "commands/exposures preserve their historical policy digest",
        "historical",
    )
    _assert_all(
        " ".join(owner_rows_by_field["transport terminal evidence receipts"]),
        "transport evidence repository is the sole receipt writer",
        "TransportResponseSpec",
        "D0-valid response envelope",
        "incomplete/truncated wire or protocol parse failure",
    )

    domain_header, domain_rows = _markdown_table(domain_addendum)
    assert domain_header == exact_owner_header
    assert len(domain_rows) == 6
    assert all(len(row) == 10 for row in domain_rows)
    assert all(all(cell for cell in row) for row in domain_rows)

    for obligation in ("OB-10.1", "OB-10.2", "OB-10.3", "OB-10.4"):
        assert obligation in obligations
        assert obligation in audit
    assert "All three block D3 transport activation" in obligations
    _assert_in_order(obligations, "a future implementation and its own evidence must close them")
    assert "D3b does not relabel them as satisfied" in obligations

    audit_header, mechanism_rows = _markdown_table(audit)
    assert audit_header == [
        "Mechanism",
        "I1",
        "I2",
        "I3",
        "I4",
        "I5",
        "I6",
        "I7",
        "I8",
        "I9",
        "I10",
    ]
    assert len(mechanism_rows) == 10
    expected_mechanisms = {
        "Canonical registry + sealed pre-claim authority",
        "Scoped review session -> OperationRun -> command -> ActivityRun -> Attempt scope",
        "Canonical D3 business fence",
        "Claim generation/token/epoch + heartbeat occurrence",
        "Stage B successor convergence + source/record split",
        "Awaiting-budget resume convergence",
        "Cross-owner dispatch + grant/cost exposure",
        "Terminal provenance registry + atomic command/attempt/event/source binding",
        "Durable late-result quarantine",
        "Public allowlist + brownfield bridge",
    }
    assert {row[0] for row in mechanism_rows} == expected_mechanisms
    assert all(len(row) == 11 for row in mechanism_rows)
    assert all(all(cell for cell in row[1:]) for row in mechanism_rows)
    _assert_all(
        audit,
        "The open cells are the existing stable obligations printed above",
        "plus the separately named Plan section 6 item 6 action-root durable-scope gate",
    )
    assert "implementation evidence is still absent" in audit
    _assert_in_order(audit, "never means runtime complete or formal `GO`")


def test_d3b_advisory_rounds_fixed_forward_are_cross_document_consistent() -> None:
    contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    design = D3_DESIGN_PATH.read_text(encoding="utf-8")
    plan = TRACK_D_PLAN_PATH.read_text(encoding="utf-8")
    todo = NEXT_TODO_PATH.read_text(encoding="utf-8")
    ledger = RESIDUAL_LEDGER_PATH.read_text(encoding="utf-8")
    index = DOCS_INDEX_PATH.read_text(encoding="utf-8")

    contract_authority = _document_section(
        contract,
        "### 5.1 Canonical registry-issued pre-claim `ClaimAuthority`",
        "### 5.2 Durable scoped review-session root and propagation chain",
    )
    contract_stage_b = _document_section(
        contract,
        "### 6.2 Stage B — claim-bound execution start and owner convergence",
        "### 6.3 Why generic claim does not create ActivityAttempt in Stage A",
    )
    contract_terminal = _document_section(
        contract,
        "### 6.6 Atomic terminal result and event UoW",
        "## 7. Complete command-transition table",
    )
    contract_quarantine = _document_section(
        contract,
        "#### 9.3.1 Durable late-result quarantine",
        "### 9.4 Current-claim deterministic non-authorized outcome — owner write allowed",
    )
    contract_columns = _document_section(
        contract,
        "## 5. Exact physical command identity contract",
        "### 5.1 Canonical registry-issued pre-claim `ClaimAuthority`",
    )
    migration_a = _document_section(
        contract,
        "### 11.1 Migration A — additive foundation",
        "### 11.2 Migration B — exact scoped-session/backfill cutover",
    )

    design_flow = _document_section(
        design,
        "### 2.2 W11 接入与 durable join（v4 采纳 round-3 critical #2 的修正链）",
        "### 2.3 Loop 步骤与出口（自包含全文）",
    )
    design_claim = _document_section(
        design,
        "### 4c. verification intent（v4 绑定物理执行身份，round-3 #4）",
        "## 5. 「人永远赢」",
    )
    design_cost = _document_section(
        design,
        "## 8. 项目级恢复与预算（v4 修正计费诚实性，round-3 #7）",
        "## 9. 验收与激活边界",
    )
    design_revision = _document_section(design, "## 10. 修订史与 findings 覆盖映射", None)
    plan_d3 = _document_section(
        plan,
        "### D3 — 第一垂直切片：公司身份自验证 loop",
        "### D4 — 之后",
    )
    plan_section_6 = _document_section(
        plan,
        "## 6. 实施批义务清单",
        "## 7. v1 评审 findings 处置总索引",
    )
    plan_item_6 = _document_section(
        plan,
        "6. workflow command claim-fence",
        "7. §4b owner 矩阵扩展",
    )
    todo_d3b = _document_section(
        todo,
        "- [x] D3b workflow-command claim-fence contract + effect/CAS consumer characterization",
        "- [x] D3c1 public workflow-command projection seal",
    )
    ledger_r019 = _unique_line(ledger, "| R-019 |")
    index_d3b = _unique_line(index, "TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md")

    # Finding 1: exact-command scheduler selection is the only factory-mint
    # point; no wrapper receives a startup or standing authority.
    _assert_all(
        plan_item_6,
        "repository exact-command selection 后由 private factory 单次铸造 one-use",
        "fenced wrapper 无启动期铸造/standing capability",
    )
    _assert_all(
        design_claim,
        "repository exact-command selection 后由 composition-installed private factory 单次铸造",
        "wrapper 启动时不得铸造或持有 standing capability",
    )
    _assert_all(
        todo_d3b,
        "exact-command selection 后单次铸 one-use ClaimAuthority",
        "禁止 fenced wrapper 启动期铸造/standing capability",
    )
    _assert_all(
        ledger_r019,
        "exact-command selection 后单次 mint one-use ClaimAuthority",
        "fenced wrapper 禁止启动期铸造/standing capability",
    )
    _assert_all(index_d3b, "exact-selection one-use ClaimAuthority", "Stage-A atomic consume/attempt+1")

    # Finding 2: the existing command operation_id remains the sole link, and
    # the future migration adds exactly twenty strict columns.
    physical_column_rows = [line for line in contract_columns.splitlines() if line.startswith("|")][2:]
    physical_column_names: list[str] = []
    for row in physical_column_rows:
        first_cell = row.split("|", maxsplit=2)[1]
        match = re.search(r"`([a-z0-9_]+)(?:\s|`)", first_cell)
        assert match is not None, first_cell
        physical_column_names.append(match.group(1))
    strict_additions = {
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
    assert physical_column_names[0] == "operation_id"
    assert set(physical_column_names[1:]) == strict_additions
    assert len(physical_column_names[1:]) == 20
    assert "operation_id" in WORKFLOW_COMMANDS.column_names()
    assert "operation_run_id" not in WORKFLOW_COMMANDS.column_names()
    _assert_all(
        migration_a,
        "reuses `workflow_commands.operation_id` as the sole operation link",
        "adds exactly these **twenty** strict command columns",
        "does not add an `operation_run_id` alias",
    )
    _assert_all(plan_item_6, "`workflow_commands` 复用既有 `operation_id`", "唯一 operation link", "不得新增 alias")
    _assert_all(
        design_claim,
        "`workflow_commands` 复用既有 `operation_id` 作为唯一 operation link",
        "不新增/双写 `operation_run_id` alias",
    )
    for scoped_document in (todo_d3b, ledger_r019):
        _assert_all(scoped_document, "`workflow_commands` 复用 existing `operation_id`", "不新增 alias")
    _assert_all(index_d3b, "existing command `operation_id` 单一 operation link")

    # Finding 3: record authorization is independent and re-locks the current
    # source command, including epoch/status/event/digest, in the same UoW.
    _assert_all(
        contract_stage_b,
        "locks the source verification and record command rows plus every current idempotency-target command row",
        "current source row's",
        "coordination review id",
        "business-fence digest",
        "`claim_generation`",
        "`control_epoch`",
        "post-claim `attempt`",
        "`source_command_attempt`",
        "terminal status, `terminal_event_id`, and `terminal_outcome_digest`",
    )
    _assert_all(
        design_claim,
        "record 命令 owner 的 terminal-UoW specialization",
        "固定顺序锁 source + record command",
        "claim generation + control epoch + post-claim command attempt + terminal status + terminal event id + canonical outcome digest",
    )
    for scoped_document in (plan_item_6, todo_d3b, ledger_r019):
        _assert_all(
            scoped_document,
            "record UoW",
            "source",
            "record command",
            "generation/epoch/post-claim command attempt/status/event/digest",
        )
    _assert_all(index_d3b, "record+terminal 单 PG UoW")

    # Finding 4: Stage B and async supersession share exactly the same physical
    # four-field expected-predecessor tuple and CAS semantics.
    predecessor_fields = (
        "expected_predecessor_intent_id",
        "expected_predecessor_phase_generation",
        "expected_predecessor_source_control_epoch",
        "expected_predecessor_decision_source_event_id",
    )
    for scoped_document in (contract_stage_b, design_claim):
        observed = tuple(dict.fromkeys(re.findall(r"`(expected_predecessor_[a-z_]+)`", scoped_document)))
        assert observed == predecessor_fields
    _assert_all(
        contract_stage_b,
        "both Stage B and asynchronous owner supersession call the same repository primitive",
        "exact-CAS the current predecessor against that stored tuple",
    )
    _assert_all(design_claim, "并与 Stage B 调同一 owner repository CAS", "exact-CAS 同一 tuple")
    _assert_all(plan_item_6, "四项具名为", "typed nullable physical columns", "all-null", "complete", "half-null")
    _assert_all(todo_d3b, "四项 `workflow_commands` typed nullable predecessor columns", "禁止 JSON/half-null")
    _assert_all(ledger_r019, "四项 typed physical predecessor CAS", "half-null fail closed")
    _assert_all(index_d3b, "strict command 精确 20 additive columns")

    # Finding 5: only final_adjudication plans record, and the typed outcome
    # enum is identical in the controlling design and Plan.
    record_outcomes = ("authorizable", "awaiting_budget", "needs_human", "failed", "timed_out")
    for scoped_document in (contract_stage_b, design_flow, plan_d3):
        _assert_all(scoped_document, "final_adjudication", "record_outcome", *record_outcomes)
    _assert_all(design_flow, "reducer 仅从上述 `final_adjudication` 计划唯一 command type")
    _assert_all(plan_d3, "reducer 仅从 final_adjudication 计划 record")
    for scoped_document in (todo_d3b, ledger_r019):
        _assert_all(scoped_document, "`final_adjudication`", "唯一", "record")
    _assert_all(index_d3b, "record+terminal 单 PG UoW")

    # Finding 6: terminal existence is enforced by a composite unique key and
    # MATCH SIMPLE DEFERRABLE FK; CHECK constraints remain table-local and
    # reject the otherwise-permitted half-null terminal pair.
    _assert_all(
        contract_terminal,
        "composite unique key",
        "`MATCH SIMPLE DEFERRABLE` composite foreign key",
        "command table separately owns only local checks",
        "A `CHECK` never claims to prove an event row in another table",
    )
    _assert_all(
        design_claim,
        "event 建 scope+existing operation_id+command+event id+digest composite unique identity",
        "`MATCH SIMPLE DEFERRABLE` composite FK",
        "本地 `CHECK` 只校验 null-pair/status/digest shape，不假装跨表检查",
    )
    _assert_all(plan_item_6, "event composite unique", "`MATCH SIMPLE DEFERRABLE` FK", "local both-null/both-non-null")
    for scoped_document in (todo_d3b, ledger_r019):
        _assert_all(scoped_document, "both-null/both-non-null CHECK", "`MATCH SIMPLE DEFERRABLE` composite FK")
    _assert_all(index_d3b, "record+terminal 单 PG UoW")

    # Finding 7: every cancel/dispatch-invalidating control coordinates; only
    # heartbeat occurrence and read-only terminal exact replay are exempt.
    _assert_all(
        design_claim,
        "generic/owner cancel",
        "所有 strict D3 dispatch-invalidating command transition",
        "coordination lock",
        "只有 heartbeat occurrence 与 read-only terminal exact replay",
    )
    for scoped_document in (plan_item_6, todo_d3b, ledger_r019):
        _assert_all(scoped_document, "所有 cancel", "coordination lock", "heartbeat", "read-only", "exact replay")
    _assert_all(
        index_d3b,
        "所有 cancel/OperationRun invalidation 共用 coordination",
    )

    # Finding 8: selection generation, repository expiry, and atomic consumed
    # authority identity jointly enforce one use.
    _assert_all(
        contract_authority,
        "`claim_selection_generation`",
        "repository-time expiry",
        "`consumed_claim_authority_id`",
        "can be issued at most once",
    )
    for scoped_document in (plan_item_6, design_claim, todo_d3b, ledger_r019):
        _assert_all(scoped_document, "selection generation", "expiry", "consumed", "one-use")
    _assert_all(index_d3b, "one-use ClaimAuthority", "Stage-A atomic consume")

    # Finding 9: quarantine cost reconciliation and retention are independent
    # axes; a mixed disposition is forbidden in the canonical docs.
    _assert_all(contract_quarantine, "cost_state", "retention_state", "one mixed `disposition` is forbidden")
    _assert_all(design_cost, "`cost_state`", "`retention_state`", "禁止用单一 disposition")
    _assert_all(plan_item_6, "cost_state/retention_state 正交", "禁止混成 disposition")
    for scoped_document in (todo_d3b, ledger_r019):
        _assert_all(scoped_document, "cost", "retention", "quarantine")
    _assert_all(index_d3b, "stable receipt/quarantine")

    # Finding 10: the action-root scope gate is Plan §6#6, has no OB-ID, and
    # is explicitly distinct from OB-10.4's ModelTurnExecutionContext scope.
    contract_obligations = _document_section(
        contract,
        "## 2. Controlling authority and obligation identity",
        "## 3. Characterized baseline",
    )
    _assert_all(
        contract_obligations,
        "Plan section 6 item 6 action-root durable-scope gate",
        "has no OB-ID and is not OB-10.4",
        "OB-10.4 governs `ModelTurnExecutionContext`, not `agent_actions` scope",
    )
    _assert_all(plan_item_6, "action-root durable-scope gate", "无 OB-ID", "不得借 OB-10.4 代替")
    _assert_all(design_claim, "action-root durable-scope gate", "无 OB-ID", "这不是 OB-10.4")
    for scoped_document in (todo_d3b, ledger_r019):
        _assert_all(scoped_document, "action-root durable-scope gate", "无 OB-ID", "非 OB-10.4")
    _assert_all(index_d3b, "action-root durable-scope gate", "OB-10.1/10.2/10.3/10.4 仍 open")

    # Finding 11: all durable summaries retain decision-only status and point
    # back to the canonical contract rather than claiming implementation/GO.
    _assert_all(
        _document_section(contract, "# Track D D3b", "## 1. Outcome and scope"),
        "documentation only",
        "zero product code",
        "zero migration",
        "not a formal `GO`",
        "operator usage limit",
        "have **no verdict**",
        "fail-closed for Live or",
        "signoff only",
    )
    _assert_all(
        design_revision,
        "`TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md`",
        "物理 migration/产品码仍未落地",
    )
    _assert_all(
        plan_section_6,
        "D3c2a/D3c2b 只落了 dormant command-table 与 scoped-root",
        "physical owner/repository/runtime 与其余 Migration A 仍未实施",
    )
    _assert_all(
        todo_d3b,
        "零产品码",
        "零 migration",
        "`0/8/3/0`",
        "11 findings",
        "`0/5/4/0`",
        "9 findings",
        "`0/3/2/0`",
        "parallel semantic audit",
        "`0/5/2/0`",
        "`0/3/3/0`",
        "`0/1/2/0`",
        "`0/4/0/0`",
        "round7 semantic",
        "`0/4/2/0`",
        "round8",
        "`0/1/0/0`",
        "`0/6/1/0`",
        "current author evidence",
        "`32 passed`",
        "`49 passed`",
        "fixed-forward",
        "operator usage limit",
        "没有 verdict",
        "fresh non-author re-review",
        "pinned formal review pending",
        "仅对 Live/signoff fail closed",
    )
    _assert_all(
        ledger_r019,
        "`TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md`",
        "`0/8/3/0`",
        "`0/5/4/0`",
        "9 findings",
        "`0/3/2/0`",
        "parallel semantic audit",
        "`0/5/2/0`",
        "`0/3/3/0`",
        "`0/1/2/0`",
        "`0/4/0/0`",
        "round7 semantic",
        "`0/4/2/0`",
        "round8 semantic `0/1/0/0`",
        "broad `0/6/1/0`",
        "current D3b author `32/49/58/81`",
        "fixed-forward",
        "operator usage limit",
        "没有 verdict",
        "causal-binding artifact invalid",
        "D3c2a author=migration/PG `9 passed + 20 subtests`",
        "fresh pinned review pending",
        "仅对 Live/signoff fail closed",
    )
    _assert_all(
        index_d3b,
        "26×10",
        "20 additive columns",
        "round8 semantic `NO-GO 0/1/0/0`",
        "broad `NO-GO 0/6/1/0`",
        "current author evidence=`32/49/58/81`",
        "operator usage limit",
        "无 verdict",
        "fresh non-author/formal review pending",
        "零产品码",
        "零 migration",
        "served=0",
        "R-019",
    )


def test_d3b_round5_physical_columns_and_bigint_lineage_are_mechanically_exact() -> None:
    contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    design = D3_DESIGN_PATH.read_text(encoding="utf-8")
    plan = TRACK_D_PLAN_PATH.read_text(encoding="utf-8")
    todo = NEXT_TODO_PATH.read_text(encoding="utf-8")
    ledger_r019 = _unique_line(RESIDUAL_LEDGER_PATH.read_text(encoding="utf-8"), "| R-019 |")
    index_d3b = _unique_line(
        DOCS_INDEX_PATH.read_text(encoding="utf-8"), "TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md"
    )
    columns = _document_section(
        contract,
        "## 5. Exact physical command identity contract",
        "### 5.1 Canonical registry-issued pre-claim `ClaimAuthority`",
    )
    header, rows = _markdown_table(columns)
    assert header == ["Column", "Exact contract"]
    physical_names = []
    physical_declarations = {}
    for declaration, exact_contract in rows:
        match = re.match(r"(?:existing )?`([a-z0-9_]+)(?:\s[^`]*)?`", declaration)
        assert match is not None, declaration
        name = match.group(1)
        physical_names.append(name)
        physical_declarations[name] = (declaration, exact_contract)
    assert physical_names == [
        "operation_id",
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
    ]
    assert len(physical_names[1:]) == 20
    assert physical_declarations["coordination_plan_review_id"][0] == "`coordination_plan_review_id BIGINT NULL`"
    assert (
        "type-compatible with canonical BIGINT `plan_review_sessions.review_id`"
        in physical_declarations["coordination_plan_review_id"][1]
    )
    predecessor_types = {
        "expected_predecessor_intent_id": "TEXT NULL",
        "expected_predecessor_phase_generation": "BIGINT NULL",
        "expected_predecessor_source_control_epoch": "BIGINT NULL",
        "expected_predecessor_decision_source_event_id": "TEXT NULL",
    }
    for field, sql_type in predecessor_types.items():
        assert physical_declarations[field][0] == f"`{field} {sql_type}`"
    _assert_all(
        columns,
        "all-null candidate-initial shape",
        "complete four-column predecessor tuple",
        "never reads those values from command payload",
        "after entering intent it may not insert or lock another command row",
    )
    _assert_in_order(
        columns,
        "first locks the operation root and every applicable typed plan/review/gate row",
        "reserves/create-or-locks the non-claimable command identity",
        "Only after that complete command segment does it enter the intent segment",
        "lock the typed base-intent/current verification lineage",
        "uses those already-locked operation/plan/review/gate/base-intent pins",
    )
    migration = _document_section(
        contract,
        "### 11.1 Migration A — additive foundation",
        "### 11.2 Migration B — exact scoped-session/backfill cutover",
    )
    _assert_all(
        migration,
        "exactly these **twenty** strict command",
        "`expected_predecessor_intent_id`",
        "`expected_predecessor_phase_generation`",
        "`expected_predecessor_source_control_epoch`",
        "`expected_predecessor_decision_source_event_id`",
    )
    migration_c = _document_section(
        contract,
        "### 11.3 Migration C — structural scope chain and active-row enforcement",
        "### 11.4 Migration D — validation and sentinel deletion eligibility",
    )
    _assert_all(
        migration_c,
        "coordination_plan_review_id IS NOT NULL",
        "coordination_plan_review_id > 0",
    )

    for scoped_document in (design, plan, todo, ledger_r019):
        _assert_all(scoped_document, "positive", "BIGINT", "strict", "20", "predecessor")
    _assert_all(
        index_d3b,
        "strict command 精确 20 additive columns",
        "canonical positive-BIGINT",
        "exact-copy",
    )
    for scoped_document in (design, plan, todo, ledger_r019):
        _assert_all(
            scoped_document,
            "current-selection one-use",
            "reselection",
            "generation+1",
            "clear",
            "不承担",
            "durable historical audit",
        )


def test_d3b_round5_closed_context_union_global_order_and_control_inventory_are_exact() -> None:
    contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    business = _document_section(
        contract,
        "### 8.1 Centralized phase-parameterized D3 business predicate",
        "### 8.2 Mandatory consumers",
    )
    context_block = re.search(
        r"verify_d3_business_fence\(phase, locked_rows, context\)\s*\n\s*sealed ClaimedCommandContext =\s*(.*?)\n\s*# every ClaimedCommandContext",
        business,
        flags=re.DOTALL,
    )
    assert context_block is not None
    assert re.findall(r"([A-Z][A-Za-z]+Context)", context_block.group(1)) == [
        "StageBContext",
        "TerminalContext",
        "RecordContext",
        "DispatchContext",
        "ResumeAfterGrantContext",
    ]
    _assert_all(
        business,
        "locked_command + sealed ClaimAuthority + matching sealed ClaimReceipt",
        "sealed AggregateControlContext = ControlContext",
        "contains no claim token",
        "registered_typed_control_authority",
        "all_affected_command_expectations",
        "wrong context/phase pair",
        "detail_code=context_shape_mismatch",
        "deterministic non-claimable command reservation",
        "any evaluator failure rolls back the reservation",
        "every returned `not_applied` still has zero durable writes",
    )
    stage_b = _document_section(
        contract,
        "### 6.2 Stage B — claim-bound execution start and owner convergence",
        "### 6.3 Why generic claim does not create ActivityAttempt in Stage A",
    )
    _assert_in_order(
        stage_b,
        "lock the root verification-intent lineage and its exact current predecessor after the command rows",
        "after all phase-required rows are locked, invoke the section-8.1 centralized predicate with `phase=stage_b`",
        "before any ActivityRun/Attempt, intent, event, claimable/terminal command, or source-tuple write",
    )
    convergence = _document_section(
        contract,
        "#### 6.2.3 Awaiting-budget grant convergence (no lost wakeup)",
        "### 6.3 Why generic claim does not create ActivityAttempt in Stage A",
    )
    _assert_in_order(
        convergence,
        "before entering intent, reserves or exact-locks the deterministic successor verification command",
        "It then locks intent/grant, calls `phase=resume_after_grant`",
        "fully populate/make claimable the already-reserved successor command",
        "append/exact-replay its outbox occurrence",
    )
    rollout = _document_section(
        contract,
        "## 12. Rolling deployment and compatibility bridge",
        "## 13. Scope-local fenced-bridge deletion gate",
    )
    _assert_in_order(
        rollout,
        "land the canonical business-fence digest and the six-phase sealed-context",
        "all strict D3 owner paths remain dormant and non-claimable until this step is complete",
        "deploy Stage B atomic successor convergence",
        "only then deploy the advisory-coordinated dispatch boundary",
    )
    phase_header, phase_rows = _markdown_table(business)
    assert phase_header == ["`phase`", "Additional required pins/rows", "Explicitly not required"]
    phases = [row[0].strip("`") for row in phase_rows]
    assert phases == ["stage_b", "terminal", "record", "dispatch", "resume_after_grant", "control"]

    operation_inventory = "terminal/cancel/retry/requeue/resume/reset/rebuild/recovery"
    participating_commands = "source/record/resume/supersession/current owner/idempotency target"
    documents = [
        D3_DESIGN_PATH.read_text(encoding="utf-8"),
        TRACK_D_PLAN_PATH.read_text(encoding="utf-8"),
        NEXT_TODO_PATH.read_text(encoding="utf-8"),
        _unique_line(RESIDUAL_LEDGER_PATH.read_text(encoding="utf-8"), "| R-019 |"),
    ]
    for scoped_document in documents:
        _assert_all(
            scoped_document,
            "stage_b",
            "terminal",
            "record",
            "dispatch",
            "resume_after_grant",
            "control",
            operation_inventory,
            "all participating",
            participating_commands,
        )
    for scoped_document in documents:
        _assert_all(scoped_document, "进入 intent", "不得回头", "insert/lock command")
    _assert_all(
        _unique_line(
            DOCS_INDEX_PATH.read_text(encoding="utf-8"),
            "TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md",
        ),
        "closed typed context union 六 phase mandatory",
        "all participating commands 全局确定序",
        "所有 cancel/OperationRun invalidation 共用 coordination",
    )


def test_d3b_round5_record_terminal_and_post_gate_resume_are_atomic_and_replayable() -> None:
    contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    record = _document_section(
        contract,
        "#### 6.2.2 Independent record-command claim",
        "#### 6.2.3 Awaiting-budget grant convergence (no lost wakeup)",
    )
    convergence = _document_section(
        contract,
        "#### 6.2.3 Awaiting-budget grant convergence (no lost wakeup)",
        "### 6.3 Why generic claim does not create ActivityAttempt in Stage A",
    )
    _assert_in_order(
        record,
        "exactly the record-command specialization of the section-6.6 terminal UoW",
        "one PG transaction",
        "`record` context",
        "`terminal` context",
        "declared `verification_state`",
        "declared `intent_state`",
        "company_identity_verification_recorded",
        "ActivityAttempt terminal state",
        "command terminal status/event pair",
        "A crash rolls every listed write back",
        "Exact replay",
    )
    _assert_all(record, "cannot coexist with a running record command as an intermediate commit")
    _assert_all(
        convergence,
        "it neither locks a grant nor creates a resume command",
        "post-apply typed pins",
        "`resume-after-grant-v2:<scope_digest>:<positive-coordination-review-decimal>:<intent_id>:<phase_generation>:<recorded_event_id>`",
        "contains no grant id or transient delivery id",
        "`maybe_plan_resume`",
        "without adding a mutable event-lock segment",
        "`retry_wait -> queued`",
        "detail_code=resume_convergence_missing)` with zero writes",
        "neither schedules repair nor inserts a command after reaching intent/grant",
        "no one-shot wakeup window",
    )
    for scoped_document in (
        D3_DESIGN_PATH.read_text(encoding="utf-8"),
        TRACK_D_PLAN_PATH.read_text(encoding="utf-8"),
        NEXT_TODO_PATH.read_text(encoding="utf-8"),
    ):
        _assert_all(
            scoped_document,
            "post-gate-apply",
            "recorded_event_id",
            "resume-after-grant-v2:<scope_digest>:<positive-coordination-review-decimal>:<intent_id>:<phase_generation>:<recorded_event_id>",
            "maybe_plan_resume",
            "retry_wait",
        )


def test_d3b_round5_terminal_receipt_and_late_quarantine_use_stable_occurrence_identity() -> None:
    contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    terminal = _document_section(
        contract,
        "### 6.6 Atomic terminal result and event UoW",
        "## 7. Complete command-transition table",
    )
    quarantine = _document_section(
        contract,
        "#### 9.3.1 Durable late-result quarantine",
        "### 9.4 Current-claim deterministic non-authorized outcome — owner write allowed",
    )
    _assert_all(
        terminal,
        "A valid terminal response uses `TransportResponseReceipt`",
        "canonical D0 §2.2 `ModelInvocationEnvelopeV1`",
        "committed dispatch exposure",
        "physical_call_index",
        "provider_call_id_state",
        "canonical_delivery_identity",
        "transport-response-occurrence-v1 + scope_digest + dispatch_exposure_id + canonical_delivery_identity",
        "Redelivery therefore returns the same receipt",
        "transport_response_receipt_collision",
        "complete-none `no_exposure` variant",
    )
    _assert_all(
        quarantine,
        "TransportResponseReceipt",
        "response_occurrence_id",
        "late-response-v1",
        "redelivery",
        "digest",
        "collision",
    )
    _assert_all(
        contract,
        "post-network transport-evidence-only",
        "committed physical-call exposure FOR UPDATE -> applicable typed transport evidence receipt ->",
        "never locks, inserts, or returns to OperationRun, command, intent",
        "cannot authorize a send, terminal event, result apply",
    )
    not_applied_section = _document_section(
        contract,
        "### 9.1 Claim-fence rejection — zero durable writes",
        "### 9.3 Already-authorized in-flight call — cost may occur, application remains fenced",
    )
    reasons = set(re.findall(r"not_applied\(reason=([a-z_]+)(?:,|\))", not_applied_section))
    assert reasons == {"stale_claim", "business_precondition_conflict"}
    _assert_all(contract, "`not_applied` is never an event/state")
    for scoped_document in (
        D3_DESIGN_PATH.read_text(encoding="utf-8"),
        TRACK_D_PLAN_PATH.read_text(encoding="utf-8"),
        NEXT_TODO_PATH.read_text(encoding="utf-8"),
        _unique_line(RESIDUAL_LEDGER_PATH.read_text(encoding="utf-8"), "| R-019 |"),
    ):
        _assert_all(
            scoped_document,
            "TransportResponseReceipt",
            "no_exposure",
            "stable occurrence",
            "late-response-v1",
            "stale_claim",
            "business_precondition_conflict",
            "exposure→receipt→quarantine/cost-axis",
            "不授权 send/apply",
        )
    _assert_all(
        _unique_line(
            DOCS_INDEX_PATH.read_text(encoding="utf-8"),
            "TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md",
        ),
        "late valid response 仅 stable receipt/quarantine",
        "post-network 不回 runtime/domain",
    )


def test_d3b_round6_population_scope_and_terminal_provenance_are_physically_closed() -> None:
    contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    design = D3_DESIGN_PATH.read_text(encoding="utf-8")
    plan = TRACK_D_PLAN_PATH.read_text(encoding="utf-8")
    todo = NEXT_TODO_PATH.read_text(encoding="utf-8")
    baseline = BASELINE_MIGRATION_PATH.read_text(encoding="utf-8")

    baseline_review = re.search(
        r"CREATE TABLE plan_review_sessions \((.*?)\n\);",
        baseline,
        flags=re.DOTALL,
    )
    assert baseline_review is not None
    for missing_scope_column in (
        "runtime_namespace",
        "provider_mode",
        "workspace_id",
        "scope_issuer",
        "scope_digest",
    ):
        assert missing_scope_column not in baseline_review.group(1)

    migration_a = _document_section(
        contract,
        "### 11.1 Migration A — additive foundation",
        "### 11.2 Migration B — exact scoped-session/backfill cutover",
    )
    _assert_all(
        migration_a,
        "adds `runtime_namespace/provider_mode/workspace_id/scope_issuer/scope_digest`",
        "to `plan_review_sessions`",
        "existing legacy creator remains unscoped/ineligible",
    )
    scope = _document_section(
        contract,
        "### 5.2 Durable scoped review-session root and propagation chain",
        "### 5.3 Heartbeat occurrence identity",
    )
    _assert_in_order(
        scope,
        "create_or_exact_replay_scoped_plan_review_session(...)",
        "The scoped review session is created first and is the physical scope root",
        "OperationRun creation locks that session",
        "exact-copies",
    )
    _assert_all(scope, "legacy `create_plan_review_session(...)`", "cannot seed a strict D3 operation")

    authority = _document_section(
        contract,
        "### 5.1 Canonical registry-issued pre-claim `ClaimAuthority`",
        "### 5.2 Durable scoped review-session root and propagation chain",
    )
    _assert_all(
        authority,
        "claim_fence_policy in {legacy_unfenced, scoped_session_bootstrap_v1, d3_v1}",
        "scoped_session_bootstrap_command_types_v1",
        "strict_d3_command_types_v1",
        "Existing immutable `workflow_commands.command_type`",
        "physical population discriminator for both fenced branches",
        "malformed fenced row must not escape a constraint by clearing a digest",
    )
    migration_c = _document_section(
        contract,
        "### 11.3 Migration C — structural scope chain and active-row enforcement",
        "### 11.4 Migration D — validation and sentinel deletion eligibility",
    )
    assert migration_c.count("command_type NOT IN strict_d3_command_types_v1") >= 2
    _assert_all(
        migration_c,
        "scoped_session_bootstrap_command_types_v1",
        "bootstrap-specific `NOT VALID` active-row invariant",
        "coordination_plan_review_id IS NULL",
        "d3_business_fence_digest=''",
        "not subject to the normal positive-review/business-fence checks",
        "bootstrap row cannot satisfy the normal `d3_v1` branch or vice versa",
        "Non-D3 command types remain outside it",
        "Non-D3 legacy success writers are unaffected",
        "an empty scope, coordination id, authority digest, or business digest is never accepted as a guard",
    )

    terminal = _document_section(
        contract,
        "### 6.6 Atomic terminal result and event UoW",
        "## 7. Complete command-transition table",
    )
    terminal_union_match = re.search(
        r"TerminalProvenanceSpec\s*=\s*"
        r"(TransportResponseSpec)\s*\|\s*"
        r"(TransportAttemptFailureSpec)\s*\|\s*"
        r"(NoExposureTerminalSpec)",
        terminal,
    )
    assert terminal_union_match is not None
    assert terminal_union_match.groups() == (
        "TransportResponseSpec",
        "TransportAttemptFailureSpec",
        "NoExposureTerminalSpec",
    )
    _assert_all(
        terminal,
        "canonical_response_digest, canonical_result_digest",
        "result_artifact_ref, result_artifact_digest",
        "result_ref=receipt.result_artifact_ref or ''",
        "result_digest=receipt.canonical_result_digest",
        "terminal_receipt_provenance_mismatch",
        "same-result-digest/different-`result_ref`",
        "half artifact pair",
        "The FK does **not** prove the reverse direction",
        "no terminal event commits without the matching terminalized ActivityAttempt/command",
    )

    assert "10 列 × 26 data rows" in design
    for scoped_document in (design, plan, todo):
        _assert_any(scoped_document, "scoped-session", "scoped review-session")
        _assert_all(
            scoped_document,
            "scoped_session_bootstrap_command_types_v1",
            "strict_d3_command_types_v1",
            "result artifact ref/digest",
        )
        _assert_any(scoped_document, "orphan event", "orphan-event")


def test_d3b_round8_bootstrap_and_terminal_provenance_fixed_forward_are_exact() -> None:
    contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    d0_design = D0_RUNTIME_DESIGN_PATH.read_text(encoding="utf-8")
    design = D3_DESIGN_PATH.read_text(encoding="utf-8")
    plan = TRACK_D_PLAN_PATH.read_text(encoding="utf-8")
    todo = NEXT_TODO_PATH.read_text(encoding="utf-8")
    ledger_r019 = _unique_line(RESIDUAL_LEDGER_PATH.read_text(encoding="utf-8"), "| R-019 |")
    index_d3b = _unique_line(
        DOCS_INDEX_PATH.read_text(encoding="utf-8"),
        "TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md",
    )

    scope = _document_section(
        contract,
        "### 5.2 Durable scoped review-session root and propagation chain",
        "### 5.3 Heartbeat occurrence identity",
    )
    _assert_all(
        scope,
        "create_or_exact_replay_scoped_plan_review_session(...)",
        "accepts only the closed `ScopedSessionBootstrapContext = ScopedSessionCreateContext | ScopedSessionCommittedReplayContext` union",
        "matching Stage-A receipt",
        "claim_fence_policy=scoped_session_bootstrap_v1",
        "mint_scoped_review_session_bootstrap_authority(...)",
        "ScopedReviewSessionBootstrapAuthority",
        "ScopedReviewSessionBootstrapReceipt",
        "ScopedReviewSessionCreateResult",
        "bootstrap Stage-A CAS",
        "returns the private immutable `ScopedReviewSessionBootstrapReceipt` **before** session creation",
        "receipt exact-copies the post-claim command attempt and is current-claim authorization, not an ActivityAttempt or session creation result",
        "verify_scoped_session_bootstrap(context, locked_rows)",
        "Neither mode accepts or manufactures a normal `ClaimAuthority`, normal `ClaimReceipt`, caller review id",
        "ScopedSessionCreateContext",
        "ScopedSessionCommittedReplayContext",
        "committed_exact_replay",
        "creation_source_workflow_command_id",
        "creation_source_event_id",
        "creation_plan_id",
        "creation_plan_revision",
        "creation_plan_bundle_digest",
        "creation_idempotency_key",
        "(scope_digest, creation_idempotency_key)",
        "scoped-plan-review-session-v1",
        "session-created event",
        "ActivityAttempt",
        "authenticated workspace",
        "The bootstrap receipt contains no ActivityRun/ActivityAttempt, `review_id`, session-created event id, or session terminal digest",
        "creates one exact claim-bound ActivityRun/ActivityAttempt",
        "preexisting `ScopedReviewSessionBootstrapReceipt` is consumed as authorization and is never rewritten into the result",
        "post-commit replay uses only `ScopedSessionCommittedReplayContext`",
        "never reconstructs authority/receipt/token",
        "writes nothing",
        "crash after Stage A but before the create UoW",
        "cross-tenant/scope mismatch",
        "scoped_review_session_identity_collision",
        "writes zero session/",
        "event/command rows",
    )
    bootstrap_receipt_match = re.search(
        r"ScopedReviewSessionBootstrapReceipt \{\s*(.*?)\s*\}",
        scope,
        flags=re.DOTALL,
    )
    assert bootstrap_receipt_match is not None
    bootstrap_receipt_shape = bootstrap_receipt_match.group(1)
    _assert_all(
        bootstrap_receipt_shape,
        "bootstrap_authority_id, bootstrap_authority_digest",
        "claim_authority_spec_digest",
        "claim_selection_generation",
        "claim_generation, control_epoch, source_command_attempt",
        "claim_token",
        "lease_owner, lease_identity, lease_expires_at, heartbeat_sequence",
        "creation_source_event_id",
        "creation_plan_id, creation_plan_revision, creation_plan_bundle_digest",
        "creation_idempotency_key",
    )
    for post_create_field in (
        "source_activity_run_id",
        "source_activity_attempt_id",
        "review_id",
        "session_created_event_id",
        "session_created_terminal_outcome_digest",
        "source_terminal_event_id",
        "source_terminal_outcome_digest",
    ):
        assert post_create_field not in bootstrap_receipt_shape
    create_result_match = re.search(
        r"ScopedReviewSessionCreateResult \{\s*(.*?)\s*\}",
        scope,
        flags=re.DOTALL,
    )
    assert create_result_match is not None
    _assert_all(
        create_result_match.group(1),
        "review_id",
        "session_created_event_id, session_created_terminal_outcome_digest",
        "source_activity_run_id, source_activity_attempt_id",
        "source_terminal_event_id, source_terminal_outcome_digest",
    )
    create_context_match = re.search(
        r"ScopedSessionCreateContext \{\s*(.*?)\s*\}",
        scope,
        flags=re.DOTALL,
    )
    replay_context_match = re.search(
        r"ScopedSessionCommittedReplayContext \{\s*(.*?)\s*\}",
        scope,
        flags=re.DOTALL,
    )
    assert create_context_match is not None
    assert replay_context_match is not None
    _assert_all(create_context_match.group(1), "mode = create", "authority:", "receipt:")
    _assert_all(
        replay_context_match.group(1),
        "mode = committed_exact_replay",
        "trusted_runtime_namespace",
        "expected_source_operation_run_id",
        "expected_creation_idempotency_key",
    )
    for forbidden_replay_field in ("authority:", "receipt:", "claim_token", "lease_owner"):
        assert forbidden_replay_field not in replay_context_match.group(1)
    bootstrap_key_match = re.search(
        r"`scoped-session-bootstrap-lock-v1\((.*?)\)`",
        scope,
        flags=re.DOTALL,
    )
    assert bootstrap_key_match is not None
    bootstrap_key_fields = tuple(part.strip() for part in re.sub(r"\s+", " ", bootstrap_key_match.group(1)).split(","))
    assert bootstrap_key_fields == (
        "runtime_namespace",
        "provider_mode",
        "workspace_id",
        "scope_digest",
        "creation_idempotency_key",
    )
    _assert_in_order(
        scope,
        "bootstrap Stage-A CAS",
        "returns the private immutable `ScopedReviewSessionBootstrapReceipt` **before** session creation",
        "For `mode=create`, under that lock the UoW",
        "calls `verify_scoped_session_bootstrap(...)`",
        "creates one exact claim-bound ActivityRun/ActivityAttempt",
        "insert-or-selects the session",
        "appends the sole immutable",
        "terminal pair",
        "returns the immutable `ScopedReviewSessionCreateResult`",
    )
    _assert_in_order(
        scope,
        "The scoped review session is created first and is the physical scope root",
        "committed create result's positive review id",
        "OperationRun creation locks that session",
        "exact-copies",
        "It may not independently remint scope",
    )

    plan_item_6 = _document_section(
        plan,
        "6. workflow command claim-fence",
        "7. §4b owner 矩阵扩展",
    )
    for scoped_document in (plan_item_6, design, todo, ledger_r019, index_d3b):
        _assert_all(
            scoped_document,
            "creation_idempotency_key",
            "scoped",
            "OperationRun",
            "exact-copy",
            "authority",
            "receipt",
            "attempt",
            "tenant",
        )
    _assert_all(
        design,
        "只接受 closed `ScopedSessionCreateContext|ScopedSessionCommittedReplayContext`",
        "`ScopedReviewSessionBootstrapAuthority` +",
        "`ScopedReviewSessionBootstrapReceipt`",
        "不要求 bootstrap 前已存在 ActivityAttempt",
        "不使用 normal",
        "specialized bootstrap Stage B 才在一个 PG UoW 创建",
        "claim-bound ActivityRun/ActivityAttempt",
        "credential-free replay",
    )
    assert "OperationRun/command/ActivityAttempt，要求 current ClaimAuthority+ClaimReceipt" not in design
    authority = _document_section(
        contract,
        "### 5.1 Canonical registry-issued pre-claim `ClaimAuthority`",
        "### 5.2 Durable scoped review-session root and propagation chain",
    )
    claim_predicate = _document_section(
        contract,
        "## 8. Central claim predicate",
        "### 8.1 Centralized phase-parameterized D3 business predicate",
    )
    migration_a = _document_section(
        contract,
        "### 11.1 Migration A — additive foundation",
        "### 11.2 Migration B — exact scoped-session/backfill cutover",
    )
    _assert_all(
        authority,
        "expected_command_attempt       # exact pre-claim value; Stage A must commit +1",
        "pre-claim `attempt`",
        "winning CAS increments `attempt` exactly once",
        "receipt carries that post-claim attempt",
        "Any attempt drift is stale before writes",
    )
    _assert_all(
        _document_section(
            contract,
            "### 5.3 Heartbeat occurrence identity",
            "## 6. Two-stage safe transaction boundary",
        ),
        "attempt                     # exact post-claim workflow_commands.attempt",
        "claim callers cannot supply or override generation, attempt, token, epoch",
    )
    _assert_all(
        claim_predicate,
        "AND wc.attempt = receipt.identity.attempt",
        "workflow_activity_attempts.command_attempt = receipt.identity.attempt = workflow_commands.attempt",
    )
    _assert_all(
        migration_a,
        "generation/post-claim `command_attempt`/epoch columns",
        "seven-field immutable source core",
    )
    for scoped_document in (plan_item_6, design, todo, ledger_r019, index_d3b):
        _assert_all(scoped_document, "pre-claim", "post-claim", "attempt")
    for scoped_document in (design, todo, ledger_r019):
        _assert_all(
            scoped_document,
            "scoped_session_bootstrap_command_types_v1",
            "strict_d3_command_types_v1",
            "observation/zero-unauthorized-increase",
            "legacy_unfenced",
            "generic bridge",
        )
    _assert_all(
        plan_item_6,
        "scoped_session_bootstrap_command_types_v1",
        "strict_d3_command_types_v1",
        "observation/zero-unauthorized-increase",
        "legacy_unfenced",
        "generic legacy bridge",
    )
    _assert_all(
        index_d3b,
        "bootstrap/strict-D3 两个 registry-generated hash-bound population manifests",
        "完整 29 wrapper 仅 observation denominator",
        "legacy_unfenced",
        "generic bridge",
    )
    for stale_issuer_claim in (
        "operation creation owner 先从 server-owned runtime context",
        "poll operation owner 从 server-owned scope issuer",
        "OperationRun scope issuer",
    ):
        assert stale_issuer_claim not in contract
        assert stale_issuer_claim not in plan_item_6
        assert stale_issuer_claim not in design
        assert stale_issuer_claim not in todo
        assert stale_issuer_claim not in ledger_r019

    _assert_all(
        d0_design,
        "**terminal-result-only**",
        "pre-call、transport 与 protocol failure",
        "禁止伪造 terminal/result",
    )
    terminal = _document_section(
        contract,
        "### 6.6 Atomic terminal result and event UoW",
        "## 7. Complete command-transition table",
    )
    _assert_all(
        terminal,
        "terminal_transport_variant       # exposure | attempt_failure | no_exposure",
        "TransportResponseReceipt",
        "TransportAttemptFailureReceipt",
        "TERMINAL_PROVENANCE_SPECS",
        "TerminalProvenanceSpec",
        "TransportResponseSpec",
        "transport_attempt_failure_receipts",
        "TransportAttemptFailureSpec",
        "failure_occurrence_id",
        "failure_code, failure_spec_digest, canonical_failure_digest",
        "failure_artifact_ref, failure_artifact_digest",
        "NoExposureTerminalSpec",
        "allowed_command_types, allowed_stage_ids",
        "allowed_terminal_statuses, allowed_terminal_event_types, allowed_terminal_outcomes",
        "terminal_provenance_policy_digest",
        "exactly one entry for each",
        "terminal_transport_variant",
        "terminal_reason if exposure",
        "failure_code if attempt_failure",
        "old spec entries remain checked in until no active or retained command/exposure/receipt/event/source-intent/quarantine/tombstone/cost-audit identity pins them",
        "retry_policy_revision",
        "retry_disposition_by_failure_code",
        "retry_disposition=terminal",
        "`retryable` receipt",
        "retry_wait",
        "result_ref=receipt.result_artifact_ref or ''",
        "same-result-digest/different-`result_ref`",
        "terminal_receipt_provenance_mismatch",
        "If a response receipt committed first, retry is rejected",
        "If retry commits first, its epoch advance invalidates application",
        "The `no_exposure` branch does **not** lock a nonexistent exposure row",
        "physical dispatch/exposure creation uses that same coordination+command prefix",
        "terminal_reason=length",
        "terminal_reason=content_filter",
        "incomplete or truncated wire envelope",
        "registered protocol parser",
        "quarantine",
        "SELECT ... FOR UPDATE",
        "if the response receipt commits first",
        "if a terminal-disposition failure receipt",
        "Attempt-failure evidence is only a cost/audit sink; it never creates quarantine",
    )
    _assert_in_order(
        terminal,
        "valid terminal response uses",
        "transport/protocol failure after a committed exposure",
        "failure before committed exposure",
        "NoExposureTerminalSpec",
    )
    quarantine = _document_section(
        contract,
        "#### 9.3.1 Durable late-result quarantine",
        "### 9.4 Current-claim deterministic non-authorized outcome — owner write allowed",
    )
    _assert_all(
        quarantine,
        "may insert only from the same immutable `TransportResponseReceipt`",
        "Only a terminal response that already passed D0 envelope validation is representable",
        "parse-valid D0 envelope",
        "`length` or `content_filter` remains a response receipt",
        "When stale it may enter this response-only quarantine",
        "incomplete/truncated wire envelope",
        "registered-protocol parse failure",
        "attempt-failure evidence, not a response receipt or quarantine row",
    )
    rollout = _document_section(
        contract,
        "## 12. Rolling deployment and compatibility bridge",
        "## 13. Scope-local fenced-bridge deletion gate",
    )
    _assert_in_order(
        rollout,
        "extend canonical `CommandTypeSpec` with the three closed `claim_fence_policy` values",
        "install the sole three-variant `TERMINAL_PROVENANCE_SPECS` registry",
        "install `mint_scoped_review_session_bootstrap_authority(...)` plus",
        "`verify_scoped_session_bootstrap(...)`",
        "**dormant**",
        "deploy the scoped review-session repository as the sole scope/coordination issuer",
        "the committed-replay variant carries no credential and is read-only",
        "Only after the session exists may normal positive-review",
        "deploy selection-generation/expiry and one-use authority consumption",
        "land the canonical business-fence digest and the six-phase sealed-context",
        "all strict D3 owner paths remain dormant and non-claimable until this step is complete",
        "deploy Stage B atomic successor convergence",
        "only then deploy the advisory-coordinated dispatch boundary",
    )
    _assert_all(
        _document_section(
            contract,
            "## 13. Scope-local fenced-bridge deletion gate",
            "## 14. R-019 bounded subclosure",
        ),
        "scoped_session_bootstrap_command_types_v1",
        "strict_d3_command_types_v1",
        "complete 29-caller inventory remains the observation and zero-unauthorized-increase denominator",
        "it is **not** a D3b requirement to invent authority policies for or cut over every legacy caller",
        "generic legacy claim compatibility path is explicitly not deletable",
        "legacy command types need not gain D3 pins merely to satisfy this scope-local gate",
    )
    for scoped_document in (design, plan_item_6, todo, ledger_r019):
        _assert_all(
            scoped_document,
            "TERMINAL_PROVENANCE_SPECS",
            "TransportResponseSpec",
            "NoExposureTerminalSpec",
            "retryable",
            "retry_wait",
            "length|content_filter",
            "incomplete",
            "truncated",
            "terminal_transport_variant",
            "terminal_reason if exposure",
            "failure_code if attempt_failure",
            "active",
            "retained",
            "source-intent",
            "quarantine/tombstone",
            "cost/audit",
        )
        _assert_any(scoped_document, "TransportAttemptFailureReceipt", "attempt-failure receipt")
    _assert_all(
        index_d3b,
        "TerminalProvenanceSpec",
        "TransportResponseSpec|TransportAttemptFailureSpec|NoExposureTerminalSpec",
        "length|content_filter",
        "incomplete/truncated",
        "retryable",
        "retry_wait",
        "no_exposure",
    )
    _assert_all(
        _document_section(
            contract, "## 14. R-019 bounded subclosure", "## 15. Served, migration, and activation boundary"
        ),
        "response receipt",
        "attempt-failure receipt",
        "registered no-exposure variant",
    )


def test_current_descriptors_freeze_private_capability_absence_and_generic_carriers() -> None:
    assert len(WORKFLOW_COMMANDS.columns) == CURRENT_WORKFLOW_COMMAND_COLUMN_COUNT
    assert PRIVATE_CAPABILITY_FIELDS.isdisjoint(WORKFLOW_COMMANDS.column_names())
    assert PUBLIC_DIAGNOSTIC_FIELDS.isdisjoint(WORKFLOW_COMMANDS.column_names())
    assert IMMUTABLE_COMMAND_SCOPE_FIELDS.isdisjoint(WORKFLOW_COMMANDS.column_names())

    assert len(WORKFLOW_ACTIVITY_RUNS.columns) == 20
    assert len(WORKFLOW_ACTIVITY_ATTEMPTS.columns) == 22
    assert len(WORKFLOW_ENTITY_DELTAS.columns) == 21
    assert len(WORKFLOW_EVENTS.columns) == 17
    assert len(OPERATION_EVENTS.columns) == 16
    assert len(RUNTIME_OUTBOX.columns) == 18
    assert {"payload_json", "result_json"} <= set(WORKFLOW_COMMANDS.column_names())
    assert "payload_json" in WORKFLOW_EVENTS.column_names()
    assert "payload_json" in OPERATION_EVENTS.column_names()
    assert "payload_json" in RUNTIME_OUTBOX.column_names()


def test_current_command_effect_and_r019_populations_are_mechanically_frozen() -> None:
    physical_mutators = _physical_workflow_command_sql_mutators()
    assert physical_mutators == EXPECTED_PHYSICAL_WORKFLOW_COMMAND_MUTATORS
    assert len(physical_mutators) == 13

    calls, state_sync_counts = _command_effect_and_r019_populations(frozenset(CURRENT_COMMAND_EFFECT_CALL_COUNTS))
    assert {name: len(call_sites) for name, call_sites in calls.items()} == CURRENT_COMMAND_EFFECT_CALL_COUNTS
    assert (
        sum(event_type == "CommandPlanRequested" for _path, _owner, event_type in calls["append_event_and_reduce"])
        == CURRENT_COMMAND_PLAN_REQUESTED_CALL_COUNT
    )

    assert state_sync_counts == CURRENT_R019_STATE_SYNC_COUNTS
    assert sum(state_sync_counts.values()) == 26


def test_d3c1_public_mapper_is_closed_and_preserves_only_safe_diagnostics() -> None:
    command = {
        "command_id": "cmd-d3b-characterization",
        "command_type": "d3b.characterization.only",
        "owner": "test-owner",
        "status": "claimed",
        **CURRENT_SYNTHETIC_IDENTITY,
    }
    public_record = CommandKernel(store=None)._workflow_command_api_record(command)
    assert public_record["claim_generation"] == CURRENT_SYNTHETIC_IDENTITY["claim_generation"]
    assert public_record["control_epoch"] == CURRENT_SYNTHETIC_IDENTITY["control_epoch"]
    assert PRIVATE_CAPABILITY_FIELDS.isdisjoint(public_record)

    calls, owners = _call_population(
        ORCHESTRATOR_PATH,
        frozenset({"_workflow_command_api_record", "_workflow_command_api_record_with_execution_summary"}),
    )
    assert calls == CURRENT_PUBLIC_MAPPER_CALL_COUNT
    assert len(owners) == CURRENT_PUBLIC_MAPPER_CALLING_FUNCTION_COUNT

    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    _assert_any(document, "public allowlist", "explicit public projection boundary")
    _assert_any(document, "never expose", "must not contain token or digest material")


def test_current_api_carriers_and_nested_raw_bypass_are_mechanically_frozen() -> None:
    api_tree = ast.parse(API_PATH.read_text(encoding="utf-8"))
    routes = {
        node.value
        for node in ast.walk(api_tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.startswith("/api/")
    }
    assert len(EXPECTED_PUBLIC_COMMAND_CARRIER_ROUTES) == 16
    assert EXPECTED_PUBLIC_COMMAND_CARRIER_ROUTES <= routes
    assert len(EXPECTED_ACTIVITY_EVIDENCE_ROUTES) == 6
    assert EXPECTED_ACTIVITY_EVIDENCE_ROUTES <= routes

    control_calls, control_owners = _repo_call_population("_sync_operation_run_from_workflow_command_control")
    assert control_calls == CURRENT_CONTROL_SYNC_CALL_COUNT
    assert len(control_owners) == CURRENT_CONTROL_SYNC_CALLING_FUNCTION_COUNT
    assert len({filename for filename, _function in control_owners}) == CURRENT_CONTROL_SYNC_FILE_COUNT

    raw_returns, raw_return_files = _raw_command_return_population()
    assert raw_returns == CURRENT_RAW_COMMAND_RETURN_COUNT
    assert raw_return_files == set()
    assert D3C_TARGET_RAW_COMMAND_RETURN_COUNT == 0


def test_d3c1_frontend_contract_is_closed_and_names_only_safe_diagnostics() -> None:
    schema = json.loads(FRONTEND_SCHEMA_PATH.read_text(encoding="utf-8"))
    definitions = schema["$defs"]
    workflow_command = definitions["WorkflowCommandRecord"]

    assert workflow_command["additionalProperties"] is False
    assert len(workflow_command["properties"]) == 42
    assert PRIVATE_CAPABILITY_FIELDS.isdisjoint(workflow_command["properties"])
    assert D3C_TARGET_PRIVATE_CAPABILITY_PUBLIC_OCCURRENCES == 0
    assert PUBLIC_DIAGNOSTIC_FIELDS <= set(workflow_command["properties"])
    assert _schema_ref_count(schema, "#/$defs/WorkflowCommandRecord") == CURRENT_FRONTEND_WORKFLOW_COMMAND_REF_COUNT
    for definition_name in (
        "WorkflowCommandExecutionSummary",
        "OperationEventRecord",
        "OperationRunStatusSummary",
        "WorkflowCommandControlResponse",
    ):
        assert definitions[definition_name]["additionalProperties"] is True
    for definition_name in (
        "WorkflowActivityControlTarget",
        "WorkflowActivityRecord",
        "WorkflowActivityAttemptRecord",
        "WorkflowEntityDeltaRecord",
    ):
        assert definitions[definition_name]["additionalProperties"] is False

    contract_source = FRONTEND_CONTRACT_PATH.read_text(encoding="utf-8")
    adapter_source = FRONTEND_ADAPTER_PATH.read_text(encoding="utf-8")
    demo_source = FRONTEND_DEMO_API_PATH.read_text(encoding="utf-8")
    mapper_source = adapter_source[
        adapter_source.index("export function mapWorkflowCommandRecord") : adapter_source.index(
            "export function mapWorkflowCommandListResponse"
        )
    ]
    control_mapper_source = adapter_source[
        adapter_source.index("export function mapWorkflowCommandControlResponse") : adapter_source.index(
            "export function mapWorkflowActivityControlTarget"
        )
    ]
    operation_sync_mapper_source = adapter_source[
        adapter_source.index("export function mapWorkflowCommandOperationSync") : adapter_source.index(
            "export function mapWorkflowCommandListResponse"
        )
    ]
    demo_mapper_source = demo_source[
        demo_source.index("function deriveWorkflowCommandRecord") : demo_source.index(
            "function deriveWorkflowActivityRecord"
        )
    ]

    assert "export interface WorkflowCommandRecord extends JsonObject" not in contract_source
    assert "operation_sync?: WorkflowCommandOperationSync" in contract_source
    assert "...(source as JsonObject)" not in mapper_source
    assert "canonical.operation_sync," in control_mapper_source
    assert "mapWorkflowCommandOperationSyncIfMeaningful," in control_mapper_source
    assert "operation_sync: operationSync," in control_mapper_source
    assert "delete mapped.operation_sync" in control_mapper_source
    assert "payloadWasExactEmpty" in operation_sync_mapper_source
    assert "Object.keys(projection.value).length > 0" in operation_sync_mapper_source
    assert "raw: record" not in demo_mapper_source


def test_current_event_sync_never_names_capability_but_generic_payload_debt_stays_open() -> None:
    terminal_sync = _class_method_source(
        COMMAND_KERNEL_PATH,
        "CommandKernel",
        "_sync_operation_run_from_workflow_command",
    )
    control_sync = _class_method_source(
        ORCHESTRATOR_PATH,
        "SourcingOrchestrator",
        "_sync_operation_run_from_workflow_command_control",
    )
    sync_source = f"{terminal_sync}\n{control_sync}"

    for private_field in PRIVATE_CAPABILITY_FIELDS:
        assert private_field not in sync_source
    assert '"workflow_command_result": command_result' in sync_source
    assert '"command_result": command_result' in sync_source
    assert '"workflow_command": self._workflow_command_api_record(command_payload)' in sync_source

    repository_source = WORKFLOW_RUNTIME_REPOSITORY_PATH.read_text(encoding="utf-8")
    for private_field in PRIVATE_CAPABILITY_FIELDS:
        assert f'Column("{private_field}")' not in repository_source

    document = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    _assert_any(
        document,
        "must not contain token or digest material",
        "capability must never",
        "capability never",
        "capability 绝不",
        "capability 不得",
    )
    _assert_any(document, "result JSON", "payload/result", "payload and result", "payload、result")
    assert "stale_claim" in document
    _assert_any(document, "zero durable writes", "zero-write", "zero write", "零写")
