from __future__ import annotations

import ast
import hashlib
import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
MIGRATIONS_ROOT = SOURCE_ROOT / "migrations"

DECISION_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2I_TYPED_PLAN_REVIEW_GATE_AND_TIER2_GRANT_PARENT_DECISION_LOCK.md"
PLAN_PATH = REPO_ROOT / "docs" / "TRACK_D_AGENT_RUNTIME_PLAN.md"
TODO_PATH = REPO_ROOT / "docs" / "NEXT_TODO.md"
LEDGER_PATH = REPO_ROOT / "docs" / "RESIDUAL_LEDGER.md"
INDEX_PATH = REPO_ROOT / "docs" / "INDEX.md"
BASELINE_PATH = MIGRATIONS_ROOT / "0001_baseline.sql"
SCOPED_ROOT_PATH = MIGRATIONS_ROOT / "0004_d3_scoped_root_foundation.sql"
STORAGE_PATH = SOURCE_ROOT / "storage.py"
REPOSITORIES_INIT_PATH = SOURCE_ROOT / "repositories" / "__init__.py"
FUTURE_REPOSITORY_PATH = SOURCE_ROOT / "repositories" / "plan_review_authority.py"


def _expected_rows(raw: str) -> tuple[tuple[str, ...], ...]:
    return tuple(tuple(cell.strip() for cell in line.split(" || ")) for line in raw.strip().splitlines())


PFX = (
    ("runtime_namespace", "TEXT", "no", "none"),
    ("provider_mode", "TEXT", "no", "none"),
    ("workspace_id", "TEXT", "no", "none"),
    ("scope_digest", "TEXT", "no", "none"),
    ("coordination_plan_review_id", "BIGINT", "no", "none"),
)

EXPECTED_DECLARATIONS = tuple(
    """D3C2I_PARENT_DECISION_V1
implementation_status = decision_locked_not_implemented
scope_prefix = runtime_namespace | provider_mode | workspace_id | scope_digest | coordination_plan_review_id
eligible_provider_modes = live | simulate | scripted
sole_owner = src/sourcing_agent/repositories/plan_review_authority.py::PlanReviewAuthorityRepository
store_entrypoint = store.repos.plan_review_authority
authority_table = plan_review_gate_authority_versions
grant_table = identity_search_budget_grants
consumption_table = identity_search_budget_consumptions
authority_column_count = 32
grant_column_count = 37
consumption_column_count = 22
authority_local_check_count = 16
grant_local_check_count = 15
consumption_local_check_count = 9
local_check_total = 40
new_structural_constraint_count = 25
new_fk_count = 16
new_index_count = 4
new_access_path_count = 5
exposure_plan_parent_column_count = 7
exposure_grant_parent_column_count = 3
human_transition_recovery_max_attempts = 8
default_searches_per_grant = 3
combined_relation_count = 10
combined_structural_constraint_count = 77
combined_fk_count = 45
combined_index_count = 15
combined_forward_action_count = 19
combined_rollback_action_count = 18
matrix_mechanism_count = 3
matrix_invariant_count = 10
matrix_cell_count = 30
legacy_json_authority = forbidden
runtime_activation = forbidden
migration_authorization = forbidden_until_matching_pinned_go""".splitlines()
)

EXPECTED_AUTHORITY = PFX + _expected_rows(
    """
authority_version_id || TEXT || no || none
transition_idempotency_key || TEXT || no || none
predecessor_authority_version_id || TEXT || yes || none
plan_id || TEXT || no || none
plan_bundle_digest || TEXT || no || none
plan_revision || BIGINT || no || none
review_revision || BIGINT || no || none
review_state || TEXT || no || none
review_decision_source_event_id || TEXT || yes || none
gate_control_epoch || BIGINT || no || none
gate_revision || BIGINT || no || none
gate_blocking_reason_digest || TEXT || no || none
gate_state || TEXT || no || none
identity_result_watermark || BIGINT || no || 0
identity_decision_source_event_id || TEXT || yes || none
human_transition_pending || BOOLEAN || no || false
human_transition_convergence_state || TEXT || no || 'none'
human_transition_source_event_id || TEXT || yes || none
human_transition_started_at || TIMESTAMPTZ || yes || none
human_transition_deadline_at || TIMESTAMPTZ || yes || none
human_transition_recovery_attempt || BIGINT || no || 0
human_transition_last_error_code || TEXT || yes || none
is_current || BOOLEAN || no || true
row_version || BIGINT || no || 0
created_at || TIMESTAMPTZ || no || transaction_timestamp()
updated_at || TIMESTAMPTZ || no || transaction_timestamp()
retired_at || TIMESTAMPTZ || yes || none
"""
)

EXPECTED_GRANT = PFX + _expected_rows(
    """
grant_id || TEXT || no || none
grant_issuance_generation || BIGINT || no || none
grant_policy_revision || TEXT || no || none
grant_event_id || TEXT || no || none
operation_run_id || TEXT || no || none
verification_intent_id || TEXT || no || none
verification_intent_phase_generation || BIGINT || no || none
plan_id || TEXT || no || none
plan_bundle_digest || TEXT || no || none
plan_revision || BIGINT || no || none
review_revision || BIGINT || no || none
gate_control_epoch || BIGINT || no || none
gate_revision || BIGINT || no || none
gate_blocking_reason_digest || TEXT || no || none
grant_state || TEXT || no || none
state_version || BIGINT || no || 0
searches_granted || BIGINT || no || none
searches_remaining || BIGINT || no || none
fetches_granted || BIGINT || no || none
fetches_remaining || BIGINT || no || none
model_tokens_granted || BIGINT || no || none
model_tokens_remaining || BIGINT || no || none
wall_time_ms_granted || BIGINT || no || none
wall_time_ms_remaining || BIGINT || no || none
supersedes_grant_id || TEXT || yes || none
supersedes_issuance_generation || BIGINT || yes || none
superseded_by_grant_id || TEXT || yes || none
superseded_by_issuance_generation || BIGINT || yes || none
terminal_source_event_id || TEXT || yes || none
issued_at || TIMESTAMPTZ || no || transaction_timestamp()
updated_at || TIMESTAMPTZ || no || transaction_timestamp()
terminal_at || TIMESTAMPTZ || yes || none
"""
)

EXPECTED_CONSUMPTION = PFX + _expected_rows(
    """
grant_consumption_id || TEXT || no || none
grant_id || TEXT || no || none
grant_issuance_generation || BIGINT || no || none
grant_policy_revision || TEXT || no || none
operation_run_id || TEXT || no || none
command_id || TEXT || no || none
activity_run_id || TEXT || no || none
activity_attempt_id || TEXT || no || none
physical_call_index || BIGINT || no || none
searches_debit || BIGINT || no || none
fetches_debit || BIGINT || no || none
model_tokens_debit || BIGINT || no || none
wall_time_ms_debit || BIGINT || no || none
expected_grant_state_version || BIGINT || no || none
resulting_grant_state_version || BIGINT || no || none
resulting_grant_state || TEXT || no || none
created_at || TIMESTAMPTZ || no || transaction_timestamp()
"""
)

EXPECTED_STRUCTURAL = _expected_rows(
    """
1 || plan_review_gate_authority_versions_pkey || PK || (PFX, authority_version_id)
2 || plan_review_gate_authority_versions_transition_uk || UNIQUE || (PFX, transition_idempotency_key)
3 || plan_review_gate_authority_versions_exposure_uk || UNIQUE || (PFX, plan_id, plan_bundle_digest, plan_revision, review_revision, gate_control_epoch, gate_revision, gate_blocking_reason_digest)
4 || plan_review_gate_authority_versions_review_fk || FK_STD || PFX -> plan_review_sessions(runtime_namespace, provider_mode, workspace_id, scope_digest, review_id)
5 || plan_review_gate_authority_versions_predecessor_fk || FK_STD || (PFX, predecessor_authority_version_id) -> authority identity
6 || plan_review_gate_authority_versions_decision_event_fk || FK_STD || (PFX, review_decision_source_event_id) -> workflow-event scope unique
7 || plan_review_gate_authority_versions_human_event_fk || FK_STD || (PFX, human_transition_source_event_id) -> workflow-event scope unique
8 || plan_review_gate_authority_versions_identity_event_fk || FK_STD || (PFX, identity_decision_source_event_id) -> workflow-event scope unique
9 || identity_search_budget_grants_pkey || PK || (PFX, grant_id)
10 || identity_search_budget_grants_exposure_uk || UNIQUE || (PFX, grant_id, grant_issuance_generation, grant_policy_revision)
11 || identity_search_budget_grants_issuance_uk || UNIQUE || (PFX, grant_issuance_generation)
12 || identity_search_budget_grants_event_uk || UNIQUE || (PFX, grant_event_id)
13 || identity_search_budget_grants_review_fk || FK_STD || PFX -> session scope/review unique
14 || identity_search_budget_grants_authority_fk || FK_STD || (PFX, plan_id, plan_bundle_digest, plan_revision, review_revision, gate_control_epoch, gate_revision, gate_blocking_reason_digest) -> authority exposure unique
15 || identity_search_budget_grants_intent_fk || FK_STD || (PFX, operation_run_id, verification_intent_id, verification_intent_phase_generation) -> verification-intent exposure-parent unique
16 || identity_search_budget_grants_event_fk || FK_STD || (PFX, grant_event_id) -> workflow-event scope unique
17 || identity_search_budget_grants_terminal_event_fk || FK_STD || (PFX, terminal_source_event_id) -> workflow-event scope unique
18 || identity_search_budget_grants_supersedes_fk || FK_STD || (PFX, supersedes_grant_id, supersedes_issuance_generation, grant_policy_revision) -> grant exposure unique
19 || identity_search_budget_grants_superseded_by_fk || FK_STD || (PFX, superseded_by_grant_id, superseded_by_issuance_generation, grant_policy_revision) -> grant exposure unique
20 || identity_search_budget_consumptions_pkey || PK || (PFX, grant_consumption_id)
21 || identity_search_budget_consumptions_call_uk || UNIQUE || (PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id, physical_call_index)
22 || identity_search_budget_consumptions_grant_fk || FK_STD || (PFX, grant_id, grant_issuance_generation, grant_policy_revision) -> grant exposure unique
23 || identity_search_budget_consumptions_attempt_fk || FK_STD || (PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id) -> activity-attempt unique
24 || dispatch_exposures_plan_review_gate_authority_fk || FK_STD || exposure (PFX, columns 19-25) -> authority exposure unique
25 || dispatch_exposures_identity_search_grant_fk || FK_STD || exposure (PFX, columns 41-43) -> grant exposure unique; Tier-1 all-NULL tuple skips under MATCH SIMPLE
"""
)

EXPECTED_INDEXES = _expected_rows(
    """
1 || plan_review_gate_authority_versions_current_uk || plan_review_gate_authority_versions || UNIQUE (PFX) WHERE is_current
2 || plan_review_gate_authority_versions_human_due_idx || plan_review_gate_authority_versions || (runtime_namespace, provider_mode, human_transition_deadline_at, workspace_id, scope_digest, coordination_plan_review_id) WHERE is_current AND human_transition_convergence_state = 'pending'
3 || identity_search_budget_grants_active_intent_uk || identity_search_budget_grants || UNIQUE (PFX, operation_run_id, verification_intent_id, verification_intent_phase_generation) WHERE grant_state = 'active'
4 || identity_search_budget_consumptions_grant_audit_idx || identity_search_budget_consumptions || (PFX, grant_id, grant_issuance_generation, grant_policy_revision, created_at, grant_consumption_id)
"""
)

EXPECTED_ACCESS = _expected_rows(
    """
current typed authority lock by complete PFX || plan_review_gate_authority_versions_current_uk
oldest due human_transition_pending recovery by namespace/mode || plan_review_gate_authority_versions_human_due_idx
active exact grant for one verification-intent phase || identity_search_budget_grants_active_intent_uk
one grant's immutable consumption history || identity_search_budget_consumptions_grant_audit_idx
exact debit replay/collision by physical call || identity_search_budget_consumptions_call_uk
"""
)

EXPECTED_CHECK_NAMES = (
    "prga_versions_runtime_namespace_nonblank_ck",
    "prga_versions_provider_mode_ck",
    "prga_versions_workspace_id_nonblank_ck",
    "prga_versions_scope_digest_shape_ck",
    "prga_versions_coordination_positive_ck",
    "prga_versions_identity_shape_ck",
    "prga_versions_plan_pin_shape_ck",
    "prga_versions_review_shape_ck",
    "prga_versions_gate_pin_shape_ck",
    "prga_versions_gate_state_ck",
    "prga_versions_watermark_source_shape_ck",
    "prga_versions_human_transition_shape_ck",
    "prga_versions_current_retirement_shape_ck",
    "prga_versions_no_self_predecessor_ck",
    "prga_versions_timestamp_order_ck",
    "prga_versions_approved_gate_shape_ck",
    "isbg_runtime_namespace_nonblank_ck",
    "isbg_provider_mode_ck",
    "isbg_workspace_id_nonblank_ck",
    "isbg_scope_digest_shape_ck",
    "isbg_coordination_positive_ck",
    "isbg_identity_shape_ck",
    "isbg_intent_shape_ck",
    "isbg_authority_pin_shape_ck",
    "isbg_state_ck",
    "isbg_state_version_nonnegative_ck",
    "isbg_balance_shape_ck",
    "isbg_supersedes_tuple_ck",
    "isbg_superseded_by_tuple_ck",
    "isbg_terminal_shape_ck",
    "isbg_timestamp_order_ck",
    "isbgc_runtime_namespace_nonblank_ck",
    "isbgc_provider_mode_ck",
    "isbgc_workspace_id_nonblank_ck",
    "isbgc_scope_digest_shape_ck",
    "isbgc_coordination_positive_ck",
    "isbgc_identity_shape_ck",
    "isbgc_physical_call_shape_ck",
    "isbgc_debit_shape_ck",
    "isbgc_version_transition_ck",
)

# SHA-256 of the exact ordered (order, name, predicate) tuples in §6 after Markdown code stripping.
EXPECTED_CHECK_ROWS_SHA256 = "76ee9c6022b42425d22ab7278b419dff197fa9b2e4f5d5beeae31726a2b6bdaf"

EXPECTED_METHODS = (
    "create_or_exact_replay_typed_authority",
    "recompile_typed_plan",
    "begin_human_identity_transition",
    "apply_identity_result_to_gate",
    "finalize_typed_plan_review",
    "recover_due_human_transition",
    "issue_identity_search_budget_grant",
    "consume_identity_search_budget_pre_transport",
    "revoke_identity_search_budget_grant",
    "supersede_identity_search_budget_grant_with_transfer",
    "reconcile_identity_search_budget_grant",
)

EXPECTED_FORWARD = _expected_rows(
    """
1 || adopt_validate_attach || strict upstream parents || D3c2h1 §9.1 exact 13 constraints; preserve parent data
2 || create_table || plan_review_gate_authority_versions || scoped review/session and workflow-event unique targets; D3c2i constraints #1-8 inline
3 || create_table || cost_reservations || D3c2h1 §9.2 #1-4 inline
4 || create_table || verification_intents || D3c2h1 §9.2 #19-24/#27 inline; receipt FKs later
5 || create_table || identity_search_budget_grants || authority, verification-intent, review/event targets; D3c2i #9-19 inline
6 || create_table || identity_search_budget_consumptions || grant and activity-attempt targets; D3c2i #20-23 inline
7 || create_table || dispatch_exposures || cost, authority, grant, upstream targets; D3c2h1 inline set plus D3c2i #24-25; six cycle/forward FKs later
8 || create_table || transport_response_receipts || D3c2h1 §9.2 #28-35 inline
9 || create_table || transport_attempt_failure_receipts || D3c2h1 §9.2 #36-42 inline
10 || create_table || transport_response_classification_intents || D3c2h1 §9.2 #43-46 inline
11 || create_table || workflow_late_result_quarantine || D3c2h1 §9.2 #47-52 inline
12 || attach_fk || dispatch_exposures_base_intent_fk || D3c2h1 §9.2 #13
13 || attach_fk || dispatch_exposures_predecessor_intent_fk || D3c2h1 §9.2 #14
14 || attach_fk || verification_intents_response_receipt_fk || D3c2h1 §9.2 #25
15 || attach_fk || verification_intents_failure_receipt_fk || D3c2h1 §9.2 #26
16 || attach_fk || dispatch_exposures_response_receipt_fk || D3c2h1 §9.2 #17
17 || attach_fk || dispatch_exposures_failure_receipt_fk || D3c2h1 §9.2 #18
18 || create_indexes || all ten future tables || D3c2h1 11 plus D3c2i 4 in listed order
19 || validate_acceptance || combined strict-D3 evidence schema || exact relation/constraint/index/access/DAG, identifier, lock, race, rollback, population, and PG acceptance
"""
)

EXPECTED_ROLLBACK = _expected_rows(
    """
1 || drop_indexes || all 15 combined indexes in reverse order
2 || detach_fk || dispatch_exposures_failure_receipt_fk
3 || detach_fk || dispatch_exposures_response_receipt_fk
4 || detach_fk || verification_intents_failure_receipt_fk
5 || detach_fk || verification_intents_response_receipt_fk
6 || detach_fk || dispatch_exposures_predecessor_intent_fk
7 || detach_fk || dispatch_exposures_base_intent_fk
8 || drop_table || workflow_late_result_quarantine
9 || drop_table || transport_response_classification_intents
10 || drop_table || transport_attempt_failure_receipts
11 || drop_table || transport_response_receipts
12 || drop_table || dispatch_exposures
13 || drop_table || identity_search_budget_consumptions
14 || drop_table || identity_search_budget_grants
15 || drop_table || verification_intents
16 || drop_table || cost_reservations
17 || drop_table || plan_review_gate_authority_versions
18 || detach_drop_if_created || D3c2h1 §9.1 #13 through #1; preserve adopted upstream parent data
"""
)

EXPECTED_MATRIX_HEADER = (
    "mechanism",
    "1 single writer",
    "2 tenant key",
    "3 generation/fence",
    "4 lifecycle",
    "5 late/partial",
    "6 cost honesty",
    "7 physical identity",
    "8 provenance/trust",
    "9 self-contained",
    "10 mode isolation",
)
EXPECTED_MATRIX_MECHANISMS = (
    "typed authority version history",
    "human transition convergence",
    "Tier-2 grant and consumption",
)
EXPECTED_MATRIX_ROWS_SHA256 = "7c4ff4998bf91abe62a3de02ce7eaf08a1770698760daa6383b030e8a130ed01"

FUTURE_TABLES = frozenset(
    {
        "plan_review_gate_authority_versions",
        "identity_search_budget_grants",
        "identity_search_budget_consumptions",
    }
)
FUTURE_SYMBOLS = frozenset(
    {
        "PlanReviewAuthorityRepository",
        "create_or_exact_replay_typed_authority",
        "consume_identity_search_budget_pre_transport",
    }
)


def _clean(value: str) -> str:
    value = value.strip()
    return value.replace("`", "")


def _section(document: str, start: str, end: str | None = None) -> str:
    start_index = document.index(start)
    if end is None:
        return document[start_index:]
    return document[start_index : document.index(end, start_index)]


def _tables(section: str) -> tuple[tuple[tuple[str, ...], tuple[tuple[str, ...], ...]], ...]:
    raw_tables: list[list[tuple[str, ...]]] = []
    current: list[tuple[str, ...]] = []
    for line in section.splitlines():
        if line.startswith("|"):
            cells = tuple(_clean(cell) for cell in line.strip().strip("|").split("|"))
            if not all(re.fullmatch(r":?-+:?", cell) for cell in cells):
                current.append(cells)
        elif current:
            raw_tables.append(current)
            current = []
    if current:
        raw_tables.append(current)
    assert raw_tables
    return tuple((table[0], tuple(table[1:])) for table in raw_tables)


def _fenced_lines_after(document: str, marker: str) -> tuple[str, ...]:
    marker_index = document.index(marker)
    fence_start = document.index("```text", marker_index) + len("```text")
    fence_end = document.index("```", fence_start)
    return tuple(line.strip() for line in document[fence_start:fence_end].splitlines() if line.strip())


def _manifest(document: str, start: str, end: str) -> tuple[tuple[str, str, str, str], ...]:
    header, rows = _tables(_section(document, start, end))[0]
    assert header == ("position", "column", "SQL type", "nullable", "default")
    assert tuple(int(row[0]) for row in rows) == tuple(range(1, len(rows) + 1))
    return tuple((row[1], row[2], row[3], row[4]) for row in rows)


def _rows_digest(rows: tuple[tuple[str, ...], ...]) -> str:
    encoded = json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _source_units() -> tuple[tuple[Path, str, ast.Module], ...]:
    return tuple(
        (path, source, ast.parse(source))
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        if (source := path.read_text(encoding="utf-8"))
    )


def _terminal_call_name(call: ast.Call) -> str:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return ""


def _descriptor_tables() -> frozenset[str]:
    tables: set[str] = set()
    for _, _, tree in _source_units():
        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
            if _terminal_call_name(call) != "TableDescriptor":
                continue
            table_keyword = next((keyword for keyword in call.keywords if keyword.arg == "table"), None)
            assert table_keyword is not None
            assert isinstance(table_keyword.value, ast.Constant)
            assert isinstance(table_keyword.value.value, str)
            tables.add(table_keyword.value.value)
    return frozenset(tables)


def _defined_top_level_symbols() -> frozenset[str]:
    symbols: set[str] = set()
    for _, _, tree in _source_units():
        for node in tree.body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                symbols.add(node.name)
            elif isinstance(node, ast.Assign):
                symbols.update(target.id for target in node.targets if isinstance(target, ast.Name))
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                symbols.add(node.target.id)
    return frozenset(symbols)


def _contract_parts(document: str) -> dict[str, object]:
    authority = _manifest(document, "### 4.1 `plan_review_gate", "### 4.2 `identity_search")
    grant = _manifest(document, "### 4.2 `identity_search", "### 4.3 `identity_search")
    consumption = _manifest(document, "### 4.3 `identity_search", "## 5. Exact structural")

    structural_header, structural = _tables(_section(document, "### 5.1", "### 5.2"))[0]
    assert structural_header == ("order", "name", "kind", "exact child columns / target")
    index_tables = _tables(_section(document, "### 5.2", "## 6. Exact local"))
    index_header, indexes = index_tables[0]
    access_header, access = index_tables[1]
    assert index_header == ("order", "name", "table", "exact ordered columns and predicate")
    assert access_header == ("access path", "sole exact index/constraint")

    check_sections = (
        _section(document, "### 6.1", "### 6.2"),
        _section(document, "### 6.2", "### 6.3"),
        _section(document, "### 6.3", "## 7. Lifecycles"),
    )
    checks: list[tuple[str, ...]] = []
    for section in check_sections:
        header, rows = _tables(section)[0]
        assert header == ("order", "constraint name", "exact predicate")
        checks.extend(rows)

    method_header, methods = _tables(_section(document, "## 8. Exact repository", "## 9. Lock order"))[0]
    assert method_header == ("order", "method", "admitted mutation / exact replay boundary")

    forward_header, forward = _tables(_section(document, "### 11.1", "### 11.2"))[0]
    rollback_header, rollback = _tables(_section(document, "### 11.2", "## 12. Mechanism"))[0]
    assert forward_header == ("order", "operation", "object", "requires present / exact effect")
    assert rollback_header == ("order", "operation", "object / exact effect")

    matrix_header, matrix = _tables(_section(document, "## 12. Mechanism", "## 13. Current"))[0]
    return {
        "declarations": _fenced_lines_after(document, "executable oracle exact-compares"),
        "authority": authority,
        "grant": grant,
        "consumption": consumption,
        "structural": structural,
        "indexes": indexes,
        "access": access,
        "checks": tuple(checks),
        "methods": methods,
        "forward": forward,
        "rollback": rollback,
        "matrix_header": matrix_header,
        "matrix": matrix,
    }


def _assert_exact_contract(document: str) -> None:
    parts = _contract_parts(document)
    assert parts["declarations"] == EXPECTED_DECLARATIONS
    assert parts["authority"] == EXPECTED_AUTHORITY
    assert parts["grant"] == EXPECTED_GRANT
    assert parts["consumption"] == EXPECTED_CONSUMPTION
    assert parts["structural"] == EXPECTED_STRUCTURAL
    assert parts["indexes"] == EXPECTED_INDEXES
    assert parts["access"] == EXPECTED_ACCESS

    checks = parts["checks"]
    assert isinstance(checks, tuple)
    assert tuple(row[1] for row in checks) == EXPECTED_CHECK_NAMES
    assert _rows_digest(checks) == EXPECTED_CHECK_ROWS_SHA256

    methods = parts["methods"]
    assert isinstance(methods, tuple)
    assert tuple(row[1] for row in methods) == EXPECTED_METHODS
    assert tuple(int(row[0]) for row in methods) == tuple(range(1, 12))

    assert parts["forward"] == EXPECTED_FORWARD
    assert parts["rollback"] == EXPECTED_ROLLBACK
    assert parts["matrix_header"] == EXPECTED_MATRIX_HEADER
    matrix = parts["matrix"]
    assert isinstance(matrix, tuple)
    assert tuple(row[0] for row in matrix) == EXPECTED_MATRIX_MECHANISMS
    assert all(len(row) == 11 and all(row[1:]) for row in matrix)
    assert _rows_digest(matrix) == EXPECTED_MATRIX_ROWS_SHA256


def test_exact_declarations_and_three_ordered_manifests() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    parts = _contract_parts(document)
    assert parts["declarations"] == EXPECTED_DECLARATIONS
    assert parts["authority"] == EXPECTED_AUTHORITY
    assert parts["grant"] == EXPECTED_GRANT
    assert parts["consumption"] == EXPECTED_CONSUMPTION
    assert tuple(len(parts[name]) for name in ("authority", "grant", "consumption")) == (32, 37, 22)
    assert all(parts[name][:5] == PFX for name in ("authority", "grant", "consumption"))


def test_structural_parent_targets_indexes_and_checks_are_exact() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    parts = _contract_parts(document)
    assert parts["structural"] == EXPECTED_STRUCTURAL
    assert sum(row[2] == "FK_STD" for row in EXPECTED_STRUCTURAL) == 16
    assert parts["indexes"] == EXPECTED_INDEXES
    assert parts["access"] == EXPECTED_ACCESS

    checks = parts["checks"]
    assert isinstance(checks, tuple)
    assert len(checks) == 40
    assert tuple(row[1] for row in checks) == EXPECTED_CHECK_NAMES
    assert _rows_digest(checks) == EXPECTED_CHECK_ROWS_SHA256
    human_predicate = checks[11][2]
    assert "human_transition_recovery_attempt BETWEEN 0 AND 7" in human_predicate
    assert "human_transition_recovery_attempt = 8" in human_predicate
    assert human_predicate.endswith(") IS TRUE")
    assert checks[27][2].endswith(") IS TRUE")
    assert checks[28][2].endswith(") IS TRUE")


def test_lifecycle_cas_single_writer_and_no_json_authority_are_closed() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    parts = _contract_parts(document)
    methods = parts["methods"]
    assert isinstance(methods, tuple)
    assert tuple(row[1] for row in methods) == EXPECTED_METHODS
    normalized = re.sub(r"\s+", " ", document)

    for marker in (
        "new issuance -> active",
        "active -> exhausted",
        "active -> revoked",
        "active -> superseded",
        "active -> reconciled",
        "there is no terminal-to-terminal transition and no transition back to `active`.",
        "granted == remaining == old.remaining",
        "There is no refund-on-retry path.",
        "Only the matching human-confirmed apply clears pending.",
        "Raw HTTP/model values cannot supply balances.",
        "none is an authorization source, a revision owner, an FK parent, or a backfill source",
    ):
        assert marker in normalized
    assert document.count("PlanReviewAuthorityRepository") >= 3
    assert "legacy_json_authority = forbidden" in document


def test_combined_forward_and_rollback_dags_are_exact_and_dependency_safe() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    parts = _contract_parts(document)
    assert parts["forward"] == EXPECTED_FORWARD
    assert parts["rollback"] == EXPECTED_ROLLBACK

    created: set[str] = {
        "strict upstream parents",
        "plan_review_sessions",
        "workflow_events",
        "workflow_activity_attempts",
    }
    for _, operation, object_name, _ in EXPECTED_FORWARD:
        if operation == "create_table":
            created.add(object_name)
        if object_name.startswith("dispatch_exposures_"):
            assert "dispatch_exposures" in created
        if object_name.startswith("verification_intents_"):
            assert "verification_intents" in created
    assert FUTURE_TABLES.issubset(created)
    assert all(row[1].lower() != "cascade" for row in EXPECTED_ROLLBACK)

    drop_order = [row[2] for row in EXPECTED_ROLLBACK if row[1] == "drop_table"]
    assert drop_order.index("dispatch_exposures") < drop_order.index("identity_search_budget_grants")
    assert drop_order.index("identity_search_budget_consumptions") < drop_order.index("identity_search_budget_grants")
    assert drop_order.index("identity_search_budget_grants") < drop_order.index("verification_intents")
    assert drop_order[-1] == "plan_review_gate_authority_versions"


def test_all_new_identifiers_fit_postgres_without_truncation_collision() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    parts = _contract_parts(document)
    structural = parts["structural"]
    indexes = parts["indexes"]
    checks = parts["checks"]
    assert isinstance(structural, tuple)
    assert isinstance(indexes, tuple)
    assert isinstance(checks, tuple)
    identifiers = tuple(row[1] for row in (*structural, *indexes, *checks))
    assert len(identifiers) == 69
    assert len(set(identifiers)) == len(identifiers)
    assert all(len(identifier.encode("utf-8")) <= 63 for identifier in identifiers)
    projected = tuple(identifier.encode("utf-8")[:63] for identifier in identifiers)
    assert len(set(projected)) == len(projected)


def test_ten_invariant_matrix_is_exact_complete_and_nonimplementing() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    parts = _contract_parts(document)
    assert parts["matrix_header"] == EXPECTED_MATRIX_HEADER
    matrix = parts["matrix"]
    assert isinstance(matrix, tuple)
    assert tuple(row[0] for row in matrix) == EXPECTED_MATRIX_MECHANISMS
    assert len(matrix) == 3
    assert sum(len(row) - 1 for row in matrix) == 30
    assert all(len(row) == 11 and all(row[1:]) for row in matrix)
    assert _rows_digest(matrix) == EXPECTED_MATRIX_ROWS_SHA256
    assert "No cell claims implementation." in document


def test_oracle_is_mutation_sensitive_to_counts_parent_fks_lifecycle_and_matrix() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    _assert_exact_contract(document)
    mutations = (
        ("authority_column_count = 32", "authority_column_count = 31"),
        ("dispatch_exposures_plan_review_gate_authority_fk", "dispatch_exposures_plan_json_fk"),
        (
            "grant_state IN ('active', 'revoked', 'exhausted', 'superseded', 'reconciled')",
            "grant_state IN ('active', 'revoked')",
        ),
        ("human transition convergence |", "human transition retry |"),
        ("19 | `validate_acceptance`", "20 | `validate_acceptance`"),
    )
    for old, new in mutations:
        assert old in document
        with pytest.raises(AssertionError):
            _assert_exact_contract(document.replace(old, new, 1))


def test_current_physical_absence_and_legacy_json_non_authority_are_mechanical() -> None:
    baseline = BASELINE_PATH.read_text(encoding="utf-8")
    baseline_tables = frozenset(
        re.findall(
            r"(?im)^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[a-z_][a-z0-9_]*\.)?([a-z_][a-z0-9_]*)",
            baseline,
        )
    )
    all_migrations = "\n".join(path.read_text(encoding="utf-8") for path in sorted(MIGRATIONS_ROOT.glob("*.sql")))
    descriptors = _descriptor_tables()
    symbols = _defined_top_level_symbols()

    assert len(baseline_tables) == 83
    assert len(descriptors) == 41
    assert FUTURE_TABLES.isdisjoint(baseline_tables)
    assert FUTURE_TABLES.isdisjoint(descriptors)
    for table_name in FUTURE_TABLES:
        assert re.search(rf"(?i)\bCREATE\s+TABLE\s+{table_name}\b", all_migrations) is None
    assert FUTURE_SYMBOLS.isdisjoint(symbols)
    assert FUTURE_REPOSITORY_PATH.exists() is False
    assert "plan_review_authority" not in REPOSITORIES_INIT_PATH.read_text(encoding="utf-8")

    session_match = re.search(r"CREATE TABLE plan_review_sessions \((.*?)\n\);", baseline, re.DOTALL)
    assert session_match is not None
    session_ddl = session_match.group(1)
    legacy_json_columns = tuple(
        name
        for name in (
            "request_json",
            "plan_json",
            "gate_json",
            "execution_bundle_json",
            "matching_request_json",
            "decision_json",
        )
        if re.search(rf"(?m)^\s+{name}\s+text\b", session_ddl)
    )
    assert legacy_json_columns == (
        "request_json",
        "plan_json",
        "gate_json",
        "execution_bundle_json",
        "matching_request_json",
        "decision_json",
    )

    scoped_root = _section(
        SCOPED_ROOT_PATH.read_text(encoding="utf-8"),
        "ALTER TABLE plan_review_sessions",
        "ALTER TABLE operation_runs",
    )
    assert tuple(re.findall(r"ADD COLUMN ([a-z_]+)", scoped_root)) == (
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

    storage = STORAGE_PATH.read_text(encoding="utf-8")
    legacy_review = _section(storage, "    def review_plan_session(", "    def find_pending_plan_review_session(")
    assert '"update_row_returning"' in legacy_review
    assert '"decision_json": json.dumps' in legacy_review
    assert '"request_json": json.dumps' in legacy_review
    assert '"plan_json": json.dumps' in legacy_review
    assert "review_revision" not in legacy_review
    assert "gate_control_epoch" not in legacy_review
    assert "human_transition_pending" not in legacy_review


def test_trackers_record_decision_only_status_and_all_residuals_stay_open() -> None:
    tracker_requirements = {
        PLAN_PATH: (
            "D3c2i",
            "plan_review_gate_authority_versions",
            "identity_search_budget_grants",
            "decision_locked_not_implemented",
            "R-019",
        ),
        TODO_PATH: (
            "D3c2i",
            "identity_search_budget_consumptions",
            "OB-1.1",
            "OB-4.1",
            "OB-10.2",
        ),
        LEDGER_PATH: (
            "R-019 / D3c2i",
            "legacy JSON",
            "R-023",
            "R-027",
            "R-028",
            "R-029",
        ),
        INDEX_PATH: (
            "TRACK_D_D3C2I_TYPED_PLAN_REVIEW_GATE_AND_TIER2_GRANT_PARENT_DECISION_LOCK.md",
            "32/37/22",
            "零 migration",
        ),
    }
    for path, markers in tracker_requirements.items():
        content = path.read_text(encoding="utf-8")
        for marker in markers:
            assert marker in content, (path, marker)

    document = DECISION_PATH.read_text(encoding="utf-8")
    nonclosure = _section(document, "## 14. Explicit non-closure", "## 15. Author")
    for marker in (
        "R-019",
        "R-023",
        "R-027",
        "R-028",
        "R-029",
        "action-root durable-scope gate",
        "OB-10.1",
        "OB-2.2/OB-10.3",
        "OB-10.4",
        "Migration B–D",
        "provider/live validation",
        "served Agent tools",
    ):
        assert marker in nonclosure
    assert "author evidence only" in document
    assert "cannot be represented as a formal `GO`" in document
