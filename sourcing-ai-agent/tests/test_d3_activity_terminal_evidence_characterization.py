from __future__ import annotations

import ast
import re
from collections import Counter
from functools import lru_cache
from pathlib import Path

from sourcing_agent.repositories.workflow_runtime import (
    WORKFLOW_ACTIVITY_ATTEMPTS,
    WORKFLOW_ACTIVITY_RUNS,
    WORKFLOW_EVENTS,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
MIGRATIONS_ROOT = SOURCE_ROOT / "migrations"

CONTROL_PLANE_PATH = SOURCE_ROOT / "control_plane_live_postgres.py"
DURABLE_RUNTIME_PATH = SOURCE_ROOT / "durable_runtime.py"
WORKFLOW_REPOSITORY_PATH = SOURCE_ROOT / "repositories" / "workflow_runtime.py"
CHARACTERIZATION_DOC_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2C_ACTIVITY_TERMINAL_EVIDENCE_CHARACTERIZATION.md"
D3B_CONTRACT_PATH = REPO_ROOT / "docs" / "TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md"

EXPECTED_ACTIVITY_RUN_COLUMNS = (
    "activity_run_id",
    "workspace_id",
    "workflow_run_id",
    "operation_run_id",
    "acquisition_run_id",
    "command_id",
    "parent_activity_run_id",
    "activity_type",
    "owner",
    "status",
    "phase",
    "idempotency_key",
    "provider_ref_json",
    "input_json",
    "output_json",
    "artifact_refs_json",
    "entity_counts_json",
    "metadata_json",
    "created_at",
    "updated_at",
)
EXPECTED_ACTIVITY_ATTEMPT_COLUMNS = (
    "attempt_id",
    "workspace_id",
    "activity_run_id",
    "workflow_run_id",
    "command_id",
    "attempt_number",
    "status",
    "provider",
    "provider_request_ref",
    "provider_run_ref",
    "started_at",
    "completed_at",
    "next_retry_at",
    "rate_limit_ref_json",
    "error_json",
    "input_json",
    "output_json",
    "artifact_refs_json",
    "idempotency_key",
    "metadata_json",
    "created_at",
    "updated_at",
)
EXPECTED_EVENT_COLUMNS = (
    "event_id",
    "workflow_run_id",
    "operation_id",
    "command_id",
    "activity_attempt_id",
    "event_family",
    "event_type",
    "sequence_number",
    "idempotency_key",
    "occurred_at",
    "recorded_at",
    "actor",
    "source",
    "payload_json",
    "artifact_refs_json",
    "schema_version",
    "created_at",
)

EXPECTED_CALL_POPULATIONS = {
    "upsert_activity_run": Counter(
        {
            "acquisition_command_owner.py": 1,
            "command_kernel.py": 2,
            "enrichment.py": 4,
            "orchestrator.py": 14,
            "profile_fetch_owner.py": 9,
        }
    ),
    "list_activity_runs": Counter(
        {
            "crm_public_web_owner.py": 2,
            "excel_intake_owner.py": 2,
            "orchestrator.py": 16,
        }
    ),
    "get_activity_run": Counter(
        {
            "orchestrator.py": 18,
            "profile_fetch_owner.py": 7,
            "repositories/workflow_runtime.py": 1,
        }
    ),
    "upsert_activity_attempt": Counter(
        {
            "command_kernel.py": 2,
            "enrichment.py": 4,
            "orchestrator.py": 9,
            "profile_fetch_owner.py": 7,
        }
    ),
    "list_activity_attempts": Counter(
        {
            "crm_public_web_owner.py": 2,
            "excel_intake_owner.py": 2,
            "orchestrator.py": 17,
        }
    ),
    "get_activity_attempt": Counter(
        {
            "orchestrator.py": 1,
            "repositories/workflow_runtime.py": 1,
        }
    ),
    "append_event_and_reduce": Counter(
        {
            "acquisition_command_owner.py": 5,
            "crm_public_web_owner.py": 9,
            "enrichment.py": 4,
            "excel_intake_owner.py": 2,
            "orchestrator.py": 35,
            "profile_fetch_owner.py": 4,
            "seed_discovery.py": 3,
        }
    ),
    "list_workflow_events": Counter(
        {
            "durable_runtime.py": 1,
            "orchestrator.py": 1,
        }
    ),
}

FUTURE_PHYSICAL_TABLES = frozenset(
    {
        "verification_intent",
        "transport_response_receipts",
        "transport_attempt_failure_receipts",
        "workflow_late_result_quarantine",
    }
)
FUTURE_TERMINAL_REGISTRY_SYMBOLS = frozenset(
    {
        "TERMINAL_PROVENANCE_SPECS",
        "TerminalProvenanceSpec",
        "TransportResponseSpec",
        "TransportAttemptFailureSpec",
        "NoExposureTerminalSpec",
    }
)


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


def _numbered_markdown_items(section: str) -> tuple[tuple[int, str], ...]:
    starts = list(re.finditer(r"(?m)^(\d+)\. ", section))
    return tuple(
        (
            int(match.group(1)),
            _normalized_source(
                section[match.end() : starts[index + 1].start() if index + 1 < len(starts) else len(section)]
            ),
        )
        for index, match in enumerate(starts)
    )


def _markdown_bullet_items(section: str) -> tuple[str, ...]:
    items: list[str] = []
    current: list[str] = []
    for line in section.splitlines():
        if line.startswith("- "):
            if current:
                items.append(_normalized_source(" ".join(current)))
            current = [line[2:]]
        elif current and line.startswith("  "):
            current.append(line.strip())
        elif current:
            items.append(_normalized_source(" ".join(current)))
            current = []
    if current:
        items.append(_normalized_source(" ".join(current)))
    return tuple(items)


def _terminal_call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    if isinstance(node.func, ast.Name):
        return node.func.id
    return ""


@lru_cache(maxsize=1)
def _source_units() -> tuple[tuple[Path, str, ast.Module], ...]:
    return tuple(
        (path, source, ast.parse(source))
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        if (source := path.read_text(encoding="utf-8"))
    )


@lru_cache(maxsize=1)
def _production_call_populations() -> dict[str, Counter[str]]:
    populations = {call_name: Counter() for call_name in EXPECTED_CALL_POPULATIONS}
    for path, _, tree in _source_units():
        relative_path = path.relative_to(SOURCE_ROOT).as_posix()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            call_name = _terminal_call_name(node)
            if call_name in populations:
                populations[call_name][relative_path] += 1
    return populations


def _production_call_population(call_name: str) -> Counter[str]:
    return Counter(_production_call_populations()[call_name])


def _method_call_lines(method: ast.FunctionDef, call_names: frozenset[str]) -> dict[str, tuple[int, ...]]:
    observed: dict[str, list[int]] = {name: [] for name in call_names}
    for node in ast.walk(method):
        if not isinstance(node, ast.Call):
            continue
        call_name = _terminal_call_name(node)
        if call_name in observed:
            observed[call_name].append(node.lineno)
    return {name: tuple(sorted(lines)) for name, lines in observed.items()}


def _physical_event_insert_owners() -> tuple[tuple[str, str, str], ...]:
    owners: list[tuple[str, str, str]] = []
    for path, _, tree in _source_units():
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        for literal in (
            node for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)
        ):
            if "insert into workflow_events" not in _normalized_source(literal.value).lower():
                continue
            current: ast.AST = literal
            method_name = "<module>"
            class_name = "<module>"
            while current in parents:
                current = parents[current]
                if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)) and method_name == "<module>":
                    method_name = current.name
                if isinstance(current, ast.ClassDef):
                    class_name = current.name
                    break
            owners.append((path.relative_to(SOURCE_ROOT).as_posix(), class_name, method_name))
    return tuple(owners)


def _direct_activity_run_update_owners() -> tuple[tuple[str, str, str], ...]:
    owners: list[tuple[str, str, str]] = []
    for path, _, tree in _source_units():
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        for literal in (
            node for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)
        ):
            if not re.search(r"\bupdate\s+workflow_activity_runs\b", _normalized_source(literal.value).lower()):
                continue
            current: ast.AST = literal
            method_name = "<module>"
            class_name = "<module>"
            while current in parents:
                current = parents[current]
                if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)) and method_name == "<module>":
                    method_name = current.name
                if isinstance(current, ast.ClassDef):
                    class_name = current.name
                    break
            owners.append((path.relative_to(SOURCE_ROOT).as_posix(), class_name, method_name))
    return tuple(owners)


def _defined_table_descriptors() -> frozenset[str]:
    tables: set[str] = set()
    for _, _, tree in _source_units():
        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
            if _terminal_call_name(call) != "TableDescriptor":
                continue
            table_keyword = next((keyword for keyword in call.keywords if keyword.arg == "table"), None)
            if table_keyword and isinstance(table_keyword.value, ast.Constant):
                assert isinstance(table_keyword.value.value, str)
                tables.add(table_keyword.value.value)
    return frozenset(tables)


def _physical_sql_table_names() -> frozenset[str]:
    table_pattern = re.compile(
        r"\b(?:create\s+table(?:\s+if\s+not\s+exists)?|alter\s+table|insert\s+into|update|delete\s+from)\s+"
        r"([a-z_][a-z0-9_]*)\b"
    )
    surfaces: list[str] = []
    for path in sorted(MIGRATIONS_ROOT.glob("*.sql")):
        surfaces.append(path.read_text(encoding="utf-8"))
    for _, _, tree in _source_units():
        surfaces.extend(
            node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)
        )
    return frozenset(
        match.group(1) for surface in surfaces for match in table_pattern.finditer(_normalized_source(surface).lower())
    )


def _defined_python_symbols() -> frozenset[str]:
    symbols: set[str] = set()
    for _, _, tree in _source_units():
        for node in ast.walk(tree):
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                symbols.add(node.name)
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                symbols.update(target.id for target in targets if isinstance(target, ast.Name))
    return frozenset(symbols)


def test_current_activity_and_event_descriptors_are_mechanically_frozen() -> None:
    assert tuple(WORKFLOW_ACTIVITY_RUNS.column_names()) == EXPECTED_ACTIVITY_RUN_COLUMNS
    assert tuple(WORKFLOW_ACTIVITY_ATTEMPTS.column_names()) == EXPECTED_ACTIVITY_ATTEMPT_COLUMNS
    assert tuple(WORKFLOW_EVENTS.column_names()) == EXPECTED_EVENT_COLUMNS
    assert (
        len(EXPECTED_ACTIVITY_RUN_COLUMNS),
        len(EXPECTED_ACTIVITY_ATTEMPT_COLUMNS),
        len(EXPECTED_EVENT_COLUMNS),
    ) == (
        20,
        22,
        17,
    )


def test_current_activity_and_event_call_populations_are_mechanically_frozen() -> None:
    observed = {call_name: _production_call_population(call_name) for call_name in EXPECTED_CALL_POPULATIONS}
    assert observed == EXPECTED_CALL_POPULATIONS

    assert sum(observed["upsert_activity_run"].values()) == 30
    assert len(observed["upsert_activity_run"]) == 5
    assert sum(observed["list_activity_runs"].values()) == 20
    assert sum(observed["get_activity_run"].values()) == 26

    assert sum(observed["upsert_activity_attempt"].values()) == 22
    assert len(observed["upsert_activity_attempt"]) == 4
    assert sum(observed["list_activity_attempts"].values()) == 21
    assert observed["get_activity_attempt"] - Counter({"repositories/workflow_runtime.py": 1}) == Counter(
        {"orchestrator.py": 1}
    )

    assert sum(observed["append_event_and_reduce"].values()) == 62
    assert len(observed["append_event_and_reduce"]) == 7
    assert sum(observed["list_workflow_events"].values()) == 2


def test_attempt_number_is_existing_activity_retry_accounting_not_future_command_attempt() -> None:
    method, _ = _class_method(
        WORKFLOW_REPOSITORY_PATH,
        "WorkflowRuntimeRepository",
        "upsert_activity_attempt",
    )
    string_literals = {
        node.value for node in ast.walk(method) if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    assert {"attempt_number", "attempt"} <= string_literals
    assert "command_attempt" not in string_literals
    assert "attempt_number" in WORKFLOW_ACTIVITY_ATTEMPTS.column_names()
    assert "command_attempt" not in WORKFLOW_ACTIVITY_ATTEMPTS.column_names()


def test_current_physical_event_and_direct_cancel_owners_are_unique_and_explicit() -> None:
    assert _physical_event_insert_owners() == (
        (
            "control_plane_live_postgres.py",
            "LiveControlPlanePostgresAdapter",
            "append_workflow_event",
        ),
    )
    assert _direct_activity_run_update_owners() == (
        (
            "control_plane_live_postgres.py",
            "LiveControlPlanePostgresAdapter",
            "cancel_acquisition_owner_command",
        ),
    )


def test_current_event_reduce_path_is_split_across_multiple_mutation_calls() -> None:
    append_method, _ = _class_method(
        DURABLE_RUNTIME_PATH,
        "DurableRuntimeWriter",
        "append_event_and_reduce",
    )
    append_lines = _method_call_lines(
        append_method,
        frozenset({"append_workflow_event", "reduce_and_persist"}),
    )
    assert len(append_lines["append_workflow_event"]) == 1
    assert len(append_lines["reduce_and_persist"]) == 1
    assert append_lines["append_workflow_event"][0] < append_lines["reduce_and_persist"][0]
    assert not any(isinstance(node, (ast.With, ast.AsyncWith)) for node in ast.walk(append_method))

    reduce_method, _ = _class_method(
        DURABLE_RUNTIME_PATH,
        "DurableRuntimeWriter",
        "reduce_and_persist",
    )
    mutation_lines = _method_call_lines(
        reduce_method,
        frozenset(
            {
                "upsert_workflow_command",
                "enqueue_runtime_outbox",
                "upsert_workflow_current_state",
            }
        ),
    )
    assert mutation_lines["upsert_workflow_command"]
    assert mutation_lines["enqueue_runtime_outbox"]
    assert mutation_lines["upsert_workflow_current_state"]
    assert min(mutation_lines["upsert_workflow_command"]) < min(mutation_lines["enqueue_runtime_outbox"])
    assert min(mutation_lines["enqueue_runtime_outbox"]) < max(mutation_lines["upsert_workflow_current_state"])
    assert not any(isinstance(node, (ast.With, ast.AsyncWith)) for node in ast.walk(reduce_method))


def test_ratified_future_tables_and_registry_symbols_are_absent_from_named_schema_surfaces() -> None:
    physical_tables = _defined_table_descriptors() | _physical_sql_table_names()
    assert FUTURE_PHYSICAL_TABLES.isdisjoint(physical_tables)

    symbols = _defined_python_symbols()
    assert FUTURE_TERMINAL_REGISTRY_SYMBOLS.isdisjoint(symbols)


def test_d3b_migration_a_orders_activity_before_event_before_receipt_surfaces() -> None:
    contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    migration_a = contract[contract.index("### 11.1 Migration A") : contract.index("### 11.2 Migration B")]
    items = _numbered_markdown_items(migration_a)
    item_numbers = tuple(item_number for item_number, _ in items)
    assert item_numbers == tuple(range(1, 11))
    assert len(item_numbers) == len(set(item_numbers))
    items_by_number = dict(items)

    activity_item = items_by_number[5]
    activity_run_position = activity_item.index("`workflow_activity_runs`")
    activity_attempt_position = activity_item.index("`workflow_activity_attempts`")
    assert activity_run_position < activity_attempt_position

    event_item = items_by_number[6]
    event_position = event_item.index("reuses `workflow_events.operation_id`")
    intent_position = event_item.index("verification-intent source terminal tuple")
    assert event_position < intent_position

    terminal_evidence_item = items_by_number[7]
    response_position = terminal_evidence_item.index("creates `transport_response_receipts`")
    failure_position = terminal_evidence_item.index("`transport_attempt_failure_receipts`")
    quarantine_position = terminal_evidence_item.index("creates `workflow_late_result_quarantine`")
    assert response_position < failure_position < quarantine_position

    ordered_markers = (
        "`workflow_activity_runs`",
        "`workflow_activity_attempts`",
        "reuses `workflow_events.operation_id`",
        "verification-intent source terminal tuple",
        "creates `transport_response_receipts`",
        "`transport_attempt_failure_receipts`",
        "creates `workflow_late_result_quarantine`",
    )
    observed_markers = tuple(marker for _, item in items for marker in ordered_markers if marker in item)
    observed_phase_numbers = tuple(
        item_number for item_number, item in items for marker in ordered_markers if marker in item
    )
    assert observed_markers == ordered_markers
    assert observed_phase_numbers == (5, 5, 6, 6, 7, 7, 7)


def test_characterization_document_preserves_owner_ratification_and_open_gates() -> None:
    document = CHARACTERIZATION_DOC_PATH.read_text(encoding="utf-8")
    for required_text in (
        "characterization-only",
        "30",
        "22",
        "62",
        "owner-ratified DDL",
        "ActivityRun + ActivityAttempt",
        "workflow event",
        "verification intent + response/failure receipts + late quarantine",
        "unratified / undetermined",
    ):
        assert required_text in document

    non_closure = document[document.index("## 7. Explicit non-closure") : document.index("## 8. Author validation")]
    assert "D3c2c changes no runtime behavior and closes none of these gates:" in non_closure
    assert _markdown_bullet_items(non_closure) == (
        "`R-019`: atomic owner effect / command-attempt-event terminal UoW remains open;",
        "`R-023`: durable-runtime cutover and residual tripwire remains open;",
        "`R-027`: scope-matched formal review status remains open where not already covered by a valid artifact;",
        "`R-029`: action request-schema compatibility/adoption remains open;",
        "action-root durable-scope gate and `OB-10.1/10.2/10.3/10.4` remain open;",
        "Migration A remainder, Migration B-D, registries/manifests/factory, Stage A/B, dispatch, provider/model, "
        "live/W6, promotion/signoff, and served Agent tool population remain closed to activation.",
    )
    assert (
        "This characterization is author evidence only. It is not a formal independent-review `GO` and does not "
        "broaden the scope-local D3c2b review artifact." in _normalized_source(non_closure)
    )
