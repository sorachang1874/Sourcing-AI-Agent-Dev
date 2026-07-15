from __future__ import annotations

import ast
import re
from pathlib import Path

from sourcing_agent.repositories.workflow_runtime import WORKFLOW_EVENTS

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"

DECISION_DOC_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2E_WORKFLOW_EVENT_TERMINAL_LINEAGE_DECISION_LOCK.md"
D3B_CONTRACT_PATH = REPO_ROOT / "docs" / "TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md"
D3C2C_DOC_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2C_ACTIVITY_TERMINAL_EVIDENCE_CHARACTERIZATION.md"
D3C2D_DOC_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2D_ACTIVITY_CLAIM_CHAIN_MIGRATION_IMPLEMENTATION.md"
PLAN_PATH = REPO_ROOT / "docs" / "TRACK_D_AGENT_RUNTIME_PLAN.md"
TODO_PATH = REPO_ROOT / "docs" / "NEXT_TODO.md"
LEDGER_PATH = REPO_ROOT / "docs" / "RESIDUAL_LEDGER.md"
INDEX_PATH = REPO_ROOT / "docs" / "INDEX.md"
CONTROL_PLANE_PATH = SOURCE_ROOT / "control_plane_live_postgres.py"
NEXT_MIGRATION_PATH = SOURCE_ROOT / "migrations" / "0006_d3_workflow_event_terminal_lineage_foundation.sql"

EXPECTED_CURRENT_EVENT_COLUMNS = (
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

EXPECTED_EVENT_CORE_COLUMNS = (
    "runtime_namespace",
    "provider_mode",
    "workspace_id",
    "scope_digest",
    "coordination_plan_review_id",
    "activity_run_id",
    "claim_generation",
    "control_epoch",
    "claim_authority_spec_digest",
    "d3_business_fence_digest",
    "terminal_outcome_digest",
)

EXPECTED_COLUMN_ROWS = (
    ("1", "`runtime_namespace`", "`text`", "no", "`''::text`", "`''`"),
    ("2", "`provider_mode`", "`text`", "no", "`''::text`", "`''`"),
    ("3", "`workspace_id`", "`text`", "no", "`''::text`", "`''`"),
    ("4", "`scope_digest`", "`text`", "no", "`''::text`", "`''`"),
    ("5", "`coordination_plan_review_id`", "`bigint`", "yes", "none", "SQL `NULL`"),
    ("6", "`activity_run_id`", "`text`", "no", "`''::text`", "`''`"),
    ("7", "`claim_generation`", "`bigint`", "no", "`0`", "`0`"),
    ("8", "`control_epoch`", "`bigint`", "no", "`0`", "`0`"),
    ("9", "`claim_authority_spec_digest`", "`text`", "no", "`''::text`", "`''`"),
    ("10", "`d3_business_fence_digest`", "`text`", "no", "`''::text`", "`''`"),
    ("11", "`terminal_outcome_digest`", "`text`", "yes", "none", "SQL `NULL`"),
)

EXPECTED_CHECK_ROWS = (
    (
        "1",
        "`workflow_events_runtime_namespace_shape_ck`",
        "`runtime_namespace = '' OR runtime_namespace ~ '[^[:space:]]'`",
    ),
    (
        "2",
        "`workflow_events_provider_mode_shape_ck`",
        "`provider_mode = '' OR provider_mode IN ('live', 'simulate', 'scripted', 'replay')`",
    ),
    (
        "3",
        "`workflow_events_workspace_id_shape_ck`",
        "`workspace_id = '' OR workspace_id ~ '[^[:space:]]'`",
    ),
    (
        "4",
        "`workflow_events_scope_digest_shape_ck`",
        "`scope_digest = '' OR scope_digest ~ '^[0-9a-f]{64}$'`",
    ),
    (
        "5",
        "`workflow_events_coordination_plan_review_id_shape_ck`",
        "`coordination_plan_review_id IS NULL OR coordination_plan_review_id > 0`",
    ),
    (
        "6",
        "`workflow_events_activity_run_id_shape_ck`",
        "`activity_run_id = '' OR activity_run_id ~ '[^[:space:]]'`",
    ),
    ("7", "`workflow_events_claim_generation_nonnegative_ck`", "`claim_generation >= 0`"),
    ("8", "`workflow_events_control_epoch_nonnegative_ck`", "`control_epoch >= 0`"),
    (
        "9",
        "`workflow_events_claim_authority_spec_digest_shape_ck`",
        "`claim_authority_spec_digest = '' OR claim_authority_spec_digest ~ '^[0-9a-f]{64}$'`",
    ),
    (
        "10",
        "`workflow_events_d3_business_fence_digest_shape_ck`",
        "`d3_business_fence_digest = '' OR d3_business_fence_digest ~ '^[0-9a-f]{64}$'`",
    ),
    (
        "11",
        "`workflow_events_terminal_outcome_digest_shape_ck`",
        "`terminal_outcome_digest IS NULL OR terminal_outcome_digest ~ '^[0-9a-f]{64}$'`",
    ),
)

EXPECTED_FORBIDDEN_EVENT_ALIASES = (
    "operation_run_id",
    "source_verification_command_id",
    "source_activity_attempt_id",
    "source_command_attempt",
)

EXPECTED_DEFERRAL_IDS = tuple(f"D3c2e-D{index}" for index in range(1, 11))
EXPECTED_MATRIX_MECHANISMS = (
    "11-column event core",
    "local check grammar",
    "logical/physical mapping",
    "deferred evidence families",
    "D3c2f dormant substrate",
)


def _normalized(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _section(document: str, start: str, end: str) -> str:
    return document[document.index(start) : document.index(end)]


def _markdown_table(section: str) -> tuple[tuple[str, ...], tuple[tuple[str, ...], ...]]:
    rows: list[tuple[str, ...]] = []
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cells = tuple(cell.strip() for cell in line.strip().strip("|").split("|"))
        if all(re.fullmatch(r":?-+:?", cell) for cell in cells):
            continue
        rows.append(cells)
    assert rows
    return rows[0], tuple(rows[1:])


def _class_method_source(path: Path, class_name: str, method_name: str) -> str:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name]
    assert len(classes) == 1
    methods = [node for node in classes[0].body if isinstance(node, ast.FunctionDef) and node.name == method_name]
    assert len(methods) == 1
    segment = ast.get_source_segment(source, methods[0])
    assert segment is not None
    return segment


def test_current_event_descriptor_and_writer_remain_dormant() -> None:
    assert tuple(WORKFLOW_EVENTS.column_names()) == EXPECTED_CURRENT_EVENT_COLUMNS
    assert set(EXPECTED_EVENT_CORE_COLUMNS).isdisjoint(WORKFLOW_EVENTS.column_names())
    assert NEXT_MIGRATION_PATH.exists() is False

    writer = _class_method_source(
        CONTROL_PLANE_PATH,
        "LiveControlPlanePostgresAdapter",
        "append_workflow_event",
    )
    for column_name in EXPECTED_EVENT_CORE_COLUMNS:
        assert f'payload.get("{column_name}")' not in writer
        assert f'"{column_name}":' not in writer


def test_decision_lock_pins_exact_ordered_columns_and_legacy_tuple() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 3. Exact additive event-core columns", "## 4. Exact local checks")
    header, rows = _markdown_table(section)

    assert header == ("position", "column", "SQL type", "nullable", "default", "brownfield sentinel")
    assert rows == EXPECTED_COLUMN_ROWS
    assert tuple(row[1].strip("`") for row in rows) == EXPECTED_EVENT_CORE_COLUMNS
    assert "('', '', '', '', NULL, '', 0, 0, '', '', NULL)" in section
    assert "no text or empty-string encoding is permitted" in _normalized(section)


def test_decision_lock_pins_exact_not_valid_local_checks() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 4. Exact local checks", "## 5. Logical-to-physical identity mapping")
    header, rows = _markdown_table(section)

    assert header == ("position", "constraint name", "local predicate")
    assert rows == EXPECTED_CHECK_ROWS
    assert "all `NOT VALID`" in section
    assert "do not assert parent existence" in _normalized(section)
    assert "must not validate them in the installation transaction" in _normalized(section)


def test_logical_identity_mapping_reuses_existing_links_and_forbids_aliases() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 5. Logical-to-physical identity mapping", "## 6. Deferred physical decisions")
    header, rows = _markdown_table(section)

    assert header == ("logical identity", "physical owner")
    mapping = dict(rows)
    assert mapping["operation run id"] == "existing `workflow_events.operation_id`"
    assert mapping["source / terminal command id"] == "existing `workflow_events.command_id`"
    assert mapping["source ActivityAttempt id"] == "existing `workflow_events.activity_attempt_id`"
    assert mapping["source ActivityRun id"] == "new `workflow_events.activity_run_id`"
    assert "workflow_activity_attempts.command_attempt" in mapping["source command attempt"]
    assert (
        "verification intent separately exact-copies it as `source_command_attempt`"
        in mapping["source command attempt"]
    )
    for alias in EXPECTED_FORBIDDEN_EVENT_ALIASES:
        assert f"`{alias}`" in section
    assert "JSON payload is never a fallback" in _normalized(section)

    contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    assert "the first dormant event-core fragment adds exactly" in contract
    assert "The event core does not duplicate `operation_run_id`" in contract
    assert "The seven-field immutable source core belongs to verification intent" in _normalized(contract)
    migration_a = _section(contract, "### 11.1 Migration A", "### 11.2 Migration B")
    assert "first adds the exact eleven-column terminal-lineage core" in migration_a
    assert "A later separately ratified Migration-A fragment" in migration_a


def test_deferral_inventory_is_complete_and_does_not_guess_exposure_or_receipts() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 6. Deferred physical decisions", "## 7. Mechanism × ten-invariant matrix")
    observed_ids = tuple(re.findall(r"\*\*(D3c2e-D\d+)\s+—", section))

    assert observed_ids == EXPECTED_DEFERRAL_IDS
    for required_text in (
        "event transport provenance",
        "receipt tables",
        "dispatch exposure",
        "unratified / undetermined",
        "late quarantine",
        "verification intent",
        "indexes",
        "foreign keys",
        "population checks",
        "adoption",
        "runtime atomicity",
        "OB-10.1 remains open",
        "R-019/runtime work",
    ):
        assert required_text in section


def test_invariant_matrix_covers_every_mechanism_and_all_ten_dimensions() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 7. Mechanism × ten-invariant matrix", "## 8. Executable oracle")
    header, rows = _markdown_table(section)

    assert header == (
        "mechanism",
        "1 owner",
        "2 tenant",
        "3 fence",
        "4 lifecycle",
        "5 late/partial",
        "6 cost",
        "7 physical identity",
        "8 provenance",
        "9 consistency",
        "10 mode isolation",
    )
    assert tuple(row[0] for row in rows) == EXPECTED_MATRIX_MECHANISMS
    assert all(len(row) == 11 and all(cell for cell in row) for row in rows)
    assert "TBD" not in section
    assert "No new OB-ID is created" in section


def test_trackers_preserve_exact_nonclosure_and_bound_d3c2f_to_dormant_ddl() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = document[document.index("## 9. Explicit non-closure and next batch") :]

    for required_text in (
        "`R-019`: terminal command/attempt/event/source-intent atomicity remains open;",
        "`R-023`: durable-runtime cutover and residual tripwire remain open;",
        "`R-027`: a scope-matched formal review remains open;",
        "`R-029`: action request-schema compatibility/adoption remains open;",
        "action-root durable-scope gate and `OB-10.1/10.2/10.3/10.4` remain open;",
        "served Agent tool population remain closed to activation",
        "D3c2f — dormant WorkflowEvent terminal-lineage foundation",
        "11 columns and 11 `NOT VALID` checks",
        "The descriptor must remain 17 columns",
        "may not add indexes, FKs, validation, backfill",
    ):
        assert required_text in _normalized(section)

    tracker_requirements = {
        PLAN_PATH: ("D3c2e", "11-column", "D3c2f"),
        TODO_PATH: ("D3c2e", "11 columns", "D3c2f"),
        LEDGER_PATH: ("D3c2e", "D3c2f", "R-019"),
        INDEX_PATH: ("TRACK_D_D3C2E_WORKFLOW_EVENT_TERMINAL_LINEAGE_DECISION_LOCK.md", "D3c2f"),
        D3C2C_DOC_PATH: ("D3c2e", "D3c2f"),
        D3C2D_DOC_PATH: ("D3c2e", "D3c2f"),
    }
    for path, markers in tracker_requirements.items():
        source = path.read_text(encoding="utf-8")
        for marker in markers:
            assert marker in source, f"{path.name} missing {marker}"
