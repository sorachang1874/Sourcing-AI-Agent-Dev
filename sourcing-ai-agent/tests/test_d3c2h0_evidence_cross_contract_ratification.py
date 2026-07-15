from __future__ import annotations

import ast
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
MIGRATIONS_ROOT = SOURCE_ROOT / "migrations"

DECISION_DOC_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2H0_EVIDENCE_CROSS_CONTRACT_RATIFICATION.md"
D0_CONTRACT_PATH = REPO_ROOT / "docs" / "TRACK_D_D0C_MODEL_INVOCATION_CONTRACT.md"
D3B_CONTRACT_PATH = REPO_ROOT / "docs" / "TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md"
D3C2G_DECISION_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2G_COST_LEDGER_DISPATCH_EXPOSURE_DECISION_LOCK.md"
PLAN_PATH = REPO_ROOT / "docs" / "TRACK_D_AGENT_RUNTIME_PLAN.md"
CHECKLIST_PATH = REPO_ROOT / "docs" / "DESIGN_INVARIANT_CHECKLIST.md"
INDEX_PATH = REPO_ROOT / "docs" / "INDEX.md"
NEXT_TODO_PATH = REPO_ROOT / "docs" / "NEXT_TODO.md"
RESIDUAL_LEDGER_PATH = REPO_ROOT / "docs" / "RESIDUAL_LEDGER.md"
MODEL_RUNTIME_PATH = SOURCE_ROOT / "model_tool_runtime.py"
CONTROL_PLANE_REPOSITORY_PATH = SOURCE_ROOT / "control_plane_repository.py"

EXPECTED_DECLARATIONS = (
    "CROSS_CONTRACT_RATIFICATION_V1",
    "scope_prefix = runtime_namespace | provider_mode | workspace_id | scope_digest | coordination_plan_review_id",
    "receipt_attempt_source = dispatch_exposures.command_attempt",
    "receipt_attempt_equality = workflow_activity_attempts.command_attempt | ClaimReceipt.identity.attempt | ClaimIdentity.attempt | workflow_commands.attempt",
    "quarantine_forbidden_identity = workflow_run_id",
    "quarantine_cost_transition = pending_reconciliation -> reconciled_confirmed | reconciled_uncertain",
    "quarantine_forbidden_cost_state = reconciled_no_call",
    "durable_envelope_prerequisite = D0f",
    "canonical_envelope_schema_owner = ModelInvocationEnvelopeV1",
    "model_transport_kind = model_tool_v1",
    "eligible_provider_modes = live | simulate | scripted",
    "replay_behavior = fail_closed_zero_write",
    "provider_search_behavior = owner_ratified_transport_variant_required",
    "quarantine_classifier_authority = stored_current_state_under_d3_dispatch_v2_global_lock_prefix",
    "response_classification_order = d3_dispatch_v2 -> operation_root -> optional_plan_review_gate -> participating_commands_sorted -> intent_predecessor -> activity_run_attempt -> dispatch_exposure_lock -> transport_response_receipt -> optional_response_quarantine -> dispatch_exposure_terminalization",
    "exposure_first_evidence_order = dispatch_exposure_lock -> transport_evidence_receipt -> dispatch_exposure_terminalization",
    "exposure_first_quarantine_permission = forbidden",
    "classification_retry_contract = deferred_to_D3c2h1",
    "schema_manifest_status = deferred_to_D3c2h1",
)

EXPECTED_SCOPE_PREFIX = (
    "runtime_namespace",
    "provider_mode",
    "workspace_id",
    "scope_digest",
    "coordination_plan_review_id",
)

EXPECTED_SUPERSESSION_IDS = tuple(f"H0-{index}" for index in range(1, 14))

EXPECTED_OWNER_OBJECTS = (
    "canonical envelope shape and digest",
    "durable envelope record and owner-issued ref",
    "dispatch exposure",
    "response and attempt-failure receipts",
    "late response quarantine",
    "verification intent",
    "post-network transaction coordinator",
)

EXPECTED_MATRIX_MECHANISMS = (
    "full-PFX evidence lineage",
    "receipt attempt identity",
    "D0f durable envelope ref",
    "post-network composition",
    "response classification authority",
    "quarantine axes",
    "model transport and modes",
    "storage prerequisites",
)

FUTURE_PHYSICAL_TOKENS = (
    "transport_response_receipts",
    "transport_attempt_failure_receipts",
    "workflow_late_result_quarantine",
    "verification_intent",
)


def _normalized(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _section(document: str, start: str, end: str | None = None) -> str:
    start_index = document.index(start)
    if end is None:
        return document[start_index:]
    return document[start_index : document.index(end, start_index)]


def _fenced_text_after(document: str, marker: str) -> tuple[str, ...]:
    marker_index = document.index(marker)
    fence_start = document.index("```text", marker_index) + len("```text")
    fence_end = document.index("```", fence_start)
    return tuple(line.strip() for line in document[fence_start:fence_end].splitlines() if line.strip())


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


def test_canonical_cross_contract_declarations_are_exact() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    declarations = _fenced_text_after(document, "machine-readable cross-contract authority")

    assert declarations == EXPECTED_DECLARATIONS
    assert tuple(declarations[1].split(" = ", 1)[1].split(" | ")) == EXPECTED_SCOPE_PREFIX
    assert declarations[-1] == "schema_manifest_status = deferred_to_D3c2h1"
    assert "No declaration above claims that the future rows or owners are implemented." in document


def test_supersession_ledger_closes_all_scout_conflicts_without_stale_positive_forms() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 2. Supersession ledger", "## 3. Owner boundaries")
    header, rows = _markdown_table(section)

    assert header == ("ID", "superseded sketch", "ratified replacement", "reason")
    assert tuple(row[0] for row in rows) == EXPECTED_SUPERSESSION_IDS
    assert all(len(row) == 4 and all(cell for cell in row) for row in rows)
    for marker in (
        "full five-field PFX",
        "post-claim `command_attempt`",
        "`workflow_run_id` is forbidden",
        "`reconciled_no_call`",
        "D0f",
        "response-classification UoW",
        "`model_tool_v1` only",
        "simulate/scripted",
        "replay is zero-write fail-closed",
        "Decimal/TIMESTAMPTZ",
        "company-identity verification owner",
        "D3c2h1 work",
        "exposure-first UoW has zero quarantine permission",
    ):
        assert marker in section

    assert "pending_reconciliation -> reconciled_confirmed | reconciled_uncertain | reconciled_no_call" not in document
    assert "operation_run_id, workflow_run_id, command_id" not in document
    assert "transport_response_receipts(scope_digest," not in document
    assert "late-response-v1:<scope_digest>" not in document


def test_owner_matrix_keeps_sql_writers_and_composition_separate() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 3. Owner boundaries", "## 4. Full PFX")
    header, rows = _markdown_table(section)

    assert header == (
        "object / composition role",
        "single owner",
        "physical source of truth",
        "allowed use",
        "forbidden use",
        "next bounded owner work",
    )
    assert tuple(row[0] for row in rows) == EXPECTED_OWNER_OBJECTS
    assert all(len(row) == 6 and all(cell for cell in row) for row in rows)

    matrix = {row[0]: row for row in rows}
    assert "ModelInvocationEnvelopeV1" in matrix["canonical envelope shape and digest"][1]
    assert "future D0f" in matrix["durable envelope record and owner-issued ref"][1]
    assert "CostLedgerRepository" in matrix["dispatch exposure"][1]
    assert "transport-evidence repository" in matrix["response and attempt-failure receipts"][1]
    assert "shared quarantine repository" in matrix["late response quarantine"][1]
    assert "company-identity verification owner" in matrix["verification intent"][1]
    assert (
        "owns order and authoritative response classification, not another table"
        in matrix["post-network transaction coordinator"][1]
    )
    assert "direct cross-owner SQL" in matrix["post-network transaction coordinator"][4]
    assert "does not redefine it" in matrix["canonical envelope shape and digest"][5]
    assert "without becoming a second SQL writer" in _normalized(section)


def test_full_pfx_and_receipt_attempt_chain_supersede_d3b_scope_only_sketches() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 4. Full PFX", "## 5. Quarantine reachability")
    pfx = _fenced_text_after(section, "PFX` means exactly")
    attempt_chain = _fenced_text_after(section, "The equality chain is")

    assert pfx == ("(runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id)",)
    assert attempt_chain == (
        "transport receipt command_attempt",
        "= dispatch_exposures.command_attempt",
        "= workflow_activity_attempts.command_attempt",
        "= ClaimReceipt.identity.attempt",
        "= ClaimIdentity.attempt",
        "= workflow_commands.attempt at the winning Stage-A claim",
    )
    normalized = _normalized(section)
    for marker in (
        "primary/unique/FK relation",
        "idempotency scope",
        "occurrence identity",
        "exact-replay equality",
        "mutating predicate",
        "scope-digest-only",
        "Both future receipt families",
        "never from a late read of mutable command state",
        "WorkflowEvent does not add an event-side `source_command_attempt` alias",
    ):
        assert marker in normalized

    d3b = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    response_fields = _fenced_text_after(d3b, "`transport_response_receipts` row containing:")
    failure_fields = _fenced_text_after(d3b, "`transport_attempt_failure_receipts` row:")
    quarantine_fields = _fenced_text_after(d3b, "The future PG-only `workflow_late_result_quarantine`")
    migration_c = _section(d3b, "### 11.3 Migration C", "### 11.4 Migration D")

    assert all("command_attempt" not in line for line in response_fields)
    assert all("command_attempt" not in line for line in failure_fields)
    assert any("workflow_run_id" in line for line in quarantine_fields)
    assert "transport_response_receipts(scope_digest," in migration_c
    assert "workflow_late_result_quarantine(scope_digest," in migration_c
    assert "reconciled_no_call" in d3b


def test_quarantine_axes_and_post_network_order_are_reachable_and_atomic() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 5. Quarantine reachability", "## 6. Mode and transport")
    axes = _fenced_text_after(section, "Consequently:")
    evidence_only_order = _fenced_text_after(section, "zero quarantine authority:")
    classification_order = _fenced_text_after(
        section,
        "then derives current/stale only from locked stored state:",
    )

    assert axes == (
        "cost_state: pending_reconciliation -> reconciled_confirmed | reconciled_uncertain",
        "retention_state: retained -> purged_tombstone",
    )
    assert evidence_only_order == (
        "dispatch_exposure_lock",
        "-> applicable_transport_evidence_receipt_insert_or_exact_replay",
        "-> dispatch_exposure_terminalization_or_exact_replay",
        "-> commit",
    )
    assert classification_order == (
        "d3_dispatch_v2",
        "-> operation_root",
        "-> optional_plan_review_gate",
        "-> participating_commands_sorted",
        "-> intent_predecessor",
        "-> activity_run_attempt",
        "-> dispatch_exposure_lock",
        "-> transport_response_receipt_insert_or_exact_replay",
        "-> optional_response_only_quarantine_insert_or_exact_replay",
        "-> dispatch_exposure_terminalization_or_exact_replay",
        "-> commit",
    )
    normalized = _normalized(section)
    for marker in (
        "valid `TransportResponseReceipt`",
        "attempt-failure receipt is cost/audit evidence only and never creates quarantine",
        "Proven no-call has neither response receipt nor attempt-failure receipt",
        "`reconciled_no_call` is explicitly forbidden",
        "`workflow_run_id` is also forbidden",
        "zero quarantine authority",
        "cannot classify current versus stale",
        "caller flag, callback label, stale `ClaimReceipt`, serialized capability",
        "durable pending classification ownership, retry/recovery, and idempotency",
        "complete global owner-row prefix",
        "derives current/stale only from locked stored state",
        "Only a transaction-local, server-derived stale classification permits",
        "After either path enters `dispatch_exposure_lock`, it cannot return",
        "Any collision, PFX/attempt mismatch, invalid transition, or final terminalization failure rolls back",
        "Neither path locks `cost_reservations`",
        "No PG transaction spans DNS",
    ):
        assert marker in normalized

    assert (
        "For a response or attempt-failure evidence path, one PG transaction uses exactly this composition order"
        not in document
    )
    assert "The post-network UoW itself never writes or locks OperationRun" not in document
    assert "Only the stale branch inserts quarantine" not in document


def test_modes_d0f_and_provider_search_are_fail_closed_without_aliasing() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    mode_section = _section(document, "## 6. Mode and transport", "## 7. D0f")
    header, rows = _markdown_table(mode_section)

    assert header == (
        "mode / transport",
        "durable exposure",
        "valid-response receipt reachable",
        "money semantics",
        "v1 decision",
    )
    assert tuple(row[0] for row in rows) == (
        "`live` + `model_tool_v1`",
        "`simulate` + `model_tool_v1`",
        "`scripted` + `model_tool_v1`",
        "`replay`",
        "Harvest/provider-search",
    )
    assert "positive reservation/worst case" in rows[0][3]
    assert "exactly zero" in rows[1][3]
    assert "exactly zero" in rows[2][3]
    assert "zero writes" in rows[3][4]
    assert "separately owner-ratified transport variant" in rows[4][4]
    assert "Zero cost is not zero evidence" in mode_section
    assert "no non-live evidence may exact-replay into live" in mode_section
    assert "may not write blanks, fake model values, or pretend to be `model_tool_v1`" in mode_section

    d0f_section = _section(document, "## 7. D0f", "## 8. D3c2h1")
    normalized_d0f = _normalized(d0f_section)
    for marker in (
        "**in-memory** schema",
        "no durable `model_invocation_envelope_ref` issuer",
        "**D0f** as a hard predecessor",
        "exactly one durable result-slot/evidence owner and no second envelope schema",
        "complete PFX",
        "strict live/simulate/scripted presence rules",
        "no placeholder ref/hash",
        "D0f owns durable issuance and persistence, not a competing shape",
    ):
        assert marker in normalized_d0f

    d0_contract = D0_CONTRACT_PATH.read_text(encoding="utf-8")
    assert "Immutable in-memory value; deterministic round trip" in d0_contract
    assert "Durable result-slot owner chooses issuance/persistence transaction" in d0_contract
    assert "durable envelope issuer/persistence" in d0_contract


def test_exact_manifests_are_deferred_to_d3c2h1_and_generic_gaps_are_prerequisites() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    d0f_section = _section(document, "## 7. D0f", "## 8. D3c2h1")
    manifest_section = _section(document, "## 8. D3c2h1", "## 9. Mechanism")

    for marker in (
        "no exact Decimal/`NUMERIC(38,12)` or timezone-aware `TIMESTAMPTZ` codec family",
        "defaults to replace-all conflict update",
        "immutable insert-once",
        "split-axis monotonic CAS",
        "must not encode money as float/text",
        "accept caller timestamps",
        "generic replace-all upsert",
    ):
        assert marker in d0f_section

    for marker in (
        "D3c2h1, not this batch",
        "ordered manifests and totals",
        "exact SQL types, nullability/defaults",
        "full-PFX FKs",
        "exact repository/module names",
        "durable pending-response-classification owner/state",
        "retry/recovery, exact replay, and idempotency",
        "full-PFX occurrence/idempotency encoders",
        "response/failure/no-call race acceptance",
        "Harvest/provider-search transport variant",
        "authorizes no migration",
    ):
        assert marker in manifest_section

    assert re.search(r"(?im)^## .*exact .*columns", document) is None
    assert re.search(r"(?i)\b(?:exact|ordered)\s+\d+\s+(?:column|columns|checks?)\b", document) is None


def test_mechanism_matrix_covers_all_ten_invariants_without_implementation_claims() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 9. Mechanism", "## 10. Executable oracle")
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
    assert sum(len(row) - 1 for row in rows) == 80
    assert "TBD" not in section
    assert "Every one of the 80 cells is populated. No cell claims physical implementation." in section
    assert "No new OB-ID is created" in section

    checklist = CHECKLIST_PATH.read_text(encoding="utf-8")
    for invariant in (
        "单写者与聚合所有权",
        "租户键",
        "世代与物理围栏",
        "生命周期完备性",
        "晚到与部分结果",
        "成本诚实性",
        "物理身份绑定",
        "Provenance 与信任边界",
        "自包含与跨文档一致",
        "运行时/模式隔离",
    ):
        assert invariant in checklist


def test_current_physical_absence_and_prerequisite_evidence_are_mechanical() -> None:
    production_python = "\n".join(path.read_text(encoding="utf-8") for path in sorted(SOURCE_ROOT.rglob("*.py")))
    migration_sql = "\n".join(path.read_text(encoding="utf-8") for path in sorted(MIGRATIONS_ROOT.glob("*.sql")))
    for token in FUTURE_PHYSICAL_TOKENS:
        assert token not in production_python
        assert token not in migration_sql

    model_runtime = MODEL_RUNTIME_PATH.read_text(encoding="utf-8")
    assert "class ModelInvocationEnvelopeV1:" in model_runtime
    assert 'if self.provider_mode not in {"simulate", "scripted", "live"}:' in model_runtime
    assert "model_invocation_envelope_ref" not in model_runtime

    control_plane_source = CONTROL_PLANE_REPOSITORY_PATH.read_text(encoding="utf-8")
    control_plane_tree = ast.parse(control_plane_source)
    kind_classes = [node for node in control_plane_tree.body if isinstance(node, ast.ClassDef) and node.name == "Kind"]
    assert len(kind_classes) == 1
    kind_members = {
        target.id
        for node in kind_classes[0].body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert kind_members == {"STR", "INT", "FLOAT", "BOOL_INT", "JSON", "JSON_LIST", "JSON_STR_LIST"}
    assert {"DECIMAL", "TIMESTAMPTZ"}.isdisjoint(kind_members)

    upsert_source = _class_method_source(
        CONTROL_PLANE_REPOSITORY_PATH,
        "TableDescriptor",
        "upsert_sql",
    )
    assert "DO UPDATE SET" in upsert_source
    assert "DO NOTHING" not in upsert_source

    d3c2g_document = D3C2G_DECISION_PATH.read_text(encoding="utf-8")
    d3c2g_owner = _section(
        d3c2g_document,
        "## 3. Owner, aggregate, and initial-v1 eligibility",
        "## 4. Exact `cost_reservations` ordered manifest",
    )
    d3c2g_prerequisites = _section(
        d3c2g_document,
        "## 14. Explicit non-closure and next bounded batch",
    )
    d3c2g_modes = _section(
        d3c2g_document,
        "## 11. Mode semantics, retention, and late invoices",
        "## 12. Mechanism",
    )
    normalized_d3c2g_owner = _normalized(d3c2g_owner)
    normalized_d3c2g_modes = _normalized(d3c2g_modes)
    normalized_d3c2g_prerequisites = _normalized(d3c2g_prerequisites)
    assert _fenced_text_after(d3c2g_owner, "immutable key prefix is exactly") == (
        "(runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id)",
    )
    for marker in (
        "`transport_kind='model_tool_v1'`",
        "`provider_mode IN ('live', 'simulate', 'scripted')`",
        "replay has no applicable v1 pricing spec",
        "creates no ledger row and fails closed",
    ):
        assert marker in normalized_d3c2g_owner
    for marker in (
        "strict-D3 `live|simulate|scripted`: durable parent plus one child per physical call",
        "strict-D3 `live`: reservation/worst-case positive",
        "strict-D3 `simulate|scripted`: every reservation/exposure/accounting",
        "amount exactly zero",
        "valid responses and reported usage still use the normal receipt/evidence path",
    ):
        assert marker in normalized_d3c2g_modes
    for marker in (
        "no exact `Decimal`/`NUMERIC(38,12)` or `TIMESTAMPTZ` codec family",
        "specialized insert-once/exact-replay/CAS primitives",
        "source defines no `model_invocation_envelope_ref` issuer",
    ):
        assert marker in normalized_d3c2g_prerequisites

    plan = PLAN_PATH.read_text(encoding="utf-8")
    assert "## 5a. 评审循环终局状态" in plan
    assert "## 6. 实施批义务清单" in plan
    assert "它不猜 exact schema" in plan

    index = INDEX_PATH.read_text(encoding="utf-8")
    todo = NEXT_TODO_PATH.read_text(encoding="utf-8")
    ledger = RESIDUAL_LEDGER_PATH.read_text(encoding="utf-8")
    plan_candidate = _section(plan, "- D3c2h0 decision-lock candidate", "### D4")
    plan_obligations = _section(plan, "## 6. 实施批义务清单")
    todo_current = _section(
        todo,
        "- [x] D3c2h0 evidence cross-contract ratification",
        "- [ ] Track D 后的 user-owned cohort selection contract",
    )
    ledger_active = _section(ledger, "## Active candidate annotations")
    current_surfaces = (index, plan_candidate, plan_obligations, todo_current, ledger_active)
    for surface in current_surfaces:
        for marker in (
            "pure exposure-first",
            "`d3-dispatch-v2`",
            "quarantine permission=0",
            "stored current state",
            "D3c2h1",
        ):
            assert marker in surface

    for stale_positive in (
        "one-PG-UoW 固定 exposure lock",
        "Post-network composition is one PG UoW in exposure lock",
        "post-network 固定 exposure lock→receipt→optional response quarantine",
        "固定 exposure→receipt→quarantine/cost-axis",
    ):
        assert stale_positive not in "\n".join(current_surfaces)

    for surface in (plan_candidate, plan_obligations, todo_current, ledger_active):
        assert "caller flag" in surface.lower()
        assert "retry/recovery" in surface
