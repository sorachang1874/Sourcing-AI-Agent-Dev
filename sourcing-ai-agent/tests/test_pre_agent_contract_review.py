from __future__ import annotations

import json
import importlib.util
import os
import re
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

from sourcing_agent.durable_runtime import (
    ACTIVITY_SPINE_LEGACY_INTERNAL,
    ACTIVITY_SPINE_REQUIRED,
    ACTIVITY_SPINE_CONTROL_PLANE,
    DEFAULT_COMMAND_OWNER_REGISTRY,
    ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
    ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
    ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
    ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
    ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
    CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES,
    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
    DOMAIN_MUTATION_COMMAND_TYPES,
    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
    EXCEL_INTAKE_RUN_COMMAND_TYPE,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
    EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
    MEDIA_ASSET_CACHE_COMMAND_TYPE,
    ORCHESTRATION_COMMAND_TYPES,
    PROVIDER_ATTEMPT_COMMAND_TYPES,
    workflow_command_control_policy,
    workflow_command_control_state,
    workflow_command_activity_spine_policy,
    workflow_command_display_contract,
    workflow_command_running_control_categories,
)
from sourcing_agent.operation_runtime import (
    ACTION_CONTINUE_ACQUISITION_RUN,
    DEFAULT_ACTION_REGISTRY,
    operation_run_control_state,
)

from tests.source_inspection import all_source_files, find_class_method


REPO_ROOT = Path(__file__).resolve().parents[1]
DOC_PATH = REPO_ROOT / "docs" / "PRE_AGENT_CONTRACT_REVIEW.md"
STORAGE_PATH = REPO_ROOT / "src" / "sourcing_agent" / "storage.py"
LIVE_PG_PATH = REPO_ROOT / "src" / "sourcing_agent" / "control_plane_live_postgres.py"
METRICS_PATH = REPO_ROOT / "src" / "sourcing_agent" / "workflow_service_metrics.py"
API_PATH = REPO_ROOT / "src" / "sourcing_agent" / "api.py"
CLI_PATH = REPO_ROOT / "src" / "sourcing_agent" / "cli.py"
WORKER_DAEMON_PATH = REPO_ROOT / "src" / "sourcing_agent" / "worker_daemon.py"
AGENT_OPERATION_PATH = REPO_ROOT / "docs" / "AGENT_OPERATION_CONTRACT.md"
DURABLE_RUNTIME_PATH = REPO_ROOT / "src" / "sourcing_agent" / "durable_runtime.py"
OPERATION_RUNTIME_PATH = REPO_ROOT / "src" / "sourcing_agent" / "operation_runtime.py"
PUBLIC_WEB_SEARCH_PATH = REPO_ROOT / "src" / "sourcing_agent" / "public_web_search.py"
SEED_DISCOVERY_PATH = REPO_ROOT / "src" / "sourcing_agent" / "seed_discovery.py"
EXPLORATORY_ENRICHMENT_PATH = REPO_ROOT / "src" / "sourcing_agent" / "exploratory_enrichment.py"
CANDIDATE_ARTIFACTS_PATH = REPO_ROOT / "src" / "sourcing_agent" / "candidate_artifacts.py"
RETRIEVAL_RUNTIME_PATH = REPO_ROOT / "src" / "sourcing_agent" / "retrieval_runtime.py"
SERVING_PROJECTION_READER_PATH = REPO_ROOT / "src" / "sourcing_agent" / "serving_projection_reader.py"
PUBLIC_WEB_RUNTIME_CORE_PATH = REPO_ROOT / "src" / "sourcing_agent" / "public_web_runtime_core.py"
SEARCH_PROVIDER_PATH = REPO_ROOT / "src" / "sourcing_agent" / "search_provider.py"
SETTINGS_PATH = REPO_ROOT / "src" / "sourcing_agent" / "settings.py"
CANONICAL_PROJECTION_CONTRACT_PATH = REPO_ROOT / "docs" / "CANONICAL_SERVING_PROJECTION_CONTRACT.md"
CRM_STATE_CONTRACT_PATH = REPO_ROOT / "docs" / "CRM_STATE_CONTRACT.md"
PERSON_ASSET_CONTRACT_PATH = REPO_ROOT / "docs" / "PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md"
DATA_ASSET_GOVERNANCE_PATH = REPO_ROOT / "docs" / "DATA_ASSET_GOVERNANCE.md"
MODEL_NATIVE_SEARCH_CONTRACT_PATH = REPO_ROOT / "docs" / "MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md"
LEGACY_PUBLIC_WEB_DROP_SCRIPT_PATH = REPO_ROOT / "scripts" / "archive_drop_legacy_public_web_tables.py"
CRM_PUBLIC_WEB_LIVE_VALIDATION_SCRIPT_PATH = (
    REPO_ROOT / "scripts" / "run_crm_public_web_live_product_validation.py"
)
ASSET_MEDIA_BACKFILL_SCRIPT_PATH = REPO_ROOT / "scripts" / "backfill_person_company_asset_media.py"
FRONTEND_API_CONTRACT_DOC_PATH = REPO_ROOT / "docs" / "FRONTEND_API_CONTRACT.md"
FRONTEND_API_TYPES_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.ts"
FRONTEND_API_SCHEMA_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.schema.json"
FRONTEND_API_ADAPTER_PATH = REPO_ROOT / "contracts" / "frontend_api_adapter.ts"
FRONTEND_APP_PATH = REPO_ROOT / "frontend-demo" / "src" / "App.tsx"
FRONTEND_SIDEBAR_PATH = REPO_ROOT / "frontend-demo" / "src" / "components" / "SearchHistorySidebar.tsx"
FRONTEND_OPERATIONS_PAGE_PATH = REPO_ROOT / "frontend-demo" / "src" / "pages" / "OperationsPage.tsx"
FRONTEND_STYLES_PATH = REPO_ROOT / "frontend-demo" / "src" / "styles.css"
MAKEFILE_PATH = REPO_ROOT / "Makefile"
TESTING_PLAYBOOK_PATH = REPO_ROOT / "docs" / "TESTING_PLAYBOOK.md"
INDEPENDENT_REVIEW_GATE_PATH = REPO_ROOT / "docs" / "INDEPENDENT_REVIEW_GATE.md"
INDEPENDENT_REVIEW_BRIEF_PATH = REPO_ROOT / "docs" / "INDEPENDENT_REVIEW_BRIEF.md"
INDEPENDENT_REVIEW_RUNNER_PATH = REPO_ROOT / "scripts" / "run_independent_review_gate.py"


def _load_crm_public_web_live_validation_module():
    spec = importlib.util.spec_from_file_location(
        "run_crm_public_web_live_product_validation",
        CRM_PUBLIC_WEB_LIVE_VALIDATION_SCRIPT_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_independent_review_runner_module():
    spec = importlib.util.spec_from_file_location(
        "run_independent_review_gate",
        INDEPENDENT_REVIEW_RUNNER_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
PROJECTION_CRM_API_CONTRACTS_PATH = REPO_ROOT / "tests" / "test_projection_crm_api_contracts.py"

DURABLE_OPERATION_TABLES = {
    "workflow_events",
    "workflow_current_state",
    "workflow_commands",
    "runtime_outbox",
    "agent_actions",
    "operation_runs",
    "acquisition_runs",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "acquisition_discovery_lanes",
    "operation_events",
    "crm_tasks",
    "company_assets",
    "company_evidence",
    "company_assertions",
}

REQUIRED_MODULE_ROWS = {
    "Durable runtime",
    "Operation runtime",
    "Acquisition root/plan",
    "Acquisition probe/scale",
    "Provider-backed discovery",
    "Profile fetch/registry",
    "Post-profile materialization",
    "Serving projection/public reader",
    "CRM",
    "Person asset/evidence/assertion",
    "Company asset/evidence/assertion",
    "CRM Public Web",
    "Model-native Search",
    "Legacy target-candidate Public Web",
    "Excel intake",
    "Export",
    "Frontend API",
}

CRM_PUBLIC_WEB_PHASE_COMMANDS = {
    "CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE": "crm.public_web.search.submit",
    "CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE": "crm.public_web.search.poll_fetch",
    "CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE": "crm.public_web.documents.fetch",
    "CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE": "crm.public_web.evidence.adjudicate",
    "CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE": "crm.public_web.model_safe.finalize",
    "CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE": "crm.public_web.signals.materialize",
}


def _module_rows(markdown: str) -> dict[str, list[str]]:
    rows: dict[str, list[str]] = {}
    for line in markdown.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|") or stripped.startswith("| ---"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if not cells or cells[0] == "Module":
            continue
        rows[cells[0]] = cells
    return rows


def _completion_evidence_rows(markdown: str) -> dict[str, list[str]]:
    section = markdown.split("## Current Completion Evidence Matrix", 1)[1].split(
        "## Fast Preflight Checklist",
        1,
    )[0]
    rows: dict[str, list[str]] = {}
    for line in section.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|") or stripped.startswith("| ---"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if not cells or cells[0] == "Scope":
            continue
        rows[cells[0]] = cells
    return rows


def _method_block(source: str, method_name: str, next_method_name: str) -> str:
    return source.split(f"def {method_name}", 1)[1].split(f"def {next_method_name}", 1)[0]


def _class_method_source(method_name: str, class_name: str = "SourcingOrchestrator") -> str:
    """Exact source segment of ``class_name.method_name`` wherever it lives under src."""
    return find_class_method(method_name, class_name=class_name)[1]


@lru_cache(maxsize=None)
def _sourcing_agent_source_texts() -> tuple[str, ...]:
    return tuple(path.read_text(encoding="utf-8") for path in all_source_files())


def _assert_token_in_sourcing_agent_sources(token: str) -> None:
    """The token must survive somewhere under src/sourcing_agent (any module)."""
    assert any(token in source for source in _sourcing_agent_source_texts()), token


def _typescript_block(source: str, start_marker: str, end_marker: str) -> str:
    return source.split(start_marker, 1)[1].split(end_marker, 1)[0]


def _frontend_normal_source_texts() -> dict[str, str]:
    frontend_src = REPO_ROOT / "frontend-demo" / "src"
    source_paths = sorted(
        path
        for path in frontend_src.rglob("*")
        if path.suffix in {".ts", ".tsx"} and path.is_file()
    )
    return {
        str(path.relative_to(REPO_ROOT)): path.read_text(encoding="utf-8")
        for path in source_paths
    }


def test_pre_agent_contract_review_has_complete_owner_matrix() -> None:
    markdown = DOC_PATH.read_text(encoding="utf-8")
    rows = _module_rows(markdown)

    assert set(rows) >= REQUIRED_MODULE_ROWS
    for module in REQUIRED_MODULE_ROWS:
        cells = rows[module]
        assert len(cells) == 5
        assert all(cell for cell in cells), module
        fast_preflight = cells[3].lower()
        assert any(
            marker in fast_preflight
            for marker in ("test", "metrics", "signoff", "preflight", "browser")
        ), module
    assert "Operation/Command/Activity contract active" in rows["Frontend API"][4]
    assert "Command control responses must expose `display_contract`, `control_policy`, and `activity_spine_policy`" in markdown


def test_pre_agent_completion_evidence_matrix_records_current_gates() -> None:
    markdown = DOC_PATH.read_text(encoding="utf-8")
    rows = _completion_evidence_rows(markdown)

    assert "## Current Completion Evidence Matrix" in markdown
    for scope in (
        "W7e legacy Public Web physical deletion / isolation",
        "W7f CRM Public Web typed-command phases",
        "W11 Agent-callable workflow atomization",
        "W10 full contract review / fail-closed signoff",
        "Independent review gate",
        "Model-native Search experiment gate",
    ):
        assert f"| {scope} |" in markdown
        assert scope in rows
        assert len(rows[scope]) == 3
        assert rows[scope][1]
        assert rows[scope][2]
    assert "legacy target-candidate facade is physically absent" in markdown
    assert "phase commands record ActivityRun/Attempt/EntityDelta evidence" in markdown
    assert "DataForSEO item-level batch retry isolation" in markdown
    assert "Provider ActivityAttempt after-start control v1 is confirmed" in markdown
    assert "broader product UI polish remains W9/Phase-13-adjacent, not an atomization blocker" in markdown
    assert "W7g real live/product validation requires reviewed CRM record ids plus explicit live-provider confirmation" in markdown
    assert "W7g guarded dry-run" in markdown
    assert "docs/INDEPENDENT_REVIEW_GATE.md" in markdown
    assert "make independent-review-gate" in markdown
    assert "docs/MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md" in markdown
    assert "DataForSEO+model-native Search" in markdown
    assert rows["W7e legacy Public Web physical deletion / isolation"][2].startswith("None for normal runtime")
    assert "not a missing typed-command boundary" in rows["W7f CRM Public Web typed-command phases"][2]
    assert "poll-cancel/quarantine" in rows["W11 Agent-callable workflow atomization"][1]
    assert "stronger provider remote-interrupt semantics must first update this Contract" in rows[
        "W10 full contract review / fail-closed signoff"
    ][2]
    assert "NO-GO" in rows["Independent review gate"][1]
    assert "live provider validation" in rows["Independent review gate"][2]


def test_independent_review_gate_is_documented_and_executable() -> None:
    gate = INDEPENDENT_REVIEW_GATE_PATH.read_text(encoding="utf-8")
    brief = INDEPENDENT_REVIEW_BRIEF_PATH.read_text(encoding="utf-8")
    runner = INDEPENDENT_REVIEW_RUNNER_PATH.read_text(encoding="utf-8")
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")
    agents = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")
    live_runner = (REPO_ROOT / "scripts" / "run_crm_public_web_live_product_validation.py").read_text(
        encoding="utf-8"
    )

    assert "Independent Review Gate" in agents
    assert "reviewer must not be the author" in agents
    assert "codex exec --sandbox read-only" in agents
    assert "NO-GO" in gate
    assert "reviewer that did not author the change" in gate
    assert "docs/INDEPENDENT_REVIEW_BRIEF.md" in gate
    assert "feeds that prompt file to Codex" in gate
    assert "< runtime/reviews/<review-id>.prompt.md" in gate
    assert "Python so it works on macOS without GNU `timeout`" in gate
    assert "REVIEW_MODEL=gpt-5.5" in gate
    assert "REVIEW_REASONING_EFFORT=xhigh" in gate
    assert "REVIEW_SERVICE_TIER=fast" in gate
    assert "codex exec \\" in gate
    assert "-c service_tier='\"fast\"'" in gate
    assert "-c model_reasoning_effort='\"xhigh\"'" in gate
    assert "Provider cost safety" in brief
    assert "Product runtime model calls currently do not use model-native web search or tool calls" in (
        REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md"
    ).read_text(encoding="utf-8")
    assert "subprocess.DEVNULL" in runner
    assert "timeout=timeout_seconds" in runner
    assert "\"bash\", \"-lc\", shell_command" in runner
    assert "## Review Metadata" in runner
    assert "contract_docs_considered" in runner
    assert "author_validation" in runner
    assert "DEFAULT_REVIEWER_MODEL = \"gpt-5.5\"" in runner
    assert "DEFAULT_REVIEWER_REASONING_EFFORT = \"xhigh\"" in runner
    assert "DEFAULT_REVIEWER_SERVICE_TIER = \"fast\"" in runner
    assert "_build_codex_args" in runner
    assert "INVALID_REVIEW_ARTIFACT: bare NO-GO" in runner
    assert "reviewer produced no output" in runner
    assert "review_body == \"NO-GO\"" in runner
    assert "No such file or directory: 'timeout'" not in runner
    assert "--sandbox" in runner
    assert "read-only" in runner
    assert "INDEPENDENT_REVIEW_GATE_CMD" in makefile
    assert "independent-review-gate" in makefile
    assert "REVIEW_MODEL ?= gpt-5.5" in makefile
    assert "REVIEW_REASONING_EFFORT ?= xhigh" in makefile
    assert "REVIEW_SERVICE_TIER ?= fast" in makefile
    assert '--reasoning-effort "$(REVIEW_REASONING_EFFORT)"' in makefile
    assert '--service-tier "$(REVIEW_SERVICE_TIER)"' in makefile
    assert "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED ?= 0" in makefile
    assert "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT ?=" in makefile
    assert "Refusing to execute CRM Public Web live validation without CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED=1" in makefile
    assert "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT does not exist" in makefile
    assert "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED=1 CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT=" in makefile
    assert 'INDEPENDENT_REVIEW_PASSED_ENV = "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED"' in live_runner
    assert 'INDEPENDENT_REVIEW_ARTIFACT_ENV = "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT"' in live_runner
    assert "_validate_independent_review_artifact" in live_runner
    assert 'INDEPENDENT_REVIEW_SCOPE_TOKENS_ENV = "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_TOKENS"' in live_runner
    assert 'INDEPENDENT_REVIEW_REQUIRED_FILES_ENV = "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_REQUIRED_FILES"' in live_runner
    assert 'DEFAULT_INDEPENDENT_REVIEW_SCOPE_TOKENS = ("W7g", "CRM Public Web", "live")' in live_runner
    assert "DEFAULT_INDEPENDENT_REVIEW_REQUIRED_FILES" in live_runner
    assert "_missing_review_metadata_fields" in live_runner
    assert "independent review artifact is not a valid GO review" in live_runner
    assert "independent review artifact must be under runtime/reviews" in live_runner
    assert "independent review artifact verdict is NO-GO" in live_runner
    assert "does not match required live validation scope tokens" in live_runner
    assert "does not cover required live validation files" in live_runner
    assert '"artifact_validation": _validate_independent_review_artifact' in live_runner
    assert '"independent_review": {' in live_runner


def test_independent_review_runner_defaults_to_gpt55_xhigh_fast_mode() -> None:
    runner = _load_independent_review_runner_module()

    codex_args = runner._build_codex_args(
        root=REPO_ROOT,
        output_path=Path("runtime/reviews/pytest_review.md"),
        model=runner.DEFAULT_REVIEWER_MODEL,
        reasoning_effort=runner.DEFAULT_REVIEWER_REASONING_EFFORT,
        service_tier=runner.DEFAULT_REVIEWER_SERVICE_TIER,
    )

    assert codex_args[:6] == ["codex", "exec", "--cd", str(REPO_ROOT), "--sandbox", "read-only"]
    assert codex_args[codex_args.index("--model") : codex_args.index("--model") + 2] == [
        "--model",
        "gpt-5.5",
    ]
    assert 'service_tier="fast"' in codex_args
    assert 'model_reasoning_effort="xhigh"' in codex_args
    assert codex_args.count("-c") == 2


def test_independent_review_runner_can_explicitly_inherit_codex_defaults() -> None:
    runner = _load_independent_review_runner_module()

    codex_args = runner._build_codex_args(
        root=REPO_ROOT,
        output_path=Path("runtime/reviews/pytest_review.md"),
        model="default",
        reasoning_effort="inherit",
        service_tier="auto",
    )

    assert "--model" not in codex_args
    assert "-c" not in codex_args
    assert "--sandbox" in codex_args
    assert "read-only" in codex_args


def test_independent_review_gate_has_mandatory_contract_and_milestone_triggers() -> None:
    gate = INDEPENDENT_REVIEW_GATE_PATH.read_text(encoding="utf-8")
    brief = INDEPENDENT_REVIEW_BRIEF_PATH.read_text(encoding="utf-8")
    pre_agent = DOC_PATH.read_text(encoding="utf-8")
    agents = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")

    for required in (
        "## Mandatory Triggers",
        "Contract docs, schema, owner matrix, source of truth",
        "Frontend/backend public API fields",
        "Durable runtime, workflow event/command/activity/attempt/entity-delta ownership",
        "Provider scheduling, after-start control, retry/circuit/rate-limit policy",
        "Public Web search, document fetch, model adjudication",
        "Migration bridge deletion, legacy endpoint retirement, PG-only storage cutover",
        "Any implementation slice being claimed as a completed milestone",
        "If the author is unsure whether a change affects shared semantics, run the gate",
        "## Allowed Skips",
        "## Independence Rule",
        "## Scope Discipline",
        "Independent review is scoped review, not a full development-session resume",
        "It should not read full `PROGRESS.md`, full `docs/NEXT_TODO.md`, or full long Contract docs",
        "runtime/reviews/<stamp>_<title>.md",
        "A bare `NO-GO` without at least one prioritized finding is an invalid review artifact",
        "A green targeted test, W6/nightly, or browser/manual pass does not override",
    ):
        assert required in gate

    for required in (
        "Scope discipline",
        "No self-certification",
        "Scoped context",
        "Start with a compact evidence header",
        "Contract docs considered",
        "If the change claims a milestone or manual/live readiness",
        "If evidence is insufficient, return `NO-GO`",
        "A bare `NO-GO` without at least one `severity / file:line / issue` finding is an invalid review artifact",
    ):
        assert required in brief

    assert "mandatory triggers" in pre_agent
    assert "claiming a complete feature/milestone" in pre_agent
    assert "explicitly accepted by the user/founder" in pre_agent
    assert "feature or phase being claimed as complete" in agents
    assert "Independent review is a scoped read-only gate" in agents
    assert "it should not read full `PROGRESS.md`, full `docs/NEXT_TODO.md`, or full long Contract files" in agents
    assert "W6/nightly, live provider validation, or manual browser review should validate long-chain behavior" in agents


def test_model_native_search_contract_is_fail_closed_before_implementation() -> None:
    contract = MODEL_NATIVE_SEARCH_CONTRACT_PATH.read_text(encoding="utf-8")
    durable_doc = (REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md").read_text(encoding="utf-8")
    pre_agent = DOC_PATH.read_text(encoding="utf-8")
    search_provider = SEARCH_PROVIDER_PATH.read_text(encoding="utf-8")
    settings = SETTINGS_PATH.read_text(encoding="utf-8")
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")

    assert "Provider id reserved: `model_native_search`" in contract
    assert "Normal runtime behavior: fail closed if `model_native_search` appears in `SEARCH_PROVIDER_ORDER`" in contract
    assert "experimental_evidence_only" in contract
    assert "must not silently replace DataForSEO evidence" in durable_doc
    assert "bypass DataForSEO item-level retry semantics" in durable_doc
    assert "Model-native Search" in pre_agent
    assert "Reserved/fail-closed" in pre_agent
    assert "MODEL_NATIVE_SEARCH_PROVIDER_NAME = \"model_native_search\"" in search_provider
    assert "must not silently fall back to DataForSEO" in search_provider
    assert "no registered provider implementation and owner contract" in search_provider
    assert "enable_model_native_search: bool = False" in settings
    assert "model_native_search_mode: str = \"disabled\"" in settings
    assert "SEARCH_PROVIDER_ENABLE_MODEL_NATIVE_SEARCH" in settings
    assert "model_native_search_provider_order_fails_closed_without_contract" in makefile


def test_pre_agent_direction_gates_are_explicit_before_goal_closeout() -> None:
    markdown = DOC_PATH.read_text(encoding="utf-8")
    durable_doc = (REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md").read_text(encoding="utf-8")
    provider_retry_owner_source = _class_method_source(
        "_execute_operation_native_profile_fetch_provider_command_payload"
    )

    assert "## Direction Gates Before Closing This Goal" in markdown
    assert "| W7g CRM Public Web live/product validation |" in markdown
    assert "Reviewed CRM record ids and explicit live-provider confirmation" in markdown
    assert "CRM_PUBLIC_WEB_LIVE_PRE_AGENT_CONTRACT_PASSED=1" in markdown
    assert "--reviewed-crm-record-ids" in markdown
    assert "CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED=1" in markdown
    assert "Do not mark W7g product validation complete from dry-run, fake-provider, Qwen healthcheck, or W6/nightly evidence alone" in markdown
    assert "Do not re-enable `/api/target-candidates/public-web...` as a validation shortcut" in markdown
    assert "Provider after-start control v1 is confirmed" in markdown
    assert "Harvest/Apify/DataForSEO use local `poll_cancel_late_result_quarantine`" in markdown
    assert "Document fetch and Qwen adjudication use `fail_closed_until_terminal`" in markdown
    assert "provider_after_start_control_status='active'" in markdown
    assert "provider_after_start_control_mode='poll_cancel_late_result_quarantine'" in markdown
    assert "provider_after_start_control_contract='w11_provider_after_start_control_v1'" in durable_doc
    assert "Do not implement remote interruption from the workflow command API" in markdown
    assert "Do not mutate provider registry, projection, CRM, Public Web, Excel, media, asset, or lane rows from a control endpoint to simulate cancellation" in markdown
    assert "late provider result must be quarantined unless an explicit owner adoption command accepts it" in markdown
    for source in (markdown, durable_doc, provider_retry_owner_source):
        assert "profile_provider_retry_bucketed" in source
        assert "profile_provider_retry_exhausted" in source
        assert "bucketed_entity_retry_after_normal_wave" in source
        assert "provider_attempt_scope" in source
        assert "retry_wave_index" in source
    assert "must not mark the whole provider command `retry_wait` merely because one URL failed" in markdown
    assert "partial provider success is item-level" in durable_doc
    assert "DataForSEO Standard Queue batch envelopes are transport batches, not retry/progress units" in durable_doc
    assert "Downstream joins, retries, manifests, and Public Web outcomes must use this key" in durable_doc
    assert "must fail closed when a batch spec lacks `task_key`" in markdown
    assert "must not generate identity from query text, task id, candidate ordinal, or array position" in durable_doc
    assert "candidate display ordinal" in durable_doc
    assert "stable candidate/query identity" in markdown
    assert "stable search query signature" in markdown
    assert "`candidate_id::index`" in markdown
    assert "instead of using request-order fallback" in durable_doc
    assert "query_identity_key" in durable_doc
    assert "dataforseo_batch_failed_query_retry_only" in markdown
    assert "dataforseo_task_get_failed_task_retry_only" in markdown
    assert "operation_native_profile_provider_fetch_partial_retry_planned" in provider_retry_owner_source


def test_query_batch_identity_sources_do_not_use_display_ordinals() -> None:
    public_web_source = PUBLIC_WEB_SEARCH_PATH.read_text(encoding="utf-8")
    runtime_core_source = PUBLIC_WEB_RUNTIME_CORE_PATH.read_text(encoding="utf-8")
    seed_discovery_source = SEED_DISCOVERY_PATH.read_text(encoding="utf-8")
    exploratory_source = EXPLORATORY_ENRICHMENT_PATH.read_text(encoding="utf-8")

    assert "public_web_query_identity_key" in public_web_source
    assert "task_key = public_web_query_identity_key(" in public_web_source
    assert 'task_key = f"{plan.ordinal:02d}:' not in public_web_source
    assert 'task_key = f"01:{query.query_id}:' not in runtime_core_source

    assert "_search_seed_worker_key(" in seed_discovery_source
    assert "query_text=query_text" in seed_discovery_source
    assert 'query_suffix = "q_"' in seed_discovery_source
    assert '_search_seed_worker_key(query_spec["bundle_id"], index, employment_status)' not in seed_discovery_source

    assert "_exploration_query_task_key" in exploratory_source
    assert 'task_key = f"{candidate_id}::{index:02d}"' not in exploratory_source
    assert 'return f"{str(candidate_id or \'\').strip()}::q_{query_hash}"' in exploratory_source


def test_w10_review_exposes_w11d_direction_decision_before_phase_13() -> None:
    markdown = DOC_PATH.read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO
    rows = _module_rows(markdown)

    assert "W11d Operation-Native Discovery" in markdown
    assert "Direction confirmed" in markdown
    assert "Future Activity-Spine Extension Rule" in markdown
    assert "workflow_activity_runs" in markdown
    assert "workflow_activity_attempts" in markdown
    assert "workflow_entity_deltas" in markdown
    assert "acquisition_discovery_lanes" in markdown
    assert "/api/workflow/discovery-lanes" in markdown
    assert "operation_native_discovery_activity_owner" in markdown
    assert "legacy job shell" in markdown
    assert "Domain tables may be added only as efficient read models" in markdown
    assert "Future W11 extension rule" in next_todo
    assert "must not own retry, cancel, resume, provider attempts, or recovery state" in next_todo
    assert "PRE_AGENT_CONTRACT_REVIEW.md" in next_todo
    assert "command-owned `display_contract`, `control_policy`, `control_state`, `activity_spine_policy`, and `fallback_status=fail_closed`" in next_todo
    assert "never infer display/control state from domain read-model fields" in next_todo
    assert "Current remaining W11 work is product UI polish beyond the minimal `/operations` queue and manual/live validation of CRM Public Web quality" in next_todo
    assert "stronger remote provider interruption remains a future provider-specific contract" in next_todo
    assert "current goal pause line" in next_todo
    assert "W7e and W7f are closed for normal runtime" in next_todo
    assert "covered by `make ci-pre-agent-contract`" in next_todo
    assert "not another hidden fallback/dual-owner implementation slice" in next_todo
    assert "W7g, which requires reviewed CRM record ids plus explicit live-provider confirmation" in next_todo
    assert "Provider after-start control v1 is confirmed and implemented" in next_todo
    assert "Do not use W6/nightly or extra guardrail patches to substitute for that product/live decision" in next_todo
    assert "richer product/API status surfaces" not in next_todo
    assert "no Operation dispatch inline `queue_workflow`" in rows["Acquisition root/plan"][4]
    assert "Operation-native Activity spine" in rows["Acquisition probe/scale"][4]
    assert "operation-native discovery writes ActivityRun/Attempt/EntityDelta/lane evidence" in rows[
        "Provider-backed discovery"
    ][4]
    assert "Active through W11c" not in markdown
    assert "provider-backed discovery handoff is W11d" not in markdown
    assert "first slices materialize activity/lane/entity-delta boundaries" not in markdown


def test_next_todo_records_future_activity_spine_target_architecture() -> None:
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO

    assert "Future Activity Spine target architecture" in next_todo
    assert "`workflow_activity_runs` as the generic bounded-side-effect spine" in next_todo
    assert "`workflow_activity_attempts` as the provider/local attempt envelope" in next_todo
    assert "`workflow_entity_deltas` as the explainability layer" in next_todo
    assert "must not become a second workflow engine" in next_todo
    assert "retry, cancel, resume, or inspect any typed activity" in next_todo
    assert "provider poll, dataset fetch, parse, profile terminalization" in next_todo
    assert "Operation/Command/Activity APIs" in next_todo
    assert "PG-only storage boundary" in next_todo
    assert "fail-closed domain read model contract" in next_todo


def test_w10_review_exposes_w11e_profile_activity_boundary() -> None:
    markdown = DOC_PATH.read_text(encoding="utf-8")
    durable_doc = (REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md").read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO
    metrics_source = METRICS_PATH.read_text(encoding="utf-8")

    for source in (markdown, durable_doc, next_todo):
        assert "linkedin.profile_fetch.activity.run" in source
        assert "linkedin.profile_fetch.provider.fetch" in source
        assert "linkedin.profile_terminal.admit" in source
        assert "projection.profile_admission.apply" in source
        assert "projection.run_scope.finalize" in source
        assert "legacy job shell" in source
    assert "linkedin_profile_activity_owner" in markdown
    assert "fetch_profile_sample` plans `linkedin.profile_fetch.activity.run" in next_todo
    assert "run_scope_projection` EntityDeltas" in markdown
    assert "facet_layering` EntityDeltas" in markdown
    assert "board_visible_patch` EntityDeltas" in markdown
    assert "local_profile_delta` EntityDeltas" in markdown
    assert "entity_type=run_scope_projection" in durable_doc
    assert "facet_layering` EntityDeltas" in durable_doc
    assert "board_visible_patch` EntityDeltas" in durable_doc
    assert "entity_type=local_profile_delta" in durable_doc
    assert "run-scope projection finalization deltas" in next_todo
    assert "facet/layering deltas" in next_todo
    assert "board-visible publication deltas" in next_todo
    assert "local-apply deltas" in next_todo
    assert "LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE" in metrics_source
    assert "LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE" in metrics_source
    # The expected-owner binding moved from a literal table in
    # workflow_service_metrics.py to the durable_runtime command type registry;
    # assert the registry still pins the activity owner for both commands so the
    # derived metrics contract keeps them contract-visible.
    durable_runtime_source = DURABLE_RUNTIME_PATH.read_text(encoding="utf-8")
    assert "LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER" in durable_runtime_source
    from sourcing_agent.durable_runtime import (
        LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
        LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
    )
    from sourcing_agent.workflow_service_metrics import _DURABLE_COMMAND_OWNER_CONTRACTS

    for command_type in (
        LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
        LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
    ):
        assert (
            _DURABLE_COMMAND_OWNER_CONTRACTS[command_type]["expected_owner"]
            == LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER
        )


def test_pre_agent_contract_gate_is_single_fast_entrypoint() -> None:
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")
    playbook = TESTING_PLAYBOOK_PATH.read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO

    assert "ci-pre-agent-contract" in makefile
    assert "CI_PRE_AGENT_CONTRACT_CMD" in makefile
    assert "tests/test_pre_agent_contract_review.py" in makefile
    assert "tests/test_operation_runtime.py" in makefile
    assert "tests/test_crm_public_web_runtime_boundary.py" in makefile
    assert "tests/test_legacy_public_web_retirement_audit.py" in makefile
    assert "tests/test_scripted_smoke_signoff.py -k durable_command_owner_contract" in makefile
    assert "tests/test_enrichment.py -k 'isolates_retry_wait or retry_wait_as_isolated_batch'" in makefile
    assert "SOURCING_REQUIRE_PG_DURABLE_RUNTIME_TESTS=1" in makefile
    assert "tests/test_dataforseo_client.py tests/test_search_provider.py tests/test_public_web_search.py" in makefile
    assert "dataforseo_provider_submit_batch_retries_only_failed_query_item" in makefile
    assert "dataforseo_provider_submit_batch_fails_closed_without_provider_identity" in makefile
    assert "batch_provider_requires_stable_query_identity_key" in makefile
    assert "provider_chain_rejects_missing_query_identity_before_fallback" in makefile
    assert "public_web_batch_query_identity_is_candidate_query_scoped_not_order_scoped" in makefile
    assert "search_seed_worker_key_uses_query_identity_not_order_ordinal" in makefile
    assert "exploration_query_task_key_uses_query_identity_not_order_ordinal" in makefile
    assert "dataforseo_provider_fetch_ready_batch_preserves_success_when_one_task_get_fails" in makefile
    assert "batch_queue_submit_failure_is_query_level_not_whole_batch" in makefile
    assert "run_crm_public_web_live_product_validation.py --expected-model" in makefile
    ci_pre_agent_contract = re.search(r"^CI_PRE_AGENT_CONTRACT_CMD = (.+)$", makefile, re.M)
    assert ci_pre_agent_contract is not None
    ci_pre_agent_contract_cmd = ci_pre_agent_contract.group(1)
    assert "tests/test_enrichment.py -k 'isolates_retry_wait or retry_wait_as_isolated_batch'" in ci_pre_agent_contract_cmd
    assert "SOURCING_REQUIRE_PG_DURABLE_RUNTIME_TESTS=1" in ci_pre_agent_contract_cmd
    assert "dataforseo_provider_submit_batch_retries_only_failed_query_item" in ci_pre_agent_contract_cmd
    assert "dataforseo_provider_submit_batch_fails_closed_without_provider_identity" in ci_pre_agent_contract_cmd
    assert "batch_provider_requires_stable_query_identity_key" in ci_pre_agent_contract_cmd
    assert "provider_chain_rejects_missing_query_identity_before_fallback" in ci_pre_agent_contract_cmd
    assert "public_web_batch_query_identity_is_candidate_query_scoped_not_order_scoped" in ci_pre_agent_contract_cmd
    assert "search_seed_worker_key_uses_query_identity_not_order_ordinal" in ci_pre_agent_contract_cmd
    assert "exploration_query_task_key_uses_query_identity_not_order_ordinal" in ci_pre_agent_contract_cmd
    assert "dataforseo_provider_fetch_ready_batch_preserves_success_when_one_task_get_fails" in ci_pre_agent_contract_cmd
    assert "run_crm_public_web_live_product_validation.py" in ci_pre_agent_contract_cmd
    assert "--expected-model" in ci_pre_agent_contract_cmd
    assert "--report-json" in ci_pre_agent_contract_cmd
    assert "--execute-live" not in ci_pre_agent_contract_cmd
    assert "--confirm-live-provider-cost" not in ci_pre_agent_contract_cmd
    assert "CRM_PUBLIC_WEB_LIVE_DRY_RUN" not in ci_pre_agent_contract_cmd
    assert "make ci-pre-agent-contract" in playbook
    assert "DataForSEO item-level batch retry checks" in playbook
    assert "Pre-Agent contract gate" in playbook
    assert "If this target fails, do not use W6/nightly" in playbook
    assert "make ci-pre-agent-contract" in next_todo
    assert "guarded W7g CRM Public Web dry-run runner" in next_todo


def test_w7g_crm_public_web_live_validation_has_guarded_runner() -> None:
    script = CRM_PUBLIC_WEB_LIVE_VALIDATION_SCRIPT_PATH.read_text(encoding="utf-8")
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")
    playbook = TESTING_PLAYBOOK_PATH.read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO
    review_doc = DOC_PATH.read_text(encoding="utf-8")

    assert "run_crm_public_web_live_product_validation.py" in makefile
    assert "test-crm-public-web-live-product-validation" in makefile
    assert "CRM_PUBLIC_WEB_LIVE_DRY_RUN ?= 1" in makefile
    assert "CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED ?= 0" in makefile
    assert "CRM_PUBLIC_WEB_LIVE_POLL_TIMEOUT_SECONDS ?= 600" in makefile
    assert "LIVE_CONFIRM" in makefile
    assert 'SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE="postgres_only"' in makefile
    assert 'SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES="1"' in makefile
    assert 'SOURCING_PG_ONLY_SQLITE_BACKEND="shared_memory"' in makefile
    assert 'SOURCING_ALLOW_TEST_REPO_POSTGRES_ENV="1"' in makefile
    assert "Running ci-pre-agent-contract before CRM Public Web live provider validation" in makefile
    assert "$(CI_PRE_AGENT_CONTRACT_CMD) || exit $$?" in makefile
    assert "CRM_PUBLIC_WEB_LIVE_PRE_AGENT_CONTRACT_PASSED=1" in makefile
    assert "CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED=1" in makefile
    assert "--confirm-live-provider-cost" in makefile
    assert "--reviewed-crm-record-ids" in makefile
    assert "/api/crm/records/public-web-search" in script
    assert "/api/crm/records/public-web-search/poll" in script
    assert "/api/crm/records/{crm_record_id}/public-web-search" in script
    assert "/api/crm/records/public-web-export" in script
    assert "/api/target-candidates" in script
    assert "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS" in script
    assert "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_TO_CRM_SYNC" in script
    assert "SOURCING_EXTERNAL_PROVIDER_MODE" in script
    assert "DISALLOWED_LIVE_PROVIDER_MODES" in script
    assert "scripted/simulate/replay/fake/mock" in script
    assert "--execute-live" in script
    assert "--confirm-live-provider-cost" in script
    assert "--reviewed-crm-record-ids" in script
    assert "--poll-timeout-seconds" in script
    assert "MODEL_VERIFIABLE_RUN_STATUSES" in script
    assert "_nonterminal_run_summaries" in script
    assert "LIVE_CONFIRM" in script
    assert "SOURCING_LIVE_PROVIDER_CONFIRM" in script
    assert "CRM_PUBLIC_WEB_LIVE_PRE_AGENT_CONTRACT_PASSED" in script
    assert "gpt-5.5" in script
    assert "target_candidate_public_web_v1" in script
    assert "live_prerequisites" in script
    assert "record_id_selection_guidance" in script
    assert "recommended_make_command" in script
    assert "dry_run_ready" in script
    assert "`ci-pre-agent-contract` includes the guarded W7g dry-run entrypoint" in playbook
    assert "make ci-pre-agent-contract" in playbook
    assert "make test-crm-public-web-live-product-validation" in playbook
    assert "live_prerequisites.ready_to_execute_live_with_current_args" in playbook
    assert "live_prerequisites.missing_or_required_before_live" in playbook
    assert "live_prerequisites.record_id_selection_guidance" in playbook
    assert "live_prerequisites.recommended_make_command" in playbook
    assert "CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED=1" in playbook
    assert "CRM_PUBLIC_WEB_LIVE_POLL_TIMEOUT_SECONDS" in playbook
    assert "must not be conflated with model fallback" in playbook
    assert "do not use legacy target-candidate ids" in playbook
    assert "Do not use the retired `/api/target-candidates/public-web...` aliases" in playbook
    assert "Guarded entrypoint added" in next_todo
    assert "CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED=1" in next_todo
    assert "record_id_selection_guidance" in next_todo
    assert "recommended_make_command" in next_todo
    assert "CRM_PUBLIC_WEB_LIVE_POLL_TIMEOUT_SECONDS" in next_todo
    assert "Poll timeout is a run-terminality failure" in next_todo
    assert "guarded W7g runner dry-run" in review_doc
    assert "live_prerequisites" in review_doc
    assert "CRM_PUBLIC_WEB_LIVE_RECORD_IDS=\"...\"" in review_doc


def test_w7g_live_runner_requires_pre_agent_contract_gate_env(tmp_path: Path) -> None:
    report_path = tmp_path / "w7g_live_report.json"
    env = os.environ.copy()
    env["LIVE_CONFIRM"] = "1"
    env.pop("CRM_PUBLIC_WEB_LIVE_PRE_AGENT_CONTRACT_PASSED", None)
    env.pop("SOURCING_EXTERNAL_PROVIDER_MODE", None)

    result = subprocess.run(
        [
            sys.executable,
            str(CRM_PUBLIC_WEB_LIVE_VALIDATION_SCRIPT_PATH),
            "--execute-live",
            "--confirm-live-provider-cost",
            "--crm-record-id",
            "crm_record_for_guard_test",
            "--report-json",
            str(report_path),
        ],
        cwd=REPO_ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["responses"] == {}
    assert any(
        "CRM_PUBLIC_WEB_LIVE_PRE_AGENT_CONTRACT_PASSED=1" in failure
        for failure in report["guard_failures"]
    )


def test_w7g_live_runner_dry_run_reports_next_live_prerequisites(tmp_path: Path) -> None:
    report_path = tmp_path / "w7g_dry_run_report.json"

    result = subprocess.run(
        [
            sys.executable,
            str(CRM_PUBLIC_WEB_LIVE_VALIDATION_SCRIPT_PATH),
            "--report-json",
            str(report_path),
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "dry_run_ready"
    prerequisites = report["live_prerequisites"]
    assert prerequisites["ready_to_execute_live_with_current_args"] is False
    assert "CRM_PUBLIC_WEB_LIVE_RECORD_IDS" in prerequisites["missing_or_required_before_live"]
    assert "CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED=1" in prerequisites["missing_or_required_before_live"]
    assert "Use reviewed CRMRecord ids, not legacy target-candidate ids." in prerequisites["record_id_selection_guidance"]
    assert "make test-crm-public-web-live-product-validation" in prerequisites["recommended_make_command"]


def test_w7g_live_runner_requires_reviewed_crm_record_ids(tmp_path: Path) -> None:
    report_path = tmp_path / "w7g_live_report.json"
    env = os.environ.copy()
    env["LIVE_CONFIRM"] = "1"
    env["CRM_PUBLIC_WEB_LIVE_PRE_AGENT_CONTRACT_PASSED"] = "1"
    env.pop("SOURCING_EXTERNAL_PROVIDER_MODE", None)

    result = subprocess.run(
        [
            sys.executable,
            str(CRM_PUBLIC_WEB_LIVE_VALIDATION_SCRIPT_PATH),
            "--execute-live",
            "--confirm-live-provider-cost",
            "--crm-record-id",
            "crm_record_for_review_guard_test",
            "--report-json",
            str(report_path),
        ],
        cwd=REPO_ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["crm_record_ids_reviewed"] is False
    assert report["responses"] == {}
    assert any("--reviewed-crm-record-ids" in failure for failure in report["guard_failures"])


def test_w7g_live_runner_accepts_only_valid_go_independent_review_artifacts(tmp_path: Path, monkeypatch) -> None:
    runner = _load_crm_public_web_live_validation_module()
    review_dir = REPO_ROOT / "runtime" / "reviews"
    review_dir.mkdir(parents=True, exist_ok=True)
    go_artifact = review_dir / f"pytest_{tmp_path.name}_go.md"
    no_go_artifact = review_dir / f"pytest_{tmp_path.name}_no_go.md"
    missing_reasoning_artifact = review_dir / f"pytest_{tmp_path.name}_missing_reasoning_go.md"
    missing_service_tier_artifact = review_dir / f"pytest_{tmp_path.name}_missing_service_tier_go.md"
    unrelated_in_reviews_artifact = review_dir / f"pytest_{tmp_path.name}_unrelated_go.md"
    missing_scope_file_artifact = review_dir / f"pytest_{tmp_path.name}_missing_scope_file_go.md"
    unrelated_artifact = tmp_path / "unrelated_go.md"
    prompt_artifact = review_dir / f"pytest_{tmp_path.name}.prompt.md"
    artifacts = [
        go_artifact,
        no_go_artifact,
        missing_reasoning_artifact,
        missing_service_tier_artifact,
        unrelated_in_reviews_artifact,
        missing_scope_file_artifact,
        unrelated_artifact,
        prompt_artifact,
    ]
    go_artifact.write_text(
        "## Review Metadata\n\n"
        "- title: W7g CRM Public Web live product validation\n"
        "- base/ref: current working tree\n"
        "- reviewer_model: gpt-5.5\n"
        "- reviewer_reasoning_effort: xhigh\n"
        "- reviewer_service_tier: fast\n"
        "- timeout_seconds: 600\n"
        "- prompt_path: `runtime/reviews/pytest.prompt.md`\n"
        "- command: `codex exec --sandbox read-only ...`\n"
        "- contract_docs_considered: `docs/PRE_AGENT_CONTRACT_REVIEW.md`, `docs/INDEPENDENT_REVIEW_GATE.md`\n"
        "- author_validation: targeted tests\n"
        "- accepted_exceptions: none\n\n"
        "Reviewed scope:\n"
        "- `scripts/run_crm_public_web_live_product_validation.py`\n"
        "- `docs/PRE_AGENT_CONTRACT_REVIEW.md`\n\n"
        "## Reviewer Output\n\n- Reviewed scope: W7g CRM Public Web live validation\n\nGO\n",
        encoding="utf-8",
    )
    scoped_review_body = go_artifact.read_text(encoding="utf-8")
    no_go_artifact.write_text(
        scoped_review_body.rsplit("GO", 1)[0] + "1. `high` / `file.py:1` / issue\n\nNO-GO\n",
        encoding="utf-8",
    )
    missing_reasoning_artifact.write_text(
        scoped_review_body.replace("- reviewer_reasoning_effort: xhigh\n", ""),
        encoding="utf-8",
    )
    missing_service_tier_artifact.write_text(
        scoped_review_body.replace("- reviewer_service_tier: fast\n", ""),
        encoding="utf-8",
    )
    unrelated_in_reviews_artifact.write_text(
        "## Review Metadata\n\n"
        "- title: storage documentation review\n"
        "- base/ref: current working tree\n"
        "- reviewer_model: gpt-5.5\n"
        "- reviewer_reasoning_effort: xhigh\n"
        "- reviewer_service_tier: fast\n"
        "- timeout_seconds: 600\n"
        "- prompt_path: `runtime/reviews/unrelated.prompt.md`\n"
        "- command: `codex exec --sandbox read-only ...`\n"
        "- contract_docs_considered: `docs/OTHER_CONTRACT.md`\n"
        "- author_validation: targeted tests\n"
        "- accepted_exceptions: none\n\n"
        "Reviewed scope:\n"
        "- `docs/OTHER_CONTRACT.md`\n\n"
        "## Reviewer Output\n\nGO\n",
        encoding="utf-8",
    )
    missing_scope_file_artifact.write_text(
        "## Review Metadata\n\n"
        "- title: W7g CRM Public Web live review\n"
        "- base/ref: current working tree\n"
        "- reviewer_model: gpt-5.5\n"
        "- reviewer_reasoning_effort: xhigh\n"
        "- reviewer_service_tier: fast\n"
        "- timeout_seconds: 600\n"
        "- prompt_path: `runtime/reviews/missing-file.prompt.md`\n"
        "- command: `codex exec --sandbox read-only ...`\n"
        "- contract_docs_considered: `docs/OTHER_CONTRACT.md`\n"
        "- author_validation: targeted tests\n"
        "- accepted_exceptions: none\n\n"
        "Reviewed scope:\n"
        "- `docs/OTHER_CONTRACT.md`\n\n"
        "## Reviewer Output\n\nGO\n",
        encoding="utf-8",
    )
    unrelated_artifact.write_text(
        scoped_review_body,
        encoding="utf-8",
    )
    prompt_artifact.write_text(
        scoped_review_body,
        encoding="utf-8",
    )
    args = type(
        "Args",
        (),
        {
            "execute_live": True,
            "confirm_live_provider_cost": True,
            "crm_record_ids": ["crmrec_review_gate_test"],
            "reviewed_crm_record_ids": True,
            "max_fetches_per_candidate": 10,
            "max_ai_evidence_documents": 10,
            "max_ai_entry_links": 10,
            "max_remote_search_wait_seconds": 120,
            "max_provider_pending_wait_seconds": 120,
            "max_concurrent_candidate_analyses": 2,
        },
    )()

    try:
        monkeypatch.setenv("LIVE_CONFIRM", "1")
        monkeypatch.setenv("CRM_PUBLIC_WEB_LIVE_PRE_AGENT_CONTRACT_PASSED", "1")
        monkeypatch.setenv("CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED", "1")
        monkeypatch.delenv("SOURCING_EXTERNAL_PROVIDER_MODE", raising=False)

        monkeypatch.setenv(
            "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT",
            str(go_artifact.relative_to(REPO_ROOT)),
        )
        failures, _warnings = runner._guard_report(args)
        assert failures == []
        readiness = runner._live_prerequisites_report(args)
        assert readiness["ready_to_execute_live_with_current_args"] is True
        assert readiness["independent_review_artifact_validation"]["valid_go"] is True

        monkeypatch.setenv(
            "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT",
            str(no_go_artifact.relative_to(REPO_ROOT)),
        )
        failures, _warnings = runner._guard_report(args)
        assert any("artifact verdict is NO-GO" in failure for failure in failures)

        monkeypatch.setenv(
            "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT",
            str(missing_reasoning_artifact.relative_to(REPO_ROOT)),
        )
        failures, _warnings = runner._guard_report(args)
        assert any("reviewer_reasoning_effort" in failure for failure in failures)

        monkeypatch.setenv(
            "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT",
            str(missing_service_tier_artifact.relative_to(REPO_ROOT)),
        )
        failures, _warnings = runner._guard_report(args)
        assert any("reviewer_service_tier" in failure for failure in failures)

        monkeypatch.setenv(
            "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT",
            str(unrelated_in_reviews_artifact.relative_to(REPO_ROOT)),
        )
        failures, _warnings = runner._guard_report(args)
        assert any("does not match required live validation scope tokens" in failure for failure in failures)

        monkeypatch.setenv(
            "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT",
            str(missing_scope_file_artifact.relative_to(REPO_ROOT)),
        )
        failures, _warnings = runner._guard_report(args)
        assert any("does not cover required live validation files" in failure for failure in failures)

        monkeypatch.setenv("CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT", str(unrelated_artifact))
        failures, _warnings = runner._guard_report(args)
        assert any("must be under runtime/reviews" in failure for failure in failures)

        monkeypatch.setenv(
            "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT",
            str(prompt_artifact.relative_to(REPO_ROOT)),
        )
        failures, _warnings = runner._guard_report(args)
        assert any("points to a prompt file" in failure for failure in failures)
    finally:
        for artifact in artifacts:
            artifact.unlink(missing_ok=True)


def test_w7g_live_runner_skips_model_check_for_nonterminal_latest_run() -> None:
    runner = _load_crm_public_web_live_validation_module()
    failures: list[str] = []
    warnings: list[str] = []

    runner._validate_model_fields_for_terminal_detail(
        "detail:crmrec_waiting",
        {
            "status": "ok",
            "latest_run": {
                "run_id": "crm-public-web-run-waiting",
                "status": "searching",
            },
            "read_contract": {
                "fallback_used": False,
            },
        },
        expected_model="gpt-5.5",
        warnings=warnings,
        failures=failures,
    )

    assert failures == []
    assert any("model validation skipped because latest run status is searching" in warning for warning in warnings)

    runner._validate_model_fields_for_terminal_detail(
        "detail:crmrec_completed_without_model",
        {
            "status": "ok",
            "latest_run": {
                "run_id": "crm-public-web-run-completed",
                "status": "completed",
            },
            "read_contract": {
                "fallback_used": False,
            },
        },
        expected_model="gpt-5.5",
        warnings=warnings,
        failures=failures,
    )

    assert any("no model field was present" in failure for failure in failures)


def test_w7g_live_runner_fails_fast_on_http_error_payload() -> None:
    runner = _load_crm_public_web_live_validation_module()
    failures: list[str] = []

    failed = runner._append_http_failure(
        "start",
        {
            "status": "not_found",
            "http_status": 404,
            "reason": "crm_record_not_found",
            "missing_record_ids": ["crmrec_missing"],
        },
        failures,
    )

    assert failed is True
    assert any("start: endpoint returned non-success response" in failure for failure in failures)
    assert any("crm_record_not_found" in failure for failure in failures)


def test_person_and_company_asset_media_contract_tracks_unfinished_boundaries() -> None:
    person_doc = PERSON_ASSET_CONTRACT_PATH.read_text(encoding="utf-8")
    data_doc = DATA_ASSET_GOVERNANCE_PATH.read_text(encoding="utf-8")
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO
    collections_page = (REPO_ROOT / "frontend-demo" / "src" / "pages" / "CollectionsPage.tsx").read_text(
        encoding="utf-8"
    )
    collection_page = (REPO_ROOT / "frontend-demo" / "src" / "pages" / "CollectionPage.tsx").read_text(
        encoding="utf-8"
    )
    frontend_api = (REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts").read_text(encoding="utf-8")
    projection_crm_api_contracts = PROJECTION_CRM_API_CONTRACTS_PATH.read_text(encoding="utf-8")
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")
    testing_playbook = TESTING_PLAYBOOK_PATH.read_text(encoding="utf-8")
    asset_media_backfill_script = ASSET_MEDIA_BACKFILL_SCRIPT_PATH.read_text(encoding="utf-8")

    assert "PersonAsset(asset_type='avatar_media')" in person_doc
    assert "LinkedIn `avatar_url` values are provider-observed metadata only" in person_doc
    assert "`media.asset.cache` is the bounded media owner" in person_doc
    assert "media_asset_owner" in person_doc
    assert "media_summary" in person_doc
    assert "/api/media/backfill-person-avatars" in person_doc
    assert "/api/media/backfill-person-avatars" in review_doc
    assert "/api/media/backfill-person-avatars" in next_todo
    assert "/api/media/backfill-person-avatars" in API_PATH.read_text(encoding="utf-8")
    assert "/api/media/backfill-person-avatars" in projection_crm_api_contracts
    assert "/api/media/backfill-company-logos" in person_doc
    assert "/api/media/backfill-company-logos" in review_doc
    assert "/api/media/backfill-company-logos" in next_todo
    assert "/api/media/backfill-company-logos" in API_PATH.read_text(encoding="utf-8")
    assert "/api/media/backfill-company-logos" in projection_crm_api_contracts
    assert "/api/company-assets/backfill-public-web-assets" in person_doc
    assert "/api/company-assets/backfill-public-web-assets" in review_doc
    assert "/api/company-assets/backfill-public-web-assets" in next_todo
    assert "/api/company-assets/backfill-public-web-assets" in API_PATH.read_text(encoding="utf-8")
    assert "/api/company-assets/backfill-public-web-assets" in projection_crm_api_contracts
    assert "PersonAsset(asset_type='public_web_signal')" in person_doc
    assert "crm_public_web_signal_person_asset_sync_v1" in person_doc
    assert "/api/persons/backfill-public-web-signals" in person_doc
    assert "/api/persons/backfill-public-web-signals" in review_doc
    assert "/api/persons/backfill-public-web-signals" in next_todo
    assert "/api/persons/backfill-public-web-signals" in API_PATH.read_text(encoding="utf-8")
    assert "/api/persons/backfill-public-web-signals" in projection_crm_api_contracts
    assert "test_asset_backfill_http_routes_are_explicit_migration_paths" in projection_crm_api_contracts
    assert "asset_backfill_http_routes_are_explicit_migration_paths" in makefile
    assert "backfill_person_company_asset_media.py" in next_todo
    assert "asset-media-backfill-dry-run" in makefile
    assert "asset-media-backfill-apply" in makefile
    assert "ASSET_MEDIA_BACKFILL_REVIEWED" in makefile
    assert "ASSET_MEDIA_BACKFILL_RUN_NOW" in makefile
    assert "asset-media-backfill-dry-run" in testing_playbook
    assert "asset-media-backfill-apply" in testing_playbook
    assert "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only" in testing_playbook
    assert "ASSET_MEDIA_BACKFILL_REVIEWED=1" in testing_playbook
    assert "Makefile operator wrappers" in next_todo
    assert "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only" in asset_media_backfill_script
    assert "--apply requires --reviewed" in asset_media_backfill_script
    assert '"reviewed": bool(args.reviewed)' in asset_media_backfill_script
    for backfill_owner_method in (
        "backfill_public_web_signals_to_person_asset_layer",
        "backfill_person_avatar_media_assets_api",
        "backfill_company_logo_media_assets_api",
        "backfill_company_public_web_assets_to_company_asset_layer_api",
    ):
        backfill_owner_source = _class_method_source(backfill_owner_method)
        assert "operator_review_required" in backfill_owner_source, backfill_owner_method
        assert "apply_requires_reviewed" in backfill_owner_source, backfill_owner_method
        assert "normal_reader_repair" in backfill_owner_source, backfill_owner_method
    historical_backfill_routes = {
        "/api/persons/backfill-public-web-signals",
        "/api/company-assets/backfill-public-web-assets",
        "/api/media/backfill-person-avatars",
        "/api/media/backfill-company-logos",
    }
    frontend_normal_sources = [
        FRONTEND_API_TYPES_PATH.read_text(encoding="utf-8"),
        FRONTEND_API_SCHEMA_PATH.read_text(encoding="utf-8"),
        FRONTEND_API_ADAPTER_PATH.read_text(encoding="utf-8"),
        frontend_api,
    ]
    for route in historical_backfill_routes:
        for frontend_source in frontend_normal_sources:
            assert route not in frontend_source
    action_registry = DEFAULT_ACTION_REGISTRY.to_record()
    assert not any("backfill" in action_type or "repair" in action_type for action_type in action_registry)
    exposed_command_types = {
        command_type
        for action_record in action_registry.values()
        for command_type in list(action_record.get("allowed_workflow_command_types") or [])
    }
    assert MEDIA_ASSET_CACHE_COMMAND_TYPE not in exposed_command_types
    assert "backfill_public_web_signals_to_person_asset_layer" in asset_media_backfill_script
    assert "backfill_person_avatar_media_assets_api" in asset_media_backfill_script
    assert "backfill_company_logo_media_assets_api" in asset_media_backfill_script
    assert "backfill_company_public_web_assets_to_company_asset_layer_api" in asset_media_backfill_script
    assert "/api/media/assets/{asset_id}" in person_doc
    assert "PersonAsset.avatar_media" in SERVING_PROJECTION_READER_PATH.read_text(encoding="utf-8")
    assert "avatar_unavailable" in SERVING_PROJECTION_READER_PATH.read_text(encoding="utf-8")
    assert "list_person_assets_for_person_keys" in SERVING_PROJECTION_READER_PATH.read_text(encoding="utf-8")
    assert "pickCanonicalMediaSummaryAvatarUrl" in frontend_api
    assert "PersonAsset.avatar_media" in frontend_api
    assert "fallback_used" in frontend_api
    assert "CompanyAsset" in person_doc
    assert "CompanyEvidence" in person_doc
    assert "CompanyAssertion" in person_doc
    assert "Company asset/evidence/assertion" in review_doc
    assert "company_public_web_asset_runs" in review_doc
    assert "company_public_web_assets" in review_doc
    assert "media.asset.cache" in review_doc
    assert "media_asset_owner" in review_doc
    assert "/api/media/assets/{asset_id}" in review_doc
    assert "frontend initials are placeholders only" in review_doc
    assert "CompanyAsset.logo_media" in _class_method_source("_collection_company_media_contract")
    assert "CompanyAssetWriter" in _class_method_source("ingest_company_logo_from_profile_experience_api")
    assert "MEDIA_ASSET_CACHE_COMMAND_TYPE" in _class_method_source("_run_media_asset_cache_command")
    assert "media_asset_cache_owner_v1" in (
        REPO_ROOT / "src" / "sourcing_agent" / "media_asset_owner.py"
    ).read_text(encoding="utf-8")
    assert "company_asset_writer_v1" in (REPO_ROOT / "src" / "sourcing_agent" / "company_asset_writer.py").read_text(
        encoding="utf-8"
    )
    assert "Phase 5c: company asset overview with stable company media" in next_todo
    assert "Phase 7c: person media and Public Web asset cutover" in next_todo
    assert "person_evidence` EntityDeltas" in next_todo
    assert "Phase 7d: company asset/evidence/assertion foundation" in next_todo
    assert "logo_unavailable" in next_todo
    assert "公司 logo 必须是稳定 media asset" in data_doc
    assert "collectionLogoText(item)" in collections_page
    assert "collectionLogoText(assetEntry)" in collection_page
    assert "collectionLogoImageUrl(item)" in collections_page
    assert "collectionLogoImageUrl(assetEntry)" in collection_page
    assert "collectionInitials" not in collections_page
    assert "collectionInitials" not in collection_page


def test_durable_operation_tables_are_not_defined_in_sqlite_schema() -> None:
    storage_source = STORAGE_PATH.read_text(encoding="utf-8")
    live_pg_source = LIVE_PG_PATH.read_text(encoding="utf-8")

    for table_name in DURABLE_OPERATION_TABLES:
        sqlite_ddl = f"CREATE TABLE IF NOT EXISTS {table_name}"
        assert sqlite_ddl not in storage_source
        assert sqlite_ddl in live_pg_source

    sqlite_index_pattern = re.compile(
        r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+idx_("
        + "|".join(re.escape(table_name) for table_name in sorted(DURABLE_OPERATION_TABLES))
        + r")",
        re.IGNORECASE,
    )
    assert sqlite_index_pattern.search(storage_source) is None


def test_durable_operation_tables_remain_registered_pg_only() -> None:
    storage_source = STORAGE_PATH.read_text(encoding="utf-8")
    for table_name in DURABLE_OPERATION_TABLES:
        assert f'"{table_name}"' in storage_source
    assert "def _require_postgres_for_durable_runtime" in storage_source


def test_candidate_documents_fallback_is_migration_only_contract() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    storage_source = STORAGE_PATH.read_text(encoding="utf-8")
    candidate_artifacts_source = CANDIDATE_ARTIFACTS_PATH.read_text(encoding="utf-8")
    retrieval_source = RETRIEVAL_RUNTIME_PATH.read_text(encoding="utf-8")

    assert "SOURCING_ENABLE_LEGACY_CANDIDATE_DOCUMENTS_FALLBACK" in review_doc
    assert "must default off" in review_doc
    assert "normal projection/public-reader paths must pass `allow_candidate_documents_fallback=False`" in review_doc
    assert "default=False" in storage_source.split("def candidate_documents_fallback_enabled", 1)[1].split(
        "def _control_plane_postgres_should_prefer_read",
        1,
    )[0]
    assert "return False" in retrieval_source.split("def candidate_documents_fallback_enabled", 1)[1].split(
        "def bootstrap_candidate_store_enabled",
        1,
    )[0]
    assert "legacy_candidate_documents_fallback_enabled" in candidate_artifacts_source
    _assert_token_in_sourcing_agent_sources("allow_candidate_documents_fallback=False")


def test_projection_filter_scan_fallback_is_not_normal_public_reader_contract() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    canonical_doc = CANONICAL_PROJECTION_CONTRACT_PATH.read_text(encoding="utf-8")
    reader_source = SERVING_PROJECTION_READER_PATH.read_text(encoding="utf-8")

    for source in (review_doc, canonical_doc):
        assert "SOURCING_ALLOW_LEGACY_PROJECTION_FILTER_SCAN_FALLBACK" in source
        assert "projection_person_search_index_unavailable" in source
    assert "_legacy_projection_filter_scan_fallback_enabled" in reader_source
    fallback_function = reader_source.split("def _legacy_projection_filter_scan_fallback_enabled", 1)[1]
    assert "SOURCING_ALLOW_LEGACY_PROJECTION_FILTER_SCAN_FALLBACK" in fallback_function
    assert "filter_contract.fallback_used=true" in canonical_doc
    assert "must never become a normal Agent/UI path" in review_doc


def test_public_read_api_methods_do_not_repair_or_mutate_state() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    forbidden_mutation_tokens = (
        "backfill",
        "rebuild_",
        "upsert_",
        "update_",
        "write_",
        "plan_workflow_command",
        "repair",
    )
    orchestrator_read_blocks = {
        method_name: _class_method_source(method_name)
        for method_name in (
            "get_serving_projection_api",
            "get_serving_projection_candidate_page",
            "search_projection_person_index_api",
            "get_serving_projection_person_detail_api",
            "get_person_summary_api",
            "list_company_assets_api",
            "list_company_evidence_api",
            "list_company_assertions_api",
            "get_media_asset_content_api",
        )
    }
    projection_reader_blocks = {
        f"ServingProjectionReader.{method_name}": _class_method_source(
            method_name, class_name="ServingProjectionReader"
        )
        for method_name in (
            "get_projection",
            "get_projection_candidates",
            "get_projection_person_detail",
            "search_projection_person_index",
            "get_person_summary",
        )
    }
    # Former slice end-markers: keep bare existence so the read/write split
    # still fails loudly if these surfaces disappear entirely.
    for boundary_method in (
        "rebuild_projection_person_search_index_api",
        "get_projection_crm_state_api",
        "start_crm_record_public_web_search",
    ):
        find_class_method(boundary_method)

    assert "Public readers must not repair state" in review_doc
    assert "normal public/read APIs identify their source of truth and fail closed" in review_doc
    for method_name, block in {**orchestrator_read_blocks, **projection_reader_blocks}.items():
        for token in forbidden_mutation_tokens:
            assert token not in block, (method_name, token)


def test_migration_only_env_inventory_is_contract_visible() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    canonical_doc = CANONICAL_PROJECTION_CONTRACT_PATH.read_text(encoding="utf-8")
    crm_doc = CRM_STATE_CONTRACT_PATH.read_text(encoding="utf-8")
    durable_doc = (REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md").read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO
    api_source = API_PATH.read_text(encoding="utf-8")
    cli_source = CLI_PATH.read_text(encoding="utf-8")
    public_web_runtime_source = PUBLIC_WEB_RUNTIME_CORE_PATH.read_text(encoding="utf-8")

    env_contracts = {
        "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS": (
            review_doc,
            canonical_doc,
            crm_doc,
            durable_doc,
            api_source,
            public_web_runtime_source,
        ),
        "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_TO_CRM_SYNC": (
            review_doc,
            canonical_doc,
            crm_doc,
            public_web_runtime_source,
        ),
        "SOURCING_ALLOW_LEGACY_TARGET_CANDIDATE_EXPORT": (
            review_doc,
            canonical_doc,
            crm_doc,
            api_source,
        ),
        "SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS": (
            review_doc,
            canonical_doc,
            next_todo,
            api_source,
        ),
        "SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS": (
            review_doc,
            canonical_doc,
            api_source,
        ),
        "SOURCING_ENABLE_LEGACY_CANDIDATE_DOCUMENTS_FALLBACK": (
            review_doc,
            next_todo,
            STORAGE_PATH.read_text(encoding="utf-8"),
            CANDIDATE_ARTIFACTS_PATH.read_text(encoding="utf-8"),
        ),
        "SOURCING_ALLOW_LEGACY_PROJECTION_FILTER_SCAN_FALLBACK": (
            review_doc,
            canonical_doc,
            next_todo,
            SERVING_PROJECTION_READER_PATH.read_text(encoding="utf-8"),
        ),
        "SOURCING_ALLOW_MIGRATION_PROJECTION_FACET_LAYERING_OVERLAY": (
            review_doc,
            next_todo,
        ),
        "SOURCING_ENABLE_SQLITE_PROFILE_REGISTRY_FALLBACK": (
            review_doc,
        ),
    }
    for env_name, sources in env_contracts.items():
        assert env_name in review_doc, env_name
        for source in sources:
            assert env_name in source, env_name
    for env_name in (
        "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS",
        "SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS",
        "SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS",
    ):
        _assert_token_in_sourcing_agent_sources(env_name)

    assert "permanently ignored" in review_doc
    assert "migration/test compatibility only" in review_doc
    assert "retired documentation-only evidence" in review_doc
    assert "retired storage-env name" in review_doc
    assert "new legacy/fallback env" in review_doc
    assert "return False" in public_web_runtime_source.split(
        "def legacy_target_public_web_execution_enabled", 1
    )[1].split("def _is_legacy_target_public_web_owner", 1)[0]
    assert '"migration_override_status": "removed"' in api_source
    _assert_token_in_sourcing_agent_sources('"migration_override_status": "removed"')
    assert "from .public_web_search import" not in cli_source
    assert "run_target_candidate_public_web_experiment(" not in cli_source
    assert "legacy_target_candidate_public_web_experiment_retired" in cli_source


def test_migration_diagnostic_routes_are_not_frontend_or_agent_normal_surfaces() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    api_source = API_PATH.read_text(encoding="utf-8")
    operations_source = FRONTEND_OPERATIONS_PAGE_PATH.read_text(encoding="utf-8")
    frontend_normal_sources = {
        "contracts/frontend_api_contract.ts": FRONTEND_API_TYPES_PATH.read_text(encoding="utf-8"),
        "contracts/frontend_api_contract.schema.json": FRONTEND_API_SCHEMA_PATH.read_text(encoding="utf-8"),
        "contracts/frontend_api_adapter.ts": FRONTEND_API_ADAPTER_PATH.read_text(encoding="utf-8"),
        **_frontend_normal_source_texts(),
    }
    migration_routes = {
        "/api/migrations/legacy-public-web",
        "/api/migrations/legacy-result-endpoints",
    }

    assert "Migration diagnostics are operator-only surfaces" in review_doc
    for route in migration_routes:
        assert route in api_source
        assert route in review_doc
        for source_path, frontend_source in frontend_normal_sources.items():
            assert route not in frontend_source
            assert "/api/migrations" not in frontend_source, source_path
    assert "/api/migrations" not in operations_source


def test_backfill_routes_are_operator_only_not_frontend_or_agent_surfaces() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    api_source = API_PATH.read_text(encoding="utf-8")
    operations_source = FRONTEND_OPERATIONS_PAGE_PATH.read_text(encoding="utf-8")
    frontend_normal_sources = {
        "contracts/frontend_api_contract.ts": FRONTEND_API_TYPES_PATH.read_text(encoding="utf-8"),
        "contracts/frontend_api_contract.schema.json": FRONTEND_API_SCHEMA_PATH.read_text(encoding="utf-8"),
        "contracts/frontend_api_adapter.ts": FRONTEND_API_ADAPTER_PATH.read_text(encoding="utf-8"),
        **_frontend_normal_source_texts(),
    }
    operator_backfill_routes = {
        "/api/crm/backfill-target-candidates",
        "/api/crm/backfill-public-web-promotions",
        "/api/projections/backfill-from-job",
        "/api/projections/backfill-person-summary-views",
        "/api/projections/backfill-person-search-indexes",
        "/api/persons/backfill-raw-evidence-indexes",
        "/api/persons/backfill-public-web-signals",
        "/api/company-assets/backfill-public-web-assets",
        "/api/media/backfill-person-avatars",
        "/api/media/backfill-company-logos",
    }

    assert "All HTTP routes whose path contains `/backfill` are operator/migration surfaces" in review_doc
    for route in operator_backfill_routes:
        assert route in api_source, route
        for source_path, frontend_source in frontend_normal_sources.items():
            assert route not in frontend_source, (route, source_path)
            assert "/backfill" not in frontend_source, source_path
    assert "/backfill" not in operations_source

    action_registry = DEFAULT_ACTION_REGISTRY.to_record()
    for action_type, action_record in action_registry.items():
        assert "backfill" not in action_type
        assert "repair" not in action_type
        assert "backfill" not in str(action_record.get("operation_type") or "")
        assert "repair" not in str(action_record.get("operation_type") or "")


def test_w7f_crm_public_web_phase_commands_are_contract_visible() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    durable_doc = (REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md").read_text(encoding="utf-8")
    agent_doc = AGENT_OPERATION_PATH.read_text(encoding="utf-8")
    metrics_source = METRICS_PATH.read_text(encoding="utf-8")
    operation_source = OPERATION_RUNTIME_PATH.read_text(encoding="utf-8")
    durable_runtime_source = DURABLE_RUNTIME_PATH.read_text(encoding="utf-8")
    worker_daemon_source = WORKER_DAEMON_PATH.read_text(encoding="utf-8")
    cli_source = CLI_PATH.read_text(encoding="utf-8")

    for constant_name, command_type in CRM_PUBLIC_WEB_PHASE_COMMANDS.items():
        assert command_type in review_doc
        assert command_type in durable_doc
        assert constant_name in metrics_source

    assert "route-local worker creation" in durable_doc
    assert "route-local and worker-daemon Public Web execution are not normal paths" in review_doc
    assert "phase commands write `workflow_activity_runs`" in review_doc
    assert "must write a `workflow_activity_runs` row" in durable_doc
    assert "run-level `crm_public_web_run`" in review_doc
    assert "run-level `crm_public_web_run`" in durable_doc
    assert "document-level `public_web_document` deltas" in review_doc
    assert "document-level `public_web_document` deltas" in durable_doc
    assert "signal-level `public_web_signal` deltas" in review_doc
    assert "signal-level `public_web_signal` deltas" in durable_doc
    find_class_method("_record_crm_public_web_phase_entity_delta", class_name="CrmPublicWebOwner")
    find_class_method("_record_crm_public_web_document_entity_deltas", class_name="CrmPublicWebOwner")
    find_class_method("_record_crm_public_web_signal_entity_deltas", class_name="CrmPublicWebOwner")
    _assert_token_in_sourcing_agent_sources("activity_type=command_type")
    assert "owner-specific running cancel" in review_doc
    assert "owner-specific control first slice" in durable_doc
    assert "CRM Public Web phase commands delegate cancel and resume to `crm_public_web_owner`" in agent_doc
    assert "workflow_command_control_policy" in review_doc
    assert "workflow_command_control_policy" in durable_doc
    assert "worker-daemon Public Web execution" in review_doc
    assert "Worker-daemon recovery must not execute" in durable_doc
    assert "crm_public_web_agent_worker_recovery_retired" in worker_daemon_source
    assert "from .legacy_target_candidate_public_web_runtime import" not in worker_daemon_source
    assert "execute_crm_public_web_run_to_local_idle" not in worker_daemon_source
    assert "execute_target_candidate_public_web_run_to_local_idle" not in worker_daemon_source
    assert "legacy_target_candidate_public_web_experiment_retired" in cli_source
    assert "control_policy" in agent_doc
    assert "control_target" in review_doc
    assert "control_target" in durable_doc
    assert "control_target" in agent_doc
    assert "allowed_workflow_command_contracts" in agent_doc
    assert "owner_has_no_safe_inflight_interrupt" in review_doc
    assert "owner_has_no_safe_inflight_interrupt" in durable_doc
    assert "WorkflowCommandControlPolicy" in durable_runtime_source
    assert "workflow_command_control_policy" in durable_runtime_source
    assert "running_cancel_blocked_reason" in durable_runtime_source
    assert "running_cancel_prerequisites" in durable_runtime_source
    assert "running_cancel_upgrade_requirements" in review_doc
    assert "running_cancel_upgrade_requirements" in durable_doc
    assert "running_cancel_upgrade_requirements" in agent_doc
    assert "running_cancel_upgrade_requirements" in durable_runtime_source
    assert "running_resume_upgrade_requirements" in review_doc
    assert "running_resume_upgrade_requirements" in durable_doc
    assert "running_resume_upgrade_requirements" in durable_runtime_source
    assert "running_command_requires_owner_specific_resume" in review_doc
    assert "running_command_requires_owner_specific_resume" in durable_doc
    assert "workflow_service_metrics.durable_command_owner_contracts" in review_doc
    metrics_source = METRICS_PATH.read_text(encoding="utf-8")
    assert "workflow_command_control_policy" in metrics_source
    assert "control_policy_violation_count" in metrics_source
    assert "display_contract_violation_count" in metrics_source
    assert "activity_spine_policy_violation_count" in metrics_source
    assert "control_policy_violation_count" in review_doc
    assert "display_contract_violation_count" in review_doc
    assert "activity_spine_policy_violation_count" in review_doc
    assert '"control_policy"' in _class_method_source("_workflow_command_api_record", class_name="CommandKernel")
    assert '"allowed_workflow_command_contracts"' in operation_source
    assert "_workflow_command_contract_record" in operation_source
    find_class_method("_workflow_activity_control_target_record")
    assert "running_command_requires_owner_specific_cancel" in review_doc
    assert "running_command_requires_owner_specific_cancel" in agent_doc
    assert "Excel intake commands delegate cancel/resume to `excel_intake_owner`" in agent_doc
    assert "Orchestration, provider-attempt, and domain-mutation commands expose owner-specific running cancel/resume at safe checkpoints" in agent_doc
    assert "provider-attempt before provider EntityDelta/downstream evidence" in agent_doc
    assert "After those boundaries, cancellation remains fail-closed" in agent_doc
    crm_public_web_phase_cancel_source = _class_method_source(
        "_cancel_running_crm_public_web_phase_command", class_name="CrmPublicWebOwner"
    )
    assert "from_statuses=(\"claimed\", \"running\")" in crm_public_web_phase_cancel_source


def test_agent_action_registry_exposes_default_typed_command_surfaces() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    agent_doc = AGENT_OPERATION_PATH.read_text(encoding="utf-8")
    frontend_doc = FRONTEND_API_CONTRACT_DOC_PATH.read_text(encoding="utf-8")
    operation_source = OPERATION_RUNTIME_PATH.read_text(encoding="utf-8")

    for command_type in (
        "ACQUISITION_RUN_CREATE_COMMAND_TYPE",
        "LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE",
        "LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE",
        "CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE",
        "CRM_RECORD_UPDATE_COMMAND_TYPE",
        "CRM_NOTE_ADD_COMMAND_TYPE",
        "CRM_TASK_CREATE_COMMAND_TYPE",
        "COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE",
        "CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE",
        "EXPORT_PROJECTION_GENERATE_COMMAND_TYPE",
        "EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE",
        "EXCEL_INTAKE_RUN_COMMAND_TYPE",
    ):
        assert command_type in operation_source
    assert "`operation_runtime.ActionRegistry.to_record()` owns default/allowed typed command contracts" in review_doc
    assert "rejects missing display labels/categories/descriptions" in review_doc
    assert "unregistered commands, duplicate command types" in review_doc
    assert "HTTP/API handlers must expose that record" in review_doc
    assert "allowed_workflow_command_contracts" in operation_source
    assert "OperationActionDisplayContract" in operation_source
    assert "display_contract" in operation_source
    assert "operation_runtime.ActionRegistry.display_contract_for" in review_doc
    assert "operation_action.display_contract" in frontend_doc
    assert "cannot expose legacy/internal workflow command type" in operation_source
    assert "Implemented W10 registry coverage includes" in agent_doc


def test_agent_callable_commands_have_activity_spine_policy() -> None:
    owner_registry = DEFAULT_COMMAND_OWNER_REGISTRY.to_record()
    action_registry = DEFAULT_ACTION_REGISTRY.to_record()

    for command_type, owner in owner_registry.items():
        display_contract = workflow_command_display_contract(command_type, owner=owner).to_record()
        assert display_contract["display_label"], command_type
        assert display_contract["display_category"], command_type
        assert display_contract["source_of_truth"] == "durable_runtime.workflow_command_display_contract"
        assert display_contract["fallback_status"] == "fail_closed"
        policy = workflow_command_activity_spine_policy(command_type, owner=owner)
        record = policy.to_record()
        assert record["fallback_status"] == "fail_closed"
        assert record["source_of_truth"] == "durable_runtime.workflow_command_activity_spine_policy"
        assert record["requirement"]
        assert policy.requirement != ACTIVITY_SPINE_LEGACY_INTERNAL, (command_type, owner)
        assert policy.requirement != ACTIVITY_SPINE_CONTROL_PLANE, (command_type, owner)

    for action_type, action_record in action_registry.items():
        display_contract = dict(action_record.get("display_contract") or {})
        assert display_contract["display_label"], action_type
        assert display_contract["display_category"], action_type
        assert display_contract["description"], action_type
        assert display_contract["source_of_truth"] == "operation_runtime.ActionRegistry.display_contract_for"
        assert display_contract["fallback_status"] == "fail_closed"
        for command_type in list(action_record.get("allowed_workflow_command_types") or []):
            policy = workflow_command_activity_spine_policy(command_type)
            assert policy.requirement != ACTIVITY_SPINE_LEGACY_INTERNAL, (action_type, command_type)
            assert policy.agent_callable is True, (action_type, command_type)

    continue_commands = set(
        action_registry[ACTION_CONTINUE_ACQUISITION_RUN]["allowed_workflow_command_types"]
    )
    assert "linkedin.profile_refill.submit_batch" not in continue_commands
    assert "linkedin.profile_url_terminal.record" not in continue_commands
    assert "linkedin.local_profile_delta.apply" not in continue_commands
    assert "projection.board_visible_patch.publish" not in continue_commands
    assert "projection.facet_layering.build" not in continue_commands
    assert "snapshot.compaction.run" not in continue_commands
    for command_type in (
        "linkedin.discovery_query.run",
        "linkedin.profile_refill.submit_batch",
        "linkedin.profile_fetch.activity.run",
        "linkedin.profile_fetch.provider.fetch",
        "linkedin.profile_terminal.admit",
        "linkedin.profile_url_terminal.record",
        "linkedin.local_profile_delta.apply",
        "projection.profile_admission.apply",
        "projection.board_visible_patch.publish",
        "projection.run_scope.finalize",
        "projection.facet_layering.build",
        "snapshot.compaction.run",
    ):
        assert workflow_command_activity_spine_policy(command_type).requirement == ACTIVITY_SPINE_REQUIRED


def test_action_registry_allowlist_is_agent_command_exposure_gate() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    agent_doc = AGENT_OPERATION_PATH.read_text(encoding="utf-8")
    frontend_doc = FRONTEND_API_CONTRACT_DOC_PATH.read_text(encoding="utf-8")
    frontend_types = FRONTEND_API_TYPES_PATH.read_text(encoding="utf-8")
    frontend_adapter = FRONTEND_API_ADAPTER_PATH.read_text(encoding="utf-8")
    frontend_schema = FRONTEND_API_SCHEMA_PATH.read_text(encoding="utf-8")
    operation_source = OPERATION_RUNTIME_PATH.read_text(encoding="utf-8")
    durable_doc = (REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md").read_text(encoding="utf-8")
    action_registry = DEFAULT_ACTION_REGISTRY.to_record()
    owner_registry = DEFAULT_COMMAND_OWNER_REGISTRY.to_record()

    allowed_commands: set[str] = set()
    for action_type, action_record in action_registry.items():
        exposure_gate = action_record["workflow_command_exposure_gate"]
        assert exposure_gate == "operation_runtime.ActionRegistry.allowed_workflow_command_types", action_type
        allowed_types = list(action_record.get("allowed_workflow_command_types") or [])
        expected_status = "action_registry_allowlisted" if allowed_types else "no_workflow_command_surface"
        assert action_record["workflow_command_exposure_status"] == expected_status, action_type
        control_summary = dict(action_record.get("workflow_command_control_summary") or {})
        assert control_summary["source_of_truth"] == "operation_runtime.ActionRegistry.allowed_workflow_command_contracts"
        assert control_summary["fallback_status"] == "fail_closed"
        assert control_summary["command_count"] == len(allowed_types)
        assert "agent_ui_guidance" in control_summary
        for command_contract in list(action_record.get("allowed_workflow_command_contracts") or []):
            command_type = str(command_contract.get("command_type") or "")
            allowed_commands.add(command_type)
            assert command_contract["agent_exposure_gate"] == exposure_gate, (action_type, command_type)
            assert command_contract["agent_exposure_status"] == "action_registry_allowlisted", (
                action_type,
                command_type,
            )
            assert command_type in owner_registry, (action_type, command_type)

    assert allowed_commands
    assert "linkedin.profile_refill.submit_batch" not in allowed_commands
    assert "projection.board_visible_patch.publish" not in allowed_commands
    assert "snapshot.compaction.run" not in allowed_commands
    assert "WORKFLOW_COMMAND_EXPOSURE_GATE_SOURCE" in operation_source
    assert "WORKFLOW_COMMAND_EXPOSURE_STATUS_ALLOWLISTED" in operation_source
    assert "ActionRegistry allowlist" in review_doc
    assert "activity_spine_policy.agent_callable" in review_doc
    assert "necessary but not sufficient" in review_doc
    assert "ActionRegistry allowlist" in agent_doc
    assert "only normal Agent exposure gate" in agent_doc
    assert "not_action_registry_allowlisted" in _class_method_source(
        "_workflow_command_agent_exposure_record", class_name="CommandKernel"
    )
    assert "Workflow command exposure must be explicit on both registry and concrete command rows" in review_doc
    assert "not_action_registry_allowlisted" in review_doc
    assert "not_action_registry_allowlisted" in durable_doc
    for source in (frontend_doc, frontend_types, frontend_adapter, frontend_schema):
        assert "workflow_command_control_summary" in source
        assert "workflow_command_exposure_gate" in source
        assert "workflow_command_exposure_status" in source
        assert "agent_exposure_gate" in source
        assert "agent_exposure_status" in source
        assert "running_control_category" in source
        assert "running_control_categories" in source
        assert "running_control_maturity" in source
        assert "running_control_gap_status" in source
        assert "running_control_surface" in source
    assert "agentExposureStatus" in (REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts").read_text(
        encoding="utf-8"
    )
    frontend_api = (REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts").read_text(encoding="utf-8")
    operations_source = FRONTEND_OPERATIONS_PAGE_PATH.read_text(encoding="utf-8")
    assert "runningControlCategory" in frontend_api
    assert "runningControlCategories" in frontend_api
    assert "runningControlMaturity" in frontend_api
    assert "runningControlGapStatus" in frontend_api
    assert "runningControlSurface" in frontend_api
    assert "commandControlPolicySummary" in operations_source
    assert "Control category:" in operations_source
    assert "not_action_registry_allowlisted" in frontend_doc


def test_action_registry_allowlist_status_is_total_over_owner_registry() -> None:
    action_registry = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=True)
    owner_registry = DEFAULT_COMMAND_OWNER_REGISTRY.to_record()

    allowed_commands = {
        str(command_type or "").strip()
        for action_record in action_registry.values()
        for command_type in list(action_record.get("allowed_workflow_command_types") or [])
        if str(command_type or "").strip()
    }
    action_contract_commands = {
        str(command_contract.get("command_type") or "").strip()
        for action_record in action_registry.values()
        for command_contract in list(action_record.get("allowed_workflow_command_contracts") or [])
        if str(command_contract.get("command_type") or "").strip()
    }

    assert action_contract_commands == allowed_commands
    assert allowed_commands <= set(owner_registry)
    assert owner_registry.keys() - allowed_commands
    for command_type in owner_registry:
        expected_status = (
            "action_registry_allowlisted"
            if command_type in allowed_commands
            else "not_action_registry_allowlisted"
        )
        matching_contracts = [
            command_contract
            for action_record in action_registry.values()
            for command_contract in list(action_record.get("allowed_workflow_command_contracts") or [])
            if str(command_contract.get("command_type") or "").strip() == command_type
        ]
        if expected_status == "action_registry_allowlisted":
            assert matching_contracts, command_type
            assert all(
                command_contract.get("agent_exposure_status") == expected_status
                and command_contract.get("agent_exposure_gate")
                == "operation_runtime.ActionRegistry.allowed_workflow_command_types"
                for command_contract in matching_contracts
            ), command_type
        else:
            assert not matching_contracts, command_type


def test_activity_spine_query_rows_are_explicitly_read_only_evidence() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    frontend_doc = FRONTEND_API_CONTRACT_DOC_PATH.read_text(encoding="utf-8")
    types_source = FRONTEND_API_TYPES_PATH.read_text(encoding="utf-8")
    schema_source = FRONTEND_API_SCHEMA_PATH.read_text(encoding="utf-8")
    adapter_source = FRONTEND_API_ADAPTER_PATH.read_text(encoding="utf-8")
    frontend_api = (REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts").read_text(encoding="utf-8")

    assert "Activity/Attempt/EntityDelta/Lane rows are read-only evidence" in review_doc
    assert "module_state_mutated=false" in frontend_doc
    assert "read_only_activity_evidence" in _class_method_source("_workflow_activity_api_record")
    assert "read_only_activity_attempt_evidence" in _class_method_source("_workflow_activity_attempt_api_record")
    assert "read_only_entity_delta_evidence" in _class_method_source("_workflow_entity_delta_api_record")
    assert "read_only_domain_read_model" in _class_method_source("_acquisition_discovery_lane_api_record")
    for source in (types_source, schema_source, adapter_source):
        assert "mutation_contract" in source
        assert "module_state_mutated" in source
    assert "mutationContract" in frontend_api
    assert "moduleStateMutated" in frontend_api


def test_owner_registry_running_command_control_is_explicit() -> None:
    owner_registry = DEFAULT_COMMAND_OWNER_REGISTRY.to_record()

    for command_type, owner in owner_registry.items():
        categories = workflow_command_running_control_categories(command_type)
        assert len(categories) == 1, (command_type, categories)
        policy = workflow_command_control_policy(command_type=command_type, owner=owner)
        record = policy.to_record()
        assert record["fallback_status"] == "fail_closed"
        assert record["control_source_of_truth"] == "durable_runtime.workflow_command_control_policy"
        assert record["running_control_category"] == categories[0], command_type
        assert record["running_control_categories"] == list(categories), command_type
        assert record["running_control_surface"] == "workflow_command_control_api_only", command_type
        assert record["running_control_maturity"] in {
            "owner_specific_cancel_resume",
            "owner_specific_cancel_only",
            "owner_specific_resume_only",
            "fail_closed_with_upgrade_requirements",
        }, command_type
        assert record["running_control_gap_status"] in {
            "closed",
            "partial_resume_gap_reported",
            "partial_cancel_gap_reported",
            "accepted_fail_closed_pending_owner_specific_control",
        }, command_type
        queued_state = workflow_command_control_state(
            command_status="queued",
            command_type=command_type,
            owner=owner,
        ).to_record()
        assert queued_state["control_source_of_truth"] == "durable_runtime.workflow_command_control_state"
        assert queued_state["policy_source_of_truth"] == "durable_runtime.workflow_command_control_policy"
        assert queued_state["fallback_status"] == "fail_closed"
        assert queued_state["can_cancel"] is True
        assert queued_state["cancel_mode"] == "generic"
        assert "cancel" in queued_state["allowed_actions"]
        retry_wait_state = workflow_command_control_state(
            command_status="retry_wait",
            command_type=command_type,
            owner=owner,
        ).to_record()
        assert retry_wait_state["can_cancel"] is True
        assert retry_wait_state["can_resume"] is True
        assert retry_wait_state["resume_mode"] == "generic"
        assert {"cancel", "resume"}.issubset(set(retry_wait_state["allowed_actions"]))
        failed_state = workflow_command_control_state(
            command_status="failed_terminal",
            command_type=command_type,
            owner=owner,
        ).to_record()
        assert failed_state["can_retry"] is True
        assert failed_state["retry_mode"] == "generic"
        if policy.running_cancel_supported:
            running_state = workflow_command_control_state(
                command_status="running",
                command_type=command_type,
                owner=owner,
            ).to_record()
            assert running_state["can_cancel"] is True, command_type
            assert running_state["cancel_mode"] == "owner_specific", command_type
            if policy.running_resume_supported:
                assert running_state["can_resume"] is True, command_type
                assert running_state["resume_mode"] == "owner_specific", command_type
                assert running_state["running_resume_delegate"], command_type
                assert "resume" in running_state["allowed_actions"], command_type
            else:
                assert running_state["can_resume"] is False, command_type
                assert running_state["disabled_reasons"]["resume"] == "running_command_requires_owner_specific_resume"
                assert running_state["disabled_reasons"]["running_resume"] == policy.running_resume_blocked_reason
            assert running_state["module_state_mutated_on_cancel"] is True, command_type
            assert policy.running_cancel_statuses == ("claimed", "running"), command_type
            assert policy.running_cancel_delegate, command_type
            assert not policy.running_cancel_blocked_reason, command_type
            if policy.running_resume_supported:
                assert record["running_control_maturity"] == "owner_specific_cancel_resume", command_type
                assert record["running_control_gap_status"] == "closed", command_type
                assert policy.running_resume_statuses == ("claimed", "running"), command_type
                assert policy.running_resume_delegate, command_type
                assert not policy.running_resume_blocked_reason, command_type
            else:
                assert record["running_control_maturity"] == "owner_specific_cancel_only", command_type
                assert record["running_control_gap_status"] == "partial_resume_gap_reported", command_type
                assert policy.running_resume_blocked_reason, command_type
                assert policy.running_resume_blocked_reason != "owner_specific_resume_not_implemented", command_type
                assert policy.running_resume_upgrade_requirements, command_type
            continue
        running_state = workflow_command_control_state(
            command_status="running",
            command_type=command_type,
            owner=owner,
        ).to_record()
        assert running_state["can_cancel"] is False, command_type
        if policy.running_resume_supported:
            assert running_state["can_resume"] is True, command_type
            assert running_state["resume_mode"] == "owner_specific", command_type
            assert running_state["running_resume_delegate"], command_type
            assert "resume" in running_state["allowed_actions"], command_type
            assert record["running_control_maturity"] == "owner_specific_resume_only", command_type
            assert record["running_control_gap_status"] == "partial_cancel_gap_reported", command_type
            assert policy.running_resume_statuses == ("claimed", "running"), command_type
            assert policy.running_resume_delegate, command_type
            assert not policy.running_resume_blocked_reason, command_type
            assert policy.running_cancel_blocked_reason, command_type
            assert policy.running_cancel_blocked_reason != "owner_specific_interrupt_not_implemented", command_type
            assert policy.running_cancel_upgrade_requirements, command_type
            continue
        assert running_state["can_resume"] is False, command_type
        assert record["running_control_maturity"] == "fail_closed_with_upgrade_requirements", command_type
        assert record["running_control_gap_status"] == "accepted_fail_closed_pending_owner_specific_control", command_type
        assert running_state["disabled_reasons"]["cancel"] == "running_command_requires_owner_specific_cancel"
        assert running_state["disabled_reasons"]["resume"] == "running_command_requires_owner_specific_resume"
        assert policy.running_cancel_blocked_reason, command_type
        assert policy.running_cancel_blocked_reason != "owner_specific_interrupt_not_implemented", command_type
        assert policy.running_cancel_upgrade_requirements, command_type
        assert policy.unsupported_running_cancel_reason == "running_command_requires_owner_specific_cancel"
        assert policy.running_resume_blocked_reason, command_type
        assert policy.running_resume_blocked_reason != "owner_specific_resume_not_implemented", command_type
        assert policy.running_resume_upgrade_requirements, command_type
        assert policy.unsupported_running_resume_reason == "running_command_requires_owner_specific_resume"

    excel_policy = workflow_command_control_policy(
        command_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
        owner=owner_registry[EXCEL_INTAKE_RUN_COMMAND_TYPE],
    )
    assert excel_policy.running_cancel_supported is True
    assert excel_policy.running_cancel_delegate == "excel_intake_owner.cancel_excel_intake_run_command"
    assert excel_policy.running_cancel_prerequisites == (
        "workflow_command_status_claimed_or_running",
        "payload.job_id",
        "thread_terminal_checkpoint",
    )
    assert excel_policy.running_resume_supported is True
    assert excel_policy.running_resume_delegate == "excel_intake_owner.resume_excel_intake_run_command"
    assert excel_policy.running_resume_prerequisites == (
        "workflow_command_status_claimed_or_running",
        "payload.job_id",
        "command_lease_expired_or_force",
    )
    assert excel_policy.to_record()["running_control_maturity"] == "owner_specific_cancel_resume"
    assert excel_policy.to_record()["running_control_gap_status"] == "closed"
    for command_type, expected_cancel_delegate, expected_resume_delegate in (
        (
            EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
            "projection_exporter.cancel_export_command",
            "projection_exporter.resume_export_command",
        ),
        (
            EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
            "crm_public_web_exporter.cancel_export_command",
            "crm_public_web_exporter.resume_export_command",
        ),
    ):
        policy = workflow_command_control_policy(command_type=command_type, owner=owner_registry[command_type])
        assert policy.running_cancel_supported is True
        assert policy.running_cancel_delegate == expected_cancel_delegate
        assert policy.running_cancel_prerequisites == (
            "workflow_command_status_claimed_or_running",
            "artifact_not_published",
        )
        assert policy.running_resume_supported is True
        assert policy.running_resume_delegate == expected_resume_delegate
        assert policy.running_resume_prerequisites == (
            "workflow_command_status_claimed_or_running",
            "artifact_not_published",
            "command_lease_expired_or_force",
        )
    for command_type in CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES:
        phase_policy = workflow_command_control_policy(
            command_type=command_type,
            owner=owner_registry[command_type],
        )
        assert phase_policy.running_cancel_supported is True, command_type
        assert phase_policy.running_cancel_delegate == "crm_public_web_owner.cancel_crm_public_web_run", command_type
        assert phase_policy.running_resume_supported is True, command_type
        assert phase_policy.running_resume_delegate == "crm_public_web_owner.resume_crm_public_web_phase_command", command_type
        assert phase_policy.running_resume_prerequisites == (
            "payload.run_id",
            "crm_public_web_run_exists",
            "command_lease_expired_or_force",
        ), command_type
        assert phase_policy.to_record()["running_control_maturity"] == "owner_specific_cancel_resume", command_type
        assert phase_policy.to_record()["running_control_gap_status"] == "closed", command_type
        phase_record = phase_policy.to_record()
        if command_type in {CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE, CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE}:
            assert phase_record["provider_after_start_control_status"] == "active", command_type
            assert phase_record["provider_after_start_control_mode"] == "poll_cancel_late_result_quarantine", command_type
            assert phase_record["provider_after_start_control_blocked_reason"] == "", command_type
        elif command_type in {
            CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
            CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
            CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
        }:
            assert phase_record["provider_after_start_control_status"] == "active", command_type
            assert phase_record["provider_after_start_control_mode"] == "fail_closed_until_terminal", command_type
            assert phase_record["provider_after_start_control_blocked_reason"] == (
                "after_start_control_deliberately_fail_closed_until_terminal"
            ), command_type
        elif command_type == CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE:
            assert phase_record["provider_after_start_control_status"] == "not_applicable", command_type
            assert phase_record["provider_after_start_control_mode"] == "not_applicable", command_type
    for command_type in ORCHESTRATION_COMMAND_TYPES:
        orchestration_policy = workflow_command_control_policy(
            command_type=command_type,
            owner=owner_registry[command_type],
        )
        assert orchestration_policy.to_record()["running_control_category"] == "orchestration", command_type
        if command_type in {
            ACQUISITION_RUN_CREATE_COMMAND_TYPE,
            ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
            ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
            COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
        }:
            assert orchestration_policy.to_record()["running_control_maturity"] == "owner_specific_cancel_resume"
            assert orchestration_policy.to_record()["running_control_gap_status"] == "closed"
            assert orchestration_policy.running_cancel_supported is True
            assert (
                orchestration_policy.running_cancel_delegate
                == "workflow_orchestrator.cancel_orchestration_before_downstream"
            )
            assert orchestration_policy.running_cancel_prerequisites == (
                "workflow_command_status_claimed_or_running",
                "no_downstream_command_planned",
                "command_lease_expired_or_force",
            )
            assert orchestration_policy.running_resume_supported is True
            assert (
                orchestration_policy.running_resume_delegate
                == "workflow_orchestrator.resume_orchestration_command"
            )
            continue
        if command_type == ACQUISITION_PLAN_COMMIT_COMMAND_TYPE:
            assert orchestration_policy.to_record()["running_control_maturity"] == "owner_specific_cancel_resume"
            assert orchestration_policy.to_record()["running_control_gap_status"] == "closed"
            assert orchestration_policy.running_cancel_supported is True
            assert (
                orchestration_policy.running_cancel_delegate
                == "workflow_orchestrator.cancel_acquisition_plan_commit_before_probe"
            )
            assert orchestration_policy.running_cancel_prerequisites == (
                "workflow_command_status_claimed_or_running",
                "no_probe_command_planned",
                "command_lease_expired_or_force",
                "acquisition_run_cancel_checkpoint",
            )
            assert orchestration_policy.running_resume_supported is True
            assert (
                orchestration_policy.running_resume_delegate
                == "workflow_orchestrator.resume_orchestration_command"
            )
            continue
        if command_type == ACQUISITION_SCALE_PLAN_COMMAND_TYPE:
            assert orchestration_policy.to_record()["running_control_maturity"] == "owner_specific_cancel_resume"
            assert orchestration_policy.to_record()["running_control_gap_status"] == "closed"
            assert orchestration_policy.running_cancel_supported is True
            assert (
                orchestration_policy.running_cancel_delegate
                == "workflow_orchestrator.cancel_acquisition_scale_plan_before_discovery"
            )
            assert orchestration_policy.running_cancel_prerequisites == (
                "workflow_command_status_claimed_or_running",
                "no_discovery_command_planned",
                "no_activity_attempt_started",
                "command_lease_expired_or_force",
                "activity_lane_cancel_checkpoint",
            )
            assert orchestration_policy.running_resume_supported is True
            assert (
                orchestration_policy.running_resume_delegate
                == "workflow_orchestrator.resume_orchestration_command"
            )
            continue
        if command_type == CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE:
            assert orchestration_policy.to_record()["running_control_maturity"] == "owner_specific_cancel_resume"
            assert orchestration_policy.to_record()["running_control_gap_status"] == "closed"
            assert orchestration_policy.running_cancel_supported is True
            assert (
                orchestration_policy.running_cancel_delegate
                == "crm_public_web_owner.cancel_queue_batch_before_phase_commands"
            )
            assert orchestration_policy.running_cancel_prerequisites == (
                "workflow_command_status_claimed_or_running",
                "no_phase_command_planned",
                "command_lease_expired_or_force",
                "batch_run_cancel_checkpoint",
            )
            assert orchestration_policy.running_resume_supported is True
            assert (
                orchestration_policy.running_resume_delegate
                == "workflow_orchestrator.resume_orchestration_command"
            )
            continue
        if command_type == ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE:
            assert orchestration_policy.to_record()["running_control_maturity"] == "owner_specific_cancel_resume"
            assert orchestration_policy.to_record()["running_control_gap_status"] == "closed"
            assert orchestration_policy.running_cancel_supported is True
            assert (
                orchestration_policy.running_cancel_delegate
                == "workflow_orchestrator.cancel_acquisition_plan_review_request"
            )
            assert orchestration_policy.running_cancel_prerequisites == (
                "workflow_command_status_claimed_or_running",
                "no_approved_plan_review_session",
                "plan_review_session_cancel_checkpoint",
            )
            assert orchestration_policy.running_resume_supported is True
            assert (
                orchestration_policy.running_resume_delegate
                == "workflow_orchestrator.resume_orchestration_command"
            )
            continue
        assert orchestration_policy.to_record()["running_control_maturity"] == "owner_specific_resume_only", command_type
        assert orchestration_policy.to_record()["running_control_gap_status"] == "partial_cancel_gap_reported", command_type
        assert orchestration_policy.running_cancel_supported is False, command_type
        assert orchestration_policy.running_resume_supported is True, command_type
        assert (
            orchestration_policy.running_resume_delegate
            == "workflow_orchestrator.resume_orchestration_command"
        ), command_type
        assert orchestration_policy.running_resume_prerequisites == (
            "workflow_command_status_claimed_or_running",
            "command_lease_expired_or_force",
            "idempotent_downstream_command_keys",
        ), command_type
    for command_type in PROVIDER_ATTEMPT_COMMAND_TYPES:
        provider_policy = workflow_command_control_policy(
            command_type=command_type,
            owner=owner_registry[command_type],
        )
        assert provider_policy.to_record()["running_control_category"] == "provider_attempt", command_type
        assert provider_policy.to_record()["running_control_maturity"] == "owner_specific_cancel_resume", command_type
        assert provider_policy.to_record()["running_control_gap_status"] == "closed", command_type
        assert provider_policy.to_record()["provider_after_start_control_contract"] == (
            "w11_provider_after_start_control_v1"
        ), command_type
        assert provider_policy.to_record()["provider_after_start_control_status"] == "active", command_type
        assert provider_policy.to_record()["provider_after_start_control_mode"] == (
            "poll_cancel_late_result_quarantine"
        ), command_type
        assert provider_policy.to_record()["provider_after_start_control_blocked_reason"] == "", command_type
        assert provider_policy.to_record()["provider_after_start_control_upgrade_requirements"] == [], command_type
        assert provider_policy.to_record()["module_state_mutated_on_provider_after_start_control"] is False, command_type
        assert provider_policy.running_cancel_supported is True, command_type
        assert provider_policy.running_cancel_prerequisites == (
            "workflow_command_status_claimed_or_running",
            "provider_activity_attempt_absent_or_poll_cancel_quarantine",
            "no_provider_entity_delta_recorded",
            "no_downstream_command_planned",
            "command_lease_expired_or_force",
        ), command_type
        assert provider_policy.running_resume_supported is True, command_type
        if command_type == COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE:
            assert (
                provider_policy.running_cancel_delegate
                == "company_public_web_owner.cancel_or_poll_stop_source_collect"
            )
            assert provider_policy.running_resume_delegate == "company_public_web_owner.resume_source_collect"
            assert provider_policy.running_resume_prerequisites == (
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "canonical_asset_sync_deferred",
            )
        else:
            assert (
                provider_policy.running_cancel_delegate
                == "workflow_provider_owner.cancel_or_poll_stop_provider_attempt"
            ), command_type
            assert (
                provider_policy.running_resume_delegate
                == "workflow_provider_owner.resume_provider_attempt_command"
            ), command_type
            assert provider_policy.running_resume_prerequisites == (
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_provider_request_key",
            ), command_type
    media_policy = workflow_command_control_policy(
        command_type=MEDIA_ASSET_CACHE_COMMAND_TYPE,
        owner=owner_registry[MEDIA_ASSET_CACHE_COMMAND_TYPE],
    )
    media_record = media_policy.to_record()
    assert media_record["running_control_category"] == "domain_mutation"
    assert media_record["running_control_maturity"] == "owner_specific_cancel_resume"
    assert media_record["running_control_gap_status"] == "closed"
    assert media_policy.running_cancel_supported is True
    assert media_policy.running_cancel_delegate == "media_asset_owner.cancel_before_fetch_upload_attempt"
    assert media_policy.running_cancel_prerequisites == (
        "workflow_command_status_claimed_or_running",
        "no_media_asset_activity_attempt_started",
        "no_media_asset_entity_delta_recorded",
        "no_downstream_command_planned",
        "command_lease_expired_or_force",
    )
    assert media_policy.running_resume_supported is True
    assert media_policy.running_resume_delegate == "media_asset_owner.resume_media_asset_cache"
    assert media_policy.running_resume_prerequisites == (
        "workflow_command_status_claimed_or_running",
        "command_lease_expired_or_force",
        "idempotent_asset_id_or_content_hash",
    )
    media_running_state = workflow_command_control_state(
        command_status="running",
        command_type=MEDIA_ASSET_CACHE_COMMAND_TYPE,
        owner=owner_registry[MEDIA_ASSET_CACHE_COMMAND_TYPE],
    ).to_record()
    assert media_running_state["can_cancel"] is True
    assert media_running_state["can_resume"] is True
    assert media_running_state["cancel_mode"] == "owner_specific"
    assert media_running_state["resume_mode"] == "owner_specific"
    for command_type in (
        "crm.record.add_from_projection",
        "crm.record.update",
        "crm.note.add",
        "crm.task.create",
    ):
        crm_policy = workflow_command_control_policy(
            command_type=command_type,
            owner=owner_registry[command_type],
        )
        crm_record = crm_policy.to_record()
        assert crm_record["running_control_category"] == "domain_mutation"
        assert crm_record["running_control_maturity"] == "owner_specific_cancel_resume"
        assert crm_record["running_control_gap_status"] == "closed"
        assert crm_policy.running_cancel_supported is True
        assert crm_policy.running_cancel_delegate == "crm_writer.cancel_before_mutation_attempt"
        assert crm_policy.running_cancel_prerequisites == (
            "workflow_command_status_claimed_or_running",
            "no_crm_writer_activity_attempt_started",
            "no_crm_writer_entity_delta_recorded",
            "no_downstream_command_planned",
            "command_lease_expired_or_force",
        )
        assert crm_policy.running_resume_supported is True
        assert crm_policy.running_resume_delegate == "crm_writer.resume_crm_writer_command"
        assert crm_policy.running_resume_prerequisites == (
            "workflow_command_status_claimed_or_running",
            "command_lease_expired_or_force",
            "idempotent_crm_write_key",
        )
    for command_type in DOMAIN_MUTATION_COMMAND_TYPES:
        if command_type in {
            LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
            MEDIA_ASSET_CACHE_COMMAND_TYPE,
            COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
            "crm.record.add_from_projection",
            "crm.record.update",
            "crm.note.add",
            "crm.task.create",
        }:
            continue
        domain_policy = workflow_command_control_policy(
            command_type=command_type,
            owner=owner_registry[command_type],
        )
        domain_record = domain_policy.to_record()
        assert domain_record["running_control_category"] == "domain_mutation", command_type
        assert domain_record["running_control_maturity"] == "owner_specific_cancel_resume", command_type
        assert domain_record["running_control_gap_status"] == "closed", command_type
        assert domain_policy.running_cancel_supported is True, command_type
        assert (
            domain_policy.running_cancel_delegate
            == "workflow_domain_owner.cancel_before_domain_mutation_attempt"
        ), command_type
        assert domain_policy.running_cancel_prerequisites == (
            "workflow_command_status_claimed_or_running",
            "no_domain_mutation_activity_attempt_started",
            "no_domain_mutation_entity_delta_recorded",
            "no_downstream_command_planned",
            "command_lease_expired_or_force",
        ), command_type
        assert domain_policy.running_resume_supported is True, command_type
        assert (
            domain_policy.running_resume_delegate
            == "workflow_domain_owner.resume_domain_mutation_command"
        ), command_type
        assert domain_policy.running_resume_prerequisites == (
            "workflow_command_status_claimed_or_running",
            "command_lease_expired_or_force",
            "idempotent_domain_effect_key",
        ), command_type
    profile_fetch_activity_policy = workflow_command_control_policy(
        command_type=LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
        owner=owner_registry[LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE],
    )
    profile_fetch_activity_record = profile_fetch_activity_policy.to_record()
    assert profile_fetch_activity_record["running_control_category"] == "domain_mutation"
    assert profile_fetch_activity_record["running_control_maturity"] == "owner_specific_cancel_resume"
    assert profile_fetch_activity_record["running_control_gap_status"] == "closed"
    assert profile_fetch_activity_policy.running_cancel_supported is True
    assert (
        profile_fetch_activity_policy.running_cancel_delegate
        == "linkedin_profile_activity_owner.cancel_before_cache_lookup_attempt"
    )
    assert profile_fetch_activity_policy.running_cancel_prerequisites == (
        "workflow_command_status_claimed_or_running",
        "no_activity_attempt_started",
        "no_profile_entity_delta_recorded",
        "no_downstream_command_planned",
        "command_lease_expired_or_force",
    )
    assert profile_fetch_activity_policy.running_resume_supported is True
    assert (
        profile_fetch_activity_policy.running_resume_delegate
        == "workflow_domain_owner.resume_domain_mutation_command"
    )
    company_public_web_policy = workflow_command_control_policy(
        command_type=COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
        owner=owner_registry[COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE],
    )
    company_public_web_record = company_public_web_policy.to_record()
    assert company_public_web_record["running_control_category"] == "orchestration"
    assert company_public_web_record["running_control_maturity"] == "owner_specific_cancel_resume"
    assert company_public_web_record["running_control_gap_status"] == "closed"
    assert company_public_web_policy.running_cancel_supported is True
    assert (
        company_public_web_policy.running_cancel_delegate
        == "workflow_orchestrator.cancel_orchestration_before_downstream"
    )
    assert company_public_web_policy.running_cancel_prerequisites == (
        "workflow_command_status_claimed_or_running",
        "no_downstream_command_planned",
        "command_lease_expired_or_force",
    )
    assert company_public_web_policy.running_resume_supported is True
    assert company_public_web_policy.running_resume_delegate == "workflow_orchestrator.resume_orchestration_command"
    company_source_policy = workflow_command_control_policy(
        command_type=COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
        owner=owner_registry[COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE],
    )
    assert company_source_policy.to_record()["running_control_category"] == "provider_attempt"
    assert company_source_policy.to_record()["running_control_maturity"] == "owner_specific_cancel_resume"
    assert company_source_policy.to_record()["running_control_gap_status"] == "closed"
    assert company_source_policy.running_cancel_supported is True
    assert (
        company_source_policy.running_cancel_delegate
        == "company_public_web_owner.cancel_or_poll_stop_source_collect"
    )
    assert company_source_policy.running_cancel_prerequisites == (
        "workflow_command_status_claimed_or_running",
        "provider_activity_attempt_absent_or_poll_cancel_quarantine",
        "no_provider_entity_delta_recorded",
        "no_downstream_command_planned",
        "command_lease_expired_or_force",
    )
    assert company_source_policy.running_resume_supported is True
    assert company_source_policy.running_resume_delegate == "company_public_web_owner.resume_source_collect"
    assert company_source_policy.running_resume_prerequisites == (
        "workflow_command_status_claimed_or_running",
        "command_lease_expired_or_force",
        "canonical_asset_sync_deferred",
    )
    company_materialize_policy = workflow_command_control_policy(
        command_type=COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
        owner=owner_registry[COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE],
    )
    assert company_materialize_policy.to_record()["running_control_category"] == "domain_mutation"
    assert company_materialize_policy.to_record()["running_control_maturity"] == "owner_specific_cancel_resume"
    assert company_materialize_policy.to_record()["running_control_gap_status"] == "closed"
    assert company_materialize_policy.running_cancel_supported is True
    assert (
        company_materialize_policy.running_cancel_delegate
        == "company_public_web_owner.cancel_assets_materialize_before_sync"
    )
    assert company_materialize_policy.running_cancel_prerequisites == (
        "workflow_command_status_claimed_or_running",
        "no_company_asset_materialize_activity_attempt_started",
        "no_company_asset_entity_delta_recorded",
        "no_downstream_command_planned",
        "command_lease_expired_or_force",
    )
    assert company_materialize_policy.running_resume_supported is True
    assert company_materialize_policy.running_resume_delegate == "company_public_web_owner.resume_assets_materialize"
    assert company_materialize_policy.running_resume_prerequisites == (
        "workflow_command_status_claimed_or_running",
        "command_lease_expired_or_force",
        "idempotent_company_asset_sync_key",
    )
    maturity_counts: dict[str, int] = {}
    for command_type, owner in owner_registry.items():
        maturity = workflow_command_control_policy(
            command_type=command_type,
            owner=owner,
        ).to_record()["running_control_maturity"]
        maturity_counts[maturity] = maturity_counts.get(maturity, 0) + 1
    assert maturity_counts.get("fail_closed_with_upgrade_requirements", 0) == 0
    assert maturity_counts.get("owner_specific_cancel_only", 0) == 0
    residual_resume_only = {
        command_type
        for command_type, owner in owner_registry.items()
        if workflow_command_control_policy(
            command_type=command_type,
            owner=owner,
        ).to_record()["running_control_maturity"]
        == "owner_specific_resume_only"
    }
    assert residual_resume_only == set()


def test_operation_run_control_state_is_contract_owned() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    frontend_doc = FRONTEND_API_CONTRACT_DOC_PATH.read_text(encoding="utf-8")
    agent_doc = AGENT_OPERATION_PATH.read_text(encoding="utf-8")

    queued_state = operation_run_control_state(
        operation_status="queued",
        action_status="queued",
        operation_phase="queued",
    ).to_record()
    assert queued_state["control_source_of_truth"] == "operation_runtime.operation_run_control_state"
    assert queued_state["fallback_status"] == "fail_closed"
    assert set(queued_state["allowed_actions"]) == {"dispatch", "resume", "cancel"}

    planned_state = operation_run_control_state(
        operation_status="planned",
        action_status="queued",
        operation_phase="command_planned",
    ).to_record()
    assert "dispatch" not in planned_state["allowed_actions"]
    assert planned_state["disabled_reasons"]["dispatch"] == "dispatch_requires_queued_operation"

    failed_state = operation_run_control_state(
        operation_status="failed",
        action_status="queued",
        operation_phase="failed",
    ).to_record()
    assert failed_state["can_retry"] is True

    cancelled_action_state = operation_run_control_state(
        operation_status="cancelled",
        action_status="cancelled",
        operation_phase="cancelled",
    ).to_record()
    assert cancelled_action_state["can_retry"] is False
    assert cancelled_action_state["disabled_reasons"]["retry"] == "linked_action_terminal"

    for source in (review_doc, frontend_doc, agent_doc):
        assert "operation_runtime.operation_run_control_state" in source
        assert "terminal-status" in source or "terminal status" in source
    find_class_method("_operation_run_control_state_record")
    find_class_method("_operation_run_control_response_record")


def test_frontend_contract_exposes_operation_command_activity_spine_policy() -> None:
    frontend_doc = FRONTEND_API_CONTRACT_DOC_PATH.read_text(encoding="utf-8")
    agent_doc = AGENT_OPERATION_PATH.read_text(encoding="utf-8")
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO
    types_source = FRONTEND_API_TYPES_PATH.read_text(encoding="utf-8")
    schema_source = FRONTEND_API_SCHEMA_PATH.read_text(encoding="utf-8")
    adapter_source = FRONTEND_API_ADAPTER_PATH.read_text(encoding="utf-8")

    for source in (frontend_doc, types_source, schema_source, adapter_source):
        assert "activity_spine_policy" in source
        assert "control_policy" in source
        assert "control_state" in source
        assert "running_cancel_upgrade_requirements" in source
        assert "running_control_category" in source
        assert "running_control_categories" in source
    for name in (
        "OperationActionRegistryResponse",
        "OperationActionDisplayContract",
        "OperationActionRecord",
        "OperationRunRecord",
        "OperationRunControlState",
        "OperationRunStatusSummary",
        "OperationEventRecord",
        "OperationActionListResponse",
        "OperationRunProvenanceResponse",
        "OperationRunControlResponse",
        "WorkflowCommandRegistryResponse",
        "WorkflowCommandControlResponse",
        "WorkflowCommandControlState",
        "WorkflowCommandDisplayContract",
        "WorkflowCommandActivitySpinePolicy",
        "WorkflowCommandExecutionSummary",
        "WorkflowActivityRecord",
        "WorkflowActivityAttemptRecord",
        "WorkflowEntityDeltaRecord",
    ):
        assert name in types_source
        assert name in schema_source
    assert "control_state?: OperationRunControlState" in types_source
    assert "display_contract?: OperationActionDisplayContract" in types_source
    assert '"display_contract": { "$ref": "#/$defs/OperationActionDisplayContract" }' in schema_source
    assert "mapOperationActionDisplayContract(source.display_contract)" in adapter_source
    assert '"control_state": { "$ref": "#/$defs/OperationRunControlState" }' in schema_source
    assert "mapOperationRunControlState(source.control_state)" in adapter_source
    assert "operation_run.control_state" in frontend_doc
    operation_control_types = types_source.split("export interface OperationRunControlResponse", 1)[1].split(
        "export interface WorkflowCommandRegistryResponse",
        1,
    )[0]
    operation_control_schema = schema_source.split('"OperationRunControlResponse"', 1)[1].split(
        '"WorkflowCommandRegistryResponse"',
        1,
    )[0]
    operation_control_adapter = adapter_source.split("export function mapOperationRunControlResponse", 1)[1].split(
        "export function mapWorkflowCommandContract",
        1,
    )[0]
    assert "control_state?: OperationRunControlState" in operation_control_types
    assert "display_contract?: OperationActionDisplayContract" in operation_control_types
    assert '"control_state": { "$ref": "#/$defs/OperationRunControlState" }' in operation_control_schema
    assert '"display_contract": { "$ref": "#/$defs/OperationActionDisplayContract" }' in operation_control_schema
    assert "mapOperationRunControlState(source.control_state)" in operation_control_adapter
    assert "mapOperationActionDisplayContract(source.display_contract)" in operation_control_adapter
    assert "retry selection follows the returned child OperationRun" in next_todo
    control_response_types = types_source.split("export interface WorkflowCommandControlResponse", 1)[1].split(
        "export interface WorkflowActivityControlTarget",
        1,
    )[0]
    control_response_schema = schema_source.split('"WorkflowCommandControlResponse"', 1)[1].split(
        '"WorkflowActivityControlTarget"',
        1,
    )[0]
    control_response_adapter = adapter_source.split("mapWorkflowCommandControlResponse", 1)[1].split(
        "mapWorkflowActivityControlTarget",
        1,
    )[0]
    assert "display_contract?: WorkflowCommandDisplayContract" in control_response_types
    assert "activity_spine_policy?: WorkflowCommandActivitySpinePolicy" in control_response_types
    assert "control_state?: WorkflowCommandControlState" in control_response_types
    assert "running_resume_supported?: boolean" in types_source
    assert '"running_resume_supported": { "type": "boolean" }' in schema_source
    assert "running_resume_supported: asOptionalBoolean(source.running_resume_supported)" in adapter_source
    assert "unsupported_running_resume_reason" in types_source
    assert "unsupported_running_resume_reason" in schema_source
    assert "unsupported_running_resume_reason: asOptionalString(source.unsupported_running_resume_reason)" in adapter_source
    assert '"display_contract": { "$ref": "#/$defs/WorkflowCommandDisplayContract" }' in control_response_schema
    assert '"control_state": { "$ref": "#/$defs/WorkflowCommandControlState" }' in control_response_schema
    assert "mapWorkflowCommandDisplayContract(source.display_contract)" in control_response_adapter
    assert "mapWorkflowCommandControlState(source.control_state)" in control_response_adapter
    assert '"activity_spine_policy": { "$ref": "#/$defs/WorkflowCommandActivitySpinePolicy" }' in control_response_schema
    assert "mapWorkflowCommandActivitySpinePolicy(source.activity_spine_policy)" in control_response_adapter
    assert "execution_summary?: WorkflowCommandExecutionSummary" in types_source
    assert '"execution_summary": { "$ref": "#/$defs/WorkflowCommandExecutionSummary" }' in schema_source
    assert "mapWorkflowCommandExecutionSummary(source.execution_summary)" in adapter_source
    assert "display_contract?: WorkflowCommandDisplayContract" in types_source
    assert '"display_contract": { "$ref": "#/$defs/WorkflowCommandDisplayContract" }' in schema_source
    assert "mapWorkflowCommandDisplayContract(source.display_contract)" in adapter_source
    assert "mapWorkflowCommandControlState(source.control_state)" in adapter_source
    assert "include_execution_summary=true" in frontend_doc
    assert "workflow_command.execution_summary" in frontend_doc
    assert "workflow_command.execution_summary" in agent_doc
    assert "workflow_command.execution_summary.activity_status_counts" in frontend_doc
    assert "activity_status_counts" in agent_doc
    assert "entity_delta_kind_counts" in agent_doc
    assert "operation-run provenance returns commands through the same API record shape" in next_todo
    assert "operation_run.status_summary" in frontend_doc
    assert "operation_run.status_summary" in agent_doc
    assert "Operation queue summaries must be operation/runtime-owned" in review_doc
    assert "include_status_summary=true" in frontend_doc
    assert "mapOperationRunStatusSummary(source.status_summary)" in adapter_source
    assert "Command status/provenance summaries must be Activity-spine-owned" in review_doc
    assert "Operation UI command status copy must be Activity-spine-owned" in review_doc
    assert "Operation UI Activity drill-down is read-only runtime inspection" in review_doc
    assert "Operation UI command control is WorkflowCommand-owned" in review_doc
    assert "Operation UI command control must surface rejected controls" in review_doc
    assert "Status-specific command controls must also be explicit" in review_doc
    assert "workflow_command_control_state" in review_doc
    assert "workflow_command_control_state" in frontend_doc
    assert "plus its `display_contract`, `control_policy`, `control_state`, `activity_spine_policy`, and `fallback_status=fail_closed`" in review_doc
    assert "Rows include `control_target` back to the owning workflow command plus its `display_contract`, `control_policy`, and `activity_spine_policy`" in agent_doc
    assert "Discovery-lane read models expose the same command-owned `control_target` shape" in agent_doc
    assert "携带 command-owned `display_contract`、`control_policy`、`control_state`、`activity_spine_policy` 和 `fallback_status=fail_closed`" in frontend_doc
    assert "不得从 activity、attempt、delta、lane 字段推导展示文案或控制能力" in frontend_doc
    assert (
        '"display_contract": self._workflow_command_display_contract_record'
        in _class_method_source("_workflow_command_control_target_record")
    )
    assert "响应顶层 `activity_spine_policy`" in frontend_doc
    assert "正常 command registry 不应出现" in frontend_doc
    for endpoint in (
        "/api/operations/action-registry",
        "/api/operations/actions",
        "/api/operations/runs",
        "/api/operations/runs/${encodeURIComponent(operationRunId)}/provenance",
        "/api/operations/actions/${encodeURIComponent(actionId)}/approve",
        "/api/operations/runs/${encodeURIComponent(operationRunId)}/dispatch",
        "/api/workflow/command-registry",
        "/api/workflow/commands",
        "/api/workflow/commands/${encodeURIComponent(commandId)}/cancel",
        "/api/workflow/commands/${encodeURIComponent(commandId)}/retry",
        "/api/workflow/commands/${encodeURIComponent(commandId)}/resume",
        "/api/workflow/activities",
        "/api/workflow/activity-attempts",
        "/api/workflow/entity-deltas",
        "/api/workflow/discovery-lanes",
    ):
        assert endpoint in adapter_source
    for mapper in (
        "mapOperationActionListResponse",
        "mapOperationRunListResponse",
        "mapOperationRunProvenanceResponse",
        "mapOperationRunControlResponse",
    ):
        assert mapper in adapter_source
    for name in (
        "AcquisitionDiscoveryLaneRecord",
        "AcquisitionDiscoveryLaneListResponse",
        "AcquisitionDiscoveryLaneDetailResponse",
    ):
        assert name in types_source
        assert name in schema_source
    assert "mapAcquisitionDiscoveryLaneRecord" in adapter_source
    assert "listAcquisitionDiscoveryLanes" in adapter_source
    assert "getAcquisitionDiscoveryLane" in adapter_source
    assert 'activity_spine_policy.requirement="legacy_internal_pending_activity_spine"' in frontend_doc


def test_operations_page_is_operation_api_only_control_surface() -> None:
    frontend_doc = FRONTEND_API_CONTRACT_DOC_PATH.read_text(encoding="utf-8")
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO
    api_source = (REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts").read_text(encoding="utf-8")
    app_source = FRONTEND_APP_PATH.read_text(encoding="utf-8")
    sidebar_source = FRONTEND_SIDEBAR_PATH.read_text(encoding="utf-8")
    operations_source = FRONTEND_OPERATIONS_PAGE_PATH.read_text(encoding="utf-8")
    styles_source = FRONTEND_STYLES_PATH.read_text(encoding="utf-8")

    assert 'path="/operations"' in app_source
    assert 'navigate("/operations")' in sidebar_source
    assert "only calls Operation APIs" in review_doc
    assert "calls only operation control APIs" in next_todo
    assert "前端不得直接写 `workflow_commands`" in frontend_doc
    assert "`/operations` 前端页面是任务审批与执行工作台" in frontend_doc
    assert "Operation helper 也必须只调用 `/api/operations/...`" in frontend_doc
    assert "control_state" in frontend_doc
    assert "controlState" in operations_source
    assert "terminalStatuses" not in operations_source
    assert "operationControlAllows" in operations_source
    assert "operationControlReason" in operations_source
    assert "nextRun?.operationRunId || operationRunId" in operations_source
    assert "commandControlSummary" in operations_source
    assert "commandControlPolicySummary" in operations_source
    assert "继续受限：" in operations_source
    assert "Control category:" in operations_source
    assert "runningControlMaturity" in operations_source
    assert "runningControlGapStatus" in operations_source
    assert "disabledReasons.running_resume" in operations_source
    assert "commandDisplayLabel" in operations_source
    assert "commandDisplayCategory" in operations_source
    assert "commandDisplayDescription" in operations_source
    assert "operationDisplayLabel" in operations_source
    assert "operationDisplayCategory" in operations_source
    assert "operationDisplayDescription" in operations_source
    assert "commandExecutionCounts" in operations_source
    assert "commandExecutionEffect" in operations_source
    assert "latestExecutionReason" in operations_source
    assert "latestExecutionEntity" in operations_source
    assert "activityStatusCounts: asNumberRecord(record.activity_status_counts)" in api_source
    assert "attemptStatusCounts: asNumberRecord(record.attempt_status_counts)" in api_source
    assert "entityDeltaStatusCounts: asNumberRecord(record.entity_delta_status_counts)" in api_source
    assert "entityDeltaKindCounts: asNumberRecord(record.entity_delta_kind_counts)" in api_source
    assert "sampleTruncated: asBoolean(record.sample_truncated) === true" in api_source
    assert "displayContract: asObjectRecord(record.display_contract)" in api_source
    assert "activityStatusCounts" in operations_source
    assert "displayContract?.display_label" in operations_source
    assert "displayContract?.display_category" in operations_source
    assert "operationDisplayLabel(action)" in operations_source
    assert "operationDisplayLabel(run)" in operations_source
    assert 'return displayLabel || "Display contract missing"' in operations_source
    assert 'return category || "display_contract_missing"' in operations_source
    assert 'return category || String(command.owner || "unknown owner")' not in operations_source
    assert 'return category || String(operation.ownerModule || "operation")' not in operations_source
    assert 'return displayLabel || "Workflow command"' not in operations_source
    assert 'return displayLabel || "Operation"' not in operations_source
    assert "entityDeltaKindCounts" in operations_source
    assert "latestEffectStatus" in operations_source
    assert "sampleTruncated" in operations_source
    assert "listWorkflowActivities" in operations_source
    assert "listWorkflowActivityAttempts" in operations_source
    assert "listWorkflowEntityDeltas" in operations_source
    assert "cancelWorkflowCommand" in operations_source
    assert "resumeWorkflowCommand" in operations_source
    assert "retryWorkflowCommand" in operations_source
    assert "loadCommandDrilldown" in operations_source
    assert "applyCommandControl" in operations_source
    assert "commandControlAllows" in operations_source
    assert "commandControlReason" in operations_source
    assert "commandDrilldown" in operations_source
    assert "查看执行证据" in operations_source
    assert "readOnlyEvidenceSummary" in operations_source
    assert "mutationContract" in operations_source
    assert "moduleStateMutated" in operations_source
    assert "只读证据" in operations_source
    assert "取消步骤" in operations_source
    assert "继续步骤" in operations_source
    assert "重试步骤" in operations_source
    assert "任务审批与执行" in operations_source
    assert "待确认操作" in operations_source
    assert "执行队列" in operations_source
    assert "执行详情" in operations_source
    assert "statusText(status)" in operations_source
    assert "controlActionText" in operations_source
    assert "statusClassName(command.status)" in operations_source
    assert "statusText(command.status)" in operations_source
    assert "{statusLabel(command.status)}" not in operations_source
    assert "operation-panel-header" in operations_source
    assert "operation-refresh-button" in operations_source
    assert ".operation-refresh-button" in styles_source
    assert "white-space: nowrap;" in styles_source
    assert "这里只消费 Operation/Command API" not in operations_source
    assert "不直接修改业务 read model" not in operations_source
    assert "命令控制必须通过正式 command/operation API" not in operations_source
    assert "export async function listWorkflowActivities" in api_source
    assert "export async function listWorkflowActivityAttempts" in api_source
    assert "export async function listWorkflowEntityDeltas" in api_source
    assert "export function cancelWorkflowCommand" in api_source
    assert "export function resumeWorkflowCommand" in api_source
    assert "export function retryWorkflowCommand" in api_source
    assert "/api/workflow/activities" in api_source
    assert "/api/workflow/activity-attempts" in api_source
    assert "/api/workflow/entity-deltas" in api_source
    assert "/api/workflow/commands/${encodeURIComponent(commandId)}/${action}" in api_source
    assert 'status === "invalid" || status === "not_found" || status === "failed"' in api_source
    assert "expectedStatus = action ===" not in api_source
    assert "throw new Error(`Workflow command ${action} failed: ${reason}`)" in api_source
    workflow_command_control_block = _typescript_block(
        api_source,
        "async function postWorkflowCommandControl",
        "export function cancelWorkflowCommand",
    )
    assert (
        'status === "invalid" || status === "not_found" || status === "failed" || status === "unsupported"'
        in workflow_command_control_block
    )
    assert "missing workflow_command" in workflow_command_control_block
    for required_command_contract in ("control_state", "display_contract", "control_policy", "activity_spine_policy"):
        assert required_command_contract in workflow_command_control_block
    operation_run_control_block = _typescript_block(
        api_source,
        "async function postOperationRunControl",
        "export function cancelOperationRun",
    )
    assert (
        'status === "invalid" || status === "not_found" || status === "failed" || status === "unsupported"'
        in operation_run_control_block
    )
    assert "missing operation_run" in operation_run_control_block
    assert "throw new Error(`Operation ${action} failed: ${reason}`)" in operation_run_control_block
    assert "Activity-spine copy/drill-down/control follow-up" in next_todo
    assert "does not control Activity/Attempt/Delta rows directly" in next_todo
    assert "workflow_command.control_state.allowed_actions" in frontend_doc
    for required_symbol in (
        "listOperationActions",
        "approveOperationAction",
        "rejectOperationAction",
        "listOperationRuns",
        "getOperationRunProvenance",
        "dispatchOperationRun",
        "resumeOperationRun",
        "retryOperationRun",
        "cancelOperationRun",
    ):
        assert required_symbol in operations_source
    forbidden_operation_page_tokens = (
        "/api/crm",
        "/api/projections",
        "/api/target-candidates",
        "/api/public-web",
        "/api/excel",
        "/api/export",
        "fetchJson(",
        "fetchPublicJson",
        "write",
        "upsert",
        "repair",
    )
    for forbidden in forbidden_operation_page_tokens:
        assert forbidden not in operations_source, forbidden

    operation_helper_block = "\n".join(
        [
            _typescript_block(api_source, "export async function listOperationRuns", "export async function listOperationActions"),
            _typescript_block(api_source, "export async function listOperationActions", "async function postOperationActionDecision"),
            _typescript_block(api_source, "async function postOperationActionDecision", "export function approveOperationAction"),
            _typescript_block(api_source, "export function approveOperationAction", "export async function getOperationRunProvenance"),
            _typescript_block(api_source, "export async function getOperationRunProvenance", "function extractCandidateArray"),
        ]
    )
    for endpoint in re.findall(r'["`](/api/[^"`]+)', operation_helper_block):
        assert endpoint.startswith("/api/operations/"), endpoint
    for forbidden in (
        "/api/crm",
        "/api/projections",
        "/api/target-candidates",
        "/api/public-web",
        "/api/excel",
        "/api/export",
        "fetchPublicJson",
    ):
        assert forbidden not in operation_helper_block, forbidden


def test_frontend_public_web_uses_crm_canonical_endpoints_only() -> None:
    frontend_api_source = (REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts").read_text(encoding="utf-8")
    contract_adapter_source = FRONTEND_API_ADAPTER_PATH.read_text(encoding="utf-8")
    contract_types_source = FRONTEND_API_TYPES_PATH.read_text(encoding="utf-8")
    contract_schema_source = FRONTEND_API_SCHEMA_PATH.read_text(encoding="utf-8")
    frontend_source_map = {
        **_frontend_normal_source_texts(),
        str(FRONTEND_API_ADAPTER_PATH.relative_to(REPO_ROOT)): contract_adapter_source,
        str(FRONTEND_API_TYPES_PATH.relative_to(REPO_ROOT)): contract_types_source,
        str(FRONTEND_API_SCHEMA_PATH.relative_to(REPO_ROOT)): contract_schema_source,
    }
    frontend_sources = "\n".join(frontend_source_map.values())
    required_crm_routes = (
        "/api/crm/records/public-web-search",
        "/api/crm/records/public-web-search/poll",
        "/api/crm/records/public-web-search/cancel",
        "/api/crm/records/public-web-search/retry",
        "/api/crm/records/public-web-export",
        "/public-web-search",
        "/public-web-promotions",
    )
    for route in required_crm_routes:
        assert route in frontend_sources, route
    for source_path, frontend_source in frontend_source_map.items():
        assert "/api/target-candidates/public-web" not in frontend_source, source_path
    target_named_wrapper_blocks = (
        _typescript_block(
            frontend_api_source,
            "export async function getTargetCandidatePublicWebSearches",
            "export async function getTargetCandidatePublicWebDetail",
        ),
        _typescript_block(
            frontend_api_source,
            "export async function getTargetCandidatePublicWebDetail",
            "export async function getTargetCandidateProfile",
        ),
        _typescript_block(
            frontend_api_source,
            "export async function startTargetCandidatePublicWebSearch",
            "export async function cancelTargetCandidatePublicWebSearch",
        ),
        _typescript_block(
            frontend_api_source,
            "export async function cancelTargetCandidatePublicWebSearch",
            "export async function retryTargetCandidatePublicWebSearch",
        ),
        _typescript_block(
            frontend_api_source,
            "export async function retryTargetCandidatePublicWebSearch",
            "export async function promoteTargetCandidatePublicWebSignal",
        ),
        _typescript_block(
            frontend_api_source,
            "export async function promoteTargetCandidatePublicWebSignal",
            "export async function getCompanies",
        ),
    )
    for block in target_named_wrapper_blocks:
        assert "/api/crm/records" in block
        assert "/api/target-candidates/public-web" not in block
    assert "function requireCrmPublicWebWorkspaceId" in contract_adapter_source
    assert "CRM Public Web body-style requests require workspace_id." in contract_adapter_source
    assert "const workspaceId = requireCrmPublicWebWorkspaceId(filters.workspace_id ?? filters.workspaceId)" in contract_adapter_source
    assert "const workspaceId = requireCrmPublicWebWorkspaceId(payload.workspace_id ?? payload.workspaceId)" in contract_adapter_source
    assert "workspace_id: workspaceId" in contract_adapter_source
    batch_mapper_block = _typescript_block(
        contract_adapter_source,
        "export function mapTargetCandidatePublicWebBatch",
        "export function mapTargetCandidatePublicWebRun",
    )
    run_mapper_block = _typescript_block(
        contract_adapter_source,
        "export function mapTargetCandidatePublicWebRun",
        "export function normalizeTargetCandidatePublicWebStatus",
    )
    assert "workspace_id: asOptionalString(source.workspace_id)" in batch_mapper_block
    assert "workspace_id: asOptionalString(source.workspace_id)" in run_mapper_block
    assert "phase_commands: source.phase_commands ? asJsonObject(source.phase_commands) : {}" in run_mapper_block
    assert "phase_command_display_line: asOptionalString(source.phase_command_display_line)" in run_mapper_block
    assert "run_control_state: source.run_control_state ? asJsonObject(source.run_control_state) : {}" in run_mapper_block
    assert "run_display_contract: source.run_display_contract ? asJsonObject(source.run_display_contract) : {}" in run_mapper_block
    assert "created_at: asOptionalString(source.created_at)" in run_mapper_block
    batch_contract_block = _typescript_block(
        contract_types_source,
        "export interface TargetCandidatePublicWebBatch",
        "export interface TargetCandidatePublicWebRun",
    )
    run_contract_block = _typescript_block(
        contract_types_source,
        "export interface TargetCandidatePublicWebRun",
        "export interface TargetCandidatePublicWebSearchState",
    )
    assert "workspace_id?: string;" in batch_contract_block
    assert "workspace_id?: string;" in run_contract_block
    assert "phase_commands?: JsonObject;" in run_contract_block
    assert "phase_command_display_line?: string;" in run_contract_block
    assert "run_control_state?: JsonObject;" in run_contract_block
    assert "run_display_contract?: JsonObject;" in run_contract_block
    assert "created_at?: string;" in run_contract_block
    assert '"TargetCandidatePublicWebBatch"' in contract_schema_source
    assert '"TargetCandidatePublicWebRun"' in contract_schema_source
    assert '"workspace_id": { "type": "string" }' in contract_schema_source
    assert '"phase_commands": { "$ref": "#/$defs/JsonObject" }' in contract_schema_source
    assert '"phase_command_display_line": { "type": "string" }' in contract_schema_source
    assert '"run_control_state": { "$ref": "#/$defs/JsonObject" }' in contract_schema_source
    assert '"run_display_contract": { "$ref": "#/$defs/JsonObject" }' in contract_schema_source
    assert '"created_at": { "type": "string" }' in contract_schema_source


def test_legacy_target_public_web_routes_are_not_priority_lane_normal_surfaces() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    api_source = API_PATH.read_text(encoding="utf-8")
    priority_lane_block = _method_block(
        api_source,
        "_request_priority_lane",
        "_legacy_target_candidate_export_allowed",
    )

    assert "Retired Public Web aliases must not receive normal priority-lane handling" in review_doc
    assert '"/api/crm/records/public-web-search"' in priority_lane_block
    assert '"/api/crm/records/public-web-search/poll"' in priority_lane_block
    assert '"/api/crm/records/public-web-search/cancel"' in priority_lane_block
    assert '"/api/crm/records/public-web-search/retry"' in priority_lane_block
    for legacy_route in (
        '"/api/target-candidates/public-web-search"',
        '"/api/target-candidates/public-web-search/poll"',
        '"/api/target-candidates/public-web-search/cancel"',
        '"/api/target-candidates/public-web-search/retry"',
        r"/api/target-candidates/[^/]+/public-web-search",
    ):
        assert legacy_route not in priority_lane_block, legacy_route


def test_operation_and_command_status_summaries_do_not_read_domain_tables() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    operation_summary_block = _class_method_source("_operation_run_status_summary")
    command_summary_block = _class_method_source("_workflow_command_execution_summary")
    # Former slice end-markers: keep bare existence of the API-record wrappers.
    find_class_method("_operation_run_api_record_with_status_summary")
    find_class_method("_workflow_command_api_record_with_execution_summary")
    forbidden_domain_reads = (
        "list_crm",
        "get_crm",
        "crm_public_web",
        "public_web",
        "serving_projection",
        "projection_reader",
        "list_acquisition_discovery_lanes",
        "get_acquisition_discovery_lane",
        "linkedin_profile_registry",
        "get_job(",
        "list_jobs",
        "excel",
        "export",
    )
    for block in (operation_summary_block, command_summary_block):
        for forbidden in forbidden_domain_reads:
            assert forbidden not in block, forbidden
    for allowed in (
        "list_operation_events",
        "list_workflow_commands",
        "list_workflow_activity_runs",
        "list_workflow_activity_attempts",
        "list_workflow_entity_deltas",
    ):
        assert allowed in operation_summary_block + command_summary_block
    assert "Operation queue summaries must be operation/runtime-owned" in review_doc
    assert "Command status/provenance summaries must be Activity-spine-owned" in review_doc


def test_legacy_public_web_archive_drop_is_migration_only_contract() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    durable_doc = (REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md").read_text(encoding="utf-8")
    crm_doc = (REPO_ROOT / "docs" / "CRM_STATE_CONTRACT.md").read_text(encoding="utf-8")
    script_source = LEGACY_PUBLIC_WEB_DROP_SCRIPT_PATH.read_text(encoding="utf-8")

    assert "scripts/archive_drop_legacy_public_web_tables.py" in review_doc
    assert "legacy_public_web_archive_v1" in durable_doc
    assert "legacy_public_web_drop_v1" in durable_doc
    assert "non-empty legacy tables require a cold archive path" in crm_doc
    assert "drop_legacy_target_public_web_tables" in script_source
    assert "--allow-non-empty-without-archive" in script_source


def test_export_command_owners_are_activity_spine_visible() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    durable_doc = (REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md").read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO

    for source in (review_doc, durable_doc, next_todo):
        assert "export.projection.generate" in source
        assert "export.crm_public_web.generate" in source
    assert "generated artifacts are recorded through ActivityRun/Attempt/EntityDelta evidence" in review_doc
    assert "Export commands are bounded activities" in durable_doc
    assert "projection export and CRM Public Web export owners now write `workflow_activity_runs`" in next_todo
    assert "activity_type=EXPORT_PROJECTION_GENERATE_COMMAND_TYPE" in _class_method_source(
        "_run_projection_export_generate_command"
    )
    assert "activity_type=EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE" in _class_method_source(
        "_run_crm_public_web_export_generate_command", class_name="CrmPublicWebOwner"
    )


def test_excel_intake_owner_is_activity_spine_visible() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    durable_doc = (REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md").read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO

    for source in (review_doc, durable_doc, next_todo):
        assert "excel.intake.run" in source
        assert "ActivityRun" in source
        assert "Attempt" in source
    assert "`excel.intake.run` is a bounded activity" in durable_doc
    assert "terminal `excel_intake_job` `workflow_entity_deltas`" in next_todo
    assert "excel_intake_owner.cancel_excel_intake_run_command" in review_doc
    assert "excel_intake_owner.cancel_excel_intake_run_command" in durable_doc
    assert "owner-specific cooperative cancel" in next_todo
    assert "activity_type=EXCEL_INTAKE_RUN_COMMAND_TYPE" in _class_method_source(
        "_run_excel_intake_run_command", class_name="ExcelIntakeOwner"
    )
    assert "entity_type=\"excel_intake_job\"" in _class_method_source(
        "_record_excel_intake_command_cancelled_terminal", class_name="ExcelIntakeOwner"
    )
    find_class_method("_raise_if_excel_intake_command_cancelled", class_name="ExcelIntakeOwner")


def test_crm_writer_command_owner_is_activity_spine_visible() -> None:
    review_doc = DOC_PATH.read_text(encoding="utf-8")
    durable_doc = (REPO_ROOT / "docs" / "DURABLE_EXECUTION_RUNTIME_CONTRACT.md").read_text(encoding="utf-8")
    agent_doc = AGENT_OPERATION_PATH.read_text(encoding="utf-8")
    next_todo = DOC_PATH.read_text(encoding="utf-8")  # decision records migrated 2026-06-11 from NEXT_TODO

    for command_type in (
        "crm.record.add_from_projection",
        "crm.record.update",
        "crm.note.add",
        "crm.task.create",
    ):
        assert command_type in review_doc
        assert command_type in durable_doc
        assert command_type in agent_doc
    assert "CRM writer commands record ActivityRun/Attempt/EntityDelta evidence" in review_doc
    assert "CRM writer commands (`crm.record.add_from_projection`" in durable_doc
    assert "CRM writer commands now record ActivityRun/Attempt/EntityDelta evidence" in next_todo
    crm_writer_delta_source = _class_method_source("_record_crm_writer_command_entity_deltas")
    assert 'entity_type="crm_record"' in crm_writer_delta_source
    assert 'entity_type="crm_task"' in crm_writer_delta_source
    _assert_token_in_sourcing_agent_sources("activity_type=command_type")
