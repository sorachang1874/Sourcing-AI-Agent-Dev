"""CRM Public Web runtime boundary guardrails.

Source-level guardrail tests in this module resolve method/function bodies via
``tests.source_inspection`` (AST-based, searched across every module under
``src/sourcing_agent`` recursively) instead of slicing ``orchestrator.py`` raw
text between ``def`` markers.  This keeps the guardrails stable when methods
move out of the orchestrator god class into domain modules: token
presence/absence is asserted against the exact source segment of the named
definition wherever it lives.
"""

import ast
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tests.source_inspection import all_source_files, find_class_method, find_def, find_module_def

from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.public_web_runtime_core import (
    CRM_PUBLIC_WEB_OWNER,
    _poll_and_fetch_ready_public_web_tasks,
    _public_web_candidate_record_from_run,
    _public_web_phase_metrics_from_summary,
)
from sourcing_agent.public_web_search import (
    CandidateSearchOutcome,
    CandidateSearchPlan,
    PublicWebCandidateContext,
    PublicWebExperimentOptions,
    PublicWebQuerySpec,
)
from sourcing_agent.search_provider import (
    BaseSearchProvider,
    SearchBatchReadyResult,
    SearchBatchReadyTask,
    SearchBatchSubmissionResult,
    SearchBatchSubmissionTask,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src" / "sourcing_agent"
TEST_ROOT = REPO_ROOT / "tests"

ALLOWED_DIRECT_IMPORT_FILES = {
    SRC_ROOT / "crm_public_web_runtime.py",
    SRC_ROOT / "legacy_target_candidate_public_web_runtime.py",
    SRC_ROOT / "public_web_runtime_core.py",
}

ALLOWED_LEGACY_STORAGE_ACCESS_FILES = {
    SRC_ROOT / "control_plane_live_postgres.py",
    SRC_ROOT / "control_plane_postgres.py",
    SRC_ROOT / "legacy_public_web_storage.py",
    SRC_ROOT / "legacy_public_web_retirement_audit.py",
    SRC_ROOT / "storage.py",
}

LEGACY_PUBLIC_WEB_STORAGE_HELPERS = {
    "get_target_candidate_public_web_batch",
    "get_target_candidate_public_web_promotion",
    "get_target_candidate_public_web_run",
    "list_latest_target_candidate_public_web_runs_by_record_ids",
    "list_target_candidate_public_web_batches",
    "list_target_candidate_public_web_promotions",
    "list_target_candidate_public_web_runs",
    "update_target_candidate_public_web_run",
    "upsert_target_candidate_public_web_batch",
    "upsert_target_candidate_public_web_promotion",
    "upsert_target_candidate_public_web_run",
}

LEGACY_PUBLIC_WEB_STORAGE_WRITE_HELPERS = {
    "update_target_candidate_public_web_run",
    "upsert_target_candidate_public_web_batch",
    "upsert_target_candidate_public_web_promotion",
    "upsert_target_candidate_public_web_run",
}

ALLOWED_LEGACY_STORAGE_WRITE_TESTS = {
    TEST_ROOT / "test_control_plane_live_postgres.py",
}

ALLOWED_LEGACY_STORAGE_WRITE_TEST_FUNCTIONS = {
    "test_legacy_target_public_web_storage_writes_fail_closed_without_migration_context",
}

FORBIDDEN_CRM_SYMBOLS_FROM_LEGACY_MODULE = {
    "CRM_PUBLIC_WEB_EXECUTION_BACKEND",
    "CRM_PUBLIC_WEB_JOB_TYPE",
    "CRM_PUBLIC_WEB_OWNER",
    "CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND",
    "cancel_crm_public_web_run",
    "execute_crm_public_web_run_once",
    "execute_crm_public_web_run_to_local_idle",
    "start_crm_public_web_batch",
    "sync_crm_public_web_batch_summary",
}


class _ProjectionContextStore:
    def get_crm_record(self, crm_record_id: str) -> dict:
        assert crm_record_id == "crmrec_margaret"
        return {
            "crm_record_id": "crmrec_margaret",
            "source_projection_id": "proj_anthropic",
            "candidate_identity_key": "linkedin:margaret-v",
            "metadata": {"linkedin_url_cache": "https://www.linkedin.com/in/margaret-v"},
            "display_name_cache": "Margaret V.",
            "headline_cache": "Education @ Anthropic",
            "primary_company_cache": "Anthropic",
        }

    def get_serving_projection_member(self, projection_id: str, candidate_identity_key: str) -> dict:
        assert projection_id == "proj_anthropic"
        assert candidate_identity_key == "linkedin:margaret-v"
        return {
            "row_readiness": "ready",
            "profile_readiness": "ready",
            "card_readiness": "ready",
            "public_summary": {
                "display_name": "Margaret V.",
                "headline": "Education @ Anthropic",
                "current_company": "Anthropic",
                "linkedin_url": "https://www.linkedin.com/in/margaret-v",
                "education_lines": ["2013~2017, Bachelor of Arts - BA, Harvard University"],
                "experience_lines": ["2018~2019, Harvard Business School, Research Assistant"],
            },
        }


class _NeverReadyBatchProvider(BaseSearchProvider):
    provider_name = "never_ready_batch"

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None):
        raise AssertionError("operation-native batch poll should not call synchronous search")

    def poll_ready_batch(self, query_specs: list[dict]) -> SearchBatchReadyResult:
        tasks = []
        for spec in query_specs:
            checkpoint = dict(spec.get("checkpoint") or {})
            tasks.append(
                SearchBatchReadyTask(
                    task_key=str(spec.get("task_key") or ""),
                    task_id=str(spec.get("task_id") or checkpoint.get("task_id") or ""),
                    query_text=str(spec.get("query_text") or ""),
                    checkpoint={**checkpoint, "status": "waiting_for_ready_cached"},
                    metadata={"ready": False},
                )
            )
        return SearchBatchReadyResult(provider_name=self.provider_name, tasks=tasks)


class _ResettablePendingBatchProvider(BaseSearchProvider):
    provider_name = "resettable_pending_batch"

    def __init__(self) -> None:
        self.submitted_specs: list[list[dict]] = []

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None):
        raise AssertionError("operation-native batch poll should not call synchronous search")

    def poll_ready_batch(self, query_specs: list[dict]) -> SearchBatchReadyResult:
        tasks = []
        for spec in query_specs:
            checkpoint = dict(spec.get("checkpoint") or {})
            tasks.append(
                SearchBatchReadyTask(
                    task_key=str(spec.get("task_key") or ""),
                    task_id=str(spec.get("task_id") or checkpoint.get("task_id") or ""),
                    query_text=str(spec.get("query_text") or ""),
                    checkpoint={**checkpoint, "status": "waiting_for_ready_cached"},
                    metadata={
                        "ready": False,
                        "provider_status_code": 40601,
                        "provider_status_message": "Task Handed.",
                        "provider_wait_state": "provider_pending",
                    },
                )
            )
        return SearchBatchReadyResult(provider_name=self.provider_name, tasks=tasks)

    def submit_batch_queries(self, query_specs: list[dict]) -> SearchBatchSubmissionResult:
        self.submitted_specs.append([dict(spec) for spec in query_specs])
        tasks = []
        for spec in query_specs:
            task_key = str(spec.get("task_key") or "")
            tasks.append(
                SearchBatchSubmissionTask(
                    task_key=task_key,
                    query_text=str(spec.get("query_text") or ""),
                    checkpoint={
                        "provider_name": self.provider_name,
                        "task_id": f"{task_key}-reset-1",
                        "status": "submitted",
                    },
                    metadata={
                        "task_id": f"{task_key}-reset-1",
                        "query_identity_key": task_key,
                        "provider_task_reset": True,
                    },
                )
            )
        return SearchBatchSubmissionResult(provider_name=self.provider_name, tasks=tasks)


def _single_query_plan(tempdir: str) -> CandidateSearchPlan:
    candidate = PublicWebCandidateContext(
        record_id="crmrec_margaret",
        candidate_id="cand_margaret",
        candidate_name="Margaret V.",
        current_company="Anthropic",
    )
    query = PublicWebQuerySpec(
        query_id="q01",
        source_family="profile_web_presence",
        query_text='"Margaret V." "Anthropic"',
        objective="Find profile-owned web presence.",
    )
    root = Path(tempdir)
    return CandidateSearchPlan(
        ordinal=1,
        candidate=candidate,
        candidate_dir=root / "candidate",
        logger=AssetLogger(root),
        queries=[query],
        started_monotonic=time.monotonic(),
    )


def _single_waiting_search_checkpoint(*, submitted_at: datetime) -> dict:
    query = PublicWebQuerySpec(
        query_id="q01",
        source_family="profile_web_presence",
        query_text='"Margaret V." "Anthropic"',
        objective="Find profile-owned web presence.",
    )
    return {
        "stage": "waiting_remote_search",
        "status": "searching",
        "search_mode": "batch_queue",
        "submitted_at": submitted_at.isoformat(),
        "search_submit_started_at": submitted_at.isoformat(),
        "tasks": {
            "task-key-1": {
                "task_key": "task-key-1",
                "query_identity_key": "task-key-1",
                "task_id": "task-1",
                "query_text": query.query_text,
                "query": query.to_record(),
                "query_index": 1,
                "checkpoint": {"status": "submitted", "task_id": "task-1"},
                "metadata": {},
                "status": "waiting",
                "poll_count": 1,
            }
        },
        "queries": [query.to_record()],
        "query_results": [],
        "raw_links": [],
        "errors": [],
    }


def test_crm_public_web_candidate_context_includes_projection_profile_detail_for_ai_adjudication() -> None:
    record = _public_web_candidate_record_from_run(
        _ProjectionContextStore(),
        {"run_id": "run_margaret", "record_id": "crmrec_margaret"},
        owner=CRM_PUBLIC_WEB_OWNER,
    )

    assert record is not None
    candidate_metadata = record["metadata"]["candidate"]
    assert candidate_metadata["education_lines"] == ["2013~2017, Bachelor of Arts - BA, Harvard University"]
    assert candidate_metadata["experience_lines"] == ["2018~2019, Harvard Business School, Research Assistant"]
    assert record["candidate_name"] == "Margaret V."
    assert record["current_company"] == "Anthropic"


def test_public_web_phase_metrics_expose_model_fallback_diagnostics() -> None:
    metrics = _public_web_phase_metrics_from_summary(
        checkpoint={"tasks": []},
        summary={},
        analysis_duration_ms=12.0,
        signals={
            "entry_links": [],
            "email_candidates": [],
            "ai_adjudication": {
                "provider": "chshapi_openai_compatible",
                "model": "gpt-5.5",
                "model_version": "gpt-5.5",
                "fallback_used": True,
                "fallback_reason": "model_call_failed",
                "model_error": "OpenAI-compatible HTTP 401: auth_unavailable",
                "result": {
                    "fallback_used": True,
                },
            },
        },
    )

    assert metrics["model_provider"] == "chshapi_openai_compatible"
    assert metrics["model"] == "gpt-5.5"
    assert metrics["model_version"] == "gpt-5.5"
    assert metrics["model_fallback_used"] is True
    assert metrics["model_fallback_reason"] == "model_call_failed"
    assert "401" in metrics["model_error"]


def test_operation_native_public_web_poll_budget_does_not_terminalize_before_remote_timeout() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        checkpoint, outcome = _poll_and_fetch_ready_public_web_tasks(
            plan=_single_query_plan(tempdir),
            search_provider=_NeverReadyBatchProvider(),
            options=PublicWebExperimentOptions(
                max_batch_ready_polls=1,
                max_remote_search_wait_seconds=1800,
            ),
            checkpoint=_single_waiting_search_checkpoint(submitted_at=datetime.now(timezone.utc)),
        )

    task = checkpoint["tasks"]["task-key-1"]
    assert checkpoint["status"] == "searching"
    assert task["status"] == "waiting"
    assert task["ready_poll_budget_exhausted"] is True
    assert "search_timeout" not in "\n".join(checkpoint["errors"])
    assert outcome.errors == []


def test_operation_native_public_web_remote_timeout_is_age_based() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        checkpoint, outcome = _poll_and_fetch_ready_public_web_tasks(
            plan=_single_query_plan(tempdir),
            search_provider=_NeverReadyBatchProvider(),
            options=PublicWebExperimentOptions(
                max_batch_ready_polls=1,
                max_remote_search_wait_seconds=1,
            ),
            checkpoint=_single_waiting_search_checkpoint(
                submitted_at=datetime.now(timezone.utc) - timedelta(seconds=5)
            ),
        )

    task = checkpoint["tasks"]["task-key-1"]
    assert checkpoint["status"] == "search_completed"
    assert task["status"] == "timeout"
    assert any("batch task not ready after 1s" in item for item in checkpoint["errors"])
    assert any("batch task not ready after 1s" in item for item in outcome.errors)


def test_operation_native_public_web_provider_pending_survives_remote_timeout() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        checkpoint, outcome = _poll_and_fetch_ready_public_web_tasks(
            plan=_single_query_plan(tempdir),
            search_provider=_ResettablePendingBatchProvider(),
            options=PublicWebExperimentOptions(
                max_batch_ready_polls=1,
                max_remote_search_wait_seconds=1,
                max_provider_pending_wait_seconds=60,
                max_provider_task_reset_attempts=0,
            ),
            checkpoint=_single_waiting_search_checkpoint(
                submitted_at=datetime.now(timezone.utc) - timedelta(seconds=5)
            ),
        )

    task = checkpoint["tasks"]["task-key-1"]
    assert checkpoint["status"] == "searching"
    assert checkpoint["stage"] == "waiting_remote_search"
    assert task["status"] == "waiting"
    assert task["provider_status_code"] == 40601
    assert task["provider_pending_wait_deferred"] is True
    assert checkpoint["phase_metrics"]["provider_pending_deferred_count"] == 1
    assert "search_timeout" not in "\n".join(checkpoint["errors"])
    assert outcome.errors == []


def test_operation_native_public_web_provider_pending_has_hard_timeout() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        checkpoint, outcome = _poll_and_fetch_ready_public_web_tasks(
            plan=_single_query_plan(tempdir),
            search_provider=_ResettablePendingBatchProvider(),
            options=PublicWebExperimentOptions(
                max_batch_ready_polls=1,
                max_remote_search_wait_seconds=1,
                max_provider_pending_wait_seconds=1,
                max_provider_task_reset_attempts=0,
            ),
            checkpoint=_single_waiting_search_checkpoint(
                submitted_at=datetime.now(timezone.utc) - timedelta(seconds=5)
            ),
        )

    task = checkpoint["tasks"]["task-key-1"]
    assert checkpoint["status"] == "search_completed"
    assert task["status"] == "timeout"
    assert any("provider pending task not ready after 1s" in item for item in checkpoint["errors"])
    assert any("provider pending task not ready after 1s" in item for item in outcome.errors)


def test_operation_native_public_web_resets_handed_task_per_query_after_poll_budget() -> None:
    provider = _ResettablePendingBatchProvider()
    with tempfile.TemporaryDirectory() as tempdir:
        checkpoint, outcome = _poll_and_fetch_ready_public_web_tasks(
            plan=_single_query_plan(tempdir),
            search_provider=provider,
            options=PublicWebExperimentOptions(
                max_batch_ready_polls=1,
                max_remote_search_wait_seconds=1800,
                max_provider_task_reset_attempts=1,
            ),
            checkpoint=_single_waiting_search_checkpoint(submitted_at=datetime.now(timezone.utc)),
        )

    task = checkpoint["tasks"]["task-key-1"]
    assert provider.submitted_specs
    assert len(provider.submitted_specs[0]) == 1
    assert provider.submitted_specs[0][0]["task_key"] == "task-key-1"
    assert checkpoint["status"] == "searching"
    assert task["status"] == "submitted"
    assert task["task_id"] == "task-key-1-reset-1"
    assert task["previous_task_ids"] == ["task-1"]
    assert task["provider_task_reset_attempt_count"] == 1
    assert task["provider_task_reset_previous_status_code"] == 40601
    assert checkpoint["phase_metrics"]["provider_task_reset_count"] == 1
    assert "search_timeout" not in "\n".join(checkpoint["errors"])
    assert outcome.errors == []


def _source_files() -> list[Path]:
    return all_source_files(SRC_ROOT)


def _test_files() -> list[Path]:
    return sorted(path for path in TEST_ROOT.glob("test_*.py") if path.is_file())


def _constant_block(source: str, constant_name: str) -> str:
    start = source.index(f"{constant_name} = (") if f"{constant_name} = (" in source else source.index(f"{constant_name} = [")
    terminator = "\n)" if f"{constant_name} = (" in source[start : start + 80] else "\n]"
    end = source.index(terminator, start) + len(terminator)
    return source[start:end]


def _call_attr_name(node: ast.AST) -> str:
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return str(node.func.attr or "")
    return ""


def _enclosing_function_names(tree: ast.AST) -> dict[ast.AST, str]:
    names: dict[ast.AST, str] = {}

    def visit(node: ast.AST, current_function: str = "<module>") -> None:
        next_function = current_function
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            next_function = node.name
        for child in ast.iter_child_nodes(node):
            names[child] = next_function
            visit(child, next_function)

    visit(tree)
    return names


def test_crm_public_web_normal_path_imports_crm_runtime_boundary() -> None:
    violations: list[str] = []
    for path in _source_files():
        if path in ALLOWED_DIRECT_IMPORT_FILES:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module in {
                "target_candidate_public_web",
                "sourcing_agent.target_candidate_public_web",
            }:
                imported = {alias.name for alias in node.names}
                forbidden = sorted(imported & FORBIDDEN_CRM_SYMBOLS_FROM_LEGACY_MODULE)
                if forbidden:
                    violations.append(f"{path.relative_to(REPO_ROOT)} imports CRM symbols from legacy module: {forbidden}")
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "sourcing_agent.target_candidate_public_web":
                        violations.append(
                            f"{path.relative_to(REPO_ROOT)} imports legacy module object directly; "
                            "normal CRM Public Web code must import crm_public_web_runtime"
                        )

    assert violations == []


def test_normal_source_does_not_call_legacy_target_public_web_storage_helpers() -> None:
    violations: list[str] = []
    for path in _source_files():
        if path in ALLOWED_LEGACY_STORAGE_ACCESS_FILES:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        function_names = _enclosing_function_names(tree)
        for node in ast.walk(tree):
            attr_name = _call_attr_name(node)
            if attr_name in LEGACY_PUBLIC_WEB_STORAGE_HELPERS:
                violations.append(
                    f"{path.relative_to(REPO_ROOT)}:{getattr(node, 'lineno', '?')} "
                    f"{function_names.get(node, '<module>')} calls legacy Public Web storage helper {attr_name}; "
                    "normal paths must use crm_public_web_* storage or a report-visible migration module"
                )

    assert violations == []


def test_legacy_public_web_test_rows_are_seeded_through_migration_helpers() -> None:
    violations: list[str] = []
    for path in _test_files():
        if path in ALLOWED_LEGACY_STORAGE_WRITE_TESTS:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        function_names = _enclosing_function_names(tree)
        for node in ast.walk(tree):
            attr_name = _call_attr_name(node)
            if attr_name in LEGACY_PUBLIC_WEB_STORAGE_WRITE_HELPERS:
                function_name = function_names.get(node, "<module>")
                if function_name in ALLOWED_LEGACY_STORAGE_WRITE_TEST_FUNCTIONS:
                    continue
                violations.append(
                    f"{path.relative_to(REPO_ROOT)}:{getattr(node, 'lineno', '?')} "
                    f"{function_name} writes legacy Public Web storage via {attr_name}; "
                    "tests must seed historical rows through legacy_public_web_storage.seed_* helpers"
                )

    assert violations == []


def test_legacy_target_public_web_storage_writes_fail_closed_without_migration_context(tmp_path) -> None:
    from sourcing_agent.storage import ControlPlaneStore

    store = ControlPlaneStore(tmp_path / "test.db")

    try:
        store.upsert_target_candidate_public_web_batch(
            {
                "batch_id": "normal-path-forbidden",
                "idempotency_key": "normal-path-forbidden",
                "status": "queued",
            }
        )
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("legacy target-candidate Public Web batch write should fail closed")

    assert "legacy_target_candidate_public_web_write_retired" in message
    assert store.list_target_candidate_public_web_batches() == []


def test_public_web_core_legacy_storage_access_is_retired_or_migration_only() -> None:
    path = SRC_ROOT / "public_web_runtime_core.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    function_names = _enclosing_function_names(tree)
    violations: list[str] = []
    observed_functions: set[str] = set()
    for node in ast.walk(tree):
        attr_name = _call_attr_name(node)
        if attr_name not in LEGACY_PUBLIC_WEB_STORAGE_HELPERS:
            continue
        function_name = function_names.get(node, "<module>")
        observed_functions.add(function_name)
        if function_name not in PUBLIC_WEB_CORE_ALLOWED_LEGACY_STORAGE_FUNCTIONS:
            violations.append(
                f"{path.relative_to(REPO_ROOT)}:{getattr(node, 'lineno', '?')} "
                f"{function_name} calls legacy Public Web storage helper {attr_name}; "
                "remaining legacy storage access must stay inside the explicit migration bridge"
            )

    assert violations == []
    assert observed_functions == set()
    assert "start_crm_public_web_batch" not in observed_functions
    assert "sync_crm_public_web_batch_summary" not in observed_functions
    assert "execute_crm_public_web_run_once" not in observed_functions
    assert "execute_crm_public_web_run_to_local_idle" not in observed_functions


def test_worker_daemon_keeps_crm_and_legacy_public_web_recovery_kinds_separate() -> None:
    source = (SRC_ROOT / "worker_daemon.py").read_text(encoding="utf-8")
    assert "from .crm_public_web_runtime import" in source
    assert "from .legacy_target_candidate_public_web_runtime import" not in source
    assert "from .target_candidate_public_web import" not in source
    assert "if recovery_kind == CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND" in source
    assert "crm_public_web_agent_worker_recovery_retired" in source
    assert "execute_crm_public_web_run_to_local_idle" not in source
    assert "if recovery_kind == RETIRED_TARGET_PUBLIC_WEB_WORKER_RECOVERY_KIND" in source
    assert "legacy_target_public_web_agent_worker_recovery_retired" in source
    assert "execute_target_candidate_public_web_run_to_local_idle" not in source


def test_crm_public_web_promotion_owner_rejects_non_latest_run_signal() -> None:
    _, promote_block = find_class_method("_promote_crm_public_web_signal_from_owner", class_name="CrmPublicWebOwner")
    find_class_method("_export_crm_public_web_archive_from_owner", class_name="CrmPublicWebOwner")

    assert "list_crm_public_web_runs(" in promote_block
    assert "limit=1" in promote_block
    assert "signal_run_workspace_id" in promote_block
    assert "signal_run_workspace_id != workspace_id" in promote_block
    assert "if not latest_run_id or signal_run_id != latest_run_id:" in promote_block
    assert "public_web_signal_not_latest_run" in promote_block
    assert "signal_run_id" in promote_block
    assert "latest_run_id" in promote_block
    assert "upsert_crm_public_web_promotion" in promote_block
    assert promote_block.index("public_web_signal_not_latest_run") < promote_block.index(
        "upsert_crm_public_web_promotion"
    )


def test_crm_public_web_detail_owner_requires_record_owned_run_before_signal_reads() -> None:
    _, detail_block = find_class_method("_get_crm_record_public_web_search_detail_from_owner", class_name="CrmPublicWebOwner")
    find_class_method("_list_crm_record_public_web_promotions_from_owner", class_name="CrmPublicWebOwner")

    assert "list_crm_public_web_runs(" in detail_block
    assert "crm_record_id=normalized_record_id" in detail_block
    assert "record_id=normalized_record_id" in detail_block
    assert detail_block.index("list_crm_public_web_runs(") < detail_block.index(
        "list_person_public_web_signals("
    )
    assert "get_crm_public_web_run(run_id=str(asset.get(\"latest_run_id\")" not in detail_block
    assert "dict(asset or {}).get(\"latest_run_id\")" not in detail_block


def test_crm_public_web_path_resource_resolves_record_workspace_without_default_filter() -> None:
    _, single_prepare_block = find_class_method("_prepare_single_crm_public_web_record_id", class_name="CrmPublicWebOwner")
    find_class_method("_prepare_crm_public_web_record_ids", class_name="CrmPublicWebOwner")
    _, detail_block = find_class_method("_get_crm_record_public_web_search_detail_from_owner", class_name="CrmPublicWebOwner")
    find_class_method("_list_crm_record_public_web_promotions_from_owner", class_name="CrmPublicWebOwner")

    assert "self.store.get_crm_record(normalized_record_id)" in single_prepare_block
    assert "_prepare_crm_public_web_record_ids([crm_record_id]" not in single_prepare_block
    assert "workspace_id=str(crm_record.get(\"workspace_id\") or \"default\")" in detail_block
    assert "list_crm_public_web_promotions(" in detail_block
    assert "list_person_public_web_signals(" in detail_block


def test_crm_public_web_action_validation_enforces_workspace_owner_boundary() -> None:
    _, action_block = find_class_method("_validate_crm_public_web_action_payload", class_name="CrmPublicWebOwner")
    _, runs_validation_block = find_class_method("_validate_crm_public_web_runs", class_name="CrmPublicWebOwner")
    _, workspace_requirement_block = find_class_method("_require_crm_public_web_body_workspace_id", class_name="CrmPublicWebOwner")

    assert "workspace_result = self._require_crm_public_web_body_workspace_id(normalized, operation=\"action\")" in action_block
    assert 'reason": "crm_public_web_workspace_id_required"' in workspace_requirement_block
    assert 'workspace_id = str(normalized.get("workspace_id") or "default").strip() or "default"' not in action_block
    assert "workspace_id=workspace_id" in action_block
    assert "self._validate_crm_public_web_runs(runs, workspace_id=workspace_id)" in action_block
    assert 'reason": "public_web_run_workspace_mismatch"' in runs_validation_block
    assert 'reason": "public_web_batch_workspace_mismatch"' in action_block
    assert "run_workspace_id != normalized_workspace_id" in runs_validation_block
    assert "record_workspace_id != normalized_workspace_id" in runs_validation_block


def test_crm_public_web_body_style_apis_require_explicit_workspace_id() -> None:
    required_calls = {
        "start": (
            "start_crm_record_public_web_search",
            'workspace_result = self._require_crm_public_web_body_workspace_id(normalized, operation="start")',
        ),
        "poll": (
            "list_crm_record_public_web_searches",
            'workspace_result = self._require_crm_public_web_body_workspace_id(normalized, operation="poll")',
        ),
        "export": (
            "export_crm_record_public_web_archive",
            'workspace_result = self._require_crm_public_web_body_workspace_id(normalized, operation="export")',
        ),
        "action_resolve": (
            "_resolve_crm_public_web_action_run_ids",
            "workspace_result = self._require_crm_public_web_body_workspace_id(query, operation=action_name)",
        ),
    }
    find_class_method("_crm_public_web_runs_with_materialized_signal_metrics", class_name="CrmPublicWebOwner")
    find_class_method("_crm_public_web_export_workflow_run_id", class_name="CrmPublicWebOwner")
    find_class_method("_interrupt_crm_public_web_workers_for_run", class_name="CrmPublicWebOwner")
    forbidden_default = 'workspace_id = str(normalized.get("workspace_id") or "default").strip() or "default"'
    forbidden_query_default = 'workspace_id = str(query.get("workspace_id") or "default").strip() or "default"'

    for _, (method_name, expected_call) in required_calls.items():
        _, block = find_class_method(method_name, class_name="CrmPublicWebOwner")
        assert expected_call in block
        assert forbidden_default not in block
        assert forbidden_query_default not in block


def test_crm_public_web_batch_poll_and_storage_are_workspace_scoped() -> None:
    _, list_runs_block = find_class_method("list_crm_public_web_runs", class_name="ControlPlaneStore")
    find_class_method("list_latest_crm_public_web_runs_by_record_ids", class_name="ControlPlaneStore")
    _, poll_block = find_class_method("_list_crm_public_web_searches_from_owner", class_name="CrmPublicWebOwner")
    find_class_method("_get_crm_record_profile_from_owner")

    batch_clause_index = list_runs_block.index("if batch_id:")
    record_clause_index = list_runs_block.index("if crm_record_id:")
    batch_scope_block = list_runs_block[batch_clause_index:record_clause_index]
    assert 'clauses_sqlite.append("workspace_id = ?")' in batch_scope_block
    assert 'clauses_pg.append("workspace_id = %s")' in batch_scope_block
    assert "batch_workspace_id != normalized_workspace_id" in poll_block
    assert 'reason": "public_web_batch_workspace_mismatch"' in poll_block


def test_crm_public_web_api_start_only_plans_queue_batch_command_before_owner_writes() -> None:
    _, start_block = find_class_method("start_crm_record_public_web_search", class_name="CrmPublicWebOwner")
    find_class_method("list_crm_record_public_web_searches", class_name="CrmPublicWebOwner")
    _, planner_block = find_class_method("_plan_crm_public_web_start_queue_batch_command", class_name="CrmPublicWebOwner")
    find_class_method("_plan_crm_public_web_operation_queue_batch_command", class_name="CrmPublicWebOwner")

    assert "_plan_crm_public_web_start_queue_batch_command(" in start_block
    assert "_drain_crm_public_web_queue_batch_commands(" in start_block
    assert "start_crm_public_web_batch(" not in start_block
    assert '"operation_planning_mode": "create_crm_public_web_batch_from_operation"' in planner_block
    assert "request_intent_key" in planner_block
    assert "build_crm_public_web_batch_idempotency_key(" in planner_block


def test_crm_public_web_latest_run_selection_is_not_updated_at_owned() -> None:
    _, storage_latest_block = find_class_method(
        "list_latest_crm_public_web_runs_by_record_ids", class_name="ControlPlaneStore"
    )
    # The original raw-text block also covered the next storage method up to
    # def get_company_public_web_asset_run; keep that coverage explicitly.
    _, storage_company_upsert_block = find_class_method(
        "upsert_company_public_web_asset_run", class_name="ControlPlaneStore"
    )
    find_class_method("get_company_public_web_asset_run", class_name="ControlPlaneStore")
    _, live_target_latest_block = find_class_method(
        "list_latest_target_candidate_public_web_runs_by_record_ids",
        class_name="LiveControlPlanePostgresAdapter",
    )
    _, live_crm_latest_block = find_class_method(
        "list_latest_crm_public_web_runs_by_record_ids",
        class_name="LiveControlPlanePostgresAdapter",
    )
    _, live_company_latest_block = find_class_method(
        "list_latest_company_public_web_asset_runs_by_company_keys",
        class_name="LiveControlPlanePostgresAdapter",
    )
    find_class_method("create_agent_trace_span", class_name="LiveControlPlanePostgresAdapter")

    for block in (storage_latest_block, live_crm_latest_block):
        assert "ORDER BY created_at DESC, run_id DESC" in block
        assert "PARTITION BY crm_record_id" in block
        assert "ORDER BY updated_at DESC, created_at DESC, run_id DESC" not in block
    assert "ORDER BY updated_at DESC, created_at DESC, run_id DESC" not in storage_company_upsert_block
    live_latest_blocks = {
        "target_candidate_public_web_runs": live_target_latest_block,
        "crm_public_web_runs": live_crm_latest_block,
        "company_public_web_asset_runs": live_company_latest_block,
    }
    for table_name, block in live_latest_blocks.items():
        assert "ensure_bootstrapped()" not in block
        assert "_ensure_table_write_schema" not in block
        assert "sync_runtime_control_plane_to_postgres" not in block
        assert f'_postgres_table_exists(cursor, "{table_name}")' in block
    _, phase_command_block = find_class_method("_run_crm_public_web_phase_command", class_name="CrmPublicWebOwner")
    assert "crm_public_web_phase_stale_run_superseded" in phase_command_block


def test_crm_public_web_pg_schema_enforces_batch_and_run_idempotency() -> None:
    live_pg_source = (SRC_ROOT / "control_plane_live_postgres.py").read_text(encoding="utf-8")
    sync_pg_source = (SRC_ROOT / "control_plane_postgres.py").read_text(encoding="utf-8")

    for source in (live_pg_source, sync_pg_source):
        assert "idx_crm_public_web_batches_idempotency_unique" in source
        assert "idx_crm_public_web_runs_idempotency_unique" in source
        assert "CREATE UNIQUE INDEX" in source
        assert "idempotency_key <> ''" in source or "idempotency_key != ''" in source


def test_crm_public_web_export_command_scopes_artifact_reuse_by_input_watermark() -> None:
    # The original raw-text "export planning" block spanned these methods; each
    # token is now asserted against the exact method that owns it.
    _, snapshot_block = find_class_method("_crm_public_web_export_record_input_snapshot", class_name="CrmPublicWebOwner")
    _, watermark_block = find_class_method("_crm_public_web_export_input_watermark", class_name="CrmPublicWebOwner")
    _, plan_export_block = find_class_method("_plan_crm_public_web_export_generate_command", class_name="CrmPublicWebOwner")
    _, contract_failure_block = find_class_method("_crm_public_web_export_command_contract_failure", class_name="CrmPublicWebOwner")
    _, export_run_block = find_class_method("_run_crm_public_web_export_generate_command", class_name="CrmPublicWebOwner")
    find_class_method("start_target_candidate_public_web_search", class_name="CrmPublicWebOwner")
    _, idempotency_block = find_module_def("export_crm_public_web_generate_idempotency_key")
    find_module_def("summarize_workflow_command_counts")

    assert "export_input_watermark_hash" in idempotency_block
    assert "export_input_watermark_hash" in plan_export_block
    assert "export_input_watermark" in watermark_block
    assert "list_crm_public_web_runs(" in watermark_block
    assert "list_person_public_web_signals(" in watermark_block
    assert "list_crm_public_web_promotions(" in watermark_block
    assert "exported_run_signals = (" in snapshot_block
    assert "_public_web_exportable_signals(" in snapshot_block
    assert "if not latest_run_status or _public_web_run_status_is_terminal(latest_run_status)" in snapshot_block
    assert "else []" in snapshot_block
    assert "exported_promotion_signals = _public_web_exportable_promotion_signals(raw_promotions)" in snapshot_block
    assert "exported_signals = _merge_public_web_export_signals(" in snapshot_block
    assert "exported_promotion_signal_count=len(exported_promotion_signals)" in snapshot_block
    assert "list_person_assertions(" in watermark_block
    assert "input_snapshot_digest" in snapshot_block
    assert "person_asset" in snapshot_block
    assert "phase_commands" in snapshot_block
    assert "include_record_inputs" in watermark_block
    assert "_record_export_inputs" in watermark_block
    assert "_crm_public_web_export_record_input_snapshot(" in watermark_block
    assert "crm_public_web_export_input_watermark_stale" in contract_failure_block
    assert plan_export_block.index("export_input_watermark_hash=export_input_watermark_hash") < plan_export_block.index(
        "append_event_and_reduce"
    )
    assert "_crm_public_web_export_command_contract_failure(" in export_run_block
    assert export_run_block.index("_crm_public_web_export_command_contract_failure(") < export_run_block.index(
        "_crm_public_web_export_payload_from_artifact"
    )
    assert "_crm_public_web_export_command_contract_failure(latest_command)" in export_run_block
    assert export_run_block.index("_crm_public_web_export_command_contract_failure(latest_command)") < export_run_block.index(
        "_start_workflow_command_activity_attempt"
    )
    assert export_run_block.index("_crm_public_web_export_command_contract_failure(latest_command)") < export_run_block.index(
        "_export_crm_public_web_archive_from_owner("
    )
    _, archive_owner_block = find_class_method("_export_crm_public_web_archive_from_owner", class_name="CrmPublicWebOwner")
    find_class_method("_validate_crm_public_web_action_payload", class_name="CrmPublicWebOwner")
    assert "current_export_input_watermark" in archive_owner_block
    assert "record_export_inputs_by_id" in archive_owner_block
    assert "current_export_input_watermark_hash" in archive_owner_block
    assert "crm_public_web_export_input_watermark_stale" in archive_owner_block
    assert "ordered_record_export_inputs" in archive_owner_block
    assert "crm_public_web_export_input_snapshot_missing" in archive_owner_block
    assert archive_owner_block.index("record_export_inputs_by_id") < archive_owner_block.index("zipfile.ZipFile")
    assert archive_owner_block.index("crm_public_web_export_input_watermark_stale") < archive_owner_block.index(
        "zipfile.ZipFile"
    )
    assert archive_owner_block.index("crm_public_web_export_input_snapshot_missing") < archive_owner_block.index(
        "zipfile.ZipFile"
    )
    zip_owner_write_block = archive_owner_block[archive_owner_block.index("zipfile.ZipFile") :]
    assert "_crm_public_web_export_record_input_snapshot(" not in zip_owner_write_block


def test_crm_public_web_export_allows_durable_promotions_during_nonterminal_latest_run() -> None:
    _, status_block = find_def("_public_web_export_record_status", class_name=None)
    find_def("_best_public_web_export_signal", class_name=None)

    assert "exported_promotion_signal_count: int = 0" in status_block
    assert "if not exported_signals" in status_block
    assert "if latest_run_status and not _public_web_run_status_is_terminal(latest_run_status)" in status_block
    assert "if exported_promotion_signal_count <= 0" in status_block
    assert "public_web_run_not_terminal" in status_block
    assert "export_basis" in status_block
    assert "durable_manual_promotion" in status_block
    assert "latest_run_terminal" in status_block


def test_crm_public_web_phase_command_summary_uses_fixed_phase_order() -> None:
    _, summary_block = find_class_method("_crm_public_web_phase_command_summaries_for_runs", class_name="CrmPublicWebOwner")
    find_class_method("_command_owned_item_result")

    assert '"phase_order": list(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES)' in summary_block
    assert '"phase_count": phase_count' in summary_block
    assert '"completed_phase_count": completed_phase_count' in summary_block
    assert '"command_count": phase_count' in summary_block
    assert '"materialized_command_count": len(items)' in summary_block


def test_crm_public_web_export_api_never_sends_binary_for_failed_owner_result() -> None:
    # The FastAPI transport registers a nested handler for the canonical export
    # route; slice its function body (nested defs are out of reach for the
    # class-method AST helper).
    api_source = (SRC_ROOT / "api.py").read_text(encoding="utf-8")
    handler_start = api_source.index("def post_crm_public_web_export")
    handler_block = api_source[handler_start : api_source.index("\n    def ", handler_start + 1)]

    assert 'result.get("status") != "ok"' in handler_block
    assert "HTTPStatus.CONFLICT" in handler_block
    assert 'not result.get("body")' in handler_block
    # Binary response is only reachable after both guards.
    assert handler_block.index('result.get("status") != "ok"') < handler_block.index("_bytes_response")
    assert handler_block.index('not result.get("body")') < handler_block.index("_bytes_response")


def test_crm_runtime_imports_physical_public_web_core_not_target_facade() -> None:
    crm_source = (SRC_ROOT / "crm_public_web_runtime.py").read_text(encoding="utf-8")
    legacy_source = (SRC_ROOT / "legacy_target_candidate_public_web_runtime.py").read_text(encoding="utf-8")

    assert "from .public_web_runtime_core import" in crm_source
    assert "from .target_candidate_public_web import" not in crm_source
    assert "from .public_web_runtime_core import" in legacy_source
    assert "from .target_candidate_public_web import" not in legacy_source


def test_retired_target_public_web_facade_is_physically_deleted() -> None:
    assert not (SRC_ROOT / "target_candidate_public_web.py").exists()


def test_pg_writer_schema_does_not_require_legacy_public_web_tables() -> None:
    source = (SRC_ROOT / "control_plane_live_postgres.py").read_text(encoding="utf-8")
    snapshot_source = (SRC_ROOT / "control_plane_postgres.py").read_text(encoding="utf-8")

    assert 'if _postgres_table_exists(cursor, "target_candidate_public_web_batches")' in source
    assert 'if _postgres_table_exists(cursor, "target_candidate_public_web_runs")' in source
    assert 'if _postgres_table_exists(cursor, "target_candidate_public_web_promotions")' in source
    assert "LEGACY_TARGET_PUBLIC_WEB_TABLES = (" in snapshot_source
    assert "LEGACY_TARGET_PUBLIC_WEB_TABLES" in source
    assert '"target_candidate_public_web_batches",' not in _constant_block(source, "CONTROL_PLANE_LIVE_TABLES")
    assert '"target_candidate_public_web_runs",' not in _constant_block(source, "CONTROL_PLANE_LIVE_TABLES")
    assert '"target_candidate_public_web_promotions",' not in _constant_block(source, "CONTROL_PLANE_LIVE_TABLES")
    assert '"target_candidate_public_web_batches",' not in _constant_block(snapshot_source, "DEFAULT_CONTROL_PLANE_TABLES")
    assert '"target_candidate_public_web_runs",' not in _constant_block(snapshot_source, "DEFAULT_CONTROL_PLANE_TABLES")
    assert '"target_candidate_public_web_promotions",' not in _constant_block(
        snapshot_source,
        "DEFAULT_CONTROL_PLANE_TABLES",
    )


def test_legacy_target_public_web_override_env_is_removed(monkeypatch) -> None:
    from sourcing_agent.legacy_target_candidate_public_web_runtime import (
        LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS_ENV,
        legacy_target_public_web_execution_enabled,
    )

    monkeypatch.setenv(LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS_ENV, "1")

    assert not legacy_target_public_web_execution_enabled()


def test_orchestrator_has_no_legacy_target_public_web_execution_helpers() -> None:
    forbidden_symbols = {
        "_LEGACY_TARGET_PUBLIC_WEB_JOB_TYPE",
        "_LEGACY_TARGET_PUBLIC_WEB_WORKER_RECOVERY_KIND",
        "_resolve_target_candidate_public_web_action_run_ids",
        "_interrupt_target_candidate_public_web_workers_for_run",
        "_ensure_target_candidate_public_web_job",
        "_queue_target_candidate_public_web_workers",
        "_target_candidate_public_web_job_request",
        "_resolve_public_web_target_candidate_record",
        "def _queue_crm_public_web_workers",
    }
    for path in all_source_files():
        source = path.read_text(encoding="utf-8")
        for symbol in forbidden_symbols:
            assert symbol not in source, (
                f"{path.relative_to(REPO_ROOT)} contains retired legacy target Public Web symbol {symbol!r}"
            )

    find_class_method("start_target_candidate_public_web_search", class_name="CrmPublicWebOwner")
    find_class_method("export_target_candidate_public_web_archive", class_name="CrmPublicWebOwner")
    find_def("_legacy_target_public_web_orchestrator_disabled_result", class_name=None)
    find_class_method("_ensure_crm_public_web_job", class_name="CrmPublicWebOwner")
    find_class_method("_plan_crm_public_web_run_phase_command", class_name="CrmPublicWebOwner")
    find_class_method("_drain_crm_public_web_phase_commands", class_name="CrmPublicWebOwner")
    _, recovery_block = find_class_method("run_worker_recovery_once")
    assert '"crm_public_web_queue_batch"' in recovery_block
    assert "callback=lambda: self._drain_crm_public_web_queue_batch_commands(payload)" in recovery_block
    assert '"crm_public_web_phase_commands"' in recovery_block
    assert "callback=lambda: self._drain_crm_public_web_phase_commands(payload)" in recovery_block
    assert "claim and execute ready CRM Public Web per-run phase workflow_commands only" in recovery_block


def test_api_legacy_target_public_web_endpoints_are_unconditional_gone() -> None:
    source = (SRC_ROOT / "api.py").read_text(encoding="utf-8")

    assert "_legacy_target_public_web_endpoint_allowed()" not in source
    assert "orchestrator.start_target_candidate_public_web_search(" not in source
    assert "orchestrator.cancel_target_candidate_public_web_search(" not in source
    assert "orchestrator.retry_target_candidate_public_web_search(" not in source
    assert "orchestrator.list_target_candidate_public_web_searches(" not in source
    assert "orchestrator.get_target_candidate_public_web_search_detail(" not in source
    assert "orchestrator.list_target_candidate_public_web_promotions(" not in source
    assert "orchestrator.promote_target_candidate_public_web_signal(" not in source
    assert "orchestrator.export_target_candidate_public_web_archive(" not in source
