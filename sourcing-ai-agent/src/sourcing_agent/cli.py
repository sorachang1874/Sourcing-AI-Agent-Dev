from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

from .acquisition import AcquisitionEngine
from .agent_runtime import AgentRuntimeCoordinator
from .api import create_server
from .asset_catalog import AssetCatalog
from .asset_reuse_planning import backfill_organization_asset_registry_for_company
from .asset_sync import AssetBundleManager
from .candidate_artifacts import (
    backfill_structured_timeline_for_company_assets,
    build_company_candidate_artifacts,
    repair_missing_company_candidate_artifacts,
    repair_projected_profile_signals_in_company_candidate_artifacts,
)
from .cloud_asset_import import hydrate_cloud_generation, import_cloud_assets
from .company_asset_completion import CompanyAssetCompletionManager
from .company_asset_supplement import CompanyAssetSupplementManager
from .control_plane_postgres import (
    export_control_plane_snapshot,
    sync_control_plane_snapshot_to_postgres,
    sync_runtime_control_plane_to_postgres,
)
from .local_postgres import describe_control_plane_runtime
from .model_provider import OpenAICompatibleChatModelClient, QwenResponsesModelClient, build_model_client
from .object_storage import build_object_storage_client
from .orchestrator import (
    SourcingOrchestrator,
    _runner_subprocess_env,
    _spawn_detached_process,
    _workflow_runner_process_alive,
)
from .outreach_layering import analyze_company_outreach_layers
from .profile_registry_backfill import backfill_linkedin_profile_registry
from .public_web_quality import evaluate_public_web_quality_paths, write_public_web_quality_report
from .public_web_search import (
    PublicWebExperimentOptions,
    build_sample_target_candidate_records,
    load_target_candidates_from_json,
    run_target_candidate_public_web_experiment,
)
from .runtime_rebuild import rebuild_runtime_control_plane
from .search_provider import build_search_provider
from .semantic_provider import build_semantic_provider
from .service_daemon import SingleInstanceError, WorkerDaemonService
from .settings import load_settings
from .storage import ControlPlaneStore
from .workflow_submission import (
    normalize_workflow_submission_payload,
    workflow_runtime_uses_managed_runner,
)


class HostedWorkflowSubmissionError(RuntimeError):
    pass


def _runner_environment(project_root: Path) -> dict[str, str]:
    return _runner_subprocess_env(project_root)


def _resolve_launch_path(project_root: Path, raw_path: str, *, default_name: str = "") -> Path:
    candidate = str(raw_path or "").strip()
    if candidate:
        path = Path(candidate).expanduser()
        if not path.is_absolute():
            path = project_root / path
        return path.resolve()
    if not default_name:
        raise ValueError("launch path is required")
    return (project_root / "runtime" / "service_logs" / default_name).resolve()


def launch_detached_command(
    *,
    command: list[str],
    log_path: str,
    cwd: str = "",
    description: str = "",
    startup_wait_seconds: float = 0.15,
    status_path: str = "",
) -> dict[str, Any]:
    if not command:
        raise ValueError("launch_detached_command requires a non-empty command")
    catalog = AssetCatalog.discover()
    project_root = catalog.project_root
    runner_env = _runner_environment(project_root)
    resolved_cwd = _resolve_launch_path(project_root, cwd) if str(cwd or "").strip() else project_root.resolve()
    resolved_log_path = _resolve_launch_path(project_root, log_path, default_name="detached-launch.log")
    result = _spawn_detached_process(
        command=command,
        cwd=resolved_cwd,
        log_path=resolved_log_path,
        env=runner_env,
        startup_wait_seconds=float(startup_wait_seconds or 0.15),
    )
    payload: dict[str, Any] = {
        "description": str(description or "").strip(),
        "cwd": str(resolved_cwd),
        "launched_at": datetime.now(timezone.utc).isoformat(),
        **result,
    }
    resolved_status_path = None
    if str(status_path or "").strip():
        resolved_status_path = _resolve_launch_path(project_root, status_path)
        resolved_status_path.parent.mkdir(parents=True, exist_ok=True)
        resolved_status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        payload["status_path"] = str(resolved_status_path)
    return payload


def build_runtime_store(settings) -> ControlPlaneStore:
    return ControlPlaneStore(settings.db_path)


def build_control_plane_runtime_summary() -> dict[str, Any]:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    summary = describe_control_plane_runtime(
        base_dir=catalog.project_root,
        runtime_dir=settings.runtime_dir,
    )
    store = build_runtime_store(settings)
    summary.update(
        {
            "project_root": str(catalog.project_root),
            "settings_db_path": str(settings.db_path),
            "settings_db_path_exists": settings.db_path.exists(),
            "control_plane_postgres_live_mode": store.control_plane_postgres_live_mode(),
            "sqlite_shadow_backend": store.sqlite_shadow_backend(),
            "sqlite_shadow_connect_target": store.sqlite_shadow_connect_target(),
            "sqlite_shadow_ephemeral": store.sqlite_shadow_is_ephemeral(),
            "bootstrap_candidate_store_enabled": store.bootstrap_candidate_store_enabled(),
            "candidate_documents_fallback_enabled": store.candidate_documents_fallback_enabled(),
        }
    )
    summary["control_plane_storage_banner"] = _control_plane_storage_banner(summary)
    return summary


def _control_plane_storage_banner(summary: dict[str, Any]) -> dict[str, Any]:
    live_mode = str(summary.get("control_plane_postgres_live_mode") or "").strip().lower()
    sqlite_shadow_backend = str(summary.get("sqlite_shadow_backend") or "").strip().lower()
    if live_mode != "postgres_only":
        return {
            "status": "non_pg_control_plane",
            "severity": "error",
            "message": (
                "This runtime is not PG-only. Disk SQLite live control-plane mode is retired; "
                "migrate state into Postgres before live or hosted use."
            ),
            "migration_exit": (
                "Set SOURCING_CONTROL_PLANE_POSTGRES_DSN, "
                "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only, "
                "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1, and migrate any remaining SQLite-only state into PG."
            ),
        }
    if sqlite_shadow_backend == "disk":
        return {
            "status": "disk_shadow_rejected",
            "severity": "error",
            "message": "PG-only live mode must use an ephemeral shared_memory compatibility shadow, not disk-backed storage.",
            "migration_exit": "Set SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory before serving live traffic.",
        }
    return {
        "status": "pg_only",
        "severity": "ok",
        "message": "Postgres is the live control plane; the compatibility shadow is ephemeral.",
        "migration_exit": "",
    }


def build_orchestrator() -> SourcingOrchestrator:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    store = build_runtime_store(settings)
    model_client = build_model_client(settings.model_provider, settings.qwen)
    semantic_provider = build_semantic_provider(settings.semantic)
    agent_runtime = AgentRuntimeCoordinator(store)
    acquisition_engine = AcquisitionEngine(catalog, settings, store, model_client, worker_runtime=agent_runtime)
    return SourcingOrchestrator(
        catalog=catalog,
        store=store,
        jobs_dir=settings.jobs_dir,
        model_client=model_client,
        semantic_provider=semantic_provider,
        acquisition_engine=acquisition_engine,
        agent_runtime=agent_runtime,
    )


def build_asset_bundle_manager() -> AssetBundleManager:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    return AssetBundleManager(catalog.project_root, settings.runtime_dir)


def build_object_storage():
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    return build_object_storage_client(settings.object_storage)


def build_asset_completion_manager() -> CompanyAssetCompletionManager:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    store = build_runtime_store(settings)
    model_client = build_model_client(settings.model_provider, settings.qwen)
    return CompanyAssetCompletionManager(
        runtime_dir=settings.runtime_dir,
        store=store,
        settings=settings,
        model_client=model_client,
    )


def build_asset_supplement_manager() -> CompanyAssetSupplementManager:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    store = build_runtime_store(settings)
    model_client = build_model_client(settings.model_provider, settings.qwen)
    return CompanyAssetSupplementManager(
        runtime_dir=settings.runtime_dir,
        store=store,
        settings=settings,
        model_client=model_client,
    )


def run_target_candidate_public_web_experiment_command(args) -> dict[str, Any]:
    provider_mode = str(args.external_provider_mode or "").strip().lower()
    if provider_mode:
        os.environ["SOURCING_EXTERNAL_PROVIDER_MODE"] = provider_mode
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    store = build_runtime_store(settings)
    if str(args.input_file or "").strip():
        target_candidates = load_target_candidates_from_json(args.input_file)
    elif bool(args.sample):
        target_candidates = build_sample_target_candidate_records(limit=max(1, int(args.limit or 10)))
    else:
        requested_record_ids = [str(item or "").strip() for item in list(args.record_id or []) if str(item or "").strip()]
        if requested_record_ids:
            target_candidates = [
                record
                for record_id in requested_record_ids
                if (record := store.get_target_candidate(record_id)) is not None
            ]
        else:
            target_candidates = store.list_target_candidates(
                job_id=str(args.job_id or "").strip(),
                history_id=str(args.history_id or "").strip(),
                follow_up_status=str(args.follow_up_status or "").strip(),
                limit=max(1, int(args.limit or 10)),
            )
        if not target_candidates and bool(args.sample_if_empty):
            target_candidates = build_sample_target_candidate_records(limit=max(1, int(args.limit or 10)))
    if not target_candidates:
        return {
            "status": "not_found",
            "reason": "no target candidates matched; use --sample or --input-file for a self-contained experiment",
        }
    output_root = (
        Path(str(args.output_dir)).expanduser()
        if str(args.output_dir or "").strip()
        else settings.runtime_dir / "public_web" / "experiments"
    )
    options = PublicWebExperimentOptions(
        max_queries_per_candidate=max(1, int(args.max_queries_per_candidate or 10)),
        max_results_per_query=max(1, int(args.max_results_per_query or 10)),
        max_entry_links_per_candidate=max(1, int(args.max_entry_links_per_candidate or 40)),
        max_fetches_per_candidate=max(0, int(args.max_fetches_per_candidate or 0)),
        max_ai_evidence_documents=max(1, int(args.max_ai_evidence_documents or 8)),
        fetch_content=not bool(args.no_fetch_content),
        extract_contact_signals=not bool(args.no_contact_extraction),
        ai_extraction=str(args.ai_extraction or "auto").strip().lower() or "auto",
        timeout_seconds=max(1, int(args.timeout_seconds or settings.search.timeout_seconds or 30)),
        use_batch_search=not bool(args.no_batch_search),
        batch_ready_poll_interval_seconds=max(0.0, float(args.batch_ready_poll_interval_seconds or 0.0)),
        max_batch_ready_polls=max(1, int(args.max_batch_ready_polls or 1)),
        max_concurrent_fetches_per_candidate=max(1, int(args.max_concurrent_fetches_per_candidate or 1)),
        max_concurrent_candidate_analyses=max(1, int(args.max_concurrent_candidate_analyses or 1)),
    )
    search_provider = build_search_provider(settings.search)
    model_client = build_model_client(settings.model_provider, settings.qwen)
    summary = run_target_candidate_public_web_experiment(
        target_candidates=target_candidates[: max(1, int(args.limit or len(target_candidates)))],
        search_provider=search_provider,
        model_client=model_client,
        output_dir=output_root,
        options=options,
        run_id=str(args.run_id or "").strip(),
    )
    return {
        "status": summary.get("status", "completed"),
        "provider_mode": os.environ.get("SOURCING_EXTERNAL_PROVIDER_MODE", "live"),
        "summary": summary,
    }


def evaluate_public_web_quality_command(args) -> dict[str, Any]:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    input_paths = [Path(str(item)).expanduser() for item in list(args.experiment_dir or []) if str(item or "").strip()]
    if not input_paths:
        input_paths = [settings.runtime_dir / "public_web" / "experiments"]
    report = evaluate_public_web_quality_paths(input_paths)
    written: dict[str, str] = {}
    output_dir_value = str(args.output_dir or "").strip()
    if output_dir_value:
        written = write_public_web_quality_report(report, Path(output_dir_value).expanduser())
        report = {**report, "written": written}
    if bool(args.fail_on_high_risk) and int(dict(report.get("summary") or {}).get("high_issue_count") or 0) > 0:
        report = {**report, "status": "failed"}
    if bool(getattr(args, "summary_only", False)):
        summary_report: dict[str, Any] = {
            "status": report.get("status", "ok"),
            "summary": report.get("summary", {}),
            "input_paths": [str(path) for path in input_paths],
        }
        if written:
            summary_report["written"] = written
        return summary_report
    return report


def spawn_workflow_runner(job_id: str, *, auto_job_daemon: bool) -> dict[str, object]:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    runner_env = _runner_environment(catalog.project_root)
    log_dir = settings.runtime_dir / "service_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"workflow-runner-{job_id}.log"
    command = [
        sys.executable,
        "-m",
        "sourcing_agent.cli",
        "supervise-workflow" if auto_job_daemon else "execute-workflow",
        "--job-id",
        job_id,
    ]
    if auto_job_daemon:
        command.append("--auto-job-daemon")

    return {
        "job_id": job_id,
        **_spawn_detached_process(
            command=command,
            cwd=catalog.project_root,
            log_path=log_path,
            env=runner_env,
        ),
    }


def _is_process_alive(pid: int) -> bool:
    return _workflow_runner_process_alive(pid)


def _wait_for_workflow_runner_progress(
    orchestrator: SourcingOrchestrator,
    *,
    job_id: str,
    pid: int,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, object]:
    return orchestrator._wait_for_workflow_runner_progress(  # noqa: SLF001
        job_id=job_id,
        pid=pid,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )


def start_workflow_runner_with_handshake(
    orchestrator: SourcingOrchestrator,
    *,
    job_id: str,
    auto_job_daemon: bool,
    handshake_timeout_seconds: float,
    poll_seconds: float = 0.1,
    max_attempts: int = 2,
) -> dict[str, object]:
    return orchestrator._start_workflow_runner_with_handshake(  # noqa: SLF001
        job_id=job_id,
        auto_job_daemon=auto_job_daemon,
        handshake_timeout_seconds=handshake_timeout_seconds,
        poll_seconds=poll_seconds,
        max_attempts=max_attempts,
    )


def run_server_runtime_watchdog_once(
    orchestrator: SourcingOrchestrator,
    *,
    shared_service_name: str = "worker-recovery-daemon",
    hosted_service_name: str = "server-runtime-watchdog",
) -> dict[str, Any]:
    return orchestrator.run_hosted_runtime_watchdog_once(
        {
            "shared_service_name": shared_service_name,
            "hosted_runtime_watchdog_service_name": hosted_service_name,
            "hosted_runtime_source": hosted_service_name,
        }
    )


def start_server_runtime_watchdog(
    orchestrator: SourcingOrchestrator,
    *,
    poll_seconds: float = 15.0,
    shared_service_name: str = "worker-recovery-daemon",
    hosted_service_name: str = "server-runtime-watchdog",
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    def _loop() -> None:
        while not stop_event.is_set():
            try:
                service = WorkerDaemonService(
                    runtime_dir=orchestrator.runtime_dir,
                    recovery_callback=lambda _: run_server_runtime_watchdog_once(
                        orchestrator,
                        shared_service_name=shared_service_name,
                        hosted_service_name=hosted_service_name,
                    ),
                    service_name=hosted_service_name,
                    poll_seconds=max(1.0, float(poll_seconds or 15.0)),
                    callback_payload={"hosted_runtime_source": hosted_service_name},
                )

                def _stop_bridge() -> None:
                    stop_event.wait()
                    service.request_stop()

                threading.Thread(target=_stop_bridge, name=f"{hosted_service_name}-stop", daemon=True).start()
                service.run_forever()
            except SingleInstanceError:
                return
            except Exception:
                return

    thread = threading.Thread(
        target=_loop,
        name="server-runtime-watchdog",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def start_shared_recovery_service(
    orchestrator: SourcingOrchestrator,
    *,
    poll_seconds: float = 5.0,
    service_name: str = "worker-recovery-daemon",
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    def _loop() -> None:
        while not stop_event.is_set():
            try:
                service = WorkerDaemonService(
                    runtime_dir=orchestrator.runtime_dir,
                    recovery_callback=lambda payload: orchestrator.run_worker_recovery_once(payload),
                    service_name=service_name,
                    poll_seconds=max(1.0, float(poll_seconds or 5.0)),
                    stale_after_seconds=180,
                    total_limit=4,
                    callback_payload={
                        "workflow_resume_stale_after_seconds": 60,
                        "workflow_queue_resume_stale_after_seconds": 60,
                        "runtime_heartbeat_source": "shared_recovery_daemon",
                        "runtime_heartbeat_service_name": service_name,
                        "runtime_heartbeat_interval_seconds": 60,
                    },
                )

                def _stop_bridge() -> None:
                    stop_event.wait()
                    service.request_stop()

                threading.Thread(target=_stop_bridge, name=f"{service_name}-stop", daemon=True).start()
                service.run_forever()
            except SingleInstanceError:
                return
            except Exception:
                return

    thread = threading.Thread(
        target=_loop,
        name=service_name,
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def _resolve_hosted_api_base_url(explicit_base_url: str = "") -> str:
    base_url = (
        str(explicit_base_url or "").strip()
        or str(os.environ.get("SOURCING_AGENT_API_BASE_URL") or "").strip()
        or "http://127.0.0.1:8765"
    )
    return base_url.rstrip("/")


def _submit_hosted_workflow_request(
    payload: dict[str, Any],
    *,
    base_url: str,
    timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    request_payload = normalize_workflow_submission_payload(
        {**dict(payload), "runtime_execution_mode": "hosted"},
        default_runtime_execution_mode="hosted",
        hosted_auto_job_daemon=False,
    )
    workflow_request = urllib_request.Request(
        f"{base_url}/api/workflows",
        data=json.dumps(request_payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
    try:
        with opener.open(workflow_request, timeout=max(1.0, float(timeout_seconds or 15.0))) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8")
        except Exception:
            detail = ""
        raise HostedWorkflowSubmissionError(
            f"HTTP {exc.code} from hosted API {base_url}/api/workflows" + (f": {detail}" if detail else "")
        ) from exc
    except urllib_error.URLError as exc:
        raise HostedWorkflowSubmissionError(f"could not reach hosted API at {base_url} ({exc.reason})") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Sourcing AI Agent backend MVP")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap", help="Load local assets into the runtime store")
    subparsers.add_parser(
        "show-control-plane-runtime",
        help="Print the resolved Postgres control-plane runtime configuration for the current workspace",
    )

    run_job_parser = subparsers.add_parser("run-job", help="Run a sourcing job from JSON file")
    run_job_parser.add_argument("--file", required=True, help="Path to job JSON")
    run_job_parser.add_argument(
        "--asset-view",
        choices=["canonical_merged", "strict_roster_only"],
        default="",
        help="Optional retrieval asset view override",
    )
    run_job_parser.add_argument(
        "--must-have-facet", action="append", default=[], help="Optional hard facet filter; repeatable"
    )
    run_job_parser.add_argument(
        "--must-have-primary-role-bucket",
        action="append",
        default=[],
        help="Optional hard primary role bucket filter; repeatable",
    )

    plan_parser = subparsers.add_parser("plan", help="Create a sourcing plan from JSON file")
    plan_parser.add_argument("--file", required=True, help="Path to workflow request JSON")
    plan_parser.add_argument(
        "--asset-view",
        choices=["canonical_merged", "strict_roster_only"],
        default="",
        help="Optional retrieval asset view override",
    )
    plan_parser.add_argument(
        "--must-have-facet", action="append", default=[], help="Optional hard facet filter; repeatable"
    )
    plan_parser.add_argument(
        "--must-have-primary-role-bucket",
        action="append",
        default=[],
        help="Optional hard primary role bucket filter; repeatable",
    )

    explain_workflow_parser = subparsers.add_parser(
        "explain-workflow",
        help="Dry-run ingress normalization, planning, reuse matching, and lane preview without creating a job",
    )
    explain_workflow_parser.add_argument("--file", default="", help="Path to workflow request JSON")
    explain_workflow_parser.add_argument(
        "--plan-review-id", type=int, default=0, help="Approved/pending plan review id to explain directly"
    )
    explain_workflow_parser.add_argument(
        "--asset-view",
        choices=["canonical_merged", "strict_roster_only"],
        default="",
        help="Optional retrieval asset view override",
    )
    explain_workflow_parser.add_argument(
        "--must-have-facet", action="append", default=[], help="Optional hard facet filter; repeatable"
    )
    explain_workflow_parser.add_argument(
        "--must-have-primary-role-bucket",
        action="append",
        default=[],
        help="Optional hard primary role bucket filter; repeatable",
    )

    review_plan_parser = subparsers.add_parser(
        "review-plan", help="Review a plan review session from JSON file or natural-language instruction"
    )
    review_plan_parser.add_argument("--file", default="", help="Path to plan review JSON")
    review_plan_parser.add_argument(
        "--review-id", type=int, default=0, help="Plan review session id used with --instruction"
    )
    review_plan_parser.add_argument("--instruction", default="", help="Natural-language review instruction")
    review_plan_parser.add_argument("--reviewer", default="", help="Reviewer name used with --instruction")
    review_plan_parser.add_argument(
        "--action",
        default="approved",
        choices=["approved", "rejected", "needs_changes"],
        help="Review action used with --instruction",
    )
    review_plan_parser.add_argument("--notes", default="", help="Optional review notes used with --instruction")
    review_plan_parser.add_argument(
        "--preview", action="store_true", help="Print the structured review payload without applying it"
    )

    refine_results_parser = subparsers.add_parser(
        "refine-results", help="Refine an existing completed result set with natural-language filtering instructions"
    )
    refine_results_parser.add_argument("--file", default="", help="Path to refinement JSON")
    refine_results_parser.add_argument("--job-id", default="", help="Baseline completed job id used with --instruction")
    refine_results_parser.add_argument("--instruction", default="", help="Natural-language refinement instruction")
    refine_results_parser.add_argument(
        "--preview", action="store_true", help="Print the compiled refinement request without executing the rerun"
    )

    show_plan_reviews_parser = subparsers.add_parser("show-plan-reviews", help="Show persisted plan review sessions")
    show_plan_reviews_parser.add_argument("--target-company", default="", help="Optional target company filter")
    show_plan_reviews_parser.add_argument(
        "--brief", action="store_true", help="Show a compact summary instead of the full persisted payload"
    )

    workflow_parser = subparsers.add_parser("start-workflow", help="Start a workflow and print the queued job metadata")
    workflow_parser.add_argument("--file", default="", help="Path to workflow request JSON")
    workflow_parser.add_argument(
        "--plan-review-id", type=int, default=0, help="Approved plan review id to execute directly"
    )
    workflow_parser.add_argument(
        "--asset-view",
        choices=["canonical_merged", "strict_roster_only"],
        default="",
        help="Optional retrieval asset view override",
    )
    workflow_parser.add_argument(
        "--must-have-facet", action="append", default=[], help="Optional hard facet filter; repeatable"
    )
    workflow_parser.add_argument(
        "--must-have-primary-role-bucket",
        action="append",
        default=[],
        help="Optional hard primary role bucket filter; repeatable",
    )
    workflow_parser.add_argument(
        "--blocking", action="store_true", help="Run the workflow synchronously in the current CLI process"
    )
    workflow_parser.add_argument(
        "--runtime-execution-mode",
        choices=["hosted", "managed_subprocess", "runner_managed", "detached_sidecar"],
        default="hosted",
        help="Workflow runtime mode. Hosted is the default cloud/server path; managed_subprocess keeps the legacy detached runner flow.",
    )
    workflow_parser.add_argument(
        "--hosted-api-base-url",
        default="",
        help="Hosted API base URL used when runtime_execution_mode=hosted. Defaults to SOURCING_AGENT_API_BASE_URL or http://127.0.0.1:8765.",
    )
    workflow_parser.add_argument(
        "--hosted-api-timeout-seconds",
        type=float,
        default=15.0,
        help="Timeout for hosted workflow submission over HTTP.",
    )
    workflow_parser.add_argument(
        "--no-auto-job-daemon", action="store_true", help="Do not auto-start a dedicated job-scoped recovery daemon"
    )

    execute_workflow_parser = subparsers.add_parser("execute-workflow", help="Internal: execute a queued workflow job")
    execute_workflow_parser.add_argument("--job-id", required=True, help="Queued workflow job identifier")
    execute_workflow_parser.add_argument(
        "--auto-job-daemon", action="store_true", help="Auto-start a dedicated job-scoped recovery daemon"
    )
    supervise_workflow_parser = subparsers.add_parser(
        "supervise-workflow", help="Internal: supervise workflow execution until it settles"
    )
    supervise_workflow_parser.add_argument("--job-id", required=True, help="Queued workflow job identifier")
    supervise_workflow_parser.add_argument(
        "--auto-job-daemon", action="store_true", help="Continuously run workflow recovery while supervising"
    )
    supervise_workflow_parser.add_argument(
        "--poll-seconds", type=float, default=2.0, help="Sleep between supervisor cycles"
    )
    supervise_workflow_parser.add_argument(
        "--max-ticks", type=int, default=0, help="Stop after N cycles; 0 means until settled"
    )

    show_job_parser = subparsers.add_parser("show-job", help="Show stored job metadata and results")
    show_job_parser.add_argument("--job-id", required=True, help="Job identifier")

    show_progress_parser = subparsers.add_parser("show-progress", help="Show workflow progress summary for a job")
    show_progress_parser.add_argument("--job-id", required=True, help="Job identifier")

    show_system_progress_parser = subparsers.add_parser(
        "show-system-progress", help="Show unified runtime/workflow/profile-sync/object-sync progress"
    )
    show_system_progress_parser.add_argument(
        "--active-limit", type=int, default=10, help="Max active workflow jobs to include"
    )
    show_system_progress_parser.add_argument(
        "--object-sync-limit", type=int, default=20, help="Max object sync progress snapshots to include"
    )
    show_system_progress_parser.add_argument(
        "--profile-registry-lookback-hours", type=int, default=24, help="Profile registry metrics lookback window"
    )
    show_system_progress_parser.add_argument(
        "--force-refresh", action="store_true", help="Bypass cached runtime metrics snapshot"
    )

    show_trace_parser = subparsers.add_parser("show-trace", help="Show agent runtime trace for a job")
    show_trace_parser.add_argument("--job-id", required=True, help="Job identifier")

    show_workers_parser = subparsers.add_parser("show-workers", help="Show autonomous workers for a job")
    show_workers_parser.add_argument("--job-id", required=True, help="Job identifier")

    show_scheduler_parser = subparsers.add_parser("show-scheduler", help="Show worker scheduler state for a job")
    show_scheduler_parser.add_argument("--job-id", required=True, help="Job identifier")

    cleanup_duplicates_parser = subparsers.add_parser(
        "cleanup-workflow-duplicates",
        help="Supersede older in-flight workflow jobs when a newer completed job exists for the same request",
    )
    cleanup_duplicates_parser.add_argument("--target-company", default="", help="Optional target company filter")
    cleanup_duplicates_parser.add_argument("--active-limit", type=int, default=200, help="Max active jobs to inspect")

    cleanup_blocked_residue_parser = subparsers.add_parser(
        "cleanup-blocked-workflow-residue",
        help="Supersede blocked/acquiring workflow residue when a newer same-family workflow already completed",
    )
    cleanup_blocked_residue_parser.add_argument("--target-company", default="", help="Optional target company filter")
    cleanup_blocked_residue_parser.add_argument(
        "--active-limit", type=int, default=200, help="Max blocked jobs to inspect"
    )
    cleanup_blocked_residue_parser.add_argument(
        "--dry-run", action="store_true", help="Preview cleanup candidates without changing state"
    )

    supersede_jobs_parser = subparsers.add_parser(
        "supersede-workflow-jobs",
        help="Force-supersede specific workflow jobs and retire their workers",
    )
    supersede_jobs_parser.add_argument(
        "--job-id", action="append", required=True, help="Workflow job id to supersede; repeatable"
    )
    supersede_jobs_parser.add_argument("--replacement-job-id", default="", help="Optional replacement workflow job id")
    supersede_jobs_parser.add_argument(
        "--reason", default="Superseded by operator cleanup.", help="Reason recorded on the retired jobs"
    )

    show_recoverable_parser = subparsers.add_parser(
        "show-recoverable-workers", help="Show recoverable workers across jobs"
    )
    show_recoverable_parser.add_argument(
        "--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable"
    )
    show_recoverable_parser.add_argument("--lane-id", default="", help="Optional lane filter")
    show_recoverable_parser.add_argument("--job-id", default="", help="Optional job filter")
    show_recoverable_parser.add_argument("--limit", type=int, default=100, help="Max workers to return")

    cleanup_recoverable_parser = subparsers.add_parser(
        "cleanup-recoverable-workers",
        help="Retire stale recoverable workers, typically those hanging off terminal workflow jobs",
    )
    cleanup_recoverable_parser.add_argument(
        "--stale-after-seconds", type=int, default=180, help="Minimum staleness threshold"
    )
    cleanup_recoverable_parser.add_argument("--lane-id", default="", help="Optional lane filter")
    cleanup_recoverable_parser.add_argument("--job-id", default="", help="Optional job filter")
    cleanup_recoverable_parser.add_argument("--target-company", default="", help="Optional target company filter")
    cleanup_recoverable_parser.add_argument(
        "--parent-job-status", action="append", default=[], help="Optional parent workflow status filter; repeatable"
    )
    cleanup_recoverable_parser.add_argument("--limit", type=int, default=200, help="Max workers to inspect")
    cleanup_recoverable_parser.add_argument(
        "--dry-run", action="store_true", help="Preview cleanup candidates without changing state"
    )
    cleanup_recoverable_parser.add_argument(
        "--include-missing-jobs",
        action="store_true",
        help="Also allow orphan workers whose parent job no longer exists",
    )
    cleanup_recoverable_parser.add_argument(
        "--terminal-workflows-only",
        action="store_true",
        default=True,
        help="Only clean workers whose parent workflow job is terminal",
    )
    cleanup_recoverable_parser.add_argument(
        "--status",
        default="",
        help="Optional override terminal worker status; defaults to cancelled or superseded based on parent job",
    )
    cleanup_recoverable_parser.add_argument(
        "--reason",
        default="Retired stale recoverable worker during operator cleanup.",
        help="Cleanup reason recorded in worker metadata",
    )

    interrupt_worker_parser = subparsers.add_parser("interrupt-worker", help="Request interrupt for a worker")
    interrupt_worker_parser.add_argument("--worker-id", required=True, type=int, help="Worker identifier")

    daemon_once_parser = subparsers.add_parser(
        "run-worker-daemon-once", help="Run one cross-process worker recovery pass"
    )
    daemon_once_parser.add_argument("--owner-id", default="", help="Optional daemon owner identifier")
    daemon_once_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_once_parser.add_argument(
        "--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable"
    )
    daemon_once_parser.add_argument(
        "--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle"
    )
    daemon_once_parser.add_argument("--job-id", default="", help="Optional job filter")

    daemon_forever_parser = subparsers.add_parser(
        "run-worker-daemon", help="Run the cross-process worker recovery daemon loop"
    )
    daemon_forever_parser.add_argument("--owner-id", default="", help="Optional daemon owner identifier")
    daemon_forever_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_forever_parser.add_argument(
        "--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable"
    )
    daemon_forever_parser.add_argument(
        "--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle"
    )
    daemon_forever_parser.add_argument("--job-id", default="", help="Optional job filter")
    daemon_forever_parser.add_argument("--poll-seconds", type=float, default=5.0, help="Sleep between cycles")
    daemon_forever_parser.add_argument("--max-ticks", type=int, default=0, help="Stop after N cycles; 0 means forever")

    daemon_service_parser = subparsers.add_parser(
        "run-worker-daemon-service", help="Run worker recovery as a single-instance service loop"
    )
    daemon_service_parser.add_argument(
        "--service-name", default="worker-recovery-daemon", help="Persistent service instance name"
    )
    daemon_service_parser.add_argument("--owner-id", default="", help="Optional daemon owner identifier")
    daemon_service_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_service_parser.add_argument(
        "--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable"
    )
    daemon_service_parser.add_argument(
        "--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle"
    )
    daemon_service_parser.add_argument("--job-id", default="", help="Optional job filter")
    daemon_service_parser.add_argument(
        "--job-scoped", action="store_true", help="Auto-stop once the target job reaches terminal state"
    )
    daemon_service_parser.add_argument("--poll-seconds", type=float, default=5.0, help="Sleep between cycles")
    daemon_service_parser.add_argument("--max-ticks", type=int, default=0, help="Stop after N cycles; 0 means forever")
    daemon_service_parser.add_argument(
        "--workflow-auto-resume-stale-after-seconds",
        type=int,
        default=60,
        help="Workflow running/acquiring auto-resume threshold used by the daemon",
    )
    daemon_service_parser.add_argument(
        "--workflow-queue-auto-takeover-stale-after-seconds",
        type=int,
        default=60,
        help="Workflow queued auto-takeover threshold used by the daemon",
    )

    hosted_watchdog_service_parser = subparsers.add_parser(
        "run-server-runtime-watchdog-service",
        help="Run the hosted runtime watchdog as a single-instance service loop",
    )
    hosted_watchdog_service_parser.add_argument(
        "--service-name",
        default="server-runtime-watchdog",
        help="Persistent hosted runtime watchdog service name",
    )
    hosted_watchdog_service_parser.add_argument(
        "--shared-service-name",
        default="worker-recovery-daemon",
        help="Shared recovery service name monitored by the watchdog",
    )
    hosted_watchdog_service_parser.add_argument(
        "--poll-seconds",
        type=float,
        default=15.0,
        help="Sleep between watchdog cycles",
    )
    hosted_watchdog_service_parser.add_argument(
        "--max-ticks",
        type=int,
        default=0,
        help="Stop after N cycles; 0 means forever",
    )

    daemon_status_parser = subparsers.add_parser(
        "show-daemon-status", help="Show persistent worker daemon service status"
    )
    daemon_status_parser.add_argument(
        "--service-name", default="worker-recovery-daemon", help="Persistent service instance name"
    )

    daemon_unit_parser = subparsers.add_parser(
        "write-worker-daemon-systemd-unit", help="Write a systemd unit for the worker daemon service"
    )
    daemon_unit_parser.add_argument(
        "--service-name", default="worker-recovery-daemon", help="Persistent service instance name"
    )
    daemon_unit_parser.add_argument("--output-path", default="", help="Optional output path for the generated unit")
    daemon_unit_parser.add_argument(
        "--python-bin", default="/usr/bin/env python3", help="Python executable used by ExecStart"
    )
    daemon_unit_parser.add_argument("--user-name", default="", help="Optional system user for the service")
    daemon_unit_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_unit_parser.add_argument(
        "--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable"
    )
    daemon_unit_parser.add_argument(
        "--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle"
    )
    daemon_unit_parser.add_argument("--poll-seconds", type=float, default=5.0, help="Sleep between cycles")

    feedback_parser = subparsers.add_parser("record-feedback", help="Record criteria feedback from JSON file")
    feedback_parser.add_argument("--file", required=True, help="Path to feedback JSON")

    review_suggestion_parser = subparsers.add_parser(
        "review-suggestion", help="Review a pattern suggestion from JSON file"
    )
    review_suggestion_parser.add_argument("--file", required=True, help="Path to suggestion review JSON")

    review_manual_item_parser = subparsers.add_parser(
        "review-manual-item", help="Review a manual review queue item from JSON file"
    )
    review_manual_item_parser.add_argument("--file", required=True, help="Path to manual review JSON")

    synthesize_manual_item_parser = subparsers.add_parser(
        "synthesize-manual-review", help="Generate and cache an evidence synthesis for one manual review item"
    )
    synthesize_manual_item_parser.add_argument(
        "--review-item-id", required=True, type=int, help="Manual review item id"
    )
    synthesize_manual_item_parser.add_argument(
        "--force-refresh", action="store_true", help="Ignore any cached synthesis and recompute it"
    )

    confidence_policy_parser = subparsers.add_parser(
        "configure-confidence-policy", help="Create, freeze, override, or clear a confidence policy control"
    )
    confidence_policy_parser.add_argument("--file", required=True, help="Path to confidence policy control JSON")

    recompile_parser = subparsers.add_parser("recompile-criteria", help="Recompile criteria from JSON file")
    recompile_parser.add_argument("--file", required=True, help="Path to recompile request JSON")

    pattern_parser = subparsers.add_parser("show-criteria", help="Show persisted criteria patterns and feedback")
    pattern_parser.add_argument("--target-company", default="", help="Optional target company filter")

    manual_review_parser = subparsers.add_parser("show-manual-review", help="Show manual review queue items")
    manual_review_parser.add_argument("--target-company", default="", help="Optional target company filter")
    manual_review_parser.add_argument("--job-id", default="", help="Optional job identifier filter")

    company_snapshot_bundle_parser = subparsers.add_parser(
        "export-company-snapshot-bundle", help="Export one company snapshot as a portable asset bundle"
    )
    company_snapshot_bundle_parser.add_argument("--company", required=True, help="Company key or name")
    company_snapshot_bundle_parser.add_argument(
        "--snapshot-id", default="", help="Optional snapshot id; defaults to latest"
    )
    company_snapshot_bundle_parser.add_argument("--output-dir", default="", help="Optional bundle export directory")

    company_handoff_bundle_parser = subparsers.add_parser(
        "export-company-handoff-bundle",
        help="Export a company handoff bundle including snapshots and related runtime assets",
    )
    company_handoff_bundle_parser.add_argument("--company", required=True, help="Company key or name")
    company_handoff_bundle_parser.add_argument("--output-dir", default="", help="Optional bundle export directory")
    company_handoff_bundle_parser.add_argument(
        "--without-live-tests", action="store_true", help="Do not include matching live test assets"
    )
    company_handoff_bundle_parser.add_argument(
        "--without-manual-review", action="store_true", help="Do not include manual review assets"
    )
    company_handoff_bundle_parser.add_argument(
        "--without-jobs", action="store_true", help="Do not include matching job JSON files"
    )

    control_plane_snapshot_bundle_parser = subparsers.add_parser(
        "export-control-plane-snapshot-bundle",
        help="Export the Postgres control-plane snapshot as a portable asset bundle",
    )
    control_plane_snapshot_bundle_parser.add_argument(
        "--output-dir", default="", help="Optional bundle export directory"
    )

    company_artifact_parser = subparsers.add_parser(
        "build-company-candidate-artifacts",
        help="Materialize normalized and reusable company candidate artifacts from snapshot candidate documents and historical snapshot evidence",
    )
    company_artifact_parser.add_argument("--company", required=True, help="Company key or name")
    company_artifact_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id; defaults to latest")
    company_artifact_parser.add_argument("--output-dir", default="", help="Optional artifact output directory")

    layered_outreach_parser = subparsers.add_parser(
        "segment-company-outreach-layers", help="Build layered outreach segmentation from company candidate JSON assets"
    )
    layered_outreach_parser.add_argument("--company", required=True, help="Company key or name")
    layered_outreach_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id; defaults to latest")
    layered_outreach_parser.add_argument(
        "--asset-view",
        choices=["canonical_merged", "strict_roster_only"],
        default="canonical_merged",
        help="Candidate asset view to read",
    )
    layered_outreach_parser.add_argument(
        "--query", default="", help="Optional natural-language query used as context for AI verification"
    )
    layered_outreach_parser.add_argument(
        "--max-ai-verifications", type=int, default=80, help="Max candidates to AI-verify (0 disables)"
    )
    layered_outreach_parser.add_argument(
        "--ai-workers", type=int, default=8, help="Concurrent AI verification worker count"
    )
    layered_outreach_parser.add_argument(
        "--ai-max-retries", type=int, default=2, help="Retries for each failed AI verification request"
    )
    layered_outreach_parser.add_argument(
        "--ai-retry-backoff-seconds", type=float, default=0.8, help="Base backoff seconds for AI retry"
    )
    layered_outreach_parser.add_argument(
        "--provider",
        choices=["auto", "openai", "qwen"],
        default="openai",
        help="AI provider selection for verification",
    )
    layered_outreach_parser.add_argument(
        "--no-ai", action="store_true", help="Disable model-based verification and only run deterministic layers"
    )
    layered_outreach_parser.add_argument(
        "--summary-only", action="store_true", help="Print only compact summary fields"
    )
    layered_outreach_parser.add_argument(
        "--output-dir", default="", help="Optional output directory for layered analysis artifacts"
    )

    complete_company_assets_parser = subparsers.add_parser(
        "complete-company-assets",
        help="Continue company asset accumulation using known profile URLs and low-cost exploration",
    )
    complete_company_assets_parser.add_argument("--company", required=True, help="Company key or name")
    complete_company_assets_parser.add_argument(
        "--snapshot-id", default="", help="Optional snapshot id; defaults to latest"
    )
    complete_company_assets_parser.add_argument(
        "--profile-detail-limit", type=int, default=12, help="Max candidates to complete via known LinkedIn URLs"
    )
    complete_company_assets_parser.add_argument(
        "--exploration-limit", type=int, default=3, help="Max unresolved candidates to explore via low-cost web search"
    )
    complete_company_assets_parser.add_argument(
        "--without-artifacts", action="store_true", help="Skip normalized/reusable artifact build"
    )

    supplement_company_assets_parser = subparsers.add_parser(
        "supplement-company-assets",
        help="Incrementally supplement an existing company snapshot with former search seed and/or profile enrichment",
    )
    supplement_company_assets_parser.add_argument("--company", required=True, help="Company key or name")
    supplement_company_assets_parser.add_argument(
        "--snapshot-id", default="", help="Optional snapshot id; defaults to latest"
    )
    supplement_company_assets_parser.add_argument(
        "--import-local-bootstrap-package",
        action="store_true",
        help="Import the Anthropic local bootstrap package into project-owned storage and merge it into the target snapshot",
    )
    supplement_company_assets_parser.add_argument(
        "--skip-project-local-package-sync",
        action="store_true",
        help="Do not first sync the Anthropic bootstrap package into project-local storage before merge",
    )
    supplement_company_assets_parser.add_argument(
        "--rebuild-linkedin-stage-1",
        action="store_true",
        help="Rebuild candidate_documents(.linkedin_stage_1) from current roster + existing search-seed snapshot, then normalize the snapshot baseline",
    )
    supplement_company_assets_parser.add_argument(
        "--run-former-search-seed",
        action="store_true",
        help="Run Harvest former-member search seed against the existing snapshot",
    )
    supplement_company_assets_parser.add_argument(
        "--former-search-limit", type=int, default=25, help="Requested former search result target"
    )
    supplement_company_assets_parser.add_argument(
        "--former-search-pages", type=int, default=1, help="Requested Harvest profile-search pages"
    )
    supplement_company_assets_parser.add_argument(
        "--former-query", action="append", default=[], help="Optional former search query text; repeatable"
    )
    supplement_company_assets_parser.add_argument(
        "--former-keyword", action="append", default=[], help="Optional former search keyword filter; repeatable"
    )
    supplement_company_assets_parser.add_argument(
        "--profile-scope",
        choices=["none", "current", "former", "all"],
        default="none",
        help="Which membership scope to profile-enrich",
    )
    supplement_company_assets_parser.add_argument(
        "--profile-limit", type=int, default=0, help="Max profiles to enrich; 0 means all selected"
    )
    supplement_company_assets_parser.add_argument(
        "--profile-only-missing-detail",
        action="store_true",
        help="Only enrich missing-detail backlog in the selected scope (default is all known URLs)",
    )
    supplement_company_assets_parser.add_argument(
        "--profile-all-known-urls",
        action="store_true",
        help="Deprecated compatibility flag: force all-known-URLs mode (already default)",
    )
    supplement_company_assets_parser.add_argument(
        "--profile-force-refresh",
        action="store_true",
        help="Bypass local profile cache and refetch selected LinkedIn profiles",
    )
    supplement_company_assets_parser.add_argument(
        "--repair-current-roster-profile-refs",
        action="store_true",
        help="Restore current-roster canonical profile refs from the original harvest_company_employees visible asset",
    )
    supplement_company_assets_parser.add_argument(
        "--repair-current-roster-registry-aliases",
        action="store_true",
        help="Backfill registry aliases for current-roster raw LinkedIn URLs by reusing historical local harvest_profiles",
    )
    supplement_company_assets_parser.add_argument(
        "--without-artifacts",
        action="store_true",
        help="Skip rebuilding normalized/reusable artifacts after supplement",
    )

    intake_excel_parser = subparsers.add_parser(
        "intake-excel",
        help="Import spreadsheet contacts, dedupe against local assets, and optionally fetch missing LinkedIn profiles",
    )
    intake_excel_parser.add_argument("--file", required=True, help="Path to the Excel workbook")
    intake_excel_parser.add_argument("--target-company", default="", help="Optional target company for snapshot attach")
    intake_excel_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id for snapshot attach")
    intake_excel_parser.add_argument(
        "--attach-to-snapshot",
        action="store_true",
        help="Merge resolved contacts directly into the target snapshot and refresh artifacts",
    )
    continue_excel_intake_parser = subparsers.add_parser(
        "continue-excel-intake",
        help="Continue an Excel intake manual-review row by selecting a local candidate or a fetched profile",
    )
    continue_excel_intake_parser.add_argument(
        "--file", required=True, help="Path to a JSON payload describing intake_id + decisions"
    )
    promote_asset_default_parser = subparsers.add_parser(
        "promote-asset-default-pointer",
        help="Promote a canonical company/scoped asset snapshot as the default pointer",
    )
    promote_asset_default_parser.add_argument("--company", required=True, help="Company key or name")
    promote_asset_default_parser.add_argument("--snapshot-id", required=True, help="Snapshot id to promote")
    promote_asset_default_parser.add_argument("--scope-kind", default="company", help="Asset scope kind")
    promote_asset_default_parser.add_argument("--scope-key", default="", help="Asset scope key")
    promote_asset_default_parser.add_argument("--asset-kind", default="company_asset", help="Asset kind")
    promote_asset_default_parser.add_argument(
        "--lifecycle-status",
        default="canonical",
        help="Lifecycle status; only canonical is promotable",
    )
    promote_asset_default_parser.add_argument("--coverage-proof-json", default="{}", help="Inline coverage proof JSON")
    promote_asset_default_parser.add_argument("--coverage-proof-file", default="", help="Path to coverage proof JSON")
    promote_asset_default_parser.add_argument("--promoted-by-job-id", default="", help="Source job id")

    backfill_registry_parser = subparsers.add_parser(
        "backfill-linkedin-profile-registry",
        help="Backfill linkedin_profile_registry from historical runtime/company_assets/*/*/harvest_profiles JSON payloads",
    )
    backfill_registry_parser.add_argument("--company", default="", help="Optional company key filter (e.g. anthropic)")
    backfill_registry_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id filter")
    backfill_registry_parser.add_argument("--checkpoint-path", default="", help="Optional checkpoint file path")
    backfill_registry_parser.add_argument(
        "--progress-interval", type=int, default=200, help="Emit progress every N processed files"
    )
    backfill_registry_parser.add_argument(
        "--no-resume", action="store_true", help="Do not resume from prior checkpoint"
    )

    backfill_org_asset_registry_parser = subparsers.add_parser(
        "backfill-organization-asset-registry",
        help="Backfill organization_asset_registry from historical normalized company artifacts",
    )
    backfill_org_asset_registry_parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Company key or canonical name; repeatable",
    )
    backfill_org_asset_registry_parser.add_argument(
        "--asset-view",
        choices=["canonical_merged", "strict_roster_only"],
        default="canonical_merged",
        help="Organization asset view to register",
    )
    rebuild_runtime_control_plane_parser = subparsers.add_parser(
        "rebuild-runtime-control-plane",
        help="Rebuild Postgres-first control-plane rows from runtime/company_assets and runtime/jobs without rerunning providers",
    )
    rebuild_runtime_control_plane_parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Optional company key or canonical name filter; repeatable",
    )
    rebuild_runtime_control_plane_parser.add_argument(
        "--snapshot-id",
        default="",
        help="Optional snapshot id filter",
    )
    rebuild_runtime_control_plane_parser.add_argument(
        "--skip-company-assets",
        action="store_true",
        help="Skip rebuilding organization asset / generation / membership rows from runtime/company_assets",
    )
    rebuild_runtime_control_plane_parser.add_argument(
        "--skip-jobs",
        action="store_true",
        help="Skip rebuilding jobs and job_result_views from runtime/jobs JSON payloads",
    )
    rebuild_runtime_control_plane_parser.add_argument(
        "--skip-missing-artifact-repair",
        action="store_true",
        help="Do not rebuild missing normalized manifests before replaying company-asset control-plane rows",
    )
    repair_candidate_artifacts_parser = subparsers.add_parser(
        "repair-company-candidate-artifacts",
        help="Repair legacy snapshots that have root candidate_documents.json but missing normalized candidate artifacts",
    )
    repair_candidate_artifacts_parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Optional company key or canonical name filter; repeatable",
    )
    repair_candidate_artifacts_parser.add_argument(
        "--snapshot-id",
        default="",
        help="Optional snapshot id filter",
    )
    repair_candidate_artifacts_parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Rebuild normalized candidate artifacts even when they already exist",
    )

    structured_timeline_backfill_parser = subparsers.add_parser(
        "backfill-structured-timeline",
        help="Backfill structured experience/education timeline into existing company candidate artifacts",
    )
    structured_timeline_backfill_parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Optional company key or canonical name filter; repeatable",
    )
    structured_timeline_backfill_parser.add_argument(
        "--snapshot-id",
        default="",
        help="Optional snapshot id filter",
    )
    structured_timeline_backfill_parser.add_argument(
        "--skip-profile-registry-backfill",
        action="store_true",
        help="Skip linkedin profile registry backfill before rewriting candidate artifacts",
    )
    structured_timeline_backfill_parser.add_argument(
        "--profile-progress-interval",
        type=int,
        default=200,
        help="Emit profile registry backfill progress every N processed files",
    )
    structured_timeline_backfill_parser.add_argument(
        "--profile-no-resume",
        action="store_true",
        help="Do not resume prior linkedin profile registry backfill checkpoints",
    )
    structured_timeline_backfill_parser.add_argument(
        "--skip-registry-refresh",
        action="store_true",
        help="Rewrite candidate artifacts without refreshing organization asset registry entries",
    )

    projected_signal_repair_parser = subparsers.add_parser(
        "repair-profile-signal-projection",
        help="Project profile signals already present in candidate metadata into top-level artifact fields",
    )
    projected_signal_repair_parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Optional company key or canonical name filter; repeatable",
    )
    projected_signal_repair_parser.add_argument(
        "--snapshot-id",
        default="",
        help="Optional snapshot id filter",
    )

    profile_registry_metrics_parser = subparsers.add_parser(
        "show-linkedin-profile-registry-metrics", help="Show profile registry cache/retry/queue metrics"
    )
    profile_registry_metrics_parser.add_argument(
        "--lookback-hours", type=int, default=24, help="Metrics lookback window in hours; 0 means all history"
    )

    restore_bundle_parser = subparsers.add_parser(
        "restore-asset-bundle", help="Restore a previously exported asset bundle into runtime"
    )
    restore_bundle_parser.add_argument("--manifest", required=True, help="Path to bundle_manifest.json")
    restore_bundle_parser.add_argument("--target-runtime-dir", default="", help="Optional runtime dir override")
    restore_bundle_parser.add_argument(
        "--conflict", choices=["skip", "overwrite", "error"], default="skip", help="How to handle existing files"
    )

    upload_bundle_parser = subparsers.add_parser(
        "upload-asset-bundle", help="Upload an exported asset bundle to configured object storage"
    )
    upload_bundle_parser.add_argument("--manifest", required=True, help="Path to bundle_manifest.json")
    upload_bundle_parser.add_argument(
        "--max-workers", type=int, default=0, help="Optional concurrent upload worker count; 0 uses config default"
    )
    upload_bundle_parser.add_argument(
        "--no-resume", action="store_true", help="Force re-upload even when the remote object already exists"
    )
    upload_bundle_parser.add_argument(
        "--archive-mode", choices=["auto", "none", "tar", "tar.gz"], default="auto", help="Bundle payload upload mode"
    )

    publish_generation_parser = subparsers.add_parser(
        "publish-candidate-generation",
        help="Publish a manifest-first candidate generation to configured object storage",
    )
    publish_generation_parser.add_argument("--company", required=True, help="Company key or name")
    publish_generation_parser.add_argument(
        "--snapshot-id", default="", help="Optional snapshot id; defaults to latest authoritative snapshot"
    )
    publish_generation_parser.add_argument(
        "--asset-view",
        choices=["canonical_merged", "strict_roster_only"],
        default="canonical_merged",
        help="Candidate asset view to publish",
    )
    publish_generation_parser.add_argument(
        "--max-workers", type=int, default=0, help="Optional concurrent upload worker count; 0 uses config default"
    )
    publish_generation_parser.add_argument(
        "--no-resume", action="store_true", help="Force re-upload even when the remote objects already exist"
    )
    publish_generation_parser.add_argument(
        "--include-compatibility-exports",
        action="store_true",
        help="Also publish legacy compatibility monoliths when present",
    )

    delete_bundle_parser = subparsers.add_parser(
        "delete-asset-bundle", help="Delete a remote asset bundle and prune bundle indexes"
    )
    delete_bundle_parser.add_argument("--bundle-kind", required=True, help="Bundle kind, e.g. company_snapshot")
    delete_bundle_parser.add_argument("--bundle-id", required=True, help="Bundle id")
    delete_bundle_parser.add_argument(
        "--max-workers", type=int, default=0, help="Optional concurrent delete worker count; 0 uses config default"
    )
    delete_bundle_parser.add_argument(
        "--keep-local-index", action="store_true", help="Do not prune the local bundle index entry"
    )

    download_bundle_parser = subparsers.add_parser(
        "download-asset-bundle", help="Download an asset bundle from configured object storage"
    )
    download_bundle_parser.add_argument("--bundle-kind", required=True, help="Bundle kind, e.g. company_handoff")
    download_bundle_parser.add_argument("--bundle-id", required=True, help="Bundle id")
    download_bundle_parser.add_argument("--output-dir", default="", help="Optional local export directory")
    download_bundle_parser.add_argument(
        "--max-workers", type=int, default=0, help="Optional concurrent download worker count; 0 uses config default"
    )
    download_bundle_parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Force re-download even when the local payload file already matches the manifest",
    )

    import_cloud_assets_parser = subparsers.add_parser(
        "import-cloud-assets",
        help="Download/restore a cloud asset bundle and automatically repair runtime registries for imported company snapshots",
    )
    import_cloud_assets_parser.add_argument("--manifest", default="", help="Optional local bundle_manifest.json path")
    import_cloud_assets_parser.add_argument(
        "--bundle-kind", default="", help="Bundle kind used when downloading from object storage"
    )
    import_cloud_assets_parser.add_argument(
        "--bundle-id", default="", help="Bundle id used when downloading from object storage"
    )
    import_cloud_assets_parser.add_argument(
        "--output-dir", default="", help="Optional download directory used with --bundle-kind/--bundle-id"
    )
    import_cloud_assets_parser.add_argument(
        "--max-workers", type=int, default=0, help="Optional concurrent download worker count; 0 uses config default"
    )
    import_cloud_assets_parser.add_argument(
        "--no-resume", action="store_true", help="Disable download resume when fetching the remote bundle"
    )
    import_cloud_assets_parser.add_argument(
        "--target-runtime-dir", default="", help="Optional runtime dir override for runtime bundle restore"
    )
    import_cloud_assets_parser.add_argument(
        "--target-db-path",
        default="",
        help="Optional control-plane store path override for post-import registry repair",
    )
    import_cloud_assets_parser.add_argument(
        "--conflict",
        choices=["skip", "overwrite", "error"],
        default="skip",
        help="How to handle existing runtime files during runtime bundle restore",
    )
    import_cloud_assets_parser.add_argument(
        "--company", action="append", default=[], help="Optional imported company scope override; repeatable"
    )
    import_cloud_assets_parser.add_argument("--snapshot-id", default="", help="Optional imported snapshot id override")
    import_cloud_assets_parser.add_argument(
        "--asset-view",
        choices=["canonical_merged", "strict_roster_only"],
        default="canonical_merged",
        help="Asset view used when warming organization registries",
    )
    import_cloud_assets_parser.add_argument(
        "--generation-key",
        default="",
        help="Optional candidate generation key override; when set, import-cloud-assets can hydrate generation-first without bundle manifests",
    )
    import_cloud_assets_parser.add_argument(
        "--disable-generation-first",
        action="store_true",
        help="Force bundle/import semantics even when a matching candidate generation exists in generation indexes",
    )
    import_cloud_assets_parser.add_argument(
        "--disable-legacy-bundle-fallback",
        action="store_true",
        help="Fail instead of restoring legacy bundle payloads when generation-first resolution does not succeed",
    )
    import_cloud_assets_parser.add_argument(
        "--disable-local-link",
        action="store_true",
        help="When hydrating generation-first, always download from object storage instead of link-first hydration from local canonical assets",
    )
    import_cloud_assets_parser.add_argument(
        "--skip-artifact-repair",
        action="store_true",
        help="Skip candidate artifact repair after runtime bundle restore",
    )
    import_cloud_assets_parser.add_argument(
        "--skip-org-warmup", action="store_true", help="Skip organization registry warmup after runtime bundle restore"
    )
    import_cloud_assets_parser.add_argument(
        "--skip-profile-registry-backfill",
        action="store_true",
        help="Skip linkedin profile registry backfill after runtime bundle restore",
    )
    import_cloud_assets_parser.add_argument(
        "--profile-progress-interval", type=int, default=200, help="Progress interval used by profile registry backfill"
    )
    import_cloud_assets_parser.add_argument(
        "--profile-no-resume",
        action="store_true",
        help="Do not resume prior linkedin profile registry backfill checkpoints",
    )

    hydrate_generation_parser = subparsers.add_parser(
        "hydrate-candidate-generation",
        help="Hydrate a published candidate generation into the configured hot-cache store",
    )
    hydrate_generation_parser.add_argument(
        "--manifest", default="", help="Optional local generation_manifest.json path"
    )
    hydrate_generation_parser.add_argument(
        "--company", default="", help="Company key or name used when downloading remote manifest"
    )
    hydrate_generation_parser.add_argument(
        "--company-key", default="", help="Optional explicit company key override for remote generation prefix"
    )
    hydrate_generation_parser.add_argument(
        "--snapshot-id", default="", help="Snapshot id used when downloading remote manifest"
    )
    hydrate_generation_parser.add_argument(
        "--asset-view",
        choices=["canonical_merged", "strict_roster_only"],
        default="canonical_merged",
        help="Candidate asset view to hydrate",
    )
    hydrate_generation_parser.add_argument(
        "--generation-key",
        default="",
        help="Optional generation key used when downloading remote manifest; if omitted the latest indexed generation for the company/snapshot/view will be used when available",
    )
    hydrate_generation_parser.add_argument(
        "--max-workers", type=int, default=0, help="Optional concurrent hydrate worker count; 0 uses config default"
    )
    hydrate_generation_parser.add_argument(
        "--no-resume", action="store_true", help="Force re-hydration even when hot-cache files already match"
    )
    hydrate_generation_parser.add_argument(
        "--disable-local-link",
        action="store_true",
        help="Always download from object storage instead of link-first hydration from canonical source paths when available",
    )

    export_control_plane_parser = subparsers.add_parser(
        "export-control-plane-snapshot",
        help="Export control-plane state plus generation indexes as a Postgres restore/migration snapshot",
    )
    export_control_plane_parser.add_argument(
        "--output",
        default="",
        help="Optional output path; defaults to runtime/object_sync/control_plane/control_plane_snapshot.json",
    )
    export_control_plane_parser.add_argument(
        "--source-backend",
        choices=["postgres", "sqlite", "auto"],
        default="postgres",
        help="Snapshot source backend; sqlite is migration-only",
    )
    export_control_plane_parser.add_argument(
        "--sqlite-path", default="", help="Migration-only sqlite source path when --source-backend=sqlite or auto"
    )
    export_control_plane_parser.add_argument("--runtime-dir", default="", help="Optional runtime dir override")
    export_control_plane_parser.add_argument(
        "--table", action="append", default=[], help="Optional table selection; repeatable"
    )
    export_control_plane_parser.add_argument(
        "--all-sqlite-tables",
        action="store_true",
        help="Migration-only: export every non-internal SQLite table plus generation_index_entries when present",
    )

    sync_control_plane_parser = subparsers.add_parser(
        "sync-control-plane-postgres",
        help="Sync an exported control-plane snapshot into Postgres, or run a migration-only runtime mirror",
    )
    sync_control_plane_parser.add_argument(
        "--snapshot", default="", help="Optional path to an exported control-plane snapshot JSON"
    )
    sync_control_plane_parser.add_argument(
        "--dsn", default="", help="Optional Postgres DSN override; defaults to SOURCING_CONTROL_PLANE_POSTGRES_DSN"
    )
    sync_control_plane_parser.add_argument(
        "--runtime-dir", default="", help="Optional runtime dir override for direct runtime mirroring"
    )
    sync_control_plane_parser.add_argument(
        "--sqlite-path", default="", help="Migration-only sqlite path override for direct runtime mirroring"
    )
    sync_control_plane_parser.add_argument(
        "--state-path", default="", help="Optional sync state path override for direct runtime mirroring"
    )
    sync_control_plane_parser.add_argument(
        "--min-interval-seconds",
        type=float,
        default=0.0,
        help="Optional throttle interval for direct runtime mirroring",
    )
    sync_control_plane_parser.add_argument(
        "--force",
        action="store_true",
        help="Force direct runtime mirroring even when the source fingerprint is unchanged",
    )
    sync_control_plane_parser.add_argument(
        "--table", action="append", default=[], help="Optional table selection; repeatable"
    )
    sync_control_plane_parser.add_argument(
        "--truncate-first", action="store_true", help="Truncate each selected table before syncing rows"
    )
    sync_control_plane_parser.add_argument(
        "--all-sqlite-tables",
        action="store_true",
        help="Migration-only: sync every non-internal SQLite table plus generation_index_entries",
    )
    sync_control_plane_parser.add_argument(
        "--validate-postgres",
        action="store_true",
        help="Validate Postgres row counts after sync; exact when combined with --truncate-first",
    )
    sync_control_plane_parser.add_argument(
        "--direct-stream",
        action="store_true",
        help="Migration-only: stream rows directly from SQLite to Postgres table-by-table",
    )
    sync_control_plane_parser.add_argument(
        "--chunk-size",
        type=int,
        default=0,
        help="Optional direct-stream batch size; smaller values reduce peak IO/memory at the cost of more round trips",
    )
    sync_control_plane_parser.add_argument(
        "--commit-every-chunks",
        type=int,
        default=0,
        help="Optional direct-stream commit cadence; smaller values reduce large-table WAL spikes",
    )
    sync_control_plane_parser.add_argument(
        "--progress-every-chunks",
        type=int,
        default=0,
        help="Optional direct-stream progress flush cadence for long-running tables",
    )
    sync_control_plane_parser.add_argument(
        "--chunk-pause-seconds",
        type=float,
        default=0.0,
        help="Optional direct-stream sleep between chunks to reduce sustained IO pressure",
    )

    launch_detached_parser = subparsers.add_parser(
        "launch-detached",
        help="Launch a repo command in a detached session with repo-aware env/logging for WSL/Cursor-safe long runs.",
    )
    launch_detached_parser.add_argument(
        "--log-path",
        default="runtime/service_logs/detached-launch.log",
        help="Log file path. Relative paths resolve from the project root.",
    )
    launch_detached_parser.add_argument(
        "--status-path",
        default="",
        help="Optional JSON status file written after launch. Relative paths resolve from the project root.",
    )
    launch_detached_parser.add_argument(
        "--cwd",
        default="",
        help="Optional working directory for the detached command. Relative paths resolve from the project root.",
    )
    launch_detached_parser.add_argument(
        "--description",
        default="",
        help="Optional human-readable label recorded in the returned launch payload.",
    )
    launch_detached_parser.add_argument(
        "--startup-wait-seconds",
        type=float,
        default=0.2,
        help="How long to wait before checking whether the detached command exited immediately.",
    )
    launch_detached_parser.add_argument(
        "launch_command",
        nargs=argparse.REMAINDER,
        help="Command to run, passed after --. Example: launch-detached --log-path runtime/service_logs/task.log -- python3 -m sourcing_agent.cli serve",
    )

    public_web_experiment_parser = subparsers.add_parser(
        "run-target-candidate-public-web-experiment",
        help=(
            "Run the candidate-level Public Web Search experiment for selected target candidates. "
            "Outputs query, entry-link, fetch, signal, and summary artifacts without adding new persistence tables."
        ),
    )
    public_web_experiment_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum target candidates to process. Defaults to the requested 10-candidate experiment batch.",
    )
    public_web_experiment_parser.add_argument("--record-id", action="append", default=[], help="Target candidate record id; repeatable")
    public_web_experiment_parser.add_argument("--job-id", default="", help="Optional target_candidates job_id filter")
    public_web_experiment_parser.add_argument("--history-id", default="", help="Optional target_candidates history_id filter")
    public_web_experiment_parser.add_argument(
        "--follow-up-status", default="", help="Optional target_candidates follow_up_status filter"
    )
    public_web_experiment_parser.add_argument(
        "--input-file",
        default="",
        help="Optional JSON file with target_candidates/candidates records; bypasses store selection.",
    )
    public_web_experiment_parser.add_argument(
        "--sample",
        action="store_true",
        help="Use the built-in 10-candidate sample instead of reading target_candidates from the store.",
    )
    public_web_experiment_parser.add_argument(
        "--sample-if-empty",
        action="store_true",
        help="Fall back to the built-in sample if the selected target_candidates scope is empty.",
    )
    public_web_experiment_parser.add_argument(
        "--output-dir",
        default="",
        help="Artifact output root. Defaults to runtime/public_web/experiments.",
    )
    public_web_experiment_parser.add_argument("--run-id", default="", help="Optional stable run id for replayable experiments")
    public_web_experiment_parser.add_argument(
        "--external-provider-mode",
        choices=["live", "simulate", "replay", "scripted"],
        default="",
        help="Optional override for SOURCING_EXTERNAL_PROVIDER_MODE before provider construction.",
    )
    public_web_experiment_parser.add_argument("--max-queries-per-candidate", type=int, default=10)
    public_web_experiment_parser.add_argument("--max-results-per-query", type=int, default=10)
    public_web_experiment_parser.add_argument("--max-entry-links-per-candidate", type=int, default=40)
    public_web_experiment_parser.add_argument("--max-fetches-per-candidate", type=int, default=5)
    public_web_experiment_parser.add_argument(
        "--max-ai-evidence-documents",
        type=int,
        default=8,
        help="Maximum fetched model-safe document summaries/slices sent to the candidate-level AI adjudicator.",
    )
    public_web_experiment_parser.add_argument(
        "--max-concurrent-fetches-per-candidate",
        type=int,
        default=4,
        help="Maximum concurrent URL fetch/document extraction calls within one candidate analysis.",
    )
    public_web_experiment_parser.add_argument(
        "--max-concurrent-candidate-analyses",
        type=int,
        default=2,
        help="Maximum target candidates finalized concurrently after batch search results are ready.",
    )
    public_web_experiment_parser.add_argument(
        "--no-fetch-content",
        action="store_true",
        help="Only discover/rank entry links; skip page/PDF fetch and document analysis.",
    )
    public_web_experiment_parser.add_argument(
        "--no-contact-extraction",
        action="store_true",
        help="Disable deterministic email/contact extraction from fetched public-web content.",
    )
    public_web_experiment_parser.add_argument(
        "--ai-extraction",
        choices=["auto", "on", "off"],
        default="auto",
        help="AI signal adjudication mode. auto skips deterministic/offline model clients.",
    )
    public_web_experiment_parser.add_argument(
        "--no-batch-search",
        action="store_true",
        help="Disable provider batch/queue search and fall back to sequential provider.search calls.",
    )
    public_web_experiment_parser.add_argument(
        "--batch-ready-poll-interval-seconds",
        type=float,
        default=10.0,
        help="Sleep interval between provider batch ready polls.",
    )
    public_web_experiment_parser.add_argument(
        "--max-batch-ready-polls",
        type=int,
        default=18,
        help="Maximum provider batch ready poll attempts before recording per-query timeouts.",
    )
    public_web_experiment_parser.add_argument("--timeout-seconds", type=int, default=30)

    public_web_quality_parser = subparsers.add_parser(
        "evaluate-public-web-quality",
        help="Evaluate Public Web signals artifacts for email/media evidence quality before product UI/export rollout.",
    )
    public_web_quality_parser.add_argument(
        "--experiment-dir",
        action="append",
        default=[],
        help="Experiment directory or signals.json path to evaluate; repeatable. Defaults to runtime/public_web/experiments.",
    )
    public_web_quality_parser.add_argument(
        "--output-dir",
        default="",
        help="Optional output directory for JSON, CSV, and Markdown quality reports.",
    )
    public_web_quality_parser.add_argument(
        "--fail-on-high-risk",
        action="store_true",
        help="Return status=failed when high-severity quality issues are found.",
    )
    public_web_quality_parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print only status, summary, input paths, and written report paths; useful for large quality runs.",
    )

    subparsers.add_parser(
        "test-model",
        help="Run provider healthcheck. For low-cost workflow smoke tests, set SOURCING_EXTERNAL_PROVIDER_MODE=simulate or replay to disable Harvest/Search/model/semantic live calls.",
    )

    serve_parser = subparsers.add_parser(
        "serve",
        help="Start the HTTP API. External provider mode still comes from SOURCING_EXTERNAL_PROVIDER_MODE=live|replay|simulate.",
    )
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)
    serve_parser.add_argument(
        "--disable-runtime-watchdog", action="store_true", help="Disable the server-side recovery watchdog loop"
    )
    serve_parser.add_argument(
        "--runtime-watchdog-poll-seconds",
        type=float,
        default=15.0,
        help="Poll interval for the server-side recovery watchdog",
    )

    args = parser.parse_args()
    if args.command == "show-control-plane-runtime":
        print(json.dumps(build_control_plane_runtime_summary(), ensure_ascii=False, indent=2))
        return
    if args.command == "run-target-candidate-public-web-experiment":
        print(json.dumps(run_target_candidate_public_web_experiment_command(args), ensure_ascii=False, indent=2))
        return
    if args.command == "evaluate-public-web-quality":
        result = evaluate_public_web_quality_command(args)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if result.get("status") == "failed":
            sys.exit(1)
        return
    if args.command in {
        "backfill-linkedin-profile-registry",
        "backfill-organization-asset-registry",
        "rebuild-runtime-control-plane",
        "repair-company-candidate-artifacts",
        "backfill-structured-timeline",
        "repair-profile-signal-projection",
        "show-linkedin-profile-registry-metrics",
    }:
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        store = build_runtime_store(settings)
        if args.command == "backfill-linkedin-profile-registry":
            checkpoint_path = (
                Path(args.checkpoint_path).expanduser() if str(args.checkpoint_path or "").strip() else None
            )

            def _progress(payload: dict[str, object]) -> None:
                print(json.dumps({"progress": payload}, ensure_ascii=False))

            result = backfill_linkedin_profile_registry(
                runtime_dir=settings.runtime_dir,
                store=store,
                company=str(args.company or "").strip(),
                snapshot_id=str(args.snapshot_id or "").strip(),
                resume=not bool(args.no_resume),
                checkpoint_path=checkpoint_path,
                progress_interval=max(1, int(args.progress_interval or 1)),
                progress_callback=_progress,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return
        if args.command == "backfill-organization-asset-registry":
            companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
            if not companies:
                raise SystemExit("backfill-organization-asset-registry requires at least one --company")
            results = []
            for company in companies:
                results.append(
                    backfill_organization_asset_registry_for_company(
                        runtime_dir=settings.runtime_dir,
                        store=store,
                        target_company=company,
                        asset_view=str(args.asset_view or "canonical_merged"),
                    )
                )
            print(
                json.dumps(
                    {
                        "asset_view": str(args.asset_view or "canonical_merged"),
                        "results": results,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "rebuild-runtime-control-plane":
            companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
            result = rebuild_runtime_control_plane(
                runtime_dir=settings.runtime_dir,
                store=store,
                companies=companies or None,
                snapshot_id=str(args.snapshot_id or "").strip(),
                rebuild_missing_artifacts=not bool(args.skip_missing_artifact_repair),
                rebuild_company_assets=not bool(args.skip_company_assets),
                rebuild_jobs=not bool(args.skip_jobs),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return
        if args.command == "repair-company-candidate-artifacts":
            companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
            result = repair_missing_company_candidate_artifacts(
                runtime_dir=settings.runtime_dir,
                store=store,
                companies=companies or None,
                snapshot_id=str(args.snapshot_id or "").strip(),
                force_rebuild_artifacts=bool(args.force_rebuild),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return
        if args.command == "backfill-structured-timeline":
            companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
            result = backfill_structured_timeline_for_company_assets(
                runtime_dir=settings.runtime_dir,
                store=store,
                companies=companies or None,
                snapshot_id=str(args.snapshot_id or "").strip(),
                backfill_profile_registry=not bool(args.skip_profile_registry_backfill),
                profile_resume=not bool(args.profile_no_resume),
                profile_progress_interval=max(1, int(args.profile_progress_interval or 1)),
                refresh_registry=not bool(args.skip_registry_refresh),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return
        if args.command == "repair-profile-signal-projection":
            companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
            result = repair_projected_profile_signals_in_company_candidate_artifacts(
                runtime_dir=settings.runtime_dir,
                companies=companies or None,
                snapshot_id=str(args.snapshot_id or "").strip(),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return
        if args.command == "show-linkedin-profile-registry-metrics":
            metrics = store.get_linkedin_profile_registry_metrics(lookback_hours=max(0, int(args.lookback_hours or 0)))
            print(json.dumps(metrics, ensure_ascii=False, indent=2))
            return

    if args.command == "export-control-plane-snapshot":
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        runtime_dir = (
            Path(str(args.runtime_dir or "").strip()).expanduser()
            if str(args.runtime_dir or "").strip()
            else settings.runtime_dir
        )
        output_path = (
            Path(str(args.output or "").strip()).expanduser()
            if str(args.output or "").strip()
            else runtime_dir / "object_sync" / "control_plane" / "control_plane_snapshot.json"
        )
        sqlite_path = (
            Path(str(args.sqlite_path or "").strip()).expanduser()
            if str(args.sqlite_path or "").strip()
            else None
        )
        print(
            json.dumps(
                export_control_plane_snapshot(
                    runtime_dir=runtime_dir,
                    output_path=output_path,
                    sqlite_path=sqlite_path,
                    tables=[str(item or "").strip() for item in list(args.table or []) if str(item or "").strip()],
                    include_all_sqlite_tables=bool(args.all_sqlite_tables),
                    source_backend=str(args.source_backend or "postgres"),
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "sync-control-plane-postgres":
        if str(args.snapshot or "").strip():
            summary = sync_control_plane_snapshot_to_postgres(
                snapshot_path=str(args.snapshot or "").strip(),
                dsn=str(args.dsn or "").strip(),
                tables=(
                    [str(item or "").strip() for item in list(args.table or []) if str(item or "").strip()] or None
                ),
                truncate_first=bool(args.truncate_first),
                validate_postgres=bool(args.validate_postgres),
            )
        else:
            catalog = AssetCatalog.discover()
            settings = load_settings(catalog.project_root)
            runtime_dir = (
                Path(str(args.runtime_dir or "").strip()).expanduser()
                if str(args.runtime_dir or "").strip()
                else settings.runtime_dir
            )
            sqlite_path = (
                Path(str(args.sqlite_path or "").strip()).expanduser()
                if str(args.sqlite_path or "").strip()
                else settings.db_path
            )
            state_path = (
                Path(str(args.state_path or "").strip()).expanduser() if str(args.state_path or "").strip() else None
            )
            summary = sync_runtime_control_plane_to_postgres(
                runtime_dir=runtime_dir,
                sqlite_path=sqlite_path,
                dsn=str(args.dsn or "").strip(),
                tables=[str(item or "").strip() for item in list(args.table or []) if str(item or "").strip()],
                truncate_first=bool(args.truncate_first),
                state_path=state_path,
                min_interval_seconds=float(args.min_interval_seconds or 0.0),
                force=bool(args.force),
                include_all_sqlite_tables=bool(args.all_sqlite_tables),
                validate_postgres=bool(args.validate_postgres),
                direct_stream=bool(args.direct_stream),
                chunk_size=int(args.chunk_size or 0),
                commit_every_chunks=int(args.commit_every_chunks or 0),
                progress_every_chunks=int(args.progress_every_chunks or 0),
                chunk_pause_seconds=float(args.chunk_pause_seconds or 0.0),
            )
        print(
            json.dumps(
                summary,
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command in {
        "export-company-snapshot-bundle",
        "export-company-handoff-bundle",
        "export-control-plane-snapshot-bundle",
        "build-company-candidate-artifacts",
        "segment-company-outreach-layers",
        "complete-company-assets",
        "supplement-company-assets",
        "restore-asset-bundle",
        "upload-asset-bundle",
        "publish-candidate-generation",
        "delete-asset-bundle",
        "download-asset-bundle",
        "import-cloud-assets",
        "hydrate-candidate-generation",
    }:
        bundle_manager = build_asset_bundle_manager()
        if args.command == "export-company-snapshot-bundle":
            print(
                json.dumps(
                    bundle_manager.export_company_snapshot_bundle(
                        args.company,
                        snapshot_id=args.snapshot_id,
                        output_dir=args.output_dir or None,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "export-company-handoff-bundle":
            print(
                json.dumps(
                    bundle_manager.export_company_handoff_bundle(
                        args.company,
                        output_dir=args.output_dir or None,
                        include_live_tests=not args.without_live_tests,
                        include_manual_review=not args.without_manual_review,
                        include_jobs=not args.without_jobs,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "export-control-plane-snapshot-bundle":
            print(
                json.dumps(
                    bundle_manager.export_control_plane_snapshot_bundle(output_dir=args.output_dir or None),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "build-company-candidate-artifacts":
            catalog = AssetCatalog.discover()
            settings = load_settings(catalog.project_root)
            store = build_runtime_store(settings)
            print(
                json.dumps(
                    build_company_candidate_artifacts(
                        runtime_dir=settings.runtime_dir,
                        store=store,
                        target_company=args.company,
                        snapshot_id=args.snapshot_id,
                        output_dir=args.output_dir or None,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "segment-company-outreach-layers":
            catalog = AssetCatalog.discover()
            settings = load_settings(catalog.project_root)
            model_client = None
            if not args.no_ai:
                provider = str(args.provider or "auto").strip().lower()
                if provider == "qwen":
                    if not settings.qwen.enabled:
                        raise SystemExit(
                            "Qwen is not enabled; provide qwen api key/base_url in runtime/secrets/providers.local.json"
                        )
                    model_client = QwenResponsesModelClient(settings.qwen)
                elif provider == "openai":
                    if not settings.model_provider.enabled:
                        raise SystemExit(
                            "OpenAI-compatible provider is not enabled; check model_provider config in runtime/secrets/providers.local.json"
                        )
                    model_client = OpenAICompatibleChatModelClient(settings.model_provider)
                else:
                    model_client = build_model_client(settings.model_provider, settings.qwen)
            result = analyze_company_outreach_layers(
                runtime_dir=settings.runtime_dir,
                target_company=args.company,
                snapshot_id=args.snapshot_id,
                view=args.asset_view,
                query=args.query,
                model_client=model_client,
                max_ai_verifications=max(0, int(args.max_ai_verifications or 0)),
                ai_workers=max(1, int(args.ai_workers or 1)),
                ai_max_retries=max(0, int(args.ai_max_retries or 0)),
                ai_retry_backoff_seconds=max(0.0, float(args.ai_retry_backoff_seconds or 0.0)),
                output_dir=args.output_dir or None,
            )
            if args.summary_only:
                compact = {
                    "status": str(result.get("status") or ""),
                    "target_company": str(result.get("target_company") or ""),
                    "snapshot_id": str(result.get("snapshot_id") or ""),
                    "asset_view": str(result.get("asset_view") or ""),
                    "ai_prompt_template_version": str((result.get("ai_prompt_template") or {}).get("version") or ""),
                    "candidate_count": int(result.get("candidate_count") or 0),
                    "layer_counts": {
                        "layer_0_roster": int((result.get("layers") or {}).get("layer_0_roster", {}).get("count") or 0),
                        "layer_1_name_signal": int(
                            (result.get("layers") or {}).get("layer_1_name_signal", {}).get("count") or 0
                        ),
                        "layer_2_greater_china_region_experience": int(
                            (result.get("layers") or {}).get("layer_2_greater_china_region_experience", {}).get("count")
                            or 0
                        ),
                        "layer_3_mainland_china_experience_or_chinese_language": int(
                            (result.get("layers") or {})
                            .get("layer_3_mainland_china_experience_or_chinese_language", {})
                            .get("count")
                            or 0
                        ),
                    },
                    "legacy_layer_count_aliases": {
                        "layer_2_greater_china_experience": int(
                            (result.get("layers") or {}).get("layer_2_greater_china_experience", {}).get("count") or 0
                        ),
                        "layer_3_mainland_or_chinese_language": int(
                            (result.get("layers") or {}).get("layer_3_mainland_or_chinese_language", {}).get("count")
                            or 0
                        ),
                    },
                    "cumulative_layer_counts": dict(result.get("cumulative_layer_counts") or {}),
                    "final_layer_distribution": dict(result.get("final_layer_distribution") or {}),
                    "ai_verification": dict(result.get("ai_verification") or {}),
                    "analysis_paths": dict(result.get("analysis_paths") or {}),
                }
                print(json.dumps(compact, ensure_ascii=False, indent=2))
                return
            print(
                json.dumps(
                    result,
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "complete-company-assets":
            manager = build_asset_completion_manager()
            print(
                json.dumps(
                    manager.complete_company_assets(
                        target_company=args.company,
                        snapshot_id=args.snapshot_id,
                        profile_detail_limit=args.profile_detail_limit,
                        exploration_limit=args.exploration_limit,
                        build_artifacts=not args.without_artifacts,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "supplement-company-assets":
            manager = build_asset_supplement_manager()
            if args.import_local_bootstrap_package:
                print(
                    json.dumps(
                        manager.import_local_bootstrap_package(
                            target_company=args.company,
                            snapshot_id=args.snapshot_id,
                            sync_project_local_package=not args.skip_project_local_package_sync,
                            build_artifacts=not args.without_artifacts,
                        ),
                        ensure_ascii=False,
                        indent=2,
                    )
                )
                return
            print(
                json.dumps(
                    manager.supplement_snapshot(
                        target_company=args.company,
                        snapshot_id=args.snapshot_id,
                        rebuild_linkedin_stage_1=bool(args.rebuild_linkedin_stage_1),
                        run_former_search_seed=bool(args.run_former_search_seed),
                        former_search_limit=int(args.former_search_limit or 25),
                        former_search_pages=int(args.former_search_pages or 1),
                        former_search_queries=list(args.former_query or []),
                        former_filter_hints={"keywords": list(args.former_keyword or [])},
                        profile_scope=str(args.profile_scope or "none"),
                        profile_limit=int(args.profile_limit or 0),
                        profile_only_missing_detail=bool(args.profile_only_missing_detail)
                        and not bool(args.profile_all_known_urls),
                        profile_force_refresh=bool(args.profile_force_refresh),
                        repair_current_roster_profile_refs=bool(args.repair_current_roster_profile_refs),
                        repair_current_roster_registry_aliases=bool(args.repair_current_roster_registry_aliases),
                        build_artifacts=not args.without_artifacts,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "restore-asset-bundle":
            print(
                json.dumps(
                    bundle_manager.restore_bundle(
                        args.manifest,
                        target_runtime_dir=args.target_runtime_dir or None,
                        conflict=args.conflict,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        storage_client = build_object_storage()
        if args.command == "upload-asset-bundle":
            print(
                json.dumps(
                    bundle_manager.upload_bundle(
                        args.manifest,
                        storage_client,
                        max_workers=args.max_workers or None,
                        resume=not args.no_resume,
                        archive_mode=str(args.archive_mode or "auto"),
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "publish-candidate-generation":
            print(
                json.dumps(
                    bundle_manager.publish_candidate_generation(
                        target_company=str(args.company or "").strip(),
                        snapshot_id=str(args.snapshot_id or "").strip(),
                        asset_view=str(args.asset_view or "canonical_merged"),
                        client=storage_client,
                        max_workers=args.max_workers or None,
                        resume=not bool(args.no_resume),
                        include_compatibility_exports=bool(args.include_compatibility_exports),
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "delete-asset-bundle":
            delete_summary = bundle_manager.delete_bundle(
                bundle_kind=args.bundle_kind,
                bundle_id=args.bundle_id,
                client=storage_client,
                max_workers=args.max_workers or None,
                prune_local_index=not bool(args.keep_local_index),
            )
            try:
                catalog = AssetCatalog.discover()
                settings = load_settings(catalog.project_root)
                store = build_runtime_store(settings)
                effective_store_target = str(
                    getattr(store, "sqlite_shadow_connect_target", lambda: str(settings.db_path))()
                ).strip() or str(settings.db_path)
                delete_summary["ledger"] = store.record_cloud_asset_operation(
                    operation_type="gc_delete_bundle",
                    bundle_kind=str(args.bundle_kind or "").strip(),
                    bundle_id=str(args.bundle_id or "").strip(),
                    status=str(delete_summary.get("status") or ""),
                    sync_run_id=str(delete_summary.get("sync_run_id") or ""),
                    target_runtime_dir=str(bundle_manager.runtime_dir),
                    target_db_path=effective_store_target,
                    summary=delete_summary,
                    metadata={
                        "keep_local_index": bool(args.keep_local_index),
                        "max_workers": int(args.max_workers or 0),
                    },
                )
            except Exception as exc:
                delete_summary["ledger"] = {
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            print(
                json.dumps(
                    delete_summary,
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "download-asset-bundle":
            print(
                json.dumps(
                    bundle_manager.download_bundle(
                        bundle_kind=args.bundle_kind,
                        bundle_id=args.bundle_id,
                        client=storage_client,
                        output_dir=args.output_dir or None,
                        max_workers=args.max_workers or None,
                        resume=not args.no_resume,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "import-cloud-assets":
            storage_client = None
            if not str(args.manifest or "").strip():
                storage_client = build_object_storage()
            print(
                json.dumps(
                    import_cloud_assets(
                        bundle_manager=bundle_manager,
                        manifest_path=str(args.manifest or "").strip(),
                        bundle_kind=str(args.bundle_kind or "").strip(),
                        bundle_id=str(args.bundle_id or "").strip(),
                        storage_client=storage_client,
                        output_dir=args.output_dir or None,
                        max_workers=args.max_workers or None,
                        resume=not bool(args.no_resume),
                        target_runtime_dir=args.target_runtime_dir or None,
                        conflict=str(args.conflict or "skip"),
                        target_db_path=args.target_db_path or None,
                        companies=[
                            str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()
                        ],
                        snapshot_id=str(args.snapshot_id or "").strip(),
                        asset_view=str(args.asset_view or "canonical_merged"),
                        generation_key=str(args.generation_key or "").strip(),
                        prefer_generation=not bool(args.disable_generation_first),
                        allow_legacy_bundle_fallback=not bool(args.disable_legacy_bundle_fallback),
                        prefer_local_link=not bool(args.disable_local_link),
                        run_artifact_repair=not bool(args.skip_artifact_repair),
                        run_org_warmup=not bool(args.skip_org_warmup),
                        run_profile_registry_backfill=not bool(args.skip_profile_registry_backfill),
                        profile_registry_resume=not bool(args.profile_no_resume),
                        profile_progress_interval=max(1, int(args.profile_progress_interval or 1)),
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "hydrate-candidate-generation":
            storage_client = None
            if not str(args.manifest or "").strip():
                storage_client = build_object_storage()
            print(
                json.dumps(
                    hydrate_cloud_generation(
                        bundle_manager=bundle_manager,
                        generation_manifest_path=str(args.manifest or "").strip(),
                        generation_key=str(args.generation_key or "").strip(),
                        storage_client=storage_client,
                        target_company=str(args.company or "").strip(),
                        company_key=str(args.company_key or "").strip(),
                        snapshot_id=str(args.snapshot_id or "").strip(),
                        asset_view=str(args.asset_view or "canonical_merged"),
                        max_workers=args.max_workers or None,
                        resume=not bool(args.no_resume),
                        prefer_local_link=not bool(args.disable_local_link),
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

    if args.command == "launch-detached":
        launch_command = list(args.launch_command or [])
        if launch_command and launch_command[0] == "--":
            launch_command = launch_command[1:]
        if not launch_command:
            raise SystemExit("launch-detached requires a command after --")
        print(
            json.dumps(
                launch_detached_command(
                    command=launch_command,
                    log_path=str(args.log_path or ""),
                    cwd=str(args.cwd or ""),
                    description=str(args.description or ""),
                    startup_wait_seconds=float(args.startup_wait_seconds or 0.2),
                    status_path=str(args.status_path or ""),
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    orchestrator = build_orchestrator()

    if args.command == "bootstrap":
        print(json.dumps(orchestrator.bootstrap(), ensure_ascii=False, indent=2))
        return

    if args.command == "run-job":
        payload = json.loads(Path(args.file).read_text())
        if args.asset_view:
            payload["asset_view"] = args.asset_view
        if args.must_have_facet:
            payload["must_have_facets"] = list(args.must_have_facet)
        if args.must_have_primary_role_bucket:
            payload["must_have_primary_role_buckets"] = list(args.must_have_primary_role_bucket)
        print(json.dumps(orchestrator.run_job(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "plan":
        payload = json.loads(Path(args.file).read_text())
        if args.asset_view:
            payload["asset_view"] = args.asset_view
        if args.must_have_facet:
            payload["must_have_facets"] = list(args.must_have_facet)
        if args.must_have_primary_role_bucket:
            payload["must_have_primary_role_buckets"] = list(args.must_have_primary_role_bucket)
        print(json.dumps(orchestrator.plan_workflow(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "explain-workflow":
        if args.file:
            payload = json.loads(Path(args.file).read_text())
        elif args.plan_review_id > 0:
            payload = {"plan_review_id": int(args.plan_review_id)}
        else:
            raise SystemExit("explain-workflow requires either --file or --plan-review-id")
        if args.asset_view:
            payload["asset_view"] = args.asset_view
        if args.must_have_facet:
            payload["must_have_facets"] = list(args.must_have_facet)
        if args.must_have_primary_role_bucket:
            payload["must_have_primary_role_buckets"] = list(args.must_have_primary_role_bucket)
        print(json.dumps(orchestrator.explain_workflow(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "intake-excel":
        print(
            json.dumps(
                orchestrator.ingest_excel_contacts(
                    {
                        "file_path": str(args.file or "").strip(),
                        "target_company": str(args.target_company or "").strip(),
                        "snapshot_id": str(args.snapshot_id or "").strip(),
                        "attach_to_snapshot": bool(args.attach_to_snapshot),
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "continue-excel-intake":
        payload = json.loads(Path(args.file).read_text())
        print(json.dumps(orchestrator.continue_excel_intake_review(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "promote-asset-default-pointer":
        coverage_proof: dict[str, Any] = {}
        if str(args.coverage_proof_file or "").strip():
            coverage_proof = json.loads(Path(args.coverage_proof_file).read_text())
        elif str(args.coverage_proof_json or "").strip():
            coverage_proof = json.loads(str(args.coverage_proof_json or "{}"))
        print(
            json.dumps(
                orchestrator.promote_asset_default_pointer(
                    {
                        "company_key": str(args.company or "").strip(),
                        "snapshot_id": str(args.snapshot_id or "").strip(),
                        "scope_kind": str(args.scope_kind or "company").strip(),
                        "scope_key": str(args.scope_key or "").strip(),
                        "asset_kind": str(args.asset_kind or "company_asset").strip(),
                        "lifecycle_status": str(args.lifecycle_status or "canonical").strip(),
                        "coverage_proof": coverage_proof,
                        "promoted_by_job_id": str(args.promoted_by_job_id or "").strip(),
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "review-plan":
        if args.file:
            payload = json.loads(Path(args.file).read_text())
        else:
            compiled = orchestrator.compile_plan_review_instruction(
                {
                    "review_id": int(args.review_id),
                    "instruction": str(args.instruction or ""),
                    "reviewer": str(args.reviewer or ""),
                    "notes": str(args.notes or ""),
                    "action": str(args.action or "approved"),
                }
            )
            if compiled.get("status") == "not_found":
                raise SystemExit(f"Plan review session {args.review_id} not found")
            if compiled.get("status") == "invalid":
                raise SystemExit(str(compiled.get("reason") or "invalid review-plan instruction payload"))
            if args.preview:
                print(json.dumps(compiled, ensure_ascii=False, indent=2))
                return
            payload = dict(compiled.get("review_payload") or {})
            reviewed = orchestrator.review_plan_session(payload)
            if isinstance(reviewed, dict):
                reviewed["instruction_compiler"] = dict(compiled.get("instruction_compiler") or {})
                reviewed["intent_rewrite"] = dict(compiled.get("intent_rewrite") or {})
            print(json.dumps(reviewed, ensure_ascii=False, indent=2))
            return
        print(json.dumps(orchestrator.review_plan_session(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "refine-results":
        if args.file:
            payload = json.loads(Path(args.file).read_text())
        else:
            payload = {
                "job_id": str(args.job_id or ""),
                "instruction": str(args.instruction or ""),
            }
            compiled = orchestrator.compile_post_acquisition_refinement(payload)
            if compiled.get("status") == "not_found":
                raise SystemExit(f"Baseline job {args.job_id} not found")
            if compiled.get("status") == "invalid":
                raise SystemExit(str(compiled.get("reason") or "invalid refine-results payload"))
            if args.preview:
                print(json.dumps(compiled, ensure_ascii=False, indent=2))
                return
        print(json.dumps(orchestrator.apply_post_acquisition_refinement(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "show-plan-reviews":
        result = orchestrator.list_plan_review_sessions(args.target_company)
        if args.brief:
            reviews = []
            for item in list(result.get("plan_reviews") or []):
                request_payload = dict(item.get("request") or {})
                reviews.append(
                    {
                        "review_id": item.get("review_id"),
                        "target_company": item.get("target_company"),
                        "status": item.get("status"),
                        "risk_level": item.get("risk_level"),
                        "required_before_execution": item.get("required_before_execution"),
                        "created_at": item.get("created_at"),
                        "updated_at": item.get("updated_at"),
                        "raw_user_request": request_payload.get("raw_user_request"),
                    }
                )
            result = {"plan_reviews": reviews}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "start-workflow":
        if args.file:
            payload = json.loads(Path(args.file).read_text())
        elif args.plan_review_id > 0:
            payload = {"plan_review_id": int(args.plan_review_id)}
        else:
            raise SystemExit("start-workflow requires either --file or --plan-review-id")
        if args.asset_view:
            payload["asset_view"] = args.asset_view
        if args.must_have_facet:
            payload["must_have_facets"] = list(args.must_have_facet)
        if args.must_have_primary_role_bucket:
            payload["must_have_primary_role_buckets"] = list(args.must_have_primary_role_bucket)
        if args.blocking:
            print(json.dumps(orchestrator.run_workflow_blocking(payload), ensure_ascii=False, indent=2))
            return
        if args.no_auto_job_daemon:
            payload["auto_job_daemon"] = False
        payload = normalize_workflow_submission_payload(
            payload,
            default_runtime_execution_mode=str(args.runtime_execution_mode or "hosted"),
            hosted_auto_job_daemon=False,
        )
        if workflow_runtime_uses_managed_runner(payload.get("runtime_execution_mode")):
            queued = orchestrator.start_workflow_runner_managed(payload)
        else:
            hosted_api_base_url = _resolve_hosted_api_base_url(args.hosted_api_base_url)
            try:
                queued = _submit_hosted_workflow_request(
                    payload,
                    base_url=hosted_api_base_url,
                    timeout_seconds=float(args.hosted_api_timeout_seconds or 15.0),
                )
            except HostedWorkflowSubmissionError as exc:
                raise SystemExit(
                    "Hosted workflow submission failed. Start `serve` and retry, or use "
                    "`--runtime-execution-mode managed_subprocess` for a standalone local run. "
                    f"Details: {exc}"
                ) from exc
        print(json.dumps(queued, ensure_ascii=False, indent=2))
        return

    if args.command == "execute-workflow":
        recovery_payload: dict[str, object] | None = None
        if args.auto_job_daemon:
            recovery_payload = {"auto_job_daemon": True}
        print(
            json.dumps(
                {
                    "event": "workflow_runner_started",
                    "job_id": str(args.job_id or ""),
                    "auto_job_daemon": bool(args.auto_job_daemon),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
            flush=True,
        )
        result = orchestrator.run_queued_workflow(args.job_id, recovery_payload=recovery_payload)
        print(
            json.dumps(
                {
                    "event": "workflow_runner_finished",
                    "job_id": str(args.job_id or ""),
                    "status": str(result.get("status") or ""),
                    "stage": str(result.get("artifact", {}).get("summary", {}).get("stage") or ""),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
            flush=True,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "supervise-workflow":
        print(
            json.dumps(
                {
                    "event": "workflow_runner_started",
                    "job_id": str(args.job_id or ""),
                    "auto_job_daemon": bool(args.auto_job_daemon),
                    "mode": "supervisor",
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
            flush=True,
        )
        result = orchestrator.run_workflow_supervisor(
            args.job_id,
            auto_job_daemon=bool(args.auto_job_daemon),
            poll_seconds=float(args.poll_seconds or 2.0),
            max_ticks=int(args.max_ticks or 0),
        )
        print(
            json.dumps(
                {
                    "event": "workflow_runner_finished",
                    "job_id": str(args.job_id or ""),
                    "status": str(result.get("status") or ""),
                    "stage": str(result.get("stage") or ""),
                    "mode": "supervisor",
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
            flush=True,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "show-job":
        result = orchestrator.get_job_results(args.job_id)
        if result is None:
            raise SystemExit(f"Job {args.job_id} not found")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "show-progress":
        result = orchestrator.get_job_progress(args.job_id)
        if result is None:
            raise SystemExit(f"Job {args.job_id} not found")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "show-system-progress":
        print(
            json.dumps(
                orchestrator.get_system_progress(
                    {
                        "active_limit": args.active_limit,
                        "object_sync_limit": args.object_sync_limit,
                        "profile_registry_lookback_hours": args.profile_registry_lookback_hours,
                        "force_refresh": bool(args.force_refresh),
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "show-trace":
        result = orchestrator.get_job_trace(args.job_id)
        if result is None:
            raise SystemExit(f"Job {args.job_id} not found")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "show-workers":
        result = orchestrator.get_job_workers(args.job_id)
        if result is None:
            raise SystemExit(f"Job {args.job_id} not found")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "show-scheduler":
        result = orchestrator.get_job_scheduler(args.job_id)
        if result is None:
            raise SystemExit(f"Job {args.job_id} not found")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "cleanup-workflow-duplicates":
        print(
            json.dumps(
                orchestrator.cleanup_duplicate_inflight_workflows(
                    {
                        "target_company": args.target_company,
                        "active_limit": args.active_limit,
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "cleanup-blocked-workflow-residue":
        print(
            json.dumps(
                orchestrator.cleanup_blocked_workflow_residue(
                    {
                        "target_company": args.target_company,
                        "active_limit": args.active_limit,
                        "dry_run": args.dry_run,
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "supersede-workflow-jobs":
        print(
            json.dumps(
                orchestrator.supersede_workflow_jobs(
                    {
                        "job_ids": list(args.job_id or []),
                        "replacement_job_id": args.replacement_job_id,
                        "reason": args.reason,
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "show-recoverable-workers":
        print(
            json.dumps(
                orchestrator.list_recoverable_agent_workers(
                    {
                        "stale_after_seconds": args.stale_after_seconds,
                        "lane_id": args.lane_id,
                        "job_id": args.job_id,
                        "limit": args.limit,
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "cleanup-recoverable-workers":
        print(
            json.dumps(
                orchestrator.cleanup_recoverable_workers(
                    {
                        "stale_after_seconds": args.stale_after_seconds,
                        "lane_id": args.lane_id,
                        "job_id": args.job_id,
                        "target_company": args.target_company,
                        "parent_job_statuses": list(args.parent_job_status or []),
                        "limit": args.limit,
                        "dry_run": bool(args.dry_run),
                        "include_missing_jobs": bool(args.include_missing_jobs),
                        "terminal_workflows_only": bool(args.terminal_workflows_only),
                        "status": args.status,
                        "reason": args.reason,
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "interrupt-worker":
        print(
            json.dumps(orchestrator.interrupt_agent_worker({"worker_id": args.worker_id}), ensure_ascii=False, indent=2)
        )
        return

    if args.command == "run-worker-daemon-once":
        print(
            json.dumps(
                orchestrator.run_worker_recovery_once(
                    {
                        "owner_id": args.owner_id,
                        "lease_seconds": args.lease_seconds,
                        "stale_after_seconds": args.stale_after_seconds,
                        "total_limit": args.total_limit,
                        "job_id": args.job_id,
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "run-worker-daemon":
        print(
            json.dumps(
                orchestrator.run_worker_recovery_forever(
                    {
                        "owner_id": args.owner_id,
                        "lease_seconds": args.lease_seconds,
                        "stale_after_seconds": args.stale_after_seconds,
                        "total_limit": args.total_limit,
                        "job_id": args.job_id,
                        "poll_seconds": args.poll_seconds,
                        "max_ticks": args.max_ticks,
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "run-worker-daemon-service":
        try:
            print(
                json.dumps(
                    orchestrator.run_worker_daemon_service(
                        {
                            "service_name": args.service_name,
                            "owner_id": args.owner_id,
                            "lease_seconds": args.lease_seconds,
                            "stale_after_seconds": args.stale_after_seconds,
                            "total_limit": args.total_limit,
                            "job_id": args.job_id,
                            "job_scoped": bool(args.job_scoped),
                            "poll_seconds": args.poll_seconds,
                            "max_ticks": args.max_ticks,
                            "workflow_resume_stale_after_seconds": args.workflow_auto_resume_stale_after_seconds,
                            "workflow_queue_resume_stale_after_seconds": args.workflow_queue_auto_takeover_stale_after_seconds,
                        }
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
        except SingleInstanceError as exc:
            raise SystemExit(str(exc)) from exc
        return

    if args.command == "run-server-runtime-watchdog-service":
        try:
            print(
                json.dumps(
                    orchestrator.run_hosted_runtime_watchdog_service(
                        {
                            "hosted_runtime_watchdog_service_name": args.service_name,
                            "shared_service_name": args.shared_service_name,
                            "hosted_runtime_watchdog_poll_seconds": args.poll_seconds,
                            "hosted_runtime_watchdog_max_ticks": args.max_ticks,
                        }
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
        except SingleInstanceError as exc:
            raise SystemExit(str(exc)) from exc
        return

    if args.command == "show-daemon-status":
        print(
            json.dumps(
                orchestrator.get_worker_daemon_status({"service_name": args.service_name}), ensure_ascii=False, indent=2
            )
        )
        return

    if args.command == "write-worker-daemon-systemd-unit":
        print(
            json.dumps(
                orchestrator.write_worker_daemon_systemd_unit(
                    {
                        "service_name": args.service_name,
                        "output_path": args.output_path,
                        "python_bin": args.python_bin,
                        "user_name": args.user_name,
                        "lease_seconds": args.lease_seconds,
                        "stale_after_seconds": args.stale_after_seconds,
                        "total_limit": args.total_limit,
                        "poll_seconds": args.poll_seconds,
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "record-feedback":
        payload = json.loads(Path(args.file).read_text())
        print(json.dumps(orchestrator.record_criteria_feedback(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "review-suggestion":
        payload = json.loads(Path(args.file).read_text())
        print(json.dumps(orchestrator.review_pattern_suggestion(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "review-manual-item":
        payload = json.loads(Path(args.file).read_text())
        print(json.dumps(orchestrator.review_manual_review_item(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "synthesize-manual-review":
        print(
            json.dumps(
                orchestrator.synthesize_manual_review_item(
                    {
                        "review_item_id": int(args.review_item_id),
                        "force_refresh": bool(args.force_refresh),
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "configure-confidence-policy":
        payload = json.loads(Path(args.file).read_text())
        print(json.dumps(orchestrator.configure_confidence_policy(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "recompile-criteria":
        payload = json.loads(Path(args.file).read_text())
        print(json.dumps(orchestrator.recompile_criteria(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "show-criteria":
        print(json.dumps(orchestrator.list_criteria_patterns(args.target_company), ensure_ascii=False, indent=2))
        return

    if args.command == "show-manual-review":
        print(
            json.dumps(
                orchestrator.list_manual_review_items(args.target_company, args.job_id), ensure_ascii=False, indent=2
            )
        )
        return

    if args.command == "test-model":
        print(json.dumps(orchestrator.healthcheck_model(), ensure_ascii=False, indent=2))
        return

    if args.command == "serve":
        shared_recovery_stop = None
        shared_recovery_thread = None
        watchdog_stop = None
        watchdog_thread = None
        if not args.disable_runtime_watchdog:
            shared_recovery_stop, shared_recovery_thread = start_shared_recovery_service(orchestrator)
            watchdog_stop, watchdog_thread = start_server_runtime_watchdog(
                orchestrator,
                poll_seconds=float(args.runtime_watchdog_poll_seconds or 15.0),
            )
        orchestrator.start_background_organization_asset_warmup()
        server = create_server(orchestrator, host=args.host, port=args.port)
        print(f"Serving on http://{args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
            if watchdog_stop is not None:
                watchdog_stop.set()
            if shared_recovery_stop is not None:
                shared_recovery_stop.set()
            if watchdog_thread is not None:
                watchdog_thread.join(timeout=max(1.0, float(args.runtime_watchdog_poll_seconds or 15.0)))
            if shared_recovery_thread is not None:
                shared_recovery_thread.join(timeout=5.0)


if __name__ == "__main__":
    main()
