from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import threading
import time
from typing import Any

from .acquisition import AcquisitionEngine
from .agent_runtime import AgentRuntimeCoordinator
from .api import create_server
from .asset_catalog import AssetCatalog
from .asset_sync import AssetBundleManager
from .candidate_artifacts import build_company_candidate_artifacts
from .company_asset_completion import CompanyAssetCompletionManager
from .company_asset_supplement import CompanyAssetSupplementManager
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
from .service_daemon import SingleInstanceError, WorkerDaemonService
from .semantic_provider import build_semantic_provider
from .settings import load_settings
from .storage import SQLiteStore


def _runner_environment(project_root: Path) -> dict[str, str]:
    return _runner_subprocess_env(project_root)


def build_orchestrator() -> SourcingOrchestrator:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    store = SQLiteStore(settings.db_path)
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
    store = SQLiteStore(settings.db_path)
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
    store = SQLiteStore(settings.db_path)
    model_client = build_model_client(settings.model_provider, settings.qwen)
    return CompanyAssetSupplementManager(
        runtime_dir=settings.runtime_dir,
        store=store,
        settings=settings,
        model_client=model_client,
    )


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Sourcing AI Agent backend MVP")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap", help="Load local assets into SQLite")

    run_job_parser = subparsers.add_parser("run-job", help="Run a sourcing job from JSON file")
    run_job_parser.add_argument("--file", required=True, help="Path to job JSON")
    run_job_parser.add_argument("--asset-view", choices=["canonical_merged", "strict_roster_only"], default="", help="Optional retrieval asset view override")
    run_job_parser.add_argument("--must-have-facet", action="append", default=[], help="Optional hard facet filter; repeatable")
    run_job_parser.add_argument("--must-have-primary-role-bucket", action="append", default=[], help="Optional hard primary role bucket filter; repeatable")

    plan_parser = subparsers.add_parser("plan", help="Create a sourcing plan from JSON file")
    plan_parser.add_argument("--file", required=True, help="Path to workflow request JSON")
    plan_parser.add_argument("--asset-view", choices=["canonical_merged", "strict_roster_only"], default="", help="Optional retrieval asset view override")
    plan_parser.add_argument("--must-have-facet", action="append", default=[], help="Optional hard facet filter; repeatable")
    plan_parser.add_argument("--must-have-primary-role-bucket", action="append", default=[], help="Optional hard primary role bucket filter; repeatable")

    review_plan_parser = subparsers.add_parser("review-plan", help="Review a plan review session from JSON file or natural-language instruction")
    review_plan_parser.add_argument("--file", default="", help="Path to plan review JSON")
    review_plan_parser.add_argument("--review-id", type=int, default=0, help="Plan review session id used with --instruction")
    review_plan_parser.add_argument("--instruction", default="", help="Natural-language review instruction")
    review_plan_parser.add_argument("--reviewer", default="", help="Reviewer name used with --instruction")
    review_plan_parser.add_argument("--action", default="approved", choices=["approved", "rejected", "needs_changes"], help="Review action used with --instruction")
    review_plan_parser.add_argument("--notes", default="", help="Optional review notes used with --instruction")
    review_plan_parser.add_argument("--preview", action="store_true", help="Print the structured review payload without applying it")

    refine_results_parser = subparsers.add_parser("refine-results", help="Refine an existing completed result set with natural-language filtering instructions")
    refine_results_parser.add_argument("--file", default="", help="Path to refinement JSON")
    refine_results_parser.add_argument("--job-id", default="", help="Baseline completed job id used with --instruction")
    refine_results_parser.add_argument("--instruction", default="", help="Natural-language refinement instruction")
    refine_results_parser.add_argument("--preview", action="store_true", help="Print the compiled refinement request without executing the rerun")

    show_plan_reviews_parser = subparsers.add_parser("show-plan-reviews", help="Show persisted plan review sessions")
    show_plan_reviews_parser.add_argument("--target-company", default="", help="Optional target company filter")
    show_plan_reviews_parser.add_argument("--brief", action="store_true", help="Show a compact summary instead of the full persisted payload")

    workflow_parser = subparsers.add_parser("start-workflow", help="Start a workflow and print the queued job metadata")
    workflow_parser.add_argument("--file", default="", help="Path to workflow request JSON")
    workflow_parser.add_argument("--plan-review-id", type=int, default=0, help="Approved plan review id to execute directly")
    workflow_parser.add_argument("--asset-view", choices=["canonical_merged", "strict_roster_only"], default="", help="Optional retrieval asset view override")
    workflow_parser.add_argument("--must-have-facet", action="append", default=[], help="Optional hard facet filter; repeatable")
    workflow_parser.add_argument("--must-have-primary-role-bucket", action="append", default=[], help="Optional hard primary role bucket filter; repeatable")
    workflow_parser.add_argument("--blocking", action="store_true", help="Run the workflow synchronously in the current CLI process")
    workflow_parser.add_argument("--no-auto-job-daemon", action="store_true", help="Do not auto-start a dedicated job-scoped recovery daemon")

    execute_workflow_parser = subparsers.add_parser("execute-workflow", help="Internal: execute a queued workflow job")
    execute_workflow_parser.add_argument("--job-id", required=True, help="Queued workflow job identifier")
    execute_workflow_parser.add_argument("--auto-job-daemon", action="store_true", help="Auto-start a dedicated job-scoped recovery daemon")
    supervise_workflow_parser = subparsers.add_parser("supervise-workflow", help="Internal: supervise workflow execution until it settles")
    supervise_workflow_parser.add_argument("--job-id", required=True, help="Queued workflow job identifier")
    supervise_workflow_parser.add_argument("--auto-job-daemon", action="store_true", help="Continuously run workflow recovery while supervising")
    supervise_workflow_parser.add_argument("--poll-seconds", type=float, default=2.0, help="Sleep between supervisor cycles")
    supervise_workflow_parser.add_argument("--max-ticks", type=int, default=0, help="Stop after N cycles; 0 means until settled")

    show_job_parser = subparsers.add_parser("show-job", help="Show stored job metadata and results")
    show_job_parser.add_argument("--job-id", required=True, help="Job identifier")

    show_progress_parser = subparsers.add_parser("show-progress", help="Show workflow progress summary for a job")
    show_progress_parser.add_argument("--job-id", required=True, help="Job identifier")

    show_system_progress_parser = subparsers.add_parser("show-system-progress", help="Show unified runtime/workflow/profile-sync/object-sync progress")
    show_system_progress_parser.add_argument("--active-limit", type=int, default=10, help="Max active workflow jobs to include")
    show_system_progress_parser.add_argument("--object-sync-limit", type=int, default=20, help="Max object sync progress snapshots to include")
    show_system_progress_parser.add_argument("--profile-registry-lookback-hours", type=int, default=24, help="Profile registry metrics lookback window")
    show_system_progress_parser.add_argument("--force-refresh", action="store_true", help="Bypass cached runtime metrics snapshot")

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

    supersede_jobs_parser = subparsers.add_parser(
        "supersede-workflow-jobs",
        help="Force-supersede specific workflow jobs and retire their workers",
    )
    supersede_jobs_parser.add_argument("--job-id", action="append", required=True, help="Workflow job id to supersede; repeatable")
    supersede_jobs_parser.add_argument("--replacement-job-id", default="", help="Optional replacement workflow job id")
    supersede_jobs_parser.add_argument("--reason", default="Superseded by operator cleanup.", help="Reason recorded on the retired jobs")

    show_recoverable_parser = subparsers.add_parser("show-recoverable-workers", help="Show recoverable workers across jobs")
    show_recoverable_parser.add_argument("--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable")
    show_recoverable_parser.add_argument("--lane-id", default="", help="Optional lane filter")
    show_recoverable_parser.add_argument("--job-id", default="", help="Optional job filter")
    show_recoverable_parser.add_argument("--limit", type=int, default=100, help="Max workers to return")

    cleanup_recoverable_parser = subparsers.add_parser(
        "cleanup-recoverable-workers",
        help="Retire stale recoverable workers, typically those hanging off terminal workflow jobs",
    )
    cleanup_recoverable_parser.add_argument("--stale-after-seconds", type=int, default=180, help="Minimum staleness threshold")
    cleanup_recoverable_parser.add_argument("--lane-id", default="", help="Optional lane filter")
    cleanup_recoverable_parser.add_argument("--job-id", default="", help="Optional job filter")
    cleanup_recoverable_parser.add_argument("--target-company", default="", help="Optional target company filter")
    cleanup_recoverable_parser.add_argument("--parent-job-status", action="append", default=[], help="Optional parent workflow status filter; repeatable")
    cleanup_recoverable_parser.add_argument("--limit", type=int, default=200, help="Max workers to inspect")
    cleanup_recoverable_parser.add_argument("--dry-run", action="store_true", help="Preview cleanup candidates without changing state")
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

    daemon_once_parser = subparsers.add_parser("run-worker-daemon-once", help="Run one cross-process worker recovery pass")
    daemon_once_parser.add_argument("--owner-id", default="", help="Optional daemon owner identifier")
    daemon_once_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_once_parser.add_argument("--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable")
    daemon_once_parser.add_argument("--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle")
    daemon_once_parser.add_argument("--job-id", default="", help="Optional job filter")

    daemon_forever_parser = subparsers.add_parser("run-worker-daemon", help="Run the cross-process worker recovery daemon loop")
    daemon_forever_parser.add_argument("--owner-id", default="", help="Optional daemon owner identifier")
    daemon_forever_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_forever_parser.add_argument("--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable")
    daemon_forever_parser.add_argument("--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle")
    daemon_forever_parser.add_argument("--job-id", default="", help="Optional job filter")
    daemon_forever_parser.add_argument("--poll-seconds", type=float, default=5.0, help="Sleep between cycles")
    daemon_forever_parser.add_argument("--max-ticks", type=int, default=0, help="Stop after N cycles; 0 means forever")

    daemon_service_parser = subparsers.add_parser("run-worker-daemon-service", help="Run worker recovery as a single-instance service loop")
    daemon_service_parser.add_argument("--service-name", default="worker-recovery-daemon", help="Persistent service instance name")
    daemon_service_parser.add_argument("--owner-id", default="", help="Optional daemon owner identifier")
    daemon_service_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_service_parser.add_argument("--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable")
    daemon_service_parser.add_argument("--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle")
    daemon_service_parser.add_argument("--job-id", default="", help="Optional job filter")
    daemon_service_parser.add_argument("--job-scoped", action="store_true", help="Auto-stop once the target job reaches terminal state")
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

    daemon_status_parser = subparsers.add_parser("show-daemon-status", help="Show persistent worker daemon service status")
    daemon_status_parser.add_argument("--service-name", default="worker-recovery-daemon", help="Persistent service instance name")

    daemon_unit_parser = subparsers.add_parser("write-worker-daemon-systemd-unit", help="Write a systemd unit for the worker daemon service")
    daemon_unit_parser.add_argument("--service-name", default="worker-recovery-daemon", help="Persistent service instance name")
    daemon_unit_parser.add_argument("--output-path", default="", help="Optional output path for the generated unit")
    daemon_unit_parser.add_argument("--python-bin", default="/usr/bin/env python3", help="Python executable used by ExecStart")
    daemon_unit_parser.add_argument("--user-name", default="", help="Optional system user for the service")
    daemon_unit_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_unit_parser.add_argument("--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable")
    daemon_unit_parser.add_argument("--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle")
    daemon_unit_parser.add_argument("--poll-seconds", type=float, default=5.0, help="Sleep between cycles")

    feedback_parser = subparsers.add_parser("record-feedback", help="Record criteria feedback from JSON file")
    feedback_parser.add_argument("--file", required=True, help="Path to feedback JSON")

    review_suggestion_parser = subparsers.add_parser("review-suggestion", help="Review a pattern suggestion from JSON file")
    review_suggestion_parser.add_argument("--file", required=True, help="Path to suggestion review JSON")

    review_manual_item_parser = subparsers.add_parser("review-manual-item", help="Review a manual review queue item from JSON file")
    review_manual_item_parser.add_argument("--file", required=True, help="Path to manual review JSON")

    synthesize_manual_item_parser = subparsers.add_parser("synthesize-manual-review", help="Generate and cache an evidence synthesis for one manual review item")
    synthesize_manual_item_parser.add_argument("--review-item-id", required=True, type=int, help="Manual review item id")
    synthesize_manual_item_parser.add_argument("--force-refresh", action="store_true", help="Ignore any cached synthesis and recompute it")

    confidence_policy_parser = subparsers.add_parser("configure-confidence-policy", help="Create, freeze, override, or clear a confidence policy control")
    confidence_policy_parser.add_argument("--file", required=True, help="Path to confidence policy control JSON")

    recompile_parser = subparsers.add_parser("recompile-criteria", help="Recompile criteria from JSON file")
    recompile_parser.add_argument("--file", required=True, help="Path to recompile request JSON")

    pattern_parser = subparsers.add_parser("show-criteria", help="Show persisted criteria patterns and feedback")
    pattern_parser.add_argument("--target-company", default="", help="Optional target company filter")

    manual_review_parser = subparsers.add_parser("show-manual-review", help="Show manual review queue items")
    manual_review_parser.add_argument("--target-company", default="", help="Optional target company filter")
    manual_review_parser.add_argument("--job-id", default="", help="Optional job identifier filter")

    company_snapshot_bundle_parser = subparsers.add_parser("export-company-snapshot-bundle", help="Export one company snapshot as a portable asset bundle")
    company_snapshot_bundle_parser.add_argument("--company", required=True, help="Company key or name")
    company_snapshot_bundle_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id; defaults to latest")
    company_snapshot_bundle_parser.add_argument("--output-dir", default="", help="Optional bundle export directory")

    company_handoff_bundle_parser = subparsers.add_parser("export-company-handoff-bundle", help="Export a company handoff bundle including snapshots and related runtime assets")
    company_handoff_bundle_parser.add_argument("--company", required=True, help="Company key or name")
    company_handoff_bundle_parser.add_argument("--output-dir", default="", help="Optional bundle export directory")
    company_handoff_bundle_parser.add_argument("--without-sqlite", action="store_true", help="Do not include SQLite snapshot")
    company_handoff_bundle_parser.add_argument("--without-live-tests", action="store_true", help="Do not include matching live test assets")
    company_handoff_bundle_parser.add_argument("--without-manual-review", action="store_true", help="Do not include manual review assets")
    company_handoff_bundle_parser.add_argument("--without-jobs", action="store_true", help="Do not include matching job JSON files")

    sqlite_snapshot_parser = subparsers.add_parser("export-sqlite-snapshot", help="Export the current SQLite database as a portable asset bundle")
    sqlite_snapshot_parser.add_argument("--output-dir", default="", help="Optional bundle export directory")

    company_artifact_parser = subparsers.add_parser("build-company-candidate-artifacts", help="Materialize normalized and reusable company candidate artifacts from SQLite + evidence")
    company_artifact_parser.add_argument("--company", required=True, help="Company key or name")
    company_artifact_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id; defaults to latest")
    company_artifact_parser.add_argument("--output-dir", default="", help="Optional artifact output directory")

    layered_outreach_parser = subparsers.add_parser("segment-company-outreach-layers", help="Build layered outreach segmentation from company candidate JSON assets")
    layered_outreach_parser.add_argument("--company", required=True, help="Company key or name")
    layered_outreach_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id; defaults to latest")
    layered_outreach_parser.add_argument("--asset-view", choices=["canonical_merged", "strict_roster_only"], default="canonical_merged", help="Candidate asset view to read")
    layered_outreach_parser.add_argument("--query", default="", help="Optional natural-language query used as context for AI verification")
    layered_outreach_parser.add_argument("--max-ai-verifications", type=int, default=80, help="Max candidates to AI-verify (0 disables)")
    layered_outreach_parser.add_argument("--ai-workers", type=int, default=8, help="Concurrent AI verification worker count")
    layered_outreach_parser.add_argument("--ai-max-retries", type=int, default=2, help="Retries for each failed AI verification request")
    layered_outreach_parser.add_argument("--ai-retry-backoff-seconds", type=float, default=0.8, help="Base backoff seconds for AI retry")
    layered_outreach_parser.add_argument("--provider", choices=["auto", "openai", "qwen"], default="openai", help="AI provider selection for verification")
    layered_outreach_parser.add_argument("--no-ai", action="store_true", help="Disable model-based verification and only run deterministic layers")
    layered_outreach_parser.add_argument("--summary-only", action="store_true", help="Print only compact summary fields")
    layered_outreach_parser.add_argument("--output-dir", default="", help="Optional output directory for layered analysis artifacts")

    complete_company_assets_parser = subparsers.add_parser("complete-company-assets", help="Continue company asset accumulation using known profile URLs and low-cost exploration")
    complete_company_assets_parser.add_argument("--company", required=True, help="Company key or name")
    complete_company_assets_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id; defaults to latest")
    complete_company_assets_parser.add_argument("--profile-detail-limit", type=int, default=12, help="Max candidates to complete via known LinkedIn URLs")
    complete_company_assets_parser.add_argument("--exploration-limit", type=int, default=3, help="Max unresolved candidates to explore via low-cost web search")
    complete_company_assets_parser.add_argument("--without-artifacts", action="store_true", help="Skip normalized/reusable artifact build")

    supplement_company_assets_parser = subparsers.add_parser("supplement-company-assets", help="Incrementally supplement an existing company snapshot with former search seed and/or profile enrichment")
    supplement_company_assets_parser.add_argument("--company", required=True, help="Company key or name")
    supplement_company_assets_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id; defaults to latest")
    supplement_company_assets_parser.add_argument("--run-former-search-seed", action="store_true", help="Run Harvest former-member search seed against the existing snapshot")
    supplement_company_assets_parser.add_argument("--former-search-limit", type=int, default=25, help="Requested former search result target")
    supplement_company_assets_parser.add_argument("--former-search-pages", type=int, default=1, help="Requested Harvest profile-search pages")
    supplement_company_assets_parser.add_argument("--former-query", action="append", default=[], help="Optional former search query text; repeatable")
    supplement_company_assets_parser.add_argument("--former-keyword", action="append", default=[], help="Optional former search keyword filter; repeatable")
    supplement_company_assets_parser.add_argument("--profile-scope", choices=["none", "current", "former", "all"], default="none", help="Which membership scope to profile-enrich")
    supplement_company_assets_parser.add_argument("--profile-limit", type=int, default=0, help="Max profiles to enrich; 0 means all selected")
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
    supplement_company_assets_parser.add_argument("--profile-force-refresh", action="store_true", help="Bypass local profile cache and refetch selected LinkedIn profiles")
    supplement_company_assets_parser.add_argument("--repair-current-roster-profile-refs", action="store_true", help="Restore current-roster canonical profile refs from the original harvest_company_employees visible asset")
    supplement_company_assets_parser.add_argument("--without-artifacts", action="store_true", help="Skip rebuilding normalized/reusable artifacts after supplement")

    backfill_registry_parser = subparsers.add_parser("backfill-linkedin-profile-registry", help="Backfill linkedin_profile_registry from historical runtime/company_assets/*/*/harvest_profiles JSON payloads")
    backfill_registry_parser.add_argument("--company", default="", help="Optional company key filter (e.g. anthropic)")
    backfill_registry_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id filter")
    backfill_registry_parser.add_argument("--checkpoint-path", default="", help="Optional checkpoint file path")
    backfill_registry_parser.add_argument("--progress-interval", type=int, default=200, help="Emit progress every N processed files")
    backfill_registry_parser.add_argument("--no-resume", action="store_true", help="Do not resume from prior checkpoint")

    profile_registry_metrics_parser = subparsers.add_parser("show-linkedin-profile-registry-metrics", help="Show profile registry cache/retry/queue metrics")
    profile_registry_metrics_parser.add_argument("--lookback-hours", type=int, default=24, help="Metrics lookback window in hours; 0 means all history")

    restore_bundle_parser = subparsers.add_parser("restore-asset-bundle", help="Restore a previously exported asset bundle into runtime")
    restore_bundle_parser.add_argument("--manifest", required=True, help="Path to bundle_manifest.json")
    restore_bundle_parser.add_argument("--target-runtime-dir", default="", help="Optional runtime dir override")
    restore_bundle_parser.add_argument("--conflict", choices=["skip", "overwrite", "error"], default="skip", help="How to handle existing files")

    upload_bundle_parser = subparsers.add_parser("upload-asset-bundle", help="Upload an exported asset bundle to configured object storage")
    upload_bundle_parser.add_argument("--manifest", required=True, help="Path to bundle_manifest.json")
    upload_bundle_parser.add_argument("--max-workers", type=int, default=0, help="Optional concurrent upload worker count; 0 uses config default")
    upload_bundle_parser.add_argument("--no-resume", action="store_true", help="Force re-upload even when the remote object already exists")
    upload_bundle_parser.add_argument("--archive-mode", choices=["auto", "none", "tar", "tar.gz"], default="auto", help="Bundle payload upload mode")

    download_bundle_parser = subparsers.add_parser("download-asset-bundle", help="Download an asset bundle from configured object storage")
    download_bundle_parser.add_argument("--bundle-kind", required=True, help="Bundle kind, e.g. company_handoff")
    download_bundle_parser.add_argument("--bundle-id", required=True, help="Bundle id")
    download_bundle_parser.add_argument("--output-dir", default="", help="Optional local export directory")
    download_bundle_parser.add_argument("--max-workers", type=int, default=0, help="Optional concurrent download worker count; 0 uses config default")
    download_bundle_parser.add_argument("--no-resume", action="store_true", help="Force re-download even when the local payload file already matches the manifest")

    restore_sqlite_parser = subparsers.add_parser("restore-sqlite-snapshot", help="Restore SQLite database from an exported bundle")
    restore_sqlite_parser.add_argument("--manifest", required=True, help="Path to bundle_manifest.json")
    restore_sqlite_parser.add_argument("--target-db-path", default="", help="Optional DB path override")
    restore_sqlite_parser.add_argument("--backup-dir", default="", help="Optional backup directory")
    restore_sqlite_parser.add_argument("--no-backup", action="store_true", help="Overwrite without backing up current DB")

    subparsers.add_parser("test-model", help="Run provider healthcheck")

    serve_parser = subparsers.add_parser("serve", help="Start the HTTP API")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)
    serve_parser.add_argument("--disable-runtime-watchdog", action="store_true", help="Disable the server-side recovery watchdog loop")
    serve_parser.add_argument("--runtime-watchdog-poll-seconds", type=float, default=15.0, help="Poll interval for the server-side recovery watchdog")

    args = parser.parse_args()
    if args.command in {"backfill-linkedin-profile-registry", "show-linkedin-profile-registry-metrics"}:
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        store = SQLiteStore(settings.db_path)
        if args.command == "backfill-linkedin-profile-registry":
            checkpoint_path = Path(args.checkpoint_path).expanduser() if str(args.checkpoint_path or "").strip() else None

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
        if args.command == "show-linkedin-profile-registry-metrics":
            metrics = store.get_linkedin_profile_registry_metrics(lookback_hours=max(0, int(args.lookback_hours or 0)))
            print(json.dumps(metrics, ensure_ascii=False, indent=2))
            return

    if args.command in {
        "export-company-snapshot-bundle",
        "export-company-handoff-bundle",
        "export-sqlite-snapshot",
        "build-company-candidate-artifacts",
        "segment-company-outreach-layers",
        "complete-company-assets",
        "supplement-company-assets",
        "restore-asset-bundle",
        "upload-asset-bundle",
        "download-asset-bundle",
        "restore-sqlite-snapshot",
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
                        include_sqlite=not args.without_sqlite,
                        include_live_tests=not args.without_live_tests,
                        include_manual_review=not args.without_manual_review,
                        include_jobs=not args.without_jobs,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        if args.command == "export-sqlite-snapshot":
            print(json.dumps(bundle_manager.export_sqlite_snapshot(output_dir=args.output_dir or None), ensure_ascii=False, indent=2))
            return
        if args.command == "build-company-candidate-artifacts":
            catalog = AssetCatalog.discover()
            settings = load_settings(catalog.project_root)
            store = SQLiteStore(settings.db_path)
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
                        raise SystemExit("Qwen is not enabled; provide qwen api key/base_url in runtime/secrets/providers.local.json")
                    model_client = QwenResponsesModelClient(settings.qwen)
                elif provider == "openai":
                    if not settings.model_provider.enabled:
                        raise SystemExit("OpenAI-compatible provider is not enabled; check model_provider config in runtime/secrets/providers.local.json")
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
                        "layer_1_name_signal": int((result.get("layers") or {}).get("layer_1_name_signal", {}).get("count") or 0),
                        "layer_2_greater_china_region_experience": int((result.get("layers") or {}).get("layer_2_greater_china_region_experience", {}).get("count") or 0),
                        "layer_3_mainland_china_experience_or_chinese_language": int(
                            (result.get("layers") or {}).get("layer_3_mainland_china_experience_or_chinese_language", {}).get("count") or 0
                        ),
                    },
                    "legacy_layer_count_aliases": {
                        "layer_2_greater_china_experience": int((result.get("layers") or {}).get("layer_2_greater_china_experience", {}).get("count") or 0),
                        "layer_3_mainland_or_chinese_language": int((result.get("layers") or {}).get("layer_3_mainland_or_chinese_language", {}).get("count") or 0),
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
            print(
                json.dumps(
                    manager.supplement_snapshot(
                        target_company=args.company,
                        snapshot_id=args.snapshot_id,
                        run_former_search_seed=bool(args.run_former_search_seed),
                        former_search_limit=int(args.former_search_limit or 25),
                        former_search_pages=int(args.former_search_pages or 1),
                        former_search_queries=list(args.former_query or []),
                        former_filter_hints={"keywords": list(args.former_keyword or [])},
                        profile_scope=str(args.profile_scope or "none"),
                        profile_limit=int(args.profile_limit or 0),
                        profile_only_missing_detail=bool(args.profile_only_missing_detail) and not bool(args.profile_all_known_urls),
                        profile_force_refresh=bool(args.profile_force_refresh),
                        repair_current_roster_profile_refs=bool(args.repair_current_roster_profile_refs),
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
        if args.command == "restore-sqlite-snapshot":
            print(
                json.dumps(
                    bundle_manager.restore_sqlite_snapshot(
                        args.manifest,
                        target_db_path=args.target_db_path or None,
                        backup_current=not args.no_backup,
                        backup_dir=args.backup_dir or None,
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
        payload["auto_job_daemon"] = not bool(args.no_auto_job_daemon)
        queued = orchestrator.start_workflow_runner_managed(payload)
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
        print(json.dumps(orchestrator.interrupt_agent_worker({"worker_id": args.worker_id}), ensure_ascii=False, indent=2))
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
        print(json.dumps(orchestrator.get_worker_daemon_status({"service_name": args.service_name}), ensure_ascii=False, indent=2))
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
        print(json.dumps(orchestrator.list_manual_review_items(args.target_company, args.job_id), ensure_ascii=False, indent=2))
        return

    if args.command == "test-model":
        print(json.dumps(orchestrator.healthcheck_model(), ensure_ascii=False, indent=2))
        return

    if args.command == "serve":
        watchdog_stop = None
        watchdog_thread = None
        if not args.disable_runtime_watchdog:
            watchdog_stop, watchdog_thread = start_server_runtime_watchdog(
                orchestrator,
                poll_seconds=float(args.runtime_watchdog_poll_seconds or 15.0),
            )
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
            if watchdog_thread is not None:
                watchdog_thread.join(timeout=max(1.0, float(args.runtime_watchdog_poll_seconds or 15.0)))


if __name__ == "__main__":
    main()
