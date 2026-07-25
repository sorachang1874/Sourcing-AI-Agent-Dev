"""CLI subcommand parser configuration — cli slice 2 (2026-07-22).

Extracted verbatim (indent shift only) from cli.main()'s 2,000-line parser
section (WS2 god-file wave; master plan cli registry-ification). One
configurator per subcommand, applied in original registration order by
cli.main(). Depends only on argparse — no import cycle with cli.
"""

from __future__ import annotations

import argparse


def _configure_bootstrap(subparsers: argparse._SubParsersAction) -> None:
    subparsers.add_parser("bootstrap", help="Load local assets into the runtime store")



def _configure_show_control_plane_runtime(subparsers: argparse._SubParsersAction) -> None:
    subparsers.add_parser(
        "show-control-plane-runtime",
        help="Print the resolved Postgres control-plane runtime configuration for the current workspace",
    )



def _configure_run_job(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_plan(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_explain_workflow(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_review_plan(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_refine_results(subparsers: argparse._SubParsersAction) -> None:
    refine_results_parser = subparsers.add_parser(
        "refine-results", help="Refine an existing completed result set with natural-language filtering instructions"
    )
    refine_results_parser.add_argument("--file", default="", help="Path to refinement JSON")
    refine_results_parser.add_argument("--job-id", default="", help="Baseline completed job id used with --instruction")
    refine_results_parser.add_argument("--instruction", default="", help="Natural-language refinement instruction")
    refine_results_parser.add_argument(
        "--preview", action="store_true", help="Print the compiled refinement request without executing the rerun"
    )



def _configure_show_plan_reviews(subparsers: argparse._SubParsersAction) -> None:
    show_plan_reviews_parser = subparsers.add_parser("show-plan-reviews", help="Show persisted plan review sessions")
    show_plan_reviews_parser.add_argument("--target-company", default="", help="Optional target company filter")
    show_plan_reviews_parser.add_argument(
        "--brief", action="store_true", help="Show a compact summary instead of the full persisted payload"
    )



def _configure_start_workflow(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_execute_workflow(subparsers: argparse._SubParsersAction) -> None:
    execute_workflow_parser = subparsers.add_parser("execute-workflow", help="Internal: execute a queued workflow job")
    execute_workflow_parser.add_argument("--job-id", required=True, help="Queued workflow job identifier")
    execute_workflow_parser.add_argument(
        "--auto-job-daemon", action="store_true", help="Auto-start a dedicated job-scoped recovery daemon"
    )



def _configure_supervise_workflow(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_show_job(subparsers: argparse._SubParsersAction) -> None:
    show_job_parser = subparsers.add_parser("show-job", help="Show stored job metadata and results")
    show_job_parser.add_argument("--job-id", required=True, help="Job identifier")



def _configure_show_progress(subparsers: argparse._SubParsersAction) -> None:
    show_progress_parser = subparsers.add_parser("show-progress", help="Show workflow progress summary for a job")
    show_progress_parser.add_argument("--job-id", required=True, help="Job identifier")



def _configure_show_system_progress(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_show_trace(subparsers: argparse._SubParsersAction) -> None:
    show_trace_parser = subparsers.add_parser("show-trace", help="Show agent runtime trace for a job")
    show_trace_parser.add_argument("--job-id", required=True, help="Job identifier")



def _configure_show_workers(subparsers: argparse._SubParsersAction) -> None:
    show_workers_parser = subparsers.add_parser("show-workers", help="Show autonomous workers for a job")
    show_workers_parser.add_argument("--job-id", required=True, help="Job identifier")



def _configure_show_scheduler(subparsers: argparse._SubParsersAction) -> None:
    show_scheduler_parser = subparsers.add_parser("show-scheduler", help="Show worker scheduler state for a job")
    show_scheduler_parser.add_argument("--job-id", required=True, help="Job identifier")



def _configure_cleanup_workflow_duplicates(subparsers: argparse._SubParsersAction) -> None:
    cleanup_duplicates_parser = subparsers.add_parser(
        "cleanup-workflow-duplicates",
        help="Supersede older in-flight workflow jobs when a newer completed job exists for the same request",
    )
    cleanup_duplicates_parser.add_argument("--target-company", default="", help="Optional target company filter")
    cleanup_duplicates_parser.add_argument("--active-limit", type=int, default=200, help="Max active jobs to inspect")



def _configure_cleanup_blocked_workflow_residue(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_supersede_workflow_jobs(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_show_recoverable_workers(subparsers: argparse._SubParsersAction) -> None:
    show_recoverable_parser = subparsers.add_parser(
        "show-recoverable-workers", help="Show recoverable workers across jobs"
    )
    show_recoverable_parser.add_argument(
        "--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable"
    )
    show_recoverable_parser.add_argument("--lane-id", default="", help="Optional lane filter")
    show_recoverable_parser.add_argument("--job-id", default="", help="Optional job filter")
    show_recoverable_parser.add_argument("--limit", type=int, default=100, help="Max workers to return")



def _configure_cleanup_recoverable_workers(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_interrupt_worker(subparsers: argparse._SubParsersAction) -> None:
    interrupt_worker_parser = subparsers.add_parser("interrupt-worker", help="Request interrupt for a worker")
    interrupt_worker_parser.add_argument("--worker-id", required=True, type=int, help="Worker identifier")



def _configure_run_worker_daemon_once(subparsers: argparse._SubParsersAction) -> None:
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
    daemon_once_parser.add_argument(
        "--disable-search-seed-discovery", action="store_true", help="Disable search-seed discovery item drain"
    )
    daemon_once_parser.add_argument(
        "--disable-profile-prefetch-refill", action="store_true", help="Disable registry profile-prefetch refill"
    )
    daemon_once_parser.add_argument(
        "--disable-snapshot-full-materialization", action="store_true", help="Disable full snapshot materialization drain"
    )
    daemon_once_parser.add_argument(
        "--disable-projection-facet-layering",
        action="store_true",
        help="Disable projection facet/layering build drain",
    )
    daemon_once_parser.add_argument(
        "--disable-excel-intake-recovery", action="store_true", help="Disable stale Excel intake recovery"
    )
    daemon_once_parser.add_argument(
        "--disable-post-recovery-housekeeping", action="store_true", help="Disable runtime heartbeat/metrics refresh"
    )



def _configure_run_worker_daemon(subparsers: argparse._SubParsersAction) -> None:
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
    daemon_forever_parser.add_argument(
        "--disable-search-seed-discovery", action="store_true", help="Disable search-seed discovery item drain"
    )
    daemon_forever_parser.add_argument(
        "--disable-profile-prefetch-refill", action="store_true", help="Disable registry profile-prefetch refill"
    )
    daemon_forever_parser.add_argument(
        "--disable-snapshot-full-materialization", action="store_true", help="Disable full snapshot materialization drain"
    )
    daemon_forever_parser.add_argument(
        "--disable-projection-facet-layering",
        action="store_true",
        help="Disable projection facet/layering build drain",
    )
    daemon_forever_parser.add_argument(
        "--disable-excel-intake-recovery", action="store_true", help="Disable stale Excel intake recovery"
    )
    daemon_forever_parser.add_argument(
        "--disable-post-recovery-housekeeping", action="store_true", help="Disable runtime heartbeat/metrics refresh"
    )



def _configure_run_worker_daemon_service(subparsers: argparse._SubParsersAction) -> None:
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
    daemon_service_parser.add_argument(
        "--explicit-worker-id",
        action="append",
        default=[],
        type=int,
        help="Explicit worker id to recover during a job-scoped event pulse; may be provided multiple times",
    )
    daemon_service_parser.add_argument(
        "--force-release-explicit-worker-leases",
        action="store_true",
        help="Release matching explicit worker leases before recovery, used for terminal remote provider events",
    )
    daemon_service_parser.add_argument(
        "--profile-prefetch-nonblocking-submit",
        action="store_true",
        help="Use nonblocking profile prefetch submit inside recovery callbacks to avoid long callback stalls",
    )
    daemon_service_parser.add_argument(
        "--disable-profile-prefetch-refill",
        action="store_true",
        help="Disable registry profile-prefetch refill inside the service callback",
    )
    daemon_service_parser.add_argument(
        "--profile-prefetch-refill-before-worker-recovery",
        action="store_true",
        help=(
            "Run one bounded job-scoped profile refill before recovering explicit workers; "
            "used for terminal remote-provider slot-release handoff"
        ),
    )
    daemon_service_parser.add_argument(
        "--disable-remote-event-followup",
        action="store_true",
        help="Disable same-tick remote-event follow-up worker recovery inside the service callback",
    )
    daemon_service_parser.add_argument(
        "--disable-search-seed-discovery",
        action="store_true",
        help="Disable search-seed discovery item drain inside the service callback",
    )
    daemon_service_parser.add_argument(
        "--disable-snapshot-full-materialization",
        action="store_true",
        help="Disable full snapshot materialization drain inside the service callback",
    )
    daemon_service_parser.add_argument(
        "--disable-projection-facet-layering",
        action="store_true",
        help="Disable projection facet/layering build drain inside the service callback",
    )
    daemon_service_parser.add_argument(
        "--disable-excel-intake-recovery",
        action="store_true",
        help="Disable stale Excel intake recovery inside the service callback",
    )
    daemon_service_parser.add_argument(
        "--disable-post-recovery-housekeeping",
        action="store_true",
        help="Disable runtime heartbeat/metrics refresh after the service callback",
    )
    daemon_service_parser.add_argument(
        "--disable-workflow-auto-resume",
        action="store_true",
        help="Disable automatic workflow auto-resume inside the service callback",
    )
    daemon_service_parser.add_argument(
        "--disable-workflow-explicit-job-resume",
        action="store_true",
        help="Disable immediate same-job workflow resume inside a job-scoped service callback",
    )
    daemon_service_parser.add_argument(
        "--disable-workflow-queue-auto-takeover",
        action="store_true",
        help="Disable automatic workflow queue takeover inside the service callback",
    )
    daemon_service_parser.add_argument("--poll-seconds", type=float, default=5.0, help="Sleep between cycles")
    daemon_service_parser.add_argument("--max-ticks", type=int, default=0, help="Stop after N cycles; 0 means forever")
    daemon_service_parser.add_argument(
        "--idle-stop-ticks",
        type=int,
        default=0,
        help="Stop after N consecutive idle cycles; 0 disables idle-based stop",
    )
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



def _configure_run_server_runtime_watchdog_service(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_show_daemon_status(subparsers: argparse._SubParsersAction) -> None:
    daemon_status_parser = subparsers.add_parser(
        "show-daemon-status", help="Show persistent worker daemon service status"
    )
    daemon_status_parser.add_argument(
        "--service-name", default="worker-recovery-daemon", help="Persistent service instance name"
    )



def _configure_write_worker_daemon_systemd_unit(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_record_feedback(subparsers: argparse._SubParsersAction) -> None:
    feedback_parser = subparsers.add_parser("record-feedback", help="Record criteria feedback from JSON file")
    feedback_parser.add_argument("--file", required=True, help="Path to feedback JSON")



def _configure_review_suggestion(subparsers: argparse._SubParsersAction) -> None:
    review_suggestion_parser = subparsers.add_parser(
        "review-suggestion", help="Review a pattern suggestion from JSON file"
    )
    review_suggestion_parser.add_argument("--file", required=True, help="Path to suggestion review JSON")



def _configure_review_manual_item(subparsers: argparse._SubParsersAction) -> None:
    review_manual_item_parser = subparsers.add_parser(
        "review-manual-item", help="Review a manual review queue item from JSON file"
    )
    review_manual_item_parser.add_argument("--file", required=True, help="Path to manual review JSON")



def _configure_synthesize_manual_review(subparsers: argparse._SubParsersAction) -> None:
    synthesize_manual_item_parser = subparsers.add_parser(
        "synthesize-manual-review", help="Generate and cache an evidence synthesis for one manual review item"
    )
    synthesize_manual_item_parser.add_argument(
        "--review-item-id", required=True, type=int, help="Manual review item id"
    )
    synthesize_manual_item_parser.add_argument(
        "--force-refresh", action="store_true", help="Ignore any cached synthesis and recompute it"
    )



def _configure_configure_confidence_policy(subparsers: argparse._SubParsersAction) -> None:
    confidence_policy_parser = subparsers.add_parser(
        "configure-confidence-policy", help="Create, freeze, override, or clear a confidence policy control"
    )
    confidence_policy_parser.add_argument("--file", required=True, help="Path to confidence policy control JSON")



def _configure_recompile_criteria(subparsers: argparse._SubParsersAction) -> None:
    recompile_parser = subparsers.add_parser("recompile-criteria", help="Recompile criteria from JSON file")
    recompile_parser.add_argument("--file", required=True, help="Path to recompile request JSON")



def _configure_show_criteria(subparsers: argparse._SubParsersAction) -> None:
    pattern_parser = subparsers.add_parser("show-criteria", help="Show persisted criteria patterns and feedback")
    pattern_parser.add_argument("--target-company", default="", help="Optional target company filter")



def _configure_show_manual_review(subparsers: argparse._SubParsersAction) -> None:
    manual_review_parser = subparsers.add_parser("show-manual-review", help="Show manual review queue items")
    manual_review_parser.add_argument("--target-company", default="", help="Optional target company filter")
    manual_review_parser.add_argument("--job-id", default="", help="Optional job identifier filter")



def _configure_export_company_snapshot_bundle(subparsers: argparse._SubParsersAction) -> None:
    company_snapshot_bundle_parser = subparsers.add_parser(
        "export-company-snapshot-bundle", help="Export one company snapshot as a portable asset bundle"
    )
    company_snapshot_bundle_parser.add_argument("--company", required=True, help="Company key or name")
    company_snapshot_bundle_parser.add_argument(
        "--snapshot-id", default="", help="Optional snapshot id; defaults to latest"
    )
    company_snapshot_bundle_parser.add_argument("--output-dir", default="", help="Optional bundle export directory")



def _configure_export_company_handoff_bundle(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_export_control_plane_snapshot_bundle(subparsers: argparse._SubParsersAction) -> None:
    control_plane_snapshot_bundle_parser = subparsers.add_parser(
        "export-control-plane-snapshot-bundle",
        help="Export the Postgres control-plane snapshot as a portable asset bundle",
    )
    control_plane_snapshot_bundle_parser.add_argument(
        "--output-dir", default="", help="Optional bundle export directory"
    )



def _configure_build_company_candidate_artifacts(subparsers: argparse._SubParsersAction) -> None:
    company_artifact_parser = subparsers.add_parser(
        "build-company-candidate-artifacts",
        help="Materialize normalized and reusable company candidate artifacts from snapshot candidate documents and historical snapshot evidence",
    )
    company_artifact_parser.add_argument("--company", required=True, help="Company key or name")
    company_artifact_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id; defaults to latest")
    company_artifact_parser.add_argument("--output-dir", default="", help="Optional artifact output directory")
    company_artifact_parser.add_argument(
        "--preferred-source-snapshot-id",
        action="append",
        default=[],
        help="Explicit source snapshot id to include; repeatable. Keeps rebuild scope auditable.",
    )
    company_artifact_parser.add_argument(
        "--build-profile",
        default="default",
        help="Artifact build profile, for example default or foreground_fast",
    )



def _configure_rebuild_company_serving_view(subparsers: argparse._SubParsersAction) -> None:
    company_serving_rebuild_parser = subparsers.add_parser(
        "rebuild-company-serving-view",
        help="Explicit company serving artifact rebuild/repair entrypoint with auditable snapshot selection",
    )
    company_serving_rebuild_parser.add_argument("--company", required=True, help="Company key or name")
    company_serving_rebuild_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id; defaults to latest")
    company_serving_rebuild_parser.add_argument("--output-dir", default="", help="Optional artifact output directory")
    company_serving_rebuild_parser.add_argument(
        "--preferred-source-snapshot-id",
        action="append",
        default=[],
        help="Explicit source snapshot id to include; repeatable",
    )
    company_serving_rebuild_parser.add_argument(
        "--build-profile",
        default="foreground_fast",
        help="Artifact build profile; foreground_fast is the default for serving projection repair",
    )



def _configure_audit_company_serving_view(subparsers: argparse._SubParsersAction) -> None:
    company_serving_audit_parser = subparsers.add_parser(
        "audit-company-serving-view",
        help="Audit company serving artifacts, source provenance projection, registry pointer, and optional job result view drift",
    )
    company_serving_audit_parser.add_argument("--company", required=True, help="Company key or name")
    company_serving_audit_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id; defaults to authoritative/latest")
    company_serving_audit_parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        help="Candidate asset view to audit",
    )
    company_serving_audit_parser.add_argument("--job-id", default="", help="Optional job id for job_result_view drift audit")
    company_serving_audit_parser.add_argument(
        "--sample-pages",
        type=int,
        default=2,
        help="Number of manifest pages to scan for source_matches/matched_keywords; use 0 to scan all pages",
    )



def _configure_audit_hot_cache_serving_artifacts(subparsers: argparse._SubParsersAction) -> None:
    hot_cache_audit_parser = subparsers.add_parser(
        "audit-hot-cache-serving-artifacts",
        help=(
            "Read-only audit for hot-cache serving artifacts. Reports missing manifest-referenced files, "
            "orphan JSON files, and canonical rehydrate plans."
        ),
    )
    hot_cache_audit_parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Optional company key/name filter; repeatable",
    )
    hot_cache_audit_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id filter")
    hot_cache_audit_parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        help="Candidate asset view to audit",
    )
    hot_cache_audit_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum hot-cache snapshots to scan; 0 scans all matching snapshots",
    )
    hot_cache_audit_parser.add_argument("--output", default="", help="Optional JSON output path")



def _configure_cleanup_hot_cache_serving_artifacts(subparsers: argparse._SubParsersAction) -> None:
    hot_cache_cleanup_parser = subparsers.add_parser(
        "cleanup-hot-cache-serving-artifacts",
        help="Dry-run or apply explicit hot-cache serving artifact cleanup and retention governance",
    )
    hot_cache_cleanup_parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Optional company key/name filter; repeatable",
    )
    hot_cache_cleanup_parser.add_argument("--snapshot-id", default="", help="Optional snapshot id filter")
    hot_cache_cleanup_parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply cleanup/retention changes; omitted means dry-run",
    )
    hot_cache_cleanup_parser.add_argument(
        "--keep-compatibility-exports",
        action="store_true",
        help="Keep legacy monolith compatibility JSON files instead of treating them as removable hot-cache files",
    )
    hot_cache_cleanup_parser.add_argument(
        "--ttl-seconds",
        type=int,
        default=0,
        help="Optional retention TTL in seconds; 0 disables TTL eviction",
    )
    hot_cache_cleanup_parser.add_argument(
        "--size-budget-bytes",
        type=int,
        default=0,
        help="Optional total hot-cache size budget; 0 disables size-budget eviction",
    )
    hot_cache_cleanup_parser.add_argument(
        "--max-bytes-per-company",
        type=int,
        default=0,
        help="Optional per-company hot-cache size budget; 0 disables per-company budget eviction",
    )
    hot_cache_cleanup_parser.add_argument(
        "--keep-latest-snapshots-per-company",
        type=int,
        default=1,
        help="Snapshot retention floor per company",
    )
    hot_cache_cleanup_parser.add_argument(
        "--max-generations-per-scope",
        type=int,
        default=0,
        help="Optional generation compaction limit per company/snapshot/view; 0 disables generation compaction",
    )
    hot_cache_cleanup_parser.add_argument("--output", default="", help="Optional JSON output path")



def _configure_audit_authoritative_reuse_planning(subparsers: argparse._SubParsersAction) -> None:
    authoritative_reuse_audit_parser = subparsers.add_parser(
        "audit-authoritative-reuse-planning",
        help="Read-only audit of requested population boundary, authoritative coverage proof, shard rows, and planner outcome",
    )
    authoritative_reuse_audit_parser.add_argument("--company", required=True, help="Company key or name")
    authoritative_reuse_audit_parser.add_argument(
        "--query",
        action="append",
        required=True,
        help="Representative user query to audit; repeat for a parity matrix",
    )
    authoritative_reuse_audit_parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        help="Candidate asset view to audit",
    )
    authoritative_reuse_audit_parser.add_argument(
        "--output",
        default="",
        help="Optional JSON output path for ECS/local parity comparison",
    )



def _configure_audit_authoritative_reuse_planning_matrix(subparsers: argparse._SubParsersAction) -> None:
    authoritative_reuse_matrix_parser = subparsers.add_parser(
        "audit-authoritative-reuse-planning-matrix",
        help="Run a read-only authoritative reuse planning audit matrix for ECS/local parity",
    )
    authoritative_reuse_matrix_parser.add_argument("--matrix", required=True, help="JSON matrix file with cases")
    authoritative_reuse_matrix_parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        help="Default candidate asset view when a case does not specify one",
    )
    authoritative_reuse_matrix_parser.add_argument(
        "--output",
        default="",
        help="Optional JSON output path",
    )
    authoritative_reuse_matrix_parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Exclude full per-case audit payloads and keep only summaries",
    )
    authoritative_reuse_matrix_parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when matrix expectations fail",
    )



def _configure_compare_authoritative_reuse_planning_matrix(subparsers: argparse._SubParsersAction) -> None:
    authoritative_reuse_compare_parser = subparsers.add_parser(
        "compare-authoritative-reuse-planning-matrix",
        help="Compare two authoritative reuse planning matrix reports from ECS/local runs",
    )
    authoritative_reuse_compare_parser.add_argument("--left", required=True, help="Left/local matrix report JSON")
    authoritative_reuse_compare_parser.add_argument("--right", required=True, help="Right/ECS matrix report JSON")
    authoritative_reuse_compare_parser.add_argument(
        "--field",
        action="append",
        default=[],
        help="Summary field to compare; repeatable. Defaults to core planner parity fields.",
    )
    authoritative_reuse_compare_parser.add_argument("--output", default="", help="Optional JSON output path")
    authoritative_reuse_compare_parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when drift is detected",
    )



def _configure_repoint_job_result_view(subparsers: argparse._SubParsersAction) -> None:
    repoint_job_result_view_parser = subparsers.add_parser(
        "repoint-job-result-view",
        help="Dry-run or apply an explicit job_result_view repoint policy",
    )
    repoint_job_result_view_parser.add_argument("--job-id", required=True, help="Job id to inspect or update")
    repoint_job_result_view_parser.add_argument("--company", default="", help="Company key/name; defaults to current result view company")
    repoint_job_result_view_parser.add_argument("--snapshot-id", default="", help="Target snapshot id; defaults to authoritative")
    repoint_job_result_view_parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        help="Target candidate asset view",
    )
    repoint_job_result_view_parser.add_argument(
        "--policy",
        choices=["historical_replay", "serve_latest_company_asset"],
        default="historical_replay",
        help="historical_replay is no-op; serve_latest_company_asset requires explicit --apply to mutate",
    )
    repoint_job_result_view_parser.add_argument("--reason", default="", help="Operator reason recorded in metadata")
    repoint_job_result_view_parser.add_argument("--apply", action="store_true", help="Apply the repoint; omitted means dry-run")



def _configure_audit_job_result_view_consistency(subparsers: argparse._SubParsersAction) -> None:
    job_result_view_consistency_parser = subparsers.add_parser(
        "audit-job-result-view-consistency",
        help=(
            "Audit job_result_view, job summary candidate_source, and authoritative registry consistency. "
            "Only full-local/full-asset reuse jobs are eligible for --apply repoint."
        ),
    )
    job_result_view_consistency_parser.add_argument("--job-id", default="", help="Optional single job id to audit")
    job_result_view_consistency_parser.add_argument("--company", default="", help="Optional company filter")
    job_result_view_consistency_parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        help="Candidate asset view to compare",
    )
    job_result_view_consistency_parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Completed jobs to scan when --job-id is omitted",
    )
    job_result_view_consistency_parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply safe repoints for eligible full-local/full-asset reuse jobs; scoped/delta jobs remain manual",
    )
    job_result_view_consistency_parser.add_argument("--output", default="", help="Optional JSON output path")



def _configure_segment_company_outreach_layers(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_complete_company_assets(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_supplement_company_assets(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_refresh_company_public_web_assets(subparsers: argparse._SubParsersAction) -> None:
    refresh_company_public_web_parser = subparsers.add_parser(
        "refresh-company-public-web-assets",
        help=(
            "Refresh company-level Public Web assets through the API/CLI-only lane. "
            "This does not enable default workflow Public Web Stage 2 or target-candidate Public Web Search."
        ),
    )
    refresh_company_public_web_parser.add_argument("--target-company", "--company", dest="target_company", required=True)
    refresh_company_public_web_parser.add_argument(
        "--source-family",
        action="append",
        default=[],
        help="Company-level source family; repeatable. Defaults to homepage/blog/research/engineering/news/docs.",
    )
    refresh_company_public_web_parser.add_argument(
        "--seed-url",
        action="append",
        default=[],
        help="Explicit company-level seed URL; repeatable. Defaults are generated from company key.",
    )
    refresh_company_public_web_parser.add_argument("--max-assets", type=int, default=50)
    refresh_company_public_web_parser.add_argument(
        "--collection-mode",
        choices=["seed_url_only", "provider_search", "collector_bundle"],
        default="seed_url_only",
        help=(
            "Collection mode. provider_search explicitly uses the configured search provider; "
            "collector_bundle imports model-safe RSS/arXiv/OpenReview/crawl records; default remains seed_url_only."
        ),
    )
    refresh_company_public_web_parser.add_argument(
        "--collector-input-json",
        default="",
        help="Optional JSON file containing collector_inputs for collector_bundle mode.",
    )
    refresh_company_public_web_parser.add_argument(
        "--collector-source-url",
        action="append",
        default=[],
        help=(
            "Live collector source URL to fetch for collector_bundle mode; repeatable. "
            "Use --collector-source-json for typed RSS/arXiv/OpenReview/crawl sources."
        ),
    )
    refresh_company_public_web_parser.add_argument(
        "--collector-source-json",
        default="",
        help="Optional JSON file containing collector_sources for live collector_bundle fetching.",
    )
    refresh_company_public_web_parser.add_argument(
        "--discover-collector-sources",
        action="store_true",
        help=(
            "For collector_bundle mode, derive RSS/arXiv/OpenReview/crawl source URLs from source families "
            "and then fetch them through the normal collector source path."
        ),
    )
    refresh_company_public_web_parser.add_argument("--max-discovered-collector-sources", type=int, default=12)
    refresh_company_public_web_parser.add_argument("--max-queries", type=int, default=6)
    refresh_company_public_web_parser.add_argument("--max-results-per-query", type=int, default=10)
    refresh_company_public_web_parser.add_argument("--force-refresh", action="store_true")
    refresh_company_public_web_parser.add_argument("--requested-by", default="cli")
    refresh_company_public_web_parser.add_argument("--run-id", default="")
    refresh_company_public_web_parser.add_argument("--output", default="", help="Optional JSON output path")



def _configure_list_company_public_web_assets(subparsers: argparse._SubParsersAction) -> None:
    list_company_public_web_parser = subparsers.add_parser(
        "list-company-public-web-assets",
        help="List persisted company-level Public Web runs/assets without triggering refresh.",
    )
    list_company_public_web_parser.add_argument("--target-company", "--company", dest="target_company", default="")
    list_company_public_web_parser.add_argument("--company-key", default="")
    list_company_public_web_parser.add_argument("--source-family", default="")
    list_company_public_web_parser.add_argument("--status", default="")
    list_company_public_web_parser.add_argument("--limit", type=int, default=100)



def _configure_intake_excel(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_continue_excel_intake(subparsers: argparse._SubParsersAction) -> None:
    continue_excel_intake_parser = subparsers.add_parser(
        "continue-excel-intake",
        help="Continue an Excel intake manual-review row by selecting a local candidate or a fetched profile",
    )
    continue_excel_intake_parser.add_argument(
        "--file", required=True, help="Path to a JSON payload describing intake_id + decisions"
    )



def _configure_promote_asset_default_pointer(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_backfill_linkedin_profile_registry(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_backfill_organization_asset_registry(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_backfill_authoritative_population_coverage(subparsers: argparse._SubParsersAction) -> None:
    backfill_population_coverage_parser = subparsers.add_parser(
        "backfill-authoritative-population-coverage",
        help="Backfill explicit population_coverage metadata for organization asset registry rows",
    )
    backfill_population_coverage_parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Company key or canonical name; repeatable. Omit to scan authoritative rows up to --limit.",
    )
    backfill_population_coverage_parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        help="Organization asset registry view to backfill",
    )
    backfill_population_coverage_parser.add_argument(
        "--include-non-authoritative",
        action="store_true",
        help="Also backfill non-authoritative historical rows; default is authoritative rows only",
    )
    backfill_population_coverage_parser.add_argument(
        "--force",
        action="store_true",
        help="Rewrite existing population_coverage metadata instead of skipping rows that already have it",
    )
    backfill_population_coverage_parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max registry rows to scan when --company is omitted",
    )
    backfill_population_coverage_parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist changes; omitted means dry-run",
    )



def _configure_repair_authoritative_serving_generation(subparsers: argparse._SubParsersAction) -> None:
    repair_authoritative_serving_parser = subparsers.add_parser(
        "repair-authoritative-serving-generation",
        help="Republish an authoritative serving generation when completed shard bundles are not subsumed",
    )
    repair_authoritative_serving_parser.add_argument("--company", required=True, help="Company key or canonical name")
    repair_authoritative_serving_parser.add_argument(
        "--query",
        action="append",
        required=True,
        help="Representative query whose audit exposes the serving-generation lag; repeatable",
    )
    repair_authoritative_serving_parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        help="Organization asset view to repair",
    )
    repair_authoritative_serving_parser.add_argument(
        "--snapshot-id",
        default="",
        help="Optional baseline snapshot id; defaults to the audited authoritative baseline",
    )
    repair_authoritative_serving_parser.add_argument(
        "--repair-snapshot-id",
        default="",
        help="Optional new repair snapshot id; defaults to the current UTC timestamp",
    )
    repair_authoritative_serving_parser.add_argument(
        "--build-profile",
        default="foreground_fast",
        help="Candidate artifact build profile for the republished serving view",
    )
    repair_authoritative_serving_parser.add_argument(
        "--output-dir",
        default="",
        help="Optional artifact output directory for the repair build",
    )
    repair_authoritative_serving_parser.add_argument(
        "--output",
        default="",
        help="Optional JSON report path",
    )
    repair_authoritative_serving_parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the repair; omitted means offline dry-run",
    )



def _configure_normalize_authoritative_source_provenance(subparsers: argparse._SubParsersAction) -> None:
    normalize_authoritative_provenance_parser = subparsers.add_parser(
        "normalize-authoritative-source-provenance",
        help="Normalize authoritative selected snapshots to serving snapshot plus reusable shard sources",
    )
    normalize_authoritative_provenance_parser.add_argument(
        "--company", required=True, help="Company key or canonical name"
    )
    normalize_authoritative_provenance_parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        help="Organization asset view to normalize",
    )
    normalize_authoritative_provenance_parser.add_argument(
        "--output",
        default="",
        help="Optional JSON report path",
    )
    normalize_authoritative_provenance_parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist normalized provenance; omitted means dry-run",
    )



def _configure_backfill_job_result_lifecycle(subparsers: argparse._SubParsersAction) -> None:
    backfill_lifecycle_parser = subparsers.add_parser(
        "backfill-job-result-lifecycle",
        help="Migrate/audit serialized job_result_lifecycle evidence; missing evidence is repair-required",
    )
    backfill_lifecycle_parser.add_argument(
        "--dry-run", action="store_true", help="Preview changes without persisting"
    )
    backfill_lifecycle_parser.add_argument(
        "--verbose", action="store_true", help="Print detailed progress for each job"
    )
    backfill_lifecycle_parser.add_argument(
        "--batch-size", type=int, default=100, help="Number of jobs to process before reporting progress"
    )



def _configure_backfill_snapshot_full_materialization_items(subparsers: argparse._SubParsersAction) -> None:
    backfill_snapshot_materialization_parser = subparsers.add_parser(
        "backfill-snapshot-full-materialization-items",
        help="Backfill durable snapshot_full_materialization queue items for legacy scheduled jobs",
    )
    backfill_snapshot_materialization_parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist queue items; omitted means dry-run",
    )
    backfill_snapshot_materialization_parser.add_argument(
        "--limit",
        type=int,
        default=100000,
        help="Max completed workflow jobs to scan",
    )
    backfill_snapshot_materialization_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of jobs to process before reporting progress",
    )
    backfill_snapshot_materialization_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed progress while scanning",
    )



def _configure_backfill_local_apply_closure_items(subparsers: argparse._SubParsersAction) -> None:
    backfill_local_apply_parser = subparsers.add_parser(
        "backfill-local-apply-closure-items",
        help="Backfill durable local_apply_closure queue items from legacy inline apply-only worker markers",
    )
    backfill_local_apply_parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist queue items; omitted means dry-run",
    )
    backfill_local_apply_parser.add_argument("--job-id", default="", help="Optional job filter")
    backfill_local_apply_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max legacy apply-only workers to scan",
    )
    backfill_local_apply_parser.add_argument(
        "--job-scan-limit",
        type=int,
        default=500,
        help="Max workflow jobs to scan when --job-id is omitted",
    )



def _configure_repair_excel_intake_artifacts(subparsers: argparse._SubParsersAction) -> None:
    repair_excel_artifacts_parser = subparsers.add_parser(
        "repair-excel-intake-artifacts",
        help="Enqueue or run deferred Excel intake full artifact materialization for one job",
    )
    repair_excel_artifacts_parser.add_argument("--job-id", required=True, help="Excel intake job id")
    repair_excel_artifacts_parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist the durable materialization item; omitted means dry-run",
    )
    repair_excel_artifacts_parser.add_argument(
        "--run-now",
        action="store_true",
        help="After enqueueing, claim and process one snapshot_full_materialization item immediately",
    )



def _configure_backfill_search_seed_discovery_items(subparsers: argparse._SubParsersAction) -> None:
    backfill_search_seed_parser = subparsers.add_parser(
        "backfill-search-seed-discovery-items",
        help="Backfill durable search_seed_discovery_query queue items from legacy search workers",
    )
    backfill_search_seed_parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist queue items; omitted means dry-run",
    )
    backfill_search_seed_parser.add_argument("--job-id", default="", help="Optional job filter")
    backfill_search_seed_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max legacy search-seed workers to scan",
    )
    backfill_search_seed_parser.add_argument(
        "--job-scan-limit",
        type=int,
        default=500,
        help="Max workflow jobs to scan when --job-id is omitted",
    )



def _configure_rebuild_runtime_control_plane(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_repair_company_candidate_artifacts(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_repair_paginated_candidate_artifacts(subparsers: argparse._SubParsersAction) -> None:
    repair_paginated_artifacts_parser = subparsers.add_parser(
        "repair-paginated-candidate-artifacts",
        help=(
            "Rebuild missing normalized manifest/pages/candidate shards from existing "
            "materialized_candidate_documents.json without changing provider state or registry authority"
        ),
    )
    repair_paginated_artifacts_parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Optional company key or canonical name filter; repeatable",
    )
    repair_paginated_artifacts_parser.add_argument(
        "--snapshot-id",
        default="",
        help="Optional snapshot id filter",
    )
    repair_paginated_artifacts_parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        choices=["canonical_merged", "strict_roster_only"],
        help="Organization asset registry view to scan; canonical_merged also repairs strict_roster_only child views",
    )
    repair_paginated_artifacts_parser.add_argument(
        "--include-history",
        action="store_true",
        help="Also repair non-authoritative historical registry rows; default is authoritative rows only",
    )
    repair_paginated_artifacts_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report missing paginated artifacts; do not write files",
    )



def _configure_backfill_structured_timeline(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_repair_profile_signal_projection(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_show_linkedin_profile_registry_metrics(subparsers: argparse._SubParsersAction) -> None:
    profile_registry_metrics_parser = subparsers.add_parser(
        "show-linkedin-profile-registry-metrics", help="Show profile registry cache/retry/queue metrics"
    )
    profile_registry_metrics_parser.add_argument(
        "--lookback-hours", type=int, default=24, help="Metrics lookback window in hours; 0 means all history"
    )



def _configure_restore_asset_bundle(subparsers: argparse._SubParsersAction) -> None:
    restore_bundle_parser = subparsers.add_parser(
        "restore-asset-bundle", help="Restore a previously exported asset bundle into runtime"
    )
    restore_bundle_parser.add_argument("--manifest", required=True, help="Path to bundle_manifest.json")
    restore_bundle_parser.add_argument("--target-runtime-dir", default="", help="Optional runtime dir override")
    restore_bundle_parser.add_argument(
        "--conflict", choices=["skip", "overwrite", "error"], default="skip", help="How to handle existing files"
    )



def _configure_upload_asset_bundle(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_publish_candidate_generation(subparsers: argparse._SubParsersAction) -> None:
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
    publish_generation_parser.add_argument(
        "--skip-hot-cache-governance",
        action="store_true",
        help="Skip the post-publish local hot-cache governance cycle",
    )



def _configure_delete_asset_bundle(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_download_asset_bundle(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_import_cloud_assets(subparsers: argparse._SubParsersAction) -> None:
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
        "--skip-hot-cache-governance",
        action="store_true",
        help="Skip the post-hydrate hot-cache governance cycle for candidate-generation imports",
    )
    import_cloud_assets_parser.add_argument(
        "--profile-progress-interval", type=int, default=200, help="Progress interval used by profile registry backfill"
    )
    import_cloud_assets_parser.add_argument(
        "--profile-no-resume",
        action="store_true",
        help="Do not resume prior linkedin profile registry backfill checkpoints",
    )



def _configure_hydrate_candidate_generation(subparsers: argparse._SubParsersAction) -> None:
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
    hydrate_generation_parser.add_argument(
        "--skip-hot-cache-governance",
        action="store_true",
        help="Skip the post-hydrate hot-cache governance cycle",
    )



def _configure_export_control_plane_snapshot(subparsers: argparse._SubParsersAction) -> None:
    export_control_plane_parser = subparsers.add_parser(
        "export-control-plane-snapshot",
        help=(
            "Export projection/domain control-plane state plus generation indexes as a Postgres migration "
            "snapshot; the PG-only durable runtime causal aggregate is excluded and recorded as a typed gap"
        ),
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



def _configure_sync_control_plane_postgres(subparsers: argparse._SubParsersAction) -> None:
    sync_control_plane_parser = subparsers.add_parser(
        "sync-control-plane-postgres",
        help=(
            "Sync an exported control-plane snapshot into Postgres, or run a migration-only runtime mirror; "
            "PG-only durable runtime tables are rejected"
        ),
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



def _configure_launch_detached(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_run_target_candidate_public_web_experiment(subparsers: argparse._SubParsersAction) -> None:
    public_web_experiment_parser = subparsers.add_parser(
        "run-target-candidate-public-web-experiment",
        help=(
            "Retired target-candidate Public Web experiment entrypoint. "
            "Use CRM Public Web live/product validation instead."
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



def _configure_evaluate_public_web_quality(subparsers: argparse._SubParsersAction) -> None:
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



def _configure_test_model(subparsers: argparse._SubParsersAction) -> None:
    subparsers.add_parser(
        "test-model",
        help="Run provider healthcheck. For low-cost workflow smoke tests, set SOURCING_EXTERNAL_PROVIDER_MODE=simulate or replay to disable Harvest/Search/model/semantic live calls.",
    )



def _configure_serve(subparsers: argparse._SubParsersAction) -> None:
    serve_parser = subparsers.add_parser(
        "serve",
        help="Start the HTTP API. External provider mode still comes from SOURCING_EXTERNAL_PROVIDER_MODE=live|replay|simulate.",
    )
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)
    runtime_watchdog_mode = serve_parser.add_mutually_exclusive_group()
    runtime_watchdog_mode.add_argument(
        "--enable-runtime-watchdog",
        action="store_true",
        help=(
            "Dev-only compatibility mode: run shared recovery and the runtime watchdog "
            "inside the API process. Production/default serve requires a fresh external daemon."
        ),
    )
    runtime_watchdog_mode.add_argument(
        "--disable-runtime-watchdog",
        action="store_true",
        help=(
            "Deprecated compatibility no-op. In-process recovery is disabled by default; "
            "serve requires a fresh external daemon unless explicitly opted out."
        ),
    )
    serve_parser.add_argument(
        "--runtime-watchdog-poll-seconds",
        type=float,
        default=15.0,
        help="Poll interval for the server-side recovery watchdog",
    )
    serve_parser.add_argument(
        "--allow-uncovered-recovery",
        action="store_true",
        help=(
            "API-only deployment opt-out (Step 5a): start serve even when neither an "
            "in-process recovery thread nor a fresh external worker-recovery-daemon "
            "covers recovery. Loudly logged. Only use when recovery truly runs elsewhere."
        ),
    )



# Applied in the original registration order (help output order preserved).
CONFIGURATORS = (
    _configure_bootstrap,
    _configure_show_control_plane_runtime,
    _configure_run_job,
    _configure_plan,
    _configure_explain_workflow,
    _configure_review_plan,
    _configure_refine_results,
    _configure_show_plan_reviews,
    _configure_start_workflow,
    _configure_execute_workflow,
    _configure_supervise_workflow,
    _configure_show_job,
    _configure_show_progress,
    _configure_show_system_progress,
    _configure_show_trace,
    _configure_show_workers,
    _configure_show_scheduler,
    _configure_cleanup_workflow_duplicates,
    _configure_cleanup_blocked_workflow_residue,
    _configure_supersede_workflow_jobs,
    _configure_show_recoverable_workers,
    _configure_cleanup_recoverable_workers,
    _configure_interrupt_worker,
    _configure_run_worker_daemon_once,
    _configure_run_worker_daemon,
    _configure_run_worker_daemon_service,
    _configure_run_server_runtime_watchdog_service,
    _configure_show_daemon_status,
    _configure_write_worker_daemon_systemd_unit,
    _configure_record_feedback,
    _configure_review_suggestion,
    _configure_review_manual_item,
    _configure_synthesize_manual_review,
    _configure_configure_confidence_policy,
    _configure_recompile_criteria,
    _configure_show_criteria,
    _configure_show_manual_review,
    _configure_export_company_snapshot_bundle,
    _configure_export_company_handoff_bundle,
    _configure_export_control_plane_snapshot_bundle,
    _configure_build_company_candidate_artifacts,
    _configure_rebuild_company_serving_view,
    _configure_audit_company_serving_view,
    _configure_audit_hot_cache_serving_artifacts,
    _configure_cleanup_hot_cache_serving_artifacts,
    _configure_audit_authoritative_reuse_planning,
    _configure_audit_authoritative_reuse_planning_matrix,
    _configure_compare_authoritative_reuse_planning_matrix,
    _configure_repoint_job_result_view,
    _configure_audit_job_result_view_consistency,
    _configure_segment_company_outreach_layers,
    _configure_complete_company_assets,
    _configure_supplement_company_assets,
    _configure_refresh_company_public_web_assets,
    _configure_list_company_public_web_assets,
    _configure_intake_excel,
    _configure_continue_excel_intake,
    _configure_promote_asset_default_pointer,
    _configure_backfill_linkedin_profile_registry,
    _configure_backfill_organization_asset_registry,
    _configure_backfill_authoritative_population_coverage,
    _configure_repair_authoritative_serving_generation,
    _configure_normalize_authoritative_source_provenance,
    _configure_backfill_job_result_lifecycle,
    _configure_backfill_snapshot_full_materialization_items,
    _configure_backfill_local_apply_closure_items,
    _configure_repair_excel_intake_artifacts,
    _configure_backfill_search_seed_discovery_items,
    _configure_rebuild_runtime_control_plane,
    _configure_repair_company_candidate_artifacts,
    _configure_repair_paginated_candidate_artifacts,
    _configure_backfill_structured_timeline,
    _configure_repair_profile_signal_projection,
    _configure_show_linkedin_profile_registry_metrics,
    _configure_restore_asset_bundle,
    _configure_upload_asset_bundle,
    _configure_publish_candidate_generation,
    _configure_delete_asset_bundle,
    _configure_download_asset_bundle,
    _configure_import_cloud_assets,
    _configure_hydrate_candidate_generation,
    _configure_export_control_plane_snapshot,
    _configure_sync_control_plane_postgres,
    _configure_launch_detached,
    _configure_run_target_candidate_public_web_experiment,
    _configure_evaluate_public_web_quality,
    _configure_test_model,
    _configure_serve,
)
