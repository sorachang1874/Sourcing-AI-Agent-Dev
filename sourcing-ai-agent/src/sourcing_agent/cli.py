from __future__ import annotations

import argparse
import json
from pathlib import Path

from .acquisition import AcquisitionEngine
from .agent_runtime import AgentRuntimeCoordinator
from .api import create_server
from .asset_catalog import AssetCatalog
from .asset_sync import AssetBundleManager
from .model_provider import build_model_client
from .orchestrator import SourcingOrchestrator
from .service_daemon import SingleInstanceError
from .semantic_provider import build_semantic_provider
from .settings import load_settings
from .storage import SQLiteStore


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Sourcing AI Agent backend MVP")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap", help="Load local assets into SQLite")

    run_job_parser = subparsers.add_parser("run-job", help="Run a sourcing job from JSON file")
    run_job_parser.add_argument("--file", required=True, help="Path to job JSON")

    plan_parser = subparsers.add_parser("plan", help="Create a sourcing plan from JSON file")
    plan_parser.add_argument("--file", required=True, help="Path to workflow request JSON")

    review_plan_parser = subparsers.add_parser("review-plan", help="Review a plan review session from JSON file")
    review_plan_parser.add_argument("--file", required=True, help="Path to plan review JSON")

    show_plan_reviews_parser = subparsers.add_parser("show-plan-reviews", help="Show persisted plan review sessions")
    show_plan_reviews_parser.add_argument("--target-company", default="", help="Optional target company filter")

    workflow_parser = subparsers.add_parser("start-workflow", help="Run a workflow in the current CLI process")
    workflow_parser.add_argument("--file", required=True, help="Path to workflow request JSON")

    show_job_parser = subparsers.add_parser("show-job", help="Show stored job metadata and results")
    show_job_parser.add_argument("--job-id", required=True, help="Job identifier")

    show_trace_parser = subparsers.add_parser("show-trace", help="Show agent runtime trace for a job")
    show_trace_parser.add_argument("--job-id", required=True, help="Job identifier")

    show_workers_parser = subparsers.add_parser("show-workers", help="Show autonomous workers for a job")
    show_workers_parser.add_argument("--job-id", required=True, help="Job identifier")

    show_scheduler_parser = subparsers.add_parser("show-scheduler", help="Show worker scheduler state for a job")
    show_scheduler_parser.add_argument("--job-id", required=True, help="Job identifier")

    show_recoverable_parser = subparsers.add_parser("show-recoverable-workers", help="Show recoverable workers across jobs")
    show_recoverable_parser.add_argument("--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable")
    show_recoverable_parser.add_argument("--lane-id", default="", help="Optional lane filter")
    show_recoverable_parser.add_argument("--limit", type=int, default=100, help="Max workers to return")

    interrupt_worker_parser = subparsers.add_parser("interrupt-worker", help="Request interrupt for a worker")
    interrupt_worker_parser.add_argument("--worker-id", required=True, type=int, help="Worker identifier")

    daemon_once_parser = subparsers.add_parser("run-worker-daemon-once", help="Run one cross-process worker recovery pass")
    daemon_once_parser.add_argument("--owner-id", default="", help="Optional daemon owner identifier")
    daemon_once_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_once_parser.add_argument("--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable")
    daemon_once_parser.add_argument("--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle")

    daemon_forever_parser = subparsers.add_parser("run-worker-daemon", help="Run the cross-process worker recovery daemon loop")
    daemon_forever_parser.add_argument("--owner-id", default="", help="Optional daemon owner identifier")
    daemon_forever_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_forever_parser.add_argument("--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable")
    daemon_forever_parser.add_argument("--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle")
    daemon_forever_parser.add_argument("--poll-seconds", type=float, default=5.0, help="Sleep between cycles")
    daemon_forever_parser.add_argument("--max-ticks", type=int, default=0, help="Stop after N cycles; 0 means forever")

    daemon_service_parser = subparsers.add_parser("run-worker-daemon-service", help="Run worker recovery as a single-instance service loop")
    daemon_service_parser.add_argument("--service-name", default="worker-recovery-daemon", help="Persistent service instance name")
    daemon_service_parser.add_argument("--owner-id", default="", help="Optional daemon owner identifier")
    daemon_service_parser.add_argument("--lease-seconds", type=int, default=300, help="Lease duration in seconds")
    daemon_service_parser.add_argument("--stale-after-seconds", type=int, default=180, help="Running workers older than this are recoverable")
    daemon_service_parser.add_argument("--total-limit", type=int, default=4, help="Max workers to recover per daemon cycle")
    daemon_service_parser.add_argument("--poll-seconds", type=float, default=5.0, help="Sleep between cycles")
    daemon_service_parser.add_argument("--max-ticks", type=int, default=0, help="Stop after N cycles; 0 means forever")

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

    restore_bundle_parser = subparsers.add_parser("restore-asset-bundle", help="Restore a previously exported asset bundle into runtime")
    restore_bundle_parser.add_argument("--manifest", required=True, help="Path to bundle_manifest.json")
    restore_bundle_parser.add_argument("--target-runtime-dir", default="", help="Optional runtime dir override")
    restore_bundle_parser.add_argument("--conflict", choices=["skip", "overwrite", "error"], default="skip", help="How to handle existing files")

    subparsers.add_parser("test-model", help="Run provider healthcheck")

    serve_parser = subparsers.add_parser("serve", help="Start the HTTP API")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)

    args = parser.parse_args()
    if args.command in {
        "export-company-snapshot-bundle",
        "export-company-handoff-bundle",
        "export-sqlite-snapshot",
        "restore-asset-bundle",
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

    orchestrator = build_orchestrator()

    if args.command == "bootstrap":
        print(json.dumps(orchestrator.bootstrap(), ensure_ascii=False, indent=2))
        return

    if args.command == "run-job":
        payload = json.loads(Path(args.file).read_text())
        print(json.dumps(orchestrator.run_job(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "plan":
        payload = json.loads(Path(args.file).read_text())
        print(json.dumps(orchestrator.plan_workflow(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "review-plan":
        payload = json.loads(Path(args.file).read_text())
        print(json.dumps(orchestrator.review_plan_session(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "show-plan-reviews":
        print(json.dumps(orchestrator.list_plan_review_sessions(args.target_company), ensure_ascii=False, indent=2))
        return

    if args.command == "start-workflow":
        payload = json.loads(Path(args.file).read_text())
        print(json.dumps(orchestrator.run_workflow_blocking(payload), ensure_ascii=False, indent=2))
        return

    if args.command == "show-job":
        result = orchestrator.get_job_results(args.job_id)
        if result is None:
            raise SystemExit(f"Job {args.job_id} not found")
        print(json.dumps(result, ensure_ascii=False, indent=2))
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

    if args.command == "show-recoverable-workers":
        print(
            json.dumps(
                orchestrator.list_recoverable_agent_workers(
                    {
                        "stale_after_seconds": args.stale_after_seconds,
                        "lane_id": args.lane_id,
                        "limit": args.limit,
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
                            "poll_seconds": args.poll_seconds,
                            "max_ticks": args.max_ticks,
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
        server = create_server(orchestrator, host=args.host, port=args.port)
        print(f"Serving on http://{args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()


if __name__ == "__main__":
    main()
