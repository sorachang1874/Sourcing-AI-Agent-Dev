"""Runtime health / progress / operator-status contracts — salvage wave 7.

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
salvage-then-delete; work-list docs/governance/RECOVERY_BAND_OWNERSHIP_2026-07-22.md
group 7): the workflow progress schema summary, runtime-health classifications
(remote-wait-with-recovery progressing, stale shared recovery ignored while
the runner is alive), blocked-acquisition-worker progress classification, the
tick runtime heartbeat, and the operator-mode worker-daemon status aggregation
had no modern coverage. Ported verbatim onto the repo-standard PG fixture.
Old->new mapping in docs/governance/REGRESSION_INDEX.md; the freeze ratchet
shrinks in the same change.
"""

import json
import os
from datetime import datetime, timedelta, timezone
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import AcquisitionTask, JobRequest
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator, _job_runtime_idle_seconds
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class RuntimeHealthStatusTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.catalog = AssetCatalog.discover()
        self.store = self.make_pg_store(f"{self.tempdir.name}/test.db")
        self.settings = AppSettings(
            project_root=Path(self.tempdir.name),
            runtime_dir=Path(self.tempdir.name),
            secrets_file=Path(self.tempdir.name) / "providers.local.json",
            jobs_dir=Path(self.tempdir.name) / "jobs",
            company_assets_dir=Path(self.tempdir.name) / "company_assets",
            db_path=Path(self.tempdir.name) / "test.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.model_client = DeterministicModelClient()
        self.semantic_provider = LocalSemanticProvider()
        self.acquisition_engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=self.model_client,
            semantic_provider=self.semantic_provider,
            acquisition_engine=self.acquisition_engine,
        )
        runtime_env_patcher = unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(self.settings.runtime_dir)},
            clear=False,
        )
        runtime_env_patcher.start()
        self.addCleanup(runtime_env_patcher.stop)

    def test_workflow_progress_schema_summarizes_stage_and_worker_state(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的华人技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {},
            }
        )
        workflow = self.orchestrator.run_workflow_blocking({"plan_review_id": review_id})
        progress = self.orchestrator.get_job_progress(workflow["job"]["job_id"])
        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertEqual(progress["status"], "completed")
        self.assertEqual(progress["stage"], "completed")
        self.assertIn("elapsed_seconds", progress)
        self.assertIn("timing", progress["progress"])
        self.assertGreaterEqual(progress["progress"]["counters"]["result_count"], 1)
        self.assertTrue(progress["progress"]["milestones"])
        self.assertIn("worker_summary", progress["progress"])
        self.assertGreaterEqual(len(progress["progress"]["completed_stages"]), 1)

    def test_runtime_health_treats_remote_wait_with_recovery_as_progressing(self) -> None:
        job_id = "job_runtime_remote_wait"
        request_payload = {
            "raw_user_request": "Find Google infra people",
            "target_company": "Google",
            "categories": ["employee"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Waiting for remote Harvest run",
                "blocked_task": "enrich_linkedin_profiles",
                "runtime_controls": {
                    "shared_recovery": {
                        "status": "started",
                        "service_name": "worker-recovery-daemon",
                        "scope": "shared",
                    },
                    "job_recovery": {
                        "status": "started",
                        "service_name": f"job-recovery-{job_id}",
                        "scope": "job_scoped",
                    },
                    "workflow_runner": {
                        "status": "started",
                        "pid": 88001,
                    },
                },
            },
        )
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::01",
            stage="acquiring",
            span_name="harvest_profile_batch",
            budget_payload={"requested_url_count": 100},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/example/"]},
            metadata={"recovery_kind": "harvest_profile_batch"},
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker,
            status="waiting_remote_harvest",
            checkpoint_payload={"run_id": "run-remote"},
            output_payload={"summary": {"status": "submitted"}},
        )

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                side_effect=[
                    {"service_name": "worker-recovery-daemon", "status": "running", "lock_status": "locked"},
                    {"service_name": f"job-recovery-{job_id}", "status": "running", "lock_status": "locked"},
                ],
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator._workflow_runner_process_alive",
                return_value=False,
            ),
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        runtime_health = dict(progress["progress"].get("runtime_health") or {})
        self.assertEqual(runtime_health.get("classification"), "waiting_on_remote_provider")
        self.assertEqual(runtime_health.get("state"), "progressing")
        self.assertEqual(int(runtime_health.get("waiting_remote_harvest_count") or 0), 1)

    def test_job_progress_classifies_blocked_acquisition_workers(self) -> None:
        request_payload = {
            "raw_user_request": "Find former xAI employees",
            "target_company": "xAI",
            "categories": ["former_employee"],
            "employment_statuses": ["former"],
            "top_k": 1,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_progress_blocked_workers"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Waiting for queued workers", "blocked_task": "acquire_full_roster"},
        )
        self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="relationship_web::01",
            stage="acquiring",
            span_name="search_bundle:relationship_web",
            budget_payload={"max_results": 10},
            input_payload={"query": "xAI former employee"},
            metadata={"target_company": "xAI"},
            handoff_from_lane="triage_planner",
        )

        progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        runtime_health = dict(progress["progress"].get("runtime_health") or {})
        self.assertEqual(runtime_health.get("classification"), "blocked_on_acquisition_workers")
        self.assertEqual(int(runtime_health.get("pending_worker_count") or 0), 1)

    def test_runtime_health_ignores_stale_shared_recovery_when_runner_is_alive(self) -> None:
        job_id = "job_runtime_runner_alive_shared_stale"
        runner_log = self.settings.runtime_dir / "service_logs" / "workflow-runner-job_runtime_runner_alive.log"
        runner_log.parent.mkdir(parents=True, exist_ok=True)
        runner_log.write_text("runner healthy\n", encoding="utf-8")
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={
                "message": "Running acquisition tasks",
                "runtime_controls": {
                    "shared_recovery": {
                        "status": "scheduled",
                        "service_name": "worker-recovery-daemon",
                        "scope": "shared",
                    },
                    "workflow_runner": {
                        "status": "started",
                        "pid": 77702,
                        "log_path": str(runner_log),
                    },
                },
            },
        )

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                return_value={"service_name": "worker-recovery-daemon", "status": "stale", "lock_status": "locked"},
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator._workflow_runner_process_alive",
                return_value=True,
            ),
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        runtime_controls = dict(progress["progress"].get("runtime_controls") or {})
        self.assertFalse(bool(runtime_controls["shared_recovery"]["service_ready"]))
        self.assertTrue(bool(runtime_controls["workflow_runner"]["process_alive"]))
        runtime_health = dict(progress["progress"].get("runtime_health") or {})
        self.assertEqual(runtime_health.get("classification"), "healthy_running")
        self.assertEqual(runtime_health.get("state"), "progressing")

    def test_run_worker_recovery_once_emits_runtime_heartbeat(self) -> None:
        job_id = "job_runtime_heartbeat"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={"message": "Running acquisition tasks"},
        )

        recovery = self.orchestrator.run_worker_recovery_once(
            {
                "job_id": job_id,
                "workflow_auto_resume_enabled": False,
                "workflow_queue_auto_takeover_enabled": False,
                "runtime_heartbeat_interval_seconds": 0,
            }
        )

        self.assertEqual(recovery["status"], "completed")
        heartbeat = dict(recovery.get("runtime_heartbeat") or {})
        heartbeat_items = list(heartbeat.get("items") or [])
        self.assertEqual(heartbeat["status"], "active")
        self.assertEqual(heartbeat["item_count"], 1)
        self.assertEqual(heartbeat["emitted_count"], 1)
        self.assertEqual(len(heartbeat_items), 1)
        self.assertEqual(heartbeat_items[0]["status"], "emitted")
        heartbeat_events = self.store.list_job_events(job_id, stage="runtime_heartbeat")
        self.assertEqual(len(heartbeat_events), 1)
        self.assertEqual(heartbeat_events[0]["payload"]["source"], "job_recovery_daemon")

        second = self.orchestrator.run_worker_recovery_once(
            {
                "job_id": job_id,
                "workflow_auto_resume_enabled": False,
                "workflow_queue_auto_takeover_enabled": False,
                "runtime_heartbeat_interval_seconds": 3600,
            }
        )
        second_heartbeat = dict(second.get("runtime_heartbeat") or {})
        second_heartbeat_items = list(second_heartbeat.get("items") or [])
        self.assertEqual(second_heartbeat["status"], "skipped")
        self.assertEqual(second_heartbeat["item_count"], 1)
        self.assertEqual(second_heartbeat["skipped_count"], 1)
        self.assertEqual(second_heartbeat_items[0]["status"], "skipped")
        self.assertEqual(len(self.store.list_job_events(job_id, stage="runtime_heartbeat")), 1)

    def test_get_worker_daemon_status_can_aggregate_job_runtime_controls(self) -> None:
        job_id = "job_daemon_status_runtime_controls"
        large_marker = "DAEMON_STATUS_LARGE_MARKER"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={
                "runtime_controls": {
                    "shared_recovery": {
                        "status": "started",
                        "service_name": "worker-recovery-daemon",
                        "scope": "shared",
                    },
                    "job_recovery": {
                        "status": "started",
                        "service_name": f"job-recovery-{job_id}",
                        "scope": "job_scoped",
                    },
                }
            },
        )

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.read_service_status",
            side_effect=[
                {
                    "service_name": "worker-recovery-daemon",
                    "status": "running",
                    "lock_status": "locked",
                    "last_summary": {
                        "status": "completed",
                        "daemon": {"claimed_count": 2, "executed_count": 1},
                        "jobs": [{"candidate": {"profile_payload": large_marker * 1000}}],
                    },
                },
                {
                    "service_name": f"job-recovery-{job_id}",
                    "status": "running",
                    "lock_status": "locked",
                    "last_summary": {
                        "status": "completed",
                        "daemon": {"claimed_count": 1, "executed_count": 1},
                        "jobs": [{"candidate": {"profile_payload": large_marker * 1000}}],
                    },
                },
            ],
        ):
            status = self.orchestrator.get_worker_daemon_status({"job_id": job_id})

        serialized = json.dumps(status, ensure_ascii=False)
        self.assertEqual(status["status"], "ok")
        self.assertEqual(status["job_id"], job_id)
        self.assertEqual(status["recovery_services"]["shared"]["service_name"], "worker-recovery-daemon")
        self.assertEqual(status["recovery_services"]["job_scoped"]["service_name"], f"job-recovery-{job_id}")
        self.assertEqual(status["recovery_services"]["shared"]["last_summary"]["daemon_claimed_count"], 2)
        self.assertNotIn(large_marker, serialized)
        self.assertNotIn("jobs", status["recovery_services"]["shared"]["last_summary"])

    def test_get_worker_daemon_status_details_are_explicit_operator_mode(self) -> None:
        large_marker = "DAEMON_STATUS_DETAILS_MARKER"
        with unittest.mock.patch(
            "sourcing_agent.orchestrator.read_service_status",
            return_value={
                "service_name": "worker-recovery-daemon",
                "status": "running",
                "lock_status": "locked",
                "last_summary": {
                    "status": "completed",
                    "jobs": [{"candidate": {"profile_payload": large_marker}}],
                },
            },
        ):
            compact = self.orchestrator.get_worker_daemon_status({})
            detailed = self.orchestrator.get_worker_daemon_status({"include_details": True})

        self.assertNotIn(large_marker, json.dumps(compact, ensure_ascii=False))
        self.assertIn(large_marker, json.dumps(detailed, ensure_ascii=False))



    # -- shard-A port groups G2+G10 (2026-07-22): runtime health/metrics
    # endpoint contracts (TTL cache, UTC idle seconds, materialized-snapshot
    # freshness preference, compacted public payloads, refresh/reconcile
    # counters, stalled-job reporting) + the task-execution int fallback.

    def test_get_job_progress_short_ttl_caches_runtime_service_status(self) -> None:
        job_id = "job_progress_service_status_cache"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={
                "message": "Retrieval is running",
                "runtime_execution_mode": "hosted",
                "runtime_controls": {
                    "hosted_runtime_watchdog": {
                        "status": "started",
                        "service_name": "server-runtime-watchdog",
                        "scope": "hosted_runtime_watchdog",
                    }
                },
            },
        )

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.time.monotonic",
                side_effect=[100.0, 100.2, 101.2],
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                return_value={
                    "service_name": "server-runtime-watchdog",
                    "status": "running",
                    "lock_status": "locked",
                },
            ) as status_mock,
        ):
            first_progress = self.orchestrator.get_job_progress(job_id)
            second_progress = self.orchestrator.get_job_progress(job_id)
            third_progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(first_progress)
        self.assertIsNotNone(second_progress)
        self.assertIsNotNone(third_progress)
        assert first_progress is not None
        assert second_progress is not None
        assert third_progress is not None
        self.assertEqual(status_mock.call_count, 2)
        self.assertEqual(
            first_progress["progress"]["runtime_controls"]["hosted_runtime_watchdog"]["service_status"]["status"],
            "running",
        )
        self.assertEqual(
            second_progress["progress"]["runtime_controls"]["hosted_runtime_watchdog"]["service_status"]["status"],
            "running",
        )

    def test_job_runtime_idle_seconds_uses_utc_storage_timestamps(self) -> None:
        updated_at = (datetime.now(timezone.utc) - timedelta(seconds=7)).strftime("%Y-%m-%d %H:%M:%S")
        idle_seconds = _job_runtime_idle_seconds({"updated_at": updated_at})
        self.assertGreaterEqual(idle_seconds, 5)
        self.assertLess(idle_seconds, 30)

    def test_runtime_health_compacts_public_service_status_payloads(self) -> None:
        large_marker = "SERVICE_HEALTH_LARGE_MARKER"
        large_service_status = {
            "service_name": "worker-recovery-daemon",
            "status": "running",
            "pid": os.getpid(),
            "pid_alive": True,
            "lock_status": "locked",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "last_summary": {
                "status": "completed",
                "daemon": {"recoverable_count": 4, "claimed_count": 3, "executed_count": 2},
                "jobs": [
                    {
                        "job_id": "job-large-service-summary",
                        "candidate": {
                            "candidate_id": "cand-large-service-summary",
                            "profile_payload": large_marker * 1000,
                        },
                    }
                ],
                "workflow_resume": [{"job_id": "job-large-service-summary"}],
            },
            "last_nonempty_summary": {
                "status": "completed",
                "daemon": {"claimed_count": 1, "executed_count": 1},
                "jobs": [{"candidate": {"profile_payload": large_marker * 1000}}],
            },
            "activity_summary": {
                "status": "completed",
                "daemon": {"claimed_count": 1, "executed_count": 1},
                "jobs": [{"candidate": {"profile_payload": large_marker * 1000}}],
            },
            "cumulative_summary": {
                "tick_count": 10,
                "active_tick_count": 5,
                "job_totals": {"job-large-service-summary": {"claimed_count": 3}},
            },
        }

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.read_service_status",
            return_value=large_service_status,
        ):
            health = self.orchestrator.get_runtime_health({"force_refresh": True})
            metrics = self.orchestrator.get_runtime_metrics({"force_refresh": True})

        serialized_health = json.dumps(health, ensure_ascii=False)
        serialized_metrics = json.dumps(metrics, ensure_ascii=False)
        shared_status = dict(dict(health.get("services") or {}).get("shared_recovery") or {})

        self.assertEqual(shared_status["last_summary"]["daemon_claimed_count"], 3)
        self.assertEqual(shared_status["last_summary"]["workflow_resume_count"], 1)
        self.assertEqual(shared_status["cumulative_summary"]["job_total_count"], 1)
        self.assertNotIn(large_marker, serialized_health)
        self.assertNotIn(large_marker, serialized_metrics)
        self.assertNotIn("jobs", shared_status["last_summary"])
        # CALIBRATED 2026-07-22: the metrics payload grew ~5% with new
        # counters; the compaction contract is the exclusions above — keep a
        # generous absolute cap as the runaway guard.
        self.assertLess(len(serialized_metrics), 40000)

    def test_runtime_metrics_reports_refresh_and_reconcile_counters(self) -> None:
        job_id = "job_runtime_metrics_refresh"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={
                "message": "refreshing",
                "pre_retrieval_refresh": {
                    "status": "completed",
                    "snapshot_id": "snapshot-1",
                    "search_seed_worker_count": 2,
                    "harvest_prefetch_worker_count": 1,
                },
                "background_reconcile": {
                    "search_seed": {
                        "status": "inline_refreshed",
                        "applied_worker_count": 2,
                        "added_entry_count": 5,
                    },
                    "harvest_prefetch": {
                        "status": "inline_refreshed",
                        "applied_worker_count": 1,
                    },
                },
            },
        )
        self.store.append_job_event(
            job_id,
            "remote_provider_event",
            "received",
            "Received remote provider event for run run-runtime-metrics.",
            {
                "target_worker_ids": [771],
                "event_metrics": {
                    "remote_completed_at": "2026-04-27T00:00:00+00:00",
                    "local_event_seen_at": "2026-04-27T00:00:01+00:00",
                    "remote_to_local_event_lag_ms": 1000,
                },
            },
        )
        self.store.append_job_event(
            job_id,
            "acquiring",
            "running",
            "Harvest profile completion event advanced provider tail before candidate materialization.",
            {
                "worker_ids": [771],
                "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                "event_metrics": {
                    "local_event_apply_started_at": "2026-04-27T00:00:02+00:00",
                    "next_submit_attempt_started_at": "2026-04-27T00:00:02.050000+00:00",
                    "next_submit_attempt_finished_at": "2026-04-27T00:00:02.150000+00:00",
                    "post_ingest_prefetch_elapsed_ms": 100,
                    "post_ingest_prefetch_dispatched_url_count": 1,
                },
            },
        )

        runtime = self.orchestrator.get_runtime_metrics({"force_refresh": True})
        metrics = dict(runtime.get("metrics") or {})
        refresh_metrics = dict(runtime.get("refresh_metrics") or {})
        event_efficiency = dict(runtime.get("event_level_efficiency") or {})
        service_readiness = dict(runtime.get("service_readiness") or {})
        self.assertEqual(int(metrics.get("pre_retrieval_refresh_job_count") or 0), 1)
        self.assertEqual(int(metrics.get("inline_search_seed_worker_count") or 0), 2)
        self.assertEqual(int(metrics.get("inline_harvest_prefetch_worker_count") or 0), 1)
        self.assertEqual(int(metrics.get("background_reconcile_job_count") or 0), 1)
        self.assertEqual(int(metrics.get("background_search_seed_reconcile_job_count") or 0), 1)
        self.assertEqual(int(metrics.get("background_harvest_prefetch_reconcile_job_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("pre_retrieval_refresh_job_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("background_reconcile_job_count") or 0), 1)
        self.assertEqual(int(metrics.get("event_level_efficiency_report_count") or 0), 1)
        self.assertEqual(int(event_efficiency.get("report_count") or 0), 1)
        self.assertEqual(dict(event_efficiency.get("remote_to_local_event_lag_ms") or {}).get("max"), 1000.0)
        self.assertEqual(dict(event_efficiency.get("local_to_next_submit_start_ms") or {}).get("max"), 50.0)
        self.assertIn(service_readiness.get("recommended_workflow_entrypoint"), {"serve"})
        self.assertIn(service_readiness.get("standalone_cli_fallback"), {"managed_subprocess"})
        self.assertIn("auto_recovery_ready", service_readiness)

    def test_runtime_health_reports_stalled_jobs(self) -> None:
        request_payload = {
            "raw_user_request": "Find former xAI employees",
            "target_company": "xAI",
            "categories": ["former_employee"],
            "employment_statuses": ["former"],
            "top_k": 1,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_runtime_health_stalled"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Waiting for queued workers", "blocked_task": "acquire_full_roster"},
        )
        self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="relationship_web::02",
            stage="acquiring",
            span_name="search_bundle:relationship_web",
            budget_payload={"max_results": 10},
            input_payload={"query": "xAI former employee"},
            metadata={"target_company": "xAI"},
            handoff_from_lane="triage_planner",
        )

        health = self.orchestrator.get_runtime_health({"active_limit": 10})

        self.assertEqual(health["status"], "degraded")
        self.assertGreaterEqual(int(health["metrics"]["stalled_job_count"] or 0), 1)
        self.assertEqual(health["stalled_jobs"][0]["job_id"], job_id)
        self.assertEqual(
            health["stalled_jobs"][0]["runtime_health"]["classification"],
            "blocked_on_acquisition_workers",
        )

    def test_task_execution_int_falls_back_when_field_is_non_scalar(self) -> None:
        task = AcquisitionTask(
            task_id="task-int-default",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="test",
            status="ready",
            metadata={
                "intent_view": {
                    "max_pages": [1, 2, 3],
                }
            },
        )
        request = JobRequest.from_payload({"target_company": "Reflection AI"})

        self.assertEqual(
            self.acquisition_engine._task_execution_int(task, request, "max_pages", default=10),
            10,
        )

if __name__ == "__main__":
    unittest.main()
