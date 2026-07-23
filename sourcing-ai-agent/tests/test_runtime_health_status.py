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
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import JobRequest
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
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


if __name__ == "__main__":
    unittest.main()
