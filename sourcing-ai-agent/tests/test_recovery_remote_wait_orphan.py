"""End-to-end regression for the orphaned remote-wait recovery gap (2026-07-20).

Production incident: two full-roster jobs blocked in ``acquiring`` with
"Resume after worker recovery completes remote dataset fetch". Their segmented
``harvest_company_employees`` shard runs finished on Apify, but the in-process
long-poll watchers died with the serve process and no provider webhook was
configured, so no terminal event was ever recorded. The worker recovery
daemon deliberately skips submitted remote-wait workers without a terminal
event marker (the event-owner contract), so nothing ever re-polled the remote
runs: shard queue summaries stayed ``queued``, the roster baseline never became
ready, and ``workflow_resume`` never fired.

Fix: ``PersistentWorkerRecoveryDaemon`` now admits a bounded number of
*orphaned* remote-wait workers per tick (updated_at older than
``remote_wait_orphan_seconds``) through the normal claim/resume path, which
performs exactly one provider status poll per claim and downloads the dataset
when the run is terminal.

These tests pin the whole loop at tick level with a real orchestrator and a
PG-backed store:

1. blocked acquiring job + aged (orphaned) shard worker whose remote run is
   terminal -> recovery tick -> dataset collected (queue summary becomes
   ``completed`` with dataset items) -> readiness flips -> job resumes.
2. the same setup with a FRESH worker -> the event-owner contract is
   preserved: no eager re-poll, no collection, job keeps waiting.

The provider effect is faked at ``execute_with_checkpoint`` (terminal completed
result); everything else — worker rows, shard queue summary files, readiness,
resume dispatch — is real.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest import mock

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import JobRequest
from sourcing_agent.harvest_connectors import HarvestExecutionArtifact, HarvestExecutionResult
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
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
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin, psycopg


class RemoteWaitOrphanRecoveryTickTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "runtime.db")
        self.catalog = AssetCatalog.discover()
        self.settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "providers.local.json",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            db_path=self.runtime_dir / "runtime.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.model_client = DeterministicModelClient()
        self.acquisition_engine = AcquisitionEngine(
            self.catalog,
            self.settings,
            self.store,
            self.model_client,
        )
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=self.settings.jobs_dir,
            model_client=self.model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=self.acquisition_engine,
        )
        self.identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        self.request = JobRequest(
            raw_user_request="Find Anthropic US people",
            target_company="Anthropic",
            categories=["employee"],
            employment_statuses=["current"],
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    # -- fixtures ---------------------------------------------------------------

    def _shard_id(self) -> str:
        return "us_engineering"

    def _snapshot_dir(self) -> Path:
        return self.settings.company_assets_dir / "anthropic" / "snapshot-orphan-recollect"

    def _shard_snapshot_dir(self) -> Path:
        return self._snapshot_dir() / "harvest_company_employees" / "shards" / self._shard_id()

    def _shard_harvest_dir(self) -> Path:
        return self._shard_snapshot_dir() / "harvest_company_employees"

    def _queue_summary_path(self) -> Path:
        return self._shard_harvest_dir() / "harvest_company_employees_queue_summary.json"

    def _dataset_items_path(self) -> Path:
        return self._shard_harvest_dir() / "harvest_company_employees_queue_dataset_items.json"

    def _roster_body(self) -> list[dict[str, Any]]:
        return [
            {
                "fullName": "Ada Engineer",
                "headline": "Member of Technical Staff",
                "location": "San Francisco, California, United States",
                "linkedinUrl": "https://www.linkedin.com/in/ada-engineer/",
            }
        ]

    def _seed_segmented_shard_files(self) -> None:
        shard_harvest_dir = self._shard_harvest_dir()
        shard_harvest_dir.mkdir(parents=True, exist_ok=True)
        plan_path = self._snapshot_dir() / "harvest_company_employees" / "adaptive_shard_plan.json"
        plan_path.parent.mkdir(parents=True, exist_ok=True)
        plan_path.write_text(
            json.dumps(
                {
                    "shards": [
                        {
                            "strategy_id": "us_primary_function_split",
                            "shard_id": self._shard_id(),
                            "title": "United States / Engineering",
                            "max_pages": 1,
                            "page_limit": 25,
                            "company_filters": {"locations": ["United States"], "function_ids": ["8"]},
                        }
                    ]
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        # Production state: the shard run is submitted remotely, the local queue
        # summary is still "queued" and no dataset items were downloaded.
        self._queue_summary_path().write_text(
            json.dumps(
                {
                    "logical_name": "harvest_company_employees",
                    "company_identity": self.identity.to_record(),
                    "status": "queued",
                    "run_id": "run-shard-orphan",
                    "dataset_id": "dataset-shard-orphan",
                    "artifact_paths": {},
                    "requested_pages": 1,
                    "requested_item_limit": 25,
                    "company_filters": {"locations": ["United States"], "function_ids": ["8"]},
                    "snapshot_dir": str(self._shard_snapshot_dir()),
                    "root_snapshot_dir": str(self._snapshot_dir()),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def _seed_blocked_job(self, job_id: str) -> None:
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=self.request.to_record(),
            plan_payload={},
            summary_payload={
                "blocked_task": "acquire_full_roster",
                "message": (
                    "Roster acquisition queued background segmented Harvest company-employees runs; "
                    "former seed discovery started in parallel. Resume after worker recovery completes "
                    "remote dataset fetch."
                ),
                "runtime_controls": {
                    # The serve process that owned the runner is dead: the job
                    # must take the in-process recovery resume path.
                    "workflow_runner": {
                        "status": "started",
                        "pid": 999999,
                        "process_alive": False,
                        "job_id": job_id,
                    },
                    "workflow_runner_control": {
                        "status": "started",
                        "handshake": {
                            "status": "advanced",
                            "job_status": "blocked",
                            "job_stage": "acquiring",
                        },
                    },
                },
                "acquisition_progress": {
                    "status": "running",
                    "latest_state": {
                        "snapshot_dir": str(self._snapshot_dir()),
                        "company_identity": self.identity.to_record(),
                    },
                    "tasks": {},
                },
            },
        )

    def _seed_shard_worker(self, job_id: str) -> int:
        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload={},
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key=f"harvest_company_employees::anthropic::{self._shard_id()}",
            stage="acquiring",
            span_name=f"harvest_company_employees:Anthropic:{self._shard_id()}",
            budget_payload={"max_pages": 1, "page_limit": 25},
            input_payload={"company_identity": self.identity.to_record()},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": self.identity.to_record(),
                "snapshot_dir": str(self._shard_snapshot_dir()),
                "root_snapshot_dir": str(self._snapshot_dir()),
                "max_pages": 1,
                "page_limit": 25,
                "company_filters": {"locations": ["United States"], "function_ids": ["8"]},
                "worker_key_suffix": f"::{self._shard_id()}",
                "request_payload": self.request.to_record(),
                "plan_payload": {},
                "runtime_mode": "workflow",
                "allow_shared_provider_cache": True,
            },
            handoff_from_lane="triage_planner",
        )
        self.store.complete_agent_worker(
            handle.worker_id,
            status="queued",
            checkpoint_payload={
                "stage": "waiting_remote_harvest",
                "run_id": "run-shard-orphan",
                "dataset_id": "dataset-shard-orphan",
                "recovery_kind": "harvest_company_employees",
                "summary_path": str(self._queue_summary_path()),
                "artifact_paths": {},
            },
            output_payload={
                "summary": {
                    "status": "queued",
                    "run_id": "run-shard-orphan",
                    "dataset_id": "dataset-shard-orphan",
                    "summary_path": str(self._queue_summary_path()),
                }
            },
        )
        return int(handle.worker_id)

    def _age_worker_updated_at(self, worker_id: int, *, age_seconds: int) -> None:
        fixture = self._pg_durable_runtime_fixture
        assert fixture is not None and psycopg is not None
        aged = (datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).strftime("%Y-%m-%d %H:%M:%S")
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        with psycopg.connect(fixture.dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {quoted_schema}.agent_worker_runs SET updated_at = %s WHERE worker_id = %s",
                    (aged, int(worker_id)),
                )

    def _terminal_execute_with_checkpoint(self, calls: list[dict[str, Any]]):
        def _fake(connector_self, identity, snapshot_dir, **kwargs):
            calls.append(
                {
                    "company_key": identity.company_key,
                    "snapshot_dir": str(snapshot_dir),
                    "checkpoint_run_id": str(dict(kwargs.get("checkpoint") or {}).get("run_id") or ""),
                }
            )
            body = self._roster_body()
            return HarvestExecutionResult(
                logical_name="harvest_company_employees",
                checkpoint={"run_id": "run-shard-orphan", "dataset_id": "dataset-shard-orphan", "status": "completed"},
                body=body,
                message="Harvest actor run run-shard-orphan completed and dataset dataset-shard-orphan was cached.",
                artifacts=[
                    HarvestExecutionArtifact(
                        label="dataset_items",
                        payload=body,
                        metadata={
                            "logical_name": "harvest_company_employees",
                            "run_id": "run-shard-orphan",
                            "dataset_id": "dataset-shard-orphan",
                        },
                    )
                ],
            )

        return _fake

    def _workflow_resume_entry(self, result: dict[str, Any], job_id: str) -> dict[str, Any]:
        for item in list(result.get("workflow_resume") or []):
            if str(dict(item or {}).get("job_id") or "") == job_id:
                return dict(item)
        return {}

    # -- tests ------------------------------------------------------------------

    def test_orphaned_terminal_shard_run_is_collected_and_blocked_job_resumes(self) -> None:
        job_id = "job_orphan_shard_recollect"
        self._seed_segmented_shard_files()
        self._seed_blocked_job(job_id)
        worker_id = self._seed_shard_worker(job_id)
        self._age_worker_updated_at(worker_id, age_seconds=3600)

        connector_calls: list[dict[str, Any]] = []
        with (
            mock.patch.object(
                type(self.acquisition_engine.harvest_company_connector),
                "execute_with_checkpoint",
                autospec=True,
                side_effect=self._terminal_execute_with_checkpoint(connector_calls),
            ),
            # Pin the in-process resume path: hosted dispatch runs a detached
            # runner thread that bypasses the resume mock; the readiness gate
            # under test is identical on both dispatch modes.
            mock.patch.object(self.orchestrator, "_workflow_prefers_hosted_execution", return_value=False),
            mock.patch.object(
                self.orchestrator,
                "_run_workflow_from_acquisition",
                return_value={"status": "completed"},
            ) as resume_mock,
        ):
            # Tick 1: the orphaned shard worker is admitted and collected. The
            # resume phase deliberately yields while same-tick durable work is
            # observed (worker recovery, then the local-apply/board-visible
            # chain the collection unblocks), so the resume belongs to a later
            # quiescent tick.
            first_tick = self.orchestrator.run_worker_recovery_once(
                {"job_id": job_id, "remote_wait_orphan_seconds": 900, "remote_wait_orphan_limit": 4}
            )
            daemon = dict(first_tick.get("daemon") or {})
            self.assertEqual(daemon.get("remote_wait_orphan_admitted_worker_ids"), [worker_id])
            self.assertEqual(int(daemon.get("claimed_count") or 0), 1)
            resume_mock.assert_not_called()

            # Subsequent ticks drain the deferred durable chain; once a tick
            # observes no new durable work, the resume phase re-evaluates
            # readiness and resumes the blocked job.
            last_tick: dict[str, Any] = {}
            for _ in range(8):
                if resume_mock.called:
                    break
                last_tick = self.orchestrator.run_worker_recovery_once(
                    {"job_id": job_id, "remote_wait_orphan_seconds": 900, "remote_wait_orphan_limit": 4}
                )

        # The orphan re-poll resumed the queued shard worker against the SAME
        # remote run id (no resubmission) and collected the terminal dataset.
        self.assertEqual(len(connector_calls), 1)
        self.assertEqual(connector_calls[0]["checkpoint_run_id"], "run-shard-orphan")
        worker = self.store.get_agent_worker(worker_id=worker_id) or {}
        self.assertEqual(str(worker.get("status") or ""), "completed")

        queue_summary = json.loads(self._queue_summary_path().read_text(encoding="utf-8"))
        self.assertEqual(str(queue_summary.get("status") or ""), "completed")
        self.assertTrue(self._dataset_items_path().exists())
        dataset_items = json.loads(self._dataset_items_path().read_text(encoding="utf-8"))
        self.assertEqual(len(dataset_items), 1)

        # Readiness flipped and the blocked job resumed.
        resume_mock.assert_called_once()
        resume_entry = self._workflow_resume_entry(last_tick, job_id)
        self.assertEqual(str(resume_entry.get("status") or ""), "resumed")
        self.assertTrue(bool(resume_entry.get("baseline_ready")))

    def test_fresh_remote_wait_shard_worker_keeps_event_owner_contract(self) -> None:
        job_id = "job_fresh_shard_not_orphaned"
        self._seed_segmented_shard_files()
        self._seed_blocked_job(job_id)
        worker_id = self._seed_shard_worker(job_id)

        connector_calls: list[dict[str, Any]] = []
        with (
            mock.patch.object(
                type(self.acquisition_engine.harvest_company_connector),
                "execute_with_checkpoint",
                autospec=True,
                side_effect=self._terminal_execute_with_checkpoint(connector_calls),
            ),
            mock.patch.object(self.orchestrator, "_workflow_prefers_hosted_execution", return_value=False),
            mock.patch.object(
                self.orchestrator,
                "_run_workflow_from_acquisition",
                return_value={"status": "completed"},
            ) as resume_mock,
        ):
            first_tick = self.orchestrator.run_worker_recovery_once(
                {"job_id": job_id, "remote_wait_orphan_seconds": 900, "remote_wait_orphan_limit": 4}
            )
            second_tick = self.orchestrator.run_worker_recovery_once(
                {"job_id": job_id, "remote_wait_orphan_seconds": 900, "remote_wait_orphan_limit": 4}
            )

        # Across two ticks the fresh remote-wait worker is never re-polled: the
        # terminal event remains webhook/watcher owned, so without an event the
        # worker stays queued and the job stays blocked.
        for tick in (first_tick, second_tick):
            daemon = dict(tick.get("daemon") or {})
            self.assertEqual(int(daemon.get("remote_wait_orphan_admitted_count") or 0), 0)
            self.assertEqual(daemon.get("remote_wait_skipped_worker_ids"), [worker_id])
        self.assertEqual(connector_calls, [])
        resume_mock.assert_not_called()

        worker = self.store.get_agent_worker(worker_id=worker_id) or {}
        self.assertEqual(str(worker.get("status") or ""), "queued")
        queue_summary = json.loads(self._queue_summary_path().read_text(encoding="utf-8"))
        self.assertEqual(str(queue_summary.get("status") or ""), "queued")
        self.assertFalse(self._dataset_items_path().exists())
        self.assertEqual(self._workflow_resume_entry(first_tick, job_id), {})
        self.assertEqual(self._workflow_resume_entry(second_tick, job_id), {})
