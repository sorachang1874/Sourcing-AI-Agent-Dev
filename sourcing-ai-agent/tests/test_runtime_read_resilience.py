"""PG-era runtime resilience contracts — salvaged from the frozen test_pipeline.py.

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
disposition = salvage-then-delete), forensic wave: the two originals injected
sqlite3 errors into paths that are PG-pure today. The deferred runtime-control
retry salvage exposed a REAL product defect — the retry loop still caught only
sqlite3.OperationalError("database is locked"), so transient Postgres failures
were never retried; the repair centralizes transient classification in
storage.is_transient_control_plane_error and this file pins that contract from
both sides (transient retries, permanent gives up). The public-read fallback
contract (persisted workers when the runtime read raises) is provider-agnostic
and ported with a PG-flavored error. Old->new mapping in
docs/governance/REGRESSION_INDEX.md; freeze ratchet shrinks in the same change.
"""

import tempfile
import unittest
import unittest.mock
from pathlib import Path

import psycopg

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
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
from sourcing_agent.storage import is_transient_control_plane_error
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


def _wrapped_transient_write_failure() -> RuntimeError:
    # Mirrors storage._raise_control_plane_postgres_write_failure: RuntimeError
    # with the psycopg error as __cause__.
    error = RuntimeError("Postgres authoritative write failed for jobs via save_job: write")
    error.__cause__ = psycopg.OperationalError("server closed the connection unexpectedly")
    return error


class TransientClassifierTest(unittest.TestCase):
    def test_classifier_owns_transient_semantics(self) -> None:
        self.assertTrue(is_transient_control_plane_error(_wrapped_transient_write_failure()))
        self.assertTrue(is_transient_control_plane_error(psycopg.OperationalError("connection reset")))
        self.assertFalse(is_transient_control_plane_error(RuntimeError("unique constraint violated")))
        self.assertFalse(is_transient_control_plane_error(None))


class RuntimeReadResilienceTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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

    def test_persist_workflow_runtime_controls_deferred_retries_transient_pg_failure(self) -> None:
        # Ported from test_pipeline.py::test_persist_workflow_runtime_controls_
        # deferred_retries_locked_database; calibrated 2026-07-22 to the PG-pure
        # store surface (wrapped psycopg.OperationalError instead of the dead
        # sqlite3 "database is locked" branch).
        job_id = "job_runtime_control_deferred_retry"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={"message": "queued"},
        )

        original = self.orchestrator._persist_workflow_runtime_controls
        call_count = {"count": 0}

        def flaky(job_id_value: str, controls_value: dict[str, object]) -> None:
            call_count["count"] += 1
            if call_count["count"] == 1:
                raise _wrapped_transient_write_failure()
            original(job_id_value, controls_value)

        with unittest.mock.patch.object(
            self.orchestrator,
            "_persist_workflow_runtime_controls",
            side_effect=flaky,
        ):
            result = self.orchestrator._persist_workflow_runtime_controls_deferred(
                job_id,
                {
                    "workflow_runner_control": {
                        "status": "started",
                        "handshake": {"status": "advanced", "job_status": "running", "job_stage": "planning"},
                    }
                },
                max_attempts=3,
                initial_delay_seconds=0.0,
                run_async=False,
            )

        self.assertEqual(result["status"], "completed")
        self.assertGreaterEqual(call_count["count"], 2)
        stored_job = self.store.get_job(job_id)
        self.assertIsNotNone(stored_job)
        assert stored_job is not None
        runtime_controls = dict(dict(stored_job.get("summary") or {}).get("runtime_controls") or {})
        self.assertEqual(runtime_controls["workflow_runner_control"]["status"], "started")

    def test_persist_workflow_runtime_controls_deferred_gives_up_on_permanent_failure(self) -> None:
        # New regression coverage for the classifier seam: a non-transient
        # failure must NOT burn retry attempts.
        job_id = "job_runtime_control_permanent_failure"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={"message": "queued"},
        )
        call_count = {"count": 0}

        def permanent(job_id_value: str, controls_value: dict[str, object]) -> None:
            call_count["count"] += 1
            raise RuntimeError("Postgres authoritative write failed for jobs via save_job: constraint")

        with unittest.mock.patch.object(
            self.orchestrator,
            "_persist_workflow_runtime_controls",
            side_effect=permanent,
        ):
            result = self.orchestrator._persist_workflow_runtime_controls_deferred(
                job_id,
                {"workflow_runner_control": {"status": "started"}},
                max_attempts=3,
                initial_delay_seconds=0.0,
                run_async=False,
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(call_count["count"], 1)
        stored_job = self.store.get_job(job_id)
        assert stored_job is not None
        self.assertNotIn("runtime_controls", dict(stored_job.get("summary") or {}))

    def test_public_worker_read_endpoints_use_persisted_workers_when_runtime_read_fails(self) -> None:
        # Ported from test_pipeline.py::test_public_worker_read_endpoints_use_
        # persisted_workers_when_runtime_closed; calibrated 2026-07-22 — the
        # production fallback catches any runtime-read failure, so the injected
        # error is the PG-era wrapped shape rather than sqlite3.ProgrammingError.
        self.orchestrator.bootstrap()
        result = self.orchestrator.run_job(
            {
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施"],
                "top_k": 2,
            }
        )
        first_result = self.store.get_job_results(result["job_id"])[0]
        candidate_id = str(first_result.get("candidate_id") or "")
        persisted_workers = [
            {
                "worker_id": 987,
                "job_id": result["job_id"],
                "lane_id": "retrieval_specialist",
                "worker_key": "persisted-worker",
                "status": "completed",
                "checkpoint": {},
                "metadata": {},
            }
        ]
        with (
            unittest.mock.patch.object(
                self.orchestrator.store,
                "list_agent_workers",
                return_value=persisted_workers,
            ),
            unittest.mock.patch.object(
                self.orchestrator.agent_runtime,
                "list_workers",
                side_effect=_wrapped_transient_write_failure(),
            ),
        ):
            trace = self.orchestrator.get_job_trace(result["job_id"])
            workers_payload = self.orchestrator.get_job_workers(result["job_id"])
            scheduler_payload = self.orchestrator.get_job_scheduler(result["job_id"])
            candidate_detail = self.orchestrator.get_job_candidate_detail(result["job_id"], candidate_id)
            candidate_batch = self.orchestrator.get_job_candidate_details_batch(result["job_id"], [candidate_id])

        self.assertIsNotNone(trace)
        self.assertEqual(trace["agent_workers"], persisted_workers)
        self.assertEqual(workers_payload["agent_workers"], persisted_workers)
        self.assertEqual(scheduler_payload["scheduler"]["lane_summary"][0]["completed"], 1)
        self.assertEqual(scheduler_payload["scheduler"]["resumable_workers"][0]["worker_id"], 987)
        self.assertIsNotNone(candidate_detail)
        self.assertEqual(candidate_detail["candidate"]["candidate_id"], candidate_id)
        self.assertEqual(candidate_batch["candidates"][0]["candidate_id"], candidate_id)


if __name__ == "__main__":
    unittest.main()
