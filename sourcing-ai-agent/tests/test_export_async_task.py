"""C1.4b end-to-end: projection export as a durable async task.

submit (enqueue, 202) -> worker drain builds the archive off the request thread
-> poll (succeeded + artifact handle) -> download (bytes + X-Sourcing-* headers),
plus idempotent replay. Only the actual archive bytes (the pre-existing
_build_projection_candidates_archive_payload) are mocked; the orchestration under
test — enqueue, drain claim+run, atomic artifact publish, status projection, and
disk-backed download — is real.
"""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

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
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin


class ExportAsyncTaskTest(PGDurableRuntimeTestMixin, unittest.TestCase):
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
            self.catalog, self.settings, self.store, self.model_client
        )
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=self.settings.jobs_dir,
            model_client=self.model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=self.acquisition_engine,
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def test_projection_export_submit_drain_poll_download(self) -> None:
        canned_archive = {
            "status": "ok",
            "filename": "projection-export.zip",
            "content_type": "application/zip",
            "body": b"REAL-ZIP-BYTES-xyz",
            "projection_id": "proj-async-1",
            "record_count": 5,
            "exported_record_count": 4,
            "skipped_assertion_count": 1,
        }
        with mock.patch.object(
            self.orchestrator,
            "_build_projection_candidates_archive_payload",
            return_value=canned_archive,
        ):
            # 1) submit -> 202-shaped queued envelope; the request thread does NOT build.
            submitted = self.orchestrator.export_projection_candidates_archive(
                {"projection_id": "proj-async-1"}
            )
            self.assertEqual(submitted.get("status"), "queued")
            self.assertEqual(submitted.get("task_type"), "export.projection.generate")
            task_id = str(submitted.get("task_id") or "")
            self.assertTrue(task_id)
            before = self.orchestrator.get_export_command_status(task_id)
            self.assertIn(before.get("status"), {"queued", "running"})
            self.assertIsNone(before.get("artifact"))

            # 2) the worker drain claims + builds it.
            drained = self.orchestrator._drain_export_projection_generate_commands({})
            self.assertGreaterEqual(int(drained.get("command_count") or 0), 1)
            self.assertEqual(int(drained.get("completed_count") or 0), 1)

        # 3) poll -> succeeded + artifact handle (headers projected from the result).
        polled = self.orchestrator.get_export_command_status(task_id)
        self.assertEqual(polled.get("status"), "succeeded")
        artifact = polled.get("artifact") or {}
        self.assertEqual(artifact.get("handle"), f"/api/exports/{task_id}/artifact")
        self.assertEqual(artifact.get("content_type"), "application/zip")
        self.assertEqual(artifact.get("headers", {}).get("X-Sourcing-Export-Record-Count"), "5")
        self.assertEqual(artifact.get("headers", {}).get("X-Sourcing-Projection-Id"), "proj-async-1")

        # 4) download -> the artifact bytes (read back from disk) + count fields.
        downloaded = self.orchestrator.get_export_command_artifact(task_id)
        self.assertEqual(downloaded.get("status"), "ok")
        self.assertEqual(downloaded.get("body"), b"REAL-ZIP-BYTES-xyz")
        self.assertEqual(downloaded.get("content_type"), "application/zip")
        self.assertEqual(int(downloaded.get("record_count") or 0), 5)
        self.assertEqual(int(downloaded.get("exported_record_count") or 0), 4)
        self.assertEqual(int(downloaded.get("skipped_assertion_count") or 0), 1)

        # 5) idempotent replay: a second identical submit short-circuits to the same
        # succeeded task + artifact handle (no rebuild needed).
        replay = self.orchestrator.export_projection_candidates_archive(
            {"projection_id": "proj-async-1"}
        )
        self.assertEqual(replay.get("status"), "succeeded")
        self.assertEqual((replay.get("artifact") or {}).get("handle"), f"/api/exports/{task_id}/artifact")

    def test_export_artifact_not_ready_before_drain(self) -> None:
        with mock.patch.object(
            self.orchestrator,
            "_build_projection_candidates_archive_payload",
            return_value={"status": "ok", "body": b"x", "filename": "f.zip", "projection_id": "p"},
        ):
            submitted = self.orchestrator.export_projection_candidates_archive(
                {"projection_id": "proj-not-ready"}
            )
            task_id = str(submitted.get("task_id") or "")
        # Downloading before the worker has run -> not_ready (the API maps this to 409).
        artifact = self.orchestrator.get_export_command_artifact(task_id)
        self.assertEqual(artifact.get("status"), "not_ready")
        # Unknown command -> not_found.
        self.assertEqual(
            self.orchestrator.get_export_command_status("cmd-does-not-exist").get("status"),
            "not_found",
        )


if __name__ == "__main__":
    unittest.main()
