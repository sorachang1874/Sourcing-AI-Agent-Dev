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

    def test_crm_public_web_export_submit_drain_poll_download(self) -> None:
        """C1.4 CRM mirror: the CRM export rides the SAME generic async-task endpoints
        (submit -> 202, GET /api/exports/{id} poll, GET .../artifact download) but
        carries the CRM 7-header set, proving the command-type header decoupling. The
        record-id validation + the archive build are mocked; the orchestration
        (enqueue -> CRM drain -> owner _run -> poll -> disk-backed download) is real."""
        owner = self.orchestrator._crm_public_web_owner
        canned_crm_archive = {
            "status": "ok",
            "filename": "crm-public-web-export.zip",
            "content_type": "application/zip",
            "body": b"CRM-REAL-ZIP-BYTES",
            "record_count": 5,
            "exported_record_count": 4,
            "exported_signal_count": 9,
            "no_public_web_result_count": 1,
            "no_exportable_signal_count": 2,
            "non_terminal_run_count": 0,
        }
        with mock.patch.object(owner, "_prepare_crm_public_web_record_ids", return_value=["rec-1"]), \
                mock.patch.object(owner, "_export_crm_public_web_archive_from_owner", return_value=canned_crm_archive):
            submitted = self.orchestrator.export_crm_record_public_web_archive(
                {"workspace_id": "ws-1", "crm_record_ids": ["rec-1"]}
            )
            self.assertEqual(submitted.get("status"), "queued")
            self.assertEqual(submitted.get("task_type"), "export.crm_public_web.generate")
            task_id = str(submitted.get("task_id") or "")
            self.assertTrue(task_id)
            drained = self.orchestrator._drain_export_crm_public_web_generate_commands({})
            self.assertGreaterEqual(int(drained.get("command_count") or 0), 1)
            self.assertEqual(int(drained.get("completed_count") or 0), 1)

        # Poll via the SHARED endpoint -> succeeded + CRM-specific headers.
        polled = self.orchestrator.get_export_command_status(task_id)
        self.assertEqual(polled.get("status"), "succeeded")
        crm_headers = (polled.get("artifact") or {}).get("headers", {})
        self.assertEqual(crm_headers.get("X-Sourcing-Exported-Signal-Count"), "9")
        self.assertEqual(crm_headers.get("X-Sourcing-No-Public-Web-Result-Count"), "1")
        self.assertEqual(crm_headers.get("X-Sourcing-Canonical-Public-Web-Owner"), "crm_records")
        # No projection-only header leaks into the CRM artifact handle.
        self.assertNotIn("X-Sourcing-Projection-Id", crm_headers)

        # Download via the SHARED endpoint -> CRM bytes + CRM headers dict.
        downloaded = self.orchestrator.get_export_command_artifact(task_id)
        self.assertEqual(downloaded.get("status"), "ok")
        self.assertEqual(downloaded.get("body"), b"CRM-REAL-ZIP-BYTES")
        self.assertEqual(
            downloaded.get("headers", {}).get("X-Sourcing-Canonical-Public-Web-Owner"), "crm_records"
        )
        self.assertEqual(downloaded.get("headers", {}).get("X-Sourcing-Exported-Signal-Count"), "9")

    def _seed_succeeded_crm_export(self, owner, *, workspace_id: str) -> str:
        canned = {
            "status": "ok",
            "filename": "crm-public-web-export.zip",
            "content_type": "application/zip",
            "body": b"CRM-REAL-ZIP-BYTES",
            "record_count": 1,
            "exported_record_count": 1,
            "exported_signal_count": 2,
            "no_public_web_result_count": 0,
            "no_exportable_signal_count": 0,
            "non_terminal_run_count": 0,
        }
        with mock.patch.object(owner, "_prepare_crm_public_web_record_ids", return_value=["rec-1"]), \
                mock.patch.object(owner, "_export_crm_public_web_archive_from_owner", return_value=canned):
            submitted = self.orchestrator.export_crm_record_public_web_archive(
                {"workspace_id": workspace_id, "crm_record_ids": ["rec-1"]}
            )
            task_id = str(submitted.get("task_id") or "")
            self.orchestrator._drain_export_crm_public_web_generate_commands({})
        return task_id

    def test_crm_export_download_fails_closed_on_stale_watermark(self) -> None:
        """Codex critical fix: the shared CRM download reroutes through the owner's
        _run, which rechecks the input watermark before serving a succeeded artifact.
        When the stored watermark no longer matches (underlying CRM data changed), the
        download fails closed with JSON — it never streams the stale ZIP."""
        owner = self.orchestrator._crm_public_web_owner
        task_id = self._seed_succeeded_crm_export(owner, workspace_id="ws-stale")
        self.assertTrue(task_id)
        # Sanity: a fresh (non-stale) download serves the bytes.
        fresh = self.orchestrator.get_export_command_artifact(task_id)
        self.assertEqual(fresh.get("status"), "ok")
        self.assertEqual(fresh.get("body"), b"CRM-REAL-ZIP-BYTES")
        # Now the watermark goes stale -> the contract recheck fails -> fail-closed.
        with mock.patch.object(
            owner,
            "_crm_public_web_export_command_contract_failure",
            return_value={"reason": "crm_public_web_export_input_watermark_stale"},
        ):
            stale = self.orchestrator.get_export_command_artifact(task_id)
        self.assertNotEqual(stale.get("status"), "ok")
        self.assertFalse(stale.get("body"))  # absent/empty — never the stale ZIP
        self.assertTrue(dict(stale.get("read_contract") or {}).get("fail_closed"))

    def test_crm_export_drain_reclaims_expired_claimed_command(self) -> None:
        """Codex high fix: the CRM owner _run claim passes reclaim_claimed=True so the
        drain reclaims an expired-lease 'claimed' command (a worker that crashed between
        claim and mark_running) instead of stranding it (selected by the drain's list
        but unclaimable). Mirrors the projection expired-claimed regression."""
        owner = self.orchestrator._crm_public_web_owner
        canned = {
            "status": "ok",
            "filename": "crm-public-web-export.zip",
            "content_type": "application/zip",
            "body": b"CRM-REAL-ZIP-BYTES",
            "record_count": 1,
        }
        with mock.patch.object(owner, "_prepare_crm_public_web_record_ids", return_value=["rec-1"]), \
                mock.patch.object(owner, "_export_crm_public_web_archive_from_owner", return_value=canned):
            submitted = self.orchestrator.export_crm_record_public_web_archive(
                {"workspace_id": "ws-reclaim", "crm_record_ids": ["rec-1"]}
            )
            task_id = str(submitted.get("task_id") or "")
            self.assertTrue(task_id)
            # A worker claims it then dies before mark_running -> stranded 'claimed'.
            self.store.claim_workflow_command(task_id, lease_owner="dead-worker", lease_seconds=300)
            self.store._control_plane_postgres.execute_non_query(  # noqa: SLF001
                "UPDATE workflow_commands SET lease_expires_at = %s WHERE command_id = %s",
                ("2000-01-01T00:00:00+00:00", task_id),
            )
            # The drain reclaims it (the owner _run claim now passes reclaim_claimed=True).
            drained = self.orchestrator._drain_export_crm_public_web_generate_commands({})
        self.assertGreaterEqual(int(drained.get("command_count") or 0), 1)
        self.assertEqual(int(drained.get("completed_count") or 0), 1)
        self.assertNotEqual(
            str(self.store.get_workflow_command(task_id).get("lease_owner") or ""),
            "dead-worker",
        )


if __name__ == "__main__":
    unittest.main()
