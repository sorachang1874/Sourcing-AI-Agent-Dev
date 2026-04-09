import json
from pathlib import Path
import tempfile
import unittest

from sourcing_agent.asset_sync import AssetBundleManager
from sourcing_agent.object_storage import ObjectStorageConfig, build_object_storage_client


class ObjectStorageSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.tempdir.name) / "project"
        self.runtime_dir = self.project_root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.object_store_dir = Path(self.tempdir.name) / "object_store"
        self.bundle_manager = AssetBundleManager(self.project_root, self.runtime_dir)
        self.client = build_object_storage_client(
            ObjectStorageConfig(
                enabled=True,
                provider="filesystem",
                local_dir=str(self.object_store_dir),
                prefix="sourcing-ai-agent-dev-test",
            )
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_upload_and_download_bundle_via_filesystem_object_storage(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "manifest.json").write_text(json.dumps({"snapshot_id": "20260406T120000"}))
        (snapshot_dir / "candidate_documents.json").write_text(json.dumps({"count": 2}))
        latest_path = self.runtime_dir / "company_assets" / "acme" / "latest_snapshot.json"
        latest_path.write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T120000",
                    "snapshot_dir": str(snapshot_dir),
                    "company_identity": {"canonical_name": "Acme", "aliases": ["acme"]},
                }
            )
        )
        export = self.bundle_manager.export_company_snapshot_bundle("Acme")
        upload = self.bundle_manager.upload_bundle(export["manifest_path"], self.client, max_workers=4)
        self.assertEqual(upload["status"], "uploaded")
        self.assertEqual(upload["uploaded_file_count"], 5)
        self.assertEqual(upload["max_workers"], 4)
        local_index_path = self.runtime_dir / "object_sync" / "bundle_index.json"
        self.assertTrue(local_index_path.exists())
        local_index = json.loads(local_index_path.read_text())
        self.assertEqual(local_index["bundles"][0]["bundle_id"], upload["bundle_id"])
        local_runs = sorted((self.runtime_dir / "object_sync" / "runs").glob("*.json"))
        self.assertEqual(len(local_runs), 1)
        remote_index_path = self.object_store_dir / "sourcing-ai-agent-dev-test" / "indexes" / "bundle_index.json"
        self.assertTrue(remote_index_path.exists())
        remote_index = json.loads(remote_index_path.read_text())
        self.assertEqual(remote_index["bundles"][0]["bundle_id"], upload["bundle_id"])
        remote_runs = sorted((self.object_store_dir / "sourcing-ai-agent-dev-test" / "indexes" / "sync_runs").glob("*.json"))
        self.assertEqual(len(remote_runs), 1)

        download_dir = self.project_root / "downloaded"
        download = self.bundle_manager.download_bundle(
            bundle_kind=upload["bundle_kind"],
            bundle_id=upload["bundle_id"],
            client=self.client,
            output_dir=download_dir,
            max_workers=3,
        )
        self.assertEqual(download["status"], "downloaded")
        self.assertEqual(download["max_workers"], 3)
        manifest_path = Path(download["manifest_path"])
        self.assertTrue(manifest_path.exists())
        manifest = json.loads(manifest_path.read_text())
        relpaths = {entry["runtime_relative_path"] for entry in manifest["files"]}
        self.assertIn("company_assets/acme/20260406T120000/manifest.json", relpaths)
        remote_runs = sorted((self.object_store_dir / "sourcing-ai-agent-dev-test" / "indexes" / "sync_runs").glob("*.json"))
        self.assertEqual(len(remote_runs), 2)

    def test_upload_and_download_resume_only_transfer_missing_or_changed_payloads(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "manifest.json").write_text(json.dumps({"snapshot_id": "20260406T120000"}))
        (snapshot_dir / "candidate_documents.json").write_text(json.dumps({"count": 2}))
        latest_path = self.runtime_dir / "company_assets" / "acme" / "latest_snapshot.json"
        latest_path.write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T120000",
                    "snapshot_dir": str(snapshot_dir),
                    "company_identity": {"canonical_name": "Acme", "aliases": ["acme"]},
                }
            )
        )
        export = self.bundle_manager.export_company_snapshot_bundle("Acme")
        first_upload = self.bundle_manager.upload_bundle(export["manifest_path"], self.client, max_workers=4)
        self.assertEqual(first_upload["uploaded_file_count"], 5)
        self.assertEqual(first_upload["progress"]["completion_ratio"], 1.0)

        remote_bundle_dir = (
            self.object_store_dir
            / "sourcing-ai-agent-dev-test"
            / "bundles"
            / "company_snapshot"
            / first_upload["bundle_id"]
        )
        remote_payload = remote_bundle_dir / "payload" / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json"
        remote_payload.unlink()

        resumed_upload = self.bundle_manager.upload_bundle(export["manifest_path"], self.client, max_workers=2)
        self.assertEqual(resumed_upload["provider"], "filesystem")
        self.assertEqual(resumed_upload["uploaded_file_count"], 1)
        self.assertEqual(resumed_upload["skipped_existing_file_count"], 4)
        self.assertEqual(resumed_upload["progress"]["completed_file_count"], 5)
        self.assertEqual(resumed_upload["progress"]["remaining_file_count"], 0)
        self.assertEqual(resumed_upload["progress"]["completion_ratio"], 1.0)

        download_dir = self.project_root / "downloaded"
        first_download = self.bundle_manager.download_bundle(
            bundle_kind=first_upload["bundle_kind"],
            bundle_id=first_upload["bundle_id"],
            client=self.client,
            output_dir=download_dir,
            max_workers=3,
        )
        self.assertEqual(first_download["requested_payload_file_count"], 3)
        self.assertEqual(first_download["progress"]["transferred_file_count"], 3)
        self.assertEqual(first_download["progress"]["skipped_file_count"], 0)

        download_bundle_dir = Path(first_download["manifest_path"]).parent
        local_payload = download_bundle_dir / "payload" / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json"
        original_bytes = local_payload.read_bytes()
        local_payload.write_bytes(b"x" * len(original_bytes))

        resumed_download = self.bundle_manager.download_bundle(
            bundle_kind=first_upload["bundle_kind"],
            bundle_id=first_upload["bundle_id"],
            client=self.client,
            output_dir=download_dir,
            max_workers=2,
        )
        self.assertEqual(resumed_download["downloaded_file_count"], 3)
        self.assertEqual(resumed_download["skipped_existing_file_count"], 2)
        self.assertEqual(resumed_download["progress"]["transferred_file_count"], 1)
        self.assertEqual(resumed_download["progress"]["skipped_file_count"], 2)
        self.assertEqual(resumed_download["progress"]["remaining_file_count"], 0)
        self.assertEqual(local_payload.read_bytes(), original_bytes)

    def test_restore_sqlite_snapshot_creates_backup(self) -> None:
        sqlite_path = self.runtime_dir / "sourcing_agent.db"
        sqlite_path.write_bytes(b"old-db")
        export = self.bundle_manager.export_sqlite_snapshot()
        sqlite_path.write_bytes(b"new-db")
        restore = self.bundle_manager.restore_sqlite_snapshot(export["manifest_path"])
        self.assertEqual(restore["status"], "sqlite_restored")
        self.assertTrue(Path(restore["backup_path"]).exists())
        self.assertEqual(sqlite_path.read_bytes(), b"old-db")


if __name__ == "__main__":
    unittest.main()
