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
        upload = self.bundle_manager.upload_bundle(export["manifest_path"], self.client)
        self.assertEqual(upload["status"], "uploaded")
        self.assertEqual(upload["uploaded_file_count"], 5)

        download_dir = self.project_root / "downloaded"
        download = self.bundle_manager.download_bundle(
            bundle_kind=upload["bundle_kind"],
            bundle_id=upload["bundle_id"],
            client=self.client,
            output_dir=download_dir,
        )
        self.assertEqual(download["status"], "downloaded")
        manifest_path = Path(download["manifest_path"])
        self.assertTrue(manifest_path.exists())
        manifest = json.loads(manifest_path.read_text())
        relpaths = {entry["runtime_relative_path"] for entry in manifest["files"]}
        self.assertIn("company_assets/acme/20260406T120000/manifest.json", relpaths)

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
