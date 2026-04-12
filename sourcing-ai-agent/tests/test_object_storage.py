import json
from pathlib import Path
import threading
import tempfile
import unittest
import unittest.mock
from urllib.parse import parse_qs, urlsplit

from sourcing_agent.asset_sync import AssetBundleManager
from sourcing_agent.object_storage import ObjectStorageConfig, build_object_storage_client


class _FakeResponse:
    def __init__(self, status_code: int, *, content: bytes = b"", text: str = "", headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.content = content
        self.text = text or content.decode("utf-8", errors="ignore")
        self.headers = dict(headers or {})


class _MultipartFakeSession:
    def __init__(self, *, fail_part_number: int = 0) -> None:
        self.fail_part_number = fail_part_number
        self.lock = threading.Lock()
        self.part_numbers: list[int] = []
        self.part_sizes: list[int] = []
        self.abort_count = 0
        self.complete_count = 0
        self.initiate_count = 0
        self.uploads: dict[str, dict[int, str]] = {}

    def post(self, url: str, data=None, headers=None, timeout=None):
        query = parse_qs(urlsplit(url).query, keep_blank_values=True)
        if "uploads" in query:
            with self.lock:
                self.initiate_count += 1
                self.uploads.setdefault("upload-123", {})
            return _FakeResponse(
                200,
                content=(
                    b"<?xml version='1.0' encoding='UTF-8'?>"
                    b"<InitiateMultipartUploadResult><UploadId>upload-123</UploadId></InitiateMultipartUploadResult>"
                ),
            )
        if "uploadId" in query:
            with self.lock:
                self.complete_count += 1
            return _FakeResponse(200, content=b"<CompleteMultipartUploadResult />")
        raise AssertionError(f"Unexpected POST url: {url}")

    def put(self, url: str, data=None, headers=None, timeout=None):
        query = parse_qs(urlsplit(url).query, keep_blank_values=True)
        part_number = int(query["partNumber"][0])
        upload_id = query["uploadId"][0]
        payload = bytes(data or b"")
        with self.lock:
            self.part_numbers.append(part_number)
            self.part_sizes.append(len(payload))
        if part_number == self.fail_part_number:
            return _FakeResponse(500, text="part failed")
        with self.lock:
            self.uploads.setdefault(upload_id, {})[part_number] = f'"etag-{part_number}"'
        return _FakeResponse(200, headers={"ETag": f'"etag-{part_number}"'})

    def get(self, url: str, headers=None, timeout=None):
        query = parse_qs(urlsplit(url).query, keep_blank_values=True)
        if "uploadId" in query:
            upload_id = query["uploadId"][0]
            if upload_id not in self.uploads:
                return _FakeResponse(404, content=b"<Error><Code>NoSuchUpload</Code></Error>")
            parts_xml = "".join(
                f"<Part><PartNumber>{part_number}</PartNumber><ETag>{etag}</ETag></Part>"
                for part_number, etag in sorted(self.uploads[upload_id].items())
            )
            return _FakeResponse(
                200,
                content=(
                    b"<?xml version='1.0' encoding='UTF-8'?>"
                    + f"<ListPartsResult><IsTruncated>false</IsTruncated>{parts_xml}</ListPartsResult>".encode("utf-8")
                ),
            )
        raise AssertionError(f"Unexpected GET url: {url}")

    def delete(self, url: str, headers=None, timeout=None):
        query = parse_qs(urlsplit(url).query, keep_blank_values=True)
        if "uploadId" in query:
            upload_id = query["uploadId"][0]
            with self.lock:
                self.abort_count += 1
                self.uploads.pop(upload_id, None)
            return _FakeResponse(204)
        raise AssertionError(f"Unexpected DELETE url: {url}")


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

    def test_upload_bundle_skips_per_file_exists_scan_for_fresh_remote_bundle(self) -> None:
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

        with unittest.mock.patch.object(self.client, "has_object", wraps=self.client.has_object) as has_object_mock:
            upload = self.bundle_manager.upload_bundle(export["manifest_path"], self.client, max_workers=2, resume=True)

        self.assertEqual(upload["status"], "uploaded")
        self.assertFalse(bool(upload.get("resume_scan_required")))
        self.assertEqual(has_object_mock.call_count, 1)
        progress_path = Path(upload["progress_path"])
        self.assertTrue(progress_path.exists())
        progress_payload = json.loads(progress_path.read_text())
        self.assertEqual(progress_payload["status"], "uploaded")
        self.assertEqual(progress_payload["completion_ratio"], 1.0)

    def test_upload_bundle_archive_mode_tar_downloads_archive_and_restore_extracts_payload(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        for index in range(140):
            (snapshot_dir / f"candidate_{index:03d}.json").write_text(json.dumps({"index": index}))
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
        upload = self.bundle_manager.upload_bundle(export["manifest_path"], self.client, archive_mode="tar")

        self.assertEqual(upload["status"], "uploaded")
        self.assertEqual(upload["transfer_mode"], "archive")
        self.assertEqual(upload["effective_archive_mode"], "tar")
        self.assertEqual(upload["uploaded_file_count"], 3)
        self.assertEqual(upload["archive"]["format"], "tar")
        remote_bundle_dir = (
            self.object_store_dir
            / "sourcing-ai-agent-dev-test"
            / "bundles"
            / "company_snapshot"
            / upload["bundle_id"]
        )
        self.assertTrue((remote_bundle_dir / "payload.tar").exists())
        self.assertFalse((remote_bundle_dir / "payload").exists())

        download_dir = self.project_root / "downloaded-archive"
        download = self.bundle_manager.download_bundle(
            bundle_kind=upload["bundle_kind"],
            bundle_id=upload["bundle_id"],
            client=self.client,
            output_dir=download_dir,
        )

        self.assertEqual(download["status"], "downloaded")
        self.assertEqual(download["transfer_mode"], "archive")
        self.assertEqual(download["requested_payload_file_count"], 1)
        downloaded_bundle_dir = Path(download["manifest_path"]).parent
        self.assertTrue((downloaded_bundle_dir / "payload.tar").exists())
        self.assertFalse((downloaded_bundle_dir / "payload" / "company_assets" / "acme" / "20260406T120000" / "candidate_000.json").exists())

        restored_runtime_dir = self.project_root / "restored-runtime"
        restore = self.bundle_manager.restore_bundle(download["manifest_path"], target_runtime_dir=restored_runtime_dir)

        self.assertEqual(restore["status"], "restored")
        self.assertEqual(restore["archive_restore"]["status"], "extracted")
        self.assertTrue((restored_runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_000.json").exists())

    def test_delete_bundle_removes_remote_archive_and_prunes_indexes(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        for index in range(140):
            (snapshot_dir / f"candidate_{index:03d}.json").write_text(json.dumps({"index": index}))
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
        upload = self.bundle_manager.upload_bundle(export["manifest_path"], self.client, archive_mode="tar")
        remote_bundle_dir = (
            self.object_store_dir
            / "sourcing-ai-agent-dev-test"
            / "bundles"
            / "company_snapshot"
            / upload["bundle_id"]
        )
        self.assertTrue((remote_bundle_dir / "bundle_manifest.json").exists())
        self.assertTrue((remote_bundle_dir / "payload.tar").exists())

        delete_summary = self.bundle_manager.delete_bundle(
            bundle_kind=upload["bundle_kind"],
            bundle_id=upload["bundle_id"],
            client=self.client,
        )

        self.assertEqual(delete_summary["status"], "deleted")
        self.assertEqual(delete_summary["deleted_object_count"], 3)
        self.assertFalse((remote_bundle_dir / "bundle_manifest.json").exists())
        self.assertFalse((remote_bundle_dir / "payload.tar").exists())
        local_index = json.loads((self.runtime_dir / "object_sync" / "bundle_index.json").read_text())
        self.assertEqual(local_index["bundles"], [])
        remote_index = json.loads(
            (
                self.object_store_dir
                / "sourcing-ai-agent-dev-test"
                / "indexes"
                / "bundle_index.json"
            ).read_text()
        )
        self.assertEqual(remote_index["bundles"], [])

    def test_upload_bundle_auto_archive_mode_uses_tar_for_large_bundle(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        for index in range(160):
            (snapshot_dir / f"candidate_{index:03d}.json").write_text(json.dumps({"index": index}))
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
        self.assertEqual(upload["requested_archive_mode"], "auto")
        self.assertEqual(upload["effective_archive_mode"], "tar")
        self.assertEqual(upload["transfer_mode"], "archive")
        self.assertEqual(upload["uploaded_file_count"], 3)

    def test_restore_sqlite_snapshot_can_materialize_archive_bundle(self) -> None:
        sqlite_path = self.runtime_dir / "sourcing_agent.db"
        sqlite_path.write_bytes(b"old-db")
        export = self.bundle_manager.export_sqlite_snapshot()
        upload = self.bundle_manager.upload_bundle(export["manifest_path"], self.client, archive_mode="tar")
        download = self.bundle_manager.download_bundle(
            bundle_kind=upload["bundle_kind"],
            bundle_id=upload["bundle_id"],
            client=self.client,
            output_dir=self.project_root / "downloaded-sqlite-archive",
        )

        sqlite_path.write_bytes(b"new-db")
        restore = self.bundle_manager.restore_sqlite_snapshot(download["manifest_path"])

        self.assertEqual(restore["status"], "sqlite_restored")
        self.assertEqual(sqlite_path.read_bytes(), b"old-db")

    def test_restore_sqlite_snapshot_creates_backup(self) -> None:
        sqlite_path = self.runtime_dir / "sourcing_agent.db"
        sqlite_path.write_bytes(b"old-db")
        export = self.bundle_manager.export_sqlite_snapshot()
        sqlite_path.write_bytes(b"new-db")
        restore = self.bundle_manager.restore_sqlite_snapshot(export["manifest_path"])
        self.assertEqual(restore["status"], "sqlite_restored")
        self.assertTrue(Path(restore["backup_path"]).exists())
        self.assertEqual(sqlite_path.read_bytes(), b"old-db")

    def test_s3_compatible_upload_file_uses_multipart_for_large_files(self) -> None:
        source = self.project_root / "multipart.bin"
        source.write_bytes(b"a" * (11 * 1024 * 1024))
        client = build_object_storage_client(
            ObjectStorageConfig(
                enabled=True,
                provider="s3_compatible",
                bucket="bucket",
                prefix="prefix",
                endpoint_url="https://storage.example.com",
                region="auto",
                access_key_id="ak",
                secret_access_key="sk",
                force_path_style=True,
                multipart_threshold_bytes=5 * 1024 * 1024,
                multipart_chunk_size_bytes=5 * 1024 * 1024,
                multipart_max_workers=2,
            )
        )
        fake_session = _MultipartFakeSession()

        with unittest.mock.patch.object(client, "_session", return_value=fake_session):
            result = client.upload_file(source, "multipart/test.bin", content_type="application/octet-stream")

        self.assertEqual(result["status"], "uploaded")
        self.assertTrue(bool(result.get("multipart")))
        self.assertEqual(int(result.get("part_count") or 0), 3)
        self.assertEqual(sorted(fake_session.part_numbers), [1, 2, 3])
        self.assertEqual(sorted(fake_session.part_sizes), [1 * 1024 * 1024, 5 * 1024 * 1024, 5 * 1024 * 1024])
        self.assertEqual(fake_session.complete_count, 1)
        self.assertEqual(fake_session.abort_count, 0)
        self.assertEqual(fake_session.initiate_count, 1)

    def test_s3_compatible_multipart_upload_resumes_missing_parts_after_failure(self) -> None:
        source = self.project_root / "multipart-fail.bin"
        source.write_bytes(b"b" * (11 * 1024 * 1024))
        client = build_object_storage_client(
            ObjectStorageConfig(
                enabled=True,
                provider="s3_compatible",
                bucket="bucket",
                prefix="prefix",
                endpoint_url="https://storage.example.com",
                region="auto",
                access_key_id="ak",
                secret_access_key="sk",
                force_path_style=True,
                multipart_threshold_bytes=5 * 1024 * 1024,
                multipart_chunk_size_bytes=5 * 1024 * 1024,
                multipart_max_workers=2,
            )
        )
        fake_session = _MultipartFakeSession(fail_part_number=2)

        with unittest.mock.patch.object(client, "_session", return_value=fake_session):
            with self.assertRaises(Exception):
                client.upload_file(source, "multipart/fail.bin", content_type="application/octet-stream")
            self.assertEqual(fake_session.initiate_count, 1)
            checkpoint_files = list(source.parent.glob(f".{source.name}.*.multipart.json"))
            self.assertEqual(len(checkpoint_files), 1)
            checkpoint_path = checkpoint_files[0]
            self.assertTrue(checkpoint_path.exists())
            self.assertIn(1, fake_session.uploads["upload-123"])
            self.assertNotIn(2, fake_session.uploads["upload-123"])
            fake_session.fail_part_number = 0
            resumed = client.upload_file(source, "multipart/fail.bin", content_type="application/octet-stream")
            self.assertEqual(resumed["status"], "uploaded")
            self.assertTrue(bool(resumed.get("resumed")))
            self.assertGreaterEqual(int(resumed.get("resumed_part_count") or 0), 1)
            self.assertLess(int(resumed.get("resumed_part_count") or 0), int(resumed.get("part_count") or 0))
            self.assertEqual(fake_session.initiate_count, 1)
            self.assertEqual(fake_session.complete_count, 1)
            self.assertEqual(fake_session.abort_count, 0)
            self.assertFalse(checkpoint_path.exists())


if __name__ == "__main__":
    unittest.main()
