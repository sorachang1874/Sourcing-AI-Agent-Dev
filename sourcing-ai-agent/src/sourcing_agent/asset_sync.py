from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import tarfile
import time
from typing import Any, Iterable

from .object_storage import ObjectStorageClient, ObjectStorageNotFoundError


class AssetBundleError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BundleFile:
    runtime_relative_path: str
    payload_relative_path: str
    size_bytes: int
    sha256: str


def _utc_now_token() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_key(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def _safe_token(value: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "_" for ch in value.strip())
    compact = "_".join(part for part in normalized.split("_") if part)
    return compact or "bundle"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class AssetBundleManager:
    def __init__(self, project_root: str | Path, runtime_dir: str | Path) -> None:
        self.project_root = Path(project_root)
        self.runtime_dir = Path(runtime_dir)
        self.exports_dir = self.runtime_dir / "asset_exports"
        self.sync_dir = self.runtime_dir / "object_sync"
        self.sync_runs_dir = self.sync_dir / "runs"
        self.sync_bundle_index_path = self.sync_dir / "bundle_index.json"

    def export_company_snapshot_bundle(
        self,
        company: str,
        *,
        snapshot_id: str = "",
        output_dir: str | Path | None = None,
    ) -> dict:
        company_key, company_dir = self._resolve_company_dir(company)
        resolved_snapshot_id = snapshot_id.strip() or self._latest_snapshot_id(company_dir)
        snapshot_dir = company_dir / resolved_snapshot_id
        if not snapshot_dir.exists():
            raise AssetBundleError(f"Snapshot {resolved_snapshot_id} not found for company {company}")
        relpaths = self._collect_files(snapshot_dir.relative_to(self.runtime_dir))
        latest_pointer = company_dir / "latest_snapshot.json"
        if latest_pointer.exists():
            relpaths.append(latest_pointer.relative_to(self.runtime_dir))
        company_registry = company_dir / "asset_registry.json"
        if company_registry.exists():
            relpaths.append(company_registry.relative_to(self.runtime_dir))
        metadata = {
            "company": company,
            "company_key": company_key,
            "snapshot_id": resolved_snapshot_id,
            "bundle_scope": "company_snapshot",
        }
        return self._export_bundle(
            bundle_kind="company_snapshot",
            bundle_key=f"{company_key}_{resolved_snapshot_id}",
            relpaths=relpaths,
            metadata=metadata,
            output_dir=output_dir,
        )

    def export_company_handoff_bundle(
        self,
        company: str,
        *,
        output_dir: str | Path | None = None,
        include_sqlite: bool = True,
        include_live_tests: bool = True,
        include_manual_review: bool = True,
        include_jobs: bool = True,
    ) -> dict:
        company_key, company_dir = self._resolve_company_dir(company)
        aliases = self._company_aliases(company_dir, company_key, company)
        relpaths: list[Path] = []
        relpaths.extend(self._collect_files(company_dir.relative_to(self.runtime_dir)))
        if include_manual_review:
            manual_dir = self.runtime_dir / "manual_review_assets" / company_key
            if manual_dir.exists():
                relpaths.extend(self._collect_files(manual_dir.relative_to(self.runtime_dir)))
        if include_live_tests:
            live_tests_dir = self.runtime_dir / "live_tests"
            if live_tests_dir.exists():
                for path in live_tests_dir.rglob("*"):
                    if not path.is_file():
                        continue
                    normalized_name = _normalize_key(path.as_posix())
                    if any(alias in normalized_name for alias in aliases):
                        relpaths.append(path.relative_to(self.runtime_dir))
        if include_jobs:
            jobs_dir = self.runtime_dir / "jobs"
            if jobs_dir.exists():
                relpaths.extend(self._matching_job_files(jobs_dir, aliases))
        if include_sqlite:
            sqlite_path = self.runtime_dir / "sourcing_agent.db"
            if sqlite_path.exists():
                relpaths.append(sqlite_path.relative_to(self.runtime_dir))
        metadata = {
            "company": company,
            "company_key": company_key,
            "bundle_scope": "company_handoff",
            "include_sqlite": include_sqlite,
            "include_live_tests": include_live_tests,
            "include_manual_review": include_manual_review,
            "include_jobs": include_jobs,
            "latest_snapshot_id": self._latest_snapshot_id(company_dir),
            "aliases": sorted(aliases),
        }
        return self._export_bundle(
            bundle_kind="company_handoff",
            bundle_key=f"{company_key}_{self._latest_snapshot_id(company_dir)}",
            relpaths=relpaths,
            metadata=metadata,
            output_dir=output_dir,
        )

    def export_sqlite_snapshot(
        self,
        *,
        output_dir: str | Path | None = None,
    ) -> dict:
        sqlite_path = self.runtime_dir / "sourcing_agent.db"
        if not sqlite_path.exists():
            raise AssetBundleError("SQLite database does not exist")
        return self._export_bundle(
            bundle_kind="sqlite_snapshot",
            bundle_key="sourcing_agent_db",
            relpaths=[sqlite_path.relative_to(self.runtime_dir)],
            metadata={"bundle_scope": "sqlite_snapshot"},
            output_dir=output_dir,
        )

    def restore_bundle(
        self,
        manifest_path: str | Path,
        *,
        target_runtime_dir: str | Path | None = None,
        conflict: str = "skip",
    ) -> dict:
        manifest_file = Path(manifest_path)
        if not manifest_file.exists():
            raise AssetBundleError(f"Manifest not found: {manifest_file}")
        payload = json.loads(manifest_file.read_text())
        if conflict not in {"skip", "overwrite", "error"}:
            raise AssetBundleError(f"Unsupported conflict mode: {conflict}")
        bundle_root = manifest_file.parent
        archive_restore = self._ensure_bundle_payload_materialized(bundle_root, payload)
        runtime_dir = Path(target_runtime_dir) if target_runtime_dir else self.runtime_dir
        restored = 0
        skipped = 0
        total_bytes = 0
        restored_files: list[str] = []
        for record in payload.get("files", []):
            source = bundle_root / record["payload_relative_path"]
            destination = runtime_dir / record["runtime_relative_path"]
            if not source.exists():
                raise AssetBundleError(f"Bundle payload missing: {source}")
            if destination.exists():
                if conflict == "skip":
                    skipped += 1
                    continue
                if conflict == "error":
                    raise AssetBundleError(f"Restore target already exists: {destination}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            restored += 1
            total_bytes += int(record.get("size_bytes", destination.stat().st_size))
            restored_files.append(record["runtime_relative_path"])
        summary = {
            "status": "restored",
            "bundle_kind": payload.get("bundle_kind", ""),
            "bundle_id": payload.get("bundle_id", ""),
            "target_runtime_dir": str(runtime_dir),
            "conflict_mode": conflict,
            "restored_file_count": restored,
            "skipped_file_count": skipped,
            "restored_total_bytes": total_bytes,
            "restored_files": restored_files,
            "archive_restore": archive_restore,
        }
        restore_summary_path = bundle_root / "restore_summary.json"
        restore_summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        return summary

    def restore_sqlite_snapshot(
        self,
        manifest_path: str | Path,
        *,
        target_db_path: str | Path | None = None,
        backup_current: bool = True,
        backup_dir: str | Path | None = None,
    ) -> dict:
        manifest_file = Path(manifest_path)
        if not manifest_file.exists():
            raise AssetBundleError(f"Manifest not found: {manifest_file}")
        payload = json.loads(manifest_file.read_text())
        db_entry = next((entry for entry in payload.get("files", []) if entry.get("runtime_relative_path") == "sourcing_agent.db"), None)
        if db_entry is None:
            raise AssetBundleError("Bundle does not contain sourcing_agent.db")
        source = manifest_file.parent / db_entry["payload_relative_path"]
        if not source.exists():
            self._ensure_bundle_payload_materialized(manifest_file.parent, payload)
        if not source.exists():
            raise AssetBundleError(f"SQLite payload missing: {source}")
        destination = Path(target_db_path) if target_db_path else (self.runtime_dir / "sourcing_agent.db")
        destination.parent.mkdir(parents=True, exist_ok=True)
        backup_path = None
        if destination.exists() and backup_current:
            resolved_backup_dir = Path(backup_dir) if backup_dir else (self.runtime_dir / "sqlite_backups")
            resolved_backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = resolved_backup_dir / f"{destination.stem}_{_utc_now_token()}{destination.suffix}"
            shutil.copy2(destination, backup_path)
        shutil.copy2(source, destination)
        summary = {
            "status": "sqlite_restored",
            "bundle_id": payload.get("bundle_id", ""),
            "source_path": str(source),
            "target_db_path": str(destination),
            "backup_current": backup_current,
            "backup_path": str(backup_path) if backup_path else "",
            "size_bytes": destination.stat().st_size,
        }
        summary_path = manifest_file.parent / "sqlite_restore_summary.json"
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        return summary

    def upload_bundle(
        self,
        manifest_path: str | Path,
        client: ObjectStorageClient,
        *,
        max_workers: int | None = None,
        resume: bool = True,
        archive_mode: str = "auto",
    ) -> dict:
        manifest_file = Path(manifest_path)
        if not manifest_file.exists():
            raise AssetBundleError(f"Manifest not found: {manifest_file}")
        payload = json.loads(manifest_file.read_text())
        bundle_kind = str(payload.get("bundle_kind", "")).strip()
        bundle_id = str(payload.get("bundle_id", "")).strip()
        if not bundle_kind or not bundle_id:
            raise AssetBundleError("Bundle manifest missing bundle_kind or bundle_id")
        bundle_root = manifest_file.parent
        logical_payload_file_count = len(list(payload.get("files") or []))
        archive_mode_resolved = self._resolve_archive_mode(payload, archive_mode)
        archive_entry = self._prepare_bundle_archive(bundle_root, payload, archive_mode_resolved)
        payload = self._persist_bundle_archive_metadata(bundle_root, payload, archive_entry)
        remote_prefix = self._bundle_remote_prefix(bundle_kind, bundle_id)
        remote_manifest_key = f"{remote_prefix}/bundle_manifest.json"
        upload_paths = [bundle_root / "bundle_manifest.json", bundle_root / "export_summary.json"]
        if archive_entry:
            upload_paths.append(bundle_root / str(archive_entry.get("relative_path") or archive_entry.get("filename") or "payload.tar"))
        else:
            for record in payload.get("files", []):
                upload_paths.append(bundle_root / record["payload_relative_path"])
        resolved_max_workers = self._resolve_max_workers(client, max_workers)
        transfers = [
            {"path": path, "relative_path": path.relative_to(bundle_root).as_posix()}
            for path in upload_paths
            if path.exists() and path.is_file()
        ]
        pending_transfers: list[dict[str, Any]] = []
        skipped_existing: list[dict[str, Any]] = []
        resume_scan_required = bool(resume and client.has_object(remote_manifest_key))
        if resume_scan_required:
            for item in transfers:
                object_key = f"{remote_prefix}/{item['relative_path']}"
                path = Path(item["path"])
                if client.has_object(object_key):
                    skipped_existing.append(
                        {
                            "relative_path": item["relative_path"],
                            "object_key": object_key,
                            "object_url": client.object_url(object_key),
                            "size_bytes": path.stat().st_size,
                            "status": "skipped_existing",
                        }
                    )
                    continue
                pending_transfers.append(item)
        else:
            pending_transfers = list(transfers)
        progress_path = bundle_root / "upload_progress.json"
        progress_state = self._build_transfer_progress_state(
            status="running",
            bundle_kind=bundle_kind,
            bundle_id=bundle_id,
            remote_prefix=remote_prefix,
            requested_file_count=len(transfers),
            skipped_existing=skipped_existing,
        )
        progress_state["transfer_mode"] = "archive" if archive_entry else "files"
        progress_state["logical_payload_file_count"] = logical_payload_file_count
        if archive_entry:
            progress_state["archive"] = dict(archive_entry)
        self._write_progress_snapshot(progress_path, progress_state)

        def _on_upload_result(result: dict[str, Any]) -> None:
            self._advance_transfer_progress(
                progress_state,
                requested_file_count=len(transfers),
                result=result,
            )
            self._write_progress_snapshot(progress_path, progress_state)

        uploads = self._parallel_transfer(
            pending_transfers,
            max_workers=resolved_max_workers,
            op=lambda item: self._upload_transfer_item(client, remote_prefix, item),
            on_result=_on_upload_result,
        )
        total_bytes = sum(int(item.get("size_bytes", 0)) for item in uploads)
        skipped_total_bytes = sum(int(item.get("size_bytes", 0)) for item in skipped_existing)
        summary = {
            "status": "uploaded",
            "bundle_kind": bundle_kind,
            "bundle_id": bundle_id,
            "remote_prefix": remote_prefix,
            "remote_manifest_key": f"{remote_prefix}/bundle_manifest.json",
            "requested_file_count": len(transfers),
            "logical_payload_file_count": logical_payload_file_count,
            "uploaded_file_count": len(uploads),
            "uploaded_total_bytes": total_bytes,
            "skipped_existing_file_count": len(skipped_existing),
            "skipped_existing_total_bytes": skipped_total_bytes,
            "provider": uploads[0]["provider"] if uploads else _provider_name_for_client(client),
            "max_workers": resolved_max_workers,
            "resume_mode": "skip_existing" if resume else "disabled",
            "resume_scan_required": resume_scan_required,
            "transfer_mode": "archive" if archive_entry else "files",
            "requested_archive_mode": str(archive_mode or "auto").strip().lower() or "auto",
            "effective_archive_mode": str(archive_entry.get("format") or "") if archive_entry else "none",
            "archive": dict(archive_entry or {}),
            "progress_path": str(progress_path),
            "progress": _build_progress_summary(
                requested_file_count=len(transfers),
                transferred_file_count=len(uploads),
                transferred_total_bytes=total_bytes,
                skipped_file_count=len(skipped_existing),
                skipped_total_bytes=skipped_total_bytes,
            ),
            "object_urls_sample": [item.get("object_url", "") for item in uploads[:5]],
            "skipped_existing_paths_sample": [item["relative_path"] for item in skipped_existing[:5]],
        }
        self._write_progress_snapshot(
            progress_path,
            {
                **progress_state,
                "status": "uploaded",
                "transferred_file_count": len(uploads),
                "transferred_total_bytes": total_bytes,
                "completed_file_count": len(uploads) + len(skipped_existing),
                "remaining_file_count": 0,
                "completion_ratio": 1.0 if transfers else 1.0,
                "updated_at": _utc_now_iso(),
            },
        )
        sync_run = self._record_sync_run(
            action="upload",
            bundle_kind=bundle_kind,
            bundle_id=bundle_id,
            summary=summary,
            extra={
                "manifest_path": str(manifest_file),
                "remote_prefix": remote_prefix,
            },
        )
        summary["sync_run_id"] = sync_run["run_id"]
        manifest_stats = payload.get("stats", {})
        bundle_index_entry = {
            "bundle_kind": bundle_kind,
            "bundle_id": bundle_id,
            "metadata": payload.get("metadata", {}),
            "stats": {
                "file_count": int(manifest_stats.get("file_count", len(payload.get("files", [])))),
                "total_bytes": int(manifest_stats.get("total_bytes", total_bytes)),
            },
            "archive": dict(archive_entry or {}),
            "remote_prefix": remote_prefix,
            "remote_manifest_key": remote_manifest_key,
            "provider": summary["provider"],
            "latest_run_id": sync_run["run_id"],
            "last_uploaded_at": sync_run["created_at"],
            "last_uploaded_total_bytes": total_bytes,
            "manifest_sha256": _sha256(manifest_file),
        }
        self._update_bundle_index(bundle_index_entry)
        self._upload_sync_records(client, sync_run, bundle_index_entry)
        (bundle_root / "upload_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        return summary

    def download_bundle(
        self,
        *,
        bundle_kind: str,
        bundle_id: str,
        client: ObjectStorageClient,
        output_dir: str | Path | None = None,
        max_workers: int | None = None,
        resume: bool = True,
    ) -> dict:
        remote_prefix = self._bundle_remote_prefix(bundle_kind, bundle_id)
        export_root = Path(output_dir) if output_dir else self.exports_dir
        export_root.mkdir(parents=True, exist_ok=True)
        bundle_dir = export_root / bundle_id
        bundle_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = bundle_dir / "bundle_manifest.json"
        client.download_file(f"{remote_prefix}/bundle_manifest.json", manifest_path)
        payload = json.loads(manifest_path.read_text())
        downloaded = 1
        total_bytes = manifest_path.stat().st_size
        export_summary = bundle_dir / "export_summary.json"
        try:
            client.download_file(f"{remote_prefix}/export_summary.json", export_summary)
            downloaded += 1
            total_bytes += export_summary.stat().st_size
        except ObjectStorageNotFoundError:
            pass
        resolved_max_workers = self._resolve_max_workers(client, max_workers)
        archive_entry = dict(payload.get("archive") or {})
        logical_payload_file_count = len(list(payload.get("files") or []))
        if archive_entry:
            archive_relative_path = str(archive_entry.get("relative_path") or archive_entry.get("filename") or "").strip()
            if not archive_relative_path:
                raise AssetBundleError("Bundle archive metadata missing relative_path")
            transfers = [
                {
                    "relative_path": archive_relative_path,
                    "path": bundle_dir / archive_relative_path,
                    "record": {
                        "payload_relative_path": archive_relative_path,
                        "size_bytes": int(archive_entry.get("size_bytes") or 0),
                        "sha256": str(archive_entry.get("sha256") or ""),
                    },
                }
            ]
        else:
            transfers = [
                {
                    "relative_path": record["payload_relative_path"],
                    "path": bundle_dir / record["payload_relative_path"],
                    "record": dict(record),
                }
                for record in payload.get("files", [])
            ]
        pending_transfers: list[dict[str, Any]] = []
        skipped_existing: list[dict[str, Any]] = []
        for item in transfers:
            destination = Path(item["path"])
            record = dict(item.get("record") or {})
            if resume and _download_target_matches(destination, record):
                skipped_existing.append(
                    {
                        "relative_path": item["relative_path"],
                        "size_bytes": int(record.get("size_bytes", destination.stat().st_size)),
                        "status": "skipped_existing",
                    }
                )
                continue
            pending_transfers.append(item)
        progress_path = bundle_dir / "download_progress.json"
        progress_state = self._build_transfer_progress_state(
            status="running",
            bundle_kind=bundle_kind,
            bundle_id=bundle_id,
            remote_prefix=remote_prefix,
            requested_file_count=len(transfers),
            skipped_existing=skipped_existing,
        )
        progress_state["transfer_mode"] = "archive" if archive_entry else "files"
        progress_state["logical_payload_file_count"] = logical_payload_file_count
        if archive_entry:
            progress_state["archive"] = dict(archive_entry)
        self._write_progress_snapshot(progress_path, progress_state)

        def _on_download_result(result: dict[str, Any]) -> None:
            self._advance_transfer_progress(
                progress_state,
                requested_file_count=len(transfers),
                result=result,
            )
            self._write_progress_snapshot(progress_path, progress_state)

        results = self._parallel_transfer(
            pending_transfers,
            max_workers=resolved_max_workers,
            op=lambda item: self._download_transfer_item(client, remote_prefix, item),
            on_result=_on_download_result,
        )
        downloaded += len(results)
        total_bytes += sum(int(item.get("size_bytes", 0)) for item in results)
        skipped_total_bytes = sum(int(item.get("size_bytes", 0)) for item in skipped_existing)
        summary = {
            "status": "downloaded",
            "bundle_kind": bundle_kind,
            "bundle_id": bundle_id,
            "bundle_dir": str(bundle_dir),
            "manifest_path": str(manifest_path),
            "downloaded_file_count": downloaded,
            "downloaded_total_bytes": total_bytes,
            "remote_prefix": remote_prefix,
            "max_workers": resolved_max_workers,
            "requested_payload_file_count": len(transfers),
            "logical_payload_file_count": logical_payload_file_count,
            "skipped_existing_file_count": len(skipped_existing),
            "skipped_existing_total_bytes": skipped_total_bytes,
            "resume_mode": "skip_existing" if resume else "disabled",
            "transfer_mode": "archive" if archive_entry else "files",
            "effective_archive_mode": str(archive_entry.get("format") or "") if archive_entry else "none",
            "archive": dict(archive_entry or {}),
            "progress_path": str(progress_path),
            "progress": _build_progress_summary(
                requested_file_count=len(transfers),
                transferred_file_count=len(results),
                transferred_total_bytes=sum(int(item.get("size_bytes", 0)) for item in results),
                skipped_file_count=len(skipped_existing),
                skipped_total_bytes=skipped_total_bytes,
            ),
            "skipped_existing_paths_sample": [item["relative_path"] for item in skipped_existing[:5]],
        }
        self._write_progress_snapshot(
            progress_path,
            {
                **progress_state,
                "status": "downloaded",
                "transferred_file_count": len(results),
                "transferred_total_bytes": sum(int(item.get("size_bytes", 0)) for item in results),
                "completed_file_count": len(results) + len(skipped_existing),
                "remaining_file_count": 0,
                "completion_ratio": 1.0 if transfers else 1.0,
                "updated_at": _utc_now_iso(),
            },
        )
        sync_run = self._record_sync_run(
            action="download",
            bundle_kind=bundle_kind,
            bundle_id=bundle_id,
            summary=summary,
            extra={
                "bundle_dir": str(bundle_dir),
                "manifest_path": str(manifest_path),
                "remote_prefix": remote_prefix,
            },
        )
        summary["sync_run_id"] = sync_run["run_id"]
        manifest_stats = payload.get("stats", {})
        bundle_index_entry = {
            "bundle_kind": bundle_kind,
            "bundle_id": bundle_id,
            "metadata": payload.get("metadata", {}),
            "stats": {
                "file_count": int(manifest_stats.get("file_count", len(payload.get("files", [])))),
                "total_bytes": int(manifest_stats.get("total_bytes", 0)),
            },
            "archive": dict(archive_entry or {}),
            "remote_prefix": remote_prefix,
            "remote_manifest_key": f"{remote_prefix}/bundle_manifest.json",
            "manifest_sha256": _sha256(manifest_path),
            "latest_run_id": sync_run["run_id"],
            "last_downloaded_at": sync_run["created_at"],
            "last_downloaded_total_bytes": total_bytes,
        }
        self._update_bundle_index(bundle_index_entry)
        self._upload_sync_records(client, sync_run, bundle_index_entry)
        (bundle_dir / "download_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        return summary

    def _resolve_max_workers(self, client: ObjectStorageClient, requested: int | None) -> int:
        if requested is not None:
            try:
                return max(1, min(int(requested), 64))
            except (TypeError, ValueError):
                return 8
        client_config = getattr(client, "config", None)
        candidate = getattr(client_config, "max_workers", 8)
        try:
            return max(1, min(int(candidate), 64))
        except (TypeError, ValueError):
            return 8

    def _parallel_transfer(
        self,
        items: list[dict[str, Any]],
        *,
        max_workers: int,
        op,
        on_result=None,
    ) -> list[dict]:
        if not items:
            return []
        if max_workers <= 1 or len(items) == 1:
            results: list[dict] = []
            for item in items:
                result = op(item)
                results.append(result)
                if on_result is not None:
                    on_result(result)
            return sorted(results, key=lambda entry: entry["relative_path"])
        results: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(op, item): item for item in items}
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if on_result is not None:
                    on_result(result)
        return sorted(results, key=lambda entry: entry["relative_path"])

    def _upload_transfer_item(self, client: ObjectStorageClient, remote_prefix: str, item: dict[str, Any]) -> dict:
        path = Path(item["path"])
        relative_path = str(item["relative_path"])
        object_key = f"{remote_prefix}/{relative_path}"
        result = self._with_retries(
            lambda: client.upload_file(path, object_key, content_type=_content_type_for_path(path)),
            action=f"upload {relative_path}",
        )
        return {
            "relative_path": relative_path,
            "size_bytes": int(result.get("size_bytes", path.stat().st_size)),
            **result,
        }

    def _download_transfer_item(self, client: ObjectStorageClient, remote_prefix: str, item: dict[str, Any]) -> dict:
        destination = Path(item["path"])
        relative_path = str(item["relative_path"])
        result = self._with_retries(
            lambda: client.download_file(f"{remote_prefix}/{relative_path}", destination),
            action=f"download {relative_path}",
        )
        return {
            "relative_path": relative_path,
            "size_bytes": int(result.get("size_bytes", destination.stat().st_size)),
            **result,
        }

    def _with_retries(self, fn, *, action: str, attempts: int = 3) -> dict:
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= attempts:
                    break
                time.sleep(min(2.0, 0.35 * attempt))
        raise AssetBundleError(f"{action} failed after {attempts} attempts: {last_error}") from last_error

    def _record_sync_run(
        self,
        *,
        action: str,
        bundle_kind: str,
        bundle_id: str,
        summary: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> dict:
        self.sync_runs_dir.mkdir(parents=True, exist_ok=True)
        created_at = _utc_now_iso()
        run_id = f"{_safe_token(action)}_{_safe_token(bundle_kind)}_{_safe_token(bundle_id)}_{_utc_now_token()}"
        payload = {
            "run_id": run_id,
            "action": action,
            "bundle_kind": bundle_kind,
            "bundle_id": bundle_id,
            "created_at": created_at,
            "summary": summary,
            "extra": extra or {},
        }
        (self.sync_runs_dir / f"{run_id}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        return payload

    def _update_bundle_index(self, entry: dict[str, Any]) -> None:
        self.sync_dir.mkdir(parents=True, exist_ok=True)
        payload = self._load_json_if_exists(self.sync_bundle_index_path, {"updated_at": "", "bundles": []})
        bundles = {
            (str(item.get("bundle_kind", "")).strip(), str(item.get("bundle_id", "")).strip()): dict(item)
            for item in payload.get("bundles", [])
            if str(item.get("bundle_kind", "")).strip() and str(item.get("bundle_id", "")).strip()
        }
        key = (entry["bundle_kind"], entry["bundle_id"])
        merged = dict(bundles.get(key, {}))
        merged.update(entry)
        bundles[key] = merged
        updated_payload = {
            "updated_at": _utc_now_iso(),
            "bundles": sorted(bundles.values(), key=lambda item: (item["bundle_kind"], item["bundle_id"])),
        }
        self.sync_bundle_index_path.write_text(json.dumps(updated_payload, ensure_ascii=False, indent=2))

    def _upload_sync_records(
        self,
        client: ObjectStorageClient,
        sync_run: dict[str, Any],
        bundle_index_entry: dict[str, Any],
    ) -> None:
        run_key = f"indexes/sync_runs/{sync_run['run_id']}.json"
        client.upload_bytes(json.dumps(sync_run, ensure_ascii=False, indent=2).encode("utf-8"), run_key, content_type="application/json")
        try:
            remote_index_payload = json.loads(client.download_bytes("indexes/bundle_index.json").decode("utf-8"))
        except ObjectStorageNotFoundError:
            remote_index_payload = {"updated_at": "", "bundles": []}
        bundles = {
            (str(item.get("bundle_kind", "")).strip(), str(item.get("bundle_id", "")).strip()): dict(item)
            for item in remote_index_payload.get("bundles", [])
            if str(item.get("bundle_kind", "")).strip() and str(item.get("bundle_id", "")).strip()
        }
        key = (bundle_index_entry["bundle_kind"], bundle_index_entry["bundle_id"])
        merged = dict(bundles.get(key, {}))
        merged.update(bundle_index_entry)
        merged["latest_remote_run_id"] = sync_run["run_id"]
        bundles[key] = merged
        updated_payload = {
            "updated_at": _utc_now_iso(),
            "bundles": sorted(bundles.values(), key=lambda item: (item["bundle_kind"], item["bundle_id"])),
        }
        client.upload_bytes(
            json.dumps(updated_payload, ensure_ascii=False, indent=2).encode("utf-8"),
            "indexes/bundle_index.json",
            content_type="application/json",
        )

    def _load_json_if_exists(self, path: Path, default: dict[str, Any]) -> dict[str, Any]:
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            return default

    def _build_transfer_progress_state(
        self,
        *,
        status: str,
        bundle_kind: str,
        bundle_id: str,
        remote_prefix: str,
        requested_file_count: int,
        skipped_existing: list[dict[str, Any]],
    ) -> dict[str, Any]:
        completed_count = len(skipped_existing)
        return {
            "status": status,
            "bundle_kind": bundle_kind,
            "bundle_id": bundle_id,
            "remote_prefix": remote_prefix,
            "requested_file_count": requested_file_count,
            "transferred_file_count": 0,
            "transferred_total_bytes": 0,
            "skipped_existing_file_count": completed_count,
            "skipped_existing_total_bytes": sum(int(item.get("size_bytes", 0)) for item in skipped_existing),
            "completed_file_count": completed_count,
            "remaining_file_count": max(requested_file_count - completed_count, 0),
            "completion_ratio": round(completed_count / requested_file_count, 4) if requested_file_count else 1.0,
            "updated_at": _utc_now_iso(),
            "last_completed_relative_path": "",
        }

    def _advance_transfer_progress(
        self,
        progress_state: dict[str, Any],
        *,
        requested_file_count: int,
        result: dict[str, Any],
    ) -> None:
        progress_state["transferred_file_count"] = int(progress_state.get("transferred_file_count", 0)) + 1
        progress_state["transferred_total_bytes"] = int(progress_state.get("transferred_total_bytes", 0)) + int(result.get("size_bytes", 0))
        progress_state["completed_file_count"] = int(progress_state.get("completed_file_count", 0)) + 1
        progress_state["remaining_file_count"] = max(requested_file_count - int(progress_state["completed_file_count"]), 0)
        progress_state["completion_ratio"] = (
            round(int(progress_state["completed_file_count"]) / requested_file_count, 4)
            if requested_file_count
            else 1.0
        )
        progress_state["updated_at"] = _utc_now_iso()
        progress_state["last_completed_relative_path"] = str(result.get("relative_path") or "")

    def _write_progress_snapshot(self, path: Path, payload: dict[str, Any]) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")

    def _resolve_archive_mode(self, manifest_payload: dict[str, Any], requested_mode: str) -> str:
        normalized = str(requested_mode or "auto").strip().lower() or "auto"
        if normalized not in {"auto", "none", "tar", "tar.gz"}:
            raise AssetBundleError(f"Unsupported archive mode: {requested_mode}")
        if normalized != "auto":
            return normalized
        stats = dict(manifest_payload.get("stats") or {})
        file_count = int(stats.get("file_count") or len(list(manifest_payload.get("files") or [])) or 0)
        total_bytes = int(stats.get("total_bytes") or 0)
        if file_count >= 128 or total_bytes >= 64 * 1024 * 1024:
            return "tar"
        return "none"

    def _prepare_bundle_archive(
        self,
        bundle_root: Path,
        manifest_payload: dict[str, Any],
        archive_mode: str,
    ) -> dict[str, Any]:
        if archive_mode == "none":
            return {}
        if archive_mode not in {"tar", "tar.gz"}:
            raise AssetBundleError(f"Unsupported effective archive mode: {archive_mode}")
        archive_filename = "payload.tar.gz" if archive_mode == "tar.gz" else "payload.tar"
        archive_path = bundle_root / archive_filename
        existing = dict(manifest_payload.get("archive") or {})
        if existing and self._bundle_archive_matches(existing, archive_path, archive_mode):
            return {
                **existing,
                "format": archive_mode,
                "filename": archive_filename,
                "relative_path": archive_filename,
                "logical_file_count": len(list(manifest_payload.get("files") or [])),
            }

        payload_dir = bundle_root / "payload"
        if not payload_dir.exists():
            raise AssetBundleError(f"Bundle payload directory missing: {payload_dir}")
        for record in manifest_payload.get("files", []):
            source = bundle_root / str(record.get("payload_relative_path") or "")
            if not source.exists():
                raise AssetBundleError(f"Cannot archive missing payload file: {source}")

        temp_archive_path = archive_path.with_name(f"{archive_path.name}.tmp")
        if temp_archive_path.exists():
            temp_archive_path.unlink()
        self._write_bundle_archive(bundle_root=bundle_root, payload_dir=payload_dir, archive_path=temp_archive_path, archive_mode=archive_mode)
        temp_archive_path.replace(archive_path)
        return {
            "enabled": True,
            "format": archive_mode,
            "filename": archive_filename,
            "relative_path": archive_filename,
            "size_bytes": archive_path.stat().st_size,
            "sha256": _sha256(archive_path),
            "logical_file_count": len(list(manifest_payload.get("files") or [])),
            "created_at": _utc_now_iso(),
        }

    def _persist_bundle_archive_metadata(
        self,
        bundle_root: Path,
        manifest_payload: dict[str, Any],
        archive_entry: dict[str, Any] | None,
    ) -> dict[str, Any]:
        updated_payload = dict(manifest_payload)
        if archive_entry:
            updated_payload["archive"] = dict(archive_entry)
        else:
            updated_payload.pop("archive", None)
        manifest_path = bundle_root / "bundle_manifest.json"
        manifest_path.write_text(json.dumps(updated_payload, ensure_ascii=False, indent=2))
        export_summary_path = bundle_root / "export_summary.json"
        if export_summary_path.exists():
            summary_payload = self._load_json_if_exists(export_summary_path, {})
            if archive_entry:
                summary_payload["archive"] = dict(archive_entry)
            else:
                summary_payload.pop("archive", None)
            export_summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2))
        return updated_payload

    def _bundle_archive_matches(self, archive_entry: dict[str, Any], archive_path: Path, archive_mode: str) -> bool:
        if not archive_path.exists() or not archive_path.is_file():
            return False
        if str(archive_entry.get("format") or "").strip().lower() != archive_mode:
            return False
        expected_size = int(archive_entry.get("size_bytes") or 0)
        if expected_size > 0 and archive_path.stat().st_size != expected_size:
            return False
        expected_sha256 = str(archive_entry.get("sha256") or "").strip().lower()
        if expected_sha256 and _sha256(archive_path).lower() != expected_sha256:
            return False
        return True

    def _write_bundle_archive(
        self,
        *,
        bundle_root: Path,
        payload_dir: Path,
        archive_path: Path,
        archive_mode: str,
    ) -> None:
        archive_path.parent.mkdir(parents=True, exist_ok=True)

        def _add_payload_files(tar_handle: tarfile.TarFile) -> None:
            for source in sorted(path for path in payload_dir.rglob("*") if path.is_file()):
                tar_handle.add(
                    source,
                    arcname=source.relative_to(bundle_root).as_posix(),
                    recursive=False,
                    filter=_normalize_tar_info,
                )

        if archive_mode == "tar.gz":
            with archive_path.open("wb") as raw_handle:
                with gzip.GzipFile(filename="", mode="wb", fileobj=raw_handle, mtime=0) as gzip_handle:
                    with tarfile.open(fileobj=gzip_handle, mode="w") as tar_handle:
                        _add_payload_files(tar_handle)
            return
        with tarfile.open(archive_path, mode="w") as tar_handle:
            _add_payload_files(tar_handle)

    def _ensure_bundle_payload_materialized(self, bundle_root: Path, manifest_payload: dict[str, Any]) -> dict[str, Any]:
        archive_entry = dict(manifest_payload.get("archive") or {})
        if not archive_entry:
            return {"enabled": False, "status": "not_applicable"}
        file_records = [dict(record) for record in list(manifest_payload.get("files") or [])]
        if not file_records:
            return {"enabled": True, "status": "empty_bundle", "archive": archive_entry}
        missing_payloads = [
            str(record.get("payload_relative_path") or "")
            for record in file_records
            if not (bundle_root / str(record.get("payload_relative_path") or "")).exists()
        ]
        if not missing_payloads:
            return {"enabled": True, "status": "already_materialized", "archive": archive_entry}
        archive_relative_path = str(archive_entry.get("relative_path") or archive_entry.get("filename") or "").strip()
        if not archive_relative_path:
            raise AssetBundleError("Bundle archive metadata missing relative_path")
        archive_path = bundle_root / archive_relative_path
        if not archive_path.exists():
            raise AssetBundleError(
                f"Bundle payload missing and archive not found: {archive_path} (missing {missing_payloads[0]})"
            )
        self._extract_bundle_archive(bundle_root=bundle_root, archive_path=archive_path)
        return {
            "enabled": True,
            "status": "extracted",
            "archive": archive_entry,
            "archive_path": str(archive_path),
            "missing_payload_count": len(missing_payloads),
        }

    def _extract_bundle_archive(self, *, bundle_root: Path, archive_path: Path) -> None:
        suffixes = [suffix.lower() for suffix in archive_path.suffixes]
        mode = "r:gz" if suffixes[-2:] == [".tar", ".gz"] else "r"
        with tarfile.open(archive_path, mode=mode) as tar_handle:
            members = tar_handle.getmembers()
            for member in members:
                target = (bundle_root / member.name).resolve()
                bundle_root_resolved = bundle_root.resolve()
                if bundle_root_resolved not in {target, *target.parents}:
                    raise AssetBundleError(f"Archive member escapes bundle root: {member.name}")
            tar_handle.extractall(bundle_root)

    def _export_bundle(
        self,
        *,
        bundle_kind: str,
        bundle_key: str,
        relpaths: Iterable[Path],
        metadata: dict,
        output_dir: str | Path | None,
    ) -> dict:
        normalized = sorted({Path(path).as_posix() for path in relpaths})
        if not normalized:
            raise AssetBundleError(f"No files selected for bundle kind {bundle_kind}")
        export_root = Path(output_dir) if output_dir else self.exports_dir
        export_root.mkdir(parents=True, exist_ok=True)
        bundle_id = f"{bundle_kind}_{_safe_token(bundle_key)}_{_utc_now_token()}"
        bundle_dir = export_root / bundle_id
        payload_dir = bundle_dir / "payload"
        payload_dir.mkdir(parents=True, exist_ok=False)

        files: list[BundleFile] = []
        total_bytes = 0
        for relpath_str in normalized:
            relpath = Path(relpath_str)
            source = self.runtime_dir / relpath
            if not source.exists() or not source.is_file():
                continue
            destination = payload_dir / relpath
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            size_bytes = destination.stat().st_size
            total_bytes += size_bytes
            files.append(
                BundleFile(
                    runtime_relative_path=relpath.as_posix(),
                    payload_relative_path=(Path("payload") / relpath).as_posix(),
                    size_bytes=size_bytes,
                    sha256=_sha256(destination),
                )
            )
        if not files:
            raise AssetBundleError(f"All selected files were missing for bundle kind {bundle_kind}")
        manifest = {
            "bundle_version": 1,
            "bundle_kind": bundle_kind,
            "bundle_id": bundle_id,
            "created_at": _utc_now_iso(),
            "project_root": str(self.project_root),
            "runtime_dir": str(self.runtime_dir),
            "metadata": metadata,
            "stats": {
                "file_count": len(files),
                "total_bytes": total_bytes,
            },
            "files": [asdict(file) for file in files],
        }
        manifest_path = bundle_dir / "bundle_manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
        summary = {
            "status": "exported",
            "bundle_kind": bundle_kind,
            "bundle_id": bundle_id,
            "bundle_dir": str(bundle_dir),
            "manifest_path": str(manifest_path),
            "file_count": len(files),
            "total_bytes": total_bytes,
            "metadata": metadata,
        }
        (bundle_dir / "export_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        return summary

    def _resolve_company_dir(self, company: str) -> tuple[str, Path]:
        company_assets_dir = self.runtime_dir / "company_assets"
        normalized_target = _normalize_key(company)
        candidates = [
            path for path in company_assets_dir.iterdir()
            if path.is_dir()
        ] if company_assets_dir.exists() else []
        for candidate in candidates:
            if _normalize_key(candidate.name) == normalized_target:
                return candidate.name, candidate
        raise AssetBundleError(f"Company assets not found for {company}")

    def _latest_snapshot_id(self, company_dir: Path) -> str:
        latest_path = company_dir / "latest_snapshot.json"
        if latest_path.exists():
            payload = json.loads(latest_path.read_text())
            snapshot_id = str(payload.get("snapshot_id", "")).strip()
            if snapshot_id:
                return snapshot_id
        snapshot_dirs = sorted(path.name for path in company_dir.iterdir() if path.is_dir())
        if not snapshot_dirs:
            raise AssetBundleError(f"No snapshots found under {company_dir}")
        return snapshot_dirs[-1]

    def _company_aliases(self, company_dir: Path, company_key: str, company: str) -> set[str]:
        aliases = {company_key, _normalize_key(company)}
        latest_path = company_dir / "latest_snapshot.json"
        if latest_path.exists():
            payload = json.loads(latest_path.read_text())
            identity = payload.get("company_identity", {})
            for value in identity.get("aliases", []):
                normalized = _normalize_key(str(value))
                if normalized:
                    aliases.add(normalized)
            for field in ("requested_name", "canonical_name", "linkedin_slug", "domain"):
                normalized = _normalize_key(str(identity.get(field, "")))
                if normalized:
                    aliases.add(normalized)
        aliases.discard("")
        return aliases

    def _matching_job_files(self, jobs_dir: Path, aliases: set[str]) -> list[Path]:
        matched: list[Path] = []
        for path in jobs_dir.glob("*.json"):
            if not path.is_file():
                continue
            try:
                payload = json.loads(path.read_text())
            except Exception:
                continue
            blob = _normalize_key(json.dumps(payload, ensure_ascii=False))
            if any(alias and alias in blob for alias in aliases):
                matched.append(path.relative_to(self.runtime_dir))
        return matched

    def _collect_files(self, relative_dir: Path) -> list[Path]:
        root = self.runtime_dir / relative_dir
        if not root.exists():
            return []
        return [path.relative_to(self.runtime_dir) for path in root.rglob("*") if path.is_file()]

    def _bundle_remote_prefix(self, bundle_kind: str, bundle_id: str) -> str:
        return f"bundles/{_safe_token(bundle_kind)}/{bundle_id}"


def _content_type_for_path(path: Path) -> str:
    name = path.name.lower()
    suffix = path.suffix.lower()
    if name.endswith(".tar.gz"):
        return "application/gzip"
    if suffix == ".tar":
        return "application/x-tar"
    if suffix == ".json":
        return "application/json"
    if suffix in {".txt", ".md"}:
        return "text/plain; charset=utf-8"
    if suffix == ".html":
        return "text/html; charset=utf-8"
    if suffix == ".xml":
        return "application/xml"
    if suffix == ".pdf":
        return "application/pdf"
    if suffix == ".db":
        return "application/octet-stream"
    return "application/octet-stream"


def _provider_name_for_client(client: ObjectStorageClient) -> str:
    config = getattr(client, "config", None)
    provider = str(getattr(config, "provider", "") or "").strip().lower()
    if provider:
        return provider
    class_name = client.__class__.__name__.lower()
    if "filesystem" in class_name:
        return "filesystem"
    if "s3" in class_name:
        return "s3_compatible"
    return ""


def _build_progress_summary(
    *,
    requested_file_count: int,
    transferred_file_count: int,
    transferred_total_bytes: int,
    skipped_file_count: int,
    skipped_total_bytes: int,
) -> dict[str, Any]:
    requested = max(0, int(requested_file_count))
    transferred = max(0, int(transferred_file_count))
    skipped = max(0, int(skipped_file_count))
    completed = min(requested, transferred + skipped)
    remaining = max(0, requested - completed)
    return {
        "requested_file_count": requested,
        "transferred_file_count": transferred,
        "skipped_file_count": skipped,
        "completed_file_count": completed,
        "remaining_file_count": remaining,
        "completion_ratio": 1.0 if requested == 0 else round(completed / requested, 4),
        "transferred_total_bytes": max(0, int(transferred_total_bytes)),
        "skipped_total_bytes": max(0, int(skipped_total_bytes)),
        "processed_total_bytes": max(0, int(transferred_total_bytes)) + max(0, int(skipped_total_bytes)),
    }


def _download_target_matches(destination: Path, record: dict[str, Any]) -> bool:
    if not destination.exists() or not destination.is_file():
        return False
    expected_size = record.get("size_bytes")
    if expected_size not in {None, ""}:
        try:
            if destination.stat().st_size != int(expected_size):
                return False
        except (TypeError, ValueError):
            return False
    expected_sha256 = str(record.get("sha256") or "").strip().lower()
    if expected_sha256:
        try:
            if _sha256(destination).lower() != expected_sha256:
                return False
        except OSError:
            return False
    return True


def _normalize_tar_info(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    return info
