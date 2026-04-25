from __future__ import annotations

import gzip
import hashlib
import json
import shutil
import sqlite3
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .artifact_cache import materialize_link_first_file
from .asset_paths import (
    canonical_company_assets_dir,
    company_assets_roots,
    hot_cache_snapshot_dir,
    iter_company_asset_company_dirs,
    load_company_snapshot_identity,
    load_company_snapshot_json,
    resolve_company_snapshot_match_selection,
)
from .control_plane_postgres import (
    control_plane_snapshot_output_path,
    export_control_plane_snapshot,
    restore_control_plane_snapshot_to_sqlite,
)
from .local_postgres import resolve_default_control_plane_db_path
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
        self.sync_generation_index_path = self.sync_dir / "generation_index.json"
        self.sync_generations_dir = self.sync_dir / "generations"

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
        relpaths = self._collect_files(snapshot_dir)
        latest_pointer = company_dir / "latest_snapshot.json"
        if latest_pointer.exists():
            relpaths.append(self._runtime_relative_path(latest_pointer))
        company_registry = company_dir / "asset_registry.json"
        if company_registry.exists():
            relpaths.append(self._runtime_relative_path(company_registry))
        metadata = {
            "company": company,
            "company_key": company_key,
            "snapshot_id": resolved_snapshot_id,
            "bundle_scope": "company_snapshot",
        }
        metadata.update(self._candidate_generation_metadata_for_snapshot(snapshot_dir))
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
        include_sqlite: bool = False,
        include_live_tests: bool = True,
        include_manual_review: bool = True,
        include_jobs: bool = True,
    ) -> dict:
        company_key, company_dir = self._resolve_company_dir(company)
        aliases = self._company_aliases(company_dir, company_key, company)
        relpaths: list[Path] = []
        relpaths.extend(self._collect_files(company_dir))
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
        control_plane_snapshot_path: Path | None = None
        if include_jobs:
            jobs_dir = self.runtime_dir / "jobs"
            if jobs_dir.exists():
                relpaths.extend(self._matching_job_files(jobs_dir, aliases))
        if include_sqlite:
            control_plane_snapshot_path = self._ensure_control_plane_snapshot_export()
            if control_plane_snapshot_path is not None and control_plane_snapshot_path.exists():
                relpaths.append(control_plane_snapshot_path.relative_to(self.runtime_dir))
        metadata = {
            "company": company,
            "company_key": company_key,
            "bundle_scope": "company_handoff",
            "requested_include_sqlite": include_sqlite,
            "include_sqlite": False,
            "include_control_plane_snapshot": bool(control_plane_snapshot_path and control_plane_snapshot_path.exists()),
            "control_plane_snapshot_role": "backup_only" if include_sqlite else "",
            "include_live_tests": include_live_tests,
            "include_manual_review": include_manual_review,
            "include_jobs": include_jobs,
            "latest_snapshot_id": self._latest_snapshot_id(company_dir),
            "aliases": sorted(aliases),
        }
        latest_snapshot_dir = company_dir / str(metadata["latest_snapshot_id"])
        if latest_snapshot_dir.exists():
            metadata["latest_candidate_generation"] = self._candidate_generation_metadata_for_snapshot(
                latest_snapshot_dir
            )
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
        control_plane_snapshot_path = self._ensure_control_plane_snapshot_export(include_all_sqlite_tables=True)
        if control_plane_snapshot_path is not None and control_plane_snapshot_path.exists():
            return self._export_bundle(
                bundle_kind="control_plane_snapshot",
                bundle_key="control_plane_snapshot",
                relpaths=[control_plane_snapshot_path.relative_to(self.runtime_dir)],
                metadata={
                    "bundle_scope": "control_plane_snapshot",
                    "control_plane_snapshot_role": "backup_only",
                    "sqlite_role": "deprecated_legacy_alias",
                },
                output_dir=output_dir,
            )
        sqlite_path = resolve_default_control_plane_db_path(self.runtime_dir, base_dir=self.project_root)
        if not sqlite_path.exists():
            raise AssetBundleError("SQLite database does not exist")
        self._checkpoint_sqlite_snapshot(sqlite_path)
        return self._export_bundle(
            bundle_kind="sqlite_snapshot",
            bundle_key="sourcing_agent_db",
            relpaths=[sqlite_path.relative_to(self.runtime_dir)],
            metadata={
                "bundle_scope": "sqlite_snapshot",
                "sqlite_role": "backup_only",
            },
            output_dir=output_dir,
        )

    def _checkpoint_sqlite_snapshot(self, sqlite_path: Path) -> None:
        try:
            header = sqlite_path.read_bytes()[:16]
        except OSError as exc:
            raise AssetBundleError(f"Failed to read sqlite snapshot before export: {exc}") from exc
        if header != b"SQLite format 3\x00":
            return
        try:
            with sqlite3.connect(sqlite_path) as connection:
                connection.execute("PRAGMA wal_checkpoint(FULL)")
        except sqlite3.Error as exc:
            raise AssetBundleError(f"Failed to checkpoint sqlite snapshot before export: {exc}") from exc

    def _ensure_control_plane_snapshot_export(self, *, include_all_sqlite_tables: bool = False) -> Path | None:
        snapshot_path = control_plane_snapshot_output_path(self.runtime_dir)
        if snapshot_path.exists():
            if not include_all_sqlite_tables:
                return snapshot_path
            try:
                snapshot_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
            except (OSError, ValueError, json.JSONDecodeError):
                snapshot_path.unlink(missing_ok=True)
            else:
                if bool(snapshot_payload.get("include_all_sqlite_tables")):
                    return snapshot_path
        sqlite_path: Path | None = resolve_default_control_plane_db_path(self.runtime_dir, base_dir=self.project_root)
        resolved_sqlite_path = sqlite_path
        if resolved_sqlite_path is not None and resolved_sqlite_path.exists():
            try:
                header = resolved_sqlite_path.read_bytes()[:16]
            except OSError:
                return None
            if header != b"SQLite format 3\x00":
                sqlite_path = None
        try:
            export_control_plane_snapshot(
                runtime_dir=self.runtime_dir,
                output_path=snapshot_path,
                sqlite_path=sqlite_path,
                include_all_sqlite_tables=include_all_sqlite_tables,
            )
        except (FileNotFoundError, RuntimeError, sqlite3.Error):
            snapshot_path.unlink(missing_ok=True)
            return None
        return snapshot_path if snapshot_path.exists() else None

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
        if str(payload.get("bundle_kind") or "").strip() == "control_plane_snapshot":
            destination = (
                Path(target_db_path)
                if target_db_path
                else resolve_default_control_plane_db_path(self.runtime_dir, base_dir=self.project_root)
            )
            backup_path = None
            if backup_current and destination.exists():
                resolved_backup_dir = Path(backup_dir) if backup_dir else (self.runtime_dir / "backups" / "sqlite")
                resolved_backup_dir.mkdir(parents=True, exist_ok=True)
                backup_path = resolved_backup_dir / f"sourcing_agent_{_utc_now_token()}.db"
                shutil.copy2(destination, backup_path)
            restore_summary = self.restore_bundle(
                manifest_path,
                target_runtime_dir=self.runtime_dir,
                conflict="overwrite",
            )
            snapshot_path = control_plane_snapshot_output_path(self.runtime_dir)
            sqlite_restore = restore_control_plane_snapshot_to_sqlite(
                snapshot_path=snapshot_path,
                runtime_dir=self.runtime_dir,
                sqlite_path=destination,
            )
            summary = {
                "status": "control_plane_snapshot_restored",
                "bundle_id": payload.get("bundle_id", ""),
                "control_plane_snapshot_role": "backup_only",
                "source_path": str(snapshot_path),
                "target_snapshot_path": str(snapshot_path),
                "target_db_path": str(destination),
                "backup_path": str(backup_path) if backup_path else "",
                "restored_file_count": int(restore_summary.get("restored_file_count") or 0),
                "sqlite_restore": sqlite_restore,
            }
            summary_path = manifest_file.parent / "sqlite_restore_summary.json"
            summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
            return summary
        db_entry = next(
            (entry for entry in payload.get("files", []) if entry.get("runtime_relative_path") == "sourcing_agent.db"),
            None,
        )
        if db_entry is None:
            raise AssetBundleError("Bundle does not contain sourcing_agent.db")
        source = manifest_file.parent / db_entry["payload_relative_path"]
        if not source.exists():
            self._ensure_bundle_payload_materialized(manifest_file.parent, payload)
        if not source.exists():
            raise AssetBundleError(f"SQLite payload missing: {source}")
        destination = (
            Path(target_db_path)
            if target_db_path
            else resolve_default_control_plane_db_path(self.runtime_dir, base_dir=self.project_root)
        )
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
            "sqlite_role": "backup_only",
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
            upload_paths.append(
                bundle_root / str(archive_entry.get("relative_path") or archive_entry.get("filename") or "payload.tar")
            )
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
                path = Path(str(item["path"]))
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
            archive_relative_path = str(
                archive_entry.get("relative_path") or archive_entry.get("filename") or ""
            ).strip()
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
            destination = Path(str(item["path"]))
            raw_record = item.get("record")
            record = dict(raw_record) if isinstance(raw_record, dict) else {}
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

    def delete_bundle(
        self,
        *,
        bundle_kind: str,
        bundle_id: str,
        client: ObjectStorageClient,
        max_workers: int | None = None,
        prune_local_index: bool = True,
    ) -> dict:
        normalized_kind = str(bundle_kind or "").strip()
        normalized_id = str(bundle_id or "").strip()
        if not normalized_kind or not normalized_id:
            raise AssetBundleError("bundle_kind and bundle_id are required")
        remote_prefix = self._bundle_remote_prefix(normalized_kind, normalized_id)
        manifest_key = f"{remote_prefix}/bundle_manifest.json"
        manifest_payload: dict[str, Any] = {}
        try:
            manifest_payload = json.loads(client.download_bytes(manifest_key).decode("utf-8"))
        except ObjectStorageNotFoundError:
            manifest_payload = {}

        object_keys = self._bundle_object_keys_from_manifest(
            bundle_kind=normalized_kind,
            bundle_id=normalized_id,
            manifest_payload=manifest_payload,
        )
        resolved_max_workers = self._resolve_max_workers(client, max_workers)
        deletions = self._parallel_transfer(
            [{"object_key": object_key} for object_key in object_keys],
            max_workers=resolved_max_workers,
            op=lambda item: client.delete_object(str(item.get("object_key") or "")),
        )
        deleted_objects = [item for item in deletions if str(item.get("status") or "") == "deleted"]
        missing_objects = [item for item in deletions if str(item.get("status") or "") == "missing"]
        sync_run_summary = {
            "status": "deleted",
            "bundle_kind": normalized_kind,
            "bundle_id": normalized_id,
            "remote_prefix": remote_prefix,
            "requested_object_count": len(object_keys),
            "deleted_object_count": len(deleted_objects),
            "missing_object_count": len(missing_objects),
            "deleted_object_keys": [str(item.get("object_key") or "") for item in deleted_objects],
            "missing_object_keys": [str(item.get("object_key") or "") for item in missing_objects],
            "provider": getattr(getattr(client, "config", None), "provider", ""),
            "max_workers": resolved_max_workers,
        }
        sync_run = self._record_sync_run(
            action="delete",
            bundle_kind=normalized_kind,
            bundle_id=normalized_id,
            summary=sync_run_summary,
            extra={
                "remote_prefix": remote_prefix,
                "requested_object_keys": list(object_keys),
            },
        )
        self._upload_bundle_delete_records(
            client,
            sync_run=sync_run,
            bundle_kind=normalized_kind,
            bundle_id=normalized_id,
        )
        if prune_local_index:
            self._remove_bundle_index_entry(normalized_kind, normalized_id)
        return {
            **sync_run_summary,
            "sync_run_id": sync_run["run_id"],
        }

    def publish_candidate_generation(
        self,
        *,
        target_company: str,
        snapshot_id: str = "",
        asset_view: str = "canonical_merged",
        client: ObjectStorageClient,
        max_workers: int | None = None,
        resume: bool = True,
        include_compatibility_exports: bool = False,
    ) -> dict[str, Any]:
        manifest_payload = self._build_candidate_generation_manifest(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            include_compatibility_exports=include_compatibility_exports,
        )
        generation_key = str(manifest_payload.get("generation_key") or "").strip()
        if not generation_key:
            raise AssetBundleError("Candidate generation manifest missing generation_key")
        local_dir = self._candidate_generation_local_dir(generation_key)
        local_dir.mkdir(parents=True, exist_ok=True)
        generation_manifest_path = local_dir / "generation_manifest.json"
        generation_manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2))
        remote_prefix = str(manifest_payload.get("remote_prefix") or "").strip()
        if not remote_prefix:
            raise AssetBundleError("Candidate generation manifest missing remote_prefix")
        generation_manifest_key = f"{remote_prefix}/generation_manifest.json"
        transfers = [
            {
                "relative_path": str(item.get("relative_path") or "").strip(),
                "path": Path(str(item.get("source_path") or "")).expanduser(),
                "object_key": str(item.get("object_key") or "").strip(),
                "record": dict(item),
            }
            for item in list(manifest_payload.get("files") or [])
            if str(item.get("relative_path") or "").strip()
            and str(item.get("source_path") or "").strip()
            and str(item.get("object_key") or "").strip()
        ]
        pending_transfers: list[dict[str, Any]] = []
        skipped_existing: list[dict[str, Any]] = []
        resume_scan_required = bool(resume and client.has_object(generation_manifest_key))
        if resume_scan_required:
            for item in transfers:
                object_key = str(item.get("object_key") or "").strip()
                path = Path(str(item.get("path") or ""))
                if client.has_object(object_key):
                    skipped_existing.append(
                        {
                            "relative_path": str(item.get("relative_path") or ""),
                            "object_key": object_key,
                            "object_url": client.object_url(object_key),
                            "size_bytes": path.stat().st_size if path.exists() else 0,
                            "status": "skipped_existing",
                        }
                    )
                    continue
                pending_transfers.append(item)
        else:
            pending_transfers = list(transfers)
        progress_path = local_dir / "upload_progress.json"
        progress_state = self._build_transfer_progress_state(
            status="running",
            bundle_kind="candidate_generation",
            bundle_id=generation_key,
            remote_prefix=remote_prefix,
            requested_file_count=len(transfers),
            skipped_existing=skipped_existing,
        )
        progress_state["transfer_mode"] = "generation_files"
        progress_state["snapshot_id"] = str(manifest_payload.get("snapshot_id") or "")
        progress_state["asset_view"] = str(manifest_payload.get("asset_view") or "")
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
            max_workers=self._resolve_max_workers(client, max_workers),
            op=lambda item: self._with_retries(
                lambda: {
                    "relative_path": str(item.get("relative_path") or ""),
                    **client.upload_file(
                        Path(str(item.get("path") or "")),
                        str(item.get("object_key") or ""),
                        content_type=_content_type_for_path(Path(str(item.get("path") or ""))),
                    ),
                    "size_bytes": int(Path(str(item.get("path") or "")).stat().st_size),
                },
                action=f"upload generation {item.get('relative_path')}",
            ),
            on_result=_on_upload_result,
        )
        uploaded_total_bytes = sum(int(item.get("size_bytes", 0)) for item in uploads)
        skipped_existing_total_bytes = sum(int(item.get("size_bytes", 0)) for item in skipped_existing)
        publish_summary = {
            "status": "uploaded",
            "generation_key": generation_key,
            "snapshot_id": str(manifest_payload.get("snapshot_id") or ""),
            "asset_view": str(manifest_payload.get("asset_view") or ""),
            "target_company": str(manifest_payload.get("target_company") or ""),
            "remote_prefix": remote_prefix,
            "generation_manifest_key": generation_manifest_key,
            "requested_file_count": len(transfers),
            "uploaded_file_count": len(uploads),
            "skipped_existing_file_count": len(skipped_existing),
            "uploaded_total_bytes": uploaded_total_bytes,
            "skipped_existing_total_bytes": skipped_existing_total_bytes,
            "provider": uploads[0]["provider"] if uploads else _provider_name_for_client(client),
            "resume_scan_required": resume_scan_required,
            "progress_path": str(progress_path),
            "progress": _build_progress_summary(
                requested_file_count=len(transfers),
                transferred_file_count=len(uploads),
                transferred_total_bytes=uploaded_total_bytes,
                skipped_file_count=len(skipped_existing),
                skipped_total_bytes=skipped_existing_total_bytes,
            ),
        }
        publish_summary_path = local_dir / "publish_summary.json"
        publish_summary_path.write_text(json.dumps(publish_summary, ensure_ascii=False, indent=2))
        client.upload_bytes(
            generation_manifest_path.read_bytes(),
            generation_manifest_key,
            content_type="application/json",
        )
        client.upload_bytes(
            publish_summary_path.read_bytes(),
            f"{remote_prefix}/publish_summary.json",
            content_type="application/json",
        )
        self._write_progress_snapshot(
            progress_path,
            {
                **progress_state,
                "status": "uploaded",
                "transferred_file_count": len(uploads),
                "transferred_total_bytes": sum(int(item.get("size_bytes", 0)) for item in uploads),
                "completed_file_count": len(uploads) + len(skipped_existing),
                "remaining_file_count": 0,
                "completion_ratio": 1.0 if transfers else 1.0,
                "updated_at": _utc_now_iso(),
            },
        )
        sync_run = self._record_sync_run(
            action="publish_generation",
            bundle_kind="candidate_generation",
            bundle_id=generation_key,
            summary=publish_summary,
            extra={
                "generation_manifest_path": str(generation_manifest_path),
                "remote_prefix": remote_prefix,
            },
        )
        generation_index_entry = {
            "generation_key": generation_key,
            "generation_sequence": int(manifest_payload.get("generation_sequence") or 0),
            "generation_watermark": str(manifest_payload.get("generation_watermark") or ""),
            "target_company": str(manifest_payload.get("target_company") or ""),
            "company_key": str(manifest_payload.get("company_key") or ""),
            "snapshot_id": str(manifest_payload.get("snapshot_id") or ""),
            "asset_view": str(manifest_payload.get("asset_view") or ""),
            "remote_prefix": remote_prefix,
            "generation_manifest_key": generation_manifest_key,
            "generation_manifest_path": str(generation_manifest_path),
            "provider": publish_summary["provider"],
            "latest_run_id": sync_run["run_id"],
            "last_uploaded_at": sync_run["created_at"],
            "last_uploaded_total_bytes": uploaded_total_bytes,
            "file_count": len(transfers),
        }
        self._update_generation_index(generation_index_entry)
        self._upload_generation_sync_records(client, sync_run, generation_index_entry)
        return {
            **publish_summary,
            "sync_run_id": sync_run["run_id"],
            "generation_manifest_path": str(generation_manifest_path),
        }

    def hydrate_published_generation(
        self,
        *,
        generation_manifest_path: str | Path = "",
        client: ObjectStorageClient | None = None,
        target_company: str = "",
        company_key: str = "",
        snapshot_id: str = "",
        asset_view: str = "canonical_merged",
        generation_key: str = "",
        max_workers: int | None = None,
        resume: bool = True,
        prefer_local_link: bool = True,
    ) -> dict[str, Any]:
        resolved_generation_key = str(generation_key or "").strip()
        local_manifest_path = (
            Path(str(generation_manifest_path or "")).expanduser() if generation_manifest_path else None
        )
        if local_manifest_path is None:
            resolved_snapshot_id = str(snapshot_id or "").strip()
            resolved_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
            resolved_company_key = _normalize_key(company_key or target_company)
            resolved_generation_entry = self.resolve_candidate_generation(
                client=client,
                target_company=target_company,
                company_key=resolved_company_key,
                snapshot_id=resolved_snapshot_id,
                asset_view=resolved_asset_view,
                generation_key=resolved_generation_key,
            )
            if resolved_generation_entry:
                resolved_generation_key = str(
                    resolved_generation_entry.get("generation_key") or resolved_generation_key
                ).strip()
                candidate_manifest_path = Path(
                    str(resolved_generation_entry.get("generation_manifest_path") or "")
                ).expanduser()
                if candidate_manifest_path.exists():
                    local_manifest_path = candidate_manifest_path
                else:
                    if client is None:
                        raise AssetBundleError(
                            "client is required when hydrating a generation without a local manifest cache"
                        )
                    local_dir = self._candidate_generation_local_dir(resolved_generation_key)
                    local_dir.mkdir(parents=True, exist_ok=True)
                    local_manifest_path = local_dir / "generation_manifest.json"
                    remote_manifest_key = str(resolved_generation_entry.get("generation_manifest_key") or "").strip()
                    if not remote_manifest_key:
                        remote_prefix = str(resolved_generation_entry.get("remote_prefix") or "").strip()
                        if not remote_prefix:
                            remote_prefix = self._candidate_generation_remote_prefix(
                                company_key=resolved_company_key,
                                snapshot_id=resolved_snapshot_id,
                                asset_view=resolved_asset_view,
                                generation_key=resolved_generation_key,
                            )
                        remote_manifest_key = f"{remote_prefix}/generation_manifest.json"
                    client.download_file(remote_manifest_key, local_manifest_path)
            else:
                if client is None:
                    raise AssetBundleError(
                        "client is required when hydrating a remote generation without local manifest"
                    )
                if not resolved_company_key or not resolved_snapshot_id or not resolved_generation_key:
                    raise AssetBundleError(
                        "hydrate_published_generation requires company_key/target_company and snapshot_id; "
                        "generation_key becomes optional when a latest generation can be resolved from index metadata"
                    )
                local_dir = self._candidate_generation_local_dir(resolved_generation_key)
                local_dir.mkdir(parents=True, exist_ok=True)
                local_manifest_path = local_dir / "generation_manifest.json"
                remote_prefix = self._candidate_generation_remote_prefix(
                    company_key=resolved_company_key,
                    snapshot_id=resolved_snapshot_id,
                    asset_view=resolved_asset_view,
                    generation_key=resolved_generation_key,
                )
                client.download_file(f"{remote_prefix}/generation_manifest.json", local_manifest_path)
        if not local_manifest_path.exists():
            raise AssetBundleError(f"Generation manifest not found: {local_manifest_path}")
        manifest_payload = json.loads(local_manifest_path.read_text(encoding="utf-8"))
        resolved_generation_key = str(manifest_payload.get("generation_key") or resolved_generation_key).strip()
        resolved_company_key = str(manifest_payload.get("company_key") or company_key or "").strip()
        resolved_snapshot_id = str(manifest_payload.get("snapshot_id") or snapshot_id or "").strip()
        resolved_asset_view = str(manifest_payload.get("asset_view") or asset_view or "").strip() or "canonical_merged"
        if not resolved_generation_key or not resolved_company_key or not resolved_snapshot_id:
            raise AssetBundleError("Generation manifest is missing company_key, snapshot_id, or generation_key")
        hot_snapshot = hot_cache_snapshot_dir(self.runtime_dir, resolved_company_key, resolved_snapshot_id)
        if hot_snapshot is None:
            return {
                "status": "disabled",
                "generation_key": resolved_generation_key,
                "snapshot_id": resolved_snapshot_id,
                "asset_view": resolved_asset_view,
                "snapshot_dir": "",
            }
        local_dir = self._candidate_generation_local_dir(resolved_generation_key)
        local_dir.mkdir(parents=True, exist_ok=True)
        progress_path = local_dir / "hydrate_progress.json"
        hot_snapshot.mkdir(parents=True, exist_ok=True)
        hot_company_dir = hot_snapshot.parent
        hot_company_dir.mkdir(parents=True, exist_ok=True)
        hot_artifact_dir = hot_snapshot / "normalized_artifacts"
        if resolved_asset_view != "canonical_merged":
            hot_artifact_dir = hot_artifact_dir / resolved_asset_view
        hot_artifact_dir.mkdir(parents=True, exist_ok=True)
        file_records = [dict(item) for item in list(manifest_payload.get("files") or []) if isinstance(item, dict)]
        pending_records: list[dict[str, Any]] = []
        skipped_existing: list[dict[str, Any]] = []
        for record in file_records:
            relative_path = str(record.get("relative_path") or "").strip()
            if not relative_path:
                continue
            destination = hot_artifact_dir / relative_path
            if resume and _download_target_matches(destination, record):
                skipped_existing.append(
                    {
                        "relative_path": relative_path,
                        "size_bytes": int(record.get("size_bytes") or 0),
                        "status": "skipped_existing",
                    }
                )
                continue
            pending_records.append(record)
        remote_prefix = str(manifest_payload.get("remote_prefix") or "").strip()
        progress_state = self._build_transfer_progress_state(
            status="running",
            bundle_kind="candidate_generation",
            bundle_id=resolved_generation_key,
            remote_prefix=remote_prefix,
            requested_file_count=len(file_records),
            skipped_existing=skipped_existing,
        )
        progress_state["transfer_mode"] = "generation_hydrate"
        progress_state["snapshot_id"] = resolved_snapshot_id
        progress_state["asset_view"] = resolved_asset_view
        self._write_progress_snapshot(progress_path, progress_state)

        def _hydrate_record(record: dict[str, Any]) -> dict[str, Any]:
            relative_path = str(record.get("relative_path") or "").strip()
            destination = hot_artifact_dir / relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            source_path = Path(str(record.get("source_path") or "")).expanduser()
            if prefer_local_link and source_path.exists() and _download_target_matches(source_path, record):
                sync_mode = materialize_link_first_file(source_path, destination)
                return {
                    "relative_path": relative_path,
                    "size_bytes": int(record.get("size_bytes") or destination.stat().st_size),
                    "status": "linked",
                    "provider": "local",
                    "object_key": str(record.get("object_key") or ""),
                    "object_url": "",
                    "sync_mode": sync_mode,
                }
            if client is None:
                raise AssetBundleError(
                    f"Cannot hydrate {relative_path} without local source file or object storage client"
                )
            object_key = str(record.get("object_key") or "").strip()
            result = self._with_retries(
                lambda: client.download_file(object_key, destination),
                action=f"hydrate generation {relative_path}",
            )
            return {
                "relative_path": relative_path,
                "size_bytes": int(result.get("size_bytes", destination.stat().st_size)),
                **result,
            }

        def _on_hydrate_result(result: dict[str, Any]) -> None:
            self._advance_transfer_progress(
                progress_state,
                requested_file_count=len(file_records),
                result=result,
            )
            self._write_progress_snapshot(progress_path, progress_state)

        hydrated = self._parallel_transfer(
            pending_records,
            max_workers=self._resolve_max_workers(client, max_workers)
            if client is not None
            else max(1, int(max_workers or 8)),
            op=_hydrate_record,
            on_result=_on_hydrate_result,
        )
        company_identity = dict(manifest_payload.get("company_identity") or {})
        (hot_snapshot / "identity.json").write_text(json.dumps(company_identity, ensure_ascii=False, indent=2))
        (hot_company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "company_identity": company_identity,
                    "snapshot_id": resolved_snapshot_id,
                    "snapshot_dir": str(hot_snapshot),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        hydrate_summary = {
            "status": "hydrated",
            "generation_key": resolved_generation_key,
            "target_company": str(manifest_payload.get("target_company") or target_company or ""),
            "company_key": resolved_company_key,
            "snapshot_id": resolved_snapshot_id,
            "asset_view": resolved_asset_view,
            "snapshot_dir": str(hot_snapshot),
            "artifact_dir": str(hot_artifact_dir),
            "generation_manifest_path": str(local_manifest_path),
            "local_generation_dir": str(local_dir),
            "hydrated_file_count": len(hydrated),
            "skipped_existing_file_count": len(skipped_existing),
            "linked_file_count": sum(1 for item in hydrated if str(item.get("status") or "") == "linked"),
            "downloaded_file_count": sum(1 for item in hydrated if str(item.get("status") or "") == "downloaded"),
            "progress_path": str(progress_path),
            "progress": _build_progress_summary(
                requested_file_count=len(file_records),
                transferred_file_count=len(hydrated),
                transferred_total_bytes=sum(int(item.get("size_bytes", 0)) for item in hydrated),
                skipped_file_count=len(skipped_existing),
                skipped_total_bytes=sum(int(item.get("size_bytes", 0)) for item in skipped_existing),
            ),
        }
        hydrate_summary_path = local_dir / "hydrate_summary.json"
        hydrate_summary_path.write_text(json.dumps(hydrate_summary, ensure_ascii=False, indent=2))
        self._write_progress_snapshot(
            progress_path,
            {
                **progress_state,
                "status": "hydrated",
                "transferred_file_count": len(hydrated),
                "transferred_total_bytes": sum(int(item.get("size_bytes", 0)) for item in hydrated),
                "completed_file_count": len(hydrated) + len(skipped_existing),
                "remaining_file_count": 0,
                "completion_ratio": 1.0 if file_records else 1.0,
                "updated_at": _utc_now_iso(),
            },
        )
        sync_run = self._record_sync_run(
            action="hydrate_generation",
            bundle_kind="candidate_generation",
            bundle_id=resolved_generation_key,
            summary=hydrate_summary,
            extra={"generation_manifest_path": str(local_manifest_path)},
        )
        generation_index_entry = {
            "generation_key": resolved_generation_key,
            "generation_sequence": int(manifest_payload.get("generation_sequence") or 0),
            "generation_watermark": str(manifest_payload.get("generation_watermark") or ""),
            "target_company": str(manifest_payload.get("target_company") or ""),
            "company_key": resolved_company_key,
            "snapshot_id": resolved_snapshot_id,
            "asset_view": resolved_asset_view,
            "remote_prefix": remote_prefix,
            "generation_manifest_key": f"{remote_prefix}/generation_manifest.json" if remote_prefix else "",
            "generation_manifest_path": str(local_manifest_path),
            "latest_run_id": sync_run["run_id"],
            "last_hydrated_at": sync_run["created_at"],
            "hot_cache_snapshot_dir": str(hot_snapshot),
            "hydrated_file_count": len(hydrated),
        }
        self._update_generation_index(generation_index_entry)
        return {
            **hydrate_summary,
            "sync_run_id": sync_run["run_id"],
        }

    def resolve_candidate_generation(
        self,
        *,
        client: ObjectStorageClient | None = None,
        target_company: str = "",
        company_key: str = "",
        snapshot_id: str = "",
        asset_view: str = "canonical_merged",
        generation_key: str = "",
    ) -> dict[str, Any]:
        normalized_generation_key = str(generation_key or "").strip()
        normalized_company_key = _normalize_key(company_key or target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"

        local_entry = self._resolve_generation_index_entry(
            entries=list(
                self._load_json_if_exists(self.sync_generation_index_path, {"generations": []}).get("generations") or []
            ),
            company_key=normalized_company_key,
            snapshot_id=normalized_snapshot_id,
            asset_view=normalized_asset_view,
            generation_key=normalized_generation_key,
        )
        if local_entry:
            return {
                **local_entry,
                "resolved_via": "local_generation_index",
            }

        if client is None:
            return {}

        try:
            remote_index_payload = json.loads(client.download_bytes("indexes/generation_index.json").decode("utf-8"))
        except ObjectStorageNotFoundError:
            return {}
        remote_entry = self._resolve_generation_index_entry(
            entries=list(remote_index_payload.get("generations") or []),
            company_key=normalized_company_key,
            snapshot_id=normalized_snapshot_id,
            asset_view=normalized_asset_view,
            generation_key=normalized_generation_key,
        )
        if not remote_entry:
            return {}
        return {
            **remote_entry,
            "resolved_via": "remote_generation_index",
        }

    def _build_candidate_generation_manifest(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        include_compatibility_exports: bool,
    ) -> dict[str, Any]:
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        selection = resolve_company_snapshot_match_selection(
            self.runtime_dir,
            target_company=target_company,
            snapshot_id=snapshot_id,
            prefer_hot_cache=False,
        )
        if selection is None:
            selection = resolve_company_snapshot_match_selection(
                self.runtime_dir,
                target_company=target_company,
                snapshot_id=snapshot_id,
                prefer_hot_cache=True,
            )
        if selection is None:
            raise AssetBundleError(f"Company assets not found for {target_company}")
        snapshot_dir = Path(selection["snapshot_dir"])
        latest_payload = dict(selection.get("latest_payload") or {})
        company_identity = dict(selection.get("identity_payload") or {}) or load_company_snapshot_identity(
            snapshot_dir,
            fallback_payload=latest_payload,
        )
        artifact_dir = snapshot_dir / "normalized_artifacts"
        if normalized_asset_view != "canonical_merged":
            artifact_dir = artifact_dir / normalized_asset_view
        manifest_payload = load_company_snapshot_json(artifact_dir / "manifest.json")
        artifact_summary = load_company_snapshot_json(artifact_dir / "artifact_summary.json")
        if not manifest_payload or not artifact_summary:
            raise AssetBundleError(
                f"Missing manifest-first candidate artifacts for {target_company}:{snapshot_dir.name}:{normalized_asset_view}"
            )
        generation_key = (
            str(
                artifact_summary.get("materialization_generation_key")
                or manifest_payload.get("materialization_generation_key")
                or ""
            ).strip()
            or hashlib.sha1(
                json.dumps(manifest_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest()[:32]
        )
        generation_sequence = int(
            artifact_summary.get("materialization_generation_sequence")
            or manifest_payload.get("materialization_generation_sequence")
            or 0
        )
        generation_watermark = str(
            artifact_summary.get("materialization_watermark") or manifest_payload.get("materialization_watermark") or ""
        ).strip()
        company_key = str(
            company_identity.get("company_key") or selection.get("company_key") or snapshot_dir.parent.name
        ).strip()
        remote_prefix = self._candidate_generation_remote_prefix(
            company_key=company_key,
            snapshot_id=snapshot_dir.name,
            asset_view=normalized_asset_view,
            generation_key=generation_key,
        )
        relative_paths: set[str] = {
            "manifest.json",
            "artifact_summary.json",
            "snapshot_manifest.json",
            "publishable_primary_emails.json",
            "manual_review_backlog.json",
            "profile_completion_backlog.json",
            "organization_completeness_ledger.json",
        }
        for entry in list(manifest_payload.get("candidate_shards") or []):
            path = str(dict(entry).get("path") or "").strip()
            if path:
                relative_paths.add(path)
        for entry in list(manifest_payload.get("pages") or []):
            path = str(dict(entry).get("path") or "").strip()
            if path:
                relative_paths.add(path)
        for item in dict(manifest_payload.get("backlogs") or {}).values():
            path = str(item or "").strip()
            if path:
                relative_paths.add(path)
        for item in dict(manifest_payload.get("auxiliary") or {}).values():
            path = str(item or "").strip()
            if path:
                relative_paths.add(path)
        if include_compatibility_exports:
            relative_paths.update(
                {
                    "materialized_candidate_documents.json",
                    "normalized_candidates.json",
                    "reusable_candidate_documents.json",
                }
            )
        file_records: list[dict[str, Any]] = []
        for relative_path in sorted(relative_paths):
            local_path = artifact_dir / relative_path
            if not local_path.exists():
                continue
            file_records.append(
                {
                    "relative_path": relative_path,
                    "runtime_relative_path": self._runtime_relative_path(local_path).as_posix(),
                    "source_path": str(local_path),
                    "size_bytes": local_path.stat().st_size,
                    "sha256": _sha256(local_path),
                    "content_type": _content_type_for_path(local_path),
                    "object_key": f"{remote_prefix}/{relative_path}",
                }
            )
        return {
            "manifest_version": 1,
            "manifest_kind": "candidate_generation",
            "published_at": _utc_now_iso(),
            "target_company": str(company_identity.get("canonical_name") or target_company).strip() or target_company,
            "company_key": company_key,
            "snapshot_id": snapshot_dir.name,
            "asset_view": normalized_asset_view,
            "generation_key": generation_key,
            "generation_sequence": generation_sequence,
            "generation_watermark": generation_watermark,
            "remote_prefix": remote_prefix,
            "artifact_dir": str(artifact_dir),
            "company_identity": company_identity,
            "summary": artifact_summary,
            "manifest": manifest_payload,
            "files": file_records,
        }

    def _candidate_generation_local_dir(self, generation_key: str) -> Path:
        return self.sync_generations_dir / str(generation_key or "").strip()

    def _candidate_generation_remote_prefix(
        self,
        *,
        company_key: str,
        snapshot_id: str,
        asset_view: str,
        generation_key: str,
    ) -> str:
        return (
            f"generations/{_normalize_key(company_key)}/"
            f"{str(snapshot_id or '').strip()}/"
            f"{_safe_token(asset_view)}/"
            f"{str(generation_key or '').strip()}"
        )

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
            sequential_results: list[dict] = []
            for item in items:
                result = op(item)
                sequential_results.append(result)
                if on_result is not None:
                    on_result(result)
            return sorted(
                sequential_results,
                key=lambda entry: str(entry.get("relative_path") or entry.get("object_key") or ""),
            )
        results: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(op, item): item for item in items}
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if on_result is not None:
                    on_result(result)
        return sorted(results, key=lambda entry: str(entry.get("relative_path") or entry.get("object_key") or ""))

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

    def _update_generation_index(self, entry: dict[str, Any]) -> None:
        self.sync_dir.mkdir(parents=True, exist_ok=True)
        payload = self._load_json_if_exists(self.sync_generation_index_path, {"updated_at": "", "generations": []})
        generations = {
            str(item.get("generation_key", "")).strip(): dict(item)
            for item in payload.get("generations", [])
            if str(item.get("generation_key", "")).strip()
        }
        generation_key = str(entry.get("generation_key", "")).strip()
        if not generation_key:
            return
        merged = dict(generations.get(generation_key, {}))
        merged.update(entry)
        generations[generation_key] = merged
        updated_payload = {
            "updated_at": _utc_now_iso(),
            "generations": sorted(
                generations.values(),
                key=lambda item: (
                    str(item.get("company_key", "")).strip(),
                    str(item.get("snapshot_id", "")).strip(),
                    str(item.get("asset_view", "")).strip(),
                    str(item.get("generation_key", "")).strip(),
                ),
            ),
        }
        self.sync_generation_index_path.write_text(json.dumps(updated_payload, ensure_ascii=False, indent=2))

    def _resolve_generation_index_entry(
        self,
        *,
        entries: list[Any],
        company_key: str,
        snapshot_id: str,
        asset_view: str,
        generation_key: str,
    ) -> dict[str, Any]:
        normalized_generation_key = str(generation_key or "").strip()
        candidates: list[dict[str, Any]] = []
        for item in entries:
            if not isinstance(item, dict):
                continue
            candidate_generation_key = str(item.get("generation_key") or "").strip()
            if not candidate_generation_key:
                continue
            if normalized_generation_key and candidate_generation_key != normalized_generation_key:
                continue
            if company_key and str(item.get("company_key") or "").strip() != company_key:
                continue
            if snapshot_id and str(item.get("snapshot_id") or "").strip() != snapshot_id:
                continue
            candidate_asset_view = str(item.get("asset_view") or "").strip() or "canonical_merged"
            if asset_view and candidate_asset_view != asset_view:
                continue
            candidates.append(dict(item))
        if not candidates:
            return {}
        candidates.sort(
            key=lambda item: (
                int(item.get("generation_sequence") or 0),
                str(item.get("last_uploaded_at") or item.get("last_hydrated_at") or ""),
                str(item.get("generation_key") or ""),
            ),
            reverse=True,
        )
        return candidates[0]

    def _upload_sync_records(
        self,
        client: ObjectStorageClient,
        sync_run: dict[str, Any],
        bundle_index_entry: dict[str, Any],
    ) -> None:
        run_key = f"indexes/sync_runs/{sync_run['run_id']}.json"
        client.upload_bytes(
            json.dumps(sync_run, ensure_ascii=False, indent=2).encode("utf-8"), run_key, content_type="application/json"
        )
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

    def _upload_generation_sync_records(
        self,
        client: ObjectStorageClient,
        sync_run: dict[str, Any],
        generation_index_entry: dict[str, Any],
    ) -> None:
        run_key = f"indexes/sync_runs/{sync_run['run_id']}.json"
        client.upload_bytes(
            json.dumps(sync_run, ensure_ascii=False, indent=2).encode("utf-8"),
            run_key,
            content_type="application/json",
        )
        try:
            remote_index_payload = json.loads(client.download_bytes("indexes/generation_index.json").decode("utf-8"))
        except ObjectStorageNotFoundError:
            remote_index_payload = {"updated_at": "", "generations": []}
        generations = {
            str(item.get("generation_key", "")).strip(): dict(item)
            for item in remote_index_payload.get("generations", [])
            if str(item.get("generation_key", "")).strip()
        }
        generation_key = str(generation_index_entry.get("generation_key", "")).strip()
        if not generation_key:
            return
        merged = dict(generations.get(generation_key, {}))
        merged.update(generation_index_entry)
        merged["latest_remote_run_id"] = sync_run["run_id"]
        generations[generation_key] = merged
        updated_payload = {
            "updated_at": _utc_now_iso(),
            "generations": sorted(
                generations.values(),
                key=lambda item: (
                    str(item.get("company_key", "")).strip(),
                    str(item.get("snapshot_id", "")).strip(),
                    str(item.get("asset_view", "")).strip(),
                    str(item.get("generation_key", "")).strip(),
                ),
            ),
        }
        client.upload_bytes(
            json.dumps(updated_payload, ensure_ascii=False, indent=2).encode("utf-8"),
            "indexes/generation_index.json",
            content_type="application/json",
        )

    def _upload_bundle_delete_records(
        self,
        client: ObjectStorageClient,
        *,
        sync_run: dict[str, Any],
        bundle_kind: str,
        bundle_id: str,
    ) -> None:
        run_key = f"indexes/sync_runs/{sync_run['run_id']}.json"
        client.upload_bytes(
            json.dumps(sync_run, ensure_ascii=False, indent=2).encode("utf-8"), run_key, content_type="application/json"
        )
        try:
            remote_index_payload = json.loads(client.download_bytes("indexes/bundle_index.json").decode("utf-8"))
        except ObjectStorageNotFoundError:
            remote_index_payload = {"updated_at": "", "bundles": []}
        bundles = [
            dict(item)
            for item in remote_index_payload.get("bundles", [])
            if not (
                str(item.get("bundle_kind", "")).strip() == bundle_kind
                and str(item.get("bundle_id", "")).strip() == bundle_id
            )
        ]
        updated_payload = {
            "updated_at": _utc_now_iso(),
            "bundles": sorted(
                bundles,
                key=lambda item: (
                    str(item.get("bundle_kind", "")).strip(),
                    str(item.get("bundle_id", "")).strip(),
                ),
            ),
        }
        client.upload_bytes(
            json.dumps(updated_payload, ensure_ascii=False, indent=2).encode("utf-8"),
            "indexes/bundle_index.json",
            content_type="application/json",
        )

    def _remove_bundle_index_entry(self, bundle_kind: str, bundle_id: str) -> None:
        self.sync_dir.mkdir(parents=True, exist_ok=True)
        payload = self._load_json_if_exists(self.sync_bundle_index_path, {"updated_at": "", "bundles": []})
        bundles = [
            dict(item)
            for item in payload.get("bundles", [])
            if not (
                str(item.get("bundle_kind", "")).strip() == bundle_kind
                and str(item.get("bundle_id", "")).strip() == bundle_id
            )
        ]
        updated_payload = {
            "updated_at": _utc_now_iso(),
            "bundles": sorted(
                bundles,
                key=lambda item: (
                    str(item.get("bundle_kind", "")).strip(),
                    str(item.get("bundle_id", "")).strip(),
                ),
            ),
        }
        self.sync_bundle_index_path.write_text(json.dumps(updated_payload, ensure_ascii=False, indent=2))

    def _bundle_object_keys_from_manifest(
        self,
        *,
        bundle_kind: str,
        bundle_id: str,
        manifest_payload: dict[str, Any] | None,
    ) -> list[str]:
        remote_prefix = self._bundle_remote_prefix(bundle_kind, bundle_id)
        object_keys = {
            f"{remote_prefix}/bundle_manifest.json",
            f"{remote_prefix}/export_summary.json",
        }
        payload = dict(manifest_payload or {})
        archive_entry = dict(payload.get("archive") or {})
        archive_relative_path = str(archive_entry.get("relative_path") or "").strip()
        if archive_relative_path:
            object_keys.add(f"{remote_prefix}/{archive_relative_path}")
        else:
            for record in list(payload.get("files") or []):
                relative_path = str(dict(record).get("payload_relative_path") or "").strip()
                if relative_path:
                    object_keys.add(f"{remote_prefix}/{relative_path}")
        return sorted(object_keys)

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
        progress_state["transferred_total_bytes"] = int(progress_state.get("transferred_total_bytes", 0)) + int(
            result.get("size_bytes", 0)
        )
        progress_state["completed_file_count"] = int(progress_state.get("completed_file_count", 0)) + 1
        progress_state["remaining_file_count"] = max(
            requested_file_count - int(progress_state["completed_file_count"]), 0
        )
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
        self._write_bundle_archive(
            bundle_root=bundle_root, payload_dir=payload_dir, archive_path=temp_archive_path, archive_mode=archive_mode
        )
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

    def _ensure_bundle_payload_materialized(
        self, bundle_root: Path, manifest_payload: dict[str, Any]
    ) -> dict[str, Any]:
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
        if suffixes[-2:] == [".tar", ".gz"]:
            tar_handle_context = tarfile.open(archive_path, mode="r:gz")
        else:
            tar_handle_context = tarfile.open(archive_path, mode="r")
        with tar_handle_context as tar_handle:
            members = tar_handle.getmembers()
            for member in members:
                target = (bundle_root / member.name).resolve()
                bundle_root_resolved = bundle_root.resolve()
                if bundle_root_resolved not in {target, *target.parents}:
                    raise AssetBundleError(f"Archive member escapes bundle root: {member.name}")
            tar_handle.extractall(bundle_root, filter="data")

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
            source = self._resolve_source_path_for_runtime_relative(relpath)
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
        normalized_target = _normalize_key(company)
        for candidate in iter_company_asset_company_dirs(
            self.runtime_dir,
            prefer_hot_cache=True,
            existing_only=True,
        ):
            if _normalize_key(candidate.name) == normalized_target:
                return candidate.name, candidate
            latest_path = candidate / "latest_snapshot.json"
            if not latest_path.exists():
                continue
            try:
                payload = json.loads(latest_path.read_text())
            except Exception:
                continue
            identity = dict(payload.get("company_identity") or {})
            alias_candidates = [
                candidate.name,
                identity.get("requested_name"),
                identity.get("canonical_name"),
                identity.get("linkedin_slug"),
                identity.get("domain"),
                *list(identity.get("aliases") or []),
            ]
            if any(_normalize_key(str(item or "")) == normalized_target for item in alias_candidates):
                return candidate.name, candidate
        raise AssetBundleError(f"Company assets not found for {company}")

    def _candidate_generation_metadata_for_snapshot(
        self, snapshot_dir: Path, *, asset_view: str = "canonical_merged"
    ) -> dict[str, Any]:
        artifact_dir = snapshot_dir / "normalized_artifacts"
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        if normalized_asset_view != "canonical_merged":
            artifact_dir = artifact_dir / normalized_asset_view
        manifest_payload = load_company_snapshot_json(artifact_dir / "manifest.json")
        artifact_summary = load_company_snapshot_json(artifact_dir / "artifact_summary.json")
        generation_key = str(
            artifact_summary.get("materialization_generation_key")
            or manifest_payload.get("materialization_generation_key")
            or ""
        ).strip()
        if not generation_key:
            return {}
        return {
            "asset_view": normalized_asset_view,
            "preferred_generation_key": generation_key,
            "materialization_generation_key": generation_key,
            "materialization_generation_sequence": int(
                artifact_summary.get("materialization_generation_sequence")
                or manifest_payload.get("materialization_generation_sequence")
                or 0
            ),
            "materialization_generation_watermark": str(
                artifact_summary.get("materialization_watermark")
                or manifest_payload.get("materialization_watermark")
                or ""
            ).strip(),
        }

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

    def _runtime_relative_path(self, path: Path) -> Path:
        resolved_path = path.resolve()
        try:
            return resolved_path.relative_to(self.runtime_dir.resolve())
        except ValueError:
            for assets_root in company_assets_roots(
                self.runtime_dir,
                prefer_hot_cache=True,
                existing_only=False,
            ):
                try:
                    return Path("company_assets") / resolved_path.relative_to(assets_root)
                except ValueError:
                    continue
            raise AssetBundleError(
                f"Unsupported bundle source path outside runtime/company asset roots: {resolved_path}"
            )

    def _resolve_source_path_for_runtime_relative(self, relative_path: Path) -> Path:
        relpath = Path(relative_path)
        parts = relpath.parts
        if parts and parts[0] == "company_assets":
            company_relative = Path(*parts[1:])
            for assets_root in company_assets_roots(
                self.runtime_dir,
                prefer_hot_cache=True,
                existing_only=False,
            ):
                candidate = assets_root / company_relative
                if candidate.exists():
                    return candidate
            return canonical_company_assets_dir(self.runtime_dir) / company_relative
        return self.runtime_dir / relpath

    def _collect_files(self, relative_dir: Path) -> list[Path]:
        root = relative_dir if relative_dir.is_absolute() else (self.runtime_dir / relative_dir)
        if not root.exists():
            return []
        return [self._runtime_relative_path(path) for path in root.rglob("*") if path.is_file()]

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
            if destination.stat().st_size != int(str(expected_size)):
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
