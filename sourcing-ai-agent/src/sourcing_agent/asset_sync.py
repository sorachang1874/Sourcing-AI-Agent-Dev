from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import shutil
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
        remote_prefix = self._bundle_remote_prefix(bundle_kind, bundle_id)
        upload_paths = [bundle_root / "bundle_manifest.json", bundle_root / "export_summary.json"]
        for record in payload.get("files", []):
            upload_paths.append(bundle_root / record["payload_relative_path"])
        resolved_max_workers = self._resolve_max_workers(client, max_workers)
        transfers = [
            {"path": path, "relative_path": path.relative_to(bundle_root).as_posix()}
            for path in upload_paths
            if path.exists() and path.is_file()
        ]
        uploads = self._parallel_transfer(
            transfers,
            max_workers=resolved_max_workers,
            op=lambda item: self._upload_transfer_item(client, remote_prefix, item),
        )
        total_bytes = sum(int(item.get("size_bytes", 0)) for item in uploads)
        summary = {
            "status": "uploaded",
            "bundle_kind": bundle_kind,
            "bundle_id": bundle_id,
            "remote_prefix": remote_prefix,
            "remote_manifest_key": f"{remote_prefix}/bundle_manifest.json",
            "uploaded_file_count": len(uploads),
            "uploaded_total_bytes": total_bytes,
            "provider": uploads[0]["provider"] if uploads else "",
            "max_workers": resolved_max_workers,
            "object_urls_sample": [item.get("object_url", "") for item in uploads[:5]],
        }
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
            "remote_prefix": remote_prefix,
            "remote_manifest_key": f"{remote_prefix}/bundle_manifest.json",
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
        transfers = [
            {"relative_path": record["payload_relative_path"], "path": bundle_dir / record["payload_relative_path"]}
            for record in payload.get("files", [])
        ]
        results = self._parallel_transfer(
            transfers,
            max_workers=resolved_max_workers,
            op=lambda item: self._download_transfer_item(client, remote_prefix, item),
        )
        downloaded += len(results)
        total_bytes += sum(int(item.get("size_bytes", 0)) for item in results)
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
        }
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
    ) -> list[dict]:
        if not items:
            return []
        if max_workers <= 1 or len(items) == 1:
            return sorted((op(item) for item in items), key=lambda entry: entry["relative_path"])
        results: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(op, item): item for item in items}
            for future in as_completed(futures):
                results.append(future.result())
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
    suffix = path.suffix.lower()
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
