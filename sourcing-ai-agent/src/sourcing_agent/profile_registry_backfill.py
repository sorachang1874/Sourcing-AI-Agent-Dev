from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

from .profile_registry_utils import (
    classify_harvest_profile_payload_status,
    extract_profile_registry_aliases_from_payload,
)
from .storage import SQLiteStore


ProgressCallback = Callable[[dict[str, Any]], None]


@dataclass(slots=True)
class BackfillScope:
    company: str = ""
    snapshot_id: str = ""

    @property
    def run_key(self) -> str:
        company = str(self.company or "*").strip() or "*"
        snapshot = str(self.snapshot_id or "*").strip() or "*"
        return f"linkedin_profile_registry_backfill::{company}::{snapshot}"


def backfill_linkedin_profile_registry(
    *,
    runtime_dir: Path,
    store: SQLiteStore,
    company: str = "",
    snapshot_id: str = "",
    resume: bool = True,
    checkpoint_path: Path | None = None,
    progress_interval: int = 200,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    scope = BackfillScope(company=str(company or "").strip(), snapshot_id=str(snapshot_id or "").strip())
    progress_every = max(1, int(progress_interval or 1))
    started_at = datetime.now(timezone.utc)
    all_files = list(_iter_harvest_profile_files(runtime_dir=runtime_dir, scope=scope))
    total_files = len(all_files)

    persisted_run = store.get_linkedin_profile_registry_backfill_run(scope.run_key) if resume else None
    checkpoint = dict((persisted_run or {}).get("checkpoint") or {})
    summary = dict((persisted_run or {}).get("summary") or {})
    last_processed_path = str(checkpoint.get("last_processed_path") or "").strip() if resume else ""

    files_to_process = [path for path in all_files if not last_processed_path or str(path) > last_processed_path]

    counters = {
        "processed": 0,
        "fetched": 0,
        "failed_retryable": 0,
        "unrecoverable": 0,
        "skipped_no_url": 0,
        "skipped_unsupported": 0,
        "decode_error": 0,
        "errors": 0,
    }
    cumulative_processed = int(summary.get("processed_total") or 0)
    cumulative_fetched = int(summary.get("fetched_total") or 0)
    cumulative_failed_retryable = int(summary.get("failed_retryable_total") or 0)
    cumulative_unrecoverable = int(summary.get("unrecoverable_total") or 0)
    cumulative_skipped_no_url = int(summary.get("skipped_no_url_total") or 0)
    cumulative_skipped_unsupported = int(summary.get("skipped_unsupported_total") or 0)
    cumulative_decode_error = int(summary.get("decode_error_total") or 0)
    cumulative_errors = int(summary.get("errors_total") or 0)

    if checkpoint_path is None:
        checkpoint_dir = runtime_dir / "maintenance" / "checkpoints"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_path = checkpoint_dir / f"{scope.run_key.replace(':', '_')}.json"

    def _emit_progress(current_path: Path | None = None, *, final: bool = False) -> None:
        nonlocal cumulative_processed, cumulative_fetched, cumulative_failed_retryable
        nonlocal cumulative_unrecoverable, cumulative_skipped_no_url, cumulative_skipped_unsupported
        nonlocal cumulative_decode_error, cumulative_errors
        checkpoint_payload = {
            "run_key": scope.run_key,
            "scope_company": scope.company,
            "scope_snapshot_id": scope.snapshot_id,
            "last_processed_path": str(current_path or checkpoint.get("last_processed_path") or ""),
            "processed_this_run": counters["processed"],
            "processed_total": cumulative_processed + counters["processed"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        summary_payload = {
            "processed_total": cumulative_processed + counters["processed"],
            "fetched_total": cumulative_fetched + counters["fetched"],
            "failed_retryable_total": cumulative_failed_retryable + counters["failed_retryable"],
            "unrecoverable_total": cumulative_unrecoverable + counters["unrecoverable"],
            "skipped_no_url_total": cumulative_skipped_no_url + counters["skipped_no_url"],
            "skipped_unsupported_total": cumulative_skipped_unsupported + counters["skipped_unsupported"],
            "decode_error_total": cumulative_decode_error + counters["decode_error"],
            "errors_total": cumulative_errors + counters["errors"],
            "total_files_in_scope": total_files,
            "remaining_files": max(0, total_files - (cumulative_processed + counters["processed"])),
        }
        store.upsert_linkedin_profile_registry_backfill_run(
            scope.run_key,
            scope_company=scope.company,
            scope_snapshot_id=scope.snapshot_id,
            checkpoint=checkpoint_payload,
            summary=summary_payload,
            status="completed" if final else "running",
        )
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint_path.write_text(
            json.dumps(
                {
                    "checkpoint": checkpoint_payload,
                    "summary": summary_payload,
                    "status": "completed" if final else "running",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        if progress_callback is not None:
            progress_callback(
                {
                    "run_key": scope.run_key,
                    "current_file": str(current_path or ""),
                    "processed_this_run": counters["processed"],
                    "processed_total": summary_payload["processed_total"],
                    "remaining_files": summary_payload["remaining_files"],
                    "fetched_total": summary_payload["fetched_total"],
                    "unrecoverable_total": summary_payload["unrecoverable_total"],
                    "status": "completed" if final else "running",
                }
            )

    for index, path in enumerate(files_to_process, start=1):
        counters["processed"] += 1
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            counters["decode_error"] += 1
            counters["errors"] += 1
            if counters["processed"] % progress_every == 0:
                _emit_progress(path)
            continue
        if not isinstance(payload, dict):
            counters["skipped_unsupported"] += 1
            if counters["processed"] % progress_every == 0:
                _emit_progress(path)
            continue
        alias_metadata = extract_profile_registry_aliases_from_payload(payload)
        alias_urls = list(alias_metadata.get("alias_urls") or [])
        profile_url = str(
            alias_metadata.get("sanity_linkedin_url")
            or alias_metadata.get("raw_linkedin_url")
            or (alias_urls[0] if alias_urls else "")
        ).strip()
        if not profile_url:
            counters["skipped_no_url"] += 1
            if counters["processed"] % progress_every == 0:
                _emit_progress(path)
            continue
        status = classify_harvest_profile_payload_status(payload)
        source_shards = _profile_registry_backfill_source_shards(path)
        source_jobs = [scope.run_key]
        snapshot_dir = _snapshot_dir_from_harvest_path(path)
        if status == "fetched":
            store.mark_linkedin_profile_registry_fetched(
                profile_url,
                raw_path=str(path),
                source_shards=source_shards,
                source_jobs=source_jobs,
                alias_urls=alias_urls,
                raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or profile_url),
                sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                snapshot_dir=snapshot_dir,
            )
            counters["fetched"] += 1
        elif status == "unrecoverable":
            store.mark_linkedin_profile_registry_failed(
                profile_url,
                error="backfill_unrecoverable_harvest_payload",
                retryable=False,
                source_shards=source_shards,
                source_jobs=source_jobs,
                alias_urls=alias_urls,
                raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or profile_url),
                sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                snapshot_dir=snapshot_dir,
            )
            counters["unrecoverable"] += 1
        else:
            store.mark_linkedin_profile_registry_failed(
                profile_url,
                error="backfill_retryable_harvest_payload",
                retryable=True,
                source_shards=source_shards,
                source_jobs=source_jobs,
                alias_urls=alias_urls,
                raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or profile_url),
                sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                snapshot_dir=snapshot_dir,
            )
            counters["failed_retryable"] += 1

        if counters["processed"] % progress_every == 0:
            _emit_progress(path)

    _emit_progress(files_to_process[-1] if files_to_process else None, final=True)

    completed_at = datetime.now(timezone.utc)
    result = {
        "status": "completed",
        "run_key": scope.run_key,
        "scope": {"company": scope.company, "snapshot_id": scope.snapshot_id},
        "checkpoint_path": str(checkpoint_path),
        "total_files_in_scope": total_files,
        "files_processed_this_run": counters["processed"],
        "files_remaining_after_run": max(0, total_files - (counters["processed"] + cumulative_processed)),
        "counters_this_run": counters,
        "counters_total": {
            "processed": cumulative_processed + counters["processed"],
            "fetched": cumulative_fetched + counters["fetched"],
            "failed_retryable": cumulative_failed_retryable + counters["failed_retryable"],
            "unrecoverable": cumulative_unrecoverable + counters["unrecoverable"],
            "skipped_no_url": cumulative_skipped_no_url + counters["skipped_no_url"],
            "skipped_unsupported": cumulative_skipped_unsupported + counters["skipped_unsupported"],
            "decode_error": cumulative_decode_error + counters["decode_error"],
            "errors": cumulative_errors + counters["errors"],
        },
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": round((completed_at - started_at).total_seconds(), 3),
    }
    return result


def _iter_harvest_profile_files(*, runtime_dir: Path, scope: BackfillScope) -> list[Path]:
    company_assets_dir = runtime_dir / "company_assets"
    if not company_assets_dir.exists():
        return []
    candidate_paths: list[Path] = []
    normalized_company = str(scope.company or "").strip().lower()
    normalized_snapshot_id = str(scope.snapshot_id or "").strip()
    for company_dir in sorted(company_assets_dir.iterdir()):
        if not company_dir.is_dir():
            continue
        company_key = company_dir.name.strip().lower()
        if normalized_company and company_key != normalized_company:
            continue
        for snapshot_dir in sorted(company_dir.iterdir()):
            if not snapshot_dir.is_dir():
                continue
            snapshot_name = snapshot_dir.name.strip()
            if normalized_snapshot_id and snapshot_name != normalized_snapshot_id:
                continue
            harvest_dir = snapshot_dir / "harvest_profiles"
            if not harvest_dir.exists():
                continue
            for path in sorted(harvest_dir.glob("*.json")):
                filename = path.name
                if filename.endswith(".request.json"):
                    continue
                if filename.startswith("harvest_profile_batch_"):
                    continue
                candidate_paths.append(path)
    return candidate_paths


def _profile_registry_backfill_source_shards(path: Path) -> list[str]:
    snapshot_dir = _snapshot_dir_from_harvest_path(path)
    if not snapshot_dir:
        return ["backfill:unknown"]
    normalized = snapshot_dir.replace("\\", "/")
    parts = [segment for segment in normalized.split("/") if segment]
    company_key = ""
    snapshot_id = ""
    for index, segment in enumerate(parts):
        if segment == "company_assets" and index + 2 < len(parts):
            company_key = parts[index + 1]
            snapshot_id = parts[index + 2]
            break
    labels = ["backfill:linkedin_profile_registry"]
    if company_key:
        labels.append(f"company:{company_key}")
    if snapshot_id:
        labels.append(f"snapshot:{snapshot_id}")
    return labels


def _snapshot_dir_from_harvest_path(path: Path) -> str:
    parent = path.parent
    if parent.name == "harvest_profiles":
        return str(parent.parent)
    return ""
