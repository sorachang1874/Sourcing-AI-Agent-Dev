from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

from .asset_registration import sync_company_asset_registration
from .asset_sync import AssetBundleError, AssetBundleManager
from .candidate_artifacts import repair_missing_company_candidate_artifacts
from .company_registry import refresh_company_identity_registry
from .control_plane_postgres import (
    control_plane_snapshot_output_path,
    sync_control_plane_snapshot_to_postgres,
)
from .local_postgres import resolve_control_plane_postgres_dsn, resolve_default_control_plane_db_path
from .profile_registry_backfill import backfill_linkedin_profile_registry
from .storage import ControlPlaneStore


_RETIRED_SQLITE_BUNDLE_KIND = "sqlite_snapshot"


def _reject_retired_sqlite_bundle_kind(bundle_kind: str) -> None:
    if str(bundle_kind or "").strip() == _RETIRED_SQLITE_BUNDLE_KIND:
        raise AssetBundleError(
            "sqlite_snapshot import/restore has been retired. Use control_plane_snapshot for PG control-plane "
            "state or candidate_generation/company_snapshot for portable candidate assets."
        )


def _normalize_string_list(values: list[Any] | tuple[Any, ...] | set[Any] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = str(item or "").strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(text)
    return normalized


def _sync_restored_control_plane_snapshot(runtime_dir: Path) -> dict[str, Any]:
    snapshot_path = control_plane_snapshot_output_path(runtime_dir)
    if not snapshot_path.exists():
        return {
            "status": "skipped_missing_snapshot",
            "snapshot_path": str(snapshot_path),
        }
    dsn = resolve_control_plane_postgres_dsn(runtime_dir)
    if not dsn:
        return {
            "status": "failed_missing_postgres_dsn",
            "snapshot_path": str(snapshot_path),
            "error": "Postgres DSN is required to restore a control_plane_snapshot.",
        }
    try:
        summary = sync_control_plane_snapshot_to_postgres(
            snapshot_path=snapshot_path,
            dsn=dsn,
            truncate_first=False,
            validate_postgres=False,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "snapshot_path": str(snapshot_path),
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {
        "status": "completed",
        "snapshot_path": str(snapshot_path),
        "sync": summary,
    }


def _manifest_company_keys(manifest_payload: dict[str, Any]) -> list[str]:
    metadata = dict(manifest_payload.get("metadata") or {})
    company_keys = _normalize_string_list(
        [
            metadata.get("company_key"),
            metadata.get("company"),
        ]
    )
    for entry in list(manifest_payload.get("files") or []):
        if not isinstance(entry, dict):
            continue
        runtime_relative_path = str(entry.get("runtime_relative_path") or "").strip()
        parts = Path(runtime_relative_path).parts
        if len(parts) >= 2 and parts[0] == "company_assets":
            company_keys = _normalize_string_list([*company_keys, parts[1]])
    return company_keys


def _manifest_snapshot_id(manifest_payload: dict[str, Any], explicit_snapshot_id: str = "") -> str:
    if str(explicit_snapshot_id or "").strip():
        return str(explicit_snapshot_id or "").strip()
    metadata = dict(manifest_payload.get("metadata") or {})
    for candidate in [
        metadata.get("snapshot_id"),
        metadata.get("latest_snapshot_id"),
    ]:
        text = str(candidate or "").strip()
        if text:
            return text
    return ""


def _generation_import_hint_from_manifest(
    manifest_payload: dict[str, Any],
    *,
    requested_companies: list[str],
    requested_snapshot_id: str,
    requested_generation_key: str,
    asset_view: str,
) -> dict[str, str]:
    bundle_kind = str(manifest_payload.get("bundle_kind") or "").strip()
    metadata = dict(manifest_payload.get("metadata") or {})
    requested_target_company = requested_companies[0] if requested_companies else ""
    target_company = requested_target_company
    company_key = str(metadata.get("company_key") or "").strip()
    snapshot_id = str(requested_snapshot_id or "").strip()
    generation_key = str(requested_generation_key or "").strip()
    resolved_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"

    if bundle_kind == "company_snapshot":
        if not target_company:
            target_company = str(metadata.get("company") or metadata.get("target_company") or "").strip()
        snapshot_id = str(
            snapshot_id or metadata.get("snapshot_id") or metadata.get("latest_snapshot_id") or ""
        ).strip()
        generation_key = str(
            generation_key
            or metadata.get("preferred_generation_key")
            or metadata.get("materialization_generation_key")
            or ""
        ).strip()
        resolved_asset_view = (
            str(metadata.get("asset_view") or resolved_asset_view or "canonical_merged").strip() or "canonical_merged"
        )
    elif bundle_kind == "company_handoff":
        latest_generation = dict(metadata.get("latest_candidate_generation") or {})
        if not target_company:
            target_company = str(metadata.get("company") or "").strip()
        snapshot_id = str(
            snapshot_id or metadata.get("latest_snapshot_id") or metadata.get("snapshot_id") or ""
        ).strip()
        generation_key = str(
            generation_key
            or latest_generation.get("preferred_generation_key")
            or latest_generation.get("materialization_generation_key")
            or ""
        ).strip()
        resolved_asset_view = (
            str(latest_generation.get("asset_view") or resolved_asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
    elif bundle_kind == "control_plane_snapshot":
        target_company = requested_target_company
    else:
        return {}

    if not generation_key and not (target_company or company_key or snapshot_id):
        return {}
    return {
        "target_company": target_company,
        "company_key": company_key,
        "snapshot_id": snapshot_id,
        "generation_key": generation_key,
        "asset_view": resolved_asset_view,
    }


def _direct_generation_import_hint(
    *,
    bundle_kind: str,
    requested_companies: list[str],
    requested_snapshot_id: str,
    requested_generation_key: str,
    asset_view: str,
) -> dict[str, str]:
    normalized_bundle_kind = str(bundle_kind or "").strip()
    if normalized_bundle_kind not in {
        "",
        "company_snapshot",
        "company_handoff",
        "control_plane_snapshot",
    }:
        return {}
    target_company = requested_companies[0] if requested_companies else ""
    snapshot_id = str(requested_snapshot_id or "").strip()
    generation_key = str(requested_generation_key or "").strip()
    if not generation_key and not (target_company or snapshot_id):
        return {}
    return {
        "target_company": target_company,
        "company_key": "",
        "snapshot_id": snapshot_id,
        "generation_key": generation_key,
        "asset_view": str(asset_view or "canonical_merged").strip() or "canonical_merged",
    }


def _annotate_generation_first_import(
    summary: dict[str, Any],
    *,
    source: str,
    bundle_kind: str = "",
    bundle_id: str = "",
    resolved_generation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    annotated = dict(summary)
    normalized_bundle_kind = str(bundle_kind or "").strip()
    normalized_bundle_id = str(bundle_id or "").strip()
    if normalized_bundle_kind:
        annotated["requested_bundle_kind"] = normalized_bundle_kind
    if normalized_bundle_id:
        annotated["requested_bundle_id"] = normalized_bundle_id
    resolved = dict(resolved_generation or {})
    annotated["generation_first_attempt"] = {
        "status": "resolved",
        "source": str(source or "").strip(),
        "bundle_kind": normalized_bundle_kind,
        "bundle_id": normalized_bundle_id,
        "generation_key": str(resolved.get("generation_key") or ""),
        "resolved_via": str(resolved.get("resolved_via") or ""),
    }
    return annotated


def _warmup_company_assets(
    *,
    runtime_dir: Path,
    store: ControlPlaneStore,
    company: str,
    asset_view: str,
) -> dict[str, Any]:
    registration_result = sync_company_asset_registration(
        runtime_dir=runtime_dir,
        store=store,
        target_company=company,
        snapshot_id="",
        asset_view=asset_view,
        registry_refresh_mode="backfill",
        refresh_company_identity=False,
    )
    sync_status = dict(registration_result.get("sync_status") or {})
    backfill_result = dict(sync_status.get("organization_asset_registry_refresh", {}).get("result") or {})
    selected_snapshot_ids = _normalize_string_list(registration_result.get("selected_snapshot_ids") or [])
    registry_entries = list(dict(sync_status.get("acquisition_shard_registry_refresh") or {}).get("results") or [])
    bundle_entries = list(dict(sync_status.get("acquisition_shard_bundle_refresh") or {}).get("results") or [])
    bundle_entries_by_snapshot = {
        str(entry.get("snapshot_id") or ""): dict(entry)
        for entry in bundle_entries
        if str(entry.get("snapshot_id") or "").strip()
    }
    shard_warmup: list[dict[str, Any]] = []
    for entry in registry_entries:
        snapshot_id = str(entry.get("snapshot_id") or "").strip()
        bundle_entry = bundle_entries_by_snapshot.get(snapshot_id, {})
        bundle_result = dict(bundle_entry.get("result") or {})
        shard_warmup.append(
            {
                "snapshot_id": snapshot_id,
                "registry": dict(entry.get("result") or {}),
                "bundles": {
                    "status": str(bundle_entry.get("status") or ""),
                    "bundle_count": int(bundle_result.get("bundle_count") or 0),
                    "manifest_path": str(bundle_result.get("manifest_path") or ""),
                },
            }
        )
    ledger_result = dict(sync_status.get("organization_completeness_ledger_refresh", {}).get("result") or {})
    return {
        "target_company": company,
        "backfill": {
            "status": str(backfill_result.get("status") or ""),
            "selected_snapshot_id": str(backfill_result.get("selected_snapshot_id") or ""),
            "record_count": int(backfill_result.get("record_count") or 0),
            "selected_reason": str(backfill_result.get("selected_reason") or ""),
        },
        "selected_snapshot_ids": selected_snapshot_ids,
        "shard_warmup": shard_warmup,
        "ledger": {
            "status": str(ledger_result.get("status") or ""),
            "ledger_path": str(ledger_result.get("ledger_path") or ""),
            "readiness": str(dict(ledger_result.get("summary") or {}).get("readiness") or ""),
        },
    }


def _resolve_post_import_refresh_mode(*, runtime_dir: Path) -> str:
    configured = str(os.getenv("SOURCING_IMPORT_POST_REFRESH_MODE") or "").strip().lower()
    if configured in {"inline", "background"}:
        return configured
    return "background" if resolve_control_plane_postgres_dsn(runtime_dir) else "inline"


def _post_import_refresh_job_id(
    *,
    bundle_kind: str,
    scoped_companies: list[str],
    scoped_snapshot_id: str,
) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    scope_token = sha1(
        "|".join(
            [
                str(bundle_kind or "").strip(),
                str(scoped_snapshot_id or "").strip(),
                *[str(item or "").strip().lower() for item in list(scoped_companies or []) if str(item or "").strip()],
            ]
        ).encode("utf-8")
    ).hexdigest()[:10]
    return f"post-import-refresh-{timestamp}-{scope_token}"


def _post_import_refresh_state_path(*, runtime_dir: Path, job_id: str) -> Path:
    return runtime_dir / "maintenance" / "import_refresh_jobs" / f"{job_id}.json"


def _write_post_import_refresh_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_post_import_refresh_background(
    *,
    runtime_dir: Path,
    db_path: Path,
    bundle_kind: str,
    scoped_companies: list[str],
    scoped_snapshot_id: str,
    asset_view: str,
    run_org_warmup: bool,
    run_profile_registry_backfill: bool,
    profile_registry_resume: bool,
    profile_progress_interval: int,
    job_id: str,
    state_path: Path,
) -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    _write_post_import_refresh_state(
        state_path,
        {
            "job_id": job_id,
            "status": "running",
            "bundle_kind": bundle_kind,
            "scoped_companies": scoped_companies,
            "scoped_snapshot_id": scoped_snapshot_id,
            "asset_view": asset_view,
            "started_at": started_at,
            "organization_warmup": {"status": "running" if run_org_warmup else "skipped_by_flag"},
            "profile_registry_backfill": {
                "status": "running" if run_profile_registry_backfill else "skipped_by_flag"
            },
        },
    )
    try:
        store = ControlPlaneStore(db_path)
        if run_org_warmup and scoped_companies:
            warmup_summary: dict[str, Any] = {
                "status": "completed",
                "asset_view": asset_view,
                "results": [
                    _warmup_company_assets(
                        runtime_dir=runtime_dir,
                        store=store,
                        company=company,
                        asset_view=asset_view,
                    )
                    for company in scoped_companies
                ],
            }
        elif run_org_warmup:
            warmup_summary = {
                "status": "skipped_missing_company_scope",
                "reason": "No company scope could be inferred from the imported asset scope; pass --company to enable organization warmup.",
            }
        else:
            warmup_summary = {"status": "skipped_by_flag"}

        if run_profile_registry_backfill and scoped_companies:
            profile_results: list[dict[str, Any]] = []
            for company in scoped_companies:
                profile_results.append(
                    backfill_linkedin_profile_registry(
                        runtime_dir=runtime_dir,
                        store=store,
                        company=company,
                        snapshot_id=scoped_snapshot_id if len(scoped_companies) == 1 else "",
                        resume=profile_registry_resume,
                        progress_interval=max(1, int(profile_progress_interval or 1)),
                    )
                )
            profile_registry_summary: dict[str, Any] = {
                "status": "completed",
                "results": profile_results,
            }
        elif run_profile_registry_backfill:
            profile_registry_summary = {
                "status": "skipped_missing_company_scope",
                "reason": "No company scope could be inferred from the imported asset scope; pass --company to enable profile registry backfill.",
            }
        else:
            profile_registry_summary = {"status": "skipped_by_flag"}
        completed_at = datetime.now(timezone.utc).isoformat()
        _write_post_import_refresh_state(
            state_path,
            {
                "job_id": job_id,
                "status": "completed",
                "bundle_kind": bundle_kind,
                "scoped_companies": scoped_companies,
                "scoped_snapshot_id": scoped_snapshot_id,
                "asset_view": asset_view,
                "started_at": started_at,
                "completed_at": completed_at,
                "organization_warmup": warmup_summary,
                "profile_registry_backfill": profile_registry_summary,
            },
        )
    except Exception as exc:
        _write_post_import_refresh_state(
            state_path,
            {
                "job_id": job_id,
                "status": "failed",
                "bundle_kind": bundle_kind,
                "scoped_companies": scoped_companies,
                "scoped_snapshot_id": scoped_snapshot_id,
                "asset_view": asset_view,
                "started_at": started_at,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "error": f"{type(exc).__name__}: {exc}",
            },
        )


def _post_import_runtime_refresh(
    *,
    runtime_dir: Path,
    db_path: Path,
    bundle_kind: str,
    scoped_companies: list[str],
    scoped_snapshot_id: str,
    asset_view: str,
    run_artifact_repair: bool,
    run_org_warmup: bool,
    run_profile_registry_backfill: bool,
    profile_registry_resume: bool,
    profile_progress_interval: int,
) -> tuple[ControlPlaneStore, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    store = ControlPlaneStore(db_path)
    matching_refresh_summary = dict(
        getattr(store, "last_matching_refresh_summary", {}) or {"status": "completed", "counts": {}}
    )
    company_identity_registry_summary: dict[str, Any] = {"status": "skipped"}
    if scoped_companies:
        company_identity_registry_summary = refresh_company_identity_registry(
            runtime_dir=runtime_dir,
            companies=scoped_companies,
        )

    repair_summary: dict[str, Any]
    warmup_summary: dict[str, Any]
    profile_registry_summary: dict[str, Any]
    if run_artifact_repair:
        if scoped_companies:
            repair_summary = repair_missing_company_candidate_artifacts(
                runtime_dir=runtime_dir,
                store=store,
                companies=scoped_companies,
                snapshot_id=scoped_snapshot_id,
            )
        else:
            repair_summary = {
                "status": "skipped_missing_company_scope",
                "reason": "No company scope could be inferred from the imported asset scope; pass --company to enable artifact repair.",
            }
    else:
        repair_summary = {"status": "skipped_by_flag"}

    defer_followup_refresh = bool(
        scoped_companies
        and (run_org_warmup or run_profile_registry_backfill)
        and _resolve_post_import_refresh_mode(runtime_dir=runtime_dir) == "background"
    )
    if defer_followup_refresh:
        job_id = _post_import_refresh_job_id(
            bundle_kind=bundle_kind,
            scoped_companies=scoped_companies,
            scoped_snapshot_id=scoped_snapshot_id,
        )
        state_path = _post_import_refresh_state_path(runtime_dir=runtime_dir, job_id=job_id)
        _write_post_import_refresh_state(
            state_path,
            {
                "job_id": job_id,
                "status": "scheduled",
                "bundle_kind": bundle_kind,
                "scoped_companies": scoped_companies,
                "scoped_snapshot_id": scoped_snapshot_id,
                "asset_view": asset_view,
                "scheduled_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        threading.Thread(
            target=_run_post_import_refresh_background,
            kwargs={
                "runtime_dir": runtime_dir,
                "db_path": db_path,
                "bundle_kind": bundle_kind,
                "scoped_companies": scoped_companies,
                "scoped_snapshot_id": scoped_snapshot_id,
                "asset_view": asset_view,
                "run_org_warmup": run_org_warmup,
                "run_profile_registry_backfill": run_profile_registry_backfill,
                "profile_registry_resume": profile_registry_resume,
                "profile_progress_interval": profile_progress_interval,
                "job_id": job_id,
                "state_path": state_path,
            },
            name=f"post-import-refresh-{job_id[-8:]}",
            daemon=True,
        ).start()
        warmup_summary = {
            "status": "scheduled" if run_org_warmup else "skipped_by_flag",
            "mode": "background",
            "asset_view": asset_view,
            "job_id": job_id,
            "state_path": str(state_path),
            "scoped_companies": scoped_companies,
        }
        profile_registry_summary = {
            "status": "scheduled" if run_profile_registry_backfill else "skipped_by_flag",
            "mode": "background",
            "job_id": job_id,
            "state_path": str(state_path),
            "scoped_companies": scoped_companies,
            "snapshot_id": scoped_snapshot_id if len(scoped_companies) == 1 else "",
        }
    else:
        if run_org_warmup:
            if scoped_companies:
                warmup_summary = {
                    "status": "completed",
                    "asset_view": asset_view,
                    "results": [
                        _warmup_company_assets(
                            runtime_dir=runtime_dir,
                            store=store,
                            company=company,
                            asset_view=asset_view,
                        )
                        for company in scoped_companies
                    ],
                }
            else:
                warmup_summary = {
                    "status": "skipped_missing_company_scope",
                    "reason": "No company scope could be inferred from the imported asset scope; pass --company to enable organization warmup.",
                }
        else:
            warmup_summary = {"status": "skipped_by_flag"}

        if run_profile_registry_backfill:
            if scoped_companies:
                profile_results: list[dict[str, Any]] = []
                for company in scoped_companies:
                    profile_results.append(
                        backfill_linkedin_profile_registry(
                            runtime_dir=runtime_dir,
                            store=store,
                            company=company,
                            snapshot_id=scoped_snapshot_id if len(scoped_companies) == 1 else "",
                            resume=profile_registry_resume,
                            progress_interval=max(1, int(profile_progress_interval or 1)),
                        )
                    )
                profile_registry_summary = {
                    "status": "completed",
                    "results": profile_results,
                }
            else:
                profile_registry_summary = {
                    "status": "skipped_missing_company_scope",
                    "reason": "No company scope could be inferred from the imported asset scope; pass --company to enable profile registry backfill.",
                }
        else:
            profile_registry_summary = {"status": "skipped_by_flag"}

    return (
        store,
        matching_refresh_summary,
        company_identity_registry_summary,
        repair_summary,
        warmup_summary,
        profile_registry_summary,
    )


def _import_candidate_generation(
    *,
    bundle_manager: AssetBundleManager,
    generation_manifest_path: str = "",
    generation_key: str = "",
    storage_client: Any | None = None,
    target_company: str = "",
    company_key: str = "",
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
    max_workers: int | None = None,
    resume: bool = True,
    prefer_local_link: bool = True,
    target_runtime_dir: str | Path | None = None,
    target_db_path: str | Path | None = None,
    companies: list[str] | None = None,
    run_artifact_repair: bool = True,
    run_org_warmup: bool = True,
    run_profile_registry_backfill: bool = True,
    profile_registry_resume: bool = True,
    profile_progress_interval: int = 200,
) -> dict[str, Any]:
    effective_runtime_dir = Path(target_runtime_dir) if target_runtime_dir else bundle_manager.runtime_dir
    effective_db_path = (
        Path(target_db_path)
        if target_db_path
        else resolve_default_control_plane_db_path(effective_runtime_dir, base_dir=effective_runtime_dir)
    )
    effective_bundle_manager = (
        bundle_manager
        if Path(bundle_manager.runtime_dir).resolve() == effective_runtime_dir.resolve()
        else AssetBundleManager(bundle_manager.project_root, effective_runtime_dir)
    )
    hydrate_summary = effective_bundle_manager.hydrate_published_generation(
        generation_manifest_path=generation_manifest_path,
        client=storage_client,
        target_company=target_company,
        company_key=company_key,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        generation_key=generation_key,
        max_workers=max_workers,
        resume=resume,
        prefer_local_link=prefer_local_link,
    )
    scoped_companies = _normalize_string_list(
        [
            *list(companies or []),
            hydrate_summary.get("target_company"),
            hydrate_summary.get("company_key"),
            target_company,
            company_key,
        ]
    )
    scoped_snapshot_id = str(hydrate_summary.get("snapshot_id") or snapshot_id or "").strip()
    (
        store,
        matching_refresh_summary,
        company_identity_registry_summary,
        repair_summary,
        warmup_summary,
        profile_registry_summary,
    ) = _post_import_runtime_refresh(
        runtime_dir=effective_runtime_dir,
        db_path=effective_db_path,
        bundle_kind="candidate_generation",
        scoped_companies=scoped_companies,
        scoped_snapshot_id=scoped_snapshot_id,
        asset_view=asset_view,
        run_artifact_repair=run_artifact_repair,
        run_org_warmup=run_org_warmup,
        run_profile_registry_backfill=run_profile_registry_backfill,
        profile_registry_resume=profile_registry_resume,
        profile_progress_interval=profile_progress_interval,
    )
    effective_store_target = str(
        getattr(store, "sqlite_shadow_connect_target", lambda: str(effective_db_path))()
    ).strip() or str(effective_db_path)
    resolved_manifest_path = str(
        hydrate_summary.get("generation_manifest_path") or generation_manifest_path or ""
    ).strip()
    summary = {
        "status": "completed",
        "bundle_kind": "candidate_generation",
        "bundle_id": str(hydrate_summary.get("generation_key") or generation_key or ""),
        "import_mode": "candidate_generation",
        "legacy_bundle_fallback_used": False,
        "manifest_path": resolved_manifest_path,
        "manifest_metadata": {},
        "target_runtime_dir": str(effective_runtime_dir),
        "target_db_path": effective_store_target,
        "scoped_companies": scoped_companies,
        "scoped_snapshot_id": scoped_snapshot_id,
        "download": {},
        "restore": hydrate_summary,
        "hydrate": hydrate_summary,
        "matching_metadata_refresh": matching_refresh_summary,
        "company_identity_registry_refresh": company_identity_registry_summary,
        "artifact_repair": repair_summary,
        "organization_warmup": warmup_summary,
        "profile_registry_backfill": profile_registry_summary,
    }
    sync_run = effective_bundle_manager._record_sync_run(
        action="import_generation",
        bundle_kind="candidate_generation",
        bundle_id=str(hydrate_summary.get("generation_key") or generation_key or ""),
        summary={
            "status": str(summary.get("status") or ""),
            "bundle_kind": "candidate_generation",
            "bundle_id": str(hydrate_summary.get("generation_key") or generation_key or ""),
            "scoped_companies": scoped_companies,
            "scoped_snapshot_id": scoped_snapshot_id,
            "target_runtime_dir": str(effective_runtime_dir),
            "target_db_path": effective_store_target,
            "artifact_repair_status": str(repair_summary.get("status") or ""),
            "organization_warmup_status": str(warmup_summary.get("status") or ""),
            "profile_registry_backfill_status": str(profile_registry_summary.get("status") or ""),
        },
        extra={
            "generation_manifest_path": resolved_manifest_path,
            "hydrate": hydrate_summary,
        },
    )
    summary["sync_run_id"] = str(sync_run.get("run_id") or "")
    ledger_entry = store.record_cloud_asset_operation(
        operation_type="import_generation",
        bundle_kind="candidate_generation",
        bundle_id=str(hydrate_summary.get("generation_key") or generation_key or ""),
        status=str(summary.get("status") or ""),
        sync_run_id=str(sync_run.get("run_id") or ""),
        manifest_path=resolved_manifest_path,
        target_runtime_dir=str(effective_runtime_dir),
        target_db_path=effective_store_target,
        scoped_companies=scoped_companies,
        scoped_snapshot_id=scoped_snapshot_id,
        summary=summary,
        metadata={
            "asset_view": str(hydrate_summary.get("asset_view") or asset_view or "").strip(),
            "prefer_local_link": bool(prefer_local_link),
            "hydrated_file_count": int(hydrate_summary.get("hydrated_file_count") or 0),
        },
    )
    summary["ledger"] = ledger_entry
    if resolved_manifest_path:
        summary_path = Path(resolved_manifest_path).expanduser().parent / "import_summary.json"
        summary["summary_path"] = str(summary_path)
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        summary["summary_path"] = ""
    return summary


def import_cloud_assets(
    *,
    bundle_manager: AssetBundleManager,
    manifest_path: str = "",
    bundle_kind: str = "",
    bundle_id: str = "",
    storage_client: Any | None = None,
    output_dir: str | Path | None = None,
    max_workers: int | None = None,
    resume: bool = True,
    target_runtime_dir: str | Path | None = None,
    conflict: str = "skip",
    target_db_path: str | Path | None = None,
    companies: list[str] | None = None,
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
    generation_key: str = "",
    prefer_generation: bool = True,
    allow_legacy_bundle_fallback: bool = True,
    prefer_local_link: bool = True,
    run_artifact_repair: bool = True,
    run_org_warmup: bool = True,
    run_profile_registry_backfill: bool = True,
    profile_registry_resume: bool = True,
    profile_progress_interval: int = 200,
) -> dict[str, Any]:
    effective_runtime_dir = Path(target_runtime_dir) if target_runtime_dir else bundle_manager.runtime_dir
    effective_db_path = (
        Path(target_db_path)
        if target_db_path
        else resolve_default_control_plane_db_path(effective_runtime_dir, base_dir=effective_runtime_dir)
    )
    effective_bundle_manager = (
        bundle_manager
        if Path(bundle_manager.runtime_dir).resolve() == effective_runtime_dir.resolve()
        else AssetBundleManager(bundle_manager.project_root, effective_runtime_dir)
    )

    download_summary: dict[str, Any] = {}
    resolved_manifest_path = (
        Path(str(manifest_path or "").strip()).expanduser() if str(manifest_path or "").strip() else None
    )
    requested_companies = _normalize_string_list(companies)
    requested_snapshot_id = str(snapshot_id or "").strip()
    requested_generation_key = str(generation_key or "").strip()
    requested_target_company = requested_companies[0] if requested_companies else ""
    generation_first_attempt: dict[str, Any] = {
        "status": "not_attempted",
        "reason": "uninitialized",
    }
    if resolved_manifest_path is not None and resolved_manifest_path.exists():
        manifest_preview = json.loads(resolved_manifest_path.read_text(encoding="utf-8"))
        _reject_retired_sqlite_bundle_kind(str(manifest_preview.get("bundle_kind") or ""))
        if str(manifest_preview.get("manifest_kind") or "").strip() == "candidate_generation":
            preview_target_company = str(
                manifest_preview.get("target_company") or requested_target_company or ""
            ).strip()
            preview_company_key = str(manifest_preview.get("company_key") or "").strip()
            preview_snapshot_id = str(manifest_preview.get("snapshot_id") or requested_snapshot_id or "").strip()
            preview_generation_key = str(
                manifest_preview.get("generation_key") or requested_generation_key or ""
            ).strip()
            return _import_candidate_generation(
                bundle_manager=effective_bundle_manager,
                generation_manifest_path=str(resolved_manifest_path),
                generation_key=preview_generation_key,
                storage_client=storage_client,
                target_company=preview_target_company,
                company_key=preview_company_key,
                snapshot_id=preview_snapshot_id,
                asset_view=str(manifest_preview.get("asset_view") or asset_view or "canonical_merged"),
                max_workers=max_workers,
                resume=resume,
                prefer_local_link=prefer_local_link,
                target_runtime_dir=effective_runtime_dir,
                target_db_path=effective_db_path,
                companies=requested_companies,
                run_artifact_repair=run_artifact_repair,
                run_org_warmup=run_org_warmup,
                run_profile_registry_backfill=run_profile_registry_backfill,
                profile_registry_resume=profile_registry_resume,
                profile_progress_interval=profile_progress_interval,
            )
        if prefer_generation and storage_client is not None:
            generation_hint = _generation_import_hint_from_manifest(
                manifest_preview,
                requested_companies=requested_companies,
                requested_snapshot_id=requested_snapshot_id,
                requested_generation_key=requested_generation_key,
                asset_view=asset_view,
            )
            if generation_hint:
                resolved_generation = effective_bundle_manager.resolve_candidate_generation(
                    client=storage_client,
                    target_company=str(generation_hint.get("target_company") or ""),
                    company_key=str(generation_hint.get("company_key") or ""),
                    snapshot_id=str(generation_hint.get("snapshot_id") or ""),
                    asset_view=str(generation_hint.get("asset_view") or asset_view or "canonical_merged"),
                    generation_key=str(generation_hint.get("generation_key") or ""),
                )
                if resolved_generation:
                    summary = _import_candidate_generation(
                        bundle_manager=effective_bundle_manager,
                        generation_key=str(
                            resolved_generation.get("generation_key")
                            or generation_hint.get("generation_key")
                            or requested_generation_key
                            or ""
                        ).strip(),
                        storage_client=storage_client,
                        target_company=str(
                            resolved_generation.get("target_company")
                            or generation_hint.get("target_company")
                            or requested_target_company
                            or ""
                        ).strip(),
                        company_key=str(
                            resolved_generation.get("company_key") or generation_hint.get("company_key") or ""
                        ).strip(),
                        snapshot_id=str(
                            resolved_generation.get("snapshot_id")
                            or generation_hint.get("snapshot_id")
                            or requested_snapshot_id
                            or ""
                        ).strip(),
                        asset_view=str(
                            resolved_generation.get("asset_view")
                            or generation_hint.get("asset_view")
                            or asset_view
                            or "canonical_merged"
                        ),
                        max_workers=max_workers,
                        resume=resume,
                        prefer_local_link=prefer_local_link,
                        target_runtime_dir=effective_runtime_dir,
                        target_db_path=effective_db_path,
                        companies=requested_companies,
                        run_artifact_repair=run_artifact_repair,
                        run_org_warmup=run_org_warmup,
                        run_profile_registry_backfill=run_profile_registry_backfill,
                        profile_registry_resume=profile_registry_resume,
                        profile_progress_interval=profile_progress_interval,
                    )
                    return _annotate_generation_first_import(
                        summary,
                        source="bundle_manifest",
                        bundle_kind=str(manifest_preview.get("bundle_kind") or ""),
                        bundle_id=str(manifest_preview.get("bundle_id") or ""),
                        resolved_generation=resolved_generation,
                    )
                generation_first_attempt = {
                    "status": "unresolved",
                    "source": "bundle_manifest",
                    "bundle_kind": str(manifest_preview.get("bundle_kind") or ""),
                    "bundle_id": str(manifest_preview.get("bundle_id") or ""),
                    "reason": "no_matching_generation",
                    "hint": generation_hint,
                }
            else:
                generation_first_attempt = {
                    "status": "skipped",
                    "source": "bundle_manifest",
                    "reason": "manifest_not_company_snapshot_or_missing_hint",
                    "bundle_kind": str(manifest_preview.get("bundle_kind") or ""),
                    "bundle_id": str(manifest_preview.get("bundle_id") or ""),
                }
        elif storage_client is None:
            generation_first_attempt = {
                "status": "skipped",
                "source": "bundle_manifest",
                "reason": "storage_client_unavailable",
                "bundle_kind": str(manifest_preview.get("bundle_kind") or ""),
                "bundle_id": str(manifest_preview.get("bundle_id") or ""),
            }
    if resolved_manifest_path is None:
        normalized_bundle_kind = str(bundle_kind or "").strip()
        normalized_bundle_id = str(bundle_id or "").strip()
        _reject_retired_sqlite_bundle_kind(normalized_bundle_kind)
        if normalized_bundle_kind == "candidate_generation":
            if storage_client is None:
                raise AssetBundleError("storage_client is required when importing a remote candidate generation")
            return _import_candidate_generation(
                bundle_manager=effective_bundle_manager,
                generation_key=str(normalized_bundle_id or requested_generation_key or "").strip(),
                storage_client=storage_client,
                target_company=requested_target_company,
                company_key="",
                snapshot_id=requested_snapshot_id,
                asset_view=asset_view,
                max_workers=max_workers,
                resume=resume,
                prefer_local_link=prefer_local_link,
                target_runtime_dir=effective_runtime_dir,
                target_db_path=effective_db_path,
                companies=requested_companies,
                run_artifact_repair=run_artifact_repair,
                run_org_warmup=run_org_warmup,
                run_profile_registry_backfill=run_profile_registry_backfill,
                profile_registry_resume=profile_registry_resume,
                profile_progress_interval=profile_progress_interval,
            )
        generation_first_attempt = {
            "status": "skipped",
            "source": "direct_request",
            "reason": "bundle_or_manifest_resolution_required",
            "bundle_kind": normalized_bundle_kind,
            "bundle_id": normalized_bundle_id,
        }
        if (
            prefer_generation
            and storage_client is not None
            and (
                not normalized_bundle_kind
                or normalized_bundle_kind
                in {"company_snapshot", "company_handoff", "control_plane_snapshot"}
            )
        ):
            generation_hint = _direct_generation_import_hint(
                bundle_kind=normalized_bundle_kind,
                requested_companies=requested_companies,
                requested_snapshot_id=requested_snapshot_id,
                requested_generation_key=requested_generation_key,
                asset_view=asset_view,
            )
            resolved_generation = effective_bundle_manager.resolve_candidate_generation(
                client=storage_client,
                target_company=str(generation_hint.get("target_company") or requested_target_company or "").strip(),
                company_key=str(generation_hint.get("company_key") or "").strip(),
                snapshot_id=str(generation_hint.get("snapshot_id") or requested_snapshot_id or "").strip(),
                asset_view=str(generation_hint.get("asset_view") or asset_view or "canonical_merged"),
                generation_key=str(generation_hint.get("generation_key") or requested_generation_key or "").strip(),
            )
            if resolved_generation:
                summary = _import_candidate_generation(
                    bundle_manager=effective_bundle_manager,
                    generation_key=str(
                        resolved_generation.get("generation_key") or requested_generation_key or ""
                    ).strip(),
                    storage_client=storage_client,
                    target_company=str(
                        resolved_generation.get("target_company") or requested_target_company or ""
                    ).strip(),
                    company_key=str(resolved_generation.get("company_key") or "").strip(),
                    snapshot_id=str(resolved_generation.get("snapshot_id") or requested_snapshot_id or "").strip(),
                    asset_view=str(resolved_generation.get("asset_view") or asset_view or "canonical_merged"),
                    max_workers=max_workers,
                    resume=resume,
                    prefer_local_link=prefer_local_link,
                    target_runtime_dir=effective_runtime_dir,
                    target_db_path=effective_db_path,
                    companies=requested_companies,
                    run_artifact_repair=run_artifact_repair,
                    run_org_warmup=run_org_warmup,
                    run_profile_registry_backfill=run_profile_registry_backfill,
                    profile_registry_resume=profile_registry_resume,
                    profile_progress_interval=profile_progress_interval,
                )
                return _annotate_generation_first_import(
                    summary,
                    source="direct_request",
                    bundle_kind=normalized_bundle_kind,
                    bundle_id=normalized_bundle_id,
                    resolved_generation=resolved_generation,
                )
            generation_first_attempt = {
                "status": "unresolved",
                "source": "direct_request",
                "reason": "no_matching_generation",
                "bundle_kind": normalized_bundle_kind,
                "bundle_id": normalized_bundle_id,
            }
        if not normalized_bundle_kind or not normalized_bundle_id:
            raise AssetBundleError("import-cloud-assets requires either --manifest or both --bundle-kind/--bundle-id")
        if storage_client is None:
            raise AssetBundleError("storage_client is required when importing from --bundle-kind/--bundle-id")
        download_summary = bundle_manager.download_bundle(
            bundle_kind=normalized_bundle_kind,
            bundle_id=normalized_bundle_id,
            client=storage_client,
            output_dir=output_dir,
            max_workers=max_workers,
            resume=resume,
        )
        resolved_manifest_path = Path(str(download_summary.get("manifest_path") or "")).expanduser()
    if not resolved_manifest_path.exists():
        raise AssetBundleError(f"Manifest not found: {resolved_manifest_path}")

    manifest_payload = json.loads(resolved_manifest_path.read_text(encoding="utf-8"))
    resolved_bundle_kind = str(manifest_payload.get("bundle_kind") or bundle_kind or "").strip()
    _reject_retired_sqlite_bundle_kind(resolved_bundle_kind)
    scoped_companies = requested_companies or _manifest_company_keys(manifest_payload)
    scoped_snapshot_id = _manifest_snapshot_id(manifest_payload, explicit_snapshot_id=snapshot_id)

    if not allow_legacy_bundle_fallback:
        raise AssetBundleError("No matching candidate generation resolved and legacy bundle fallback is disabled.")

    if resolved_bundle_kind == "control_plane_snapshot" and not resolve_control_plane_postgres_dsn(effective_runtime_dir):
        raise AssetBundleError("control_plane_snapshot restore requires a Postgres control-plane DSN.")

    restore_summary = bundle_manager.restore_bundle(
        resolved_manifest_path,
        target_runtime_dir=effective_runtime_dir,
        conflict=conflict,
    )
    restore_summary["restore_path_kind"] = (
        "control_plane_snapshot" if resolved_bundle_kind == "control_plane_snapshot" else "bundle_restore"
    )
    restore_summary["legacy_bundle_restore_used"] = False
    control_plane_snapshot_sync = _sync_restored_control_plane_snapshot(effective_runtime_dir)
    if resolved_bundle_kind == "control_plane_snapshot" and str(control_plane_snapshot_sync.get("status") or "") != "completed":
        raise AssetBundleError(
            "control_plane_snapshot restore requires Postgres sync to complete; "
            f"status={control_plane_snapshot_sync.get('status')}"
        )

    manifest_metadata = dict(manifest_payload.get("metadata") or {})
    (
        store,
        matching_refresh_summary,
        company_identity_registry_summary,
        repair_summary,
        warmup_summary,
        profile_registry_summary,
    ) = _post_import_runtime_refresh(
        runtime_dir=effective_runtime_dir,
        db_path=effective_db_path,
        bundle_kind=resolved_bundle_kind,
        scoped_companies=scoped_companies,
        scoped_snapshot_id=scoped_snapshot_id,
        asset_view=asset_view,
        run_artifact_repair=run_artifact_repair,
        run_org_warmup=run_org_warmup,
        run_profile_registry_backfill=run_profile_registry_backfill,
        profile_registry_resume=profile_registry_resume,
        profile_progress_interval=profile_progress_interval,
    )
    effective_store_target = str(
        getattr(store, "sqlite_shadow_connect_target", lambda: str(effective_db_path))()
    ).strip() or str(effective_db_path)

    summary = {
        "status": "completed",
        "bundle_kind": resolved_bundle_kind,
        "bundle_id": str(manifest_payload.get("bundle_id") or bundle_id or ""),
        "import_mode": "legacy_bundle_restore",
        "compatibility_mode": "legacy_bundle_restore",
        "legacy_bundle_fallback_used": True,
        "generation_first_attempt": generation_first_attempt,
        "manifest_path": str(resolved_manifest_path),
        "manifest_metadata": manifest_metadata,
        "target_runtime_dir": str(effective_runtime_dir),
        "target_db_path": effective_store_target,
        "scoped_companies": scoped_companies,
        "scoped_snapshot_id": scoped_snapshot_id,
        "download": download_summary,
        "restore": restore_summary,
        "control_plane_snapshot_sync": control_plane_snapshot_sync,
        "matching_metadata_refresh": matching_refresh_summary,
        "company_identity_registry_refresh": company_identity_registry_summary,
        "artifact_repair": repair_summary,
        "organization_warmup": warmup_summary,
        "profile_registry_backfill": profile_registry_summary,
    }
    sync_run = effective_bundle_manager._record_sync_run(
        action="import",
        bundle_kind=resolved_bundle_kind,
        bundle_id=str(manifest_payload.get("bundle_id") or bundle_id or ""),
        summary={
            "status": str(summary.get("status") or ""),
            "bundle_kind": resolved_bundle_kind,
            "bundle_id": str(manifest_payload.get("bundle_id") or bundle_id or ""),
            "scoped_companies": scoped_companies,
            "scoped_snapshot_id": scoped_snapshot_id,
            "target_runtime_dir": str(effective_runtime_dir),
            "target_db_path": effective_store_target,
            "artifact_repair_status": str(repair_summary.get("status") or ""),
            "organization_warmup_status": str(warmup_summary.get("status") or ""),
            "profile_registry_backfill_status": str(profile_registry_summary.get("status") or ""),
        },
        extra={
            "manifest_path": str(resolved_manifest_path),
            "manifest_metadata": manifest_metadata,
            "download": download_summary,
            "restore": restore_summary,
        },
    )
    summary["sync_run_id"] = str(sync_run.get("run_id") or "")
    ledger_entry = store.record_cloud_asset_operation(
        operation_type="import_bundle",
        bundle_kind=resolved_bundle_kind,
        bundle_id=str(manifest_payload.get("bundle_id") or bundle_id or ""),
        status=str(summary.get("status") or ""),
        sync_run_id=str(sync_run.get("run_id") or ""),
        manifest_path=str(resolved_manifest_path),
        target_runtime_dir=str(effective_runtime_dir),
        target_db_path=effective_store_target,
        scoped_companies=scoped_companies,
        scoped_snapshot_id=scoped_snapshot_id,
        summary=summary,
        metadata={
            "manifest_metadata": manifest_metadata,
            "download_summary_present": bool(download_summary),
            "restore_status": str(restore_summary.get("status") or ""),
        },
    )
    summary["ledger"] = ledger_entry
    summary_path = resolved_manifest_path.parent / "import_summary.json"
    summary["summary_path"] = str(summary_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def hydrate_cloud_generation(
    *,
    bundle_manager: AssetBundleManager,
    generation_manifest_path: str = "",
    generation_key: str = "",
    storage_client: Any | None = None,
    target_company: str = "",
    company_key: str = "",
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
    max_workers: int | None = None,
    resume: bool = True,
    prefer_local_link: bool = True,
    target_db_path: str | Path | None = None,
) -> dict[str, Any]:
    result = bundle_manager.hydrate_published_generation(
        generation_manifest_path=generation_manifest_path,
        client=storage_client,
        target_company=target_company,
        company_key=company_key,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        generation_key=generation_key,
        max_workers=max_workers,
        resume=resume,
        prefer_local_link=prefer_local_link,
    )
    effective_db_path = (
        Path(target_db_path)
        if target_db_path
        else resolve_default_control_plane_db_path(bundle_manager.runtime_dir, base_dir=bundle_manager.runtime_dir)
    )
    store = ControlPlaneStore(effective_db_path)
    ledger_entry = store.record_cloud_asset_operation(
        operation_type="hydrate_generation",
        bundle_kind="candidate_generation",
        bundle_id=str(result.get("generation_key") or generation_key or "").strip(),
        status=str(result.get("status") or ""),
        manifest_path=str(result.get("generation_manifest_path") or generation_manifest_path or "").strip(),
        target_runtime_dir=str(bundle_manager.runtime_dir),
        target_db_path=str(effective_db_path),
        scoped_companies=_normalize_string_list([target_company or result.get("target_company"), company_key]),
        scoped_snapshot_id=str(result.get("snapshot_id") or snapshot_id or "").strip(),
        summary=result,
        metadata={
            "asset_view": str(result.get("asset_view") or asset_view or "").strip(),
            "prefer_local_link": bool(prefer_local_link),
        },
    )
    return {
        **result,
        "ledger": ledger_entry,
    }
