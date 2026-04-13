from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .asset_reuse_planning import (
    backfill_organization_asset_registry_for_company,
    ensure_acquisition_shard_registry_for_snapshot,
)
from .asset_sync import AssetBundleError, AssetBundleManager
from .candidate_artifacts import repair_missing_company_candidate_artifacts
from .organization_assets import (
    ensure_acquisition_shard_bundles_for_snapshot,
    ensure_organization_completeness_ledger,
)
from .profile_registry_backfill import backfill_linkedin_profile_registry
from .storage import SQLiteStore


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


def _warmup_company_assets(
    *,
    runtime_dir: Path,
    store: SQLiteStore,
    company: str,
    asset_view: str,
) -> dict[str, Any]:
    backfill_result = backfill_organization_asset_registry_for_company(
        runtime_dir=runtime_dir,
        store=store,
        target_company=company,
        asset_view=asset_view,
    )
    selected_record = dict(backfill_result.get("selected_record") or {})
    selected_snapshot_ids = _normalize_string_list(
        list(selected_record.get("selected_snapshot_ids") or []) + [selected_record.get("snapshot_id")]
    )
    shard_warmup: list[dict[str, Any]] = []
    for snapshot_id in selected_snapshot_ids:
        registry_result = ensure_acquisition_shard_registry_for_snapshot(
            runtime_dir=runtime_dir,
            store=store,
            target_company=company,
            snapshot_id=snapshot_id,
        )
        bundle_result = ensure_acquisition_shard_bundles_for_snapshot(
            runtime_dir=runtime_dir,
            store=store,
            target_company=company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
        )
        shard_warmup.append(
            {
                "snapshot_id": snapshot_id,
                "registry": registry_result,
                "bundles": {
                    "status": str(bundle_result.get("status") or ""),
                    "bundle_count": int(bundle_result.get("bundle_count") or 0),
                    "manifest_path": str(bundle_result.get("manifest_path") or ""),
                },
            }
        )
    ledger_result: dict[str, Any] = {}
    if selected_record:
        ledger_result = ensure_organization_completeness_ledger(
            runtime_dir=runtime_dir,
            store=store,
            target_company=company,
            snapshot_id=str(selected_record.get("snapshot_id") or ""),
            asset_view=asset_view,
            selected_snapshot_ids=selected_snapshot_ids,
        )
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
    backup_current_db: bool = True,
    backup_dir: str | Path | None = None,
    companies: list[str] | None = None,
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
    run_artifact_repair: bool = True,
    run_org_warmup: bool = True,
    run_profile_registry_backfill: bool = True,
    profile_registry_resume: bool = True,
    profile_progress_interval: int = 200,
) -> dict[str, Any]:
    effective_runtime_dir = Path(target_runtime_dir) if target_runtime_dir else bundle_manager.runtime_dir
    effective_db_path = Path(target_db_path) if target_db_path else (effective_runtime_dir / "sourcing_agent.db")

    download_summary: dict[str, Any] = {}
    resolved_manifest_path = Path(str(manifest_path or "").strip()).expanduser() if str(manifest_path or "").strip() else None
    if resolved_manifest_path is None:
        normalized_bundle_kind = str(bundle_kind or "").strip()
        normalized_bundle_id = str(bundle_id or "").strip()
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
    requested_companies = _normalize_string_list(companies)
    scoped_companies = requested_companies or _manifest_company_keys(manifest_payload)
    scoped_snapshot_id = _manifest_snapshot_id(manifest_payload, explicit_snapshot_id=snapshot_id)

    if resolved_bundle_kind == "sqlite_snapshot":
        restore_summary = bundle_manager.restore_sqlite_snapshot(
            resolved_manifest_path,
            target_db_path=effective_db_path,
            backup_current=backup_current_db,
            backup_dir=backup_dir,
        )
    else:
        restore_summary = bundle_manager.restore_bundle(
            resolved_manifest_path,
            target_runtime_dir=effective_runtime_dir,
            conflict=conflict,
        )

    repair_summary: dict[str, Any] = {"status": "skipped"}
    warmup_summary: dict[str, Any] = {"status": "skipped"}
    profile_registry_summary: dict[str, Any] = {"status": "skipped"}
    manifest_metadata = dict(manifest_payload.get("metadata") or {})

    if resolved_bundle_kind != "sqlite_snapshot":
        store = SQLiteStore(effective_db_path)

        if run_artifact_repair:
            if scoped_companies:
                repair_summary = repair_missing_company_candidate_artifacts(
                    runtime_dir=effective_runtime_dir,
                    store=store,
                    companies=scoped_companies,
                    snapshot_id=scoped_snapshot_id,
                )
            else:
                repair_summary = {
                    "status": "skipped_missing_company_scope",
                    "reason": "No company scope could be inferred from bundle manifest; pass --company to enable artifact repair.",
                }
        else:
            repair_summary = {"status": "skipped_by_flag"}

        if run_org_warmup:
            if scoped_companies:
                warmup_summary = {
                    "status": "completed",
                    "asset_view": asset_view,
                    "results": [
                        _warmup_company_assets(
                            runtime_dir=effective_runtime_dir,
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
                    "reason": "No company scope could be inferred from bundle manifest; pass --company to enable organization warmup.",
                }
        else:
            warmup_summary = {"status": "skipped_by_flag"}

        if run_profile_registry_backfill:
            if scoped_companies:
                profile_results: list[dict[str, Any]] = []
                for company in scoped_companies:
                    profile_results.append(
                        backfill_linkedin_profile_registry(
                            runtime_dir=effective_runtime_dir,
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
                    "reason": "No company scope could be inferred from bundle manifest; pass --company to enable profile registry backfill.",
                }
        else:
            profile_registry_summary = {"status": "skipped_by_flag"}
    else:
        repair_summary = {"status": "skipped_by_bundle_kind", "bundle_kind": resolved_bundle_kind}
        warmup_summary = {"status": "skipped_by_bundle_kind", "bundle_kind": resolved_bundle_kind}
        profile_registry_summary = {"status": "skipped_by_bundle_kind", "bundle_kind": resolved_bundle_kind}

    summary = {
        "status": "completed",
        "bundle_kind": resolved_bundle_kind,
        "bundle_id": str(manifest_payload.get("bundle_id") or bundle_id or ""),
        "manifest_path": str(resolved_manifest_path),
        "manifest_metadata": manifest_metadata,
        "target_runtime_dir": str(effective_runtime_dir),
        "target_db_path": str(effective_db_path),
        "scoped_companies": scoped_companies,
        "scoped_snapshot_id": scoped_snapshot_id,
        "download": download_summary,
        "restore": restore_summary,
        "artifact_repair": repair_summary,
        "organization_warmup": warmup_summary,
        "profile_registry_backfill": profile_registry_summary,
    }
    summary_path = resolved_manifest_path.parent / "import_summary.json"
    summary["summary_path"] = str(summary_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary
